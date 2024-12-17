import pandas as pd
import numpy as np
from soilgrids import SoilGrids
import matplotlib.pyplot as plt 
from pathlib import Path
import rasterio
from pyproj import Transformer
import requests
from tqdm import tqdm
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

class SoilGridsFetcher:
    def __init__(self, data_manager):
        self.data_manager = data_manager
        self.output_dir = str(data_manager.soilgrids_dir)
        self.soil_grids = SoilGrids()
        self.buffer_size = 10000  # meters instead of degrees
        
        # Create output directories
        self.data_dir = os.path.join(self.output_dir, "tifs")
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        
        # Only SOC and OCD variables at 0-5cm depth
        self.variables = {
            "soc": ["0-5cm"],    # Soil organic carbon content
            "ocd": ["0-5cm"]     # Organic carbon density
        }
        
        # Create lookup for full variable names
        self.variable_names = {
            "soc": "Soil organic carbon content",
            "ocd": "Organic carbon density" 
        }
        
        self.max_workers = 10  # Concurrent downloads
        self.max_retries = 3
        self.retry_delay = 5
        self.download_lock = threading.Lock()  # Thread-safe downloads

    def convert_to_isric_crs(self, lat, lon):
        """Convert lat/lon to ISRIC CRS coordinates"""
        # Replace the target CRS with EPSG:3857 (Web Mercator) which uses meters
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857")
        x, y = transformer.transform(lat, lon)
        return x, y

    def _download_variable(self, var_name, depth, west, south, east, north, location_dir, width, height):
        """Download a single variable with retries"""
        for attempt in range(self.max_retries):
            try:
                coverage_id = f"{var_name}_{depth}_mean"
                output_file = os.path.join(location_dir, f"{coverage_id}.tif")

                print(f"Downloading {self.variable_names[var_name]} ({depth})...")
                
                with self.download_lock:  # Thread-safe API access
                    data = self.soil_grids.get_coverage_data(
                        service_id=var_name,
                        coverage_id=coverage_id,
                        west=west,
                        south=south,
                        east=east,
                        north=north,
                        crs="urn:ogc:def:crs:EPSG::3857",
                        width=width,
                        height=height,
                        output=output_file
                    )

                if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                    self._save_metadata(var_name, depth, west, south, east, north, location_dir)
                    print(f"Successfully downloaded: {coverage_id}")
                    return True
                    
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {var_name} {depth}: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                continue
                
        return False

    def _save_metadata(self, var_name, depth, west, south, east, north, location_dir):
        """Save metadata for downloaded variable"""
        coverage_id = f"{var_name}_{depth}_mean"
        meta_file = os.path.join(location_dir, f"{coverage_id}_metadata.txt")
        with open(meta_file, 'w') as f:
            f.write(f"Variable: {self.variable_names[var_name]}\n")
            f.write(f"Depth: {depth}\n")
            f.write(f"Bounds: west={west}, south={south}, east={east}, north={north}\n")
            if hasattr(self.soil_grids, 'metadata'):
                for key, value in self.soil_grids.metadata.items():
                    f.write(f"{key}: {value}\n")

    def get_location_data(self, lat, lon, location_id):
        """Download SOC/OCD data using concurrent downloads"""
        try:
            print(f"Processing location {location_id} ({lat}, {lon})")
            
            # Calculate bounding box and dimensions
            west = lon - self.buffer_size
            east = lon + self.buffer_size
            south = lat - self.buffer_size
            north = lat + self.buffer_size
            
            width = height = 128  # Fixed size for better performance

            # Create location directory
            location_dir = os.path.join(self.data_dir, f"location_{location_id}")
            Path(location_dir).mkdir(exist_ok=True)

            # Download variables concurrently
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                for var_name, depths in self.variables.items():
                    for depth in depths:
                        future = executor.submit(
                            self._download_variable,
                            var_name, depth, west, south, east, north,
                            location_dir, width, height
                        )
                        futures.append((future, var_name, depth))

                # Track success of all downloads
                download_success = True
                for future, var_name, depth in futures:
                    try:
                        if not future.result():
                            download_success = False
                            print(f"Failed to download {var_name} {depth}")
                    except Exception as e:
                        download_success = False
                        print(f"Error downloading {var_name} {depth}: {str(e)}")

            if download_success:
                self.data_manager.update_soilgrids_id(location_id, str(location_id))
            else:
                # Clean up failed downloads
                if os.path.exists(location_dir):
                    for file in os.listdir(location_dir):
                        os.remove(os.path.join(location_dir, file))
                    os.rmdir(location_dir)
                print(f"Failed to download all data for location {location_id}")

        except Exception as e:
            print(f"Error downloading data for location {location_id}: {str(e)}")

    def process_coordinates(self, csv_file):
        """Process coordinates with concurrent location processing"""
        df = pd.read_csv(csv_file)
        print(f"Processing {len(df)} locations...")
        
        # Process multiple locations concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for idx, row in df.iterrows():
                if not row.get('soilgrids_id'):  # Skip already processed
                    future = executor.submit(
                        self.get_location_data,
                        row['latitude'],
                        row['longitude'],
                        row['location_id']
                    )
                    futures.append((future, idx))

            for future, idx in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing location at index {idx}: {str(e)}")

def main():
    fetcher = SoilGridsFetcher()
    fetcher.process_coordinates("/csv/north_american_forests.csv")

if __name__ == "__main__":
    main()
