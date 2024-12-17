import os
import numpy as np
import rasterio
from rasterio.warp import reproject, Resampling
from pathlib import Path
import pandas as pd
from rasterio.windows import Window
from rasterio.transform import from_bounds

class DataProcessor:
    def __init__(self, soilgrids_dir, landsat_dir, output_dir, locations_csv):
        self.soilgrids_dir = soilgrids_dir
        self.landsat_dir = landsat_dir
        self.output_dir = output_dir
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        
        # Load location mappings
        self.locations_df = pd.read_csv(locations_csv)
        # Create dictionary mapping landsat_scene_id to location_id
        self.location_map = dict(zip(
            self.locations_df['landsat_scene_id'],
            self.locations_df['location_id']
         ))
        self.target_dimensions = (128, 128)  # Fixed dimensions matching SoilGrids
        self.target_resolution = 30  # meters per pixel
    
    def resample_landsat(self, landsat_image, soilgrids_image, output_path):
        """Resample Landsat image to match SoilGrids exactly in geography and resolution"""
        try:
            with rasterio.open(soilgrids_image) as soil_ds, rasterio.open(landsat_image) as landsat_ds:
                # Get the exact geographical bounds from SoilGrids
                soil_bounds = soil_ds.bounds
                
                # Create transform that exactly matches SoilGrids pixels
                output_transform = from_bounds(
                    soil_bounds.left, soil_bounds.bottom,
                    soil_bounds.right, soil_bounds.top,
                    self.target_dimensions[1], self.target_dimensions[0]
                )
                
                # Create output profile with exact matching
                output_profile = soil_ds.profile.copy()
                output_profile.update({
                    'dtype': 'uint16',
                    'count': landsat_ds.count,
                    'width': self.target_dimensions[1],
                    'height': self.target_dimensions[0],
                    'transform': output_transform,
                    'crs': soil_ds.crs,  # Use SoilGrids CRS
                    'compress': 'lzw',
                    'nodata': 0
                })
                
                # Create output array
                landsat_resampled = np.zeros(
                    (landsat_ds.count, self.target_dimensions[0], self.target_dimensions[1]),
                    dtype=np.uint16
                )
                
                # Reproject each band to exactly match SoilGrids
                for i in range(landsat_ds.count):
                    reproject(
                        source=landsat_ds.read(i + 1),
                        destination=landsat_resampled[i],
                        src_transform=landsat_ds.transform,
                        src_crs=landsat_ds.crs,
                        dst_transform=output_transform,
                        dst_crs=soil_ds.crs,
                        resampling=Resampling.average,
                        src_nodata=landsat_ds.nodata,
                        dst_nodata=0
                    )
                    # Ensure valid range
                    landsat_resampled[i] = np.clip(landsat_resampled[i], 0, 65535)
                
                # Write resampled data
                with rasterio.open(output_path, 'w', **output_profile) as dst:
                    dst.write(landsat_resampled)
                    
                    # Add georeferencing metadata
                    dst.update_tags(
                        TIFFTAG_GEOTIFF_VERSION='1.1.0',
                        TIFFTAG_GEOTIFF_KEYS=f'Aligned to SoilGrids image: {os.path.basename(soilgrids_image)}'
                    )

            # Verify alignment
            self._verify_alignment(output_path, soilgrids_image)
            
            # Cleanup original
            os.remove(landsat_image)
            print(f"Successfully resampled and aligned: {landsat_image}")
            return True
            
        except Exception as e:
            print(f"Error during resampling: {str(e)}")
            return False

    def _verify_alignment(self, resampled_path, reference_path):
        """Verify that images are exactly aligned"""
        with rasterio.open(resampled_path) as resampled, rasterio.open(reference_path) as reference:
            # Check spatial properties
            if not np.allclose(resampled.bounds, reference.bounds, rtol=1e-5):
                print("Warning: Image bounds do not exactly match")
            if not np.allclose(resampled.transform, reference.transform, rtol=1e-5):
                print("Warning: Image transforms do not exactly match")
            if resampled.crs != reference.crs:
                print("Warning: Image CRS do not match")
            if (resampled.width != reference.width or 
                resampled.height != reference.height):
                print("Warning: Image dimensions do not match")

    def process_all_images(self):
        """Process all Landsat images to match SoilGrids images"""
        print(f"Looking for images in: {self.landsat_dir}")
        if not os.path.exists(self.landsat_dir):
            print(f"ERROR: Landsat directory does not exist: {self.landsat_dir}")
            return
            
        found_files = False
        for root, dirs, files in os.walk(self.landsat_dir):
            print(f"Checking directory: {root}")
            
            for file in files:
                if file.upper().endswith('.TIF'):
                    found_files = True
                    landsat_image = os.path.join(root, file)
                    
                    # Extract Landsat scene ID from the directory name
                    scene_dir = os.path.basename(os.path.dirname(landsat_image))
                    
                    # Look up the corresponding location_id
                    location_id = self.location_map.get(scene_dir)
                    
                    if location_id:
                        soil_dir = os.path.join(self.soilgrids_dir, "tifs", f"location_{location_id}")
                        soilgrids_image = os.path.join(soil_dir, "soc_0-5cm_mean.tif")
                        
                        print(f"\nProcessing:")
                        print(f"Landsat scene: {scene_dir}")
                        print(f"Location ID: {location_id}")
                        print(f"Landsat image: {landsat_image}")
                        print(f"Looking for SoilGrids image: {soilgrids_image}")
                        
                        if os.path.exists(soilgrids_image):
                            output_path = os.path.join(self.output_dir, f"resampled_{location_id}_{file}")
                            print(f"Resampling to: {output_path}")
                            self.resample_landsat(landsat_image, soilgrids_image, output_path)
                            print(f"Successfully resampled {landsat_image}")
                        else:
                            print(f"ERROR: SoilGrids image not found: {soilgrids_image}")
                    else:
                        print(f"WARNING: No location ID mapping found for scene: {scene_dir}")
        
        if not found_files:
            print("No .TIF files were found in the Landsat directory!")

def main():
    base_dir = "c:/Users/Rohan/carbon-seq-model/processed_data"
    locations_csv = os.path.join(base_dir, "master_locations.csv")
    
    processor = DataProcessor(
        soilgrids_dir=os.path.join(base_dir, "soilgrids_data"),
        landsat_dir=os.path.join(base_dir, "landsat_data"),
        output_dir=os.path.join(base_dir, "resampled_landsat_data"),
        locations_csv=locations_csv
    )
    processor.process_all_images()

if __name__ == "__main__":
    main()
