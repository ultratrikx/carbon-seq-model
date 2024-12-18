import json
import requests
import sys
import time
import cgi
import os
import pandas as pd
import warnings
import backoff
import urllib3
import tarfile
<<<<<<< HEAD
=======
import logging
import subprocess
>>>>>>> 1c54ca0022e8f864771e022c50c4d37cffdfa670
import shutil
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from data_process import DataProcessor
from http.client import IncompleteRead
from requests.exceptions import ConnectionError
warnings.filterwarnings("ignore")

USERNAME = "ArjunGupta"
TOKEN = "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"

class LandsatFetcher:
    def __init__(self, data_manager, max_workers=5):
        self.service_url = "https://m2m.cr.usgs.gov/api/api/json/stable/"
        self.api_key = None
        self.data_manager = data_manager
        self.output_dir = str(data_manager.landsat_dir)
        self.max_workers = max_workers
        self.desired_bands = ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7']
        self.band_patterns = [
            '_SR_B1.TIF', '_SR_B2.TIF', '_SR_B3.TIF',
            '_SR_B4.TIF', '_SR_B5.TIF', '_SR_B7.TIF'
        ]
        self.required_files = [
            '_SR_B1.TIF',  # Surface Reflectance Band 1
            '_SR_B2.TIF',  # Surface Reflectance Band 2
            '_SR_B3.TIF',  # Surface Reflectance Band 3
            '_SR_B4.TIF',  # Surface Reflectance Band 4
            '_SR_B5.TIF',  # Surface Reflectance Band 5
            '_SR_B7.TIF',  # Surface Reflectance Band 7
            '_MTL.txt'     # Metadata file
        ]
        self.required_bands = [
            '_SR_B1.TIF',  # Surface Reflectance Band 1 
            '_SR_B2.TIF',  # Surface Reflectance Band 2
            '_SR_B3.TIF',  # Surface Reflectance Band 3
            '_SR_B4.TIF',  # Surface Reflectance Band 4 
            '_SR_B5.TIF',  # Surface Reflectance Band 5
            '_SR_B7.TIF',  # Surface Reflectance Band 7
            '_MTL.TXT'     # Metadata file
        ]
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # Create subdirectories for extracted bands
        self.bands_dir = os.path.join(self.output_dir, "bands")
        if not os.path.exists(self.bands_dir):
            os.makedirs(self.bands_dir)
        
        # Correct the resampled directory path to prevent extra 'resampled' folder creation
        self.resampled_dir = os.path.join(self.bands_dir, "resampled")
        if not os.path.exists(self.resampled_dir):
            os.makedirs(self.resampled_dir)

        # Use urllib.parse instead of deprecated cgi module
        from urllib.parse import unquote
        self.unquote = unquote

        self.scene_mappings = {}  # To store coordinate-to-scene mappings

        # Initialize DataProcessor with the correct resampled directory
        self.processor = DataProcessor(
            str(data_manager.soilgrids_dir),
            str(data_manager.landsat_dir),
            self.resampled_dir,  # Corrected resampled directory
            os.path.join(str(data_manager.base_dir), "master_locations.csv")
        )
        self.session_start_time = None
        self.session_duration = 3600  # 1 hour session duration
        
    def is_session_valid(self):
        """Check if current session is still valid"""
        if not self.session_start_time or not self.api_key:
            return False
        return (time.time() - self.session_start_time) < self.session_duration

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, urllib3.exceptions.ProtocolError),
        max_tries=5
    )
    def send_request(self, endpoint, data, api_key=None):
        """Send request to M2M API with retries and exponential backoff"""
        # Create a session for this request
        with requests.Session() as session:
            retries = Retry(
                total=5,
                backoff_factor=1,
                status_forcelist=[500, 502, 503, 504],
                allowed_methods=["POST", "GET"]
            )
            adapter = HTTPAdapter(max_retries=retries)
            session.mount('https://', adapter)

            json_data = json.dumps(data)
            headers = {'X-Auth-Token': api_key} if api_key else {}
            
            try:
                response = session.post(
                    self.service_url + endpoint, 
                    data=json_data,
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()
                
                output = response.json()
                if output.get('errorCode'):
                    print(f"API Error: {output['errorCode']} - {output['errorMessage']}")
                    return False
                    
                return output.get('data')
                
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {str(e)}")
                raise

    def login(self, username, token):
        """Login to M2M API using username and token"""
        print("Logging in...\n")
        login_payload = {'username': username, 'token': token}
        self.api_key = self.send_request("login-token", login_payload)
        
        if self.api_key:
            self.session_start_time = time.time()
            print('\nLogin Successful!')
            return True
        return False

    def search_scenes(self, latitude, longitude, date_range):
        """Search for Landsat scenes using metadata filters"""
        # Create spatial filter using bounding box around point
        spatial_filter = {
            'filterType': 'mbr',
            'lowerLeft': {
                'latitude': latitude - 0.5,
                'longitude': longitude - 0.5
            },
            'upperRight': {
                'latitude': latitude + 0.5,
                'longitude': longitude + 0.5
            }
        }

        # Create search payload
        search_payload = {
            'datasetName': 'landsat_ot_c2_l2',  # Landsat 8-9 Collection 2 Level-2
            'maxResults': 10,
            'sceneFilter': {
                'spatialFilter': spatial_filter,
                'acquisitionFilter': date_range,
                'cloudCoverFilter': {'min': 0, 'max': 20}
            }
        }

        # Submit scene search
        scenes = self.send_request("scene-search", search_payload, self.api_key)
        if not scenes:
            return None
            
        return scenes.get('results', [])

    def get_download_options(self, entity_ids):
        """Get download options for scenes, filtering for Level-2 bundles"""
        payload = {
            'datasetName': 'landsat_ot_c2_l2',
            'entityIds': entity_ids
        }
        
        download_options = self.send_request("download-options", payload, self.api_key)
        if not download_options:
            print(f"No download options returned for entities: {entity_ids}")
            return []

        # Filter for Level-2 bundles
        filtered_options = []
        for option in download_options:
            if (option.get('available') and 
                option.get('downloadSystem') != 'folder' and
                'Level-2 Product Bundle' in option.get('productName', '')):
                filtered_options.append(option)

        return filtered_options

    def request_download(self, products):
        """Request download for products"""
        payload = {
            'downloads': products,
            'label': 'download-sample'
        }
        
        return self.send_request("download-request", payload, self.api_key)

    def download_files(self, download_urls):
        """Download files concurrently with retries and chunked transfer"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for download in download_urls:
                future = executor.submit(self._download_single_file, download)
                futures.append(future)
            
            # Wait for all downloads to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Download failed: {str(e)}")

    def extract_and_resample_bands(self, tar_path, location_id):
        """Extract bands, resample, and cleanup original files"""
        scene_id = Path(tar_path).stem.split('.')[0]
        print(f"\nProcessing {scene_id}")

        extracted_files = []
        resampled_files = []
        tar_file = None

        try:
            # Update data manager with scene ID
            self.data_manager.update_landsat_scene(location_id, scene_id)

            scene_dir = os.path.join(self.bands_dir, scene_id)
            os.makedirs(scene_dir, exist_ok=True)

            # Verify the TAR file integrity before extraction
            if not tarfile.is_tarfile(tar_path):
                raise Exception(f"Invalid or corrupted TAR file: {tar_path}")

            # Extract files with proper file handling
            try:
                tar_file = tarfile.open(tar_path)
                members = tar_file.getmembers()
                band_files = [m for m in members if any(band in m.name.upper() for band in self.required_bands)]

                if len(band_files) < len(self.required_bands):
                    raise Exception(f"Missing required bands in {tar_path}")

                for band_file in band_files:
                    band_file.name = os.path.basename(band_file.name)
                    dest_path = os.path.join(scene_dir, band_file.name)

                    print(f"Extracting {band_file.name}")
                    with tar_file.extractfile(band_file) as source, open(dest_path, 'wb') as target:
                        shutil.copyfileobj(source, target)
                    extracted_files.append(dest_path)

                    # Validate the extracted file
                    if os.path.getsize(dest_path) == 0:
                        raise Exception(f"Extracted file is empty: {dest_path}")

            finally:
                if tar_file:
                    tar_file.close()

            # Ensure all files are properly closed
            time.sleep(1)

            # Resample files
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                for input_file in extracted_files:
                    output_name = f"resampled_{location_id}_{os.path.basename(input_file)}"
                    output_path = os.path.join(self.resampled_dir, output_name)
                    cmd = [
                        "gdalwarp",
                        "-tr", "30", "30",
                        "-r", "bilinear",
                        "-overwrite",
                        input_file,
                        output_path
                    ]
                    futures.append(executor.submit(self._run_subprocess, cmd, output_path))

                # Collect resampled files
                for future in as_completed(futures):
                    output_path = future.result()
                    if output_path:
                        resampled_files.append(output_path)

            # Verify that all bands have been resampled
            expected_bands = [band for band in self.required_bands if band.endswith('.TIF')]
            if len(resampled_files) != len(expected_bands):
                raise Exception(f"Resampling incomplete. Expected {len(expected_bands)} bands, got {len(resampled_files)}")

            # Delete the TAR file after resampling is complete
            os.remove(tar_path)
            print(f"Deleted tar file: {tar_path}")

            # Clean up extracted files after resampling
            for file in extracted_files:
                os.remove(file)
                print(f"Deleted original file: {file}")

            # Remove scene directory if empty
            if not os.listdir(scene_dir):
                os.rmdir(scene_dir)
                print(f"Deleted empty scene directory: {scene_dir}")

            return scene_id

        except Exception as e:
            print(f"Error processing {scene_id}: {str(e)}")
            # Clean up in case of error
            for file in resampled_files + extracted_files:
                if os.path.exists(file):
                    os.remove(file)
                    print(f"Cleaned up file: {file}")

            # Attempt to delete the TAR file if it still exists
            if os.path.exists(tar_path):
                os.remove(tar_path)

            raise

    def _run_subprocess(self, cmd, output_path):
        """Run subprocess command and handle exceptions"""
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(f"Successfully resampled: {os.path.basename(output_path)}")

            # Validate the resampled file
            if os.path.getsize(output_path) == 0:
                raise Exception(f"Resampled file is empty: {output_path}")

            return output_path

        except subprocess.CalledProcessError as e:
            print(f"Error during resampling: {e.stderr.decode().strip()}")
            return None

    def _download_single_file(self, download, location_id=None):
        """Download file with coordinate tracking"""
        url = download['url']
        filepath = None

        try:
            with requests.Session() as session:
                response = session.get(url, stream=True)
                response.raise_for_status()

                content_disp = response.headers.get('Content-Disposition', '')
                filename = content_disp.split('filename=')[-1].strip('"')
                filename = self.unquote(filename)
                filepath = os.path.join(self.output_dir, filename)

                # If file exists, delete it first
                if os.path.exists(filepath):
                    os.remove(filepath)

                total_size = int(response.headers.get('content-length', 0))
                block_size = 8192

                with open(filepath, 'wb') as f:
                    with tqdm(total=total_size, unit='B', unit_scale=True) as pbar:
                        for chunk in response.iter_content(chunk_size=block_size):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                                f.flush()

                print(f"Successfully downloaded: {filename}")

                # Verify the integrity of the downloaded file
                if not tarfile.is_tarfile(filepath):
                    raise Exception(f"Downloaded file is not a valid TAR archive: {filepath}")

                # Extract bands and get scene ID
                scene_id = self.extract_and_resample_bands(filepath, location_id)

                return filepath, scene_id

        except Exception as e:
            print(f"Download or extraction failed for {url}: {str(e)}")
            # Clean up partial download
            if filepath and os.path.exists(filepath):
                os.remove(filepath)
            raise

    def logout(self):
        """Logout from M2M API"""
        if self.send_request("logout", None, self.api_key) is None:
            print("\nLogged out successfully")
            return True
        print("\nLogout failed")
        return False

    def process_coordinates(self, coordinates_df, batch_size=10):
        """Process coordinates and update CSV with scene IDs"""
        try:
            # Process coordinates in batches
            coordinate_batches = [
                coordinates_df[i:i + batch_size] 
                for i in range(0, len(coordinates_df), batch_size)
            ]

            for batch in coordinate_batches:
                print(f"\nProcessing batch of {len(batch)} coordinates")
                
                # Process batch concurrently
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = []
                    for _, row in batch.iterrows():
                        future = executor.submit(
                            self._process_single_coordinate,
                            row['latitude'],
                            row['longitude'],
                            row['location_id']
                        )
                        futures.append(future)
                    
                    # Wait for batch to complete
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            print(f"Batch processing error: {str(e)}")
                            continue

            # Add scene IDs to DataFrame
            scene_ids = []
            for _, row in coordinates_df.iterrows():
                coord_key = f"{row['latitude']:.4f}_{row['longitude']:.4f}"
                scene_ids.append(self.scene_mappings.get(coord_key, None))
            
            # Update DataFrame with scene IDs
            coordinates_df['scene_id'] = scene_ids
            
            # Save updated CSV
            output_csv = os.path.join(self.output_dir, "coordinates_with_scenes.csv")
            coordinates_df.to_csv(output_csv, index=False)
            print(f"\nUpdated coordinates saved to: {output_csv}")
            
        except Exception as e:
            print(f"Error processing coordinates: {str(e)}")
            raise

    def select_best_scene(self, scenes):
        """Select the best scene based on quality criteria"""
        if not scenes:
            return None
            
        # Convert list to DataFrame for easier filtering
        scenes_df = pd.DataFrame(scenes)
        
        # Apply filters in order of priority:
        # 1. Filter out scenes with snow/ice (if that metadata is available)
        # 2. Keep only scenes with cloud cover below threshold
        # 3. Sort by cloud cover and quality metrics
        scenes_df = scenes_df[scenes_df['cloudCover'] <= 20]
        
        if len(scenes_df) == 0:
            return None
            
        # Sort by multiple criteria:
        # - Lower cloud cover is better
        # - Prefer Tier 1 data (if available in metadata)
        # - Newer data is generally better
        scenes_df = scenes_df.sort_values(
            by=['cloudCover', 'displayId'],
            ascending=[True, False]
        )
        
        # Return the best scene
        if len(scenes_df) > 0:
            return scenes_df.iloc[0].to_dict()
        return None

    def _process_single_coordinate(self, lat, lon, location_id):
        retries = 3
        success = False  # Track if we've successfully downloaded
        
        for attempt in range(retries):
            try:
                if success:  # Skip retry if we've already succeeded
                    break
                    
                if attempt > 0:
                    logging.info(f"Retry attempt {attempt+1} for location {location_id}")
                    if not self.refresh_api_key():
                        raise Exception("Failed to refresh API key")
                    time.sleep(5 * attempt)  # Exponential backoff
                
                print(f"Processing lat: {lat}, lon: {lon}")
                
                date_range = {'start': '2024-06-01', 'end': '2024-07-31'}
                scenes = self.search_scenes(lat, lon, date_range)
                
                if not scenes:
                    print(f"No scenes found for {lat}, {lon}")
                    return

                best_scene = self.select_best_scene(scenes)
                if not best_scene:
                    print(f"No suitable scenes found for {lat}, {lon}")
                    return

                print(f"Selected best scene: {best_scene['displayId']} (Cloud cover: {best_scene['cloudCover']}%)")
                
                download_options = self.get_download_options([best_scene['entityId']])
                
                if not download_options:
                    print(f"No download options available for {best_scene['displayId']}")
                    return

                products = [{
                    'entityId': option['entityId'],
                    'productId': option['id']
                } for option in download_options]

                if products:
                    download_results = self.request_download(products)
                    
                    if download_results and 'availableDownloads' in download_results:
                        download_success = True  # Track success for this batch
                        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                            futures = []
                            for download in download_results['availableDownloads']:
                                future = executor.submit(
                                    self._download_single_file,
                                    download,
                                    location_id
                                )
                                futures.append(future)
                            
                            # Wait for all downloads to complete
                            for future in as_completed(futures):
                                try:
                                    result = future.result()
                                    if result:  # If download was successful
                                        success = True  # Mark overall process as successful
                                        self.data_manager._save_data()  # Save progress
                                except Exception as e:
                                    print(f"Download failed: {str(e)}")
                                    download_success = False
                            
                            if download_success:
                                break  # Exit retry loop if downloads were successful
                    else:
                        print(f"Failed to get download URLs for {best_scene['displayId']}")
                else:
                    print(f"No valid products found for {best_scene['displayId']}")
                    return  # No need to retry if no products found
                    
            except Exception as e:
                print(f"Error processing coordinate {lat}, lon: {str(e)}")
                if attempt == retries - 1:  # Only raise on final attempt
                    raise

    def refresh_api_key(self):
        """Refresh API key to prevent session timeout"""
        try:
            self.logout()
            time.sleep(1)
            return self.login(USERNAME, TOKEN)
        except Exception as e:
            logging.error(f"Failed to refresh API key: {str(e)}")
            return False

    def _download_single_file_only(self, lat, lon, location_id):
        """Download file only, with progress bar and retry mechanism"""
        try:
            date_range = {'start': '2024-06-01', 'end': '2024-07-31'}
            scenes = self.search_scenes(lat, lon, date_range)
            
            if not scenes:
                return None

            best_scene = self.select_best_scene(scenes)
            if not best_scene:
                return None

            download_options = self.get_download_options([best_scene['entityId']])
            if not download_options:
                return None

            products = [{
                'entityId': option['entityId'],
                'productId': option['id']
            } for option in download_options]

            if not products:
                return None

            download_results = self.request_download(products)
            if not download_results or 'availableDownloads' not in download_results:
                return None

            # Download first available file
            download = download_results['availableDownloads'][0]
            url = download['url']
            
            # Setup retry strategy
            retry_strategy = Retry(
                total=5,
                status_forcelist=[429, 500, 502, 503, 504],
                method_whitelist=["GET"],
                backoff_factor=1
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            
            with requests.Session() as session:
                session.mount("https://", adapter)
                session.mount("http://", adapter)
                
                response = session.get(url, stream=True, timeout=30)
                response.raise_for_status()
                
                content_disp = response.headers.get('Content-Disposition', '')
                filename = content_disp.split('filename=')[-1].strip('"')
                filename = self.unquote(filename)
                filepath = os.path.join(self.output_dir, filename)

                total_size = int(response.headers.get('content-length', 0))
                block_size = 8192
                
                with open(filepath, 'wb') as f:
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Location {location_id}") as pbar:
                        for chunk in response.iter_content(chunk_size=block_size):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                
                return filepath
                
        except (ConnectionError, IncompleteRead) as e:
            print(f"Download error for {lat}, {lon}: {str(e)}")
            return None
        except Exception as e:
            print(f"Download failed for {lat}, {lon}: {str(e)}")
            return None

    def cleanup_temp_files(self):
        """Clean up any temporary files and empty directories"""
        try:
            # Clean up temp directory
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                os.makedirs(self.temp_dir)
                
            # Remove empty directories in bands_dir
            for root, dirs, files in os.walk(self.bands_dir, topdown=False):
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    if not os.listdir(dir_path):
                        os.rmdir(dir_path)
        except Exception as e:
            print(f"Error during cleanup: {str(e)}")

def main():
    fetcher = LandsatFetcher()
    
    # Replace with your ERS credentials
    username = "ArjunGupta"
    token = "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"
    
    if not fetcher.login(username, token):
        return
    
    try:
        # Read coordinates from CSV
        coords_df = pd.read_csv("csv\\north_american_forests.csv")
        fetcher.process_coordinates(coords_df)
    finally:
        fetcher.logout()

if __name__ == "__main__":
    main()

