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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
warnings.filterwarnings("ignore")

class LandsatFetcher:
    def __init__(self, output_dir="landsat_images", max_workers=5):
        self.service_url = "https://m2m.cr.usgs.gov/api/api/json/stable/"
        self.api_key = None
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.desired_bands = ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7']
        self.band_patterns = [
            '_SR_B1.TIF', '_SR_B2.TIF', '_SR_B3.TIF',
            '_SR_B4.TIF', '_SR_B5.TIF', '_SR_B7.TIF'
        ]
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Configure requests session with retries
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST", "GET"]
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, urllib3.exceptions.ProtocolError),
        max_tries=5
    )
    def send_request(self, endpoint, data, api_key=None):
        """Send request to M2M API with retries and exponential backoff"""
        json_data = json.dumps(data)
        headers = {'X-Auth-Token': api_key} if api_key else {}
        
        try:
            response = self.session.post(
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
            return None

        # Filter for Level-2 bundles that are available for direct download
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

    def _download_single_file(self, download):
        """Download a single file with retries"""
        url = download['url']
        print(f"Downloading: {url}")
        
        try:
            response = self.session.get(url, stream=True)
            response.raise_for_status()
            
            content_disposition = cgi.parse_header(response.headers['Content-Disposition'])[1]
            filename = os.path.basename(content_disposition['filename'])
            filepath = os.path.join(self.output_dir, filename)

            total_size = int(response.headers.get('content-length', 0))
            block_size = 8192
            
            with open(filepath, 'wb') as f:
                with tqdm(total=total_size, unit='B', unit_scale=True) as pbar:
                    for chunk in response.iter_content(chunk_size=block_size):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            
            print(f"Successfully downloaded: {filename}")
            return filepath
            
        except Exception as e:
            print(f"Download failed for {url}: {str(e)}")
            raise

    def logout(self):
        """Logout from M2M API"""
        if self.send_request("logout", None, self.api_key) is None:
            print("\nLogged out successfully")
            return True
        print("\nLogout failed")
        return False

    def process_coordinates(self, coordinates_df, batch_size=10):
        """Process coordinates in parallel batches"""
        try:
            # Split coordinates into batches
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
                            row['longitude']
                        )
                        futures.append(future)
                    
                    # Wait for batch to complete
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            print(f"Batch processing error: {str(e)}")
                            continue

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

    def _process_single_coordinate(self, lat, lon):
        """Process a single coordinate and download only the best scene"""
        try:
            print(f"Processing lat: {lat}, lon: {lon}")
            
            # Search for scenes
            date_range = {'start': '2024-06-01', 'end': '2024-07-31'}
            scenes = self.search_scenes(lat, lon, date_range)
            
            if not scenes:
                print(f"No scenes found for {lat}, {lon}")
                return

            # Select best scene
            best_scene = self.select_best_scene(scenes)
            if not best_scene:
                print(f"No suitable scenes found for {lat}, {lon}")
                return

            print(f"Selected best scene: {best_scene['displayId']} (Cloud cover: {best_scene['cloudCover']}%)")
            
            # Get download options for best scene
            download_options = self.get_download_options([best_scene['entityId']])
            
            if not download_options:
                print(f"No download options available for {best_scene['displayId']}")
                return

            # Request download for the bundle
            products = [{
                'entityId': option['entityId'],
                'productId': option['id']
            } for option in download_options]

            if products:
                # Request download
                download_results = self.request_download(products)
                
                if download_results and 'availableDownloads' in download_results:
                    self.download_files(download_results['availableDownloads'])
                else:
                    print(f"Failed to get download URLs for {best_scene['displayId']}")
            else:
                print(f"No valid products found for {best_scene['displayId']}")

        except Exception as e:
            print(f"Error processing coordinate {lat}, {lon}: {str(e)}")
            raise

def main():
    fetcher = LandsatFetcher()
    
    # Replace with your ERS credentials
    username = "ArjunGupta"
    token = "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"
    
    if not fetcher.login(username, token):
        return
    
    try:
        # Read coordinates from CSV
        coords_df = pd.read_csv("north_american_forests.csv")
        fetcher.process_coordinates(coords_df)
    finally:
        fetcher.logout()

if __name__ == "__main__":
    main()
