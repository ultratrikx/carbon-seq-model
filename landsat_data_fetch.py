import pandas as pd
import requests
import os
from tqdm import tqdm

# Constants
M2M_API_URL = "https://m2m.cr.usgs.gov/api/api/json/stable"  # Base URL for USGS M2M API
LOGIN_ENDPOINT = f"{M2M_API_URL}/login"
SEARCH_ENDPOINT = f"{M2M_API_URL}/scene-search"
DOWNLOAD_REQUEST_ENDPOINT = f"{M2M_API_URL}/download-request"  # Changed endpoint
DOWNLOAD_OPTIONS_ENDPOINT = f"{M2M_API_URL}/download-options"

# USERNAME = "rohanth"  # Replace with your Earthdata username
# PASSWORD = "fmv@azm-cvh4HKW-egm"  # Replace with your Earthdata password
USERNAME = "ArjunGupta"  # Replace with your Earthdata username
PASSWORD = "GJ86*gJKdv&i\"LM"  # Replace with your Earthdata password
# API_TOKEN = "wm0DyYKik@qdHwdYloSFZwsaJPITZ0aCPljss67jttT9@dVSsSxuDDxb_pJD9uga"
API_TOKEN = "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"

# Authenticate and get an API key
def get_api_key():
    payload = {
        "username": USERNAME,
        "password": PASSWORD
    }
    response = requests.post(LOGIN_ENDPOINT, json=payload)
    if response.status_code == 200:
        return response.json().get("data")
    else:
        print(f"Error during authentication: {response.status_code}, {response.text}")
        return None

def list_datasets(api_key):
    dataset_search_endpoint = f"{M2M_API_URL}/dataset-search"
    payload = {"datasetName": ""}
    headers = {"X-Auth-Token": api_key}
    response = requests.post(dataset_search_endpoint, json=payload, headers=headers)
    if response.status_code == 200:
        datasets = response.json().get("data", [])
        for dataset in datasets:
            collection_name = dataset.get("collectionName", "Unknown Collection")
            dataset_name = dataset.get("datasetAlias", "Unknown Dataset")
            print(f"{dataset_name} - {collection_name}")
            # Match the correct collection name
            if collection_name == "Landsat 8-9 OLI/TIRS C2 L2":
                print(f"Found matching dataset: {dataset_name} - {collection_name}")
                return dataset_name
        print("Desired collection not found in available datasets.")
    else:
        print(f"Error listing datasets: {response.status_code}, {response.text}")
    return None
# Search for Landsat scenes
def search_landsat_granules(api_key, latitude, longitude, dataset="landsat_ot_c2_l2"):
    payload = {
        "datasetName": dataset,
        "spatialFilter": {
            "filterType": "mbr",
            "lowerLeft": {"latitude": latitude - 0.05, "longitude": longitude - 0.05},
            "upperRight": {"latitude": latitude + 0.05, "longitude": longitude + 0.05}
        },
        "temporalFilter": {
            "startDate": "2024-06-01",
            "endDate": "2024-06-30"
        },
        "maxResults": 1,
        "startingNumber": 1,
        "sortOrder": "DESC",
        "metadataType": "full"
    }
    headers = {
        "X-Auth-Token": api_key,
        "Content-Type": "application/json"
    }
    response = requests.post(SEARCH_ENDPOINT, json=payload, headers=headers)
    if response.status_code == 200:
        try:
            scenes = response.json().get("data", {}).get("results", [])
            return scenes[0] if scenes else None
        except Exception as e:
            print(f"Error parsing JSON: {e}")
            print(f"Response text: {response.text}")
            return None
    else:
        print(f"Error during search: {response.status_code}, {response.text}")
        return None

# Retrieve product ID
def get_product_id(api_key, entity_id, dataset):
    payload = {
        "datasetName": dataset,
        "entityIds": [entity_id],
        "downloadApplication": "EE"  # Specify Earth Explorer as download application
    }
    headers = {
        "X-Auth-Token": api_key,
        "Content-Type": "application/json"
    }
    response = requests.post(DOWNLOAD_OPTIONS_ENDPOINT, json=payload, headers=headers)
    if response.status_code == 200:
        products = response.json().get("data", [])
        if products:
            # Select the first available product ID
            return products[0]["id"]
        else:
            print("No products available for this scene.")
            return None
    else:
        print(f"Error retrieving product ID: {response.status_code}, {response.text}")
        return None

# Modified download function
def download_landsat_image(api_key, entity_id, product_id, output_folder):
    payload = {
        "downloads": [{
            "entityId": entity_id,
            "productId": product_id
        }]
    }
    headers = {"X-Auth-Token": api_key}
    
    response = requests.post(DOWNLOAD_REQUEST_ENDPOINT, json=payload, headers=headers)
    if response.status_code == 200:
        available_downloads = response.json().get("data", {}).get("availableDownloads", [])
        if available_downloads:
            download_url = available_downloads[0].get("url")
            if download_url:
                # Create a simpler filename using just the entity_id
                file_name = os.path.join(output_folder, f"landsat_{entity_id}.tar.gz")
                print(f"Downloading: {download_url}")
                
                # Stream the download with progress bar
                file_response = requests.get(download_url, stream=True)
                if file_response.status_code == 200:
                    total_size = int(file_response.headers.get('content-length', 0))
                    
                    with open(file_name, "wb") as f:
                        pbar = tqdm(
                            total=total_size,
                            desc=f"Downloading {entity_id}",
                            unit='B',
                            unit_scale=True,
                            unit_divisor=1024,
                            dynamic_ncols=True
                        )
                        for data in file_response.iter_content(chunk_size=1024):
                            size = f.write(data)
                            pbar.update(size)
                        pbar.close()
                    print(f"Saved to {file_name}")
                else:
                    print(f"Failed to download file: {file_response.status_code}")
            else:
                print("No download URL found.")
        else:
            print("No available downloads found in response.")
    else:
        print(f"Error during download request: {response.status_code}, {response.text}")

def fetch_landsat_images(csv_file, output_folder):
    api_key = get_api_key()
    if not api_key:
        print("Failed to authenticate.")
        return

    # Use known dataset name directly
    dataset_name = "landsat_ot_c2_l2"
    print(f"Using dataset: {dataset_name}")

    # Read CSV file
    data = pd.read_csv(csv_file)

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for index, row in data.iterrows():
        latitude = row["latitude"]
        longitude = row["longitude"]

        print(f"Searching Landsat images for lat: {latitude}, lon: {longitude}")
        scene = search_landsat_granules(api_key, latitude, longitude, dataset_name)
        if scene:
            entity_id = scene["entityId"]
            product_id = get_product_id(api_key, entity_id, dataset_name)
            if product_id:
                print(f"Downloading scene with Entity ID: {entity_id} and Product ID: {product_id}")
                download_landsat_image(api_key, entity_id, product_id, output_folder)
            else:
                print(f"No valid product found for Entity ID: {entity_id}")
        else:
            print(f"No granules found for lat: {latitude}, lon: {longitude}")

# Example usage
csv_file = "north_american_forests.csv"  # Path to your CSV file with 'latitude' and 'longitude' columns
output_folder = "landsat_images"  # Folder to save downloaded images
fetch_landsat_images(csv_file, output_folder)
