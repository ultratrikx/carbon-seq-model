from data_manager import DataManager
from landsat_data_fetch import LandsatFetcher
from soilgrids_data_fetch import SoilGridsFetcher
from concurrent.futures import ThreadPoolExecutor, as_completed

def main():
    # Initialize data manager
    data_manager = DataManager("north_american_forests.csv")
    
    # Replace with your ERS credentials
    username = "ArjunGupta"
    token = "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"
    
    # Create instances of fetchers
    landsat = LandsatFetcher(data_manager)
    soilgrids = SoilGridsFetcher(data_manager)
    
    if not landsat.login(username, token):
        return
    
    try:
        # Define a function to process a single location
        def process_location(row):
            location_id = row['location_id']
            lat = row['latitude']
            lon = row['longitude']
            
            # Fetch Landsat data
            landsat._process_single_coordinate(lat, lon, location_id)
            
            # Fetch SoilGrids data
            soilgrids.get_location_data(lat, lon, location_id)
        
        # Process all locations concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_location, row) for _, row in data_manager.data.iterrows()]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing location: {str(e)}")
        
        # Get locations with complete data
        complete_data = data_manager.get_collocated_data()
        print(f"Successfully processed {len(complete_data)} locations with both datasets")
    
    finally:
        landsat.logout()

if __name__ == "__main__":
    main()
