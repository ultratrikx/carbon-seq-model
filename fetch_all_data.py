import os
import time
import logging
from data_manager import DataManager
from landsat_data_fetch import LandsatFetcher
from soilgrids_data_fetch import SoilGridsFetcher
from concurrent.futures import ThreadPoolExecutor, as_completed

# Fallback credentials
CREDENTIALS = [
    {"username": "ArjunGupta", "token": "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"},
    {"username": "rohanth", "token": "twjgA07U5R0jobSd/91yaTVt+3pEsC0Bb1Wpy2Qvg2k="},
]

def process_batch(landsat, soilgrids, batch_df, logger):
    """Process a batch sequentially with enhanced error handling"""
    batch_results = set()
    successful_soilgrids = set()
    
    # Step 1: Download all SoilGrids data first
    logger.info("Starting SoilGrids downloads for batch...")
    for _, row in batch_df.iterrows():
        try:
            soilgrids.get_location_data(
                row['latitude'],
                row['longitude'],
                row['location_id']
            )
            successful_soilgrids.add(row['location_id'])
            logger.info(f"Successfully downloaded SoilGrids for location {row['location_id']}")
        except Exception as e:
            logger.error(f"SoilGrids error for location {row['location_id']}: {str(e)}")
            continue

    if not successful_soilgrids:
        logger.error("No successful SoilGrids downloads in batch")
        return batch_results

    # Step 2: Download Landsat data in groups of 5 with retries
    logger.info("Starting Landsat downloads...")
    successful_downloads = []
    subset_df = batch_df[batch_df['location_id'].isin(successful_soilgrids)]
    
    for i in range(0, len(subset_df), 5):
        chunk = subset_df.iloc[i:i+5]
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(
                    landsat._download_single_file_only,  # Updated method with retries
                    row['latitude'],
                    row['longitude'],
                    row['location_id']
                ): row['location_id']
                for _, row in chunk.iterrows()
            }
            
            for future in as_completed(futures):
                location_id = futures[future]
                try:
                    filepath = future.result()
                    if filepath:
                        successful_downloads.append((filepath, location_id))
                        logger.info(f"Successfully downloaded Landsat for location {location_id}")
                except Exception as e:
                    logger.error(f"Landsat download error for {location_id}: {str(e)}")
                    # Optionally, implement additional retry logic here

    # Step 3: Extract and process all downloaded files
    logger.info("Processing downloaded Landsat files...")
    for filepath, location_id in successful_downloads:
        try:
            scene_id = landsat.extract_and_resample_bands(filepath, location_id)
            if scene_id:
                batch_results.add(location_id)
                logger.info(f"Successfully processed Landsat data for location {location_id}")
        except Exception as e:
            logger.error(f"Processing error for {location_id}: {str(e)}")

    # Step 4: Cleanup any remaining temporary files
    landsat.cleanup_temp_files()
    
    return batch_results

def main():
    # Initialize data manager
    data_manager = DataManager("csv\\data.csv")
    logger = logging.getLogger('MainProcess')
    
    # Load previous progress
    processed_ids = data_manager.load_progress()
    logger.info(f"Loaded {len(processed_ids)} previously processed locations")
    
    current_cred_idx = 0
    landsat = None
    
    while True:
        # Get next batch
        batch = data_manager.get_next_batch(batch_size=10, processed_ids=processed_ids)
        if len(batch) == 0:
            logger.info("All locations processed")
            break
            
        # Initialize or refresh fetchers
        if landsat is None or not landsat.is_session_valid():
            if landsat:
                try:
                    landsat.logout()
                except:
                    pass
            
            # Try credentials until one works
            while current_cred_idx < len(CREDENTIALS):
                try:
                    creds = CREDENTIALS[current_cred_idx]
                    landsat = LandsatFetcher(data_manager)
                    if landsat.login(creds["username"], creds["token"]):
                        logger.info(f"Logged in with credentials set {current_cred_idx}")
                        break
                except Exception as e:
                    logger.error(f"Login failed with credentials set {current_cred_idx}: {str(e)}")
                    current_cred_idx += 1
            
            if current_cred_idx >= len(CREDENTIALS):
                logger.error("All credentials failed")
                break
                
        soilgrids = SoilGridsFetcher(data_manager)
        
        try:
            # Process batch
            logger.info(f"Processing batch of {len(batch)} locations")
            batch_results = process_batch(landsat, soilgrids, batch, logger)
            
            # Update progress
            processed_ids.update(batch_results)
            data_manager.save_progress(processed_ids)
            
            # Minimal cooldown between batches
            time.sleep(5)  # 5 seconds cooldown between batches
            
        except Exception as e:
            logger.error(f"Batch processing error: {str(e)}")
            time.sleep(60)  # 1 minute cooldown on error
            landsat = None  # Force session renewal
            continue
    
    if landsat:
        landsat.logout()
    
    # Final statistics
    complete_data = data_manager.get_collocated_data()
    logger.info(f"Processing complete. {len(complete_data)} locations have both datasets")

if __name__ == "__main__":
    main()
