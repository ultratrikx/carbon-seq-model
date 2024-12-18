import os
import time
import logging
from data_manager import DataManager
from landsat_data_fetch import LandsatFetcher
from soilgrids_data_fetch import SoilGridsFetcher
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import datetime
from logging.handlers import RotatingFileHandler
import sys
import time
import html

class HTMLFileHandler(logging.Handler):
    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        try:
            with open(self.filename, 'w') as f:
                f.write('<html><head><title>Data Fetch Log</title>')
                f.write('<style>body{font-family:monospace;background:#f0f0f0;padding:20px}')
                f.write('.error{color:red}.warning{color:orange}.info{color:blue}.debug{color:gray}</style>')
                f.write('</head><body><pre>\n')
        except Exception as e:
            print(f"Error creating HTML log file: {str(e)}")
            raise

    def emit(self, record):
        try:
            with open(self.filename, 'a', encoding='utf-8') as f:
                if record.levelname == 'ERROR':
                    css_class = 'error'
                elif record.levelname == 'WARNING':
                    css_class = 'warning'
                elif record.levelname == 'INFO':
                    css_class = 'info'
                else:
                    css_class = 'debug'
                    
                timestamp = datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
                msg = html.escape(self.format(record))
                f.write(f'<span class="{css_class}">[{timestamp}] {msg}</span>\n')
        except Exception as e:
            print(f"Error writing to HTML log: {str(e)}")
            
    def close(self):
        try:
            with open(self.filename, 'a') as f:
                f.write('</pre></body></html>')
        except Exception as e:
            print(f"Error closing HTML log: {str(e)}")
        finally:
            super().close()

def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    html_handler = HTMLFileHandler(f"{log_dir}/fetch_log_{timestamp}.html")
    html_handler.setLevel(logging.INFO)
    
    file_handler = RotatingFileHandler(
        f"{log_dir}/fetch_log_{timestamp}.txt",
        maxBytes=10*1024*1024,
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[html_handler, file_handler, console_handler]
    )
    
    return html_handler

<<<<<<< HEAD
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
=======
def main():
    html_handler = setup_logging()
    logging.info("Starting data fetch process")
    
    start_time = time.time()
    data_manager = DataManager("csv\\data.csv")
    
    # Load checkpoint and determine current stage
    checkpoint = data_manager.load_checkpoint()
    current_stage = 'soilgrids'  # Default starting stage
    
    if checkpoint:
        logging.info(f"Resuming from checkpoint: {checkpoint['stage']}")
        batch_start = checkpoint['batch_index']
        processed_count = checkpoint['processed_count']
        error_count = checkpoint['error_count']
        
        # Determine current stage from checkpoint
        if 'stage' in checkpoint:
            if checkpoint['stage'].startswith('landsat'):
                current_stage = 'landsat'
                logging.info("Resuming Landsat processing")
            elif checkpoint['stage'] == 'soilgrids':
                logging.info("Resuming SoilGrids processing")
    else:
        batch_start = 0
        processed_count = 0
        error_count = 0
    
    total_locations = len(data_manager.data)
    
    try:
        username = "ArjunGupta"
        token = "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"
        
        landsat = LandsatFetcher(data_manager)
        soilgrids = SoilGridsFetcher(data_manager)
        
        if not landsat.login(username, token):
            logging.error("Failed to login to Landsat service")
            return
        
        logging.info(f"Processing {total_locations} locations")
        
        batch_size = 10
        for i in range(batch_start, total_locations, batch_size):
            batch = data_manager.data.iloc[i:i+batch_size]
            current_batch_processed = 0
            
            logging.info(f"Processing batch {i//batch_size + 1} of {(total_locations+batch_size-1)//batch_size}")
            
            # Only process SoilGrids if we haven't moved to Landsat stage
            if current_stage == 'soilgrids':
                for _, row in batch.iterrows():
                    try:
                        if not row['soilgrids_id']:
                            soilgrids.get_location_data(
                                row['latitude'],
                                row['longitude'],
                                row['location_id']
                            )
                            processed_count += 1
                            current_batch_processed += 1
                            data_manager.save_checkpoint(
                                i + current_batch_processed, 
                                processed_count, 
                                error_count,
                                'soilgrids'
                            )
                    except Exception as e:
                        error_count += 1
                        logging.error(f"SoilGrids error at {row['location_id']}: {str(e)}")
                        continue
            
            # Process Landsat regardless of stage
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for _, row in batch.iterrows():
                    if row['soilgrids_id'] and not row['landsat_scene_id']:
                        # Check existing progress
                        location_checkpoints = [c for c in data_manager.list_checkpoints() 
                                             if c.get('stage', '').endswith(row['location_id'])]
                        
                        # Skip if this location is fully processed
                        if any(c['stage'] == f'landsat_complete_{row["location_id"]}' 
                              for c in location_checkpoints):
                            logging.info(f"Skipping completed location {row['location_id']}")
                            continue
                        
                        # Submit for processing
                        future = executor.submit(
                            landsat._process_single_coordinate,
                            row['latitude'],
                            row['longitude'],
                            row['location_id']
                        )
                        futures.append((future, row['location_id']))
                        current_stage = 'landsat'  # Mark that we've moved to Landsat stage
                
                for future, loc_id in futures:
                    try:
                        result = future.result()
                        if result:  # If Landsat processing was successful
                            processed_count += 1
                            current_batch_processed += 1
                            # Save checkpoint after complete Landsat processing
                            data_manager.save_checkpoint(
                                i + current_batch_processed, 
                                processed_count, 
                                error_count,
                                f'landsat_complete_{loc_id}'
                            )
                    except Exception as e:
                        error_count += 1
                        logging.error(f"Landsat error at {loc_id}: {str(e)}")

            # Save batch checkpoint
            data_manager.save_checkpoint(
                i + batch_size, 
                processed_count, 
                error_count,
                'batch_complete'
            )
            
            elapsed_time = time.time() - start_time
            avg_time_per_location = elapsed_time / (i + len(batch))
            remaining_locations = total_locations - (i + len(batch))
            estimated_time_remaining = remaining_locations * avg_time_per_location
            
            logging.info(f"Progress: {i + len(batch)}/{total_locations} locations")
            logging.info(f"Estimated time remaining: {datetime.timedelta(seconds=int(estimated_time_remaining))}")
            logging.info(f"Success rate: {processed_count}/{i + len(batch)} ({error_count} errors)")
            
            data_manager._save_data()
            time.sleep(2)
        
        # Clear checkpoint after successful completion
        data_manager.clear_checkpoint()
        
        complete_data = data_manager.get_collocated_data()
        logging.info(f"Final results: {len(complete_data)} locations with complete data")
        logging.info(f"Total processing time: {datetime.timedelta(seconds=int(time.time() - start_time))}")
        
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user")
        logging.info("You can resume from the last checkpoint by running the script again")
    except Exception as e:
        logging.error(f"Critical error: {str(e)}", exc_info=True)
        logging.info("You can resume from the last checkpoint by running the script again")
    finally:
        try:
            landsat.logout()
        except:
            pass
        if html_handler:
            html_handler.close()
        for handler in logging.getLogger().handlers:
            handler.close()
        logging.info("Process completed")
>>>>>>> 1c54ca0022e8f864771e022c50c4d37cffdfa670

if __name__ == "__main__":
    main()
