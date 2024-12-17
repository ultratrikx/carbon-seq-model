import os
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

def main():
    html_handler = setup_logging()
    logging.info("Starting data fetch process")
    
    start_time = time.time()
    data_manager = DataManager("csv\\north_american_forests.csv")
    
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

if __name__ == "__main__":
    main()
