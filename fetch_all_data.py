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
    processed_count = 0
    error_count = 0
    
    try:
        data_manager = DataManager("csv\\north_american_forests.csv")
        total_locations = len(data_manager.data)
        
        username = "ArjunGupta"
        token = "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"
        
        landsat = LandsatFetcher(data_manager)
        soilgrids = SoilGridsFetcher(data_manager)
        
        if not landsat.login(username, token):
            logging.error("Failed to login to Landsat service")
            return
        
        logging.info(f"Processing {total_locations} locations")
        
        batch_size = 10
        for i in range(0, total_locations, batch_size):
            batch = data_manager.data.iloc[i:i+batch_size]
            
            logging.info(f"Processing batch {i//batch_size + 1} of {(total_locations+batch_size-1)//batch_size}")
            
            for _, row in batch.iterrows():
                try:
                    if not row['soilgrids_id']:
                        soilgrids.get_location_data(
                            row['latitude'],
                            row['longitude'],
                            row['location_id']
                        )
                        processed_count += 1
                except Exception as e:
                    error_count += 1
                    logging.error(f"SoilGrids error at {row['location_id']}: {str(e)}")
                    continue
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for _, row in batch.iterrows():
                    if row['soilgrids_id'] and not row['landsat_scene_id']:
                        future = executor.submit(
                            landsat._process_single_coordinate,
                            row['latitude'],
                            row['longitude'],
                            row['location_id']
                        )
                        futures.append((future, row['location_id']))
                
                for future, loc_id in futures:
                    try:
                        future.result()
                        processed_count += 1
                    except Exception as e:
                        error_count += 1
                        logging.error(f"Landsat error at {loc_id}: {str(e)}")
            
            elapsed_time = time.time() - start_time
            avg_time_per_location = elapsed_time / (i + len(batch))
            remaining_locations = total_locations - (i + len(batch))
            estimated_time_remaining = remaining_locations * avg_time_per_location
            
            logging.info(f"Progress: {i + len(batch)}/{total_locations} locations")
            logging.info(f"Estimated time remaining: {datetime.timedelta(seconds=int(estimated_time_remaining))}")
            logging.info(f"Success rate: {processed_count}/{i + len(batch)} ({error_count} errors)")
            
            data_manager._save_data()
            time.sleep(2)
        
        complete_data = data_manager.get_collocated_data()
        logging.info(f"Final results: {len(complete_data)} locations with complete data")
        logging.info(f"Total processing time: {datetime.timedelta(seconds=int(time.time() - start_time))}")
        
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user")
    except Exception as e:
        logging.error(f"Critical error: {str(e)}", exc_info=True)
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
