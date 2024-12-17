import os
from data_manager import DataManager
from landsat_data_fetch import LandsatFetcher
from soilgrids_data_fetch import SoilGridsFetcher
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import setup_logging, CheckpointManager
import datetime
import time
import pandas as pd

class CredentialManager:
    def __init__(self, credentials, logger):
        self.credentials = credentials
        self.current_idx = 0
        self.logger = logger
        self.failed_attempts = {}  # Track failed login attempts
        
    def get_next_credential(self):
        """Get next available credential with retry backoff"""
        attempts = 0
        while attempts < len(self.credentials):
            cred = self.credentials[self.current_idx]
            self.current_idx = (self.current_idx + 1) % len(self.credentials)
            
            # Check if credential is in cooldown
            last_failed = self.failed_attempts.get(cred['username'], 0)
            if time.time() - last_failed > 3600:  # 1 hour cooldown
                return cred
                
            attempts += 1
            
        return None
        
    def mark_failed(self, username):
        """Mark credential as failed"""
        self.failed_attempts[username] = time.time()
        self.logger.warning(f"Credential {username} marked as failed, will retry after 1 hour")
        
    def mark_successful(self, username):
        """Mark credential as successful"""
        if username in self.failed_attempts:
            del self.failed_attempts[username]

def main():
    # Setup logging and checkpointing
    logger = setup_logging()
    checkpoint_mgr = CheckpointManager()
    
    # Primary credentials
    primary_credentials = [
        {
            'username': "ArjunGupta",
            'token': "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"
        },
    ]
    
    # Fallback credentials
    fallback_credentials = [
        {
            'username': "rohanth",
            'token': "dLk8G4hnAsCQI0c!813bswEyxVcE_PxZciCgOerfY90U!H_9j2_KgFhpqx7pQZyf"
        },
    ]
    
    # Initialize credential managers
    primary_creds = CredentialManager(primary_credentials, logger)
    fallback_creds = CredentialManager(fallback_credentials, logger)
    
    try:
        # Initialize data manager
        data_manager = DataManager("csv\\north_american_forests.csv")
        
        # Load checkpoint if exists
        checkpoint = checkpoint_mgr.load_latest("fetch_progress")
        if checkpoint:
            logger.info(f"Resuming from checkpoint at {checkpoint['timestamp']}")
            data_manager.data = pd.DataFrame(checkpoint['data'])
        
        while True:
            try:
                # Try primary credentials first
                cred = primary_creds.get_next_credential()
                if not cred:
                    # If all primary credentials are in cooldown, try fallback
                    logger.warning("All primary credentials in cooldown, trying fallback")
                    cred = fallback_creds.get_next_credential()
                    
                if not cred:
                    logger.error("No available credentials, waiting 15 minutes")
                    time.sleep(900)  # Wait 15 minutes
                    continue
                
                # Create fetcher instances
                landsat = LandsatFetcher(data_manager)
                if not landsat.login(cred['username'], cred['token']):
                    logger.error(f"Failed to login with {cred['username']}")
                    primary_creds.mark_failed(cred['username'])
                    continue
                    
                # Mark credential as successful
                primary_creds.mark_successful(cred['username'])
                logger.info(f"Successfully logged in with {cred['username']}")
                
                soilgrids = SoilGridsFetcher(data_manager)
                
                # Process data
                process_data(data_manager, landsat, soilgrids, logger, checkpoint_mgr)
                
                # Check if all data is processed
                complete_data = data_manager.get_collocated_data()
                if len(complete_data) == len(data_manager.data):
                    logger.info("All locations processed successfully")
                    break
                    
                # Rotate credentials after 4 hours of use
                logger.info("Rotating credentials after 4 hours")
                time.sleep(4 * 60 * 60)
                
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                time.sleep(300)
                
            finally:
                if 'landsat' in locals():
                    landsat.logout()
                    
    except Exception as e:
        logger.error(f"Critical error: {str(e)}")
        
def process_data(data_manager, landsat, soilgrids, logger, checkpoint_mgr):
    """Process data with checkpointing and progress tracking"""
    try:
        # Process SoilGrids first
        unprocessed_soilgrids = data_manager.data[data_manager.data['soilgrids_id'].isna()]
        for idx, row in unprocessed_soilgrids.iterrows():
            try:
                soilgrids.get_location_data(
                    row['latitude'],
                    row['longitude'],
                    row['location_id']
                )
                # Save checkpoint every 5 locations
                if idx % 5 == 0:
                    checkpoint_mgr.save(data_manager.data.to_dict(), "fetch_progress")
                    logger.info(f"Checkpoint saved at location {idx}")
            except Exception as e:
                logger.error(f"Error with SoilGrids at {row['location_id']}: {str(e)}")

        # Process Landsat data
        unprocessed_landsat = data_manager.data[
            data_manager.data['soilgrids_id'].notna() & 
            data_manager.data['landsat_scene_id'].isna()
        ]
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for idx, row in unprocessed_landsat.iterrows():
                future = executor.submit(
                    landsat._process_single_coordinate,
                    row['latitude'],
                    row['longitude'],
                    row['location_id']
                )
                futures.append((future, idx))

            for future, idx in as_completed(futures):
                try:
                    future.result()
                    if idx % 5 == 0:
                        checkpoint_mgr.save(data_manager.data.to_dict(), "fetch_progress")
                        logger.info(f"Checkpoint saved at Landsat location {idx}")
                except Exception as e:
                    logger.error(f"Error with Landsat at index {idx}: {str(e)}")

    except Exception as e:
        logger.error(f"Error in process_data: {str(e)}")
        checkpoint_mgr.save(data_manager.data.to_dict(), "fetch_progress")

if __name__ == "__main__":
    main()
