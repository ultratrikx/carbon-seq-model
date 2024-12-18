import pandas as pd
from pathlib import Path
import uuid
import os
import logging
from datetime import datetime

class DataManager:
    def __init__(self, csv_file, output_dir="processed_data"):
        self.base_dir = Path(output_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # Load and enhance the coordinate data
        self.data = pd.read_csv(csv_file)
        self.data['location_id'] = [str(uuid.uuid4())[:8] for _ in range(len(self.data))]
        self.data['landsat_scene_id'] = None
        self.data['soilgrids_id'] = None
        
        # Save enhanced CSV
        self.master_csv = self.base_dir / "master_locations.csv"
        self.data.to_csv(self.master_csv, index=False)
        
        # Create organized directory structure
        self.landsat_dir = self.base_dir / "landsat_data"
        self.soilgrids_dir = self.base_dir / "soilgrids_data"
        self.landsat_dir.mkdir(exist_ok=True)
        self.soilgrids_dir.mkdir(exist_ok=True)
        
        # Setup logging
        self.log_dir = self.base_dir / "logs"
        self.log_dir.mkdir(exist_ok=True)
        self.setup_logging()
        
        self.logger.info(f"DataManager initialized with {len(self.data)} locations")

    def setup_logging(self):
        """Setup logging configuration"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"processing_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('DataManager')

    def update_landsat_scene(self, location_id, scene_id):
        """Update Landsat scene ID for a location"""
        idx = self.data['location_id'] == location_id
        self.data.loc[idx, 'landsat_scene_id'] = scene_id
        self._save_data()

    def update_soilgrids_id(self, location_id, soilgrids_id):
        """Update SoilGrids ID for a location"""
        idx = self.data['location_id'] == location_id
        self.data.loc[idx, 'soilgrids_id'] = soilgrids_id
        self._save_data()

    def _save_data(self):
        """Save current state to CSV"""
        self.data.to_csv(self.master_csv, index=False)

    def get_data_paths(self, location_id):
        """Get paths to all data for a location"""
        location = self.data[self.data['location_id'] == location_id].iloc[0]
        return {
            'landsat_path': self.landsat_dir / f"scene_{location['landsat_scene_id']}" if location['landsat_scene_id'] else None,
            'soilgrids_path': self.soilgrids_dir / f"location_{location['soilgrids_id']}" if location['soilgrids_id'] else None,
            'lat': location['latitude'],
            'lon': location['longitude']
        }

    def get_collocated_data(self):
        """Get locations with both Landsat and SoilGrids data"""
        return self.data[self.data['landsat_scene_id'].notna() & 
                        self.data['soilgrids_id'].notna()]

    def get_next_batch(self, batch_size=10, processed_ids=None):
        """Get next batch of unprocessed locations"""
        if processed_ids is None:
            processed_ids = set()
            
        mask = ~self.data['location_id'].isin(processed_ids)
        return self.data[mask].head(batch_size)

    def save_progress(self, processed_ids):
        """Save processing progress"""
        progress_file = self.base_dir / "processing_progress.csv"
        pd.Series(list(processed_ids)).to_csv(progress_file, index=False)
        
    def load_progress(self):
        """Load processing progress"""
        progress_file = self.base_dir / "processing_progress.csv"
        if progress_file.exists():
            return set(pd.read_csv(progress_file)['0'].values)
        return set()
