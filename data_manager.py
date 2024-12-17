import pandas as pd
from pathlib import Path
import uuid
import os
import json
import datetime
import logging

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

    def save_checkpoint(self, batch_index, processed_count, error_count, stage='batch'):
        """Save checkpoint data with processing stage information"""
        checkpoint = {
            'batch_index': batch_index,
            'processed_count': processed_count,
            'error_count': error_count,
            'stage': stage,
            'timestamp': datetime.datetime.now().isoformat(),
            'processed_locations': self.data[self.data['soilgrids_id'].notna() | 
                                          self.data['landsat_scene_id'].notna()]
                                 ['location_id'].tolist()
        }
        
        # Save main checkpoint
        checkpoint_path = self.base_dir / 'checkpoint.json'
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint, f)
            
        # Save timestamped backup checkpoint
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = self.base_dir / f'checkpoint_{timestamp}.json'
        with open(backup_file, 'w') as f:
            json.dump(checkpoint, f)
            
        # Cleanup old checkpoints
        try:
            checkpoints = sorted(self.base_dir.glob('checkpoint_*.json'))
            if len(checkpoints) > 10:  # Keep only 10 most recent backups
                for old_checkpoint in checkpoints[:-10]:
                    try:
                        old_checkpoint.unlink()
                        logging.debug(f"Removed old checkpoint: {old_checkpoint}")
                    except Exception as e:
                        logging.warning(f"Failed to remove old checkpoint {old_checkpoint}: {e}")
        except Exception as e:
            logging.error(f"Error during checkpoint cleanup: {e}")

    def load_checkpoint(self):
        """Load checkpoint data with enhanced error handling"""
        try:
            # Try loading main checkpoint first
            with open(self.base_dir / 'checkpoint.json', 'r') as f:
                checkpoint = json.load(f)
                
            # Verify checkpoint data and restore processed locations
            if 'processed_locations' in checkpoint:
                for loc_id in checkpoint['processed_locations']:
                    if loc_id in self.data['location_id'].values:
                        # Restore location data from master CSV
                        self._save_data()
                        
            return checkpoint
            
        except FileNotFoundError:
            # Try loading latest backup checkpoint
            try:
                checkpoints = sorted(self.base_dir.glob('checkpoint_*.json'))
                if checkpoints:
                    with open(checkpoints[-1], 'r') as f:
                        return json.load(f)
            except Exception:
                pass
            return None
        except Exception as e:
            logging.error(f"Error loading checkpoint: {str(e)}")
            return None

    def clear_checkpoint(self):
        """Clear checkpoint file"""
        try:
            (self.base_dir / 'checkpoint.json').unlink()
        except FileNotFoundError:
            pass

    def list_checkpoints(self):
        """List all available checkpoints"""
        checkpoints = []
        try:
            # Try main checkpoint
            with open(self.base_dir / 'checkpoint.json', 'r') as f:
                checkpoints.append(json.load(f))
            
            # Get all backup checkpoints
            backup_files = sorted(self.base_dir.glob('checkpoint_*.json'))
            for bf in backup_files:
                with open(bf, 'r') as f:
                    checkpoints.append(json.load(f))
                    
        except Exception as e:
            logging.error(f"Error listing checkpoints: {e}")
        
        return checkpoints
