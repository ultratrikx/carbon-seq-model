import logging
import os
import json
import datetime
import html
from logging.handlers import RotatingFileHandler
from pathlib import Path

class HTMLFormatter(logging.Formatter):
    """Custom formatter for HTML output with proper timestamp and styling"""
    def __init__(self):
        super().__init__(
            fmt='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.html_header = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Data Fetching Log</title>
            <style>
                body { font-family: monospace; padding: 20px; background: #f5f5f5; }
                .error { color: #dc3545; background: #ffe6e6; padding: 5px; margin: 2px 0; }
                .warning { color: #ffc107; background: #fff3cd; padding: 5px; margin: 2px 0; }
                .info { color: #0c5460; background: #d1ecf1; padding: 5px; margin: 2px 0; }
                .debug { color: #666; background: #f8f9fa; padding: 5px; margin: 2px 0; }
                .success { color: #28a745; background: #d4edda; padding: 5px; margin: 2px 0; }
                .timestamp { color: #666; font-size: 0.9em; }
                .progress { margin: 10px 0; }
            </style>
        </head>
        <body>
        <h2>Data Fetching Log</h2>
        <div class="progress"></div>
        """
        self.html_footer = "</body></html>"

    def format(self, record):
        formatted = super().format(record)
        if record.levelname == 'ERROR':
            class_name = 'error'
        elif record.levelname == 'WARNING':
            class_name = 'warning'
        elif record.levelname == 'INFO':
            class_name = 'info'
        elif record.levelname == 'DEBUG':
            class_name = 'debug'
        else:
            class_name = 'success'
            
        return f'<div class="{class_name}">{html.escape(formatted)}</div>'

class CheckpointManager:
    def __init__(self, checkpoint_dir="checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
    def save(self, data, name):
        """Save checkpoint with timestamp"""
        checkpoint = {
            'data': data,
            'timestamp': datetime.datetime.now().isoformat(),
            'name': name
        }
        filepath = self.checkpoint_dir / f"{name}_{datetime.datetime.now():%Y%m%d_%H%M%S}.json"
        with open(filepath, 'w') as f:
            json.dump(checkpoint, f)
        
        # Keep only last 5 checkpoints
        checkpoints = sorted(self.checkpoint_dir.glob(f"{name}_*.json"))
        for old in checkpoints[:-5]:
            old.unlink()
            
    def load_latest(self, name):
        """Load most recent checkpoint for given name"""
        checkpoints = sorted(self.checkpoint_dir.glob(f"{name}_*.json"))
        if not checkpoints:
            return None
        with open(checkpoints[-1]) as f:
            return json.load(f)

def setup_logging(log_dir="logs"):
    """Setup logging with HTML output and console"""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # HTML log handler
    html_handler = RotatingFileHandler(
        f"{log_dir}/fetch_log_{timestamp}.html",
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    html_handler.setFormatter(HTMLFormatter())
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )
    
    # Configure logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(html_handler)
    logger.addHandler(console_handler)
    
    return logger
