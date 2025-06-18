import yaml
import logging
from logging.handlers import RotatingFileHandler

def load_config(config_path):
    """Load configuration from YAML file."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def setup_logging(log_file, log_level):
    """Set up logging with rotation."""
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level))
    
    # Create rotating file handler
    handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)