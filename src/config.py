import yaml
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Config:
    """Loads and validates configuration from a YAML file."""
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> dict:
        """Load YAML configuration file."""
        try:
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
                logger.info(f"Configuration loaded from {self.config_path}")
                return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _validate_config(self):
        """Validate required configuration fields."""
        required_keys = ['api', 'transform', 's3']
        for key in required_keys:
            if key not in self.config:
                logger.error(f"Missing required configuration section: {key}")
                raise ValueError(f"Missing required configuration section: {key}")
        
        # Validate API section
        if not all(k in self.config['api'] for k in ['base_url', 'endpoint']):
            logger.error("API configuration missing required fields")
            raise ValueError("API configuration missing required fields")
        
        # Validate S3 section
        if 'bucket_name' not in self.config['s3']:
            logger.error("S3 configuration missing bucket_name")
            raise ValueError("S3 configuration missing bucket_name")
    
    def get_api_config(self) -> dict:
        """Return API configuration."""
        return self.config['api']
    
    def get_transform_config(self) -> dict:
        """Return transform configuration."""
        return self.config['transform']
    
    def get_s3_config(self) -> dict:
        """Return S3 configuration."""
        return self.config['s3']