import logging
from .config import Config
from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Pipeline:
    """Orchestrates the ETL pipeline."""
    
    def __init__(self, config_path: str):
        self.config = Config(config_path)
    
    def run(self):
        """Execute the ETL pipeline."""
        try:
            logger.info("Starting ETL pipeline")
            
            # Extract
            extractor = DataExtractor(self.config.get_api_config())
            raw_data = extractor.extract()
            
            # Transform
            transformer = DataTransformer(self.config.get_transform_config())
            transformed_data = transformer.transform(raw_data)
            
            # Load
            loader = DataLoader(self.config.get_s3_config())
            loader.load(transformed_data)
            
            logger.info("ETL pipeline completed successfully")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise