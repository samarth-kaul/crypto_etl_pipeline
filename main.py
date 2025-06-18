# import yaml
# import logging
# from datetime import datetime
# from src.extract import extract_data
# from src.transform import transform_data
# from src.load import load_to_s3
# from src.utils import setup_logging, load_config

# def main():
#     # Load configuration
#     config = load_config("config/config.yaml")
    
#     # Setup logging
#     setup_logging(config["logging"]["file"], config["logging"]["level"])
    
#     logging.info("Starting Crypto Market Data Pipeline")
    
#     try:
#         # Step 1: Extract data from CoinGecko API
#         logging.info("Extracting data from CoinGecko API")
#         raw_data = extract_data(config["api"])
        
#         # Step 2: Transform data
#         logging.info("Transforming data")
#         transformed_data = transform_data(raw_data, config["transform"])
        
#         # Step 3: Load to S3
#         logging.info("Loading data to S3")
#         s3_key = f"{config['s3']['prefix']}crypto_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
#         load_to_s3(transformed_data, config["s3"]["bucket_name"], s3_key, config["s3"]["region"])
        
#         logging.info("Pipeline completed successfully")
        
#     except Exception as e:
#         logging.error(f"Pipeline failed: {str(e)}")
#         raise

# if __name__ == "__main__":
#     main()


import logging
from src.pipeline import Pipeline
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Run the data pipeline."""
    load_dotenv()  # Load AWS credentials from .env
    config_path = 'config/config.yaml'
    pipeline = Pipeline(config_path)
    pipeline.run()

if __name__ == '__main__':
    main()