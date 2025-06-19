# Dagster assets (ETL steps)

import logging
from dagster import asset, AssetExecutionContext
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.config import Config
import pandas as pd
from typing import List, Dict

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@asset
def raw_crypto_data(context: AssetExecutionContext, config: Config) -> List[Dict]:
    """Extract raw cryptocurrency data from CoinGecko API."""
    context.log.info("Starting data extraction from CoinGecko API")
    logger.info("Starting data extraction from CoinGecko API")
    try:
        extractor = DataExtractor(config.get_api_config())
        data = extractor.extract()
        context.log.info(f"Extracted {len(data)} records from CoinGecko API")
        logger.info(f"Extracted {len(data)} records from CoinGecko API")
        return data
    except Exception as e:
        context.log.error(f"Data extraction failed: {e}")
        logger.error(f"Data extraction failed: {e}")
        raise

@asset
def transformed_crypto_data(context: AssetExecutionContext, config: Config, raw_crypto_data: List[Dict]) -> pd.DataFrame:
    """Transform raw data (clean, filter, aggregate)."""
    context.log.info(f"Starting transformation of {len(raw_crypto_data)} records")
    logger.info(f"Starting transformation of {len(raw_crypto_data)} records")
    try:
        transformer = DataTransformer(config.get_transform_config())
        df = transformer.transform(raw_crypto_data)
        context.log.info(f"Transformed data to {len(df)} rows")
        logger.info(f"Transformed data to {len(df)} rows")
        return df
    except Exception as e:
        context.log.error(f"Transformation failed: {e}")
        logger.error(f"Transformation failed: {e}")
        raise

@asset
def s3_crypto_data(context: AssetExecutionContext, config: Config, s3_client: boto3.client, transformed_crypto_data: pd.DataFrame):
    """Load transformed data to S3 as Parquet."""
    context.log.info(f"Starting load of {len(transformed_crypto_data)} rows to S3")
    logger.info(f"Starting load of {len(transformed_crypto_data)} rows to S3")
    try:
        loader = DataLoader(config.get_s3_config())
        loader.s3_client = s3_client  # Inject Dagster's S3 client
        loader.load(transformed_crypto_data)
        bucket = config.get_s3_config()["bucket_name"]
        prefix = config.get_s3_config()["prefix"]
        context.log.info(f"Loaded data to s3://{bucket}/{prefix}")
        logger.info(f"Loaded data to s3://{bucket}/{prefix}")
        context.add_output_metadata({
            "bucket": bucket,
            "prefix": prefix,
            "num_rows": len(transformed_crypto_data)
        })
    except Exception as e:
        context.log.error(f"Failed to load data to S3: {e}")
        logger.error(f"Failed to load data to S3: {e}")
        raise