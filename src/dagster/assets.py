# Dagster assets (ETL steps)

from dagster import asset, AssetExecutionContext
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.config import Config
import pandas as pd
from typing import List, Dict

@asset
def raw_crypto_data(context: AssetExecutionContext, config: Config) -> List[Dict]:
    """Extract raw cryptocurrency data from CoinGecko API."""
    context.log.info("Extracting data from CoinGecko API")
    extractor = DataExtractor(config.get_api_config())
    return extractor.extract()

@asset
def transformed_crypto_data(context: AssetExecutionContext, config: Config, raw_crypto_data: List[Dict]) -> pd.DataFrame:
    """Transform raw data (clean, filter, aggregate)."""
    context.log.info("Transforming raw data")
    transformer = DataTransformer(config.get_transform_config())
    return transformer.transform(raw_crypto_data)

@asset
def s3_crypto_data(context: AssetExecutionContext, config: Config, s3_client: boto3.client, transformed_crypto_data: pd.DataFrame):
    """Load transformed data to S3 as Parquet."""
    context.log.info("Loading data to S3")
    loader = DataLoader(config.get_s3_config())
    loader.s3_client = s3_client  # Inject Dagster's S3 client
    loader.load(transformed_crypto_data)
    context.add_output_metadata({
        "bucket": config.get_s3_config()["bucket_name"],
        "prefix": config.get_s3_config()["prefix"],
        "num_rows": len(transformed_crypto_data)
    })