# Dagster resources (e.g., S3 client)

import logging
from dagster import resource, InitResourceContext
from src.config import Config
import boto3
from pathlib import Path

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

@resource
def config_resource(init_context: InitResourceContext) -> Config:
    """Dagster resource for loading configuration."""
    config_path = init_context.resource_config.get("config_path", "config/config.yaml")
    logger.info(f"Loading configuration from {config_path}")
    init_context.log.info(f"Loading configuration from {config_path}")
    try:
        config = Config(config_path)
        logger.info("Configuration loaded successfully")
        init_context.log.info("Configuration loaded successfully")
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        init_context.log.error(f"Failed to load configuration: {e}")
        raise

@resource
def s3_client_resource(init_context: InitResourceContext) -> boto3.client:
    """Dagster resource for S3 client."""
    logger.info("Initializing S3 client")
    init_context.log.info("Initializing S3 client")
    try:
        s3_client = boto3.client("s3")
        logger.info("S3 client initialized successfully")
        init_context.log.info("S3 client initialized successfully")
        return s3_client
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        init_context.log.error(f"Failed to initialize S3 client: {e}")
        raise