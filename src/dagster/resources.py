# Dagster resources (e.g., S3 client)

from dagster import resource, InitResourceContext
from src.config import Config
import boto3

@resource
def config_resource(init_context: InitResourceContext) -> Config:
    """Dagster resource for loading configuration."""
    config_path = init_context.resource_config.get("config_path", "config/config.yaml")
    return Config(config_path)

@resource
def s3_client_resource(_: InitResourceContext) -> boto3.client:
    """Dagster resource for S3 client."""
    return boto3.client("s3")