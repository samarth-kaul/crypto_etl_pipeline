# Dagster jobs (pipeline definition)

import logging
from dagster import define_asset_job, AssetSelection

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

crypto_pipeline_job = define_asset_job(
    name="crypto_pipeline_job",
    selection=AssetSelection.all(),
    description="ETL pipeline for cryptocurrency data"
)