# Dagster schedules

import logging
from dagster import ScheduleDefinition
from .jobs import crypto_pipeline_job

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

logger.info("Defining daily crypto pipeline schedule")
daily_crypto_pipeline_schedule = ScheduleDefinition(
    job=crypto_pipeline_job,
    schedule_name="daily_crypto_pipeline",
    cron_schedule="0 0 * * *",  # Run daily at midnight UTC
    description="Daily run of the crypto data pipeline"
)
logger.info("Daily crypto pipeline schedule defined")