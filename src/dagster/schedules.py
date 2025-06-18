# Dagster schedules

from dagster import ScheduleDefinition
from .jobs import crypto_pipeline_job

daily_crypto_pipeline_schedule = ScheduleDefinition(
    job=crypto_pipeline_job,
    schedule_name="daily_crypto_pipeline",
    cron_schedule="0 0 * * *",  # Run daily at midnight UTC
    description="Daily run of the crypto data pipeline"
)