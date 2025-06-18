# Dagster jobs (pipeline definition)

from dagster import define_asset_job, AssetSelection

crypto_pipeline_job = define_asset_job(
    name="crypto_pipeline_job",
    selection=AssetSelection.all(),
    description="ETL pipeline for cryptocurrency data"
)