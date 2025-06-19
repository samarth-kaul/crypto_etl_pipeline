# Tests for Dagster assets

import pytest
import logging
from dagster import materialize
from src.dagster.assets import raw_crypto_data, transformed_crypto_data, s3_crypto_data
from src.dagster.resources import config_resource, s3_client_resource
from src.config import Config
import pandas as pd
from pathlib import Path

def test_dagster_assets(mocker, tmp_path, caplog):
    """Test Dagster assets end-to-end with logging."""
    caplog.set_level(logging.INFO)
    
    # Mock config
    config_content = {
        "api": {"base_url": "https://api.coingecko.com/api/v3", "endpoint": "/coins/markets"},
        "transform": {"min_market_cap": 1000000, "columns_to_keep": ["id", "symbol"], "aggregate": {}},
        "s3": {"bucket_name": "test-bucket", "prefix": "test/"}
    }
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        import yaml
        yaml.safe_dump(config_content, f)
    
    mock_config = Config(config_path)
    
    # Mock API response
    mock_raw_data = [{"id": "bitcoin", "symbol": "btc", "current_price": 50000, "market_cap": 1000000}]
    mocker.patch("src.extract.DataExtractor.extract", return_value=mock_raw_data)
    
    # Mock S3 client
    mock_s3 = mocker.patch("boto3.client")
    
    # Mock logger to capture file output
    log_file = tmp_path / "pipeline.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(log_file)]
    )
    
    # Materialize assets
    result = materialize(
        [raw_crypto_data, transformed_crypto_data, s3_crypto_data],
        resources={
            "config": config_resource.configured({"config_path": str(config_path)}),
            "s3_client": mock_s3
        }
    )
    
    assert result.success
    
    # Verify transformed data
    transformed_asset = result.asset_value(transformed_crypto_data.key)
    assert isinstance(transformed_asset, pd.DataFrame)
    assert len(transformed_asset) == 1
    assert transformed_asset["symbol"].iloc[0] == "btc"
    
    # Verify S3 call
    mock_s3.put_object.assert_called_once()
    
    # Verify Dagster logs
    log_records = [record for record in result.get_step_stats() if record.logging_metadata]
    assert any("Extracted 1 records from CoinGecko API" in str(record.logging_metadata) for record in log_records)
    assert any("Transformed data to 1 rows" in str(record.logging_metadata) for record in log_records)
    assert any("Loaded data to s3://test-bucket/test/" in str(record.logging_metadata) for record in log_records)
    
    # Verify file logs
    with open(log_file, "r") as f:
        log_content = f.read()
    assert "Extracted 1 records from CoinGecko API" in log_content
    assert "Transformed data to 1 rows" in log_content
    assert "Loaded data to s3://test-bucket/test/" in log_content
