# Tests for Dagster assets

import pytest
from dagster import materialize
from src.dagster.assets import raw_crypto_data, transformed_crypto_data, s3_crypto_data
from src.dagster.resources import config_resource, s3_client_resource
from src.config import Config
import pandas as pd

def test_dagster_assets(mocker, tmp_path):
    """Test Dagster assets end-to-end."""
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