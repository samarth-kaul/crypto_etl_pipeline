# Tests for config loading

import pytest
import yaml
from pathlib import Path
from src.config import Config

def test_load_config_success(tmp_path, mocker):
    """Test successful loading of a valid config file."""
    config_content = {
        'api': {'base_url': 'https://api.coingecko.com/api/v3', 'endpoint': '/coins/markets'},
        'transform': {'min_market_cap': 1000000, 'columns_to_keep': ['id', 'symbol'], 'aggregate': {}},
        's3': {'bucket_name': 'test-bucket', 'prefix': 'test/'}
    }
    config_path = tmp_path / 'config.yaml'
    with open(config_path, 'w') as f:
        yaml.safe_dump(config_content, f)
    
    config = Config(config_path)
    assert config.get_api_config() == config_content['api']
    assert config.get_transform_config() == config_content['transform']
    assert config.get_s3_config() == config_content['s3']

def test_load_config_missing_file():
    """Test error handling for missing config file."""
    with pytest.raises(FileNotFoundError):
        Config('nonexistent.yaml')

def test_validate_config_missing_section(tmp_path):
    """Test validation error for missing config section."""
    config_content = {
        'api': {'base_url': 'https://api.coingecko.com/api/v3', 'endpoint': '/coins/markets'}
        # Missing 'transform' and 's3' sections
    }
    config_path = tmp_path / 'config.yaml'
    with open(config_path, 'w') as f:
        yaml.safe_dump(config_content, f)
    
    with pytest.raises(ValueError, match="Missing required configuration section: transform"):
        Config(config_path)