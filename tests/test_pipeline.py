# Tests for pipeline orchestration

import pytest
from src.pipeline import Pipeline
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader

def test_pipeline_run_success(mocker, tmp_path):
    """Test successful pipeline execution."""
    config_content = {
        'api': {'base_url': 'https://api.coingecko.com/api/v3', 'endpoint': '/coins/markets'},
        'transform': {'min_market_cap': 1000000, 'columns_to_keep': ['id', 'symbol'], 'aggregate': {}},
        's3': {'bucket_name': 'test-bucket', 'prefix': 'test/'}
    }
    config_path = tmp_path / 'config.yaml'
    with open(config_path, 'w') as f:
        yaml.safe_dump(config_content, f)
    
    # Mock dependencies
    mock_extract = mocker.patch.object(DataExtractor, 'extract', return_value=[
        {'id': 'bitcoin', 'symbol': 'btc', 'current_price': 50000, 'market_cap': 1000000}
    ])
    mock_transform = mocker.patch.object(DataTransformer, 'transform', return_value=pd.DataFrame([
        {'id': 'bitcoin', 'symbol': 'btc'}
    ]))
    mock_load = mocker.patch.object(DataLoader, 'load')
    
    pipeline = Pipeline(config_path)
    pipeline.run()
    
    mock_extract.assert_called_once()
    mock_transform.assert_called_once()
    mock_load.assert_called_once()

def test_pipeline_run_failure(mocker, tmp_path):
    """Test pipeline failure handling."""
    config_content = {
        'api': {'base_url': 'https://api.coingecko.com/api/v3', 'endpoint': '/coins/markets'},
        'transform': {'min_market_cap': 1000000, 'columns_to_keep': ['id', 'symbol'], 'aggregate': {}},
        's3': {'bucket_name': 'test-bucket', 'prefix': 'test/'}
    }
    config_path = tmp_path / 'config.yaml'
    with open(config_path, 'w') as f:
        yaml.safe_dump(config_content, f)
    
    # Mock extract to raise an error
    mocker.patch.object(DataExtractor, 'extract', side_effect=Exception("Extraction error"))
    
    pipeline = Pipeline(config_path)
    with pytest.raises(Exception, match="Extraction error"):
        pipeline.run()