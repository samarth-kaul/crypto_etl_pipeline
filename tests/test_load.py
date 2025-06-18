# Tests for S3 loading

import pytest
import pandas as pd
from src.load import DataLoader

def test_load_success(mocker):
    """Test successful data loading to S3."""
    s3_config = {
        'bucket_name': 'test-bucket',
        'prefix': 'test/'
    }
    df = pd.DataFrame([
        {'symbol': 'btc', 'current_price': 50000},
        {'symbol': 'eth', 'current_price': 3000}
    ])
    
    mock_s3 = mocker.patch('boto3.client')
    loader = DataLoader(s3_config)
    loader.load(df)
    
    # Verify S3 put_object was called
    mock_s3.return_value.put_object.assert_called_once()
    call_args = mock_s3.return_value.put_object.call_args
    assert call_args[1]['Bucket'] == 'test-bucket'
    assert call_args[1]['Key'].startswith('test/crypto_data_')

def test_load_failure(mocker):
    """Test error handling for S3 upload failure."""
    s3_config = {
        'bucket_name': 'test-bucket',
        'prefix': 'test/'
    }
    df = pd.DataFrame([{'symbol': 'btc', 'current_price': 50000}])
    
    mock_s3 = mocker.patch('boto3.client')
    mock_s3.return_value.put_object.side_effect = Exception("S3 error")
    
    loader = DataLoader(s3_config)
    with pytest.raises(Exception, match="S3 error"):
        loader.load(df)