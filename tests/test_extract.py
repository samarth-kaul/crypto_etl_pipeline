# Tests for data extraction

import pytest
import requests
from src.extract import DataExtractor

def test_extract_success(mocker):
    """Test successful data extraction from API."""
    api_config = {
        'base_url': 'https://api.coingecko.com/api/v3',
        'endpoint': '/coins/markets',
        'params': {'vs_currency': 'usd'}
    }
    mock_response = [
        {'id': 'bitcoin', 'symbol': 'btc', 'current_price': 50000, 'market_cap': 1000000},
        {'id': 'ethereum', 'symbol': 'eth', 'current_price': 3000, 'market_cap': 500000}
    ]
    mocker.patch('requests.get', return_value=mocker.Mock(status_code=200, json=lambda: mock_response))
    
    extractor = DataExtractor(api_config)
    data = extractor.extract()
    assert len(data) == 2
    assert data[0]['id'] == 'bitcoin'
    assert data[1]['id'] == 'ethereum'

def test_extract_api_failure(mocker):
    """Test error handling for API failure."""
    api_config = {
        'base_url': 'https://api.coingecko.com/api/v3',
        'endpoint': '/coins/markets'
    }
    mocker.patch('requests.get', side_effect=requests.exceptions.RequestException("API error"))
    
    extractor = DataExtractor(api_config)
    with pytest.raises(requests.exceptions.RequestException, match="API error"):
        extractor.extract()