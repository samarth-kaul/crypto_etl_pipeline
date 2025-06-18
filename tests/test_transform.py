# Tests for data transformation

import pytest
import pandas as pd
from src.transform import DataTransformer

def test_transform_success():
    """Test successful data transformation."""
    transform_config = {
        'min_market_cap': 1000000,
        'columns_to_keep': ['id', 'symbol', 'current_price', 'market_cap'],
        'aggregate': {
            'group_by': 'symbol',
            'metrics': {'avg_price': 'current_price', 'total_market_cap': 'market_cap'}
        }
    }
    raw_data = [
        {'id': 'bitcoin', 'symbol': 'btc', 'current_price': 50000, 'market_cap': 1000000, 'total_volume': 1000},
        {'id': 'ethereum', 'symbol': 'eth', 'current_price': None, 'market_cap': 500000, 'total_volume': 500},
        {'id': 'litecoin', 'symbol': 'ltc', 'current_price': 200, 'market_cap': 2000000, 'total_volume': 200}
    ]
    
    transformer = DataTransformer(transform_config)
    df = transformer.transform(raw_data)
    
    # Check cleaning (nulls removed)
    assert len(df) == 2  # Ethereum should be dropped due to null current_price
    
    # Check filtering (market cap >= 1M)
    assert all(df['market_cap'] >= 1000000)
    
    # Check aggregation
    assert list(df.columns) == ['symbol', 'current_price', 'market_cap']
    assert len(df) == 2  # Grouped by symbol (btc, ltc)

def test_transform_empty_data():
    """Test transformation with empty data."""
    transform_config = {
        'min_market_cap': 1000000,
        'columns_to_keep': ['id', 'symbol'],
        'aggregate': {}
    }
    transformer = DataTransformer(transform_config)
    df = transformer.transform([])
    assert df.empty