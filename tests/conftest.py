"""Test fixtures for ChronoX tests."""
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime
from typing import Generator

import pytest


@pytest.fixture
def temp_db():
    """Provide temporary SQLite database for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    yield path
    os.close(fd)
    os.unlink(path)


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for testing."""
    
    class MockKafkaProducer:
        def __init__(self):
            self.messages = []
        
        async def send(self, topic: str, value: bytes, key: bytes = None):
            self.messages.append({
                'topic': topic,
                'value': value,
                'key': key,
                'timestamp': datetime.utcnow()
            })
        
        async def flush(self):
            pass
        
        async def close(self):
            pass
    
    return MockKafkaProducer()


@pytest.fixture
def mock_redis_client():
    """Mock Redis client for testing."""
    
    class MockRedisClient:
        def __init__(self):
            self.published = []
        
        async def publish(self, channel: str, message: str):
            self.published.append({
                'channel': channel,
                'message': message,
                'timestamp': datetime.utcnow()
            })
        
        async def close(self):
            pass
    
    return MockRedisClient()


@pytest.fixture
def mock_polygon_snapshot():
    """Mock Polygon snapshot response."""
    return {
        'status': 'success',
        'ticker': {
            'ticker': 'AAPL',
            'day': {
                'o': 150.0,
                'h': 152.5,
                'l': 149.0,
                'c': 151.5,
                'v': 75000000,
                't': 1698019200000
            },
            'min': {
                'o': 151.0,
                'h': 151.8,
                'l': 150.5,
                'c': 151.5,
                'v': 1500000,
                't': 1698105600000
            },
            'prevDay': {
                'o': 148.0,
                'h': 150.0,
                'l': 147.5,
                'c': 149.5,
                'v': 70000000,
                't': 1697932800000
            }
        }
    }


@pytest.fixture
def sample_registry_entries():
    """Sample registry entries for testing."""
    return [
        {
            'symbol': 'AAPL',
            'strategy': 'flatpack',
            'timeframe': '1m',
            'enabled': True,
            'status': 'idle'
        },
        {
            'symbol': 'NVDA',
            'strategy': 'snapshot+stream',
            'timeframe': '1m',
            'enabled': True,
            'status': 'idle'
        },
        {
            'symbol': 'SPY',
            'strategy': 'flatpack+snapshot+stream',
            'timeframe': '1d',
            'enabled': True,
            'status': 'idle'
        }
    ]
