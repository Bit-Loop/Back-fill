"""Test fixtures for ChronoX tests."""
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime
from typing import Generator, List, Dict, Any

import pytest


# Mock classes (importable for direct instantiation in tests)
class MockKafkaProducer:
    """Mock Kafka producer for testing envelope publishing."""
    
    def __init__(self):
        self.messages: List[Dict[str, Any]] = []
    
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


class MockRedisClient:
    """Mock Redis client for testing pub/sub mirroring."""
    
    def __init__(self):
        self.published: List[Dict[str, Any]] = []
    
    async def publish(self, channel: str, message: str):
        self.published.append({
            'channel': channel,
            'message': message,
            'timestamp': datetime.utcnow()
        })
    
    async def close(self):
        pass


class MockPolygonClient:
    """Mock Polygon API client for testing API backfill."""
    
    def __init__(self):
        self.api_calls: List[Dict[str, Any]] = []
    
    async def get_aggregates(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Mock aggregates endpoint."""
        self.api_calls.append({
            "method": "get_aggregates",
            "symbol": symbol,
            "timeframe": timeframe,
            "start": start,
            "end": end,
            "timestamp": datetime.utcnow()
        })
        
        # Return mock OHLCV data
        return [
            {
                "t": int(start.timestamp() * 1000),
                "o": 100.0,
                "h": 101.0,
                "l": 99.0,
                "c": 100.5,
                "v": 10000
            }
        ]
    
    async def get_snapshot(self, symbol: str) -> Dict[str, Any]:
        """Mock snapshot endpoint."""
        self.api_calls.append({
            "method": "get_snapshot",
            "symbol": symbol,
            "timestamp": datetime.utcnow()
        })
        
        return {
            "ticker": symbol,
            "day": {
                "o": 100.0,
                "h": 105.0,
                "l": 98.0,
                "c": 102.0,
                "v": 1000000
            }
        }


# Fixtures
@pytest.fixture
def temp_db():
    """Provide temporary SQLite database for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    yield path
    os.close(fd)
    os.unlink(path)


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer fixture."""
    return MockKafkaProducer()


@pytest.fixture
def mock_redis_client():
    """Mock Redis client fixture."""
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
