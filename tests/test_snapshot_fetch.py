"""Test snapshot fetch and validation."""
import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestSnapshotFetch:
    """Test snapshot fetching and validation."""
    
    @pytest.mark.asyncio
    async def test_fetch_valid_snapshot(self, mock_polygon_snapshot):
        """Test fetching valid snapshot from Polygon."""
        from scraper.pipeline.snapshot_validator import validate_snapshot
        
        result = validate_snapshot(mock_polygon_snapshot)
        
        assert result['valid'] is True
        assert result['symbol'] == 'AAPL'
        assert 'o' in result['data']
        assert 'h' in result['data']
        assert 'l' in result['data']
        assert 'c' in result['data']
        assert 'v' in result['data']
    
    @pytest.mark.asyncio
    async def test_snapshot_missing_ohlcv(self):
        """Test snapshot validation with missing OHLCV keys."""
        from scraper.pipeline.snapshot_validator import validate_snapshot
        
        invalid_snapshot = {
            'status': 'success',
            'ticker': {
                'ticker': 'AAPL',
                'min': {
                    'o': 151.0,
                    'h': 151.8
                }
            }
        }
        
        result = validate_snapshot(invalid_snapshot)
        
        assert result['valid'] is False
        assert 'missing' in result['error'].lower()
    
    @pytest.mark.asyncio
    async def test_snapshot_timestamp_validation(self, mock_polygon_snapshot):
        """Test snapshot timestamp > last_backfill."""
        from scraper.pipeline.snapshot_validator import validate_snapshot_timestamp
        
        last_backfill = datetime.utcnow() - timedelta(hours=1)
        snapshot_time = datetime.fromtimestamp(mock_polygon_snapshot['ticker']['min']['t'] / 1000)
        
        result = validate_snapshot_timestamp(snapshot_time, last_backfill)
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_snapshot_stale_timestamp(self):
        """Test snapshot with stale timestamp."""
        from scraper.pipeline.snapshot_validator import validate_snapshot_timestamp
        
        last_backfill = datetime.utcnow()
        snapshot_time = datetime.utcnow() - timedelta(hours=2)
        
        result = validate_snapshot_timestamp(snapshot_time, last_backfill)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_snapshot_kafka_publish(self, mock_kafka_producer, mock_polygon_snapshot):
        """Test snapshot publishes to correct Kafka topic."""
        from scraper.pipeline.snapshot_validator import publish_snapshot_event
        
        symbol = 'AAPL'
        await publish_snapshot_event(mock_kafka_producer, symbol, mock_polygon_snapshot)
        
        assert len(mock_kafka_producer.messages) == 1
        message = mock_kafka_producer.messages[0]
        assert message['topic'] == f'chronox.snapshot.{symbol.lower()}'
        assert message['key'] == symbol.encode('utf-8')
        
        payload = json.loads(message['value'])
        assert payload['symbol'] == symbol
        assert payload['type'] == 'snapshot'
        assert 'data' in payload
    
    @pytest.mark.asyncio
    async def test_snapshot_invalid_triggers_error(self):
        """Test invalid snapshot triggers error state."""
        from scraper.pipeline.snapshot_validator import validate_snapshot
        
        invalid_snapshot = {
            'status': 'error',
            'error': 'Symbol not found'
        }
        
        result = validate_snapshot(invalid_snapshot)
        
        assert result['valid'] is False
        assert 'error' in result
    
    @pytest.mark.asyncio
    async def test_snapshot_registry_update(self):
        """Test snapshot updates registry timestamp."""
        import sqlite3
        from datetime import datetime
        
        conn = sqlite3.connect(':memory:')
        conn.execute("""
            CREATE TABLE symbol_registry (
                symbol TEXT PRIMARY KEY,
                last_snapshot TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        conn.execute("INSERT INTO symbol_registry (symbol) VALUES (?)", ('AAPL',))
        conn.commit()
        
        now = datetime.utcnow().isoformat()
        conn.execute("""
            UPDATE symbol_registry
            SET last_snapshot = ?, updated_at = ?
            WHERE symbol = ?
        """, (now, now, 'AAPL'))
        conn.commit()
        
        cursor = conn.execute("SELECT last_snapshot FROM symbol_registry WHERE symbol = ?", ('AAPL',))
        row = cursor.fetchone()
        
        assert row[0] is not None
        conn.close()
    
    @pytest.mark.asyncio
    async def test_snapshot_retry_on_failure(self):
        """Test snapshot retry logic on transient failure."""
        from scraper.pipeline.snapshot_validator import fetch_with_retry
        
        mock_client = AsyncMock()
        mock_client.get_snapshot.side_effect = [
            Exception("Timeout"),
            {'status': 'success', 'ticker': {'ticker': 'AAPL'}}
        ]
        
        result = await fetch_with_retry(mock_client, 'AAPL', max_retries=2)
        
        assert result is not None
        assert mock_client.get_snapshot.call_count == 2
    
    @pytest.mark.asyncio
    async def test_snapshot_max_retries_exceeded(self):
        """Test snapshot fails after max retries."""
        from scraper.pipeline.snapshot_validator import fetch_with_retry
        
        mock_client = AsyncMock()
        mock_client.get_snapshot.side_effect = Exception("Persistent error")
        
        with pytest.raises(Exception):
            await fetch_with_retry(mock_client, 'AAPL', max_retries=3)
        
        assert mock_client.get_snapshot.call_count == 3
