"""Snapshot validation and Kafka publishing."""
import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def validate_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate snapshot response from Polygon.
    
    Args:
        snapshot: Snapshot response dictionary
    
    Returns:
        Dict with 'valid' boolean and optional 'error' message
    """
    if not snapshot or snapshot.get('status') == 'error':
        return {
            'valid': False,
            'error': snapshot.get('error', 'Unknown error')
        }
    
    ticker_data = snapshot.get('ticker', {})
    if not ticker_data:
        return {
            'valid': False,
            'error': 'Missing ticker data'
        }
    
    symbol = ticker_data.get('ticker')
    min_data = ticker_data.get('min', {})
    
    required_keys = ['o', 'h', 'l', 'c', 'v']
    missing_keys = [k for k in required_keys if k not in min_data]
    
    if missing_keys:
        return {
            'valid': False,
            'error': f'Missing OHLCV keys: {missing_keys}'
        }
    
    return {
        'valid': True,
        'symbol': symbol,
        'data': min_data
    }


def validate_snapshot_timestamp(
    snapshot_time: datetime,
    last_backfill: Optional[datetime]
) -> bool:
    """
    Validate snapshot timestamp is newer than last backfill.
    
    Args:
        snapshot_time: Timestamp from snapshot
        last_backfill: Last backfill timestamp
    
    Returns:
        True if snapshot is newer
    """
    if last_backfill is None:
        return True
    
    return snapshot_time > last_backfill


async def publish_snapshot_event(
    kafka_producer: Any,
    symbol: str,
    snapshot: Dict[str, Any]
) -> None:
    """
    Publish snapshot event to Kafka.
    
    Args:
        kafka_producer: Kafka producer instance
        symbol: Symbol ticker
        snapshot: Snapshot data
    """
    topic = f'chronox.snapshot.{symbol.lower()}'
    
    payload = {
        'symbol': symbol,
        'type': 'snapshot',
        'timestamp': datetime.utcnow().isoformat(),
        'data': snapshot
    }
    
    message_bytes = json.dumps(payload).encode('utf-8')
    key_bytes = symbol.encode('utf-8')
    
    await kafka_producer.send(topic, message_bytes, key_bytes)
    logger.info(f"Published snapshot event to {topic}")


async def fetch_with_retry(
    client: Any,
    symbol: str,
    max_retries: int = 3,
    retry_delay: float = 1.0
) -> Optional[Dict[str, Any]]:
    """
    Fetch snapshot with retry logic.
    
    Args:
        client: Snapshot client
        symbol: Symbol ticker
        max_retries: Maximum retry attempts
        retry_delay: Delay between retries in seconds
    
    Returns:
        Snapshot data or None on failure
    
    Raises:
        Exception: After max retries exceeded
    """
    last_error = None
    
    for attempt in range(max_retries):
        try:
            result = await client.get_snapshot(symbol)
            return result
        except Exception as e:
            last_error = e
            logger.warning(f"Snapshot fetch attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
    
    raise last_error
