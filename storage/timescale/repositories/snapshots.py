"""
Snapshots repository for TimescaleDB.

Handles reading and writing market snapshots.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from psycopg2 import extras

logger = logging.getLogger(__name__)


class SnapshotsRepository:
    """
    Repository for market snapshots data operations.
    
    Responsibilities:
    - Write real-time snapshot data
    - Handle timestamp normalization
    - Batch insert optimization with upsert
    """
    
    def __init__(self, pool):
        """
        Initialize snapshots repository.
        
        Args:
            pool: PostgresConnectionPool instance
        """
        self.pool = pool
    
    def write_snapshots(self, snapshots: List[Dict]) -> int:
        """
        Write snapshot payloads to database.
        
        Args:
            snapshots: List of snapshot dictionaries
        
        Returns:
            Number of snapshots written
        """
        if not snapshots:
            return 0
        
        with self.pool.get_connection() as conn:
            cur = conn.cursor()
            
            data = []
            for snapshot in snapshots:
                updated = snapshot.get('updated_at')
                if isinstance(updated, datetime):
                    updated_at = updated if updated.tzinfo else updated.replace(tzinfo=timezone.utc)
                else:
                    numeric_ts: Optional[float] = None
                    if isinstance(updated, (int, float)):
                        numeric_ts = float(updated)
                    elif isinstance(updated, str):
                        try:
                            numeric_ts = float(updated)
                        except ValueError:
                            numeric_ts = None
                    
                    if numeric_ts is not None:
                        updated_at = datetime.fromtimestamp(numeric_ts / 1000.0, tz=timezone.utc)
                    else:
                        updated_at = datetime.now(timezone.utc)
                
                data.append((
                    snapshot.get('symbol'),
                    updated_at,
                    snapshot.get('last_trade_price'),
                    snapshot.get('last_trade_size'),
                    snapshot.get('last_trade_exchange'),
                    snapshot.get('last_quote_bid'),
                    snapshot.get('last_quote_ask'),
                    snapshot.get('last_quote_bid_size'),
                    snapshot.get('last_quote_ask_size'),
                    snapshot.get('day_open'),
                    snapshot.get('day_high'),
                    snapshot.get('day_low'),
                    snapshot.get('day_close'),
                    snapshot.get('day_volume'),
                    snapshot.get('prev_close'),
                    snapshot.get('prev_volume'),
                    snapshot.get('todays_change'),
                    snapshot.get('todays_change_percent'),
                    snapshot.get('minute_vwap'),
                    snapshot.get('minute_volume'),
                    extras.Json(snapshot.get('raw'))
                ))
            
            extras.execute_batch(cur, """
                INSERT INTO market_snapshots (
                    symbol, updated_at, last_trade_price, last_trade_size, last_trade_exchange,
                    last_quote_bid, last_quote_ask, last_quote_bid_size, last_quote_ask_size,
                    day_open, day_high, day_low, day_close, day_volume,
                    prev_close, prev_volume, todays_change, todays_change_percent,
                    minute_vwap, minute_volume, raw_data
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (symbol, updated_at) DO UPDATE SET
                    last_trade_price = EXCLUDED.last_trade_price,
                    last_trade_size = EXCLUDED.last_trade_size,
                    last_trade_exchange = EXCLUDED.last_trade_exchange,
                    last_quote_bid = EXCLUDED.last_quote_bid,
                    last_quote_ask = EXCLUDED.last_quote_ask,
                    last_quote_bid_size = EXCLUDED.last_quote_bid_size,
                    last_quote_ask_size = EXCLUDED.last_quote_ask_size,
                    day_open = EXCLUDED.day_open,
                    day_high = EXCLUDED.day_high,
                    day_low = EXCLUDED.day_low,
                    day_close = EXCLUDED.day_close,
                    day_volume = EXCLUDED.day_volume,
                    prev_close = EXCLUDED.prev_close,
                    prev_volume = EXCLUDED.prev_volume,
                    todays_change = EXCLUDED.todays_change,
                    todays_change_percent = EXCLUDED.todays_change_percent,
                    minute_vwap = EXCLUDED.minute_vwap,
                    minute_volume = EXCLUDED.minute_volume,
                    raw_data = EXCLUDED.raw_data
            """, data, page_size=500)
            
            conn.commit()
            logger.debug("Wrote %s market snapshots", len(data))
            return len(data)
