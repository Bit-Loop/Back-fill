"""
OHLCV data repository for TimescaleDB.

Handles reading and writing OHLCV (Open, High, Low, Close, Volume) bar data.
"""
import logging
from datetime import datetime
from typing import Dict, List

from psycopg2 import extras

logger = logging.getLogger(__name__)


class OHLCVRepository:
    """
    Repository for OHLCV bar data operations.
    
    Responsibilities:
    - Write OHLCV bars to appropriate timeframe tables
    - Map timeframe aliases to canonical table names
    - Batch insert optimization with conflict handling
    """
    
    # Timeframe mapping
    TIMEFRAME_MAP = {
        '1min': '1m', '1minute': '1m', '1m': '1m',
        '5min': '5m', '5minute': '5m', '5m': '5m',
        '15min': '15m', '15minute': '15m', '15m': '15m',
        '30min': '30m', '30minute': '30m', '30m': '30m',
        '1hour': '1h', '1h': '1h',
        '2hour': '2h', '2h': '2h',
        '4hour': '4h', '4h': '4h',
        '12hour': '12h', '12h': '12h',
        '1day': '1d', '1d': '1d',
        '1week': '1w', '1w': '1w',
        '1month': '1mo', '1mo': '1mo'
    }
    
    def __init__(self, pool):
        """
        Initialize OHLCV repository.
        
        Args:
            pool: PostgresConnectionPool instance
        """
        self.pool = pool
    
    def write_bars(self, symbol: str, bars: List[Dict], timeframe: str = "1m") -> int:
        """
        Write OHLCV bars to database.
        
        Args:
            symbol: Ticker symbol
            bars: List of bar dictionaries with keys: t, o, h, l, c, v, vw, n
            timeframe: Timeframe (1m, 5m, 15m, 30m, 1h, 2h, 4h, 12h, 1d, 1w, 1mo)
        
        Returns:
            Number of bars written
        """
        if not bars:
            return 0
        
        # Map timeframe to table name
        tf = self.TIMEFRAME_MAP.get(timeframe, '1m')
        table_name = f'market_data_{tf}'
        
        with self.pool.get_connection() as conn:
            cur = conn.cursor()
            
            # Prepare data for batch insert
            data = []
            for bar in bars:
                timestamp = datetime.fromtimestamp(bar.get('t', bar.get('timestamp', 0)) / 1000.0)
                data.append((
                    timestamp,
                    symbol,
                    bar.get('o', bar.get('open')),
                    bar.get('h', bar.get('high')),
                    bar.get('l', bar.get('low')),
                    bar.get('c', bar.get('close')),
                    bar.get('v', bar.get('volume')),
                    bar.get('vw', bar.get('vwap')),
                    bar.get('n', bar.get('transactions'))
                ))
            
            # Batch insert with ON CONFLICT DO NOTHING (idempotent)
            extras.execute_batch(cur, f"""
                INSERT INTO {table_name} 
                (time, symbol, open, high, low, close, volume, vwap, transactions)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, symbol) DO NOTHING
            """, data, page_size=1000)
            
            conn.commit()
            logger.debug(f"Wrote {len(data)} bars for {symbol} ({timeframe})")
            return len(data)
