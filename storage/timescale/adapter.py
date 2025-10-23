"""Adapter wrapping TimescaleWriter to implement MarketDataRepository protocol."""
from typing import Any, Dict, List, Optional

from ..interfaces import MarketDataRepository
from .writer import TimescaleWriter


class TimescaleAdapter(MarketDataRepository):
    """
    Adapter implementing MarketDataRepository protocol using TimescaleWriter.
    
    This adapter allows TimescaleWriter to be used anywhere the MarketDataRepository
    protocol is expected, enabling dependency injection and easy swapping of storage backends.
    """
    
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 database: Optional[str] = None, user: Optional[str] = None,
                 password: Optional[str] = None, min_conn: int = 1, max_conn: int = 20):
        """
        Initialize adapter with TimescaleWriter.
        
        Args:
            host: Database host (default: localhost)
            port: Database port (default: 5432)
            database: Database name (default: trading)
            user: Database user (default: postgres)
            password: Database password
            min_conn: Minimum connections in pool
            max_conn: Maximum connections in pool
        """
        self._writer = TimescaleWriter(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            min_conn=min_conn,
            max_conn=max_conn
        )
    
    def write_ohlcv_bars(self, symbol: str, bars: List[Dict], timeframe: str = "1m") -> int:
        """Write OHLCV bars via TimescaleWriter."""
        return self._writer.write_ohlcv_bars(symbol, bars, timeframe)
    
    def write_reference_data(self, symbol: str, details: Dict[str, Any]) -> bool:
        """Write ticker reference data via TimescaleWriter."""
        return self._writer.write_reference_data(symbol, details)
    
    def write_corporate_actions(self, symbol: str, actions: List[Dict], action_type: str) -> int:
        """Write corporate actions via TimescaleWriter."""
        return self._writer.write_corporate_actions(symbol, actions, action_type)
    
    def write_news(self, articles: List[Dict]) -> int:
        """Write news articles via TimescaleWriter."""
        return self._writer.write_news(articles)
    
    def write_snapshots(self, snapshots: List[Dict]) -> int:
        """Write snapshots via TimescaleWriter."""
        return self._writer.write_snapshots(snapshots)
    
    def close(self) -> None:
        """Close TimescaleWriter connections."""
        self._writer.close()
