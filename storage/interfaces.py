"""Storage interfaces using Protocol for duck typing."""
from typing import Any, Dict, List, Protocol, runtime_checkable


@runtime_checkable
class MarketDataRepository(Protocol):
    """
    Protocol defining the interface for market data persistence.
    
    Any storage backend (TimescaleDB, InfluxDB, etc.) can implement this
    interface for compatibility with the orchestrator layer.
    """
    
    def write_ohlcv_bars(self, symbol: str, bars: List[Dict], timeframe: str = "1m") -> int:
        """
        Write OHLCV bars to storage.
        
        Args:
            symbol: Ticker symbol
            bars: List of bar dictionaries with keys: t, o, h, l, c, v, vw, n
            timeframe: Timeframe (1m, 5m, 15m, 30m, 1h, 2h, 4h, 12h, 1d, 1w, 1mo)
        
        Returns:
            Number of bars written
        """
        ...
    
    def write_reference_data(self, symbol: str, details: Dict[str, Any]) -> bool:
        """
        Write ticker reference/metadata to storage.
        
        Args:
            symbol: Ticker symbol
            details: Dictionary with ticker details
        
        Returns:
            True if successful
        """
        ...
    
    def write_corporate_actions(self, symbol: str, actions: List[Dict], action_type: str) -> int:
        """
        Write corporate actions to storage.
        
        Args:
            symbol: Ticker symbol
            actions: List of action dictionaries
            action_type: 'dividend' or 'split'
        
        Returns:
            Number of actions written
        """
        ...
    
    def write_news(self, articles: List[Dict]) -> int:
        """
        Write news articles to storage.
        
        Args:
            articles: List of article dictionaries
        
        Returns:
            Number of articles written
        """
        ...
    
    def write_snapshots(self, snapshots: List[Dict]) -> int:
        """
        Write snapshot payloads to storage.
        
        Args:
            snapshots: List of snapshot dictionaries
        
        Returns:
            Number of snapshots written
        """
        ...
    
    def close(self) -> None:
        """Close all connections and cleanup resources."""
        ...
