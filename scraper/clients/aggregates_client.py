"""
Polygon.io Aggregates (OHLCV Bars) Client
Fetches historical and real-time market data bars.
"""
import logging
from typing import List, Dict, Optional
from datetime import datetime

from .polygon_client import PolygonClient

logger = logging.getLogger(__name__)


class AggregatesClient:
    """
    Client for fetching aggregates (OHLCV bars) from Polygon.io.
    
    Endpoints:
    - /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
    - /v2/aggs/grouped/locale/{locale}/market/{market}/{date}
    """
    
    def __init__(self, client: PolygonClient):
        """
        Initialize aggregates client.
        
        Args:
            client: PolygonClient instance
        """
        self.client = client
    
    def get_bars(self, symbol: str, multiplier: int, timespan: str,
                 from_date: str, to_date: str, limit: int = 50000,
                 adjusted: bool = True, sort: str = 'asc') -> List[Dict]:
        """
        Get aggregate bars for a symbol.
        
        Args:
            symbol: Ticker symbol (e.g., 'AAPL')
            multiplier: Size of timespan multiplier (1 for 1 minute, 5 for 5 minutes)
            timespan: Size of time window ('minute', 'hour', 'day', 'week', 'month')
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            limit: Limit of results per page (max 50000)
            adjusted: Whether to adjust for splits
            sort: Sort direction ('asc' or 'desc')
        
        Returns:
            List of bar dictionaries with keys: t, o, h, l, c, v, vw, n
        """
        endpoint = f"/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        
        params = {
            'adjusted': 'true' if adjusted else 'false',
            'sort': sort,
            'limit': limit
        }
        
        try:
            results = self.client._paginate(endpoint, params)
            logger.debug(f"Fetched {len(results)} bars for {symbol} ({timespan})")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching bars for {symbol}: {e}")
            return []
    
    def get_minute_bars(self, symbol: str, from_date: str, to_date: str,
                       adjusted: bool = True) -> List[Dict]:
        """
        Get 1-minute bars for a symbol.
        
        Args:
            symbol: Ticker symbol
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            adjusted: Whether to adjust for splits
        
        Returns:
            List of 1-minute bar dictionaries
        """
        return self.get_bars(symbol, 1, 'minute', from_date, to_date, adjusted=adjusted)
    
    def get_daily_bars(self, symbol: str, from_date: str, to_date: str,
                      adjusted: bool = True) -> List[Dict]:
        """
        Get daily bars for a symbol.
        
        Args:
            symbol: Ticker symbol
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            adjusted: Whether to adjust for splits
        
        Returns:
            List of daily bar dictionaries
        """
        return self.get_bars(symbol, 1, 'day', from_date, to_date, adjusted=adjusted)
    
    def get_hourly_bars(self, symbol: str, from_date: str, to_date: str,
                       adjusted: bool = True) -> List[Dict]:
        """
        Get hourly bars for a symbol.
        
        Args:
            symbol: Ticker symbol
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            adjusted: Whether to adjust for splits
        
        Returns:
            List of hourly bar dictionaries
        """
        return self.get_bars(symbol, 1, 'hour', from_date, to_date, adjusted=adjusted)
    
    def get_grouped_daily(self, date: str, locale: str = 'us', market: str = 'stocks',
                         adjusted: bool = True) -> List[Dict]:
        """
        Get grouped daily bars for all tickers on a date.
        
        Args:
            date: Date (YYYY-MM-DD)
            locale: Locale ('us', 'global')
            market: Market type ('stocks', 'crypto', 'fx')
            adjusted: Whether to adjust for splits
        
        Returns:
            List of bar dictionaries for all tickers
        """
        endpoint = f"/v2/aggs/grouped/locale/{locale}/market/{market}/{date}"
        
        params = {
            'adjusted': 'true' if adjusted else 'false'
        }
        
        try:
            response = self.client._make_request(endpoint, params)
            results = response.get('results', [])
            logger.debug(f"Fetched grouped daily bars for {len(results)} tickers on {date}")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching grouped daily bars for {date}: {e}")
            return []
