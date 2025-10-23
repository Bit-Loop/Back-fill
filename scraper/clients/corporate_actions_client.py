"""
Polygon.io Corporate Actions Client
Fetches dividends, splits, and other corporate actions.
"""
import logging
from typing import List, Dict, Optional

from .polygon_client import PolygonClient

logger = logging.getLogger(__name__)


class CorporateActionsClient:
    """
    Client for fetching corporate actions from Polygon.io.
    
    Endpoints:
    - /v3/reference/dividends
    - /v3/reference/splits
    """
    
    def __init__(self, client: PolygonClient):
        """
        Initialize corporate actions client.
        
        Args:
            client: PolygonClient instance
        """
        self.client = client
    
    def get_dividends(self, ticker: Optional[str] = None, ex_dividend_date: Optional[str] = None,
                     record_date: Optional[str] = None, declaration_date: Optional[str] = None,
                     pay_date: Optional[str] = None, limit: int = 1000) -> List[Dict]:
        """
        Get dividend data.
        
        Args:
            ticker: Filter by ticker (optional)
            ex_dividend_date: Filter by ex-dividend date (YYYY-MM-DD, optional)
            record_date: Filter by record date (YYYY-MM-DD, optional)
            declaration_date: Filter by declaration date (YYYY-MM-DD, optional)
            pay_date: Filter by payment date (YYYY-MM-DD, optional)
            limit: Limit of results per page
        
        Returns:
            List of dividend dictionaries
        """
        endpoint = "/v3/reference/dividends"
        
        params = {'limit': limit}
        if ticker:
            params['ticker'] = ticker
        if ex_dividend_date:
            params['ex_dividend_date'] = ex_dividend_date
        if record_date:
            params['record_date'] = record_date
        if declaration_date:
            params['declaration_date'] = declaration_date
        if pay_date:
            params['pay_date'] = pay_date
        
        try:
            results = self.client._paginate(endpoint, params)
            logger.debug(f"Fetched {len(results)} dividends")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching dividends: {e}")
            return []
    
    def get_dividends_for_ticker(self, ticker: str, since: Optional[str] = None) -> List[Dict]:
        """
        Get all dividends for a specific ticker.
        
        Args:
            ticker: Ticker symbol
            since: Only fetch dividends since this date (YYYY-MM-DD, optional)
        
        Returns:
            List of dividend dictionaries
        """
        endpoint = "/v3/reference/dividends"
        params = {'ticker': ticker, 'limit': 1000}
        
        if since:
            params['ex_dividend_date.gte'] = since
        
        try:
            results = self.client._paginate(endpoint, params)
            logger.debug(f"Fetched {len(results)} dividends for {ticker}")
            return results
        except Exception as e:
            logger.error(f"Error fetching dividends for {ticker}: {e}")
            return []
    
    def get_splits(self, ticker: Optional[str] = None, execution_date: Optional[str] = None,
                  limit: int = 1000) -> List[Dict]:
        """
        Get stock split data.
        
        Args:
            ticker: Filter by ticker (optional)
            execution_date: Filter by execution date (YYYY-MM-DD, optional)
            limit: Limit of results per page
        
        Returns:
            List of split dictionaries
        """
        endpoint = "/v3/reference/splits"
        
        params = {'limit': limit}
        if ticker:
            params['ticker'] = ticker
        if execution_date:
            params['execution_date'] = execution_date
        
        try:
            results = self.client._paginate(endpoint, params)
            logger.debug(f"Fetched {len(results)} splits")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching splits: {e}")
            return []
    
    def get_splits_for_ticker(self, ticker: str, since: Optional[str] = None) -> List[Dict]:
        """
        Get all splits for a specific ticker.
        
        Args:
            ticker: Ticker symbol
            since: Only fetch splits since this date (YYYY-MM-DD, optional)
        
        Returns:
            List of split dictionaries
        """
        endpoint = "/v3/reference/splits"
        params = {'ticker': ticker, 'limit': 1000}
        
        if since:
            params['execution_date.gte'] = since
        
        try:
            results = self.client._paginate(endpoint, params)
            logger.debug(f"Fetched {len(results)} splits for {ticker}")
            return results
        except Exception as e:
            logger.error(f"Error fetching splits for {ticker}: {e}")
            return []

