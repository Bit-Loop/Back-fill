"""
Polygon.io Reference Data Client
Fetches ticker metadata, exchanges, and market information.
"""
import logging
from typing import List, Dict, Optional

from .polygon_client import PolygonClient

logger = logging.getLogger(__name__)


class ReferenceClient:
    """
    Client for fetching reference data from Polygon.io.
    
    Endpoints:
    - /v3/reference/tickers
    - /v3/reference/tickers/{ticker}
    - /v3/reference/exchanges
    """
    
    def __init__(self, client: PolygonClient):
        """
        Initialize reference client.
        
        Args:
            client: PolygonClient instance
        """
        self.client = client
    
    def get_tickers(self, ticker: Optional[str] = None, type: Optional[str] = None,
                   market: str = 'stocks', exchange: Optional[str] = None,
                   active: bool = True, limit: int = 1000) -> List[Dict]:
        """
        Get list of tickers.
        
        Args:
            ticker: Filter by ticker symbol (optional)
            type: Filter by ticker type (CS, ADRC, ETF, etc., optional)
            market: Filter by market ('stocks', 'crypto', 'fx')
            exchange: Filter by exchange (optional)
            active: Only return active tickers
            limit: Limit of results per page
        
        Returns:
            List of ticker dictionaries
        """
        endpoint = "/v3/reference/tickers"
        
        params = {
            'market': market,
            'active': 'true' if active else 'false',
            'limit': limit
        }
        
        if ticker:
            params['ticker'] = ticker
        if type:
            params['type'] = type
        if exchange:
            params['exchange'] = exchange
        
        try:
            results = self.client._paginate(endpoint, params)
            logger.debug(f"Fetched {len(results)} tickers")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching tickers: {e}")
            return []
    
    def get_ticker_details(self, ticker: str) -> Optional[Dict]:
        """
        Get detailed information for a specific ticker.
        
        Args:
            ticker: Ticker symbol
        
        Returns:
            Ticker details dictionary or None if not found
        """
        endpoint = f"/v3/reference/tickers/{ticker}"
        
        try:
            response = self.client._make_request(endpoint)
            results = response.get('results')
            
            if results:
                logger.debug(f"Fetched details for {ticker}")
                return results
            else:
                logger.warning(f"No details found for {ticker}")
                return None
        
        except Exception as e:
            logger.error(f"Error fetching details for {ticker}: {e}")
            return None
    
    def get_ticker_types(self) -> List[Dict]:
        """
        Get list of ticker types.
        
        Returns:
            List of ticker type dictionaries
        """
        endpoint = "/v3/reference/tickers/types"
        
        try:
            response = self.client._make_request(endpoint)
            results = response.get('results', [])
            logger.debug(f"Fetched {len(results)} ticker types")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching ticker types: {e}")
            return []
    
    def get_market_holidays(self) -> List[Dict]:
        """
        Get list of market holidays.
        
        Returns:
            List of holiday dictionaries
        """
        endpoint = "/v1/marketstatus/upcoming"
        
        try:
            response = self.client._make_request(endpoint)
            results = response if isinstance(response, list) else []
            logger.debug(f"Fetched {len(results)} market holidays")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching market holidays: {e}")
            return []
    
    def get_market_status(self) -> Optional[Dict]:
        """
        Get current market status.
        
        Returns:
            Market status dictionary or None
        """
        endpoint = "/v1/marketstatus/now"
        
        try:
            response = self.client._make_request(endpoint)
            logger.debug("Fetched market status")
            return response
        
        except Exception as e:
            logger.error(f"Error fetching market status: {e}")
            return None
    
    def get_exchanges(self) -> List[Dict]:
        """
        Get list of exchanges.
        
        Returns:
            List of exchange dictionaries
        """
        endpoint = "/v3/reference/exchanges"
        
        try:
            results = self.client._paginate(endpoint)
            logger.debug(f"Fetched {len(results)} exchanges")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching exchanges: {e}")
            return []
