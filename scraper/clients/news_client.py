"""
Polygon.io News Client
Fetches news articles and related data.
"""
import logging
from typing import List, Dict, Optional

from .polygon_client import PolygonClient

logger = logging.getLogger(__name__)


class NewsClient:
    """
    Client for fetching news articles from Polygon.io.
    
    Endpoints:
    - /v2/reference/news
    """
    
    def __init__(self, client: PolygonClient):
        """
        Initialize news client.
        
        Args:
            client: PolygonClient instance
        """
        self.client = client
    
    def get_news(self, ticker: Optional[str] = None, published_utc: Optional[str] = None,
                order: str = 'desc', limit: int = 100) -> List[Dict]:
        """
        Get news articles.
        
        Args:
            ticker: Filter by ticker symbol (optional)
            published_utc: Filter by publication date (YYYY-MM-DD, optional)
            order: Sort order ('desc' or 'asc')
            limit: Limit of results per page
        
        Returns:
            List of news article dictionaries
        """
        endpoint = "/v2/reference/news"
        
        params = {
            'order': order,
            'limit': limit
        }
        
        if ticker:
            params['ticker'] = ticker
        if published_utc:
            params['published_utc'] = published_utc
        
        try:
            results = self.client._paginate(endpoint, params, max_results=limit)
            logger.debug(f"Fetched {len(results)} news articles")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching news: {e}")
            return []
    
    def get_news_for_ticker(self, ticker: str, limit: int = 100) -> List[Dict]:
        """
        Get news articles for a specific ticker.
        
        Args:
            ticker: Ticker symbol
            limit: Maximum number of articles
        
        Returns:
            List of news article dictionaries
        """
        return self.get_news(ticker=ticker, limit=limit)
