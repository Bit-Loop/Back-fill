"""
Polygon.io REST API Client
Base client with rate limiting, retry logic, and connection pooling.
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import threading

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Thread-safe rate limiter using token bucket algorithm.
    """
    def __init__(self, requests_per_second: int = 5):
        """
        Initialize rate limiter.
        
        Args:
            requests_per_second: Maximum requests per second
        """
        self.rate = requests_per_second
        self.tokens = requests_per_second
        self.max_tokens = requests_per_second
        self.last_update = time.time()
        self.lock = threading.Lock()
    
    def acquire(self):
        """Acquire a token (blocks if necessary)"""
        with self.lock:
            while self.tokens < 1:
                # Refill tokens based on elapsed time
                now = time.time()
                elapsed = now - self.last_update
                self.tokens += elapsed * self.rate
                self.tokens = min(self.tokens, self.max_tokens)
                self.last_update = now
                
                if self.tokens < 1:
                    time.sleep(0.1)
            
            self.tokens -= 1


class PolygonClient:
    """
    Polygon.io REST API base client.
    
    Features:
    - HTTP connection pooling
    - Automatic retry with exponential backoff
    - Rate limiting (5 req/s free tier, 100 req/s professional)
    - Thread-safe request handling
    """
    
    BASE_URL = "https://api.polygon.io"
    
    def __init__(self, api_key: str, rate_limit: int = 5, max_retries: int = 3,
                 timeout: int = 30, max_connections: int = 800):
        """
        Initialize Polygon client.
        
        Args:
            api_key: Polygon.io API key
            rate_limit: Requests per second (5 for free, 100 for professional)
            max_retries: Maximum retry attempts
            timeout: Request timeout in seconds
            max_connections: Maximum HTTP connections in pool
        """
        self.api_key = api_key
        self.rate_limiter = RateLimiter(rate_limit)
        self.timeout = timeout
        
        # Create session with connection pooling and retry logic
        self.session = requests.Session()
        
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(
            pool_connections=max_connections,
            pool_maxsize=max_connections,
            max_retries=retry_strategy,
            pool_block=True
        )
        
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        logger.info(f"Polygon client initialized: {rate_limit} req/s, {max_connections} max connections")
    
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make rate-limited HTTP request to Polygon API.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
        
        Returns:
            JSON response dictionary
        
        Raises:
            requests.HTTPError: If request fails after retries
        """
        # Wait for rate limiter
        self.rate_limiter.acquire()
        
        # Add API key to params
        if params is None:
            params = {}
        params['apiKey'] = self.api_key
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        
        except requests.HTTPError as e:
            logger.error(f"HTTP error for {endpoint}: {e}")
            raise
        
        except Exception as e:
            logger.error(f"Request error for {endpoint}: {e}")
            raise
    
    def _paginate(self, endpoint: str, params: Optional[Dict[str, Any]] = None,
                  max_results: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Paginate through API results using cursor.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            max_results: Maximum number of results to fetch (None for all)
        
        Returns:
            List of result dictionaries
        """
        if params is None:
            params = {}
        
        all_results = []
        next_cursor = None
        
        while True:
            if next_cursor:
                params['cursor'] = next_cursor
            
            response = self._make_request(endpoint, params)
            
            # Extract results
            results = response.get('results', [])
            if not results:
                break
            
            all_results.extend(results)
            
            # Check if we've reached max_results
            if max_results and len(all_results) >= max_results:
                all_results = all_results[:max_results]
                break
            
            # Get next cursor
            next_cursor = response.get('next_url')
            if not next_cursor:
                break
            
            # Extract cursor from next_url (format: ...?cursor=XXXXX)
            if 'cursor=' in next_cursor:
                next_cursor = next_cursor.split('cursor=')[1].split('&')[0]
            else:
                break
        
        return all_results
    
    def close(self):
        """Close the HTTP session"""
        self.session.close()
        logger.info("Closed Polygon client session")
