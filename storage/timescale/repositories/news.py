"""
News repository for TimescaleDB.

Handles reading and writing news articles.
"""
import logging
from datetime import datetime
from typing import Dict, List

from psycopg2 import extras

logger = logging.getLogger(__name__)


class NewsRepository:
    """
    Repository for news articles data operations.
    
    Responsibilities:
    - Write news articles with metadata
    - Batch insert optimization
    - Handle article deduplication
    """
    
    def __init__(self, pool):
        """
        Initialize news repository.
        
        Args:
            pool: PostgresConnectionPool instance
        """
        self.pool = pool
    
    def write_articles(self, articles: List[Dict]) -> int:
        """
        Write news articles to database.
        
        Args:
            articles: List of article dictionaries
        
        Returns:
            Number of articles written
        """
        if not articles:
            return 0
        
        with self.pool.get_connection() as conn:
            cur = conn.cursor()
            
            data = []
            for article in articles:
                data.append((
                    article.get('id'),
                    article.get('title'),
                    article.get('author'),
                    datetime.fromisoformat(article['published_utc'].replace('Z', '+00:00')) if 'published_utc' in article else None,
                    article.get('article_url'),
                    article.get('tickers', []),
                    article.get('description'),
                    article.get('keywords', [])
                ))
            
            extras.execute_batch(cur, """
                INSERT INTO news_articles 
                (id, title, author, published_utc, article_url, tickers, description, keywords)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, data, page_size=100)
            
            conn.commit()
            logger.debug(f"Wrote {len(data)} news articles")
            return len(data)
