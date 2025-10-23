"""TimescaleDB Writer - Facade Pattern delegating to specialized components."""
import logging
from typing import Any, Dict, List, Optional

from .pool import PostgresConnectionPool
from .schema import SchemaManager
from .repositories.ohlcv import OHLCVRepository
from .repositories.reference import ReferenceDataRepository
from .repositories.corporate_actions import CorporateActionsRepository
from .repositories.news import NewsRepository
from .repositories.snapshots import SnapshotsRepository

logger = logging.getLogger(__name__)


class TimescaleWriter:
    """
    TimescaleDB writer facade - delegates to specialized components.
    
    This facade maintains backward compatibility while delegating to:
    - PostgresConnectionPool: Connection management
    - SchemaManager: DDL operations
    - Repository classes: Data access
    
    Features:
    - Connection pooling
    - Batch inserts for performance
    - TimescaleDB-specific optimizations (hypertables, continuous aggregates)
    - Transaction safety with rollback on error
    - Automatic schema creation
    """
    
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 database: Optional[str] = None, user: Optional[str] = None,
                 password: Optional[str] = None, min_conn: int = 1, max_conn: int = 20):
        """
        Initialize TimescaleDB writer.
        
        Args:
            host: Database host (default: localhost)
            port: Database port (default: 5432)
            database: Database name (default: trading)
            user: Database user (default: postgres)
            password: Database password
            min_conn: Minimum connections in pool
            max_conn: Maximum connections in pool
        """
        # Create connection pool
        try:
            self.pool = PostgresConnectionPool(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                min_conn=min_conn,
                max_conn=max_conn
            )
            logger.info(f"Connected to TimescaleDB: {host or 'localhost'}:{port or 5432}/{database or 'trading'}")
            
            # Initialize components
            self.schema_manager = SchemaManager(self.pool)
            self.ohlcv_repo = OHLCVRepository(self.pool)
            self.reference_repo = ReferenceDataRepository(self.pool)
            self.corporate_actions_repo = CorporateActionsRepository(self.pool)
            self.news_repo = NewsRepository(self.pool)
            self.snapshots_repo = SnapshotsRepository(self.pool)
            
            # Initialize schema
            self.schema_manager.initialize_schema()
            
        except Exception as e:
            logger.error(f"Failed to initialize TimescaleWriter: {e}")
            raise
    
    def get_connection(self):
        """Context manager for getting a connection from the pool."""
        return self.pool.get_connection()
    
    def write_ohlcv_bars(self, symbol: str, bars: List[Dict], timeframe: str = "1m") -> int:
        """Write OHLCV bars to database."""
        return self.ohlcv_repo.write_bars(symbol, bars, timeframe)
    
    def write_reference_data(self, symbol: str, details: Dict[str, Any]) -> bool:
        """Write ticker reference data to database."""
        return self.reference_repo.write_reference(symbol, details)
    
    def write_corporate_actions(self, symbol: str, actions: List[Dict], action_type: str) -> int:
        """Write corporate actions to database."""
        return self.corporate_actions_repo.write_actions(symbol, actions, action_type)
    
    def write_news(self, articles: List[Dict]) -> int:
        """Write news articles to database."""
        return self.news_repo.write_articles(articles)
    
    def write_snapshots(self, snapshots: List[Dict]) -> int:
        """Write snapshot payloads to database."""
        return self.snapshots_repo.write_snapshots(snapshots)
    
    def close(self):
        """Close all connections in the pool."""
        self.pool.close()
