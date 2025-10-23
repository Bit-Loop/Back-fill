"""
PostgreSQL connection pool manager for TimescaleDB.

Provides thread-safe connection pooling with proper resource management.
"""
import logging
from contextlib import contextmanager
from typing import Optional

import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)


class PostgresConnectionPool:
    """
    Thread-safe PostgreSQL connection pool.
    
    Features:
    - ThreadedConnectionPool for multi-threaded access
    - Automatic connection recycling
    - Context manager support for safe connection handling
    - Configurable pool size
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        min_conn: int = 1,
        max_conn: int = 20
    ):
        """
        Initialize connection pool.
        
        Args:
            host: Database host (default: localhost)
            port: Database port (default: 5432)
            database: Database name (default: trading)
            user: Database user (default: postgres)
            password: Database password
            min_conn: Minimum connections in pool
            max_conn: Maximum connections in pool
        """
        self.host = host or 'localhost'
        self.port = port or 5432
        self.database = database or 'trading'
        self.user = user or 'postgres'
        self.password = password
        self.min_conn = min_conn
        self.max_conn = max_conn
        
        # Create connection pool
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=min_conn,
                maxconn=max_conn,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info(f"Connection pool created: {host}:{port}/{database} (min={min_conn}, max={max_conn})")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a connection from the pool.
        
        Automatically commits on success, rolls back on error, and returns
        the connection to the pool.
        
        Yields:
            psycopg2 connection object
        
        Example:
            with pool.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM table")
        """
        conn = self.pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error in connection context: {e}")
            raise
        finally:
            self.pool.putconn(conn)
    
    def close(self):
        """Close all connections in the pool."""
        if hasattr(self, 'pool') and self.pool:
            self.pool.closeall()
            logger.info("Connection pool closed")
    
    def __enter__(self):
        """Support using pool as context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup on context manager exit."""
        self.close()
        return False
