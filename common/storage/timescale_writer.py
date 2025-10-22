"""
TimescaleDB Writer
Handles writing market data to TimescaleDB (PostgreSQL + TimescaleDB extension).
"""
import psycopg2
from psycopg2 import pool, extras
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class TimescaleWriter:
    """
    TimescaleDB writer with connection pooling and batch insert optimization.
    
    Features:
    - Connection pooling (psycopg2 ThreadedConnectionPool)
    - Batch inserts for performance
    - TimescaleDB-specific optimizations (hypertables, continuous aggregates)
    - Transaction safety with rollback on error
    - Automatic schema creation
    """
    
    def __init__(self, host: str = 'localhost', port: int = 5432,
                 database: str = 'trading', user: str = 'postgres',
                 password: Optional[str] = None, min_conn: int = 1, max_conn: int = 20):
        """
        Initialize TimescaleDB writer.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            min_conn: Minimum connections in pool
            max_conn: Maximum connections in pool
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
        # Create connection pool
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=min_conn,
                maxconn=max_conn,
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            logger.info(f"Connected to TimescaleDB: {host}:{port}/{database}")
            
            # Initialize schema
            self._initialize_schema()
            
        except Exception as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a connection from the pool.
        
        Yields:
            psycopg2 connection object
        """
        conn = self.pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            self.pool.putconn(conn)
    
    def _initialize_schema(self):
        """Initialize database schema with hypertables"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            # Create TimescaleDB extension if not exists
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            
            # Create OHLCV tables for different timeframes
            timeframes = ['1m', '5m', '15m', '30m', '1h', '2h', '4h', '12h', '1d', '1w', '1mo']
            
            for tf in timeframes:
                table_name = f'market_data_{tf}'
                
                # Create table
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        time TIMESTAMPTZ NOT NULL,
                        symbol TEXT NOT NULL,
                        open DOUBLE PRECISION,
                        high DOUBLE PRECISION,
                        low DOUBLE PRECISION,
                        close DOUBLE PRECISION,
                        volume BIGINT,
                        vwap DOUBLE PRECISION,
                        transactions INTEGER,
                        PRIMARY KEY (time, symbol)
                    );
                """)
                
                # Convert to hypertable
                cur.execute(f"""
                    SELECT create_hypertable('{table_name}', 'time', 
                                             chunk_time_interval => INTERVAL '1 day',
                                             if_not_exists => TRUE);
                """)
                
                # Create index on symbol for faster queries
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol 
                    ON {table_name} (symbol, time DESC);
                """)
            
            # Create reference data table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS reference_data (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    market TEXT,
                    locale TEXT,
                    primary_exchange TEXT,
                    type TEXT,
                    active BOOLEAN,
                    currency_name TEXT,
                    cik TEXT,
                    composite_figi TEXT,
                    share_class_figi TEXT,
                    market_cap BIGINT,
                    phone_number TEXT,
                    address TEXT,
                    description TEXT,
                    sic_code TEXT,
                    employees INTEGER,
                    list_date DATE,
                    logo_url TEXT,
                    icon_url TEXT,
                    homepage_url TEXT,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            
            # Create corporate actions table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS corporate_actions (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    execution_date DATE NOT NULL,
                    ex_dividend_date DATE,
                    record_date DATE,
                    pay_date DATE,
                    cash_amount DOUBLE PRECISION,
                    split_from INTEGER,
                    split_to INTEGER,
                    declaration_date DATE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_corporate_actions_symbol 
                ON corporate_actions (symbol, execution_date);
            """)
            
            # Create news table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS news_articles (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    author TEXT,
                    published_utc TIMESTAMPTZ,
                    article_url TEXT,
                    tickers TEXT[],
                    description TEXT,
                    keywords TEXT[],
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_news_published 
                ON news_articles (published_utc DESC);
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_news_tickers 
                ON news_articles USING GIN (tickers);
            """)
            
            conn.commit()
            logger.info("Database schema initialized")
    
    def write_ohlcv_bars(self, symbol: str, bars: List[Dict], timeframe: str = "1m") -> int:
        """
        Write OHLCV bars to database.
        
        Args:
            symbol: Ticker symbol
            bars: List of bar dictionaries with keys: t, o, h, l, c, v, vw, n
            timeframe: Timeframe (1m, 5m, 15m, 30m, 1h, 2h, 4h, 12h, 1d, 1w, 1mo)
        
        Returns:
            Number of bars written
        """
        if not bars:
            return 0
        
        # Map timeframe to table name
        table_map = {
            '1min': '1m', '1minute': '1m', '1m': '1m',
            '5min': '5m', '5minute': '5m', '5m': '5m',
            '15min': '15m', '15minute': '15m', '15m': '15m',
            '30min': '30m', '30minute': '30m', '30m': '30m',
            '1hour': '1h', '1h': '1h',
            '2hour': '2h', '2h': '2h',
            '4hour': '4h', '4h': '4h',
            '12hour': '12h', '12h': '12h',
            '1day': '1d', '1d': '1d',
            '1week': '1w', '1w': '1w',
            '1month': '1mo', '1mo': '1mo'
        }
        
        tf = table_map.get(timeframe, '1m')
        table_name = f'market_data_{tf}'
        
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            # Prepare data for batch insert
            data = []
            for bar in bars:
                timestamp = datetime.fromtimestamp(bar.get('t', bar.get('timestamp', 0)) / 1000.0)
                data.append((
                    timestamp,
                    symbol,
                    bar.get('o', bar.get('open')),
                    bar.get('h', bar.get('high')),
                    bar.get('l', bar.get('low')),
                    bar.get('c', bar.get('close')),
                    bar.get('v', bar.get('volume')),
                    bar.get('vw', bar.get('vwap')),
                    bar.get('n', bar.get('transactions'))
                ))
            
            # Batch insert with ON CONFLICT DO NOTHING (idempotent)
            extras.execute_batch(cur, f"""
                INSERT INTO {table_name} 
                (time, symbol, open, high, low, close, volume, vwap, transactions)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, symbol) DO NOTHING
            """, data, page_size=1000)
            
            conn.commit()
            logger.debug(f"Wrote {len(data)} bars for {symbol} ({timeframe})")
            return len(data)
    
    def write_reference_data(self, symbol: str, details: Dict[str, Any]) -> bool:
        """
        Write ticker reference data to database.
        
        Args:
            symbol: Ticker symbol
            details: Dictionary with ticker details
        
        Returns:
            True if successful
        """
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO reference_data 
                (symbol, name, market, locale, primary_exchange, type, active,
                 currency_name, cik, composite_figi, share_class_figi, market_cap,
                 phone_number, address, description, sic_code, employees, list_date,
                 logo_url, icon_url, homepage_url, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name,
                    market = EXCLUDED.market,
                    locale = EXCLUDED.locale,
                    primary_exchange = EXCLUDED.primary_exchange,
                    type = EXCLUDED.type,
                    active = EXCLUDED.active,
                    currency_name = EXCLUDED.currency_name,
                    cik = EXCLUDED.cik,
                    composite_figi = EXCLUDED.composite_figi,
                    share_class_figi = EXCLUDED.share_class_figi,
                    market_cap = EXCLUDED.market_cap,
                    phone_number = EXCLUDED.phone_number,
                    address = EXCLUDED.address,
                    description = EXCLUDED.description,
                    sic_code = EXCLUDED.sic_code,
                    employees = EXCLUDED.employees,
                    list_date = EXCLUDED.list_date,
                    logo_url = EXCLUDED.logo_url,
                    icon_url = EXCLUDED.icon_url,
                    homepage_url = EXCLUDED.homepage_url,
                    updated_at = NOW()
            """, (
                symbol,
                details.get('name'),
                details.get('market'),
                details.get('locale'),
                details.get('primary_exchange'),
                details.get('type'),
                details.get('active'),
                details.get('currency_name'),
                details.get('cik'),
                details.get('composite_figi'),
                details.get('share_class_figi'),
                details.get('market_cap'),
                details.get('phone_number'),
                details.get('address', {}).get('address1') if isinstance(details.get('address'), dict) else None,
                details.get('description'),
                details.get('sic_code'),
                details.get('total_employees'),
                details.get('list_date'),
                details.get('branding', {}).get('logo_url') if isinstance(details.get('branding'), dict) else None,
                details.get('branding', {}).get('icon_url') if isinstance(details.get('branding'), dict) else None,
                details.get('homepage_url')
            ))
            
            conn.commit()
            return True
    
    def write_corporate_actions(self, symbol: str, actions: List[Dict], action_type: str) -> int:
        """
        Write corporate actions to database.
        
        Args:
            symbol: Ticker symbol
            actions: List of action dictionaries
            action_type: 'dividend' or 'split'
        
        Returns:
            Number of actions written
        """
        if not actions:
            return 0
        
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            data = []
            for action in actions:
                if action_type == 'dividend':
                    data.append((
                        symbol,
                        'dividend',
                        action.get('ex_dividend_date'),
                        action.get('ex_dividend_date'),
                        action.get('record_date'),
                        action.get('pay_date'),
                        action.get('cash_amount'),
                        None,
                        None,
                        action.get('declaration_date')
                    ))
                else:  # split
                    data.append((
                        symbol,
                        'split',
                        action.get('execution_date'),
                        action.get('execution_date'),
                        None,
                        None,
                        None,
                        action.get('split_from'),
                        action.get('split_to'),
                        None
                    ))
            
            extras.execute_batch(cur, """
                INSERT INTO corporate_actions 
                (symbol, action_type, execution_date, ex_dividend_date, record_date,
                 pay_date, cash_amount, split_from, split_to, declaration_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, data, page_size=100)
            
            conn.commit()
            logger.debug(f"Wrote {len(data)} {action_type} actions for {symbol}")
            return len(data)
    
    def write_news(self, articles: List[Dict]) -> int:
        """
        Write news articles to database.
        
        Args:
            articles: List of article dictionaries
        
        Returns:
            Number of articles written
        """
        if not articles:
            return 0
        
        with self.get_connection() as conn:
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
    
    def close(self):
        """Close all connections in the pool"""
        if hasattr(self, 'pool'):
            self.pool.closeall()
            logger.info("Closed TimescaleDB connection pool")
