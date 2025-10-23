"""
Schema management for TimescaleDB.

Handles database schema creation, migrations, and DDL operations.
"""
import logging

from psycopg2 import sql

logger = logging.getLogger(__name__)


class SchemaManager:
    """
    Manages TimescaleDB schema creation and migrations.
    
    Responsibilities:
    - Create TimescaleDB extension
    - Create and maintain hypertables for OHLCV data
    - Create reference data, corporate actions, news, and snapshot tables
    - Handle schema migrations (column additions, renames)
    - Create indexes for query optimization
    """
    
    # All supported timeframes
    TIMEFRAMES = ['1m', '5m', '15m', '30m', '1h', '2h', '4h', '12h', '1d', '1w', '1mo']
    
    def __init__(self, pool):
        """
        Initialize schema manager.
        
        Args:
            pool: PostgresConnectionPool instance
        """
        self.pool = pool
    
    def initialize_schema(self):
        """Initialize complete database schema."""
        with self.pool.get_connection() as conn:
            cur = conn.cursor()
            
            # Create TimescaleDB extension
            self._create_timescaledb_extension(cur)
            
            # Create OHLCV tables
            self._create_ohlcv_tables(cur)
            
            # Create reference data table
            self._create_reference_table(cur)
            
            # Create corporate actions table
            self._create_corporate_actions_table(cur)
            
            # Create news table
            self._create_news_table(cur)
            
            # Create snapshots table
            self._create_snapshots_table(cur)
            
            conn.commit()
            logger.info("Database schema initialized")
    
    def _create_timescaledb_extension(self, cur):
        """Create TimescaleDB extension if not exists."""
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        logger.debug("TimescaleDB extension created")
    
    def _create_ohlcv_tables(self, cur):
        """Create OHLCV hypertables for all timeframes."""
        for tf in self.TIMEFRAMES:
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
            
            # Ensure schema migrations
            self._ensure_symbol_column(cur, table_name)
            self._ensure_transactions_column(cur, table_name)
            
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
            
            logger.debug(f"Created OHLCV table: {table_name}")
    
    def _ensure_symbol_column(self, cur, table_name: str):
        """Ensure symbol column exists (migrate from legacy 'ticker' column)."""
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
              AND column_name = 'symbol'
            """,
            (table_name,),
        )
        if cur.fetchone():
            return
        
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
              AND column_name = 'ticker'
            """,
            (table_name,),
        )
        if cur.fetchone():
            logger.info("Renaming legacy ticker column to symbol on %s", table_name)
            cur.execute(
                sql.SQL("ALTER TABLE {} RENAME COLUMN ticker TO symbol").format(
                    sql.Identifier(table_name)
                )
            )
        else:
            logger.info("Adding missing symbol column to %s", table_name)
            cur.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN symbol TEXT").format(
                    sql.Identifier(table_name)
                )
            )
    
    def _ensure_transactions_column(self, cur, table_name: str):
        """Ensure transactions column exists."""
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
              AND column_name = 'transactions'
            """,
            (table_name,),
        )
        if not cur.fetchone():
            logger.info("Adding missing transactions column to %s", table_name)
            cur.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN transactions INTEGER").format(
                    sql.Identifier(table_name)
                )
            )
    
    def _create_reference_table(self, cur):
        """Create reference data table."""
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
        logger.debug("Created reference_data table")
    
    def _create_corporate_actions_table(self, cur):
        """Create corporate actions table."""
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
        
        # Ensure symbol column exists (migrate legacy ticker column)
        self._ensure_symbol_column(cur, 'corporate_actions')
        
        # Ensure all date columns exist
        for col in ['ex_dividend_date', 'record_date', 'pay_date', 'declaration_date']:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'corporate_actions'
                  AND column_name = %s
                """,
                (col,),
            )
            if not cur.fetchone():
                logger.info(f"Adding missing {col} column to corporate_actions")
                cur.execute(
                    sql.SQL("ALTER TABLE corporate_actions ADD COLUMN {} DATE").format(
                        sql.Identifier(col)
                    )
                )
        
        # Ensure numeric columns exist
        numeric_cols = {
            'cash_amount': 'DOUBLE PRECISION',
            'split_from': 'INTEGER',
            'split_to': 'INTEGER'
        }
        for col, col_type in numeric_cols.items():
            cur.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'corporate_actions'
                  AND column_name = %s
                """,
                (col,),
            )
            if not cur.fetchone():
                logger.info(f"Adding missing {col} column to corporate_actions")
                cur.execute(
                    sql.SQL("ALTER TABLE corporate_actions ADD COLUMN {} {}").format(
                        sql.Identifier(col),
                        sql.SQL(col_type)
                    )
                )
        
        # Create index
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_corporate_actions_symbol 
            ON corporate_actions (symbol, execution_date);
        """)
        logger.debug("Created corporate_actions table")
    
    def _create_news_table(self, cur):
        """Create news articles table."""
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
        
        # Create indexes
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_news_published 
            ON news_articles (published_utc DESC);
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_news_tickers 
            ON news_articles USING GIN (tickers);
        """)
        logger.debug("Created news_articles table")
    
    def _create_snapshots_table(self, cur):
        """Create market snapshots table."""
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots (
                symbol TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                last_trade_price DOUBLE PRECISION,
                last_trade_size BIGINT,
                last_trade_exchange TEXT,
                last_quote_bid DOUBLE PRECISION,
                last_quote_ask DOUBLE PRECISION,
                last_quote_bid_size BIGINT,
                last_quote_ask_size BIGINT,
                day_open DOUBLE PRECISION,
                day_high DOUBLE PRECISION,
                day_low DOUBLE PRECISION,
                day_close DOUBLE PRECISION,
                day_volume BIGINT,
                prev_close DOUBLE PRECISION,
                prev_volume BIGINT,
                todays_change DOUBLE PRECISION,
                todays_change_percent DOUBLE PRECISION,
                minute_vwap DOUBLE PRECISION,
                minute_volume BIGINT,
                raw_data JSONB,
                PRIMARY KEY (symbol, updated_at)
            );
        """)
        
        # Create index
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_snapshots_updated
            ON market_snapshots (updated_at DESC);
        """)
        logger.debug("Created market_snapshots table")
