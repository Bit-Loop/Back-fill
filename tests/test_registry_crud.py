"""Test registry CRUD operations."""
import sqlite3
from datetime import datetime, timedelta

import pytest

from scraper.registry.models import IngestionStrategy, IngestionStatus


class TestRegistryManager:
    """Test RegistryManager CRUD operations."""
    
    def setup_method(self):
        """Set up test database."""
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
    
    def teardown_method(self):
        """Clean up test database."""
        self.conn.close()
    
    def _init_schema(self):
        """Initialize test schema."""
        self.conn.execute("""
            CREATE TABLE symbol_registry (
                symbol TEXT PRIMARY KEY,
                strategy TEXT NOT NULL,
                timeframe TEXT DEFAULT '1m',
                enabled BOOLEAN DEFAULT TRUE,
                last_backfill TIMESTAMP,
                last_snapshot TIMESTAMP,
                last_stream TIMESTAMP,
                status TEXT DEFAULT 'idle',
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()
    
    def test_add_symbol(self):
        """Test adding symbol to registry."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, timeframe, enabled)
            VALUES (?, ?, ?, ?)
        """, ('AAPL', 'flatpack', '1m', True))
        self.conn.commit()
        
        cursor = self.conn.execute("SELECT * FROM symbol_registry WHERE symbol = ?", ('AAPL',))
        row = cursor.fetchone()
        
        assert row is not None
        assert row['symbol'] == 'AAPL'
        assert row['strategy'] == 'flatpack'
        assert row['timeframe'] == '1m'
        assert row['enabled'] == 1
        assert row['status'] == 'idle'
    
    def test_update_status(self):
        """Test updating symbol status."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, timeframe)
            VALUES (?, ?, ?)
        """, ('NVDA', 'snapshot', '1m'))
        self.conn.commit()
        
        self.conn.execute("""
            UPDATE symbol_registry
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE symbol = ?
        """, ('running', 'NVDA'))
        self.conn.commit()
        
        cursor = self.conn.execute("SELECT status FROM symbol_registry WHERE symbol = ?", ('NVDA',))
        row = cursor.fetchone()
        
        assert row['status'] == 'running'
    
    def test_update_timestamp_backfill(self):
        """Test updating last_backfill timestamp."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, timeframe)
            VALUES (?, ?, ?)
        """, ('SPY', 'flatpack', '1d'))
        self.conn.commit()
        
        now = datetime.utcnow().isoformat()
        self.conn.execute("""
            UPDATE symbol_registry
            SET last_backfill = ?, updated_at = CURRENT_TIMESTAMP
            WHERE symbol = ?
        """, (now, 'SPY'))
        self.conn.commit()
        
        cursor = self.conn.execute("SELECT last_backfill FROM symbol_registry WHERE symbol = ?", ('SPY',))
        row = cursor.fetchone()
        
        assert row['last_backfill'] is not None
    
    def test_update_timestamp_snapshot(self):
        """Test updating last_snapshot timestamp."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, timeframe)
            VALUES (?, ?, ?)
        """, ('TSLA', 'snapshot', '1m'))
        self.conn.commit()
        
        now = datetime.utcnow().isoformat()
        self.conn.execute("""
            UPDATE symbol_registry
            SET last_snapshot = ?, updated_at = CURRENT_TIMESTAMP
            WHERE symbol = ?
        """, (now, 'TSLA'))
        self.conn.commit()
        
        cursor = self.conn.execute("SELECT last_snapshot FROM symbol_registry WHERE symbol = ?", ('TSLA',))
        row = cursor.fetchone()
        
        assert row['last_snapshot'] is not None
    
    def test_list_enabled_symbols(self):
        """Test listing enabled symbols."""
        symbols = [
            ('AAPL', 'flatpack', True),
            ('NVDA', 'snapshot', True),
            ('AMD', 'stream', False),
            ('SPY', 'flatpack+snapshot+stream', True)
        ]
        
        for symbol, strategy, enabled in symbols:
            self.conn.execute("""
                INSERT INTO symbol_registry (symbol, strategy, enabled)
                VALUES (?, ?, ?)
            """, (symbol, strategy, enabled))
        self.conn.commit()
        
        cursor = self.conn.execute("SELECT symbol FROM symbol_registry WHERE enabled = 1")
        rows = cursor.fetchall()
        
        assert len(rows) == 3
        enabled_symbols = [row['symbol'] for row in rows]
        assert 'AAPL' in enabled_symbols
        assert 'NVDA' in enabled_symbols
        assert 'SPY' in enabled_symbols
        assert 'AMD' not in enabled_symbols
    
    def test_disable_symbol(self):
        """Test disabling symbol."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, enabled)
            VALUES (?, ?, ?)
        """, ('AAPL', 'flatpack', True))
        self.conn.commit()
        
        self.conn.execute("""
            UPDATE symbol_registry
            SET enabled = 0, updated_at = CURRENT_TIMESTAMP
            WHERE symbol = ?
        """, ('AAPL',))
        self.conn.commit()
        
        cursor = self.conn.execute("SELECT enabled FROM symbol_registry WHERE symbol = ?", ('AAPL',))
        row = cursor.fetchone()
        
        assert row['enabled'] == 0
    
    def test_set_error_status(self):
        """Test setting error status with message."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, timeframe)
            VALUES (?, ?, ?)
        """, ('FAIL', 'flatpack', '1m'))
        self.conn.commit()
        
        error_msg = "Connection timeout"
        self.conn.execute("""
            UPDATE symbol_registry
            SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
            WHERE symbol = ?
        """, ('error', error_msg, 'FAIL'))
        self.conn.commit()
        
        cursor = self.conn.execute("SELECT status, error_message FROM symbol_registry WHERE symbol = ?", ('FAIL',))
        row = cursor.fetchone()
        
        assert row['status'] == 'error'
        assert row['error_message'] == error_msg
    
    def test_clear_error_status(self):
        """Test clearing error status."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, status, error_message)
            VALUES (?, ?, ?, ?)
        """, ('RECOVER', 'flatpack', 'error', 'Previous error'))
        self.conn.commit()
        
        self.conn.execute("""
            UPDATE symbol_registry
            SET status = ?, error_message = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE symbol = ?
        """, ('idle', 'RECOVER'))
        self.conn.commit()
        
        cursor = self.conn.execute("SELECT status, error_message FROM symbol_registry WHERE symbol = ?", ('RECOVER',))
        row = cursor.fetchone()
        
        assert row['status'] == 'idle'
        assert row['error_message'] is None
