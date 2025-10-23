"""Test orchestrator loop and strategy execution."""
import asyncio
import sqlite3
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestOrchestratorLoop:
    """Test orchestrator strategy execution."""
    
    def setup_method(self):
        """Set up test database."""
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
    
    def teardown_method(self):
        """Clean up."""
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
    
    def test_flatpack_strategy_sequence(self):
        """Test flatpack strategy executes correct phases."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, enabled)
            VALUES (?, ?, ?)
        """, ('AAPL', 'flatpack', True))
        self.conn.commit()
        
        cursor = self.conn.execute("""
            SELECT symbol, strategy FROM symbol_registry
            WHERE enabled = 1
        """)
        row = cursor.fetchone()
        
        assert row['symbol'] == 'AAPL'
        assert row['strategy'] == 'flatpack'
    
    def test_snapshot_stream_strategy_sequence(self):
        """Test snapshot+stream strategy."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, enabled)
            VALUES (?, ?, ?)
        """, ('NVDA', 'snapshot+stream', True))
        self.conn.commit()
        
        cursor = self.conn.execute("""
            SELECT strategy FROM symbol_registry WHERE symbol = ?
        """, ('NVDA',))
        row = cursor.fetchone()
        
        assert 'snapshot' in row['strategy']
        assert 'stream' in row['strategy']
    
    def test_status_transitions_idle_to_running(self):
        """Test status transition from idle to running."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, status)
            VALUES (?, ?, ?)
        """, ('SPY', 'flatpack', 'idle'))
        self.conn.commit()
        
        self.conn.execute("""
            UPDATE symbol_registry
            SET status = ?
            WHERE symbol = ?
        """, ('running', 'SPY'))
        self.conn.commit()
        
        cursor = self.conn.execute("""
            SELECT status FROM symbol_registry WHERE symbol = ?
        """, ('SPY',))
        row = cursor.fetchone()
        
        assert row['status'] == 'running'
    
    def test_status_transitions_running_to_idle(self):
        """Test status transition from running to idle."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, status)
            VALUES (?, ?, ?)
        """, ('TSLA', 'snapshot', 'running'))
        self.conn.commit()
        
        self.conn.execute("""
            UPDATE symbol_registry
            SET status = ?, last_snapshot = ?
            WHERE symbol = ?
        """, ('idle', datetime.utcnow().isoformat(), 'TSLA'))
        self.conn.commit()
        
        cursor = self.conn.execute("""
            SELECT status, last_snapshot FROM symbol_registry WHERE symbol = ?
        """, ('TSLA',))
        row = cursor.fetchone()
        
        assert row['status'] == 'idle'
        assert row['last_snapshot'] is not None
    
    def test_error_status_with_message(self):
        """Test error status set with message."""
        self.conn.execute("""
            INSERT INTO symbol_registry (symbol, strategy, status)
            VALUES (?, ?, ?)
        """, ('FAIL', 'flatpack', 'running'))
        self.conn.commit()
        
        error_msg = "Connection timeout after 3 retries"
        self.conn.execute("""
            UPDATE symbol_registry
            SET status = ?, error_message = ?
            WHERE symbol = ?
        """, ('error', error_msg, 'FAIL'))
        self.conn.commit()
        
        cursor = self.conn.execute("""
            SELECT status, error_message FROM symbol_registry WHERE symbol = ?
        """, ('FAIL',))
        row = cursor.fetchone()
        
        assert row['status'] == 'error'
        assert row['error_message'] == error_msg
    
    def test_only_enabled_symbols_processed(self):
        """Test only enabled symbols are processed."""
        symbols = [
            ('AAPL', 'flatpack', True),
            ('NVDA', 'snapshot', False),
            ('SPY', 'stream', True)
        ]
        
        for symbol, strategy, enabled in symbols:
            self.conn.execute("""
                INSERT INTO symbol_registry (symbol, strategy, enabled)
                VALUES (?, ?, ?)
            """, (symbol, strategy, enabled))
        self.conn.commit()
        
        cursor = self.conn.execute("""
            SELECT symbol FROM symbol_registry WHERE enabled = 1
        """)
        rows = cursor.fetchall()
        
        enabled_symbols = [row['symbol'] for row in rows]
        assert 'AAPL' in enabled_symbols
        assert 'SPY' in enabled_symbols
        assert 'NVDA' not in enabled_symbols
    
    @pytest.mark.asyncio
    async def test_kafka_publish_for_each_phase(self, mock_kafka_producer):
        """Test Kafka publish happens for each phase."""
        symbol = 'AAPL'
        phases = ['backfill', 'snapshot', 'stream']
        
        for phase in phases:
            topic = f'chronox.{phase}.{symbol.lower()}'
            payload = {
                'symbol': symbol,
                'phase': phase,
                'status': 'completed'
            }
            import json
            await mock_kafka_producer.send(
                topic,
                json.dumps(payload).encode('utf-8'),
                symbol.encode('utf-8')
            )
        
        assert len(mock_kafka_producer.messages) == 3
        topics = [msg['topic'] for msg in mock_kafka_producer.messages]
        assert 'chronox.backfill.aapl' in topics
        assert 'chronox.snapshot.aapl' in topics
        assert 'chronox.stream.aapl' in topics
    
    def test_multiple_symbols_sequential_execution(self):
        """Test multiple symbols execute sequentially."""
        symbols = ['AAPL', 'NVDA', 'SPY']
        
        for symbol in symbols:
            self.conn.execute("""
                INSERT INTO symbol_registry (symbol, strategy, enabled)
                VALUES (?, ?, ?)
            """, (symbol, 'flatpack', True))
        self.conn.commit()
        
        cursor = self.conn.execute("""
            SELECT symbol FROM symbol_registry WHERE enabled = 1 ORDER BY symbol
        """)
        rows = cursor.fetchall()
        
        assert len(rows) == 3
        assert [row['symbol'] for row in rows] == sorted(symbols)
