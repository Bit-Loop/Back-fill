"""Registry manager for symbol ingestion state."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from storage.timescale.writer import TimescaleWriter

from .models import IngestionStatus, IngestionStrategy, SymbolRegistry

logger = logging.getLogger(__name__)


class RegistryManager:
    """Manages symbol registry CRUD operations."""

    def __init__(self, db_writer: TimescaleWriter):
        self.db_writer = db_writer
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create symbol_registry table if not exists."""
        with self.db_writer.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS symbol_registry (
                    symbol TEXT PRIMARY KEY,
                    strategy TEXT NOT NULL,
                    timeframe TEXT DEFAULT '1m',
                    enabled BOOLEAN DEFAULT TRUE,
                    last_backfill TIMESTAMPTZ,
                    last_snapshot TIMESTAMPTZ,
                    last_stream TIMESTAMPTZ,
                    status TEXT DEFAULT 'idle',
                    error_message TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_symbol_registry_enabled
                ON symbol_registry (enabled, status);
            """)
            conn.commit()
            logger.info("Symbol registry schema initialized")

    def add_symbol(
        self,
        symbol: str,
        strategy: IngestionStrategy,
        timeframe: str = "1m",
        enabled: bool = True,
    ) -> SymbolRegistry:
        """Add or update symbol in registry."""
        with self.db_writer.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO symbol_registry (symbol, strategy, timeframe, enabled, created_at, updated_at)
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    strategy = EXCLUDED.strategy,
                    timeframe = EXCLUDED.timeframe,
                    enabled = EXCLUDED.enabled,
                    updated_at = NOW()
                RETURNING symbol, strategy, timeframe, enabled, last_backfill, last_snapshot, 
                          last_stream, status, error_message, created_at, updated_at
            """,
                (symbol, strategy.value, timeframe, enabled),
            )
            row = cur.fetchone()
            conn.commit()
            logger.info("Added/updated symbol %s with strategy %s", symbol, strategy.value)
            return SymbolRegistry.from_db_row(row)

    def get_symbol(self, symbol: str) -> Optional[SymbolRegistry]:
        """Get registry entry for symbol."""
        with self.db_writer.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT symbol, strategy, timeframe, enabled, last_backfill, last_snapshot,
                       last_stream, status, error_message, created_at, updated_at
                FROM symbol_registry
                WHERE symbol = %s
            """,
                (symbol,),
            )
            row = cur.fetchone()
            return SymbolRegistry.from_db_row(row) if row else None

    def list_symbols(self, enabled_only: bool = False) -> List[SymbolRegistry]:
        """List all symbols in registry."""
        with self.db_writer.get_connection() as conn:
            cur = conn.cursor()
            query = """
                SELECT symbol, strategy, timeframe, enabled, last_backfill, last_snapshot,
                       last_stream, status, error_message, created_at, updated_at
                FROM symbol_registry
            """
            if enabled_only:
                query += " WHERE enabled = TRUE"
            query += " ORDER BY symbol"

            cur.execute(query)
            return [SymbolRegistry.from_db_row(row) for row in cur.fetchall()]

    def update_status(
        self,
        symbol: str,
        status: IngestionStatus,
        error_message: Optional[str] = None,
    ) -> None:
        """Update symbol status."""
        with self.db_writer.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE symbol_registry
                SET status = %s, error_message = %s, updated_at = NOW()
                WHERE symbol = %s
            """,
                (status.value, error_message, symbol),
            )
            conn.commit()
            logger.debug("Updated %s status to %s", symbol, status.value)

    def update_timestamp(
        self,
        symbol: str,
        backfill: bool = False,
        snapshot: bool = False,
        stream: bool = False,
    ) -> None:
        """Update last run timestamps."""
        updates = []
        if backfill:
            updates.append("last_backfill = NOW()")
        if snapshot:
            updates.append("last_snapshot = NOW()")
        if stream:
            updates.append("last_stream = NOW()")

        if not updates:
            return

        with self.db_writer.get_connection() as conn:
            cur = conn.cursor()
            query = f"""
                UPDATE symbol_registry
                SET {', '.join(updates)}, updated_at = NOW()
                WHERE symbol = %s
            """
            cur.execute(query, (symbol,))
            conn.commit()

    def disable_symbol(self, symbol: str) -> bool:
        """Disable symbol ingestion."""
        with self.db_writer.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE symbol_registry
                SET enabled = FALSE, updated_at = NOW()
                WHERE symbol = %s
            """,
                (symbol,),
            )
            affected = cur.rowcount
            conn.commit()
            if affected:
                logger.info("Disabled symbol %s", symbol)
            return affected > 0

    def delete_symbol(self, symbol: str) -> bool:
        """Remove symbol from registry."""
        with self.db_writer.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM symbol_registry WHERE symbol = %s", (symbol,))
            affected = cur.rowcount
            conn.commit()
            if affected:
                logger.info("Deleted symbol %s from registry", symbol)
            return affected > 0
