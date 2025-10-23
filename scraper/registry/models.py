"""Data models for symbol registry."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class IngestionStrategy(str, Enum):
    """Available ingestion strategies per symbol."""

    FLATPACK = "flatpack"
    SNAPSHOT = "snapshot"
    STREAM = "stream"
    FLATPACK_API = "flatpack+api"
    SNAPSHOT_STREAM = "snapshot+stream"
    FLATPACK_SNAPSHOT_STREAM = "flatpack+snapshot+stream"


class IngestionStatus(str, Enum):
    """Current ingestion status."""

    IDLE = "idle"
    RUNNING = "running"
    ERROR = "error"
    PAUSED = "paused"
    COMPLETED = "completed"


@dataclass
class SymbolRegistry:
    """Registry entry for a symbol's ingestion configuration."""

    symbol: str
    strategy: IngestionStrategy
    timeframe: str = "1m"
    enabled: bool = True
    last_backfill: Optional[datetime] = None
    last_snapshot: Optional[datetime] = None
    last_stream: Optional[datetime] = None
    status: IngestionStatus = IngestionStatus.IDLE
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "symbol": self.symbol,
            "strategy": self.strategy.value,
            "timeframe": self.timeframe,
            "enabled": self.enabled,
            "last_backfill": self.last_backfill.isoformat() if self.last_backfill else None,
            "last_snapshot": self.last_snapshot.isoformat() if self.last_snapshot else None,
            "last_stream": self.last_stream.isoformat() if self.last_stream else None,
            "status": self.status.value,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_db_row(cls, row: tuple) -> SymbolRegistry:
        """Create instance from database row."""
        return cls(
            symbol=row[0],
            strategy=IngestionStrategy(row[1]),
            timeframe=row[2],
            enabled=row[3],
            last_backfill=row[4],
            last_snapshot=row[5],
            last_stream=row[6],
            status=IngestionStatus(row[7]),
            error_message=row[8],
            created_at=row[9],
            updated_at=row[10],
        )
