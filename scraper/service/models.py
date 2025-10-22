"""Pydantic and dataclass models for the scraper daemon service."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional


class JobState(str, Enum):
    """Lifecycle states for a backfill job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class BackfillRequest:
    """Configuration payload for starting a backfill."""

    tickers: List[str]
    years: int
    use_flatfiles: bool = False
    debug: bool = False
    skip_reference: bool = False
    skip_corporate: bool = False
    skip_daily: bool = False
    skip_minute: bool = False
    skip_news: bool = False


@dataclass
class StreamRequest:
    """Configuration for initiating a live stream."""

    tickers: List[str]
    timeframe: str = "1m"


@dataclass
class JobStatus:
    """Runtime state for a submitted job."""

    job_id: str
    request: BackfillRequest
    state: JobState = JobState.PENDING
    submitted_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    message: Optional[str] = None
    result: Optional[Dict] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict:
        """Serialize status to a JSON-friendly dictionary."""

        return {
            "job_id": self.job_id,
            "tickers": self.request.tickers,
            "state": self.state.value,
            "submitted_at": self.submitted_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "message": self.message,
            "result": self.result,
            "error": self.error,
        }


@dataclass
class StreamStatus:
    """Represents the lifecycle state of the streaming subsystem."""

    active: bool
    tickers: List[str] = field(default_factory=list)
    timeframe: str = "1m"
    started_at: Optional[datetime] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "active": self.active,
            "tickers": self.tickers,
            "timeframe": self.timeframe,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "error": self.error,
        }
