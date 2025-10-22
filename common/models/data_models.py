"""
Data models for the backfill system.
"""
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict
from datetime import datetime
from pathlib import Path
import threading


class FileState(Enum):
    """File download and processing state machine"""
    WAITING = "WAITING"
    DOWNLOADING = "DOWNLOADING"
    RETRYING = "RETRYING"
    DOWNLOADED = "DOWNLOADED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class FileTask:
    """
    Task object for file download/processing pipeline.
    Thread-safe state management.
    """
    
    def __init__(self, file_id: str, s3_key: str, date_str: str, filename: str):
        self.id = file_id
        self.s3_key = s3_key
        self.date = date_str
        self.filename = filename
        self.state = FileState.WAITING
        self.attempts = 0
        self.local_path: Optional[Path] = None
        self.error: Optional[str] = None
        self._lock = threading.Lock()
    
    def set_state(self, new_state: FileState, error: Optional[str] = None):
        """Thread-safe state transition"""
        with self._lock:
            self.state = new_state
            if error:
                self.error = error
    
    def get_state(self) -> FileState:
        """Thread-safe state getter"""
        with self._lock:
            return self.state
    
    def increment_attempts(self) -> int:
        """Thread-safe attempt counter increment"""
        with self._lock:
            self.attempts += 1
            return self.attempts


@dataclass
class OHLCVBar:
    """OHLCV bar data model"""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    timeframe: str
    vwap: Optional[float] = None
    trades: Optional[int] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'timeframe': self.timeframe,
            'vwap': self.vwap,
            'trades': self.trades
        }


@dataclass
class EnrichedBar(OHLCVBar):
    """OHLCV bar enriched with technical indicators"""
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    ema_20: Optional[float] = None
    ema_50: Optional[float] = None
    ema_100: Optional[float] = None
    ema_200: Optional[float] = None
    rsi_14: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    vwap: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_hist: Optional[float] = None


@dataclass
class CorporateAction:
    """Corporate action (dividend or split) data model"""
    ticker: str
    date: str
    action_type: str  # 'dividend' or 'split'
    amount: float
    subtype: Optional[str] = None


@dataclass
class ReferenceData:
    """Ticker reference data model"""
    ticker: str
    name: str
    market: str
    locale: str
    primary_exchange: Optional[str] = None
    type: Optional[str] = None
    active: bool = True
    currency_name: Optional[str] = None
    cik: Optional[str] = None
    composite_figi: Optional[str] = None
    market_cap: Optional[float] = None


@dataclass
class NewsArticle:
    """News article data model"""
    id: str
    ticker: str
    title: str
    author: Optional[str]
    published_utc: datetime
    article_url: str
    tickers: list
    amp_url: Optional[str] = None
    image_url: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[list] = None
