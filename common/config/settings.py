"""
Configuration settings for the backfill system.
Centralizes all configurable parameters for scraper and GUI.
"""
import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class HTTPConfig:
    """HTTP connection pool configuration"""
    max_connections: int = 800
    pool_block: bool = True
    max_retries: int = 3
    timeout: int = 30


@dataclass
class SystemConfig:
    """System resource configuration"""
    max_workers: int = 100
    max_download_threads: int = 24
    max_process_threads: int = 32
    download_queue_size: int = 300
    process_queue_size: int = 500
    batch_size: int = 100
    batch_timeout_seconds: float = 1.5


@dataclass
class S3Config:
    """S3/Flat files configuration"""
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    endpoint: str = "https://files.polygon.io"
    bucket: str = "flatfiles"
    max_retries: int = 5
    base_backoff: float = 2.0
    
    def __post_init__(self):
        if self.access_key is None:
            self.access_key = os.getenv('POLYGON_S3_ACCESS_KEY')
        if self.secret_key is None:
            self.secret_key = os.getenv('POLYGON_S3_SECRET_KEY')


@dataclass
class RedisConfig:
    """Redis configuration for real-time updates"""
    host: str = 'localhost'
    port: int = 6380
    db: int = 0
    password: Optional[str] = None
    
    def __post_init__(self):
        if self.password is None:
            self.password = os.getenv('REDIS_PASSWORD')


@dataclass
class DatabaseConfig:
    """Database configuration (TimescaleDB)"""
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    
    def __post_init__(self):
        self.host = self.host or os.getenv('DB_HOST', 'localhost')
        self.port = self.port or int(os.getenv('DB_PORT', '5432'))
        self.database = self.database or os.getenv('DB_NAME', 'trading')
        self.user = self.user or os.getenv('DB_USER', 'postgres')
        self.password = self.password or os.getenv('DB_PASSWORD')


@dataclass
class PolygonConfig:
    """Polygon.io API configuration"""
    api_key: Optional[str] = None
    
    def __post_init__(self):
        if self.api_key is None:
            self.api_key = os.getenv('POLYGON_API_KEY')


@dataclass
class BackfillConfig:
    """Complete backfill system configuration"""
    http: HTTPConfig
    system: SystemConfig
    s3: S3Config
    redis: RedisConfig
    database: DatabaseConfig
    polygon: PolygonConfig
    debug: bool = False
    
    @classmethod
    def default(cls):
        """Create default configuration"""
        return cls(
            http=HTTPConfig(),
            system=SystemConfig(),
            s3=S3Config(),
            redis=RedisConfig(),
            database=DatabaseConfig(),
            polygon=PolygonConfig(),
            debug=os.getenv('DEBUG_CPU', 'false').lower() == 'true'
        )


# Timeframe definitions
TIMEFRAME_CONFIGS = {
    '1s': 1,
    '1m': 60,
    '5m': 300,
    '15m': 900,
    '30m': 1800,
    '1h': 3600,
    '2h': 7200,
    '4h': 14400,
    '12h': 43200,
    '1d': 86400,
    '1w': 604800,
    '1mo': 2592000,  # Approximate
}

# Table name mapping
TABLE_MAP = {
    '1m': 'market_data_1m',
    '5m': 'market_data_5m',
    '15m': 'market_data_15m',
    '30m': 'market_data_30m',
    '1h': 'market_data_1h',
    '2h': 'market_data_2h',
    '4h': 'market_data_4h',
    '12h': 'market_data_12h',
    '1d': 'market_data_1d',
    '1w': 'market_data_1w',
    '1mo': 'market_data_1mo',
    '1y': 'market_data_1y'
}
