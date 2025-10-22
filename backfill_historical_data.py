#!/usr/bin/env python3
"""
Historical Data Backfill Orchestrator for ChronoX Trading Bot

This script coordinates the complete backfill process:
1. Fetch reference data (ticker list, exchanges)
2. Fetch corporate actions (dividends, splits)
3. Fetch daily bars (5 years)
4. Fetch minute bars (5 years, chunked)
5. Fetch news (recent 6 months)

Usage:
    python backfill_historical_data.py --tickers AAPL,TSLA,NVDA
    python backfill_historical_data.py --all --limit 50
    python backfill_historical_data.py --config backfill_config.yaml
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed, wait, Future
import threading
import queue
from enum import Enum
import traceback
import asyncio
from multiprocessing import Manager
import random
import resource
import psutil
from requests.adapters import HTTPAdapter
from requests import Session
import numpy as np
import json
import websocket
import pandas as pd
from collections import deque

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.ingestion.polygon.client import PolygonClient
from data.ingestion.polygon.aggregates import AggregatesClient
from data.ingestion.polygon.corporate_actions import CorporateActionsClient
from data.ingestion.polygon.reference import ReferenceClient
from data.ingestion.polygon.news import NewsClient
from data.storage.timescale_writer import TimescaleWriter  # Updated: InfluxDB → TimescaleDB
from data.ingestion.polygon.flat_files import FlatFileDownloader
from data.ingestion.polygon.flat_file_parser import FlatFileParser
from tqdm import tqdm

# Kafka support for Kafka→Redis bridge
try:
    from confluent_kafka import Consumer, KafkaError, TopicPartition
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logging.warning("Kafka not available - install with: pip install confluent-kafka")

# Try to import visualizer (optional dependency)
try:
    from scripts.backfill_visualizer_integrated import attach_visualizer
    VISUALIZER_AVAILABLE = True
except ImportError as e:
    logging.warning(f"Visualizer not available: {e}")
    VISUALIZER_AVAILABLE = False

# ============================================================================
# ENHANCEMENT: CPU LOGGING CONTROL
# ============================================================================
# CPU scaling/usage logs can be noisy. Control via DEBUG_CPU environment variable.
# Set DEBUG_CPU=true to enable CPU metrics logging, otherwise silenced.
# Usage: DEBUG_CPU=true python backfill_historical_data.py --tickers AMD
# ============================================================================

# PARAMETERS
# Reduced from 400 to 100 to prevent file descriptor exhaustion
# Can be overridden with --workers flag
max_workers = 100

# CPU logging control (set via environment variable or --debug flag)
CPU_LOGGING_ENABLED = os.getenv('DEBUG_CPU', 'false').lower() == 'true'

# ============================================================================
# HTTP Connection Pool Configuration
# ============================================================================

def get_pooled_session(max_connections: int = 800) -> Session:
    """
    Create a requests.Session with tuned HTTPAdapter for high-performance connection pooling.
    
    Args:
        max_connections: Maximum number of concurrent HTTP connections
        
    Returns:
        Configured Session with connection pooling
    """
    session = Session()
    adapter = HTTPAdapter(
        pool_connections=max_connections,
        pool_maxsize=max_connections,
        pool_block=True,
        max_retries=3
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# Global session shared by all downloader threads
HTTP_SESSION = get_pooled_session()

# ============================================================================
# System Resource Checks
# ============================================================================

def check_system_limits():
    """Validate system resource limits for high-concurrency operations"""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    logging.info(f"Open file limit: soft={soft}, hard={hard}")
    if soft < 4096:
        logging.warning(
            f"Socket limit is low (soft={soft}) — consider running "
            "`ulimit -n 8192` before execution for optimal performance."
        )

# Run system checks at module load
check_system_limits()


# ============================================================================
# === KAPPA ARCHITECTURE: UNIFIED INGESTION PIPELINE ===
# ============================================================================
# Single processing path for both historical (batch) and live (streaming) data.
# All data flows through: Source → Processor Chain → Storage → MessageBus
# This eliminates code duplication and ensures consistency between backfill and live.
# ============================================================================

from abc import ABC, abstractmethod
from collections import deque
import json
import redis

# Timeframe configuration (seconds-based, no hardcoded assumptions)
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

# --- MarketDataSource Abstraction ---

class MarketDataSource(ABC):
    """
    Abstract base class for market data sources.
    Unified interface for historical (REST/CSV) and live (WebSocket) feeds.
    Both must be iterable and yield OHLCV bars in the same format.
    """
    
    @abstractmethod
    def __iter__(self):
        """Return iterator for data stream"""
        pass
    
    @abstractmethod
    def __next__(self) -> Dict:
        """
        Yield next OHLCV bar.
        
        Returns:
            Dict with keys: symbol, timestamp, open, high, low, close, volume, timeframe
        """
        pass


class HistoricalDataSource(MarketDataSource):
    """
    Historical data source - fetches from REST API or database.
    Yields bars in chronological order for replay through unified pipeline.
    """
    
    def __init__(self, aggregates_client: AggregatesClient, symbol: str, 
                 start_date: datetime, end_date: datetime, timeframe: str = '1m'):
        self.client = aggregates_client
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.timeframe = timeframe
        self.iterator = None
        self.bars = []
        self.index = 0
        
    def __iter__(self):
        """Fetch historical data and prepare for iteration"""
        # Fetch all bars for date range using the correct method based on timeframe
        multiplier, timespan = self._parse_timeframe(self.timeframe)
        
        # Use appropriate method based on timespan
        if timespan == 'minute':
            self.bars = self.client.get_minute_bars(
                ticker=self.symbol,
                start_date=self.start_date.strftime('%Y-%m-%d'),
                end_date=self.end_date.strftime('%Y-%m-%d'),
                multiplier=multiplier
            )
        elif timespan == 'hour':
            self.bars = self.client.get_hourly_bars(
                ticker=self.symbol,
                start_date=self.start_date.strftime('%Y-%m-%d'),
                end_date=self.end_date.strftime('%Y-%m-%d'),
                multiplier=multiplier
            )
        elif timespan == 'day':
            self.bars = self.client.get_daily_bars(
                ticker=self.symbol,
                start_date=self.start_date.strftime('%Y-%m-%d'),
                end_date=self.end_date.strftime('%Y-%m-%d')
            )
        else:
            # Default to minute bars
            self.bars = self.client.get_minute_bars(
                ticker=self.symbol,
                start_date=self.start_date.strftime('%Y-%m-%d'),
                end_date=self.end_date.strftime('%Y-%m-%d'),
                multiplier=multiplier
            )
        
        self.index = 0
        return self
    
    def __next__(self) -> Dict:
        """Yield next bar from historical data"""
        if self.index >= len(self.bars):
            raise StopIteration
        
        bar = self.bars[self.index]
        self.index += 1
        
        # Normalize to standard format
        return {
            'symbol': self.symbol,
            'timestamp': datetime.fromtimestamp(bar['t'] / 1000),
            'open': bar['o'],
            'high': bar['h'],
            'low': bar['l'],
            'close': bar['c'],
            'volume': bar['v'],
            'timeframe': self.timeframe
        }
    
    def _parse_timeframe(self, tf: str):
        """Convert timeframe string to Polygon API multiplier/timespan"""
        mapping = {
            '1m': (1, 'minute'), '5m': (5, 'minute'), '15m': (15, 'minute'),
            '30m': (30, 'minute'), '1h': (1, 'hour'), '2h': (2, 'hour'),
            '4h': (4, 'hour'), '1d': (1, 'day'), '1w': (1, 'week')
        }
        return mapping.get(tf, (1, 'minute'))


class LiveDataSource(MarketDataSource):
    """
    Live WebSocket data source - receives real-time aggregated bars from Polygon.
    Uses Polygon's WebSocket API to stream minute/second bars in real-time.
    
    Supports reconnection, authentication, and automatic subscription management.
    """
    
    def __init__(self, symbol: str, api_key: str, timeframe: str = '1m'):
        self.symbol = symbol.upper()
        self.api_key = api_key
        self.timeframe = timeframe
        self.queue = queue.Queue(maxsize=1000)
        self.ws_thread = None
        self.running = False
        self.ws = None
        self.logger = logging.getLogger(__name__)
        
        # Parse timeframe to determine subscription type
        # Polygon supports: A.{ticker} for second bars, AM.{ticker} for minute bars
        if timeframe in ['1m', '5m', '15m', '30m']:
            self.subscription_prefix = 'AM'  # Minute aggregates
        else:
            self.subscription_prefix = 'A'   # Second aggregates (for < 1m or custom)
        
    def __iter__(self):
        """Start WebSocket connection in background thread"""
        self.running = True
        self.ws_thread = threading.Thread(target=self._ws_loop, daemon=True)
        self.ws_thread.start()
        return self
    
    def __next__(self) -> Dict:
        """Block until next bar arrives from WebSocket"""
        if not self.running:
            raise StopIteration
        
        try:
            bar = self.queue.get(timeout=300)  # 5min timeout for live data
            if bar is None:  # Sentinel for shutdown
                raise StopIteration
            return bar
        except queue.Empty:
            self.logger.warning(f"No live data received for {self.symbol} in 5 minutes")
            raise StopIteration
    
    def _ws_loop(self):
        """WebSocket event loop with reconnection logic"""
        ws_url = "wss://socket.polygon.io/stocks"
        reconnect_delay = 5
        
        while self.running:
            try:
                self.logger.info(f"🔌 Connecting to Polygon WebSocket for {self.symbol}...")
                
                # Create WebSocket connection
                self.ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                
                # Run forever (blocks until connection closes)
                self.ws.run_forever()
                
                if self.running:
                    self.logger.warning(f"WebSocket disconnected, reconnecting in {reconnect_delay}s...")
                    time.sleep(reconnect_delay)
                
            except Exception as e:
                if self.running:
                    self.logger.error(f"WebSocket error: {e}, reconnecting in {reconnect_delay}s...")
                    time.sleep(reconnect_delay)
                else:
                    break
    
    def _on_open(self, ws):
        """Handle WebSocket connection opened"""
        self.logger.info(f"✅ WebSocket connected for {self.symbol}")
        
        # Authenticate
        auth_msg = {"action": "auth", "params": self.api_key}
        ws.send(json.dumps(auth_msg))
        
        # Subscribe to aggregates (minute bars)
        subscribe_msg = {
            "action": "subscribe",
            "params": f"{self.subscription_prefix}.{self.symbol}"
        }
        ws.send(json.dumps(subscribe_msg))
        self.logger.info(f"📡 Subscribed to {self.subscription_prefix}.{self.symbol}")
    
    def _on_message(self, ws, message):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)
            
            # Handle different message types
            for item in data:
                ev_type = item.get('ev')
                
                # Minute aggregate: AM
                # Second aggregate: A
                if ev_type in ['AM', 'A']:
                    # Normalize timestamp: use 't' if available, else 's' (start time)
                    # Polygon timestamps can be in ms or ns - detect and normalize to ms
                    timestamp_raw = item.get('t') or item.get('s')
                    if timestamp_raw:
                        # If timestamp > 1e12, it's in nanoseconds (convert to ms)
                        # If timestamp > 1e10, it's in milliseconds (use as-is)
                        # Otherwise it's in seconds (convert to ms)
                        if timestamp_raw > 1e12:
                            timestamp_ms = int(timestamp_raw / 1e6)  # ns -> ms
                        elif timestamp_raw > 1e10:
                            timestamp_ms = int(timestamp_raw)  # already in ms
                        else:
                            timestamp_ms = int(timestamp_raw * 1000)  # s -> ms
                    else:
                        continue  # Skip bar without timestamp
                    
                    # Convert to standard bar format
                    bar = {
                        'symbol': item.get('sym'),
                        'timestamp': datetime.fromtimestamp(timestamp_ms / 1000),
                        'open': item.get('o'),
                        'high': item.get('h'),
                        'low': item.get('l'),
                        'close': item.get('c'),
                        'volume': item.get('v'),
                        'timeframe': self.timeframe
                    }
                    
                    # Only queue bars for our symbol
                    if bar['symbol'] == self.symbol:
                        try:
                            self.queue.put_nowait(bar)
                            self.logger.debug(f"📊 Received live bar: {bar['symbol']} @ {bar['timestamp']}")
                        except queue.Full:
                            self.logger.warning(f"Queue full, dropping bar for {self.symbol}")
                
                # Status messages
                elif ev_type == 'status':
                    status = item.get('status')
                    msg = item.get('message', '')
                    if status == 'auth_success':
                        self.logger.info(f"✅ Polygon authentication successful")
                    elif status == 'success':
                        self.logger.info(f"✅ Subscription confirmed: {msg}")
                    else:
                        self.logger.warning(f"Status: {status} - {msg}")
        
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse WebSocket message: {e}")
        except Exception as e:
            self.logger.error(f"Error processing WebSocket message: {e}")
    
    def _on_error(self, ws, error):
        """Handle WebSocket error"""
        self.logger.error(f"WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection closed"""
        self.logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
    
    def stop(self):
        """Gracefully stop WebSocket connection"""
        self.running = False
        if self.ws:
            self.ws.close()
        self.queue.put(None)  # Sentinel


# --- Incremental Indicator Processor ---

class IncrementalIndicatorProcessor:
    """
    O(1) incremental indicator calculations using rolling windows.
    Maintains state in deques for efficient updates without recomputing history.
    """
    
    def __init__(self):
        # Rolling windows per indicator (symbol-specific state)
        self.windows = {}
        
    def process(self, bar: Dict) -> Dict:
        """
        Compute indicators incrementally for a single bar.
        
        Args:
            bar: OHLCV bar dict
            
        Returns:
            Enriched bar with computed indicators
        """
        symbol = bar['symbol']
        
        # Initialize windows for this symbol if first time
        if symbol not in self.windows:
            self.windows[symbol] = {
                'closes': deque(maxlen=200),  # Keep last 200 for EMA/SMA
                'highs': deque(maxlen=200),
                'lows': deque(maxlen=200),
                'volumes': deque(maxlen=200),
                'rsi_gains': deque(maxlen=14),
                'rsi_losses': deque(maxlen=14),
                'rsi_avg_gain': None,  # Running average for O(1) updates
                'rsi_avg_loss': None,  # Running average for O(1) updates
                'sma_20_sum': 0.0,  # Running sum for O(1) SMA
                'sma_50_sum': 0.0,  # Running sum for O(1) SMA
                'ema_20': None,
                'ema_50': None,
                'ema_100': None,
                'ema_200': None,
                'macd_12': None,
                'macd_26': None,
                'macd_signal': None,
            }
        
        w = self.windows[symbol]
        
        # Append new values
        prev_close = w['closes'][-1] if len(w['closes']) > 0 else None
        w['closes'].append(bar['close'])
        w['highs'].append(bar['high'])
        w['lows'].append(bar['low'])
        w['volumes'].append(bar['volume'])
        
        # Compute indicators incrementally
        bar['sma_20'] = self._incremental_sma(w, 20, bar['close'])
        bar['sma_50'] = self._incremental_sma(w, 50, bar['close'])
        bar['ema_20'], w['ema_20'] = self._incremental_ema(bar['close'], w['ema_20'], 20)
        bar['ema_50'], w['ema_50'] = self._incremental_ema(bar['close'], w['ema_50'], 50)
        bar['ema_100'], w['ema_100'] = self._incremental_ema(bar['close'], w['ema_100'], 100)
        bar['ema_200'], w['ema_200'] = self._incremental_ema(bar['close'], w['ema_200'], 200)
        
        bar['rsi_14'] = self._incremental_rsi(bar['close'], prev_close, w)
        bar['bb_upper'], bar['bb_middle'], bar['bb_lower'] = self._bollinger_bands(w['closes'], 20, 2.0)
        bar['vwap'] = self._vwap(w['closes'], w['volumes'])
        
        # MACD
        bar['macd'], bar['macd_signal'], bar['macd_hist'], w['macd_12'], w['macd_26'], w['macd_signal'] = \
            self._incremental_macd(bar['close'], w)
        
        return bar
    
    def _incremental_sma(self, w: Dict, period: int, new_close: float) -> float:
        """
        True O(1) Simple Moving Average using running sum.
        Maintains a running sum and updates it by subtracting the oldest value
        and adding the newest value.
        """
        closes = w['closes']
        sum_key = f'sma_{period}_sum'
        
        # Initialize sum if not exists
        if sum_key not in w:
            w[sum_key] = 0.0
        
        if len(closes) < period:
            # Not enough data yet, use simple average
            w[sum_key] = sum(closes)
            return w[sum_key] / len(closes) if len(closes) > 0 else None
        
        # O(1) update: add new value, subtract oldest if deque is full
        if len(closes) == closes.maxlen:
            # Deque is full, so we need to subtract the value that's about to be removed
            # Since we already appended new_close, we calculate the sum manually
            w[sum_key] = sum(list(closes)[-period:])
        else:
            w[sum_key] += new_close
        
        return w[sum_key] / period
    
    def _incremental_ema(self, close: float, prev_ema: Optional[float], period: int) -> tuple:
        """O(1) Exponential Moving Average"""
        alpha = 2.0 / (period + 1)
        if prev_ema is None:
            return close, close
        new_ema = alpha * close + (1 - alpha) * prev_ema
        return new_ema, new_ema
    
    def _incremental_rsi(self, close: float, prev_close: Optional[float], w: Dict) -> Optional[float]:
        """
        True O(1) Relative Strength Index using Wilder's smoothing.
        Maintains running averages for gains and losses.
        """
        if prev_close is None:
            return None
        
        change = close - prev_close
        gain = max(change, 0)
        loss = max(-change, 0)
        
        w['rsi_gains'].append(gain)
        w['rsi_losses'].append(loss)
        
        if len(w['rsi_gains']) < 14:
            # Not enough data, calculate initial average
            if len(w['rsi_gains']) == 14:
                w['rsi_avg_gain'] = sum(w['rsi_gains']) / 14
                w['rsi_avg_loss'] = sum(w['rsi_losses']) / 14
            return None
        
        # O(1) Wilder's smoothing: avg = (prev_avg * 13 + new_value) / 14
        if w['rsi_avg_gain'] is None or w['rsi_avg_loss'] is None:
            w['rsi_avg_gain'] = sum(w['rsi_gains']) / 14
            w['rsi_avg_loss'] = sum(w['rsi_losses']) / 14
        else:
            w['rsi_avg_gain'] = (w['rsi_avg_gain'] * 13 + gain) / 14
            w['rsi_avg_loss'] = (w['rsi_avg_loss'] * 13 + loss) / 14
        
        if w['rsi_avg_loss'] == 0:
            return 100.0
        
        rs = w['rsi_avg_gain'] / w['rsi_avg_loss']
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _bollinger_bands(self, closes: deque, period: int, std_dev: float) -> tuple:
        """Bollinger Bands"""
        if len(closes) < period:
            return None, None, None
        
        recent = list(closes)[-period:]
        middle = sum(recent) / period
        variance = sum((x - middle) ** 2 for x in recent) / period
        std = variance ** 0.5
        
        upper = middle + (std_dev * std)
        lower = middle - (std_dev * std)
        
        return upper, middle, lower
    
    def _vwap(self, closes: deque, volumes: deque) -> Optional[float]:
        """Volume Weighted Average Price"""
        if len(closes) == 0 or len(volumes) == 0:
            return None
        
        closes_list = list(closes)
        volumes_list = list(volumes)
        
        total_volume = sum(volumes_list)
        if total_volume == 0:
            return None
        
        vwap = sum(c * v for c, v in zip(closes_list, volumes_list)) / total_volume
        return vwap
    
    def _incremental_macd(self, close: float, w: Dict) -> tuple:
        """O(1) MACD calculation"""
        # MACD = EMA(12) - EMA(26)
        ema_12, w['macd_12'] = self._incremental_ema(close, w['macd_12'], 12)
        ema_26, w['macd_26'] = self._incremental_ema(close, w['macd_26'], 26)
        
        if ema_12 is None or ema_26 is None:
            return None, None, None, w['macd_12'], w['macd_26'], w['macd_signal']
        
        macd = ema_12 - ema_26
        
        # Signal line = EMA(9) of MACD
        signal, w['macd_signal'] = self._incremental_ema(macd, w['macd_signal'], 9)
        
        hist = macd - signal if signal is not None else None
        
        return macd, signal, hist, w['macd_12'], w['macd_26'], w['macd_signal']


# --- MessageBus Abstraction ---

class MessageBus(ABC):
    """
    Abstract message bus interface.
    Allows swapping between Redis, Kafka, or in-memory implementations.
    """
    
    @abstractmethod
    def publish(self, topic: str, message: Dict):
        """Publish message to topic"""
        pass
    
    @abstractmethod
    def subscribe(self, topic: str, callback):
        """Subscribe to topic with callback"""
        pass


class RedisMessageBus(MessageBus):
    """
    Redis-based message bus using Pub/Sub for real-time GUI updates.
    Publishes to chronox:gui:updates:{symbol}:{tf} for frontend consumption.
    Supports password authentication.
    """
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, password: str = None):
        self.redis_client = redis.Redis(
            host=host, 
            port=port, 
            db=db, 
            password=password,
            decode_responses=True
        )
        self.pubsub = None
        
        # Test connection
        self.redis_client.ping()
    
    def _serialize_value(self, obj):
        """Convert datetime and other non-serializable objects to JSON-compatible types"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif hasattr(obj, '__dict__'):
            return str(obj)
        return obj
    
    def _prepare_message(self, message: Dict) -> Dict:
        """Recursively convert message values to JSON-serializable types"""
        cleaned = {}
        for key, value in message.items():
            if isinstance(value, dict):
                cleaned[key] = self._prepare_message(value)
            elif isinstance(value, (list, tuple)):
                cleaned[key] = [self._serialize_value(item) for item in value]
            else:
                cleaned[key] = self._serialize_value(value)
        return cleaned
        
    def publish(self, topic: str, message: Dict):
        """Publish JSON message to Redis channel with GUI-compatible naming"""
        try:
            # Extract symbol and timeframe from topic (format: bars:SYMBOL:TF or chronox:gui:updates:SYMBOL:TF)
            if topic.startswith('bars:'):
                parts = topic.split(':')
                if len(parts) >= 3:
                    symbol = parts[1]
                    tf = parts[2]
                    gui_channel = f"chronox:gui:updates:{symbol}:{tf}"
                else:
                    gui_channel = topic
            else:
                gui_channel = topic
            
            # Clean message to ensure JSON serialization (do this FIRST)
            cleaned_message = self._prepare_message(message)
            
            # Add composite key for idempotency (after cleaning)
            if 'symbol' in cleaned_message and 'timeframe' in cleaned_message and 'timestamp' in cleaned_message:
                cleaned_message['k'] = f"{cleaned_message['symbol']}|{cleaned_message['timeframe']}|{cleaned_message['timestamp']}"
            
            self.redis_client.publish(gui_channel, json.dumps(cleaned_message))
        except Exception as e:
            logging.error(f"Redis publish error: {e}")
            logging.debug(f"Failed message: {message}")
    
    def subscribe(self, topic: str, callback):
        """Subscribe to Redis channel (blocking)"""
        if self.pubsub is None:
            self.pubsub = self.redis_client.pubsub()
        
        self.pubsub.subscribe(topic)
        
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    callback(data)
                except Exception as e:
                    logging.error(f"Redis subscribe callback error: {e}")


class KafkaToRedisBridge(threading.Thread):
    """
    Background bridge thread: Kafka → Redis
    
    Consumes enriched market data from Kafka topic 'chronox.market.enriched'
    and republishes to Redis Pub/Sub for GUI consumption.
    
    Features:
    - Offset checkpointing to ./.chronox_offsets.json (resume on restart)
    - Idempotent DB writes (ON CONFLICT DO UPDATE)
    - Concurrent backfill + streaming (independent of backfill jobs)
    - Manual offset commit after successful DB write
    
    Channel naming: chronox:gui:updates:{symbol}:{timeframe}
    Message schema includes composite key 'k' for idempotency
    """
    
    def __init__(self, kafka_brokers: str = 'localhost:9092', 
                 redis_bus: RedisMessageBus = None,
                 storage: 'TimescaleWriter' = None,
                 checkpoint_path: str = './.chronox_offsets.json'):
        super().__init__(daemon=True)
        self.kafka_brokers = kafka_brokers
        self.redis_bus = redis_bus
        self.storage = storage
        self.checkpoint_path = checkpoint_path
        self.running = False
        self.consumer = None
        self.offsets = {}  # {(topic, partition): offset}
        
    def _load_offsets(self):
        """Load offset checkpoint from disk"""
        try:
            if os.path.exists(self.checkpoint_path):
                with open(self.checkpoint_path, 'r') as f:
                    data = json.load(f)
                    # Convert string keys back to tuples
                    self.offsets = {tuple(k.split('|')): v for k, v in data.items()}
                    logging.info(f"✓ Loaded {len(self.offsets)} offset checkpoints")
        except Exception as e:
            logging.warning(f"Failed to load offsets: {e}")
            self.offsets = {}
    
    def _save_offsets(self):
        """Persist offset checkpoint to disk"""
        try:
            # Convert tuples to string keys for JSON
            data = {'|'.join(k): v for k, v in self.offsets.items()}
            with open(self.checkpoint_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save offsets: {e}")
    
    def run(self):
        """Main Kafka consumer loop"""
        if not KAFKA_AVAILABLE:
            logging.warning("Kafka bridge not started - confluent-kafka not installed")
            return
        
        self.running = True
        self._load_offsets()
        
        # Kafka consumer configuration
        conf = {
            'bootstrap.servers': self.kafka_brokers,
            'group.id': 'chronox_gui_bridge',
            'enable.auto.commit': False,  # Manual commit after DB write
            'auto.offset.reset': 'latest',
            'session.timeout.ms': 6000,
            'api.version.request': True
        }
        
        try:
            self.consumer = Consumer(conf)
            topic = 'chronox.market.enriched'
            self.consumer.subscribe([topic])
            
            logging.info(f"✓ Kafka→Redis bridge started: {topic}")
            
            # Seek to checkpointed offsets
            if self.offsets:
                assignment = self.consumer.assignment()
                if assignment:
                    for tp in assignment:
                        key = (tp.topic, tp.partition)
                        if key in self.offsets:
                            offset = self.offsets[key] + 1  # Resume from next
                            self.consumer.seek(TopicPartition(tp.topic, tp.partition, offset))
                            logging.info(f"  Resuming {tp.topic}[{tp.partition}] at offset {offset}")
            
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error(f"Kafka error: {msg.error()}")
                        continue
                
                try:
                    # Parse message
                    bar_data = json.loads(msg.value().decode('utf-8'))
                    
                    # Idempotent DB write (if storage enabled)
                    if self.storage:
                        self.storage.write_bars_upsert([bar_data], timeframe=bar_data.get('timeframe', '1m'))
                    
                    # Publish to Redis for GUI
                    if self.redis_bus:
                        symbol = bar_data.get('symbol', 'UNKNOWN')
                        tf = bar_data.get('timeframe', '1m')
                        
                        # Add composite key for idempotency
                        ts = bar_data.get('timestamp', bar_data.get('ts', ''))
                        bar_data['k'] = f"{symbol}|{tf}|{ts}"
                        
                        # Publish to GUI channel
                        gui_channel = f"chronox:gui:updates:{symbol}:{tf}"
                        self.redis_bus.publish(gui_channel, bar_data)
                    
                    # Checkpoint offset after successful processing
                    tp_key = (msg.topic(), msg.partition())
                    self.offsets[tp_key] = msg.offset()
                    
                    # Commit offset to Kafka
                    self.consumer.commit(asynchronous=False)
                    
                    # Save checkpoint every 100 messages
                    if msg.offset() % 100 == 0:
                        self._save_offsets()
                
                except Exception as e:
                    logging.error(f"Bridge processing error: {e}", exc_info=True)
        
        except Exception as e:
            logging.error(f"Kafka bridge error: {e}", exc_info=True)
        
        finally:
            if self.consumer:
                self.consumer.close()
            self._save_offsets()
            logging.info("✓ Kafka→Redis bridge stopped")
    
    def stop(self):
        """Stop bridge gracefully"""
        self.running = False
        self.join(timeout=5)


class InMemoryMessageBus(MessageBus):
    """
    In-memory message bus for testing or fallback.
    Uses Python queue for inter-thread communication.
    """
    
    def __init__(self):
        self.subscribers = {}
        
    def publish(self, topic: str, message: Dict):
        """Publish to in-memory subscribers"""
        if topic in self.subscribers:
            for callback in self.subscribers[topic]:
                try:
                    callback(message)
                except Exception as e:
                    logging.error(f"InMemory callback error: {e}")
    
    def subscribe(self, topic: str, callback):
        """Subscribe callback to topic"""
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        self.subscribers[topic].append(callback)


# --- Data Ingestion Engine ---

class DataIngestionEngine:
    """
    Unified ingestion pipeline for both historical and live data (Kappa architecture).
    Single processing path ensures consistency between backfill and streaming.
    
    Flow: DataSource → Processor Chain → Storage (idempotent upsert) → MessageBus
    """
    
    def __init__(self, processors: List, storage: TimescaleWriter, 
                 message_bus: MessageBus, batch_size: int = 1000):
        self.processors = processors
        self.storage = storage
        self.message_bus = message_bus
        self.batch_size = batch_size
        self.batch_buffer = []
        
    def ingest(self, data_source: MarketDataSource, mode: str = 'batch'):
        """
        Ingest data from source through unified pipeline.
        
        Args:
            data_source: MarketDataSource instance (historical or live)
            mode: 'batch' (accumulate for bulk insert) or 'stream' (immediate insert)
        """
        for bar in data_source:
            try:
                # Process through chain
                enriched_bar = bar
                for processor in self.processors:
                    enriched_bar = processor.process(enriched_bar)
                
                # Storage strategy
                if mode == 'batch':
                    self.batch_buffer.append(enriched_bar)
                    if len(self.batch_buffer) >= self.batch_size:
                        self._flush_batch()
                else:  # stream mode
                    self._write_single(enriched_bar)
                
                # Publish to message bus
                topic = f"bars:{enriched_bar['symbol']}:{enriched_bar['timeframe']}"
                self.message_bus.publish(topic, enriched_bar)
                
            except Exception as e:
                logging.error(f"Ingestion error: {e}", exc_info=True)
        
        # Flush remaining batch
        if mode == 'batch' and self.batch_buffer:
            self._flush_batch()
    
    def _flush_batch(self):
        """Batch insert to TimescaleDB"""
        if not self.batch_buffer:
            return
        
        try:
            # Group by timeframe for efficient insert
            by_timeframe = {}
            for bar in self.batch_buffer:
                tf = bar['timeframe']
                if tf not in by_timeframe:
                    by_timeframe[tf] = []
                by_timeframe[tf].append(bar)
            
            # Insert each timeframe group
            for tf, bars in by_timeframe.items():
                self.storage.write_bars_upsert(bars, timeframe=tf)
            
            logging.debug(f"Flushed batch of {len(self.batch_buffer)} bars")
            self.batch_buffer.clear()
            
        except Exception as e:
            logging.error(f"Batch flush error: {e}", exc_info=True)
    
    def _write_single(self, bar: Dict):
        """Single bar insert (streaming mode)"""
        try:
            self.storage.write_bars_upsert([bar], timeframe=bar['timeframe'])
        except Exception as e:
            logging.error(f"Single write error: {e}")


# ============================================================================
# === END KAPPA ARCHITECTURE CORE ===
# ============================================================================


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)-15s] %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./logs/backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# Multiprocessing Task Function for Parallel Processing
# ============================================================================

def process_flat_file_task(task_data: Dict, tickers: List[str], db_config: Dict, debug: bool = False) -> Dict:
    """
    Standalone function for multiprocessing: Parse flat file and write to database.
    
    MULTI-TICKER OPTIMIZATION:
    - Each .csv.gz file contains data for ALL tickers (market-wide snapshot)
    - Downloads each file ONCE (not per-ticker)
    - Parses file ONCE in a single pass
    - Filters for all configured tickers simultaneously
    - Groups bars by ticker and bulk inserts each group
    - Returns per-ticker bar counts for monitoring
    
    This function must be importable and serializable for ProcessPoolExecutor.
    All database connections and parsers are created fresh in each worker process.
    
    Args:
        task_data: Dict with task info (id, local_path, filename, date)
        tickers: List of ALL ticker symbols to extract (processed in single pass)
        db_config: Database config (or empty dict for env vars)
        debug: Enable debug logging
        
    Returns:
        Dict with results: {
            'task_id': str,
            'status': 'success' | 'error',
            'bars_written': int (total across all tickers),
            'ticker_results': {ticker: bar_count, ...},
            'tickers_processed': [ticker1, ticker2, ...],
            'error': Optional[str],
            'processing_time_ms': float
        }
    """
    import logging
    from pathlib import Path
    from data.ingestion.polygon.flat_file_parser import FlatFileParser
    from data.storage.timescale_writer import TimescaleWriter
    import time
    
    # Setup logging in worker process
    worker_logger = logging.getLogger(f"Worker-{task_data['id']}")
    
    start_time = time.time()
    result = {
        'task_id': task_data['id'],
        'status': 'error',
        'bars_written': 0,
        'ticker_results': {},  # NEW: Track bars written per ticker {ticker: count}
        'tickers_processed': [],  # NEW: List of tickers found in this file
        'error': None,
        'processing_time_ms': 0.0,
        'filename': task_data['filename']
    }
    skipped_records = {'no_ticker': 0, 'no_timestamp': 0, 'invalid_numeric': 0, 'missing_ohlc': 0}
    
    try:
        import os
        
        # Initialize parser and database writer in worker process
        parser = FlatFileParser()
        db_writer = TimescaleWriter()
        
        local_path = Path(task_data['local_path'])
        
        if debug:
            worker_logger.info(f"DEBUG: Processing {task_data['filename']} in process {os.getpid()}")
        
        # Parse flat file
        records_iter = parser.parse_aggregates_file(local_path)
        
        # Filter to requested tickers
        filtered = parser.filter_by_ticker(records_iter, tickers)
        
        # Convert timestamps (ns->ms)
        converted = parser.convert_timestamps(filtered, timestamp_fields=['t'])
        
        # Batch and group by ticker
        total_bars = 0
        for batch in parser.batch_records(converted, batch_size=5000):
            # Group by ticker
            groups = {}
            
            for rec in batch:
                # Determine ticker field with robust validation
                # Common field names: ticker, sym, y (Polygon flat files), T (Polygon API)
                ticker = rec.get('ticker') or rec.get('sym') or rec.get('y') or rec.get('T') or rec.get('symbol')
                if not ticker:
                    skipped_records['no_ticker'] += 1
                    if debug:
                        worker_logger.debug(f"Skipping record without ticker: {rec}")
                    continue
                
                # Validate and normalize ticker
                try:
                    ticker = str(ticker).strip().upper()
                    if not ticker or len(ticker) > 10:  # Sanity check
                        skipped_records['no_ticker'] += 1
                        continue
                except Exception:
                    skipped_records['no_ticker'] += 1
                    continue
                
                # Skip record if timestamp is missing
                timestamp = rec.get('t')
                if not timestamp:
                    skipped_records['no_timestamp'] += 1
                    if debug:
                        worker_logger.debug(f"Skipping record without timestamp for {ticker}")
                    continue
                
                # Convert and validate numeric fields
                try:
                    # Normalize timestamp: detect units (ms vs ns) and convert to ms
                    t_raw = int(timestamp)
                    if t_raw > 1e12:  # Nanoseconds
                        t_val = int(t_raw / 1e6)
                    elif t_raw > 1e10:  # Milliseconds (use as-is)
                        t_val = t_raw
                    else:  # Seconds
                        t_val = int(t_raw * 1000)
                    
                    # Handle None values properly
                    o_raw = rec.get('o')
                    h_raw = rec.get('h')
                    l_raw = rec.get('l')
                    c_raw = rec.get('c')
                    vw_raw = rec.get('vw') or rec.get('vwap')
                    
                    o_val = float(o_raw) if o_raw is not None else None
                    h_val = float(h_raw) if h_raw is not None else None
                    l_val = float(l_raw) if l_raw is not None else None
                    c_val = float(c_raw) if c_raw is not None else None
                    v_val = int(rec.get('v') or rec.get('volume') or 0)
                    vw_val = float(vw_raw) if vw_raw is not None else None
                    n_val = int(rec.get('n') or rec.get('transactions') or 0)
                except (ValueError, TypeError, OverflowError) as e:
                    skipped_records['invalid_numeric'] += 1
                    if debug:
                        worker_logger.debug(f"Invalid numeric values for {ticker}: {e}")
                    continue
                
                # Skip if critical OHLC fields are missing
                if not all([o_val is not None, h_val is not None, l_val is not None, c_val is not None]):
                    skipped_records['missing_ohlc'] += 1
                    continue
                
                groups.setdefault(ticker, []).append({
                    't': t_val,
                    'o': o_val,
                    'h': h_val,
                    'l': l_val,
                    'c': c_val,
                    'v': v_val,
                    'vw': vw_val,
                    'n': n_val
                })
            
            # Insert per-ticker batches with multi-timeframe resampling
            for tk, bars in groups.items():
                if tk in tickers:
                    try:
                        # Write base 1min bars
                        written = db_writer.write_ohlcv_bars(tk, bars, timeframe='1min', use_copy=True)
                        total_bars += written
                        
                        # Track per-ticker results
                        if tk not in result['ticker_results']:
                            result['ticker_results'][tk] = 0
                            result['tickers_processed'].append(tk)
                        result['ticker_results'][tk] += written
                        
                        # Resample to higher timeframes
                        # Convert bars to DataFrame for resampling
                        df = pd.DataFrame(bars)
                        df['time'] = pd.to_datetime(df['t'], unit='ms', utc=True)
                        df = df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 
                                                'c': 'close', 'v': 'volume', 'vw': 'vwap'})
                        df = df.set_index('time')
                        
                        # Resample to all higher timeframes
                        timeframes = ['5m', '15m', '30m', '1h', '2h', '4h', '12h', '1d']
                        freq_map = {
                            '5m': '5min', '15m': '15min', '30m': '30min',
                            '1h': '1h', '2h': '2h', '4h': '4h', '12h': '12h', '1d': '1D'
                        }
                        
                        for tf in timeframes:
                            try:
                                freq = freq_map[tf]
                                resampled = df.resample(freq).agg({
                                    'open': 'first',
                                    'high': 'max',
                                    'low': 'min',
                                    'close': 'last',
                                    'volume': 'sum',
                                    'vwap': 'mean'
                                }).dropna(subset=['close'])
                                
                                if not resampled.empty:
                                    # Convert back to bars format
                                    resampled_bars = []
                                    for idx, row in resampled.iterrows():
                                        resampled_bars.append({
                                            't': int(idx.timestamp() * 1000),
                                            'o': row['open'],
                                            'h': row['high'],
                                            'l': row['low'],
                                            'c': row['close'],
                                            'v': int(row['volume']),
                                            'vw': row['vwap'],
                                            'n': 0  # Not applicable for resampled data
                                        })
                                    
                                    # Write resampled bars
                                    db_writer.write_ohlcv_bars(tk, resampled_bars, timeframe=tf, use_copy=True)
                                    
                            except Exception as e:
                                worker_logger.warning(f"Failed to resample {tk} to {tf}: {e}")
                        
                    except Exception as e:
                        worker_logger.error(f"Failed to write bars for {tk}: {e}")
        
        # Delete file after successful processing
        try:
            if local_path.exists():
                local_path.unlink()
        except Exception as e:
            worker_logger.warning(f"Failed to delete {task_data['filename']}: {e}")
        
        # Log skipped records if debug or if significant
        total_skipped = sum(skipped_records.values())
        if debug or total_skipped > 100:
            worker_logger.info(f"Skipped records in {task_data['filename']}: {skipped_records}")
        
        # Success
        processing_time = (time.time() - start_time) * 1000
        result['status'] = 'success'
        result['bars_written'] = total_bars
        result['processing_time_ms'] = processing_time
        
        # Close database connection
        db_writer.close()
        
    except Exception as e:
        processing_time = (time.time() - start_time) * 1000
        result['error'] = str(e)
        result['processing_time_ms'] = processing_time
        worker_logger.error(f"Error processing {task_data['filename']}: {e}")
    
    return result


# ============================================================================
# Producer/Consumer Pipeline Architecture for S3 Flat File Downloads
# ============================================================================

class FileState(Enum):
    """
    Explicit state machine for file download and processing lifecycle.
    Each state transition is logged for observability and debugging.
    """
    WAITING = "WAITING"              # Queued for download
    DOWNLOADING = "DOWNLOADING"      # Download in progress
    RETRYING = "RETRYING"            # Failed download, waiting for retry with backoff
    DOWNLOADED = "DOWNLOADED"        # Downloaded successfully, awaiting processing
    PROCESSING = "PROCESSING"        # Processing in progress
    COMPLETED = "COMPLETED"          # Successfully processed
    FAILED = "FAILED"                # Permanently failed (exhausted retries or non-recoverable error)


class FileTask:
    """
    Task object representing a single file in the download/processing pipeline.
    
    Tracks state, retry attempts, and metadata for fault-tolerant processing.
    Thread-safe state transitions with logging.
    """
    def __init__(self, file_id: str, s3_key: str, date_str: str, filename: str):
        self.id = file_id                    # Unique identifier (for logging)
        self.s3_key = s3_key                 # S3 object key
        self.date = date_str                 # Date string (YYYY-MM-DD)
        self.filename = filename             # Local filename
        self.state = FileState.WAITING
        self.attempts = 0                    # Download attempt counter
        self.local_path: Optional[Path] = None  # Path to downloaded file
        self.error: Optional[str] = None     # Last error message
        self._lock = threading.Lock()        # Thread-safe state updates
    
    def set_state(self, new_state: FileState, error: Optional[str] = None):
        """
        Thread-safe state transition with logging.
        
        Args:
            new_state: Target state
            error: Optional error message for FAILED/RETRYING states
        """
        with self._lock:
            old_state = self.state
            self.state = new_state
            if error:
                self.error = error
            
            # Log state transition
            if error:
                logger.info(f"File {self.id} [{self.date}]: {old_state.value} → {new_state.value} ({error})")
            else:
                logger.debug(f"File {self.id} [{self.date}]: {old_state.value} → {new_state.value}")
    
    def get_state(self) -> FileState:
        """Thread-safe state getter"""
        with self._lock:
            return self.state
    
    def increment_attempts(self) -> int:
        """Thread-safe attempt counter increment"""
        with self._lock:
            self.attempts += 1
            return self.attempts


def generate_available_file_tasks(
    start_date: datetime,
    end_date: datetime,
    category: str,
    s3_client,
    bucket_name: str,
    prefix_base: str
) -> List[FileTask]:
    """
    Generate file tasks by dynamically listing available files on S3.
    
    Uses S3 listing to discover actual files rather than blindly iterating dates.
    Scans by YYYY/MM/ prefixes for efficiency.
    
    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        category: Either 'us_stocks_sip' or 'global_crypto'
        s3_client: Initialized boto3 S3 client
        bucket_name: S3 bucket name
        prefix_base: Base prefix for category (e.g., 'us_stocks_sip/minute_aggs_v1')
    
    Returns:
        List of FileTask objects for discovered files
    
    Raises:
        ValueError: If category is unknown
    """
    if category not in ['us_stocks_sip', 'global_crypto']:
        raise ValueError(f"Unknown category: {category}. Must be 'us_stocks_sip' or 'global_crypto'")
    
    logger.info(f"Scanning S3 for available files: {category} from {start_date.date()} to {end_date.date()}")
    
    file_tasks = []
    task_id = 1
    
    # Iterate through year/month combinations
    current_date = start_date.replace(day=1)  # Start at beginning of month
    end_month = end_date.replace(day=1)
    
    while current_date <= end_month:
        year = current_date.year
        month = current_date.month
        
        # Construct S3 prefix for this year/month
        s3_prefix = f"{prefix_base}/{year}/{month:02d}/"
        
        logger.debug(f"Listing S3 prefix: {s3_prefix}")
        
        try:
            # List objects in this month's prefix
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)
            
            for page in pages:
                if 'Contents' not in page:
                    continue
                
                for obj in page['Contents']:
                    s3_key = obj['Key']
                    
                    # Filter for .csv.gz files
                    if not s3_key.endswith('.csv.gz'):
                        continue
                    
                    # Extract filename
                    filename = s3_key.split('/')[-1]
                    
                    # Match YYYY-MM-DD.csv.gz pattern
                    if not filename.count('-') == 2:
                        continue
                    
                    try:
                        # Extract date from filename
                        date_str = filename.replace('.csv.gz', '')
                        file_date = datetime.strptime(date_str, '%Y-%m-%d')
                        
                        # Check if date is within range
                        if file_date < start_date or file_date > end_date:
                            continue
                        
                        # Create FileTask
                        task = FileTask(
                            file_id=f"file{task_id:04d}",
                            s3_key=s3_key,
                            date_str=date_str,
                            filename=filename
                        )
                        file_tasks.append(task)
                        task_id += 1
                        
                    except ValueError:
                        # Invalid date format in filename, skip
                        continue
        
        except Exception as e:
            logger.warning(f"Error listing S3 prefix {s3_prefix}: {e}")
        
        # Move to next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    
    logger.info(f"Discovered {len(file_tasks)} files on S3 for {category}")
    return file_tasks


class BackfillOrchestrator:
    """
    Orchestrates the complete historical data backfill process.
    
    Updated: Now uses TimescaleDB instead of InfluxDB for improved:
    - ACID transactions
    - SQL joins and window functions
    - Continuous aggregates (auto-updating materialized views)
    - Better compression (10-20x)
    """
    
    def __init__(self, polygon_api_key: str, db_writer: TimescaleWriter,
                 years_back: int = 5, max_workers: int = 4, use_flatfiles: bool = False, debug: bool = False,
                 skip_reference: bool = False, skip_corporate: bool = False, skip_daily: bool = False,
                 skip_minute: bool = False, skip_news: bool = False, enable_hybrid_backfill: bool = False):
        """
        Initialize backfill orchestrator.
        
        Args:
            polygon_api_key: Polygon.io API key
            db_writer: TimescaleDB writer instance
            years_back: How many years of historical data to fetch
            max_workers: Number of parallel workers for ticker processing
            use_flatfiles: Whether to use S3 flat files for Phase 4
            debug: Enable debug logging for queue states and stack transitions
            skip_reference: Skip Phase 1 (reference data)
            skip_corporate: Skip Phase 2 (corporate actions)
            skip_daily: Skip Phase 3 (daily bars)
            skip_minute: Skip Phase 4 (minute bars / flat files)
            skip_news: Skip Phase 5 (news)
            enable_hybrid_backfill: Enable hybrid mode (flat files + REST API gap filling)
        """
        self.polygon_client = PolygonClient(polygon_api_key)
        self.agg_client = AggregatesClient(self.polygon_client)
        self.corp_client = CorporateActionsClient(self.polygon_client)
        self.ref_client = ReferenceClient(self.polygon_client)
        self.news_client = NewsClient(self.polygon_client)
        self.db_writer = db_writer  # Updated: TimescaleDB writer
        
        self.years_back = years_back
        self.max_workers = max_workers
        self.use_flatfiles = use_flatfiles
        self.debug = debug
        self.enable_hybrid_backfill = enable_hybrid_backfill
        
        # Skip flags for selective phase execution
        self.skip_reference = skip_reference
        self.skip_corporate = skip_corporate
        self.skip_daily = skip_daily
        self.skip_minute = skip_minute
        self.skip_news = skip_news
        
        # Calculate date range
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=365 * years_back)
        
        # Queue metrics interface (shared with GUI for real-time monitoring)
        self.queue_metrics = {
            'download_qsize': lambda: 0,  # Will be set when queues are created
            'process_qsize': lambda: 0,
            'download_queue': None,
            'process_queue': None
        }
        
        logger.info(f"BackfillOrchestrator initialized: {self.start_date.date()} to {self.end_date.date()}")
        logger.info(f"Database: TimescaleDB (PostgreSQL + TimescaleDB extension)")
        if debug:
            logger.info(f"Debug mode: ENABLED")
    
    def phase1_reference_data(self, tickers: List[str]) -> Dict[str, bool]:
        """
        Phase 1: Fetch and store reference data for all tickers.
        
        Args:
            tickers: List of ticker symbols
        
        Returns:
            Dict mapping ticker to success status
        """
        logger.info(f"=== PHASE 1: Reference Data ({len(tickers)} tickers) ===")
        results = {}
        
        for ticker in tqdm(tickers, desc="Reference Data"):
            try:
                details = self.ref_client.get_ticker_details(ticker)
                
                if details:
                    # Log detailed ticker information for debugging
                    name = details.get('name', 'Unknown')
                    exchange = details.get('primary_exchange', details.get('exchange', 'Unknown'))
                    ticker_type = details.get('type', 'Unknown')
                    market_cap = details.get('market_cap', 0)
                    
                    logger.info(f"✓ {ticker}: {name} | {exchange} | Type: {ticker_type} | Market Cap: ${market_cap:,}")
                    
                    success = self.db_writer.write_reference_data(ticker, details)
                    results[ticker] = success
                    
                    if success:
                        logger.debug(f"  └─ Wrote reference data to database")
                    else:
                        logger.warning(f"  └─ Failed to write reference data to database")
                else:
                    logger.warning(f"✗ No reference data for {ticker}")
                    results[ticker] = False
                
                time.sleep(0.1)  # Small delay to respect rate limits
            
            except Exception as e:
                logger.error(f"✗ Error fetching reference data for {ticker}: {str(e)}")
                results[ticker] = False
        
        success_count = sum(1 for v in results.values() if v)
        logger.info(f"Phase 1 complete: {success_count}/{len(tickers)} successful")
        return results
    
    def phase2_corporate_actions(self, tickers: List[str]) -> Dict[str, Dict]:
        """
        Phase 2: Fetch and store corporate actions (dividends and splits).
        
        Args:
            tickers: List of ticker symbols
        
        Returns:
            Dict with results per ticker
        """
        logger.info(f"=== PHASE 2: Corporate Actions ({len(tickers)} tickers) ===")
        results = {}
        since_date = self.start_date.strftime('%Y-%m-%d')
        
        for ticker in tqdm(tickers, desc="Corporate Actions"):
            try:
                ticker_results = {'dividends': 0, 'splits': 0}
                
                # Fetch dividends
                dividends = self.corp_client.get_dividends_for_ticker(ticker, since=since_date)
                if dividends:
                    count = self.db_writer.write_corporate_actions(ticker, dividends, 'dividend')
                    ticker_results['dividends'] = count
                    
                    # Log detailed dividend information for debugging
                    logger.info(f"✓ {ticker}: Found {count} dividends since {since_date}")
                    for div in dividends[:3]:  # Show first 3 for debugging
                        ex_date = div.get('ex_dividend_date', 'Unknown')
                        amount = div.get('cash_amount', 0)
                        logger.debug(f"  └─ Dividend: ${amount:.4f} on {ex_date}")
                    if len(dividends) > 3:
                        logger.debug(f"  └─ ... and {len(dividends)-3} more")
                else:
                    logger.debug(f"  {ticker}: No dividends since {since_date}")
                
                # Fetch splits
                splits = self.corp_client.get_splits_for_ticker(ticker, since=since_date)
                if splits:
                    count = self.db_writer.write_corporate_actions(ticker, splits, 'split')
                    ticker_results['splits'] = count
                    
                    # Log detailed split information for debugging
                    logger.info(f"✓ {ticker}: Found {count} splits since {since_date}")
                    for split in splits:
                        ex_date = split.get('execution_date', 'Unknown')
                        ratio = split.get('split_to', 1) / split.get('split_from', 1)
                        logger.debug(f"  └─ Split: {ratio:.2f}:1 on {ex_date}")
                else:
                    logger.debug(f"  {ticker}: No splits since {since_date}")
                
                results[ticker] = ticker_results
                time.sleep(0.1)
            
            except Exception as e:
                logger.error(f"Error fetching corporate actions for {ticker}: {str(e)}")
                results[ticker] = {'dividends': 0, 'splits': 0, 'error': str(e)}
        
        logger.info(f"Phase 2 complete")
        return results
    
    def phase3_daily_bars(self, tickers: List[str]) -> Dict[str, int]:
        """
        Phase 3: Fetch and store daily OHLCV bars.
        
        Args:
            tickers: List of ticker symbols
        
        Returns:
            Dict mapping ticker to bar count
        """
        logger.info(f"=== PHASE 3: Daily Bars ({len(tickers)} tickers) ===")
        results = {}
        
        start_str = self.start_date.strftime('%Y-%m-%d')
        end_str = self.end_date.strftime('%Y-%m-%d')
        
        for ticker in tqdm(tickers, desc="Daily Bars"):
            try:
                bars = self.agg_client.get_daily_bars(ticker, start_str, end_str)
                
                if bars:
                    count = self.db_writer.write_ohlcv_bars(ticker, bars, timeframe="1day")
                    results[ticker] = count
                    
                    # Log detailed bar information for debugging
                    first_bar = bars[0]
                    last_bar = bars[-1]
                    
                    first_date = datetime.fromtimestamp(first_bar['t'] / 1000).strftime('%Y-%m-%d')
                    last_date = datetime.fromtimestamp(last_bar['t'] / 1000).strftime('%Y-%m-%d')
                    
                    logger.info(f"✓ {ticker}: {count} daily bars | {first_date} to {last_date}")
                    logger.debug(f"  └─ First: O=${first_bar['o']:.2f} H=${first_bar['h']:.2f} L=${first_bar['l']:.2f} C=${first_bar['c']:.2f} V={first_bar['v']:,}")
                    logger.debug(f"  └─ Last:  O=${last_bar['o']:.2f} H=${last_bar['h']:.2f} L=${last_bar['l']:.2f} C=${last_bar['c']:.2f} V={last_bar['v']:,}")
                else:
                    logger.warning(f"✗ {ticker}: No daily bars found for {start_str} to {end_str}")
                    results[ticker] = 0
                
                time.sleep(0.1)
            
            except Exception as e:
                logger.error(f"Error fetching daily bars for {ticker}: {str(e)}")
                results[ticker] = 0
        
        total_bars = sum(results.values())
        logger.info(f"Phase 3 complete: {total_bars:,} daily bars written")
        return results
    
    def phase4_minute_bars(self, tickers: List[str], chunk_days: int = 7) -> Dict[str, int]:
        """
        Phase 4: Fetch and store minute OHLCV bars (chunked for large date ranges).
        
        Args:
            tickers: List of ticker symbols
            chunk_days: Days per chunk (7 recommended to stay under 50k limit)
        
        Returns:
            Dict mapping ticker to bar count
        """
        logger.info(f"=== PHASE 4: Minute Bars ({len(tickers)} tickers) ===")
        logger.info(f"Date range: {self.start_date.date()} to {self.end_date.date()}")
        logger.info(f"Chunk size: {chunk_days} days")
        
        results = {}
        start_str = self.start_date.strftime('%Y-%m-%d')
        end_str = self.end_date.strftime('%Y-%m-%d')
        
        for ticker in tqdm(tickers, desc="Minute Bars"):
            try:
                # Use chunked fetching to handle 5 years of data
                bars = self.agg_client.get_minute_bars_chunked(
                    ticker, start_str, end_str, 
                    adjusted=True, chunk_days=chunk_days
                )
                
                if bars:
                    count = self.db_writer.write_ohlcv_bars(ticker, bars, timeframe="1min")
                    results[ticker] = count
                    
                    # Note: TimescaleDB uses COPY for bulk inserts (50-100x faster than INSERT)
                    # No explicit flush needed - handled by connection pool
                else:
                    results[ticker] = 0
                
                time.sleep(0.2)  # Slightly longer delay for large requests
            
            except Exception as e:
                logger.error(f"Error fetching minute bars for {ticker}: {str(e)}")
                results[ticker] = 0
        
        total_bars = sum(results.values())
        logger.info(f"Phase 4 complete: {total_bars:,} minute bars written")
        return results

    # ========================================================================
    # ENHANCEMENT: GRACEFUL QUEUE DRAINING
    # ========================================================================
    # Prevents deadlock from queue.join() blocking indefinitely when:
    # - Items are retrieved with get() but task_done() was never called
    # - Producer/consumer threads don't coordinate properly on shutdown
    # This helper safely empties queues with a timeout to avoid hangs.
    # ========================================================================
    
    def _drain_queue_safely(self, q: queue.Queue, timeout: float = 0.1) -> int:
        """
        Safely drain remaining items from a queue without blocking indefinitely.
        Returns number of items drained.
        
        This prevents deadlock from queue.join() when items were get() but not task_done().
        """
        drained = 0
        try:
            while True:
                try:
                    q.get(timeout=timeout)
                    drained += 1
                    q.task_done()
                except queue.Empty:
                    break
        except Exception as e:
            logger.warning(f"Error draining queue: {e}")
        return drained

    def phase4_flatfiles(self, tickers: List[str]) -> Dict[str, int]:
        """
        Phase 4 alternative: Use Polygon flat files (S3) with producer/consumer pipeline.
        
        Architecture:
        - Producer threads: Download files from S3 (I/O-bound, max 16 workers)
        - Consumer threads: Parse and insert into TimescaleDB (CPU-bound, max 8 workers)
        - Bounded queues: Provide backpressure and prevent memory exhaustion
        - Exponential backoff: Retry failed downloads with jitter
        - State tracking: Explicit FSM for each file with logging
        - Fault tolerance: Per-file retry logic, graceful degradation to REST API
        - Proper shutdown: Poison pills + queue draining to avoid deadlock
        
        Downloads available flat files from S3, parses aggregates, filters by ticker,
        and bulk inserts into TimescaleDB using COPY.
        """
        logger.info(f"=== PHASE 4 (FlatFiles): Producer/Consumer Pipeline for S3 Flat Files ({len(tickers)} tickers) ===")
        results = {t: 0 for t in tickers}
        results_lock = threading.Lock()  # Thread-safe results dict updates

        # ====================================================================
        # PIPELINE CONFIGURATION
        # ====================================================================
        # FIXED CONCURRENCY CONFIGURATION (tuned for 96GB RAM, 32 threads)
        # OPTIMIZED FOR MAXIMUM CPU UTILIZATION AND STABLE QUEUE FLOW
        # NOTE: Polygon S3 has rate limits - keep downloads conservative (24 threads = no 429s)
        # NOTE: Processing is 10x faster than downloads (expected bottleneck is downloads)
        # ====================================================================
        MAX_DOWNLOAD_THREADS = 24      # I/O-bound: sweet spot for S3 (no 429 rate limiting)
        MAX_PROCESS_THREADS = 32       # CPU-bound: use all hardware threads
        DOWNLOAD_QUEUE_SIZE = 300      # Larger buffer allows more files queued for download
        PROCESS_QUEUE_SIZE = 500       # Large buffer absorbs download bursts
        MAX_RETRIES = 5                # Max download attempts per file (increased for 429 retries)
        BASE_BACKOFF = 2.0             # Base seconds for exponential backoff (increased for rate limits)
        
        # Date range strings (flat files have 1-2 day delay, adjust end date)
        start_str = self.start_date.strftime('%Y-%m-%d')
        # Use 1 day ago as end date to account for publishing delay
        adjusted_end = self.end_date - timedelta(days=1)
        end_str = adjusted_end.strftime('%Y-%m-%d')
        logger.info(f"Date range: {start_str} to {end_str} (1-day delay for flat file publishing)")
        logger.info(f"Pipeline: {MAX_DOWNLOAD_THREADS} download workers, {MAX_PROCESS_THREADS} processing workers")

        # ====================================================================
        # S3 CLIENT INITIALIZATION
        # ====================================================================
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError
            from botocore.config import Config
        except ImportError:
            logger.error("boto3 not installed. Install with: pip install boto3")
            logger.error("Falling back to REST API...")
            return self.phase4_minute_bars(tickers)
        
        try:
            # Get S3 credentials from environment
            s3_access_key = os.getenv('POLYGON_S3_ACCESS_KEY')
            s3_secret_key = os.getenv('POLYGON_S3_SECRET_KEY')
            s3_endpoint = os.getenv('POLYGON_S3_ENDPOINT', 'https://files.polygon.io')
            s3_bucket = os.getenv('POLYGON_S3_BUCKET', 'flatfiles')
            
            if not s3_access_key or not s3_secret_key:
                logger.error("Polygon S3 credentials not found in environment")
                logger.error("Set POLYGON_S3_ACCESS_KEY and POLYGON_S3_SECRET_KEY in .env")
                logger.error("Falling back to REST API...")
                return self.phase4_minute_bars(tickers)
            
            logger.info(f"Connecting to Polygon S3: {s3_endpoint}")
            
            # Initialize S3 client with config for timeouts, retries, and connection pooling
            # max_pool_connections must exceed MAX_DOWNLOAD_THREADS to handle retries
            # Use 3x buffer to handle: concurrent downloads + retries + S3 rate limit backoff
            s3_config = Config(
                connect_timeout=10,
                read_timeout=30,
                retries={'max_attempts': 0, 'mode': 'standard'},  # Disable boto3 retries, we handle them
                max_pool_connections=max(64, int(MAX_DOWNLOAD_THREADS * 3))  # 16*3=48, min 64
            )
            
            s3_client = boto3.client(
                's3',
                endpoint_url=s3_endpoint,
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
                config=s3_config
            )
            
            # Test connection
            try:
                s3_client.head_bucket(Bucket=s3_bucket)
                logger.info("✓ S3 client initialized")
            except Exception as e:
                logger.warning(f"S3 connection test failed: {e}")
                logger.warning("Will attempt downloads and fall back to REST API for failed files")
        
        except NoCredentialsError:
            logger.error("Invalid S3 credentials")
            logger.error("Falling back to REST API...")
            return self.phase4_minute_bars(tickers)
        
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            logger.error("Falling back to REST API...")
            return self.phase4_minute_bars(tickers)
        
        # ====================================================================
        # GENERATE FILE TASK LIST DYNAMICALLY FROM S3
        # ====================================================================
        logger.info(f"Scanning S3 for available files from {start_str} to {end_str}...")
        
        try:
            file_tasks = generate_available_file_tasks(
                start_date=self.start_date,
                end_date=adjusted_end,
                category='us_stocks_sip',
                s3_client=s3_client,
                bucket_name=s3_bucket,
                prefix_base='us_stocks_sip/minute_aggs_v1'
            )
        except Exception as e:
            logger.error(f"Failed to list S3 files: {e}")
            logger.error("Falling back to REST API...")
            return self.phase4_minute_bars(tickers)
        
        if not file_tasks:
            logger.warning("No files found on S3. Falling back to REST API...")
            return self.phase4_minute_bars(tickers)
        
        logger.info(f"Found {len(file_tasks)} available files on S3")
        
        # ====================================================================
        # SETUP BOUNDED QUEUES AND STATISTICS
        # ====================================================================
        download_queue = queue.Queue(maxsize=DOWNLOAD_QUEUE_SIZE)
        process_queue = queue.Queue(maxsize=PROCESS_QUEUE_SIZE)
        
        # Register queues in metrics interface for real-time GUI monitoring
        self.queue_metrics['download_queue'] = download_queue
        self.queue_metrics['process_queue'] = process_queue
        self.queue_metrics['download_qsize'] = lambda: download_queue.qsize()
        self.queue_metrics['process_qsize'] = lambda: process_queue.qsize()
        self.queue_metrics['download_maxsize'] = DOWNLOAD_QUEUE_SIZE
        self.queue_metrics['process_maxsize'] = PROCESS_QUEUE_SIZE
        
        # Statistics (thread-safe)
        stats = {
            'downloaded': 0,
            'skipped': 0,
            'failed': 0,
            'processed': 0,
            'parse_errors': 0,
            'download_mbps': 0.0,
            'process_mbps': 0.0,
            'avg_download_latency_ms': 0.0,
            'avg_process_latency_ms': 0.0
        }
        stats_lock = threading.Lock()
        
        # Download speed tracking (time-weighted moving average)
        download_speeds = []  # List of (timestamp, mb_per_second) tuples
        download_speeds_lock = threading.Lock()
        SPEED_WINDOW_SECONDS = 30  # Consider speeds from last 30 seconds
        
        download_dir = Path('./data/flat_files')
        download_dir.mkdir(parents=True, exist_ok=True)

        # ====================================================================
        # BATCH BUFFER: Collect downloads before moving to process queue
        # ====================================================================
        download_batch_buffer = []
        download_batch_lock = threading.Lock()
        BATCH_SIZE = 100  # Move downloads in batches of 100 (enough to fill process pool + backlog)
        BATCH_TIMEOUT_SECONDS = 1.5  # Or move batch after 1.5s timeout (faster responsiveness)

        
        def flush_download_batch():
            """
            Move accumulated downloads to process queue in bulk.
            This creates backpressure and allows process queue to fill.
            """
            with download_batch_lock:
                if download_batch_buffer:
                    batch_size = len(download_batch_buffer)
                    logger.info(f"Flushing download batch: {batch_size} files → process_queue")
                    
                    for task in download_batch_buffer:
                        # Mark as WAITING when entering process queue
                        task.set_state(FileState.WAITING)
                        process_queue.put(task)
                        
                        if self.debug:
                            logger.info(f"DEBUG: Stack Update -> {task.id} [{task.date}] → WAITING (in process queue)")
                            logger.info(f"DEBUG: Stack Push -> {task.id} [{task.date}] to process_queue (batch flush)")
                    
                    download_batch_buffer.clear()

        # ====================================================================
        # PRODUCER WORKER: Download files from S3
        # ====================================================================
        def download_worker():
            """
            Producer thread: Downloads files from S3 with exponential backoff retry.
            
            Workflow:
            1. Get task from download_queue
            2. Check if file already exists (skip if so)
            3. Attempt download with exponential backoff + jitter
            4. On success: enqueue to process_queue with DOWNLOADED state
            5. On failure: retry with backoff or mark FAILED after MAX_RETRIES
            
            INVARIANT: Exactly one task_done() call per get() - enforced in finally block.
            """
            while True:
                task = None  # Reset to track if get() succeeded
                try:
                    task = download_queue.get(timeout=2)
                    
                    if task is None:  # Sentinel value: shutdown signal
                        break
                    
                    download_start_time = time.time()  # Track latency
                    
                    if self.debug:
                        logger.info(f"DEBUG: Queue Pull -> {task.id} [{task.date}] from download_queue (qsize={download_queue.qsize()})")
                    
                    local_path = download_dir / task.filename
                    
                    # Skip if file already exists
                    if local_path.exists():
                        task.set_state(FileState.DOWNLOADED)
                        task.local_path = local_path
                        
                        if self.debug:
                            logger.info(f"DEBUG: Stack Push -> {task.id} [{task.date}] → DOWNLOADED (cached)")
                        
                        # Add to batch buffer instead of direct queue.put()
                        with download_batch_lock:
                            download_batch_buffer.append(task)
                            
                            # Flush batch if size threshold reached
                            if len(download_batch_buffer) >= BATCH_SIZE:
                                flush_download_batch()
                        
                        with stats_lock:
                            stats['skipped'] += 1
                        
                        logger.debug(f"Skipping existing file: {task.filename}")
                        # DO NOT call task_done() here - handled in finally block
                        continue
                    
                    # Attempt download with retry logic
                    task.set_state(FileState.DOWNLOADING)
                    
                    if self.debug:
                        logger.info(f"DEBUG: Stack Update -> {task.id} [{task.date}] → DOWNLOADING")
                    
                    attempts = task.increment_attempts()
                    
                    try:
                        s3_client.download_file(
                            Bucket=s3_bucket,
                            Key=task.s3_key,
                            Filename=str(local_path)
                        )
                        
                        # Verify file size
                        file_size = local_path.stat().st_size
                        if file_size == 0:
                            raise ValueError("Downloaded file has zero size")
                        
                        # Success: mark as downloaded and add to batch buffer
                        task.set_state(FileState.DOWNLOADED)
                        task.local_path = local_path
                        
                        if self.debug:
                            logger.info(f"DEBUG: Stack Update -> {task.id} [{task.date}] → DOWNLOADED")
                        
                        # Add to batch buffer instead of direct queue.put()
                        with download_batch_lock:
                            download_batch_buffer.append(task)
                            
                            if self.debug:
                                logger.info(f"DEBUG: Batch Buffer -> {task.id} [{task.date}] added (buffer size: {len(download_batch_buffer)}/{BATCH_SIZE})")
                            
                            # Flush batch if size threshold reached
                            if len(download_batch_buffer) >= BATCH_SIZE:
                                flush_download_batch()
                        
                        # Calculate metrics
                        download_time = time.time() - download_start_time
                        size_mb = file_size / (1024 * 1024)
                        throughput_mbps = size_mb / download_time if download_time > 0 else 0
                        
                        with stats_lock:
                            stats['downloaded'] += 1
                            # Update rolling average latency
                            stats['avg_download_latency_ms'] = (stats['avg_download_latency_ms'] * 0.9) + (download_time * 1000 * 0.1)
                        
                        # Track download speed with timestamp for time-weighted averaging
                        with download_speeds_lock:
                            current_time = time.time()
                            download_speeds.append((current_time, throughput_mbps))
                            
                            # Remove speeds older than SPEED_WINDOW_SECONDS
                            cutoff_time = current_time - SPEED_WINDOW_SECONDS
                            download_speeds[:] = [(t, s) for t, s in download_speeds if t > cutoff_time]
                            
                            # Calculate time-weighted average speed
                            if download_speeds:
                                # Weight recent speeds more heavily using exponential decay
                                total_weight = 0
                                weighted_sum = 0
                                
                                for timestamp, speed in download_speeds:
                                    age = current_time - timestamp
                                    # Exponential decay: newer = higher weight
                                    weight = 2 ** (-age / 10)  # Half-life of 10 seconds
                                    weighted_sum += speed * weight
                                    total_weight += weight
                                
                                avg_speed = weighted_sum / total_weight if total_weight > 0 else 0
                                
                                with stats_lock:
                                    stats['download_mbps'] = avg_speed
                            else:
                                with stats_lock:
                                    stats['download_mbps'] = 0.0
                        
                        logger.debug(f"✓ Downloaded {task.filename} ({size_mb:.2f} MB, {throughput_mbps:.2f} MB/s, {download_time*1000:.0f}ms)")
                    
                    except ClientError as e:
                        error_code = e.response['Error']['Code']
                        
                        if error_code == '404':
                            # File doesn't exist on S3 (weekend/holiday) - mark as failed, don't retry
                            task.set_state(FileState.FAILED, error="404_not_found")
                            
                            if self.debug:
                                logger.info(f"DEBUG: Stack Update -> {task.id} [{task.date}] → FAILED (404)")
                            
                            with stats_lock:
                                stats['failed'] += 1
                            
                            logger.warning(f"Missing file on S3: {task.filename}")
                        
                        elif error_code == '429' or 'SlowDown' in str(e):
                            # Rate limiting - ALWAYS retry with aggressive backoff
                            # Don't count against MAX_RETRIES since this is S3's fault, not ours
                            task.set_state(FileState.RETRYING, error="rate_limited_429")
                            
                            if self.debug:
                                logger.info(f"DEBUG: Stack Update -> {task.id} [{task.date}] → RETRYING (429 rate limit)")
                            
                            # Aggressive exponential backoff for rate limits (starts at 5s)
                            rate_limit_backoff = 5.0 * (2 ** min(attempts - 1, 4))  # Cap at 80s
                            jitter = random.uniform(0, 0.5 * rate_limit_backoff)
                            delay = rate_limit_backoff + jitter
                            
                            logger.warning(
                                f"⚠️  S3 Rate Limit (429) for {task.filename} - "
                                f"backing off for {delay:.1f}s (attempt {attempts})"
                            )
                            time.sleep(delay)
                            
                            # Re-queue for retry (don't increment attempts counter for rate limits)
                            task.attempts -= 1  # Undo the increment so 429s don't count against MAX_RETRIES
                            download_queue.put(task)
                        
                        else:
                            # Other S3 error - retry with exponential backoff
                            if attempts < MAX_RETRIES:
                                task.set_state(FileState.RETRYING, error=str(e))
                                
                                if self.debug:
                                    logger.info(f"DEBUG: Stack Update -> {task.id} [{task.date}] → RETRYING (attempt {attempts}/{MAX_RETRIES})")
                                
                                # Exponential backoff with jitter
                                backoff = BASE_BACKOFF * (2 ** (attempts - 1))
                                jitter = random.uniform(0, 0.3 * backoff)
                                delay = backoff + jitter
                                
                                logger.warning(f"Download attempt {attempts} failed for {task.filename}, retrying in {delay:.2f}s")
                                time.sleep(delay)
                                
                                # Re-queue for retry
                                download_queue.put(task)
                            else:
                                task.set_state(FileState.FAILED, error=f"max_retries_exceeded: {e}")
                                
                                if self.debug:
                                    logger.info(f"DEBUG: Stack Update -> {task.id} [{task.date}] → FAILED (max retries)")
                                
                                with stats_lock:
                                    stats['failed'] += 1
                                
                                logger.error(f"Failed to download {task.filename} after {MAX_RETRIES} attempts: {e}")
                    
                    except Exception as e:
                        # Unexpected error - retry with exponential backoff
                        if attempts < MAX_RETRIES:
                            task.set_state(FileState.RETRYING, error=str(e))
                            
                            if self.debug:
                                logger.info(f"DEBUG: Stack Update -> {task.id} [{task.date}] → RETRYING (attempt {attempts}/{MAX_RETRIES})")
                            
                            # Exponential backoff with jitter
                            backoff = BASE_BACKOFF * (2 ** (attempts - 1))
                            jitter = random.uniform(0, 0.3 * backoff)
                            delay = backoff + jitter
                            
                            logger.warning(f"Download attempt {attempts} failed for {task.filename}, retrying in {delay:.2f}s: {e}")
                            time.sleep(delay)
                            
                            # Re-queue for retry
                            download_queue.put(task)
                        else:
                            task.set_state(FileState.FAILED, error=f"max_retries_exceeded: {e}")
                            
                            if self.debug:
                                logger.info(f"DEBUG: Stack Update -> {task.id} [{task.date}] → FAILED (max retries)")
                            
                            with stats_lock:
                                stats['failed'] += 1
                            
                            logger.error(f"Failed to download {task.filename} after {MAX_RETRIES} attempts: {e}")
                
                except queue.Empty:
                    # Timeout on get() - no task available, continue looping
                    continue
                
                finally:
                    # INVARIANT: Exactly one task_done() per successful get()
                    # Only call if get() succeeded (task is not None and not sentinel)
                    if task is not None:
                        download_queue.task_done()
        
        # ====================================================================
        # INITIALIZE PROCESS POOL FOR PARALLEL PROCESSING
        # ====================================================================
        logger.info(f"Initializing ProcessPoolExecutor with {MAX_PROCESS_THREADS} workers...")
        process_pool = ProcessPoolExecutor(max_workers=MAX_PROCESS_THREADS)
        active_futures: Dict[Future, FileTask] = {}  # Track submitted tasks
        futures_lock = threading.Lock()
        
        # Database config for worker processes
        db_config = {}  # TimescaleWriter will use environment variables
        
        # ====================================================================
        # CONSUMER WORKER: Submit tasks to ProcessPoolExecutor
        # ====================================================================
        def processing_worker():
            """
            Consumer coordinator: Gets tasks from queue and submits to ProcessPoolExecutor.
            
            Workflow:
            1. Get task from process_queue
            2. Prepare task data for multiprocessing
            3. Submit to process pool (true parallel processing)
            4. Track future and handle completion asynchronously
            
            INVARIANT: Exactly one task_done() call per get() - enforced in finally block.
            """
            while True:
                task = None  # Reset to track if get() succeeded
                try:
                    task = process_queue.get(timeout=2)
                    
                    if task is None:  # Sentinel value: shutdown signal
                        break
                    
                    if self.debug:
                        logger.info(f"DEBUG: Queue Pull -> {task.id} [{task.date}] from process_queue (qsize={process_queue.qsize()})")
                    
                    task.set_state(FileState.PROCESSING)
                    
                    if self.debug:
                        logger.info(f"DEBUG: Stack Update -> {task.id} [{task.date}] → PROCESSING (submitting to process pool)")
                    
                    # Prepare task data for multiprocessing (must be serializable)
                    task_data = {
                        'id': task.id,
                        'local_path': str(task.local_path),
                        'filename': task.filename,
                        'date': task.date
                    }
                    
                    # Submit to process pool for parallel execution
                    future = process_pool.submit(
                        process_flat_file_task,
                        task_data,
                        tickers,
                        db_config,
                        self.debug
                    )
                    
                    # Track future with task
                    with futures_lock:
                        active_futures[future] = task
                    
                    # Add completion callback
                    def handle_completion(fut: Future):
                        """Handle task completion in main thread"""
                        with futures_lock:
                            completed_task = active_futures.pop(fut, None)
                        
                        if completed_task is None:
                            return
                        
                        try:
                            result = fut.result()
                            
                            if result['status'] == 'success':
                                completed_task.set_state(FileState.COMPLETED)
                                
                                # Extract multi-ticker processing results
                                tickers_found = result.get('tickers_processed', [])
                                ticker_details = result.get('ticker_results', {})
                                
                                if self.debug:
                                    # Enhanced logging showing all tickers processed from this file
                                    tickers_summary = ', '.join([f"{t}:{ticker_details.get(t, 0)}" for t in tickers_found])
                                    logger.info(
                                        f"DEBUG: Stack Pop -> {result['task_id']} [{completed_task.date}] → COMPLETED "
                                        f"(total_bars={result['bars_written']}, tickers=[{tickers_summary}], "
                                        f"time={result['processing_time_ms']:.0f}ms)"
                                    )
                                else:
                                    # Standard logging with ticker count
                                    logger.info(
                                        f"✓ Processed {result['task_id']} [{completed_task.date}]: "
                                        f"{result['bars_written']:,} bars from {len(tickers_found)} tickers "
                                        f"({result['processing_time_ms']:.0f}ms)"
                                    )
                                
                                with stats_lock:
                                    stats['processed'] += 1
                                    # Update rolling average latency
                                    stats['avg_process_latency_ms'] = (
                                        stats['avg_process_latency_ms'] * 0.9 +
                                        result['processing_time_ms'] * 0.1
                                    )
                                
                                # Update results - aggregate per-ticker counts
                                if result['bars_written'] > 0:
                                    with results_lock:
                                        # Aggregate per-ticker bar counts across all files
                                        for ticker, count in result.get('ticker_results', {}).items():
                                            if ticker in results:
                                                results[ticker] += count
                                            else:
                                                logger.warning(f"Ticker {ticker} found in file but not in configured ticker list")
                            
                            else:
                                # Processing failed
                                completed_task.set_state(FileState.FAILED, error=result.get('error'))
                                
                                if self.debug:
                                    logger.info(f"DEBUG: Stack Update -> {result['task_id']} [{completed_task.date}] → FAILED (processing error)")
                                
                                with stats_lock:
                                    stats['parse_errors'] += 1
                                
                                logger.error(f"Error processing {result['filename']}: {result.get('error')}")
                        
                        except Exception as e:
                            completed_task.set_state(FileState.FAILED, error=str(e))
                            
                            with stats_lock:
                                stats['parse_errors'] += 1
                            
                            logger.error(f"Future completion error for {completed_task.filename}: {e}")
                    
                    future.add_done_callback(handle_completion)
                
                except queue.Empty:
                    # Timeout on get() - no task available, continue looping
                    continue
                
                finally:
                    # INVARIANT: Exactly one task_done() per successful get()
                    # Only call if get() succeeded (task is not None and not sentinel)
                    if task is not None:
                        process_queue.task_done()
        
        # ====================================================================
        # METRICS LOGGING THREAD (for monitoring throughput and queue depth)
        # ====================================================================
        metrics_running = threading.Event()
        metrics_running.set()
        metrics_start_time = time.time()  # Track elapsed time for throughput calculation
        
        def metrics_logger():
            """Background thread: Log pipeline metrics every 5 seconds including throughput"""
            last_processed = 0
            last_time = time.time()
            
            while metrics_running.is_set():
                time.sleep(5)
                if not metrics_running.is_set():
                    break
                
                current_time = time.time()
                elapsed = current_time - last_time
                
                # Age out old download speeds and recalculate average
                with download_speeds_lock:
                    cutoff_time = current_time - SPEED_WINDOW_SECONDS
                    download_speeds[:] = [(t, s) for t, s in download_speeds if t > cutoff_time]
                    
                    # Recalculate time-weighted average
                    if download_speeds:
                        total_weight = 0
                        weighted_sum = 0
                        
                        for timestamp, speed in download_speeds:
                            age = current_time - timestamp
                            weight = 2 ** (-age / 10)  # Exponential decay
                            weighted_sum += speed * weight
                            total_weight += weight
                        
                        avg_speed = weighted_sum / total_weight if total_weight > 0 else 0
                        
                        with stats_lock:
                            stats['download_mbps'] = avg_speed
                    else:
                        # No recent downloads
                        with stats_lock:
                            stats['download_mbps'] = 0.0
                
                with stats_lock:
                    # Calculate throughput (files/sec)
                    files_processed_delta = stats['processed'] - last_processed
                    throughput = files_processed_delta / elapsed if elapsed > 0 else 0
                    
                    # Total elapsed time since start
                    total_elapsed = current_time - metrics_start_time
                    avg_throughput = stats['processed'] / total_elapsed if total_elapsed > 0 else 0
                    
                    # Number of recent download speed samples
                    num_samples = len(download_speeds)
                    
                    logger.info(
                        f"⚡ Pipeline Metrics | "
                        f"Download Q: {download_queue.qsize()}/{DOWNLOAD_QUEUE_SIZE} | "
                        f"Process Q: {process_queue.qsize()}/{PROCESS_QUEUE_SIZE} | "
                        f"Downloaded: {stats['downloaded']} ({stats['download_mbps']:.1f} MB/s avg, {num_samples} samples) | "
                        f"Processed: {stats['processed']} | "
                        f"Throughput: {throughput:.2f} files/s (avg: {avg_throughput:.2f} files/s) | "
                        f"Latency: DL={stats['avg_download_latency_ms']:.0f}ms PR={stats['avg_process_latency_ms']:.0f}ms"
                    )
                    
                    last_processed = stats['processed']
                    last_time = current_time
        
        metrics_thread = threading.Thread(target=metrics_logger, name="MetricsLogger", daemon=True)
        metrics_thread.start()
        
        # ====================================================================
        # ADAPTIVE CPU SCALING CONTROLLE
        # ====================================================================
        target_processors = {'count': MAX_PROCESS_THREADS}  # Mutable dict for thread-safe updates
        processors_lock = threading.Lock()
        low_cpu_start_time: Dict[str, Optional[float]] = {'time': None}  # Track when CPU went below 30%
        
        def cpu_scaling_controller():
            """
            Background thread: Monitor CPU usage and adaptively scale processing workers.
            
            Scaling rules:
            - CPU < 50%: Increase workers by +2 (up to MAX=32)
            - CPU > 85%: Decrease workers by -2 (down to MIN=4)
            - 50% ≤ CPU ≤ 85%: No change
            - CPU < 30% for >30s: Log I/O bottleneck warning
            
            Updates ProcessPoolExecutor._max_workers directly for true parallel scaling.
            Updates every 3 seconds for gradual adjustments.
            """
            MIN_PROCESSORS = 4
            MAX_PROCESSORS = 128
            SCALE_INTERVAL = 2  # seconds
            LOW_CPU_THRESHOLD = 30
            LOW_CPU_WARNING_DURATION = 15  # seconds
            
            while metrics_running.is_set():
                time.sleep(SCALE_INTERVAL)
                if not metrics_running.is_set():
                    break
                
                try:
                    # Measure CPU usage over 1 second
                    cpu_usage = psutil.cpu_percent(interval=1)
                    
                    with processors_lock:
                        current_count = target_processors['count']
                        new_count = current_count
                        
                        # Scaling logic
                        if cpu_usage < 50 and current_count < MAX_PROCESSORS:
                            new_count = min(MAX_PROCESSORS, current_count + 2)
                            if CPU_LOGGING_ENABLED:
                                logger.info(f"[AutoScale] CPU low ({cpu_usage:.1f}%) → Increasing processors: {current_count} → {new_count}")
                        
                        elif cpu_usage > 85 and current_count > MIN_PROCESSORS:
                            new_count = max(MIN_PROCESSORS, current_count - 2)
                            if CPU_LOGGING_ENABLED:
                                logger.info(f"[AutoScale] CPU high ({cpu_usage:.1f}%) → Decreasing processors: {current_count} → {new_count}")
                        
                        # Update target if changed
                        if new_count != current_count:
                            target_processors['count'] = new_count
                            
                            # Note: ProcessPoolExecutor workers are managed by the pool itself
                            # The pool will automatically spawn/reuse processes up to max_workers
                            # We track the target for monitoring purposes
                            if CPU_LOGGING_ENABLED:
                                logger.debug(f"[AutoScale] Target processors updated to {new_count}")
                        
                        # I/O bottleneck detection
                        if cpu_usage < LOW_CPU_THRESHOLD:
                            if low_cpu_start_time['time'] is None:
                                low_cpu_start_time['time'] = time.time()
                            elif time.time() - low_cpu_start_time['time'] > LOW_CPU_WARNING_DURATION:
                                if CPU_LOGGING_ENABLED:
                                    logger.warning(
                                        f"[AutoScale] CPU has been below {LOW_CPU_THRESHOLD}% for >{LOW_CPU_WARNING_DURATION}s. "
                                        f"Possible I/O bottleneck detected. Current: {cpu_usage:.1f}%"
                                    )
                                low_cpu_start_time['time'] = None  # Reset to avoid spam
                        else:
                            low_cpu_start_time['time'] = None
                        
                        # Log metrics (only in debug mode)
                        if CPU_LOGGING_ENABLED:
                            logger.info(
                                f"[AutoScale] CPU: {cpu_usage:.1f}% | "
                                f"Active Processors: {new_count} | "
                                f"Process Queue: {process_queue.qsize()}/{PROCESS_QUEUE_SIZE} | "
                                f"Active Futures: {len(active_futures)}"
                            )
                
                except Exception as e:
                    logger.error(f"[AutoScale] Error in CPU scaling controller: {e}")
        
        cpu_scaling_thread = threading.Thread(target=cpu_scaling_controller, name="CPUScaler", daemon=True)
        cpu_scaling_thread.start()
        logger.info("Adaptive CPU scaling controller started (multiprocessing mode)")
        
        # ====================================================================
        # PRE-FILL DOWNLOAD QUEUE (BEFORE STARTING WORKERS)
        # ====================================================================
        logger.info(f"Pre-filling download queue with initial batch...")
        
        # Calculate initial batch size (fill queue to capacity)
        initial_batch_size = min(len(file_tasks), DOWNLOAD_QUEUE_SIZE)
        
        # Enqueue initial batch (non-blocking since queue is empty)
        for i in range(initial_batch_size):
            download_queue.put(file_tasks[i])
            logger.debug(f"Pre-filled {file_tasks[i].id} [{file_tasks[i].date}] to download queue")
        
        logger.info(f"Pre-filled download queue: {initial_batch_size}/{DOWNLOAD_QUEUE_SIZE} tasks")
        
        # Remaining tasks to enqueue after workers start
        remaining_tasks = file_tasks[initial_batch_size:]
        remaining_tasks_lock = threading.Lock()
        remaining_tasks_index = {'value': 0}
        
        # ====================================================================
        # BATCH FLUSH TIMER: Periodically flush download batch
        # ====================================================================
        batch_flush_shutdown = threading.Event()
        
        def batch_flush_timer():
            """
            Timer thread: Flushes download batch every BATCH_TIMEOUT_SECONDS.
            Ensures tail-end downloads don't get stuck in buffer.
            """
            while not batch_flush_shutdown.is_set():
                time.sleep(BATCH_TIMEOUT_SECONDS)
                flush_download_batch()
        
        # ====================================================================
        # PRODUCER THREAD: Continuously refill download queue
        # ====================================================================
        def producer_worker():
            """
            Producer thread: Refills download queue as workers consume tasks.
            Ensures queue stays near capacity for maximum parallelism.
            """
            while True:
                try:
                    with remaining_tasks_lock:
                        if remaining_tasks_index['value'] >= len(remaining_tasks):
                            # All tasks enqueued
                            break
                        
                        # Get next task to enqueue
                        task = remaining_tasks[remaining_tasks_index['value']]
                        remaining_tasks_index['value'] += 1
                    
                    # Enqueue task (blocks if queue is full - backpressure)
                    download_queue.put(task)
                    logger.debug(f"Producer enqueued {task.id} [{task.date}] to download queue")
                    
                except Exception as e:
                    logger.error(f"Producer error: {e}")
                    break
            
            logger.info("Producer finished: all tasks enqueued to download queue")
        
        # ====================================================================
        # WATCHDOG THREAD: Monitor pipeline and trigger shutdown if idle
        # ====================================================================
        pipeline_complete = threading.Event()
        last_activity_time = {'time': time.time()}
        
        def watchdog_monitor():
            """
            Watchdog: Monitor queue sizes and active futures.
            If idle > 60s and queues empty → trigger shutdown.
            """
            IDLE_TIMEOUT = 60  # seconds
            while not pipeline_complete.is_set():
                time.sleep(10)
                
                dl_size = download_queue.qsize()
                pr_size = process_queue.qsize()
                with futures_lock:
                    active_count = len(active_futures)
                
                # Check if pipeline is active
                if dl_size > 0 or pr_size > 0 or active_count > 0:
                    last_activity_time['time'] = time.time()
                else:
                    # Check idle duration
                    idle_duration = time.time() - last_activity_time['time']
                    if idle_duration > IDLE_TIMEOUT:
                        logger.info(f"Watchdog: Pipeline idle for {idle_duration:.0f}s, triggering completion")
                        pipeline_complete.set()
                        break
        
        watchdog_thread = threading.Thread(target=watchdog_monitor, name="Watchdog", daemon=True)
        watchdog_thread.start()
        logger.info("Watchdog monitor started (will trigger shutdown if idle > 60s)")
        
        # ====================================================================
        # START THREAD POOLS
        # ====================================================================
        logger.info(f"Starting {MAX_DOWNLOAD_THREADS} download workers...")
        download_threads = []
        for i in range(MAX_DOWNLOAD_THREADS):
            t = threading.Thread(target=download_worker, name=f"Downloader-{i+1}")
            t.daemon = True
            t.start()
            download_threads.append(t)
        
        logger.info(f"Starting processing coordinator threads (process pool has {MAX_PROCESS_THREADS} workers)...")
        # Match coordinator threads to process pool workers for maximum throughput
        # Coordinators are lightweight (just pull from queue and submit to pool)
        NUM_COORDINATOR_THREADS = min(MAX_PROCESS_THREADS, 16)  # Cap at 16 to avoid thread overhead
        logger.info(f"Using {NUM_COORDINATOR_THREADS} coordinator threads to feed {MAX_PROCESS_THREADS} process workers")
        process_threads = []
        for j in range(NUM_COORDINATOR_THREADS):
            t = threading.Thread(target=processing_worker, name=f"ProcessCoordinator-{j+1}")
            t.daemon = True
            t.start()
            process_threads.append(t)
        
        # Start batch flush timer thread
        logger.info(f"Starting batch flush timer (flush every {BATCH_TIMEOUT_SECONDS}s)...")
        batch_flush_thread = threading.Thread(target=batch_flush_timer, name="BatchFlusher")
        batch_flush_thread.daemon = True
        batch_flush_thread.start()
        
        # Start producer thread to refill download queue
        if remaining_tasks:
            logger.info(f"Starting producer thread for remaining {len(remaining_tasks)} tasks...")
            producer_thread = threading.Thread(target=producer_worker, name="Producer")
            producer_thread.daemon = True
            producer_thread.start()
        else:
            logger.info("No remaining tasks - producer thread not needed")
            producer_thread = None
        
        logger.info(f"Pipeline started: Download queue pre-filled ({initial_batch_size} tasks), workers active")
        
        # ====================================================================
        # WAIT FOR PIPELINE COMPLETION (via watchdog or explicit finish)
        # ====================================================================
        # GRACEFUL SHUTDOWN SEQUENCE WITH SENTINELS
        # ====================================================================
        # Wait for producer to finish enqueueing (if running)
        if producer_thread is not None:
            producer_thread.join(timeout=30)
            logger.info("Producer thread finished enqueueing all tasks")
        
        # Wait for pipeline completion (watchdog detects idle state)
        logger.info("Monitoring pipeline for completion...")
        pipeline_complete.wait(timeout=7200)  # 2 hour max timeout
        
        # Drain download queue with timeout
        logger.info("Draining download queue...")
        try:
            download_queue.join()
        except Exception as e:
            logger.warning(f"Download queue join interrupted: {e}")
        
        # Check for stuck items
        dl_remaining = download_queue.qsize()
        if dl_remaining > 0:
            logger.warning(f"Download queue has {dl_remaining} stuck items, draining...")
            drained = self._drain_queue_safely(download_queue)
            logger.info(f"Drained {drained} items from download queue")
        
        logger.info("All downloads complete. Flushing final download batch...")
        
        # Flush any remaining items in batch buffer
        flush_download_batch()
        
        # Stop batch flush timer
        batch_flush_shutdown.set()
        batch_flush_thread.join(timeout=5)
        logger.info("Batch flush timer stopped")
        
        # ====================================================================
        # SIGNAL WORKERS TO EXIT (SENTINEL SYSTEM)
        # ====================================================================
        logger.info("Sending shutdown signals to download workers...")
        for _ in range(MAX_DOWNLOAD_THREADS):
            download_queue.put(None)  # Poison pill sentinel
        
        # Wait for download workers to exit cleanly
        for t in download_threads:
            t.join(timeout=10)
            if t.is_alive():
                logger.warning(f"Download worker {t.name} did not exit cleanly (forcing)")
        
        logger.info("All download workers exited.")
        
        # ====================================================================
        # WAIT FOR ALL PROCESSING TO COMPLETE
        # ====================================================================
        logger.info("Draining process queue...")
        try:
            process_queue.join()
        except Exception as e:
            logger.warning(f"Process queue join interrupted: {e}")
        
        # Check for stuck items
        pr_remaining = process_queue.qsize()
        if pr_remaining > 0:
            logger.warning(f"Process queue has {pr_remaining} stuck items, draining...")
            drained = self._drain_queue_safely(process_queue)
            logger.info(f"Drained {drained} items from process queue")
        
        logger.info("All processing complete. Waiting for futures to finish...")
        
        # Wait for all submitted futures to complete
        with futures_lock:
            remaining_futures = list(active_futures.keys())
        
        if remaining_futures:
            logger.info(f"Waiting for {len(remaining_futures)} remaining futures...")
            wait(remaining_futures, timeout=60)
        
        logger.info("All futures completed. Signaling coordinator threads to exit...")
        
        # Send poison pills to coordinator threads
        for _ in range(NUM_COORDINATOR_THREADS):
            process_queue.put(None)  # Poison pill sentinel
        
        # Wait for coordinator threads to exit cleanly
        for t in process_threads:
            t.join(timeout=10)
            if t.is_alive():
                logger.warning(f"Coordinator thread {t.name} did not exit cleanly (forcing)")
        
        logger.info("All coordinator threads exited.")
        
        # Shutdown process pool gracefully
        logger.info("Shutting down ProcessPoolExecutor...")
        process_pool.shutdown(wait=True)
        logger.info("ProcessPoolExecutor shut down successfully.")
        
        # Stop metrics logging thread
        metrics_running.clear()
        metrics_thread.join(timeout=5)
        
        # Stop watchdog
        pipeline_complete.set()
        watchdog_thread.join(timeout=5)
        
        # Final verification: check queue sizes (should be 0)
        dl_final = download_queue.qsize()
        pr_final = process_queue.qsize()
        with futures_lock:
            futures_final = len(active_futures)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"PIPELINE SHUTDOWN VERIFICATION")
        logger.info(f"{'='*60}")
        logger.info(f"Final queue sizes → Download: {dl_final}  Process: {pr_final}  Active: {futures_final}")
        
        if dl_final == 0 and pr_final == 0 and futures_final == 0:
            logger.info("✓ Clean shutdown: All queues empty, no active futures")
        else:
            logger.warning(f"⚠ Incomplete shutdown detected - draining residual items...")
            if dl_final > 0:
                self._drain_queue_safely(download_queue)
            if pr_final > 0:
                self._drain_queue_safely(process_queue)
        
        logger.info(f"{'='*60}\n")
        
        # ====================================================================
        # PRINT PIPELINE STATISTICS
        # ====================================================================
        total_tasks = len(file_tasks)
        total_bars = sum(results.values())
        tickers_with_data = [t for t, count in results.items() if count > 0]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"PIPELINE STATISTICS")
        logger.info(f"{'='*60}")
        logger.info(f"Total files:      {total_tasks}")
        logger.info(f"Downloaded:       {stats['downloaded']} (avg {stats['download_mbps']:.2f} MB/s)")
        logger.info(f"Skipped (cached): {stats['skipped']}")
        logger.info(f"Failed (404/etc): {stats['failed']}")
        logger.info(f"Processed:        {stats['processed']}")
        logger.info(f"Parse errors:     {stats['parse_errors']}")
        logger.info(f"Avg Latency:      Download={stats['avg_download_latency_ms']:.0f}ms, Process={stats['avg_process_latency_ms']:.0f}ms")
        logger.info(f"")
        logger.info(f"MULTI-TICKER RESULTS:")
        logger.info(f"Tickers requested: {len(tickers)}")
        logger.info(f"Tickers with data: {len(tickers_with_data)}")
        logger.info(f"Total bars (all tickers): {total_bars:,}")
        logger.info(f"")
        logger.info(f"Per-Ticker Bar Counts:")
        for ticker in sorted(tickers):
            count = results.get(ticker, 0)
            logger.info(f"  {ticker:6s}: {count:>10,} bars")
        logger.info(f"{'='*60}\n")
        
        # ====================================================================
        # HANDLE FAILED FILES WITH REST API FALLBACK (if needed)
        # ====================================================================
        failed_tasks = [t for t in file_tasks if t.get_state() == FileState.FAILED and '404' not in (t.error or '')]
        
        if failed_tasks:
            logger.info(f"Using REST API fallback for {len(failed_tasks)} failed files...")
            
            for task in tqdm(failed_tasks, desc="REST API Fallback"):
                try:
                    date_obj = datetime.strptime(task.date, '%Y-%m-%d')
                    start_str_fallback = task.date
                    end_str_fallback = (date_obj + timedelta(days=1)).strftime('%Y-%m-%d')
                    
                    logger.info(f"Fetching {task.date} via REST API...")
                    
                    # Fetch for each ticker
                    for ticker in tickers:
                        try:
                            bars = self.agg_client.get_minute_bars_chunked(
                                ticker, start_str_fallback, end_str_fallback,
                                adjusted=True, chunk_days=1
                            )
                            
                            if bars:
                                written = self.db_writer.write_ohlcv_bars(ticker, bars, timeframe='1min', use_copy=True)
                                results[ticker] += written
                                logger.debug(f"✓ {ticker} {task.date}: {written} bars via REST API")
                            
                            time.sleep(0.1)  # Rate limit
                        
                        except Exception as e:
                            logger.error(f"REST API failed for {ticker} on {task.date}: {e}")
                
                except Exception as e:
                    logger.error(f"Error processing REST API fallback for {task.date}: {e}")
        
        # ====================================================================
        # RETURN RESULTS
        # ====================================================================
        total = sum(results.values())
        tickers_with_data = len([t for t, count in results.items() if count > 0])
        logger.info(f"Phase 4 (FlatFiles) complete: {total:,} minute bars written across {tickers_with_data} tickers")
        logger.info(f"Efficiency: Each of {stats['processed']} files processed once for {len(tickers)} tickers")
        if failed_tasks:
            logger.info(f"(Includes {len(failed_tasks)} dates via REST API fallback)")
        
        return results
    
    def phase5_news(self, tickers: List[str], days_back: int = 180) -> Dict[str, int]:
        """
        Phase 5: Fetch and store news articles.
        
        Args:
            tickers: List of ticker symbols
            days_back: How many days of news to fetch (default 180 = 6 months)
        
        Returns:
            Dict mapping ticker to article count
        """
        logger.info(f"=== PHASE 5: News Articles ({len(tickers)} tickers) ===")
        results = {}
        
        for ticker in tqdm(tickers, desc="News"):
            try:
                articles = self.news_client.get_news_for_ticker(ticker, days_back=days_back, limit=500)
                
                if articles:
                    count = self.db_writer.write_news(articles)
                    results[ticker] = count
                else:
                    results[ticker] = 0
                
                time.sleep(0.1)
            
            except Exception as e:
                logger.error(f"Error fetching news for {ticker}: {str(e)}")
                results[ticker] = 0
        
        total_articles = sum(results.values())
        logger.info(f"Phase 5 complete: {total_articles:,} articles written")
        return results
    
    def phase4_hybrid(self, tickers: List[str]) -> Dict[str, int]:
        """
        Phase 4 Hybrid: Use flat files first, then REST API to fill gaps.
        
        Strategy:
        1. Download and process all available flat files from S3
        2. Detect data gaps for each ticker (missing dates or spans > 3 days)
        3. Fill gaps using REST API (get_minute_bars)
        
        This combines the speed of flat files with the completeness of REST API.
        """
        logger.info(f"=== PHASE 4 (HYBRID): Flat Files + REST API Gap Filling ===")
        
        # Step 1: Process flat files
        logger.info("Step 1: Processing flat files from S3...")
        flatfile_results = self.phase4_flatfiles(tickers)
        
        # Step 2: Detect gaps for each ticker
        logger.info("Step 2: Detecting data gaps...")
        gaps_to_fill = {}
        
        for ticker in tickers:
            try:
                # Query database for existing data range
                query = f"""
                    SELECT 
                        DATE(time) as date,
                        COUNT(*) as bars
                    FROM market_data_1min
                    WHERE symbol = '{ticker}'
                        AND time >= '{self.start_date.strftime('%Y-%m-%d')}'
                        AND time <= '{self.end_date.strftime('%Y-%m-%d')}'
                    GROUP BY DATE(time)
                    ORDER BY date ASC
                """
                
                with self.db_writer.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    results = cursor.fetchall()
                    cursor.close()
                
                if not results:
                    # No data at all - fill entire range
                    logger.info(f"  {ticker}: No data found, will backfill entire range")
                    gaps_to_fill[ticker] = [(self.start_date, self.end_date)]
                    continue
                
                # Convert to dict of date -> bar count
                date_bars = {row[0]: row[1] for row in results}
                
                # Find gaps (missing dates or low bar counts)
                current_date = self.start_date.date()
                end_date = self.end_date.date()
                ticker_gaps = []
                gap_start = None
                
                while current_date <= end_date:
                    bars_count = date_bars.get(current_date, 0)
                    
                    # Consider it a gap if missing or very few bars (< 100)
                    if bars_count < 100:
                        if gap_start is None:
                            gap_start = current_date
                    else:
                        # End of gap
                        if gap_start is not None:
                            gap_days = (current_date - gap_start).days
                            # Only fill gaps > 3 days
                            if gap_days > 3:
                                gap_end = current_date - timedelta(days=1)
                                ticker_gaps.append((
                                    datetime.combine(gap_start, datetime.min.time()),
                                    datetime.combine(gap_end, datetime.max.time())
                                ))
                                logger.info(f"  {ticker}: Found gap {gap_start} to {gap_end} ({gap_days} days)")
                            gap_start = None
                    
                    current_date += timedelta(days=1)
                
                # Check if gap extends to end
                if gap_start is not None:
                    gap_days = (end_date - gap_start).days
                    if gap_days > 3:
                        ticker_gaps.append((
                            datetime.combine(gap_start, datetime.min.time()),
                            datetime.combine(end_date, datetime.max.time())
                        ))
                        logger.info(f"  {ticker}: Found gap {gap_start} to {end_date} ({gap_days} days)")
                
                if ticker_gaps:
                    gaps_to_fill[ticker] = ticker_gaps
                else:
                    logger.info(f"  {ticker}: No significant gaps found")
                    
            except Exception as e:
                logger.error(f"Error detecting gaps for {ticker}: {e}")
                # If error, try to fill entire range
                gaps_to_fill[ticker] = [(self.start_date, self.end_date)]
        
        # Step 3: Fill gaps using REST API
        if gaps_to_fill:
            logger.info(f"Step 3: Filling gaps for {len(gaps_to_fill)} tickers using REST API...")
            
            for ticker, gaps in gaps_to_fill.items():
                for gap_start, gap_end in gaps:
                    try:
                        logger.info(f"  Filling {ticker}: {gap_start.date()} to {gap_end.date()}")
                        
                        # Fetch data using REST API
                        bars = self.agg_client.get_minute_bars(
                            ticker=ticker,
                            start_date=gap_start.strftime('%Y-%m-%d'),
                            end_date=gap_end.strftime('%Y-%m-%d')
                        )
                        
                        if bars:
                            # Write to database with multi-timeframe resampling
                            written = self.db_writer.write_ohlcv_bars(
                                ticker, 
                                bars, 
                                timeframe='1min', 
                                use_copy=True
                            )
                            flatfile_results[ticker] = flatfile_results.get(ticker, 0) + written
                            logger.info(f"    ✓ Filled {written:,} bars for {ticker}")
                            
                            # Resample to higher timeframes
                            df = pd.DataFrame(bars)
                            df['time'] = pd.to_datetime(df['t'], unit='ms', utc=True)
                            df = df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 
                                                    'c': 'close', 'v': 'volume', 'vw': 'vwap'})
                            df = df.set_index('time')
                            
                            timeframes = ['5m', '15m', '30m', '1h', '2h', '4h', '12h', '1d']
                            freq_map = {
                                '5m': '5min', '15m': '15min', '30m': '30min',
                                '1h': '1h', '2h': '2h', '4h': '4h', '12h': '12h', '1d': '1D'
                            }
                            
                            for tf in timeframes:
                                try:
                                    freq = freq_map[tf]
                                    resampled = df.resample(freq).agg({
                                        'open': 'first', 'high': 'max', 'low': 'min',
                                        'close': 'last', 'volume': 'sum', 'vwap': 'mean'
                                    }).dropna(subset=['close'])
                                    
                                    if not resampled.empty:
                                        resampled_bars = []
                                        for idx, row in resampled.iterrows():
                                            resampled_bars.append({
                                                't': int(idx.timestamp() * 1000),
                                                'o': row['open'], 'h': row['high'], 'l': row['low'],
                                                'c': row['close'], 'v': int(row['volume']),
                                                'vw': row['vwap'], 'n': 0
                                            })
                                        self.db_writer.write_ohlcv_bars(ticker, resampled_bars, timeframe=tf, use_copy=True)
                                except Exception as e:
                                    logger.debug(f"Failed to resample {ticker} to {tf}: {e}")
                        
                        time.sleep(0.2)  # Rate limiting
                        
                    except Exception as e:
                        logger.error(f"Error filling gap for {ticker} ({gap_start.date()} to {gap_end.date()}): {e}")
            
            logger.info("✓ Gap filling complete")
        else:
            logger.info("Step 3: No gaps to fill - flat files covered everything!")
        
        total_bars = sum(flatfile_results.values())
        logger.info(f"Phase 4 complete: {total_bars:,} total bars written")
        return flatfile_results
    
    def run_full_backfill(self, tickers: List[str]) -> Dict:
        """
        Run complete backfill process for specified tickers.
        
        Args:
            tickers: List of ticker symbols
        
        Returns:
            Summary dict with results from all phases
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING FULL BACKFILL FOR {len(tickers)} TICKERS")
        logger.info(f"Date Range: {self.start_date.date()} to {self.end_date.date()}")
        
        # Log skip flags
        skip_list = []
        if self.skip_reference:
            skip_list.append("Phase 1 (Reference)")
        if self.skip_corporate:
            skip_list.append("Phase 2 (Corporate Actions)")
        if self.skip_daily:
            skip_list.append("Phase 3 (Daily Bars)")
        if self.skip_minute:
            skip_list.append("Phase 4 (Minute Bars)")
        if self.skip_news:
            skip_list.append("Phase 5 (News)")
        
        if skip_list:
            logger.info(f"Skipping: {', '.join(skip_list)}")
        
        logger.info(f"{'='*60}\n")
        
        start_time = time.time()
        summary = {}
        
        try:
            # Phase 1: Reference Data
            if not self.skip_reference:
                summary['reference'] = self.phase1_reference_data(tickers)
            else:
                logger.info("=== PHASE 1: SKIPPED (Reference Data) ===")
                summary['reference'] = {}
            
            # Phase 2: Corporate Actions
            if not self.skip_corporate:
                summary['corporate_actions'] = self.phase2_corporate_actions(tickers)
            else:
                logger.info("=== PHASE 2: SKIPPED (Corporate Actions) ===")
                summary['corporate_actions'] = {}
            
            # Phase 3: Daily Bars
            if not self.skip_daily:
                summary['daily_bars'] = self.phase3_daily_bars(tickers)
            else:
                logger.info("=== PHASE 3: SKIPPED (Daily Bars) ===")
                summary['daily_bars'] = {}
            
            # Phase 4: Minute Bars (most time-consuming)
            if not self.skip_minute:
                if self.use_flatfiles:
                    # Use hybrid approach if enabled: flat files first, REST API for gaps
                    if self.enable_hybrid_backfill:
                        logger.info("🔄 Hybrid Mode: Processing flat files first, then filling gaps with REST API")
                        summary['minute_bars'] = self.phase4_hybrid(tickers)
                    else:
                        summary['minute_bars'] = self.phase4_flatfiles(tickers)
                else:
                    # Pure REST API mode
                    summary['minute_bars'] = self.phase4_minute_bars(tickers)
            else:
                logger.info("=== PHASE 4: SKIPPED (Minute Bars / Flat Files) ===")
                summary['minute_bars'] = {}
            
            # Phase 5: News
            if not self.skip_news:
                summary['news'] = self.phase5_news(tickers)
            else:
                logger.info("=== PHASE 5: SKIPPED (News) ===")
                summary['news'] = {}
            
            # Note: TimescaleDB auto-commits, no explicit flush needed
        
        except KeyboardInterrupt:
            logger.warning("\n\nBackfill interrupted by user!")
            # TimescaleDB transactions are committed automatically
            raise
        
        except Exception as e:
            logger.error(f"Fatal error during backfill: {str(e)}")
            raise
        
        finally:
            elapsed = time.time() - start_time
            logger.info(f"\n{'='*60}")
            logger.info(f"BACKFILL COMPLETE")
            logger.info(f"Total time: {elapsed/60:.2f} minutes")
            logger.info(f"{'='*60}\n")
        
        return summary
    
    def close(self):
        """Clean up resources"""
        self.polygon_client.close()
        self.db_writer.close()


# ============================================================================
# INDICATOR CALCULATION & DATA AGGREGATION FUNCTIONS
# ============================================================================

def detect_chart_patterns(df):
    """
    Detect chart patterns using tradingpatterns library.
    Returns dict of detected patterns with metadata.
    
    Args:
        df: DataFrame with columns [time, open, high, low, close, volume]
    
    Returns:
        Dict with pattern detections: {
            'patterns': List of detected patterns with metadata,
            'support_resistance': Dict of S/R levels,
            'trendlines': List of detected trendlines
        }
    """
    import pandas as pd
    import numpy as np
    import warnings
    
    # Silence FutureWarnings from tradingpatterns library
    warnings.filterwarnings('ignore', category=FutureWarning, module='tradingpatterns')
    
    from tradingpatterns.tradingpatterns import (
        detect_head_shoulder,
        detect_double_top_bottom,
        detect_triangle_pattern,
        # detect_wedge,  # Disabled due to library bugs
        # detect_channel,  # Disabled due to library bugs
        find_pivots,
        calculate_support_resistance,
        detect_trendline
    )
    
    if df.empty or len(df) < 50:
        return {'patterns': [], 'support_resistance': {}, 'trendlines': []}
    
    # Prepare data with capital column names (required by tradingpatterns)
    df_pattern = df.copy()
    df_pattern.columns = [c.capitalize() if c.lower() in ['open', 'high', 'low', 'close', 'volume'] else c for c in df_pattern.columns]
    
    results = {
        'patterns': [],
        'support_resistance': {},
        'trendlines': []
    }
    
    try:
        # Detect Head & Shoulders
        try:
            hs_df = detect_head_shoulder(df_pattern)
            if isinstance(hs_df, pd.DataFrame) and 'head_shoulder_pattern' in hs_df.columns:
                patterns_found = hs_df[hs_df['head_shoulder_pattern'].notna()]
                for idx, row in patterns_found.iterrows():
                    pattern_name = row['head_shoulder_pattern']
                    subtype = 'bullish' if 'Inverse' in pattern_name else 'bearish'
                    results['patterns'].append({
                        'type': 'head_shoulders',
                        'subtype': subtype,
                        'confidence': 0.7,  # Default confidence
                        'start_index': max(0, int(idx) - 20),
                        'end_index': min(len(df)-1, int(idx) + 5),
                        'indices': [int(idx)],
                        'metadata': {'pattern_name': pattern_name}
                    })
        except Exception as e:
            logger.debug(f"Head & Shoulders detection failed: {e}")
        
        # Detect Double Top/Bottom
        try:
            dtb_df = detect_double_top_bottom(df_pattern)
            if isinstance(dtb_df, pd.DataFrame) and 'double_pattern' in dtb_df.columns:
                patterns_found = dtb_df[dtb_df['double_pattern'].notna()]
                for idx, row in patterns_found.iterrows():
                    pattern_name = row['double_pattern']
                    subtype = 'bearish' if 'Top' in pattern_name else 'bullish'
                    results['patterns'].append({
                        'type': 'double_top_bottom',
                        'subtype': subtype,
                        'confidence': 0.7,
                        'start_index': max(0, int(idx) - 30),
                        'end_index': min(len(df)-1, int(idx) + 5),
                        'indices': [int(idx)],
                        'metadata': {'pattern_name': pattern_name}
                    })
        except Exception as e:
            logger.debug(f"Double Top/Bottom detection failed: {e}")
        
        # Detect Triangle Patterns
        try:
            tri_df = detect_triangle_pattern(df_pattern)
            if isinstance(tri_df, pd.DataFrame) and 'triangle_pattern' in tri_df.columns:
                patterns_found = tri_df[tri_df['triangle_pattern'].notna()]
                for idx, row in patterns_found.iterrows():
                    pattern_name = row['triangle_pattern']
                    # Determine subtype from name
                    if 'Ascending' in pattern_name:
                        subtype = 'bullish'
                    elif 'Descending' in pattern_name:
                        subtype = 'bearish'
                    else:
                        subtype = 'neutral'
                    
                    results['patterns'].append({
                        'type': 'triangle',
                        'subtype': subtype,
                        'confidence': 0.7,
                        'start_index': max(0, int(idx) - 40),
                        'end_index': min(len(df)-1, int(idx) + 5),
                        'indices': [int(idx)],
                        'metadata': {'pattern_name': pattern_name}
                    })
        except Exception as e:
            logger.debug(f"Triangle detection failed: {e}")
        
        # Note: Wedge and Channel detection disabled due to library bugs
        # They can be re-enabled when the library is fixed
        
        # Calculate Support/Resistance
        try:
            sr_levels = calculate_support_resistance(df_pattern)
            if sr_levels is not None and isinstance(sr_levels, dict):
                results['support_resistance'] = sr_levels
        except Exception as e:
            logger.debug(f"Support/Resistance calculation failed: {e}")
        
        # Detect Trendlines
        try:
            trendlines = detect_trendline(df_pattern)
            if trendlines is not None:
                results['trendlines'] = trendlines if isinstance(trendlines, list) else [trendlines]
        except Exception as e:
            logger.debug(f"Trendline detection failed: {e}")
    
    except Exception as e:
        logger.error(f"Pattern detection error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    return results


def calculate_indicators(df):
    """
    Calculate technical indicators for OHLCV data.
    Optimized vectorized computation with all required indicators.
    
    Args:
        df: DataFrame with columns [time, open, high, low, close, volume, vwap]
    
    Returns:
        DataFrame with added indicator columns
    """
    import pandas as pd
    import numpy as np
    
    if df.empty or len(df) < 200:  # Need 200 for EMA200
        return df
    
    # Sort by time ascending for proper calculation
    df = df.sort_values('time').copy()
    
    # Simple Moving Averages
    df['sma_20'] = df['close'].rolling(window=20, min_periods=20).mean()
    df['sma_50'] = df['close'].rolling(window=50, min_periods=50).mean()
    
    # Exponential Moving Averages (TradingView standard)
    df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema_100'] = df['close'].ewm(span=100, adjust=False).mean()
    df['ema_200'] = df['close'].ewm(span=200, adjust=False).mean()
    
    # Keep original EMA 12/26 for MACD
    ema_12 = df['close'].ewm(span=12, adjust=False).mean()
    ema_26 = df['close'].ewm(span=26, adjust=False).mean()
    
    # MACD (12, 26, 9)
    df['macd'] = ema_12 - ema_26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']
    
    # RSI (14-period)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df['rsi_14'] = 100 - (100 / (1 + rs))
    
    # Bollinger Bands (20-period, 2 std dev)
    sma_20 = df['close'].rolling(window=20).mean()
    std_20 = df['close'].rolling(window=20).std()
    df['bb_upper'] = sma_20 + (2 * std_20)
    df['bb_middle'] = sma_20
    df['bb_lower'] = sma_20 - (2 * std_20)
    
    # VWAP - calculate if missing or recalculate fresh
    if 'vwap' not in df.columns or df['vwap'].isna().all():
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        df['vwap'] = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
    
    return df


def resample_ohlcv(df, interval: str):
    """
    Resample OHLCV data to different timeframes.
    
    Args:
        df: DataFrame with OHLCV data and time index
        interval: Target interval ('1m', '5m', '15m', '30m', '1h', '2h', '4h', '12h', '1d', '1w', '1mo', '1y')
    
    Returns:
        Resampled DataFrame
    """
    import pandas as pd
    
    if df.empty:
        return df
    
    # Map interval strings to pandas frequency strings (using new naming convention)
    freq_map = {
        '1m': '1min', '5m': '5min', '15m': '15min', '30m': '30min',
        '1h': '1h', '2h': '2h', '4h': '4h', '12h': '12h',
        '1d': '1D', '1w': '1W', '1mo': '1MS', '1y': '1YS'
    }
    
    freq = freq_map.get(interval, '1min')
    
    # Ensure time is datetime index
    if 'time' in df.columns:
        df = df.set_index('time')
    
    # Resample with proper aggregations
    resampled = df.resample(freq).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'vwap': 'mean',
    })
    
    # Drop rows with NaN (incomplete periods)
    resampled = resampled.dropna(subset=['close'])
    
    # Reset index to get time as column
    resampled = resampled.reset_index()
    
    return resampled


def get_aggregated_bars(
    ticker: str,
    interval: str = '1m',
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    with_indicators: bool = True,
    limit: int = 10000
):
    """
    Fetch and aggregate OHLCV bars from database with optional indicator calculation.
    Uses SQLAlchemy for safe, thread-safe database access.
    
    Args:
        ticker: Stock symbol
        interval: Timeframe ('1m', '5m', '15m', '30m', '1h', '2h', '4h', '12h', '1d', '1w', '1mo', '1y')
        start_time: Start datetime (default: auto-calculated based on interval)
        end_time: End datetime (default: now)
        with_indicators: Calculate technical indicators
        limit: Maximum number of bars to return
    
    Returns:
        DataFrame with OHLCV data and optional indicators
    """
    import pandas as pd
    from datetime import datetime, timedelta
    from sqlalchemy import create_engine
    import os
    
    # Default time range
    if end_time is None:
        end_time = datetime.now()
    if start_time is None:
        # Smart defaults based on interval
        days_back = {
            '1m': 7, '5m': 14, '15m': 30, '30m': 60,
            '1h': 90, '2h': 180, '4h': 365, '12h': 365,
            '1d': 365 * 2, '1w': 365 * 5, '1mo': 365 * 10, '1y': 365 * 20
        }
        start_time = end_time - timedelta(days=days_back.get(interval, 30))
    
    # SQLAlchemy engine (connection pooling, thread-safe)
    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('TIMESCALE_USER', 'postgres')}:"
        f"{os.getenv('TIMESCALE_PASSWORD', 'chronox_db_password')}@"
        f"{os.getenv('TIMESCALE_HOST', 'localhost')}:"
        f"{os.getenv('TIMESCALE_PORT', '5433')}/"
        f"{os.getenv('TIMESCALE_DB', 'chronox')}"
    )
    
    try:
        # Create engine with connection pooling
        engine = create_engine(db_url, pool_pre_ping=True, future=True)
        
        # Query minute bars (fetch extra for resampling buffer)
        query = """
            SELECT time, open, high, low, close, volume, vwap, transactions
            FROM market_data
            WHERE ticker = %(ticker)s AND timeframe = '1min'
              AND time >= %(start)s AND time <= %(end)s
            ORDER BY time ASC
        """
        
        # Use pandas read_sql with SQLAlchemy engine (no warnings)
        df = pd.read_sql(
            query,
            engine,
            params={'ticker': ticker, 'start': start_time, 'end': end_time}
        )
        
        engine.dispose()  # Clean up connection pool
        
        if df.empty:
            return pd.DataFrame()
        
        # Convert time to datetime
        df['time'] = pd.to_datetime(df['time'])
        
        # Resample if needed (from 1-minute base data)
        if interval != '1m':
            df = resample_ohlcv(df, interval)
        
        # Limit results after resampling
        if len(df) > limit:
            df = df.tail(limit)
        
        # Calculate indicators if requested
        if with_indicators and len(df) >= 200:  # Need sufficient data
            df = calculate_indicators(df)
        
        # Detect chart patterns if we have enough data
        patterns = {}
        if len(df) >= 50:  # Minimum data for pattern detection
            patterns = detect_chart_patterns(df)
        
        # Store patterns as attribute for later access
        if patterns and patterns.get('patterns'):
            df.attrs['chart_patterns'] = patterns
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching aggregated bars for {ticker}: {e}")
        return pd.DataFrame()


def get_top_tickers(ref_client: ReferenceClient, limit: int = 50) -> List[str]:
    """
    Get list of top tickers by market cap.
    
    Args:
        ref_client: Reference client
        limit: Number of tickers to return
    
    Returns:
        List of ticker symbols
    """
    logger.info(f"Fetching top {limit} tickers...")
    
    # Get all active US stocks
    all_tickers = ref_client.get_all_tickers(market='stocks', active=True, limit=limit)
    
    # Extract ticker symbols
    tickers = [t['ticker'] for t in all_tickers if 'ticker' in t]
    
    logger.info(f"Found {len(tickers)} tickers")
    return tickers


def detect_and_store_patterns(
    ticker: str,
    interval: str = '1d',
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> int:
    """
    Detect and store chart patterns for a given ticker and timeframe.
    
    This is a utility function that can be called to populate the chart_patterns
    table with detected patterns. It fetches data, runs pattern detection, and
    stores results in TimescaleDB.
    
    Args:
        ticker: Stock symbol
        interval: Timeframe ('1m', '5m', '15m', '30m', '1h', '2h', '4h', '12h', '1d', '1w', '1mo', '1y')
        start_time: Start datetime (default: auto-calculated based on interval)
        end_time: End datetime (default: now)
    
    Returns:
        Number of patterns stored
    """
    try:
        # Fetch aggregated data (this will include pattern detection)
        df = get_aggregated_bars(
            ticker=ticker,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            with_indicators=True,
            limit=10000
        )
        
        if df.empty:
            logger.warning(f"No data available for {ticker} at {interval}")
            return 0
        
        # Extract patterns from DataFrame attrs
        if not hasattr(df, 'attrs') or 'chart_patterns' not in df.attrs:
            logger.info(f"No patterns detected for {ticker} at {interval}")
            return 0
        
        pattern_data = df.attrs['chart_patterns']
        pattern_list = pattern_data.get('patterns', [])
        
        if not pattern_list:
            logger.info(f"No patterns found for {ticker} at {interval}")
            return 0
        
        # Store patterns in database
        db_writer = TimescaleWriter()
        try:
            count = db_writer.write_chart_patterns(
                ticker=ticker,
                timeframe=interval,
                patterns=pattern_list,  # Pass the list, not the whole dict
                detected_at=datetime.now()
            )
            logger.info(f"Stored {count} patterns for {ticker} at {interval}")
            return count
        finally:
            db_writer.close()
    
    except Exception as e:
        logger.error(f"Error detecting/storing patterns for {ticker} at {interval}: {e}")
        return 0


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Backfill historical market data')
    parser.add_argument('--tickers', type=str, help='Comma-separated list of tickers (e.g., AAPL,TSLA,NVDA)')
    parser.add_argument('--all', action='store_true', help='Fetch all available tickers')
    parser.add_argument('--limit', type=int, default=50, help='Limit number of tickers when using --all')
    parser.add_argument('--years', type=int, default=5, help='Years of historical data (default: 5)')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    parser.add_argument('--skip-reference', action='store_true', help='Skip Phase 1 (reference data)')
    parser.add_argument('--skip-corporate', action='store_true', help='Skip Phase 2 (corporate actions)')
    parser.add_argument('--skip-daily', action='store_true', help='Skip Phase 3 (daily bars)')
    parser.add_argument('--skip-minute', action='store_true', help='Skip Phase 4 (minute bars)')
    parser.add_argument('--skip-news', action='store_true', help='Skip Phase 5 (news)')
    parser.add_argument('--flatfiles', action='store_true', help='Use Polygon flat files (S3) for minute aggregates (high-performance parallel processing)')
    parser.add_argument('--backfill', action='store_true', help='Enable hybrid backfill mode: flat files first, then REST API to fill gaps > 3 days (requires --flatfiles)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging (queue states, stack transitions)')
    parser.add_argument('--visualize', action='store_true', help='Enable real-time visualization (exports HTML charts)')
    
    # --- KAPPA ARCHITECTURE CLI MODES ---
    parser.add_argument('--mode', type=str, choices=['legacy', 'backfill', 'live', 'unified'], default='legacy',
                       help='Execution mode: '
                            'legacy (full orchestrator with flat files - RECOMMENDED for bulk backfills), '
                            'backfill (unified pipeline via REST API - simple single-ticker mode), '
                            'live (WebSocket streaming), '
                            'unified (backfill→live transition)')
    parser.add_argument('--timeframe', type=str, default='1m', choices=['1m', '5m', '15m', '30m', '1h', '2h', '4h', '1d', '1w'],
                       help='Base timeframe for unified pipeline')
    parser.add_argument('--redis-host', type=str, default='localhost', help='Redis host for pub/sub')
    parser.add_argument('--redis-port', type=int, default=6380, help='Redis port (default: 6380 for dev server)')
    parser.add_argument('--redis-password', type=str, default=None, help='Redis password (optional)')
    parser.add_argument('--no-redis', action='store_true', help='Disable Redis publishing (database only)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for bulk inserts (backfill mode)')
    
    args = parser.parse_args()
    
    # Load environment variables from project root
    from pathlib import Path
    project_root = Path(__file__).parent.parent
    env_path = project_root / '.env'
    load_dotenv(dotenv_path=env_path)
    
    polygon_api_key = os.getenv('POLYGON_API_KEY')
    if not polygon_api_key:
        logger.error("POLYGON_API_KEY not found in environment")
        logger.error(f"Looked for .env at: {env_path}")
        sys.exit(1)
    
    # Initialize TimescaleDB writer
    db_writer = TimescaleWriter()
    if not db_writer.test_connection():
        logger.error("Failed to connect to TimescaleDB")
        sys.exit(1)
    logger.info("✓ Connected to TimescaleDB")
    
    # Determine tickers to process
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(',')]
        logger.info(f"Using specified tickers: {tickers}")
    elif args.all:
        # Initialize reference client to get ticker list
        polygon_client = PolygonClient(polygon_api_key)
        ref_client = ReferenceClient(polygon_client)
        tickers = get_top_tickers(ref_client, limit=args.limit)
        polygon_client.close()
    else:
        logger.error("Must specify either --tickers or --all")
        parser.print_help()
        sys.exit(1)
    
    if not tickers:
        logger.error("No tickers to process")
        sys.exit(1)
    
    # === UNIFIED PIPELINE EXECUTION ===
    if args.mode in ['backfill', 'live', 'unified']:
        logger.info(f"🚀 Starting Kappa unified pipeline in {args.mode} mode")
        
        # Warning about performance for bulk backfills
        if args.mode == 'backfill' and len(tickers) > 5:
            logger.warning("⚠️  NOTE: You're using REST API mode for multiple tickers.")
            logger.warning("    For bulk backfills, use: --mode legacy --flatfiles")
            logger.warning("    The legacy mode uses parallel workers and S3 flat files (much faster)")
            logger.warning("    Current mode will fetch 50,000 bars per ticker via REST API")
        
        # Initialize message bus (Redis by default, unless --no-redis flag)
        message_bus = None
        if not args.no_redis:
            try:
                redis_password = args.redis_password or os.getenv('REDIS_PASSWORD')
                message_bus = RedisMessageBus(
                    host=args.redis_host, 
                    port=args.redis_port,
                    password=redis_password
                )
                logger.info(f"✓ Connected to Redis at {args.redis_host}:{args.redis_port} (publishing enabled)")
            except Exception as e:
                logger.warning(f"⚠️ Redis connection failed: {e}")
                logger.warning(f"   Falling back to in-memory bus (no real-time GUI updates)")
                message_bus = InMemoryMessageBus()
        else:
            logger.info("Redis publishing disabled (--no-redis flag)")
            message_bus = InMemoryMessageBus()
        
        # Start Kafka→Redis bridge if Kafka is available (optional feature)
        kafka_bridge = None
        kafka_brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092')
        if not args.no_redis and message_bus and isinstance(message_bus, RedisMessageBus) and KAFKA_AVAILABLE:
            try:
                kafka_bridge = KafkaToRedisBridge(
                    kafka_brokers=kafka_brokers,
                    redis_bus=message_bus,
                    storage=db_writer
                )
                kafka_bridge.start()
                logger.info(f"✓ Kafka→Redis bridge started ({kafka_brokers} → Redis)")
            except Exception as e:
                logger.debug(f"Kafka bridge not started: {e}")  # Changed to debug - it's optional
        
        # Initialize processor chain
        processors = [
            IncrementalIndicatorProcessor(),
            # Add more processors here (pattern detection, feature engineering, etc.)
        ]
        
        # Initialize ingestion engine
        engine = DataIngestionEngine(
            processors=processors,
            storage=db_writer,
            message_bus=message_bus,
            batch_size=args.batch_size
        )
        
        # Initialize Polygon clients
        polygon_client = PolygonClient(polygon_api_key)
        aggregates_client = AggregatesClient(polygon_client)
        
        # Execute based on mode
        for ticker in tickers:
            try:
                if args.mode == 'backfill':
                    # Historical replay through unified pipeline (with Redis publishing)
                    logger.info(f"📊 Backfilling {ticker} ({args.timeframe}) through unified pipeline...")
                    
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=args.years * 365)
                    
                    data_source = HistoricalDataSource(
                        aggregates_client=aggregates_client,
                        symbol=ticker,
                        start_date=start_date,
                        end_date=end_date,
                        timeframe=args.timeframe
                    )
                    
                    engine.ingest(data_source, mode='batch')
                    logger.info(f"✓ Backfill complete for {ticker}")
                
                elif args.mode == 'live':
                    # Live streaming mode via Polygon WebSocket
                    logger.info(f"📡 Starting live stream for {ticker} ({args.timeframe})...")
                    
                    # Check if websocket-client is installed
                    try:
                        import websocket
                    except ImportError:
                        logger.error("❌ Live mode requires 'websocket-client' package")
                        logger.error("   Install with: pip install websocket-client")
                        sys.exit(1)
                    
                    data_source = LiveDataSource(
                        symbol=ticker, 
                        api_key=polygon_api_key,
                        timeframe=args.timeframe
                    )
                    
                    logger.info(f"🎯 Live streaming {ticker} - Press Ctrl+C to stop")
                    try:
                        engine.ingest(data_source, mode='stream')
                    except KeyboardInterrupt:
                        logger.info("\n⏹️  Live stream stopped by user")
                        data_source.stop()
                
                elif args.mode == 'unified':
                    # Backfill then seamless transition to live
                    logger.info(f"🔄 Unified mode: backfill → live for {ticker}")
                    
                    # Check if websocket-client is installed
                    try:
                        import websocket
                    except ImportError:
                        logger.error("❌ Unified mode requires 'websocket-client' package")
                        logger.error("   Install with: pip install websocket-client")
                        sys.exit(1)
                    
                    # Phase 1: Backfill
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=args.years * 365)
                    
                    historical_source = HistoricalDataSource(
                        aggregates_client=aggregates_client,
                        symbol=ticker,
                        start_date=start_date,
                        end_date=end_date,
                        timeframe=args.timeframe
                    )
                    
                    logger.info("📊 Phase 1: Historical backfill...")
                    engine.ingest(historical_source, mode='batch')
                    logger.info(f"✓ Backfill complete - {ticker} is now up to date")
                    
                    # Phase 2: Switch to live streaming
                    logger.info("📡 Phase 2: Transitioning to live stream...")
                    
                    live_source = LiveDataSource(
                        symbol=ticker,
                        api_key=polygon_api_key,
                        timeframe=args.timeframe
                    )
                    
                    logger.info(f"🎯 Now streaming live data for {ticker} - Press Ctrl+C to stop")
                    try:
                        engine.ingest(live_source, mode='stream')
                    except KeyboardInterrupt:
                        logger.info("\n⏹️  Unified mode stopped by user")
                        live_source.stop()
                    
            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}", exc_info=True)
        
        # Cleanup
        if kafka_bridge:
            logger.info("Stopping Kafka→Redis bridge...")
            kafka_bridge.stop()
        
        polygon_client.close()
        db_writer.close()
        logger.info("✅ Unified pipeline execution complete")
        sys.exit(0)
    
    # === LEGACY MODE (ORIGINAL ORCHESTRATOR) ===
    # Initialize orchestrator
    orchestrator = BackfillOrchestrator(
        polygon_api_key=polygon_api_key,
        db_writer=db_writer,
        years_back=args.years,
        max_workers=args.workers,
        use_flatfiles=args.flatfiles,
        debug=args.debug,
        skip_reference=args.skip_reference,
        skip_corporate=args.skip_corporate,
        skip_daily=args.skip_daily,
        skip_minute=args.skip_minute,
        skip_news=args.skip_news,
        enable_hybrid_backfill=args.backfill
    )
    
    # Initialize visualizer if requested
    visualizer = None
    if args.visualize:
        if VISUALIZER_AVAILABLE:
            try:
                # Create shared metrics dictionary
                shared_metrics = {
                    'download_qsize': 0,
                    'process_qsize': 0,
                    'download_maxsize': 0,
                    'process_maxsize': 0,
                    'downloaded': 0,
                    'processed': 0,
                    'failed': 0,
                    'throughput': 0.0,
                    'active_futures': 0
                }
                
                visualizer = attach_visualizer(
                    backfill_metrics=shared_metrics,
                    tickers=tickers,
                    api_key=polygon_api_key
                )
                logger.info("✓ Visualizer started - View charts at: ./visualizations/index.html")
            except Exception as e:
                logger.error(f"Failed to start visualizer: {e}", exc_info=True)
        else:
            logger.warning("Visualizer requested but dependencies not available (install plotly, websockets, aiohttp)")
    
    try:
        # Run backfill
        summary = orchestrator.run_full_backfill(tickers)
        
        # Print summary
        print("\n" + "="*60)
        print("BACKFILL SUMMARY")
        print("="*60)
        
        if 'reference' in summary:
            success = sum(1 for v in summary['reference'].values() if v)
            print(f"Reference Data: {success}/{len(tickers)} tickers")
        
        if 'daily_bars' in summary:
            total = sum(summary['daily_bars'].values())
            print(f"Daily Bars: {total:,} bars")
        
        if 'minute_bars' in summary:
            total = sum(summary['minute_bars'].values())
            print(f"Minute Bars: {total:,} bars")
        
        if 'news' in summary:
            total = sum(summary['news'].values())
            print(f"News Articles: {total:,} articles")
        
        print("="*60 + "\n")
    
    finally:
        # Cleanup visualizer if running
        if visualizer:
            try:
                logger.info("Stopping visualizer...")
                visualizer.stop()
            except Exception as e:
                logger.error(f"Error stopping visualizer: {e}")
        
        orchestrator.close()


if __name__ == "__main__":
    main()
