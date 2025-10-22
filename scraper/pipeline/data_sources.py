"""
Data Sources for Kappa Architecture
Unified interface for historical (REST/CSV) and live (WebSocket) feeds.
"""
import logging
import time
import json
import queue
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger(__name__)


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
    
    def __init__(self, aggregates_client, symbol: str, 
                 start_date: datetime, end_date: datetime, timeframe: str = '1m'):
        """
        Initialize historical data source.
        
        Args:
            aggregates_client: AggregatesClient instance
            symbol: Ticker symbol
            start_date: Start date for historical data
            end_date: End date for historical data
            timeframe: Timeframe (1m, 5m, 15m, 30m, 1h, 2h, 4h, 1d, 1w)
        """
        self.client = aggregates_client
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.timeframe = timeframe
        self.bars = []
        self.index = 0
        
    def __iter__(self):
        """Fetch historical data and prepare for iteration"""
        multiplier, timespan = self._parse_timeframe(self.timeframe)
        
        # Fetch bars based on timespan
        if timespan == 'minute':
            self.bars = self.client.get_minute_bars(
                self.symbol,
                self.start_date.strftime('%Y-%m-%d'),
                self.end_date.strftime('%Y-%m-%d')
            )
        elif timespan == 'hour':
            self.bars = self.client.get_hourly_bars(
                self.symbol,
                self.start_date.strftime('%Y-%m-%d'),
                self.end_date.strftime('%Y-%m-%d')
            )
        elif timespan == 'day':
            self.bars = self.client.get_daily_bars(
                self.symbol,
                self.start_date.strftime('%Y-%m-%d'),
                self.end_date.strftime('%Y-%m-%d')
            )
        else:
            self.bars = self.client.get_minute_bars(
                self.symbol,
                self.start_date.strftime('%Y-%m-%d'),
                self.end_date.strftime('%Y-%m-%d')
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
        """
        Initialize live data source.
        
        Args:
            symbol: Ticker symbol
            api_key: Polygon.io API key
            timeframe: Timeframe (1m, 5m, 15m, 30m)
        """
        self.symbol = symbol.upper()
        self.api_key = api_key
        self.timeframe = timeframe
        self.queue = queue.Queue(maxsize=1000)
        self.ws_thread = None
        self.running = False
        self.ws = None
        
        # Parse timeframe to determine subscription type
        # Polygon supports: A.{ticker} for second bars, AM.{ticker} for minute bars
        if timeframe in ['1m', '5m', '15m', '30m']:
            self.subscription_prefix = 'AM'  # Minute aggregates
        else:
            self.subscription_prefix = 'A'   # Second aggregates
        
    def __iter__(self):
        """Start WebSocket connection in background thread"""
        try:
            import websocket
        except ImportError:
            logger.error("websocket-client not installed. Install with: pip install websocket-client")
            raise
        
        self.running = True
        self.ws_thread = threading.Thread(target=self._ws_loop, daemon=True)
        self.ws_thread.start()
        return self
    
    def __next__(self) -> Dict:
        """Block until next bar arrives from WebSocket"""
        if not self.running:
            raise StopIteration
        
        try:
            bar = self.queue.get(timeout=300)  # 5min timeout
            if bar is None:  # Sentinel for shutdown
                raise StopIteration
            return bar
        except queue.Empty:
            logger.warning(f"No live data received for {self.symbol} in 5 minutes")
            raise StopIteration
    
    def _ws_loop(self):
        """WebSocket event loop with reconnection logic"""
        import websocket
        
        ws_url = "wss://socket.polygon.io/stocks"
        reconnect_delay = 5
        
        while self.running:
            try:
                logger.info(f"🔌 Connecting to Polygon WebSocket for {self.symbol}...")
                
                self.ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                
                self.ws.run_forever()
                
                if self.running:
                    logger.warning(f"WebSocket disconnected, reconnecting in {reconnect_delay}s...")
                    time.sleep(reconnect_delay)
                
            except Exception as e:
                if self.running:
                    logger.error(f"WebSocket error: {e}, reconnecting in {reconnect_delay}s...")
                    time.sleep(reconnect_delay)
                else:
                    break
    
    def _on_open(self, ws):
        """Handle WebSocket connection opened"""
        logger.info(f"✅ WebSocket connected for {self.symbol}")
        
        # Authenticate
        auth_msg = {"action": "auth", "params": self.api_key}
        ws.send(json.dumps(auth_msg))
        
        # Subscribe to aggregates
        subscribe_msg = {
            "action": "subscribe",
            "params": f"{self.subscription_prefix}.{self.symbol}"
        }
        ws.send(json.dumps(subscribe_msg))
        logger.info(f"📡 Subscribed to {self.subscription_prefix}.{self.symbol}")
    
    def _on_message(self, ws, message):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)
            
            for item in data:
                ev_type = item.get('ev')
                
                if ev_type in ['AM', 'A']:  # Minute or second aggregate
                    # Normalize timestamp
                    timestamp_raw = item.get('t') or item.get('s')
                    if timestamp_raw:
                        if timestamp_raw > 1e12:
                            timestamp_ms = int(timestamp_raw / 1e6)  # ns -> ms
                        elif timestamp_raw > 1e10:
                            timestamp_ms = int(timestamp_raw)  # already in ms
                        else:
                            timestamp_ms = int(timestamp_raw * 1000)  # s -> ms
                    else:
                        continue
                    
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
                    
                    if bar['symbol'] == self.symbol:
                        try:
                            self.queue.put_nowait(bar)
                            logger.debug(f"📊 Received live bar: {bar['symbol']} @ {bar['timestamp']}")
                        except queue.Full:
                            logger.warning(f"Queue full, dropping bar for {self.symbol}")
                
                elif ev_type == 'status':
                    status = item.get('status')
                    msg = item.get('message', '')
                    if status == 'auth_success':
                        logger.info(f"✅ Polygon authentication successful")
                    elif status == 'success':
                        logger.info(f"✅ Subscription confirmed: {msg}")
                    else:
                        logger.warning(f"Status: {status} - {msg}")
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse WebSocket message: {e}")
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}")
    
    def _on_error(self, ws, error):
        """Handle WebSocket error"""
        logger.error(f"WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection closed"""
        logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
    
    def stop(self):
        """Gracefully stop WebSocket connection"""
        self.running = False
        if self.ws:
            self.ws.close()
        self.queue.put(None)  # Sentinel
