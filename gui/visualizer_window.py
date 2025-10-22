#!/usr/bin/env python3
"""
Backfill Visualizer & Debugger - Real-time GUI for monitoring producer/consumer pipeline

Features:
- Stock ticker selection and date range input
- Real-time monitoring of download/processing queues
- File task state tracking with detailed table view
- Pipeline statistics and progress bars
- Live log output with filtering
- Stop/Start controls

Usage:
    python scripts/backfill_visualizer.py
"""

import sys
import os
import threading
import queue
import logging
import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any
import subprocess
import signal

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QLineEdit, QTextEdit, QTableWidget, QTableWidgetItem,
    QTabWidget, QGroupBox, QProgressBar, QComboBox, QDateEdit, QSplitter,
    QHeaderView, QStyle, QCheckBox
)
from PyQt6.QtCore import Qt, QTimer, QThread, pyqtSignal, QDate
from PyQt6.QtGui import QColor, QFont, QPalette

# Matplotlib for live charts
import matplotlib
matplotlib.use('QtAgg')
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from collections import deque

# Polygon.io market data dependencies (optional)
try:
    import asyncio
    import websockets
    import aiohttp
    import json
    import pandas as pd
    import numpy as np
    from dotenv import load_dotenv
    try:
        import mplfinance as mpf
        MPLFINANCE_AVAILABLE = True
    except ImportError:
        MPLFINANCE_AVAILABLE = False
    POLYGON_DEPS_AVAILABLE = True
except ImportError as e:
    POLYGON_DEPS_AVAILABLE = False
    logging.warning(f"Market Data dependencies not available: {e}")
    logging.warning("Run: python scripts/install_market_data_deps.py")

# Configure logging to capture output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)-15s] %(name)s - %(levelname)s - %(message)s'
)


# ============================================================================
# === EVENT-DRIVEN ARCHITECTURE: REDIS SUBSCRIBER ===
# ============================================================================
# Real-time market data updates via Redis Pub/Sub.
# GUI subscribes to Redis channels and receives precomputed data.
# No polling, no calculations - pure event-driven rendering.
# ============================================================================

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("Redis not available - install with: pip install redis")
    redis = None  # type: ignore[assignment]

try:
    from lightweight_charts import Chart
    LIGHTWEIGHT_CHARTS_AVAILABLE = True
except ImportError:
    LIGHTWEIGHT_CHARTS_AVAILABLE = False
    logging.warning(
        "lightweight-charts not available - install with: pip install lightweight-charts"
    )
    Chart = Any  # type: ignore[misc, assignment]





class RedisSubscriberThread(QThread):
    """
    Background thread that subscribes to Redis Pub/Sub channels.
    Emits Qt signals when new bar data arrives from the ingestion pipeline.
    
    Pattern: bars:{symbol}:{timeframe}
    Example: bars:AMD:1m, bars:TSLA:5m
    
    This eliminates polling and makes the GUI purely event-driven.
    """
    
    # Signals emitted to main thread
    bar_received = pyqtSignal(dict)  # Single bar update
    error_occurred = pyqtSignal(str)  # Connection/parsing errors
    connected = pyqtSignal()  # Successfully connected
    disconnected = pyqtSignal()  # Connection lost
    
    def __init__(self, symbols: List[str], timeframes: List[str], 
                 redis_host: str = 'localhost', redis_port: int = 6379, redis_password: Optional[str] = None):
        super().__init__()
        self.symbols = symbols
        self.timeframes = timeframes
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.running = False
        self.redis_client = None
        self.pubsub = None
        
    def run(self):
        """Subscribe to Redis channels in background thread with auto-reconnect"""
        if not REDIS_AVAILABLE:
            self.error_occurred.emit("Redis library not available")
            return
        
        self.running = True
        
        while self.running:
            try:
                # Connect to Redis (with optional password)
                self.redis_client = redis.Redis(
                    host=self.redis_host,
                    port=self.redis_port,
                    db=0,
                    password=self.redis_password,
                    decode_responses=True,
                    socket_keepalive=True,
                    socket_connect_timeout=5,
                    health_check_interval=30
                )
                
                # Test connection
                self.redis_client.ping()
                self.connected.emit()
                logging.info(f"✓ Redis subscriber connected to {self.redis_host}:{self.redis_port}")
                
                # Subscribe to GUI update channels
                self.pubsub = self.redis_client.pubsub()
                channels = []
                for symbol in self.symbols:
                    for tf in self.timeframes:
                        channel = f"chronox:gui:updates:{symbol}:{tf}"
                        channels.append(channel)
                
                self.pubsub.subscribe(*channels)
                logging.info(f"✓ Subscribed to {len(channels)} channels: {channels[:5]}...")
                
                # Listen for messages (blocking loop)
                for message in self.pubsub.listen():
                    if not self.running:
                        break
                    
                    if message['type'] == 'message':
                        try:
                            # Parse JSON bar data
                            bar_data = json.loads(message['data'])
                            
                            # Emit signal to main thread (throttled by GUI)
                            self.bar_received.emit(bar_data)
                            
                        except json.JSONDecodeError as e:
                            self.error_occurred.emit(f"JSON parse error: {e}")
                        except Exception as e:
                            self.error_occurred.emit(f"Message processing error: {e}")
            
            except redis.ConnectionError as e:
                self.error_occurred.emit(f"Redis connection lost: {e}")
                logging.error(f"Redis connection failed: {e}")
                
                # Auto-reconnect after 2 seconds
                if self.running:
                    logging.info("Reconnecting in 2 seconds...")
                    import time
                    time.sleep(2)
            
            except redis.AuthenticationError as e:
                self.error_occurred.emit(f"Redis authentication failed - check password")
                logging.error(f"Redis auth failed: {e}")
                break  # Don't retry auth errors
            
            except Exception as e:
                self.error_occurred.emit(f"Redis subscriber error: {e}")
                logging.error(f"Redis subscriber error: {e}", exc_info=True)
                
                if self.running:
                    import time
                    time.sleep(2)
        
        self.cleanup()
    
    def cleanup(self):
        """Clean up Redis connections"""
        try:
            if self.pubsub:
                self.pubsub.unsubscribe()
                self.pubsub.close()
            if self.redis_client:
                self.redis_client.close()
            self.disconnected.emit()
            logging.info("✓ Redis subscriber disconnected")
        except Exception as e:
            logging.error(f"Cleanup error: {e}")
    
    def stop(self):
        """Stop subscriber thread"""
        self.running = False


# ============================================================================
# === END EVENT-DRIVEN SUBSCRIBER ===
# ============================================================================


# ============================================================================
# === CHART DATA INITIAL LOADER (DATABASE QUERY) ===
# ============================================================================
# Loads initial chart data from TimescaleDB on ticker/timeframe change.
# After initial load, all updates come via Redis Pub/Sub (event-driven).
# No polling - this only runs once when user changes symbol or timeframe.
# ============================================================================

class ChartDataInitialLoader(QThread):
    """
    Background thread for initial chart data loading from TimescaleDB.
    After initial load, all updates come via Redis Pub/Sub (event-driven).
    
    Fixes:
    - January-only bug: Use ORDER BY time ASC and proper date range
    - Full-year loads: Accept start_date/end_date parameters
    """
    
    data_loaded = pyqtSignal(object)  # pandas DataFrame
    error_occurred = pyqtSignal(str)  # Error message
    
    def __init__(self, ticker: str, timeframe: str, limit: int = 10000, 
                 start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
        super().__init__()
        self.ticker = ticker
        self.timeframe = timeframe
        self.limit = limit
        self.start_date = start_date
        self.end_date = end_date
    
    def run(self):
        """Load data from TimescaleDB"""
        try:
            # Lazy import to avoid circular dependencies
            sys.path.insert(0, str(Path(__file__).parent))
            from common.storage import TimescaleWriter
            import pandas as pd
            
            db = TimescaleWriter()
            
            try:
                # Determine table name from timeframe
                table_map = {
                    '1m': 'market_data_1m', '5m': 'market_data_5m',
                    '15m': 'market_data_15m', '30m': 'market_data_30m',
                    '1h': 'market_data_1h', '2h': 'market_data_2h',
                    '4h': 'market_data_4h', '12h': 'market_data_12h',
                    '1d': 'market_data_1d', '1w': 'market_data_1w',
                    '1mo': 'market_data_1mo', '1y': 'market_data_1y'
                }
                
                table = table_map.get(self.timeframe, 'market_data_1d')
                
                # Build query with date range (fixes January-only bug)
                if self.start_date and self.end_date:
                    query = f"""
                        SELECT * FROM {table}
                        WHERE symbol = %s 
                        AND time >= %s 
                        AND time <= %s
                        ORDER BY time ASC
                        LIMIT %s
                    """
                    params = (self.ticker, self.start_date, self.end_date, self.limit)
                else:
                    query = f"""
                        SELECT * FROM {table}
                        WHERE symbol = %s
                        ORDER BY time ASC
                        LIMIT %s
                    """
                    params = (self.ticker, self.limit)
                
                # Execute query using TimescaleWriter's connection pool
                with db.get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute(query, params)
                    
                    # Fetch column names
                    columns = [desc[0] for desc in cur.description]
                    
                    # Fetch all rows
                    rows = cur.fetchall()
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(rows, columns=columns)
                
                if df.empty:
                    self.error_occurred.emit(
                        f"📊 No data available yet for {self.ticker} at {self.timeframe}\n\n"
                        f"To populate data, run:\n"
                        f"python scripts/backfill_historical_data.py --mode backfill "
                        f"--tickers {self.ticker} --timeframe {self.timeframe}"
                    )
                    return
                
                # Ensure time column is datetime with timezone awareness
                if 'time' in df.columns:
                    df['time'] = pd.to_datetime(df['time'], utc=True)
                
                logging.info(f"✓ Loaded {len(df)} bars for {self.ticker} ({self.timeframe})")
                self.data_loaded.emit(df)
                
            finally:
                db.close()
                
        except Exception as e:
            logging.error(f"Data load error: {e}", exc_info=True)
            self.error_occurred.emit(f"Failed to load data: {str(e)}")
    
    def run(self):
        """Load initial data in background thread"""
        try:
            import sys
            sys.path.insert(0, str(Path(__file__).parent))
            
            # Query TimescaleDB for historical bars with indicators
            from common.storage import TimescaleWriter
            import pandas as pd
            
            db = TimescaleWriter()
            
            # Check if table exists first
            table_name = f"market_data_{self.timeframe}"
            
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        AND table_name = %s
                    )
                """, (table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    self.error_occurred.emit(
                        f"📊 No data available yet for {self.ticker} at {self.timeframe}\n\n"
                        f"To populate data, run:\n"
                        f"python scripts/backfill_historical_data.py --mode backfill --tickers {self.ticker} --timeframe {self.timeframe}"
                    )
                    db.close()
                    return
                
                # Build query for enriched data
                query = f"""
                    SELECT 
                        time, ticker, open, high, low, close, volume,
                        sma_20, sma_50, ema_20, ema_50, ema_100, ema_200,
                        rsi_14, macd, macd_signal, macd_hist,
                        bb_upper, bb_middle, bb_lower, vwap
                    FROM {table_name}
                    WHERE ticker = %s
                    ORDER BY time DESC
                    LIMIT %s
                """
                
                df = pd.read_sql_query(
                    query,
                    conn,
                    params=(self.ticker, self.limit)
                )
                
                if df.empty:
                    self.error_occurred.emit(
                        f"📊 Table exists but no data for {self.ticker} at {self.timeframe}\n\n"
                        f"To populate data, run:\n"
                        f"python scripts/backfill_historical_data.py --mode backfill --tickers {self.ticker} --timeframe {self.timeframe}"
                    )
                else:
                    # Reverse to chronological order
                    df = df.iloc[::-1].reset_index(drop=True)
                    self.data_loaded.emit(df)
                    
            # Don't close the connection pool - just return the connection to the pool
            # db.close() would close ALL connections, breaking subsequent queries
                    
        except Exception as e:
            error_msg = str(e)
            if "does not exist" in error_msg or "relation" in error_msg:
                self.error_occurred.emit(
                    f"📊 No data table found for {self.timeframe} timeframe\n\n"
                    f"To populate data, run:\n"
                    f"python scripts/backfill_historical_data.py --mode backfill --tickers {self.ticker} --timeframe {self.timeframe}"
                )
            else:
                self.error_occurred.emit(f"Data load failed: {e}")


# ============================================================================
# === END INITIAL LOADER ===
# ============================================================================


class QueueChartWidget(QWidget):
    """Live multi-line charts: Pipeline Progress + Performance Metrics"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Data storage (last 100 samples)
        self.max_samples = 100
        self.time_history = deque(maxlen=self.max_samples)
        
        # Pipeline Progress metrics
        self.downloaded_history = deque(maxlen=self.max_samples)
        self.processed_history = deque(maxlen=self.max_samples)
        self.skipped_history = deque(maxlen=self.max_samples)
        self.failed_history = deque(maxlen=self.max_samples)
        self.retrying_history = deque(maxlen=self.max_samples)
        
        # Performance metrics - latencies and queue
        self.download_latency_history = deque(maxlen=self.max_samples)
        self.process_latency_history = deque(maxlen=self.max_samples)
        self.process_qsize_history = deque(maxlen=self.max_samples)
        
        # State metrics (active tasks by state)
        self.downloading_history = deque(maxlen=self.max_samples)
        self.processing_history = deque(maxlen=self.max_samples)
        self.waiting_history = deque(maxlen=self.max_samples)
        
        # Setup matplotlib figure with 2 vertically stacked charts
        self.figure = Figure(figsize=(14, 8), facecolor='#2b2b2b')
        self.canvas = FigureCanvas(self.figure)
        
        # Create 2 subplots (vertical stack)
        self.ax_progress = self.figure.add_subplot(2, 1, 1)
        self.ax_performance = self.figure.add_subplot(2, 1, 2)
        
        # Style both axes
        for ax in [self.ax_progress, self.ax_performance]:
            ax.set_facecolor('#1e1e1e')
            ax.tick_params(colors='white', which='both')
            ax.spines['bottom'].set_color('white')
            ax.spines['top'].set_color('white')
            ax.spines['left'].set_color('white')
            ax.spines['right'].set_color('white')
            ax.grid(True, alpha=0.3, color='gray')
        
        # ============================================================
        # TOP CHART: Pipeline Progress (5 lines)
        # ============================================================
        self.ax_progress.set_xlabel('Time (samples)', color='white', fontsize=9)
        self.ax_progress.set_ylabel('File Count', color='white', fontsize=9)
        self.ax_progress.set_title('Pipeline Progress (Downloaded/Processed/Skipped/Failed/Retrying)', 
                                   color='white', fontweight='bold', fontsize=12)
        
        self.line_downloaded, = self.ax_progress.plot([], [], '#2196F3', linewidth=2.5, 
                                                     label='Downloaded', marker='o', markersize=3)
        self.line_processed, = self.ax_progress.plot([], [], '#4CAF50', linewidth=2.5, 
                                                    label='Processed', marker='s', markersize=3)
        self.line_skipped, = self.ax_progress.plot([], [], '#FF9800', linewidth=2.5, 
                                                   label='Skipped', marker='^', markersize=3)
        self.line_failed, = self.ax_progress.plot([], [], '#f44336', linewidth=2.5, 
                                                  label='Failed', marker='x', markersize=4)
        self.line_retrying, = self.ax_progress.plot([], [], '#9C27B0', linewidth=2.5, 
                                                    label='Retrying', marker='d', markersize=3)
        
        self.ax_progress.legend(loc='upper left', facecolor='#2b2b2b', edgecolor='white',
                               labelcolor='white', framealpha=0.9, fontsize=9, ncol=5)
        
        # ============================================================
        # BOTTOM CHART: Performance Metrics (6 lines, dual y-axis)
        # ============================================================
        self.ax_performance.set_xlabel('Time (samples)', color='white', fontsize=9)
        self.ax_performance.set_ylabel('Active Tasks / Queue Size', color='white', fontsize=9)
        self.ax_performance.set_title('Performance Metrics (State/Queue/Latency)', 
                                     color='white', fontweight='bold', fontsize=12)
        
        # Left y-axis: State metrics + Queue size
        self.line_downloading, = self.ax_performance.plot([], [], '#2196F3', linewidth=2.5, 
                                                          label='Downloading', marker='o', markersize=3)
        self.line_processing, = self.ax_performance.plot([], [], '#4CAF50', linewidth=2.5, 
                                                         label='Processing', marker='s', markersize=3)
        self.line_waiting, = self.ax_performance.plot([], [], '#FF9800', linewidth=2.5, 
                                                      label='Waiting', marker='^', markersize=3)
        self.line_pr_qsize, = self.ax_performance.plot([], [], '#E91E63', linewidth=2, 
                                                       label='PR Queue', alpha=0.7)
        
        # Right y-axis: Latencies (ms)
        self.ax_performance_latency = self.ax_performance.twinx()
        self.ax_performance_latency.set_facecolor('#1e1e1e')
        self.ax_performance_latency.tick_params(colors='white', which='both')
        self.ax_performance_latency.spines['right'].set_color('white')
        self.ax_performance_latency.set_ylabel('Latency (ms)', color='white', fontsize=9)
        
        self.line_dl_latency, = self.ax_performance_latency.plot([], [], '#00BCD4', linewidth=2, 
                                                                 label='DL Latency (ms)', linestyle='--')
        self.line_pr_latency, = self.ax_performance_latency.plot([], [], '#FF5722', linewidth=2, 
                                                                 label='PR Latency (ms)', linestyle='--')
        
        # Combined legend for both y-axes
        lines1 = [self.line_downloading, self.line_processing, self.line_waiting, self.line_pr_qsize]
        lines2 = [self.line_dl_latency, self.line_pr_latency]
        labels1 = [l.get_label() for l in lines1]
        labels2 = [l.get_label() for l in lines2]
        self.ax_performance.legend(lines1 + lines2, labels1 + labels2, loc='upper left',
                                  facecolor='#2b2b2b', edgecolor='white', labelcolor='white',
                                  framealpha=0.9, fontsize=8, ncol=3)
        
        # Tight layout
        self.figure.tight_layout(pad=2.0)
        
        # Layout
        layout = QVBoxLayout()
        layout.addWidget(self.canvas)
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)
        
        # Initialize sample counter
        self.sample_count = 0
    
    def update_data(self, downloaded: int = 0, processed: int = 0, skipped: int = 0, 
                   failed: int = 0, retrying: int = 0, downloading: int = 0, processing: int = 0,
                   waiting: int = 0, download_latency_ms: float = 0,
                   process_latency_ms: float = 0, process_qsize: int = 0):
        """Add new data point and refresh all charts with multiple metrics"""
        self.sample_count += 1
        self.time_history.append(self.sample_count)
        
        # Update pipeline progress data
        self.downloaded_history.append(downloaded)
        self.processed_history.append(processed)
        self.skipped_history.append(skipped)
        self.failed_history.append(failed)
        self.retrying_history.append(retrying)
        
        # Update performance data - state metrics
        self.downloading_history.append(downloading)
        self.processing_history.append(processing)
        self.waiting_history.append(waiting)
        self.process_qsize_history.append(process_qsize)
        
        # Update latency data
        self.download_latency_history.append(download_latency_ms)
        self.process_latency_history.append(process_latency_ms)
        
        # Update all line charts
        if len(self.time_history) > 0:
            time_list = list(self.time_history)
            
            # Pipeline Progress chart (5 lines)
            self.line_downloaded.set_data(time_list, list(self.downloaded_history))
            self.line_processed.set_data(time_list, list(self.processed_history))
            self.line_skipped.set_data(time_list, list(self.skipped_history))
            self.line_failed.set_data(time_list, list(self.failed_history))
            self.line_retrying.set_data(time_list, list(self.retrying_history))
            
            # Performance chart - Left y-axis (state counts, queue size)
            self.line_downloading.set_data(time_list, list(self.downloading_history))
            self.line_processing.set_data(time_list, list(self.processing_history))
            self.line_waiting.set_data(time_list, list(self.waiting_history))
            self.line_pr_qsize.set_data(time_list, list(self.process_qsize_history))
            
            # Performance chart - Right y-axis (latencies in ms)
            self.line_dl_latency.set_data(time_list, list(self.download_latency_history))
            self.line_pr_latency.set_data(time_list, list(self.process_latency_history))
            
            # Auto-scale axes
            if len(self.time_history) > 10:
                x_min, x_max = self.time_history[0], self.time_history[-1]
                self.ax_progress.set_xlim(x_min, x_max)
                self.ax_performance.set_xlim(x_min, x_max)
            
            # Rescale y-axes
            self.ax_progress.relim()
            self.ax_progress.autoscale_view(True, True, True)
            self.ax_performance.relim()
            self.ax_performance.autoscale_view(True, True, True)
            self.ax_performance_latency.relim()
            self.ax_performance_latency.autoscale_view(True, True, True)
        
        # Redraw canvas
        self.canvas.draw()


class BackfillWorker(QThread):
    """Background thread that runs the backfill process"""
    log_signal = pyqtSignal(str)
    stats_signal = pyqtSignal(dict)
    task_update_signal = pyqtSignal(str, str, dict)  # task_id, state, info
    finished_signal = pyqtSignal(bool, str)  # success, message
    phase_signal = pyqtSignal(str, dict)  # phase_name, data (for Phase 1/2/3)
    queue_signal = pyqtSignal(int, int, int, int)  # dl_qsize, pr_qsize, dl_maxsize, pr_maxsize
    
    def __init__(self, ticker: str, start_date: str, end_date: str, years: int = 5, debug: bool = False, backfill: bool = True):
        super().__init__()
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date
        self.years = years
        self.debug = debug
        self.backfill = backfill
        self.process: Optional[subprocess.Popen] = None
        self.is_running = False
        
    def run(self):
        """Run the backfill process"""
        try:
            self.is_running = True
            
            # Get the Python executable from the virtual environment
            venv_python = sys.executable
            
            # Build command
            cmd = [
                venv_python,
                'scripts/backfill_historical_data.py',
                '--tickers', self.ticker,
                '--flatfiles',
                '--years', str(self.years)
            ]
            
            # Add debug flag if enabled
            if self.debug:
                cmd.append('--debug')
            
            # Add backfill flag if enabled (triggers hybrid mode: flat files + REST API gap filling)
            if self.backfill:
                cmd.append('--backfill')
            
            self.log_signal.emit(f"Starting backfill: {' '.join(cmd)}")
            
            # Activate venv and set environment
            env = os.environ.copy()
            
            # Start process
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
                cwd=str(Path(__file__).parent.parent)
            )
            
            # Read output line by line
            for line in iter(self.process.stdout.readline, ''):
                if not self.is_running:
                    break
                
                line = line.rstrip()
                if line:
                    self.log_signal.emit(line)
                    self._parse_log_line(line)
            
            # Wait for process to complete
            return_code = self.process.wait()
            
            if return_code == 0:
                self.finished_signal.emit(True, "Backfill completed successfully")
            else:
                self.finished_signal.emit(False, f"Backfill failed with exit code {return_code}")
                
        except Exception as e:
            self.log_signal.emit(f"ERROR: {e}")
            self.finished_signal.emit(False, str(e))
        finally:
            self.is_running = False
    
    def _parse_log_line(self, line: str):
        """Parse log line and emit appropriate signals"""
        # ====================================================================
        # REAL-TIME QUEUE METRICS (from metrics logger thread)
        # ====================================================================
        # Format: "⚡ Pipeline Metrics | Download Q: 45/100 | Process Q: 12/100 | Downloaded: 123 (45.2 MB/s avg) | Processed: 100 | Throughput: 8.42 files/s (avg: 6.78 files/s) | Latency: DL=150ms PR=80ms"
        if "⚡ Pipeline Metrics" in line and "Download Q:" in line:
            try:
                # Extract queue sizes (current/max format)
                dl_part = line.split("Download Q:")[1].split("|")[0].strip()
                pr_part = line.split("Process Q:")[1].split("|")[0].strip()
                
                # Parse "current/max" format
                dl_current, dl_max = map(int, dl_part.split("/"))
                pr_current, pr_max = map(int, pr_part.split("/"))
                
                # Emit queue sizes AND maxsizes to GUI for dynamic progress bars
                self.queue_signal.emit(dl_current, pr_current, dl_max, pr_max)
                
                # Extract Downloaded count
                downloaded_count = 0
                if "Downloaded:" in line:
                    downloaded_part = line.split("Downloaded:")[1].split("(")[0].strip()
                    downloaded_count = int(downloaded_part)
                
                # Extract Processed count
                processed_count = 0
                if "Processed:" in line:
                    processed_part = line.split("Processed:")[1].split("|")[0].strip()
                    processed_count = int(processed_part)
                
                # Extract throughput (MB/s)
                download_mbps = 0.0
                if "MB/s avg" in line:
                    mbps_part = line.split("(")[1].split("MB/s")[0].strip()
                    download_mbps = float(mbps_part)
                
                # Extract latencies (ms)
                download_latency_ms = 0.0
                process_latency_ms = 0.0
                if "Latency: DL=" in line:
                    latency_part = line.split("Latency: DL=")[1]
                    download_latency_ms = float(latency_part.split("ms")[0])
                    if "PR=" in latency_part:
                        process_latency_ms = float(latency_part.split("PR=")[1].split("ms")[0])
                
                # Emit all stats including counts
                self.stats_signal.emit({
                    'downloaded': downloaded_count,
                    'processed': processed_count,
                    'download_mbps': download_mbps,
                    'download_latency_ms': download_latency_ms,
                    'process_latency_ms': process_latency_ms
                })
            except Exception as e:
                pass  # Silently ignore parsing errors
        
        # ====================================================================
        # PHASE 1: Reference Data
        # ====================================================================
        if "=== PHASE 1: Reference Data" in line:
            self.phase_signal.emit('phase1_start', {})
        
        # Parse detailed ticker info: "✓ AMD: Advanced Micro Devices Inc | NASDAQ | Type: CS | Market Cap: $123,456,789"
        if line.strip().startswith("✓") and "|" in line and "Type:" in line:
            try:
                # Extract after the checkmark
                parts = line.split("✓")[1].strip()
                
                # Split by ticker (before first colon)
                ticker = parts.split(":")[0].strip()
                
                # Extract name (between : and first |)
                name = parts.split(":")[1].split("|")[0].strip()
                
                # Extract exchange (between first | and second |)
                exchange = parts.split("|")[1].strip()
                
                # Extract type (between Type: and |)
                type_part = parts.split("Type:")[1].split("|")[0].strip()
                
                # Extract market cap (after Market Cap:)
                market_cap_str = parts.split("Market Cap:")[1].strip()
                
                self.phase_signal.emit('phase1_ticker', {
                    'ticker': ticker,
                    'name': name,
                    'exchange': exchange,
                    'type': type_part,
                    'market_cap': market_cap_str,
                    'status': 'Success'
                })
            except Exception as e:
                pass
        
        if "✗ No reference data for" in line or "No reference data for" in line:
            # Extract ticker
            try:
                ticker = line.split("reference data for")[1].strip()
                self.phase_signal.emit('phase1_ticker', {
                    'ticker': ticker,
                    'status': 'Missing',
                    'name': '',
                    'exchange': '',
                    'type': '',
                    'market_cap': ''
                })
            except:
                pass
        
        if "Phase 1 complete:" in line:
            # Extract: "Phase 1 complete: 1/1 successful"
            try:
                parts = line.split("Phase 1 complete:")[1].strip()
                success, total = parts.split("/")
                total = int(total.split()[0])
                self.phase_signal.emit('phase1_complete', {
                    'success': int(success),
                    'total': total
                })
            except:
                pass
        
        # ====================================================================
        # PHASE 2: Corporate Actions
        # ====================================================================
        if "=== PHASE 2: Corporate Actions" in line:
            self.phase_signal.emit('phase2_start', {})
        
        # Parse dividend info: "✓ AMD: Found 4 dividends since 2020-01-01"
        if "Found" in line and "dividends since" in line:
            try:
                parts = line.split("✓")[1].strip() if "✓" in line else line.strip()
                ticker = parts.split(":")[0].strip()
                count = int(parts.split("Found")[1].split("dividends")[0].strip())
                since_date = parts.split("since")[1].strip()
                
                self.phase_signal.emit('phase2_dividend', {
                    'ticker': ticker,
                    'count': count,
                    'since': since_date
                })
            except:
                pass
        
        # Parse individual dividend: "  └─ Dividend: $0.2500 on 2024-03-15"
        if "└─ Dividend:" in line:
            try:
                amount_str = line.split("$")[1].split("on")[0].strip()
                date_str = line.split("on")[1].strip()
                
                self.phase_signal.emit('phase2_dividend_detail', {
                    'amount': float(amount_str),
                    'date': date_str
                })
            except:
                pass
        
        # Parse split info: "✓ AMD: Found 1 splits since 2020-01-01"
        if "Found" in line and "splits since" in line:
            try:
                parts = line.split("✓")[1].strip() if "✓" in line else line.strip()
                ticker = parts.split(":")[0].strip()
                count = int(parts.split("Found")[1].split("splits")[0].strip())
                since_date = parts.split("since")[1].strip()
                
                self.phase_signal.emit('phase2_split', {
                    'ticker': ticker,
                    'count': count,
                    'since': since_date
                })
            except:
                pass
        
        # Parse individual split: "  └─ Split: 2.00:1 on 2024-06-01"
        if "└─ Split:" in line:
            try:
                ratio_str = line.split("Split:")[1].split(":1")[0].strip()
                date_str = line.split("on")[1].strip()
                
                self.phase_signal.emit('phase2_split_detail', {
                    'ratio': float(ratio_str),
                    'date': date_str
                })
            except:
                pass
        
        if "Phase 2 complete" in line:
            self.phase_signal.emit('phase2_complete', {})
        
        # ====================================================================
        # PHASE 3: Daily Bars
        # ====================================================================
        if "=== PHASE 3: Daily Bars" in line:
            self.phase_signal.emit('phase3_start', {})
        
        # Parse daily bars info: "✓ AMD: 1234 daily bars | 2020-01-01 to 2024-12-31"
        if "daily bars |" in line and "to" in line:
            try:
                parts = line.split("✓")[1].strip() if "✓" in line else line.strip()
                ticker = parts.split(":")[0].strip()
                count = int(parts.split(":")[1].split("daily bars")[0].strip())
                date_range = parts.split("|")[1].strip()
                
                self.phase_signal.emit('phase3_ticker', {
                    'ticker': ticker,
                    'bars': count,
                    'date_range': date_range
                })
            except:
                pass
        
        # Parse first/last bar details for debugging
        if "└─ First:" in line or "└─ Last:" in line:
            try:
                # Extract OHLC values for validation
                # Format: "  └─ First: O=$123.45 H=$124.56 L=$122.34 C=$123.89 V=12,345,678"
                pass  # Just for debugging in logs, not displayed in table
            except:
                pass
        
        if "Phase 3 complete:" in line:
            # Extract: "Phase 3 complete: 1,234 daily bars written"
            try:
                parts = line.split("Phase 3 complete:")[1].strip()
                bars = parts.split()[0].replace(',', '')
                self.phase_signal.emit('phase3_complete', {
                    'bars': int(bars)
                })
            except:
                pass
        
        # ====================================================================
        # PHASE 4: Pipeline Statistics
        # ====================================================================
        if "Total files:" in line:
            try:
                total = int(line.split("Total files:")[1].strip())
                self.stats_signal.emit({'total': total})
            except:
                pass
        
        if "Downloaded:" in line:
            try:
                downloaded = int(line.split("Downloaded:")[1].strip())
                self.stats_signal.emit({'downloaded': downloaded})
            except:
                pass
        
        if "Skipped (cached):" in line or "Skipped:" in line:
            try:
                if "Skipped (cached):" in line:
                    skipped = int(line.split("Skipped (cached):")[1].strip())
                else:
                    skipped = int(line.split("Skipped:")[1].strip())
                self.stats_signal.emit({'skipped': skipped})
            except:
                pass
        
        if "Failed (404/etc):" in line or "Failed:" in line:
            try:
                if "Failed (404/etc):" in line:
                    failed = int(line.split("Failed (404/etc):")[1].strip())
                else:
                    failed = int(line.split("Failed:")[1].strip())
                self.stats_signal.emit({'failed': failed})
            except:
                pass
        
        if "Processed:" in line:
            try:
                processed = int(line.split("Processed:")[1].strip())
                self.stats_signal.emit({'processed': processed})
            except:
                pass
        
        # ====================================================================
        # FILE STATE TRANSITIONS (Legacy format - still supported)
        # ====================================================================
        # Format: "File file0123 [2024-01-15]: WAITING → DOWNLOADING"
        if "File file" in line and "→" in line:
            try:
                parts = line.split("File ")[1]
                task_id = parts.split()[0]
                
                # Extract date
                date_match = parts.split("[")[1].split("]")[0]
                
                # Extract states
                state_part = parts.split(": ")[1]
                states = state_part.split("→")
                old_state = states[0].strip()
                
                # New state might include error in parentheses
                new_state_full = states[1].strip()
                error = ""
                
                if "(" in new_state_full:
                    new_state = new_state_full.split("(")[0].strip()
                    error = new_state_full.split("(")[1].split(")")[0]
                else:
                    new_state = new_state_full
                
                info = {
                    'date': date_match,
                    'old_state': old_state,
                    'error': error,
                    'filename': f"{date_match}.csv.gz"
                }
                
                self.task_update_signal.emit(task_id, new_state, info)
            except Exception as e:
                pass
        
        # ====================================================================
        # DEBUG MODE: Queue Pull Operations
        # ====================================================================
        # Format: "DEBUG: Queue Pull -> file0043 [2025-01-31] from download_queue (qsize=99)"
        if "DEBUG: Queue Pull" in line and "from download_queue" in line:
            try:
                parts = line.split("DEBUG: Queue Pull -> ")[1]
                task_id = parts.split()[0]
                date_match = parts.split("[")[1].split("]")[0]
                qsize = int(parts.split("qsize=")[1].split(")")[0])
                
                info = {
                    'date': date_match,
                    'old_state': 'WAITING',
                    'error': '',
                    'filename': f"{date_match}.csv.gz",
                    'queue': 'download',
                    'qsize': qsize
                }
                
                # Don't change state yet, just log the pull
                self.task_update_signal.emit(task_id, 'WAITING', info)
            except:
                pass
        
        if "DEBUG: Queue Pull" in line and "from process_queue" in line:
            try:
                parts = line.split("DEBUG: Queue Pull -> ")[1]
                task_id = parts.split()[0]
                date_match = parts.split("[")[1].split("]")[0]
                qsize = int(parts.split("qsize=")[1].split(")")[0])
                
                info = {
                    'date': date_match,
                    'old_state': 'DOWNLOADED',
                    'error': '',
                    'filename': f"{date_match}.csv.gz",
                    'queue': 'process',
                    'qsize': qsize
                }
                
                # Don't change state yet
                self.task_update_signal.emit(task_id, 'DOWNLOADED', info)
            except:
                pass
        
        # ====================================================================
        # DEBUG MODE: Stack Update Operations
        # ====================================================================
        # Format: "DEBUG: Stack Update -> file0043 [2025-01-31] → DOWNLOADING"
        if "DEBUG: Stack Update" in line and "→" in line:
            try:
                parts = line.split("DEBUG: Stack Update -> ")[1]
                task_id = parts.split()[0]
                date_match = parts.split("[")[1].split("]")[0]
                
                # Extract new state
                state_part = parts.split("→")[1].strip()
                
                # Handle error/retry messages in parentheses
                error = ""
                if "(" in state_part:
                    new_state = state_part.split("(")[0].strip()
                    error = state_part.split("(")[1].split(")")[0]
                else:
                    new_state = state_part
                
                info = {
                    'date': date_match,
                    'error': error,
                    'filename': f"{date_match}.csv.gz"
                }
                
                self.task_update_signal.emit(task_id, new_state, info)
            except:
                pass
        
        # ====================================================================
        # DEBUG MODE: Stack Push Operations
        # ====================================================================
        # Format: "DEBUG: Stack Push -> file0043 [2025-01-31] to process_queue (qsize=1)"
        if "DEBUG: Stack Push" in line and "to process_queue" in line:
            try:
                parts = line.split("DEBUG: Stack Push -> ")[1]
                task_id = parts.split()[0]
                date_match = parts.split("[")[1].split("]")[0]
                qsize = int(parts.split("qsize=")[1].split(")")[0])
                
                info = {
                    'date': date_match,
                    'error': '',
                    'filename': f"{date_match}.csv.gz",
                    'process_qsize': qsize
                }
                
                # Update to DOWNLOADED state when pushed to process queue
                self.task_update_signal.emit(task_id, 'DOWNLOADED', info)
            except:
                pass
        
        # Format: "DEBUG: Stack Push -> file0043 [2025-01-31] → DOWNLOADED (cached)"
        if "DEBUG: Stack Push" in line and "DOWNLOADED (cached)" in line:
            try:
                parts = line.split("DEBUG: Stack Push -> ")[1]
                task_id = parts.split()[0]
                date_match = parts.split("[")[1].split("]")[0]
                
                info = {
                    'date': date_match,
                    'error': '',
                    'filename': f"{date_match}.csv.gz",
                    'cached': True
                }
                
                self.task_update_signal.emit(task_id, 'DOWNLOADED', info)
            except:
                pass
        
        # ====================================================================
        # DEBUG MODE: Stack Pop Operations (Completion)
        # ====================================================================
        # Format: "DEBUG: Stack Pop -> file0043 [2025-01-31] → COMPLETED"
        if "DEBUG: Stack Pop" in line and "COMPLETED" in line:
            try:
                parts = line.split("DEBUG: Stack Pop -> ")[1]
                task_id = parts.split()[0]
                date_match = parts.split("[")[1].split("]")[0]
                
                info = {
                    'date': date_match,
                    'error': '',
                    'filename': f"{date_match}.csv.gz"
                }
                
                self.task_update_signal.emit(task_id, 'COMPLETED', info)
            except:
                pass
        
        # ====================================================================
        # BARS WRITTEN (for processing table and daily bars in debug mode)
        # ====================================================================
        # Phase 3 format: "✓ AMD: 1234 daily bars | 2020-01-01 to 2024-12-31"
        # Already parsed above in phase3_ticker
        
        # Phase 4 Debug format: "DEBUG: Wrote 12,345 bars from 2025-01-31 (AMD, AAPL, GOOGL)"
        if "DEBUG: Wrote" in line and "bars from" in line:
            try:
                bars = int(line.split("Wrote")[1].split("bars")[0].strip().replace(',', ''))
                date_str = line.split("bars from")[1].split("(")[0].strip()
                
                # Extract ticker list if present
                tickers_str = ""
                if "(" in line:
                    tickers_str = line.split("(")[1].split(")")[0]
                
                # Emit as phase4_bars_written signal
                self.phase_signal.emit('phase4_bars_written', {
                    'bars': bars,
                    'date': date_str,
                    'tickers': tickers_str
                })
            except:
                pass
    
    def stop(self):
        """Stop the backfill process"""
        self.is_running = False
        if self.process and self.process.poll() is None:
            self.log_signal.emit("Stopping backfill process...")
            self.process.send_signal(signal.SIGINT)
            self.process.wait(timeout=5)


# ============================================================================
# ENHANCEMENT: PURGE FLATFILES FEATURE
# ============================================================================
# Allows users to delete all downloaded flatpack files (.csv, .csv.gz, .parquet)
# from the data/flatfiles directory via GUI button with confirmation dialog.
# Runs in background thread to avoid blocking UI. Reports files deleted and space freed.
# ============================================================================

class PurgeWorker(QThread):
    """Background worker for purging flatpack files"""
    progress_signal = pyqtSignal(str)  # Progress messages
    finished_signal = pyqtSignal(int, int)  # files_deleted, bytes_freed
    
    def run(self):
        """Delete all .csv and .csv.gz files from flatpack directory"""
        flatpack_dir = Path(__file__).parent.parent / 'data' / 'flatfiles'
        
        files_deleted = 0
        bytes_freed = 0
        
        if not flatpack_dir.exists():
            self.progress_signal.emit(f"Flatfiles directory not found: {flatpack_dir}")
            self.finished_signal.emit(0, 0)
            return
        
        # Find all flatpack files
        patterns = ['**/*.csv', '**/*.csv.gz', '**/*.parquet']
        for pattern in patterns:
            for file_path in flatpack_dir.glob(pattern):
                try:
                    size = file_path.stat().st_size
                    file_path.unlink()
                    files_deleted += 1
                    bytes_freed += size
                    self.progress_signal.emit(f"Deleted: {file_path.name} ({size / 1024 / 1024:.2f} MB)")
                except Exception as e:
                    self.progress_signal.emit(f"Error deleting {file_path.name}: {e}")
        
        self.finished_signal.emit(files_deleted, bytes_freed)


# ============================================================================
# POLYGON.IO MARKET DATA INTEGRATION
# ============================================================================

if POLYGON_DEPS_AVAILABLE:
    class PolygonDataWorker(QThread):
        """Background worker for Polygon.io WebSocket + REST API data streaming"""
        
        # Qt signals for thread-safe communication
        candle_signal = pyqtSignal(str, dict)  # ticker, candle_data
        indicator_signal = pyqtSignal(str, dict)  # ticker, indicators
        news_signal = pyqtSignal(list)  # news articles
        reference_signal = pyqtSignal(dict)  # reference data
        corporate_signal = pyqtSignal(dict)  # dividends and splits
        status_signal = pyqtSignal(str)  # connection status
        error_signal = pyqtSignal(str)  # error messages
        
        def __init__(self, ticker: str, api_key: str, parent=None):
            super().__init__(parent)
            self.ticker = ticker
            self.api_key = api_key
            self.running = True
            self.loop = None
            
            # WebSocket URL (delayed for starter plan)
            self.ws_url = "wss://delayed.polygon.io/stocks"
            
            # Data storage
            self.candles: deque = deque(maxlen=1000)
            self.last_indicator_calc = datetime.now()
            
        def run(self):
            """Run asyncio event loop in this thread"""
            try:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
                self.loop.run_until_complete(self._run_all_tasks())
            except Exception as e:
                self.error_signal.emit(f"Worker error: {e}")
            finally:
                if self.loop:
                    self.loop.close()
        
        async def _run_all_tasks(self):
            """Run all async tasks concurrently"""
            tasks = [
                asyncio.create_task(self._websocket_client()),
                asyncio.create_task(self._fetch_reference_data()),
                asyncio.create_task(self._fetch_corporate_actions()),
                asyncio.create_task(self._fetch_news_loop()),
                asyncio.create_task(self._calculate_indicators_loop()),
            ]
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                for task in tasks:
                    task.cancel()
        
        async def _websocket_client(self):
            """Connect to Polygon WebSocket and stream candles"""
            while self.running:
                try:
                    self.status_signal.emit("🔄 Connecting to Polygon WebSocket...")
                    
                    async with websockets.connect(self.ws_url) as ws:
                        # Step 1: Wait for connection status
                        connect_response = await ws.recv()
                        connect_data = json.loads(connect_response)
                        logging.info(f"WebSocket connected: {connect_data}")
                        
                        # Step 2: Authenticate
                        auth_msg = {"action": "auth", "params": self.api_key}
                        await ws.send(json.dumps(auth_msg))
                        
                        # Step 3: Wait for auth response
                        auth_response = await ws.recv()
                        auth_data = json.loads(auth_response)
                        logging.info(f"Auth response: {auth_data}")
                        
                        # Check if authentication was successful
                        auth_success = False
                        if isinstance(auth_data, list) and len(auth_data) > 0:
                            auth_success = auth_data[0].get("status") == "auth_success"
                        elif isinstance(auth_data, dict):
                            auth_success = auth_data.get("status") == "auth_success"
                        
                        if not auth_success:
                            error_msg = f"❌ WebSocket auth failed. Response: {auth_data}"
                            self.error_signal.emit(error_msg)
                            self.status_signal.emit("❌ Auth Failed")
                            logging.error(error_msg)
                            return
                        
                        self.status_signal.emit("✓ Authenticated")
                        
                        # Step 2: Subscribe to aggregate minute bars
                        subscribe_msg = {"action": "subscribe", "params": f"AM.{self.ticker}"}
                        await ws.send(json.dumps(subscribe_msg))
                        
                        self.status_signal.emit(f"✓ Subscribed to {self.ticker}")
                        
                        # Step 3: Stream candles
                        async for message in ws:
                            if not self.running:
                                break
                            
                            data = json.loads(message)
                            
                            for item in data:
                                if item.get("ev") == "AM":  # Aggregate Minute
                                    candle = {
                                        'timestamp': datetime.fromtimestamp(item['s'] / 1000),
                                        'open': item['o'],
                                        'high': item['h'],
                                        'low': item['l'],
                                        'close': item['c'],
                                        'volume': item['v'],
                                        'vwap': item.get('vw', item['c']),
                                        'trades': item.get('n', 0)
                                    }
                                    
                                    self.candles.append(candle)
                                    self.candle_signal.emit(self.ticker, candle)
                
                except Exception as e:
                    self.error_signal.emit(f"WebSocket error: {e}")
                    self.status_signal.emit("❌ Disconnected")
                    
                    if self.running:
                        await asyncio.sleep(5)  # Reconnect delay
        
        async def _fetch_reference_data(self):
            """Fetch reference data (company info)"""
            try:
                url = f"https://api.polygon.io/v3/reference/tickers/{self.ticker}"
                params = {"apiKey": self.api_key}
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'results' in data:
                                self.reference_signal.emit(data['results'])
            except Exception as e:
                self.error_signal.emit(f"Reference data error: {e}")
        
        async def _fetch_corporate_actions(self):
            """Fetch dividends and splits"""
            try:
                # Fetch dividends
                div_url = f"https://api.polygon.io/v3/reference/dividends"
                params = {"ticker": self.ticker, "limit": 100, "apiKey": self.api_key}
                
                dividends = []
                splits = []
                
                async with aiohttp.ClientSession() as session:
                    # Get dividends
                    async with session.get(div_url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            dividends = data.get('results', [])
                    
                    # Get splits
                    split_url = f"https://api.polygon.io/v3/reference/splits"
                    async with session.get(split_url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            splits = data.get('results', [])
                
                self.corporate_signal.emit({
                    'dividends': dividends,
                    'splits': splits
                })
            except Exception as e:
                self.error_signal.emit(f"Corporate actions error: {e}")
        
        async def _fetch_news_loop(self):
            """Fetch news articles periodically"""
            while self.running:
                try:
                    url = "https://api.polygon.io/v2/reference/news"
                    params = {
                        "ticker": self.ticker,
                        "limit": 10,
                        "apiKey": self.api_key
                    }
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, params=params) as response:
                            if response.status == 200:
                                data = await response.json()
                                articles = data.get('results', [])
                                self.news_signal.emit(articles)
                    
                    await asyncio.sleep(30)  # Update every 30 seconds
                except Exception as e:
                    self.error_signal.emit(f"News fetch error: {e}")
                    await asyncio.sleep(30)
        
        async def _calculate_indicators_loop(self):
            """Calculate technical indicators periodically"""
            while self.running:
                try:
                    await asyncio.sleep(5)  # Calculate every 5 seconds
                    
                    if len(self.candles) < 50:
                        continue
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(list(self.candles))
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.set_index('timestamp')
                    
                    # Calculate indicators
                    indicators = {}
                    
                    # SMA
                    indicators['sma_20'] = df['close'].rolling(20).mean().tolist()
                    indicators['sma_50'] = df['close'].rolling(50).mean().tolist()
                    
                    # EMA
                    indicators['ema_12'] = df['close'].ewm(span=12).mean().tolist()
                    indicators['ema_26'] = df['close'].ewm(span=26).mean().tolist()
                    
                    # RSI
                    delta = df['close'].diff()
                    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                    rs = gain / loss
                    indicators['rsi'] = (100 - (100 / (1 + rs))).tolist()
                    
                    # MACD
                    ema12 = df['close'].ewm(span=12).mean()
                    ema26 = df['close'].ewm(span=26).mean()
                    indicators['macd'] = (ema12 - ema26).tolist()
                    indicators['macd_signal'] = (ema12 - ema26).ewm(span=9).mean().tolist()
                    indicators['macd_hist'] = ((ema12 - ema26) - (ema12 - ema26).ewm(span=9).mean()).tolist()
                    
                    # Bollinger Bands
                    sma20 = df['close'].rolling(20).mean()
                    std20 = df['close'].rolling(20).std()
                    indicators['bb_upper'] = (sma20 + 2 * std20).tolist()
                    indicators['bb_lower'] = (sma20 - 2 * std20).tolist()
                    indicators['bb_middle'] = sma20.tolist()
                    
                    # VWAP (from candle data)
                    indicators['vwap'] = df['vwap'].tolist()
                    
                    # Timestamps
                    indicators['timestamps'] = df.index.tolist()
                    
                    self.indicator_signal.emit(self.ticker, indicators)
                    
                except Exception as e:
                    self.error_signal.emit(f"Indicator calc error: {e}")
                    await asyncio.sleep(5)
        
        def stop(self):
            """Stop the worker thread"""
            self.running = False
            if self.loop:
                self.loop.call_soon_threadsafe(self.loop.stop)


    # =================================================================
    # CHART BACKEND ABSTRACTION LAYER
    # =================================================================
    # Allows swapping between PyQtGraph (current) and FinPlot (future)
    # without changing the rest of the GUI code
    # =================================================================
    
    class ChartBackend:
        """Abstract base class for chart rendering backends"""
        
        def __init__(self, parent_widget):
            self.parent = parent_widget
        
        def create_widget(self):
            """Create and return the native chart widget"""
            raise NotImplementedError
        
        def update_data(self, df, indicators, patterns=None, ticker='', timeframe='', display_mode='price', reference_price=None):
            """Update chart with new data"""
            raise NotImplementedError
        
        def add_pattern_overlay(self, pattern, x, closes, highs, lows, n):
            """Add pattern visualization overlay"""
            raise NotImplementedError
        
        def clear(self):
            """Clear chart"""
            raise NotImplementedError
        
        def set_mode(self, mode):
            """Set display mode (price/percentage)"""
            raise NotImplementedError
    
    
    class PyQtGraphBackend(ChartBackend):
        """PyQtGraph implementation (current default)"""
        
        def __init__(self, parent_widget):
            super().__init__(parent_widget)
            import pyqtgraph as pg
            self.pg = pg
            self.plot_main = None
            self.plot_rsi = None
            self.plot_macd = None
            self.volume_axis = None
            self.display_mode = 'price'
            self.reference_price = None
        
        def create_widget(self):
            """Create PyQtGraph widget"""
            import pyqtgraph as pg
            pg.setConfigOptions(antialias=True)
            
            # Single GraphicsLayoutWidget with one primary plot
            chart_view = pg.GraphicsLayoutWidget()
            chart_view.setBackground('#1e1e1e')
            
            # Main price plot with overlays
            self.plot_main = chart_view.addPlot(row=0, col=0)
            self.plot_main.showGrid(x=True, y=True, alpha=0.3)
            self.plot_main.setLabel('left', 'Price ($)', color='white', size='12pt')
            self.plot_main.setLabel('bottom', 'Time', color='white', size='10pt')
            self.plot_main.setMenuEnabled(False)
            
            # Secondary Y-axis for volume
            self.volume_axis = pg.ViewBox()
            self.plot_main.scene().addItem(self.volume_axis)
            self.plot_main.getAxis('right').linkToView(self.volume_axis)
            self.volume_axis.setXLink(self.plot_main)
            
            # RSI panel (collapsible)
            self.plot_rsi = chart_view.addPlot(row=1, col=0)
            self.plot_rsi.showGrid(x=True, y=True, alpha=0.3)
            self.plot_rsi.setLabel('left', 'RSI', color='white', size='10pt')
            self.plot_rsi.setYRange(0, 100)
            self.plot_rsi.setMaximumHeight(100)
            self.plot_rsi.setXLink(self.plot_main)
            self.plot_rsi.addLine(y=70, pen=pg.mkPen('#ef5350', width=1, style=pg.QtCore.Qt.PenStyle.DashLine))
            self.plot_rsi.addLine(y=30, pen=pg.mkPen('#66bb6a', width=1, style=pg.QtCore.Qt.PenStyle.DashLine))
            self.plot_rsi.hide()
            
            # MACD panel (collapsible)
            self.plot_macd = chart_view.addPlot(row=2, col=0)
            self.plot_macd.showGrid(x=True, y=True, alpha=0.3)
            self.plot_macd.setLabel('left', 'MACD', color='white', size='10pt')
            self.plot_macd.setMaximumHeight(100)
            self.plot_macd.setXLink(self.plot_main)
            self.plot_macd.hide()
            
            self.plot_main.setMouseEnabled(x=True, y=True)
            self.plot_main.enableAutoRange(axis='y')
            
            return chart_view
        
        def update_data(self, df, indicators, patterns=None, ticker='', timeframe='', display_mode='price', reference_price=None):
            """Update chart with OHLCV data, indicators, and patterns - NO AGGREGATION"""
            if df is None or df.empty:
                return
            
            import numpy as np
            n = len(df)
            x = np.arange(n)
            
            # Extract OHLCV (already aggregated from get_aggregated_bars)
            opens = df['open'].values
            highs = df['high'].values
            lows = df['low'].values
            closes = df['close'].values
            volumes = df['volume'].values
            
            # Apply Y-axis scaling for percentage mode
            if display_mode == 'percentage' and reference_price:
                ref = reference_price
                opens = (opens / ref - 1) * 100
                highs = (highs / ref - 1) * 100
                lows = (lows / ref - 1) * 100
                closes = (closes / ref - 1) * 100
                ylabel = '% Change'
            else:
                ylabel = 'Price ($)'
            
            # Disable autorange during update (performance)
            self.plot_main.enableAutoRange(enable=False)
            
            # Clear and redraw
            self.plot_main.clear()
            
            # Draw candlesticks
            for i in range(n):
                color = (38, 166, 154) if closes[i] >= opens[i] else (239, 83, 80)
                self.plot_main.plot([i, i], [lows[i], highs[i]], pen=self.pg.mkPen(color, width=1))
                body_height = abs(closes[i] - opens[i]) if abs(closes[i] - opens[i]) > 0 else 0.001
                body_y = min(opens[i], closes[i])
                rect = self.pg.QtWidgets.QGraphicsRectItem(i - 0.3, body_y, 0.6, body_height)
                rect.setBrush(self.pg.mkBrush(color))
                rect.setPen(self.pg.mkPen(color))
                self.plot_main.addItem(rect)
            
            # Draw volume on secondary axis
            self.volume_axis.setGeometry(self.plot_main.vb.sceneBoundingRect())
            self.volume_axis.linkedViewChanged(self.plot_main.vb, self.volume_axis.XAxis)
            for item in self.volume_axis.addedItems[:]:
                self.volume_axis.removeItem(item)
            
            colors_vol = [(38, 166, 154, 80) if closes[i] >= opens[i] else (239, 83, 80, 80) for i in range(n)]
            for i in range(n):
                bar = self.pg.QtWidgets.QGraphicsRectItem(i - 0.4, 0, 0.8, volumes[i])
                bar.setBrush(self.pg.mkBrush(*colors_vol[i]))
                bar.setPen(self.pg.mkPen(None))
                self.volume_axis.addItem(bar)
            self.volume_axis.setYRange(0, volumes.max() * 4)
            
            # Draw indicators (from pre-calculated indicators in df)
            def add_line(col_name, color, width=2, style=self.pg.QtCore.Qt.PenStyle.SolidLine):
                if col_name in df.columns:
                    data = df[col_name].values
                    if display_mode == 'percentage' and reference_price:
                        data = (data / reference_price - 1) * 100
                    valid = ~np.isnan(data)
                    if valid.any():
                        self.plot_main.plot(x[valid], data[valid], pen=self.pg.mkPen(color, width=width, style=style))
            
            if indicators.get('show_sma'):
                add_line('sma_20', '#FFA726', 2)
                add_line('sma_50', '#FF7043', 2)
            
            if indicators.get('show_ema'):
                add_line('ema_20', '#42A5F5', 2)
                add_line('ema_50', '#1E88E5', 2)
                add_line('ema_100', '#1565C0', 1)
                add_line('ema_200', '#0D47A1', 1)
            
            if indicators.get('show_bb'):
                add_line('bb_upper', '#78909C', 1, self.pg.QtCore.Qt.PenStyle.DashLine)
                add_line('bb_lower', '#78909C', 1, self.pg.QtCore.Qt.PenStyle.DashLine)
                if 'bb_upper' in df.columns and 'bb_lower' in df.columns:
                    upper = df['bb_upper'].values
                    lower = df['bb_lower'].values
                    if display_mode == 'percentage' and reference_price:
                        upper = (upper / reference_price - 1) * 100
                        lower = (lower / reference_price - 1) * 100
                    valid = ~(np.isnan(upper) | np.isnan(lower))
                    if valid.any():
                        fill = self.pg.FillBetweenItem(
                            self.plot_main.plot(x[valid], upper[valid], pen=None),
                            self.plot_main.plot(x[valid], lower[valid], pen=None),
                            brush=self.pg.mkBrush(120, 144, 156, 30)
                        )
                        self.plot_main.addItem(fill)
            
            if indicators.get('show_vwap'):
                add_line('vwap', '#FFEE58', 2)
            
            # Draw pattern overlays (limit to 20 most recent patterns for performance)
            if patterns and patterns.get('patterns'):
                pattern_list = patterns['patterns'][:20]  # Limit to 20 patterns
                for pattern in pattern_list:
                    self.add_pattern_overlay(pattern, x, closes, highs, lows, n)
            
            # Update RSI panel
            if indicators.get('show_rsi') and 'rsi_14' in df.columns:
                self.plot_rsi.show()
                self.plot_rsi.clear()
                rsi = df['rsi_14'].values
                valid = ~np.isnan(rsi)
                if valid.any():
                    self.plot_rsi.plot(x[valid], rsi[valid], pen=self.pg.mkPen('#ab47bc', width=2))
                self.plot_rsi.addLine(y=70, pen=self.pg.mkPen('#ef5350', width=1, style=self.pg.QtCore.Qt.PenStyle.DashLine))
                self.plot_rsi.addLine(y=30, pen=self.pg.mkPen('#66bb6a', width=1, style=self.pg.QtCore.Qt.PenStyle.DashLine))
            else:
                self.plot_rsi.hide()
            
            # Update MACD panel
            if indicators.get('show_macd') and 'macd' in df.columns:
                self.plot_macd.show()
                self.plot_macd.clear()
                macd = df['macd'].values
                signal = df.get('macd_signal', pd.Series()).values if 'macd_signal' in df.columns else None
                hist = df.get('macd_hist', pd.Series()).values if 'macd_hist' in df.columns else None
                
                valid = ~np.isnan(macd)
                if valid.any():
                    self.plot_macd.plot(x[valid], macd[valid], pen=self.pg.mkPen('#2196F3', width=2))
                if signal is not None:
                    valid_sig = ~np.isnan(signal)
                    if valid_sig.any():
                        self.plot_macd.plot(x[valid_sig], signal[valid_sig], pen=self.pg.mkPen('#FF9800', width=2))
                if hist is not None:
                    valid_hist = ~np.isnan(hist)
                    colors_hist = [(76, 175, 80) if h >= 0 else (244, 67, 54) for h in hist[valid_hist]]
                    bg = self.pg.BarGraphItem(x=x[valid_hist], height=hist[valid_hist], width=0.6, brushes=colors_hist)
                    self.plot_macd.addItem(bg)
            else:
                self.plot_macd.hide()
            
            self.plot_main.setLabel('left', ylabel)
            self.plot_main.setTitle(f"{ticker} - {timeframe}")
            
            # Re-enable autorange
            self.plot_main.enableAutoRange(axis='y', enable=True)
        
        def add_pattern_overlay(self, pattern, x, closes, highs, lows, n):
            """Add pattern visualization as overlay"""
            try:
                start_idx = pattern.get('start_index', 0)
                end_idx = pattern.get('end_index', n-1)
                start_idx = max(0, min(start_idx, n-1))
                end_idx = max(0, min(end_idx, n-1))
                
                if start_idx >= end_idx:
                    return
                
                pattern_type = pattern.get('type', 'unknown')
                subtype = pattern.get('subtype', '')
                confidence = pattern.get('confidence', 0.5)
                
                # Determine color based on type
                if 'bullish' in subtype.lower() or pattern_type in ['ascending_triangle', 'inverse_head_shoulders']:
                    color = (76, 175, 80, 100)  # Green
                elif 'bearish' in subtype.lower() or pattern_type in ['descending_triangle', 'head_shoulders']:
                    color = (244, 67, 54, 100)  # Red
                else:
                    color = (255, 235, 59, 100)  # Yellow (neutral)
                
                # Draw shaded region over pattern
                y_min = min(lows[start_idx:end_idx+1]) * 0.99
                y_max = max(highs[start_idx:end_idx+1]) * 1.01
                
                region = self.pg.QtWidgets.QGraphicsRectItem(
                    start_idx, y_min,
                    end_idx - start_idx, y_max - y_min
                )
                region.setBrush(self.pg.mkBrush(*color))
                region.setPen(self.pg.mkPen(None))
                self.plot_main.addItem(region)
                
                # Add label annotation
                label_text = f"{pattern_type.replace('_', ' ').title()}\n{confidence:.1%}"
                label = self.pg.TextItem(label_text, anchor=(0.5, 1), color='white')
                label.setPos((start_idx + end_idx) / 2, y_max)
                self.plot_main.addItem(label)
            
            except Exception as e:
                logging.error(f"Pattern overlay error: {e}")
        
        def clear(self):
            """Clear all chart data"""
            if self.plot_main:
                self.plot_main.clear()
            if self.plot_rsi:
                self.plot_rsi.clear()
            if self.plot_macd:
                self.plot_macd.clear()
        
        def set_mode(self, mode):
            """Set display mode - handled during update_data() call"""
            # Mode switching is stateless - applied during update_data()
            pass
    
    
    class FinPlotBackend(ChartBackend):
        """FinPlot implementation using mplfinance - professional financial charting"""
        
        def __init__(self, parent_widget):
            super().__init__(parent_widget)
            self.canvas = None
            self.figure = None
            self.axes = {}
            self.container = None
            self.container_layout = None
            
            self.last_df = None
            self.last_indicators = {}
            self.last_patterns = None
            self.current_display_mode = 'price'
            self.current_reference_price = None
            
            # Lazy imports - will be done in create_widget
            self.mpf = None
            self.plt = None
            self.FigureCanvas = None
            self.Figure = None
        
        def create_widget(self):
            """Create matplotlib canvas widget for embedding charts"""
            from PyQt6.QtWidgets import QWidget, QVBoxLayout
            import mplfinance as mpf
            from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
            from matplotlib.figure import Figure
            import matplotlib.pyplot as plt
            import matplotlib
            
            # Set matplotlib to use Qt backend
            matplotlib.use('QtAgg')
            
            # Store imports for later use
            self.mpf = mpf
            self.plt = plt
            self.FigureCanvas = FigureCanvas
            self.Figure = Figure
            
            # Configure matplotlib style for dark theme
            plt.style.use('dark_background')
            
            # Create a container widget
            self.container = QWidget()
            self.container_layout = QVBoxLayout(self.container)
            self.container_layout.setContentsMargins(0, 0, 0, 0)
            
            # Placeholder canvas (will be replaced when data is plotted)
            self.figure = self.Figure(figsize=(12, 8), facecolor='#1e1e1e')
            self.canvas = self.FigureCanvas(self.figure)
            self.container_layout.addWidget(self.canvas)
            
            return self.container
        
        def update_data(self, df, indicators, patterns=None, ticker='', timeframe='', display_mode='price', reference_price=None):
            """Update chart with OHLCV data using mplfinance"""
            if df is None or df.empty:
                return
            
            # Ensure matplotlib imports are loaded (might be None on first call before create_widget)
            if self.mpf is None:
                logging.warning("FinPlotBackend not initialized, skipping update")
                return
            
            try:
                import pandas as pd
                import numpy as np
                
                # Store for later use
                self.last_df = df.copy()
                self.last_indicators = indicators
                self.last_patterns = patterns
                self.current_display_mode = display_mode
                self.current_reference_price = reference_price
                
                # Clear previous plots
                self.figure.clear()
                
                # Prepare dataframe with datetime index (required by mplfinance)
                plot_df = df.copy()
                if not isinstance(plot_df.index, pd.DatetimeIndex):
                    if 'timestamp' in plot_df.columns:
                        plot_df.index = pd.to_datetime(plot_df['timestamp'])
                    else:
                        plot_df.index = pd.date_range(start='2024-01-01', periods=len(plot_df), freq='1min')
                
                # Apply percentage mode if needed
                if display_mode == 'percentage' and reference_price:
                    ref = reference_price
                    plot_df['open'] = (plot_df['open'] / ref - 1) * 100
                    plot_df['high'] = (plot_df['high'] / ref - 1) * 100
                    plot_df['low'] = (plot_df['low'] / ref - 1) * 100
                    plot_df['close'] = (plot_df['close'] / ref - 1) * 100
                    ylabel = '% Change'
                else:
                    ylabel = 'Price ($)'
                
                # Build list of additional plots (indicators)
                add_plots = []
                
                # Add moving averages
                if indicators.get('show_sma'):
                    if 'sma_20' in plot_df.columns:
                        sma20 = plot_df['sma_20'].copy()
                        if display_mode == 'percentage' and reference_price:
                            sma20 = (sma20 / reference_price - 1) * 100
                        add_plots.append(self.mpf.make_addplot(sma20, color='#FFA726', width=2))
                    
                    if 'sma_50' in plot_df.columns:
                        sma50 = plot_df['sma_50'].copy()
                        if display_mode == 'percentage' and reference_price:
                            sma50 = (sma50 / reference_price - 1) * 100
                        add_plots.append(self.mpf.make_addplot(sma50, color='#FF7043', width=2))
                
                if indicators.get('show_ema'):
                    if 'ema_20' in plot_df.columns:
                        ema20 = plot_df['ema_20'].copy()
                        if display_mode == 'percentage' and reference_price:
                            ema20 = (ema20 / reference_price - 1) * 100
                        add_plots.append(self.mpf.make_addplot(ema20, color='#42A5F5', width=2))
                    
                    if 'ema_50' in plot_df.columns:
                        ema50 = plot_df['ema_50'].copy()
                        if display_mode == 'percentage' and reference_price:
                            ema50 = (ema50 / reference_price - 1) * 100
                        add_plots.append(self.mpf.make_addplot(ema50, color='#1E88E5', width=2))
                    
                    if 'ema_100' in plot_df.columns:
                        ema100 = plot_df['ema_100'].copy()
                        if display_mode == 'percentage' and reference_price:
                            ema100 = (ema100 / reference_price - 1) * 100
                        add_plots.append(self.mpf.make_addplot(ema100, color='#1565C0', width=1))
                    
                    if 'ema_200' in plot_df.columns:
                        ema200 = plot_df['ema_200'].copy()
                        if display_mode == 'percentage' and reference_price:
                            ema200 = (ema200 / reference_price - 1) * 100
                        add_plots.append(self.mpf.make_addplot(ema200, color='#0D47A1', width=1))
                
                # Add Bollinger Bands
                if indicators.get('show_bb'):
                    if 'bb_upper' in plot_df.columns and 'bb_lower' in plot_df.columns:
                        bb_upper = plot_df['bb_upper'].copy()
                        bb_lower = plot_df['bb_lower'].copy()
                        
                        if display_mode == 'percentage' and reference_price:
                            bb_upper = (bb_upper / reference_price - 1) * 100
                            bb_lower = (bb_lower / reference_price - 1) * 100
                        
                        add_plots.append(self.mpf.make_addplot(bb_upper, color='#78909C', linestyle='--', width=1))
                        add_plots.append(self.mpf.make_addplot(bb_lower, color='#78909C', linestyle='--', width=1))
                
                # Add VWAP
                if indicators.get('show_vwap'):
                    if 'vwap' in plot_df.columns:
                        vwap = plot_df['vwap'].copy()
                        if display_mode == 'percentage' and reference_price:
                            vwap = (vwap / reference_price - 1) * 100
                        add_plots.append(self.mpf.make_addplot(vwap, color='#FFEE58', width=2))
                
                # Create custom style
                mc = self.mpf.make_marketcolors(up='#26a69a', down='#ef5350',
                                               edge='inherit',
                                               wick={'up':'#26a69a','down':'#ef5350'},
                                               volume={'up':'#26a69a80','down':'#ef535080'})
                
                s = self.mpf.make_mpf_style(marketcolors=mc, gridstyle=':', 
                                           facecolor='#1e1e1e', figcolor='#1e1e1e',
                                           edgecolor='#4e4e4e', gridcolor='#4e4e4e')
                
                # Determine panel ratios dynamically based on actual panel count
                # Start with main + volume
                panel_ratios = [3, 1]
                if indicators.get('show_rsi'):
                    panel_ratios.append(1)
                if indicators.get('show_macd'):
                    panel_ratios.append(1)
                
                # Plot using mplfinance
                kwargs = {
                    'type': 'candle',
                    'style': s,
                    'volume': True,
                    'figsize': (12, 8),
                    'title': f'{ticker} - {timeframe}',
                    'ylabel': ylabel,
                    'returnfig': True,
                    'warn_too_much_data': 10000
                }
                
                if add_plots:
                    kwargs['addplot'] = add_plots
                
                # Create the plot with error handling for panel mismatch
                try:
                    # First attempt with calculated panel_ratios
                    kwargs['panel_ratios'] = tuple(panel_ratios)
                    self.figure, axes = self.mpf.plot(plot_df, **kwargs)
                except ValueError as e:
                    if 'num_panels' in str(e) or 'panel_ratios' in str(e):
                        # Panel count mismatch - let mplfinance auto-calculate
                        logging.warning(f"Panel ratios mismatch: {e}. Using auto-sizing.")
                        kwargs.pop('panel_ratios', None)
                        self.figure, axes = self.mpf.plot(plot_df, **kwargs)
                    else:
                        raise
                
                # Replace canvas with new one containing the updated figure
                if self.canvas:
                    self.container_layout.removeWidget(self.canvas)
                    self.canvas.deleteLater()
                
                self.canvas = self.FigureCanvas(self.figure)
                self.container_layout.addWidget(self.canvas)
                
                # Store axes for later use
                self.axes = {'main': axes[0], 'volume': axes[1] if len(axes) > 1 else None}
                panel_idx = 2
                
                # Add RSI panel
                if indicators.get('show_rsi') and 'rsi_14' in plot_df.columns:
                    ax_rsi = axes[panel_idx] if panel_idx < len(axes) else None
                    if ax_rsi:
                        ax_rsi.plot(plot_df.index, plot_df['rsi_14'], color='#ab47bc', linewidth=2, label='RSI 14')
                        ax_rsi.axhline(70, color='#ef5350', linestyle='--', linewidth=1)
                        ax_rsi.axhline(30, color='#66bb6a', linestyle='--', linewidth=1)
                        ax_rsi.set_ylim(0, 100)
                        ax_rsi.set_ylabel('RSI')
                        ax_rsi.legend(loc='upper left')
                        self.axes['rsi'] = ax_rsi
                        panel_idx += 1
                
                # Add MACD panel
                if indicators.get('show_macd') and 'macd' in plot_df.columns:
                    ax_macd = axes[panel_idx] if panel_idx < len(axes) else None
                    if ax_macd:
                        ax_macd.plot(plot_df.index, plot_df['macd'], color='#2196F3', linewidth=2, label='MACD')
                        if 'macd_signal' in plot_df.columns:
                            ax_macd.plot(plot_df.index, plot_df['macd_signal'], color='#FF9800', linewidth=2, label='Signal')
                        
                        if 'macd_hist' in plot_df.columns:
                            colors = ['#4caf50' if h >= 0 else '#f44336' for h in plot_df['macd_hist']]
                            ax_macd.bar(plot_df.index, plot_df['macd_hist'], color=colors, width=0.8, alpha=0.3)
                        
                        ax_macd.set_ylabel('MACD')
                        ax_macd.legend(loc='upper left')
                        ax_macd.axhline(0, color='#ffffff', linestyle=':', linewidth=1)
                        self.axes['macd'] = ax_macd
                
                # Add pattern overlays
                if patterns and patterns.get('patterns') and self.axes.get('main'):
                    pattern_list = patterns['patterns'][:20]  # Limit to 20
                    for pattern in pattern_list:
                        self.add_pattern_overlay(pattern, plot_df, self.axes['main'])
                
                # Refresh canvas
                self.canvas.draw()
                
            except Exception as e:
                logging.error(f"FinPlot update error: {e}", exc_info=True)
        
        def add_pattern_overlay(self, pattern, df, ax):
            """Add pattern visualization as overlay"""
            try:
                start_idx = pattern.get('start_index', 0)
                end_idx = pattern.get('end_index', len(df)-1)
                start_idx = max(0, min(start_idx, len(df)-1))
                end_idx = max(0, min(end_idx, len(df)-1))
                
                if start_idx >= end_idx:
                    return
                
                pattern_type = pattern.get('type', 'unknown')
                subtype = pattern.get('subtype', '')
                confidence = pattern.get('confidence', 0.5)
                
                # Determine color
                if 'bullish' in subtype.lower() or pattern_type in ['ascending_triangle', 'inverse_head_shoulders']:
                    color = '#4caf50'
                    alpha = 0.2
                elif 'bearish' in subtype.lower() or pattern_type in ['descending_triangle', 'head_shoulders']:
                    color = '#f44336'
                    alpha = 0.2
                else:
                    color = '#ffeb3b'
                    alpha = 0.2
                
                # Get price range for pattern
                pattern_df = df.iloc[start_idx:end_idx+1]
                y_min = pattern_df['low'].min() * 0.995
                y_max = pattern_df['high'].max() * 1.005
                
                # Draw shaded rectangle
                from matplotlib.patches import Rectangle
                from matplotlib.dates import date2num
                
                x_start = date2num(df.index[start_idx])
                x_end = date2num(df.index[end_idx])
                
                rect = Rectangle((x_start, y_min), x_end - x_start, y_max - y_min,
                               facecolor=color, alpha=alpha, edgecolor=color, linewidth=1)
                ax.add_patch(rect)
                
                # Add text label
                label_text = f"{pattern_type.replace('_', ' ').title()}\n{confidence:.0%}"
                mid_x = date2num(df.index[int((start_idx + end_idx) / 2)])
                
                ax.text(mid_x, y_max, label_text, 
                       ha='center', va='bottom', color='white',
                       fontsize=8, bbox=dict(boxstyle='round,pad=0.3', facecolor=color, alpha=0.7))
                
            except Exception as e:
                logging.error(f"Pattern overlay error: {e}")
        
        def clear(self):
            """Clear all chart data"""
            try:
                if self.figure:
                    self.figure.clear()
                    self.axes = {}
            except Exception as e:
                logging.debug(f"Clear error: {e}")
        
        def set_mode(self, mode):
            """Set display mode - re-render with new mode"""
            if self.last_df is not None:
                ref_price = self.last_df['close'].iloc[0] if mode == 'percentage' else None
                self.update_data(
                    self.last_df,
                    self.last_indicators,
                    self.last_patterns,
                    display_mode=mode,
                    reference_price=ref_price
                )


class MarketDataWidget(QWidget):
    """Market data view backed by lightweight-charts."""

    CHART_TIMEFRAMES = ("1m", "5m", "15m", "1h", "1d")
    INDICATOR_GROUPS = {
        "sma": [
            ("sma_20", "SMA 20", "#FFA726"),
            ("sma_50", "SMA 50", "#AB47BC"),
        ],
        "ema": [
            ("ema_20", "EMA 20", "#42A5F5"),
            ("ema_50", "EMA 50", "#66BB6A"),
        ],
        "bb": [
            ("bb_upper", "BB Upper", "#78909C"),
            ("bb_middle", "BB Middle", "#90A4AE"),
            ("bb_lower", "BB Lower", "#78909C"),
        ],
        "vwap": [
            ("vwap", "VWAP", "#FDD835"),
        ],
    }

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)

        if not POLYGON_DEPS_AVAILABLE:
            raise RuntimeError(
                "Polygon market data dependencies are not installed. "
                "Run install_market_data_deps script first."
            )

        self.ticker = "AMD"
        self.api_key = ""
        self.current_timeframe = "1m"
        self.display_mode = "price"
        self.reference_price: Optional[float] = None

        self.chart: Optional[Chart] = None
        self.chart_shown = False
        self.chart_data = None
        self.indicator_lines: Dict[str, Any] = {}
        self.indicator_state = {
            "sma": True,
            "ema": True,
            "bb": True,
            "vwap": True,
        }
        self.indicator_arrays: Dict[str, Any] = {}
        self.chart_patterns: Dict[str, Any] = {}

        self.redis_subscriber: Optional[RedisSubscriberThread] = None
        self.worker: Optional[PolygonDataWorker] = None
        self.data_loader: Optional[ChartDataInitialLoader] = None
        self.cancel_load = False

        self.candles: deque = deque(maxlen=1000)
        self.news_articles: List[dict] = []
        self.reference_data: Dict[str, Any] = {}
        self.dividends: List[dict] = []
        self.splits: List[dict] = []
        self._last_chart_update = datetime.now()

        self._init_ui()

    # ------------------------------------------------------------------
    # UI SETUP
    # ------------------------------------------------------------------
    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)

        control_group = QGroupBox("Market Data Controls")
        control_layout = QHBoxLayout()

        control_layout.addWidget(QLabel("Ticker:"))
        self.ticker_input = QLineEdit(self.ticker)
        self.ticker_input.setMaximumWidth(120)
        control_layout.addWidget(self.ticker_input)

        self.redis_btn = QPushButton("⚡ Redis Live")
        self.redis_btn.setStyleSheet("background-color: #4CAF50; font-weight: bold;")
        self.redis_btn.clicked.connect(self._start_redis_subscriber)
        control_layout.addWidget(self.redis_btn)

        self.websocket_btn = QPushButton("📡 Polygon WebSocket")
        self.websocket_btn.clicked.connect(self._start_worker)
        control_layout.addWidget(self.websocket_btn)

        self.disconnect_btn = QPushButton("⏹ Disconnect")
        self.disconnect_btn.setEnabled(False)
        self.disconnect_btn.clicked.connect(self._stop_all)
        control_layout.addWidget(self.disconnect_btn)

        self.resume_live_btn = QPushButton("▶ Resume Live")
        self.resume_live_btn.setEnabled(False)
        self.resume_live_btn.clicked.connect(self._start_redis_live)
        control_layout.addWidget(self.resume_live_btn)

        self.open_chart_btn = QPushButton("🪟 Open Chart Window")
        self.open_chart_btn.clicked.connect(self._show_chart_window)
        control_layout.addWidget(self.open_chart_btn)

        self.status_label = QLabel("⚫ Not Connected")
        control_layout.addWidget(self.status_label)
        control_layout.addStretch()

        control_group.setLayout(control_layout)
        layout.addWidget(control_group)

        indicator_group = QGroupBox("Indicators")
        indicator_layout = QHBoxLayout()
        self.indicator_checks: Dict[str, QCheckBox] = {}
        for key, label in (
            ("sma", "SMA"),
            ("ema", "EMA"),
            ("bb", "Bollinger"),
            ("vwap", "VWAP"),
        ):
            cb = QCheckBox(label)
            cb.setChecked(self.indicator_state[key])
            cb.stateChanged.connect(lambda state, k=key: self._on_indicator_toggle(k, state))
            indicator_layout.addWidget(cb)
            self.indicator_checks[key] = cb
        indicator_layout.addStretch()
        indicator_group.setLayout(indicator_layout)
        layout.addWidget(indicator_group)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        chart_panel = QWidget()
        chart_layout = QVBoxLayout(chart_panel)
        chart_layout.setContentsMargins(0, 0, 0, 0)
        self.chart_status = QLabel(
            "Chart opens in a dedicated window powered by lightweight-charts."
        )
        self.chart_status.setWordWrap(True)
        self.chart_status.setStyleSheet("color: #bbb;")
        chart_layout.addWidget(self.chart_status)

        self.last_update_label = QLabel("Last update: –")
        self.last_update_label.setStyleSheet("color: #888;")
        chart_layout.addWidget(self.last_update_label)
        chart_layout.addStretch()

        splitter.addWidget(chart_panel)

        sidebar = QWidget()
        sidebar_layout = QVBoxLayout(sidebar)

        news_group = QGroupBox("📰 Latest News")
        news_layout = QVBoxLayout()
        self.news_text = QTextEdit()
        self.news_text.setReadOnly(True)
        self.news_text.setStyleSheet("background-color: #1e1e1e; color: #d4d4d4;")
        self.news_text.setMaximumHeight(200)
        news_layout.addWidget(self.news_text)
        news_group.setLayout(news_layout)
        sidebar_layout.addWidget(news_group)

        ref_group = QGroupBox("📋 Reference Data")
        ref_layout = QVBoxLayout()
        self.ref_table = QTableWidget()
        self.ref_table.setColumnCount(2)
        self.ref_table.setHorizontalHeaderLabels(["Field", "Value"])
        self.ref_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.ref_table.setMaximumHeight(160)
        ref_layout.addWidget(self.ref_table)
        ref_group.setLayout(ref_layout)
        sidebar_layout.addWidget(ref_group)

        corp_group = QGroupBox("💼 Corporate Actions")
        corp_layout = QVBoxLayout()
        self.corp_table = QTableWidget()
        self.corp_table.setColumnCount(4)
        self.corp_table.setHorizontalHeaderLabels(["Date", "Type", "Amount", "Details"])
        self.corp_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.corp_table.setMaximumHeight(160)
        corp_layout.addWidget(self.corp_table)
        corp_group.setLayout(corp_layout)
        sidebar_layout.addWidget(corp_group)

        sidebar_layout.addStretch()
        splitter.addWidget(sidebar)
        splitter.setSizes([720, 320])

        layout.addWidget(splitter)

    # ------------------------------------------------------------------
    # CHART MANAGEMENT
    # ------------------------------------------------------------------
    def _ensure_chart(self) -> Optional[Chart]:
        if not LIGHTWEIGHT_CHARTS_AVAILABLE:
            self.chart_status.setText(
                "Install lightweight-charts to enable the interactive chart "
                "(pip install lightweight-charts)."
            )
            return None

        if self.chart is None:
            self.chart = Chart(title="ChronoX Market Data", toolbox=True)
            self.chart.legend(True)
            self.chart.events.search += self._on_chart_search
            self.chart.topbar.textbox('symbol', self.ticker)
            self.chart.topbar.switcher(
                'timeframe',
                self.CHART_TIMEFRAMES,
                default=self.current_timeframe,
                func=self._on_chart_timeframe_change,
            )
            self.chart.topbar.button('resume', 'Resume Live', func=self._on_chart_resume)
        return self.chart

    def _show_chart_window(self) -> None:
        chart = self._ensure_chart()
        if chart is None:
            return
        if not self.chart_shown:
            chart.show()
            self.chart_shown = True
        if self.chart_data is not None:
            self._set_chart_data(self.chart_data)

    def _prepare_chart_frame(self, df):
        import pandas as pd

        formatted = df.copy()
        if 'time' in formatted.columns:
            formatted['time'] = pd.to_datetime(formatted['time'], utc=True)
            formatted['time'] = formatted['time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        return formatted

    def _set_chart_data(self, df) -> None:
        if df is None or df.empty:
            return

        chart = self._ensure_chart()
        if chart is None:
            return

        formatted = self._prepare_chart_frame(df)
        required_cols = ["time", "open", "high", "low", "close"]
        if not all(col in formatted.columns for col in required_cols):
            logging.error("Missing required OHLC columns for chart rendering")
            return

        chart.set(formatted[["time", "open", "high", "low", "close", "volume"]] if "volume" in formatted.columns else formatted[required_cols])
        chart.watermark(f"{self.ticker} {self.current_timeframe}")
        self.chart_status.setText(
            f"Loaded {len(formatted)} bars for {self.ticker} ({self.current_timeframe})"
        )
        self.last_update_label.setText(
            f"Last update: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )
        self._refresh_indicators(formatted)

    def _update_chart_bar(self, bar: Dict[str, Any]) -> None:
        chart = self._ensure_chart()
        if chart is None:
            return

        import pandas as pd

        update_bar = dict(bar)
        if 'time' in update_bar:
            update_bar['time'] = pd.to_datetime(update_bar['time'], utc=True).strftime('%Y-%m-%dT%H:%M:%S')

        series = pd.Series(update_bar)
        chart.update(series)

        for column, line in list(self.indicator_lines.items()):
            value = update_bar.get(column)
            if value is None:
                continue
            line.update({'time': update_bar['time'], line.name: value})

    def _refresh_indicators(self, formatted_df) -> None:
        chart = self._ensure_chart()
        if chart is None:
            return

        import pandas as pd

        for column, line in list(self.indicator_lines.items()):
            if column not in formatted_df.columns or all(formatted_df[column].isna()):
                line.delete()
                self.indicator_lines.pop(column, None)

        for key, columns in self.INDICATOR_GROUPS.items():
            if not self.indicator_state.get(key, False):
                for column, _, _ in columns:
                    line = self.indicator_lines.pop(column, None)
                    if line:
                        line.delete()
                continue

            for column, label, color in columns:
                if column not in formatted_df.columns:
                    continue
                series_df = formatted_df[["time", column]].dropna()
                if series_df.empty:
                    continue
                line = self.indicator_lines.get(column)
                if line is None:
                    line = chart.create_line(label, color=color)
                    self.indicator_lines[column] = line
                line.set(series_df.rename(columns={column: label}))

    def _on_indicator_toggle(self, key: str, state: int) -> None:
        self.indicator_state[key] = state == Qt.CheckState.Checked.value
        if self.chart_data is not None:
            formatted = self._prepare_chart_frame(self.chart_data)
            self._refresh_indicators(formatted)

    # ------------------------------------------------------------------
    # DATA LOADING / STREAMING
    # ------------------------------------------------------------------
    def _start_worker(self) -> None:
        from dotenv import load_dotenv

        load_dotenv()
        self.api_key = os.getenv('POLYGON_API_KEY', '')
        if not self.api_key:
            self.status_label.setText("❌ No API key in .env")
            return

        self.ticker = self.ticker_input.text().strip().upper()
        if not self.ticker:
            self.status_label.setText("❌ Enter ticker")
            return

        self._stop_worker()

        self.worker = PolygonDataWorker(self.ticker, self.api_key)
        self.worker.candle_signal.connect(self._on_candle)
        self.worker.indicator_signal.connect(self._on_indicators)
        self.worker.news_signal.connect(self._on_news)
        self.worker.reference_signal.connect(self._on_reference)
        self.worker.corporate_signal.connect(self._on_corporate)
        self.worker.status_signal.connect(self._on_status)
        self.worker.error_signal.connect(self._on_error)
        self.worker.start()

        self.candles.clear()
        self.indicator_arrays.clear()
        self.news_articles.clear()
        self.reference_data.clear()
        self.dividends.clear()
        self.splits.clear()

    def _stop_worker(self) -> None:
        if self.worker:
            self.worker.stop()
            self.worker.wait()
            self.worker = None

    def _start_redis_subscriber(self) -> None:
        if not REDIS_AVAILABLE:
            self.status_label.setText("❌ Redis library missing")
            return

        self.ticker = self.ticker_input.text().strip().upper()
        if not self.ticker:
            self.status_label.setText("❌ Enter ticker")
            return

        redis_password = os.getenv('REDIS_PASSWORD', None)
        redis_port = int(os.getenv('REDIS_PORT', '6380'))

        try:
            test_client = redis.Redis(
                host='localhost',
                port=redis_port,
                password=redis_password,
                socket_connect_timeout=2,
            )
            test_client.ping()
            test_client.close()
        except redis.AuthenticationError:
            self.status_label.setText("⚠️ Redis auth failed")
            self._reload_chart_data()
            return
        except (redis.ConnectionError, redis.TimeoutError) as exc:
            logging.warning(f"Redis unavailable: {exc}")
            self.status_label.setText("⚠️ Redis offline - DB fallback")
            self._reload_chart_data()
            return

        if self.redis_subscriber and self.redis_subscriber.isRunning():
            self.redis_subscriber.stop()

        self.redis_subscriber = RedisSubscriberThread(
            symbols=[self.ticker],
            timeframes=[self.current_timeframe],
            redis_host='localhost',
            redis_port=redis_port,
            redis_password=redis_password,
        )
        self.redis_subscriber.bar_received.connect(self._on_redis_bar)
        self.redis_subscriber.error_occurred.connect(self._on_redis_error)
        self.redis_subscriber.connected.connect(self._on_redis_connected)
        self.redis_subscriber.disconnected.connect(self._on_redis_disconnected)
        self.redis_subscriber.start()

        self.redis_btn.setEnabled(False)
        self.websocket_btn.setEnabled(False)
        self.disconnect_btn.setEnabled(True)
        self.status_label.setText("🔄 Connecting to Redis...")

    def _start_redis_live(self) -> None:
        self.resume_live_btn.setEnabled(False)
        self._reload_chart_data(trigger_redis_after=True)

    def _stop_redis_subscriber(self) -> None:
        if self.redis_subscriber and self.redis_subscriber.isRunning():
            self.redis_subscriber.stop()
            self.redis_subscriber = None
            self.status_label.setText("⚫ Redis disconnected")
        self.redis_btn.setEnabled(True)
        self.websocket_btn.setEnabled(True)
        self.disconnect_btn.setEnabled(False)

    def _stop_all(self) -> None:
        self._stop_worker()
        self._stop_redis_subscriber()

    def _reload_chart_data(self, trigger_redis_after: bool = False) -> None:
        if self.data_loader and self.data_loader.isRunning():
            self.cancel_load = True
            self.data_loader.quit()

        self.cancel_load = False
        self.data_loader = ChartDataInitialLoader(
            ticker=self.ticker,
            timeframe=self.current_timeframe,
            limit=10000,
        )
        if trigger_redis_after:
            self.data_loader.data_loaded.connect(self._on_data_loaded_then_redis)
        else:
            self.data_loader.data_loaded.connect(self._on_data_loaded)
        self.data_loader.error_occurred.connect(self._on_data_error)
        self.data_loader.start()
        self.chart_status.setText("Loading historical data from TimescaleDB...")

    # ------------------------------------------------------------------
    # REDIS CALLBACKS
    # ------------------------------------------------------------------
    def _on_redis_bar(self, bar_data: dict) -> None:
        try:
            if bar_data.get('symbol') and bar_data['symbol'] != self.ticker:
                return

            import pandas as pd

            new_row = pd.DataFrame([bar_data])
            if 'time' in new_row.columns:
                new_row['time'] = pd.to_datetime(new_row['time'])

            if self.chart_data is None or getattr(self.chart_data, 'empty', True):
                self.chart_data = new_row
                self._set_chart_data(self.chart_data)
            else:
                last_time = self.chart_data['time'].iloc[-1]
                new_time = new_row['time'].iloc[0]
                if new_time <= last_time:
                    self.chart_data.iloc[-1] = new_row.iloc[0]
                else:
                    self.chart_data = pd.concat([self.chart_data, new_row], ignore_index=True)
                self._update_chart_bar(new_row.iloc[0].to_dict())

            self.last_update_label.setText(
                f"Last update: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
            )
        except Exception as exc:
            logging.error(f"Redis bar update error: {exc}", exc_info=True)

    def _on_redis_error(self, message: str) -> None:
        logging.error(f"Redis error: {message}")
        self.status_label.setText(f"❌ Redis error: {message}")

    def _on_redis_connected(self) -> None:
        logging.info("Redis subscriber connected")
        self.status_label.setText("✅ Redis connected - streaming live data")
        self._reload_chart_data()

    def _on_redis_disconnected(self) -> None:
        logging.warning("Redis subscriber disconnected")
        self.status_label.setText("⚫ Redis disconnected")
        self.redis_btn.setEnabled(True)
        self.websocket_btn.setEnabled(True)
        self.disconnect_btn.setEnabled(False)

    # ------------------------------------------------------------------
    # DATA LOADER CALLBACKS
    # ------------------------------------------------------------------
    def _on_data_loaded(self, df) -> None:
        if self.cancel_load:
            return

        self.chart_data = df
        self.indicator_arrays = {}
        if not df.empty:
            for column in df.columns:
                if column.startswith(('sma_', 'ema_', 'bb_', 'vwap', 'rsi', 'macd')):
                    self.indicator_arrays[column] = df[column].values

        self._set_chart_data(df)
        self.resume_live_btn.setEnabled(True)

    def _on_data_loaded_then_redis(self, df) -> None:
        self._on_data_loaded(df)
        self._start_redis_subscriber()

    def _on_data_error(self, message: str) -> None:
        logging.error(f"Data load error: {message}")
        self.chart_status.setText(message)

    # ------------------------------------------------------------------
    # WORKER CALLBACKS (LEGACY POLYGON WS)
    # ------------------------------------------------------------------
    def _on_candle(self, ticker: str, candle: dict) -> None:
        self.candles.append(candle)
        if (datetime.now() - self._last_chart_update).total_seconds() > 2:
            if self.chart_data is not None and not getattr(self.chart_data, 'empty', True):
                self._set_chart_data(self.chart_data)
            self._last_chart_update = datetime.now()

    def _on_indicators(self, ticker: str, indicators: dict) -> None:
        self.indicator_arrays.update(indicators)
        if self.chart_data is not None and not getattr(self.chart_data, 'empty', True):
            formatted = self._prepare_chart_frame(self.chart_data)
            self._refresh_indicators(formatted)

    def _on_news(self, articles: list) -> None:
        self.news_articles = articles
        self._update_news()

    def _on_reference(self, data: dict) -> None:
        self.reference_data = data
        self._update_reference_table()

    def _on_corporate(self, data: dict) -> None:
        self.dividends = data.get('dividends', [])
        self.splits = data.get('splits', [])
        self._update_corporate_table()

    def _on_status(self, status: str) -> None:
        self.status_label.setText(status)

    def _on_error(self, error: str) -> None:
        logging.error(f"Market data worker error: {error}")

    # ------------------------------------------------------------------
    # NEWS / REFERENCE PANELS
    # ------------------------------------------------------------------
    def _update_news(self) -> None:
        html = [
            "<html><body style='background-color:#1e1e1e; color:#d4d4d4; font-family:Arial;'>"
        ]
        for article in self.news_articles[:10]:
            title = article.get('title', 'No title')
            published = article.get('published_utc', '')
            url = article.get('article_url', '#')
            html.append(
                f"<div style='margin-bottom:12px;border-bottom:1px solid #444;padding-bottom:8px;'>"
                f"<a href='{url}' style='color:#42A5F5;text-decoration:none;font-weight:bold;'>{title}</a>"
                f"<br><span style='color:#888;font-size:11px;'>{published}</span>"
                "</div>"
            )
        html.append("</body></html>")
        self.news_text.setHtml("".join(html))

    def _update_reference_table(self) -> None:
        self.ref_table.setRowCount(0)
        if not self.reference_data:
            return
        fields = [
            ('name', 'Company Name'),
            ('market', 'Exchange'),
            ('type', 'Type'),
            ('market_cap', 'Market Cap'),
            ('currency_name', 'Currency'),
            ('primary_exchange', 'Primary Exchange'),
        ]
        self.ref_table.setRowCount(len(fields))
        for row, (key, label) in enumerate(fields):
            value = self.reference_data.get(key, 'N/A')
            if key == 'market_cap' and isinstance(value, (int, float)):
                value = f"${value/1e9:.2f}B"
            self.ref_table.setItem(row, 0, QTableWidgetItem(label))
            self.ref_table.setItem(row, 1, QTableWidgetItem(str(value)))

    def _update_corporate_table(self) -> None:
        self.corp_table.setRowCount(0)
        actions = []
        for div in self.dividends[:20]:
            actions.append({
                'date': div.get('ex_dividend_date', 'N/A'),
                'type': 'Dividend',
                'amount': f"${div.get('cash_amount', 0):.4f}",
                'details': f"Pay: {div.get('pay_date', 'N/A')}",
            })
        for split in self.splits[:20]:
            actions.append({
                'date': split.get('execution_date', 'N/A'),
                'type': 'Split',
                'amount': f"{split.get('split_from', 1)}:{split.get('split_to', 1)}",
                'details': '',
            })
        actions.sort(key=lambda item: item['date'], reverse=True)
        self.corp_table.setRowCount(len(actions))
        for row, action in enumerate(actions):
            self.corp_table.setItem(row, 0, QTableWidgetItem(action['date'][:10]))
            type_item = QTableWidgetItem(action['type'])
            type_item.setForeground(QColor('#4CAF50' if action['type'] == 'Dividend' else '#FF9800'))
            self.corp_table.setItem(row, 1, type_item)
            self.corp_table.setItem(row, 2, QTableWidgetItem(action['amount']))
            self.corp_table.setItem(row, 3, QTableWidgetItem(action['details']))

    # ------------------------------------------------------------------
    # CHART EVENT CALLBACKS
    # ------------------------------------------------------------------
    def _on_chart_search(self, chart_obj, searched_string: str) -> None:
        ticker = searched_string.strip().upper()
        if not ticker or ticker == self.ticker:
            return
        self.ticker = ticker
        self.ticker_input.setText(ticker)
        self._reload_chart_data(trigger_redis_after=True)

    def _on_chart_timeframe_change(self, chart_obj) -> None:
        timeframe = chart_obj.topbar['timeframe'].value
        if timeframe == self.current_timeframe:
            return
        self.current_timeframe = timeframe
        self._reload_chart_data(trigger_redis_after=True)

    def _on_chart_resume(self, chart_obj) -> None:
        self._start_redis_live()


class BackfillVisualizer(QMainWindow):
    """Main GUI window for backfill visualization"""
    
    def __init__(self):
        super().__init__()
        self.worker: Optional[BackfillWorker] = None
        self.task_states: Dict[str, Dict] = {}
        self.total_files = 0
        
        # Real-time metrics tracking for charts
        self.current_stats = {
            'downloaded': 0,
            'processed': 0,
            'skipped': 0,
            'failed': 0,
            'download_mbps': 0.0,
            'download_latency_ms': 0.0,
            'process_latency_ms': 0.0
        }
        
        # Phase tracking
        self.phase1_data: Dict[str, Dict] = {}  # ticker -> {status, name, exchange, type, market_cap}
        self.phase2_data: Dict[str, Dict] = {}  # ticker -> {dividends: count, splits: count, div_details: [], split_details: []}
        self.phase3_data: Dict[str, Dict] = {}  # ticker -> {bars, status, progress, date_range}
        self.phase4_data: Dict[str, Dict] = {}  # date -> {bars, tickers, status}  (Phase 4 flatfiles)
        self.current_ticker_p2: str = ""  # Track current ticker for Phase 2 details
        self.debug_mode: bool = False  # Track if debug mode is enabled
        
        self.setWindowTitle("ChronoX Backfill Visualizer & Debugger")
        self.setGeometry(100, 100, 1400, 900)
        
        self._init_ui()
    
    def _init_ui(self):
        """Initialize the user interface"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        
        # ============================================================
        # CONTROL PANEL
        # ============================================================
        control_group = QGroupBox("Control Panel")
        control_layout = QHBoxLayout()
        
        # Stock ticker
        control_layout.addWidget(QLabel("Ticker:"))
        self.ticker_input = QLineEdit()
        self.ticker_input.setText("AMD")
        self.ticker_input.setMaximumWidth(100)
        control_layout.addWidget(self.ticker_input)
        
        # Start date
        control_layout.addWidget(QLabel("Start Date:"))
        self.start_date = QDateEdit()
        self.start_date.setDate(QDate.currentDate().addYears(-5))
        self.start_date.setCalendarPopup(True)
        self.start_date.setDisplayFormat("yyyy-MM-dd")
        control_layout.addWidget(self.start_date)
        
        # End date
        control_layout.addWidget(QLabel("End Date:"))
        self.end_date = QDateEdit()
        self.end_date.setDate(QDate.currentDate())
        self.end_date.setCalendarPopup(True)
        self.end_date.setDisplayFormat("yyyy-MM-dd")
        control_layout.addWidget(self.end_date)
        
        # Go button
        self.go_button = QPushButton("▶ Start Backfill")
        self.go_button.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold; padding: 8px;")
        self.go_button.clicked.connect(self._start_backfill)
        control_layout.addWidget(self.go_button)
        
        # Stop button
        self.stop_button = QPushButton("⬛ Stop")
        self.stop_button.setStyleSheet("background-color: #f44336; color: white; font-weight: bold; padding: 8px;")
        self.stop_button.clicked.connect(self._stop_backfill)
        self.stop_button.setEnabled(False)
        control_layout.addWidget(self.stop_button)
        
        # Backfill checkbox (enable hybrid backfill mode)
        self.backfill_checkbox = QCheckBox("Run Backfill")
        self.backfill_checkbox.setChecked(True)
        self.backfill_checkbox.setToolTip("Enable hybrid backfill: flat files first, then REST API to fill gaps > 3 days")
        control_layout.addWidget(self.backfill_checkbox)
        
        # Purge Flatfiles button
        self.purge_button = QPushButton("🗑️ Purge Flatfiles")
        self.purge_button.setStyleSheet("background-color: #D32F2F; color: white; font-weight: bold; padding: 8px;")
        self.purge_button.clicked.connect(self._purge_flatfiles)
        self.purge_button.setToolTip("Delete all downloaded flatpack files (.csv, .csv.gz) from data/flatfiles")
        control_layout.addWidget(self.purge_button)
        
        control_layout.addStretch()
        control_group.setLayout(control_layout)
        main_layout.addWidget(control_group)
        
        # ============================================================
        # STATISTICS (for internal tracking - displayed in status bar)
        # ============================================================
        # Create labels for tracking (not displayed in UI, used for status bar updates)
        self.downloaded_label = QLabel("0")
        self.skipped_label = QLabel("0")
        self.failed_label = QLabel("0")
        self.processed_label = QLabel("0")
        self.progress_bar = QProgressBar()  # Used for calculation only
        
        # ============================================================
        # MAIN TAB WIDGET
        # ============================================================
        self.tab_widget = QTabWidget()
        
        # Tab 1: Phase 4 Pipeline Monitor
        pipeline_tab = QWidget()
        pipeline_layout = QVBoxLayout(pipeline_tab)
        
        # ============================================================
        # REAL-TIME QUEUE DEPTH INDICATORS
        # ============================================================
        queue_metrics_group = QGroupBox("Real-Time Queue Metrics")
        queue_metrics_layout = QVBoxLayout()
        
        # Download Queue Depth (dynamically updated maxsize)
        dl_queue_layout = QHBoxLayout()
        dl_queue_layout.addWidget(QLabel("Download Queue:"))
        self.download_queue_bar = QProgressBar()
        self.download_queue_bar.setMinimum(0)
        self.download_queue_bar.setMaximum(100)  # Default, updated dynamically
        self.download_queue_bar.setValue(0)
        self.download_queue_bar.setTextVisible(True)
        self.download_queue_bar.setFormat("%v/100")  # Default, updated dynamically
        self.download_queue_bar.setStyleSheet("QProgressBar::chunk { background-color: #2196F3; }")
        dl_queue_layout.addWidget(self.download_queue_bar)
        queue_metrics_layout.addLayout(dl_queue_layout)
        
        # Process Queue Depth (dynamically updated maxsize)
        pr_queue_layout = QHBoxLayout()
        pr_queue_layout.addWidget(QLabel("Process Queue:"))
        self.process_queue_bar = QProgressBar()
        self.process_queue_bar.setMinimum(0)
        self.process_queue_bar.setMaximum(100)  # Default, updated dynamically
        self.process_queue_bar.setValue(0)
        self.process_queue_bar.setTextVisible(True)
        self.process_queue_bar.setFormat("%v/100")  # Default, updated dynamically
        self.process_queue_bar.setStyleSheet("QProgressBar::chunk { background-color: #FF9800; }")
        pr_queue_layout.addWidget(self.process_queue_bar)
        queue_metrics_layout.addLayout(pr_queue_layout)
        
        queue_metrics_group.setLayout(queue_metrics_layout)
        pipeline_layout.addWidget(queue_metrics_group)
        
        # ============================================================
        # LIVE QUEUE CHART (replaces tables)
        # ============================================================
        self.queue_chart = QueueChartWidget()
        pipeline_layout.addWidget(self.queue_chart)
        
        # ============================================================
        # TASK SUMMARY TABLES (Compact view, below chart)
        # ============================================================
        tables_splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Download Queue / Tasks Table (compact)
        download_group = QGroupBox("Download Queue Summary (Compact)")
        download_layout = QVBoxLayout()
        self.download_table = QTableWidget()
        self.download_table.setColumnCount(6)
        self.download_table.setHorizontalHeaderLabels([
            "Task ID", "Date", "State", "Attempts", "Error", "File"
        ])
        self.download_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.download_table.setAlternatingRowColors(True)
        self.download_table.setMaximumHeight(200)  # Compact view
        download_layout.addWidget(self.download_table)
        download_group.setLayout(download_layout)
        tables_splitter.addWidget(download_group)
        
        # Processing Queue / Stack Table (compact)
        process_group = QGroupBox("Processing Queue Summary (Compact)")
        process_layout = QVBoxLayout()
        self.process_table = QTableWidget()
        self.process_table.setColumnCount(5)
        self.process_table.setHorizontalHeaderLabels([
            "Task ID", "Date", "State", "Bars Written", "Status"
        ])
        self.process_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.process_table.setAlternatingRowColors(True)
        self.process_table.setMaximumHeight(200)  # Compact view
        process_layout.addWidget(self.process_table)
        process_group.setLayout(process_layout)
        tables_splitter.addWidget(process_group)
        
        pipeline_layout.addWidget(tables_splitter)
        self.tab_widget.addTab(pipeline_tab, "📊 Phase 4: Pipeline Monitor")
        
        # Tab 2: Reference Data
        ref_tab = QWidget()
        ref_layout = QVBoxLayout(ref_tab)
        self.ref_table = QTableWidget()
        self.ref_table.setColumnCount(6)
        self.ref_table.setHorizontalHeaderLabels(["Ticker", "Name", "Exchange", "Type", "Market Cap", "Status"])
        self.ref_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        ref_layout.addWidget(self.ref_table)
        self.tab_widget.addTab(ref_tab, "📋 Reference Data")
        
        # Tab 3: Corporate Actions
        corp_tab = QWidget()
        corp_layout = QVBoxLayout(corp_tab)
        self.corp_table = QTableWidget()
        self.corp_table.setColumnCount(6)
        self.corp_table.setHorizontalHeaderLabels(["Ticker", "Type", "Date", "Amount/Ratio", "Count", "Status"])
        self.corp_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        corp_layout.addWidget(self.corp_table)
        self.tab_widget.addTab(corp_tab, "💼 Corporate Actions")
        
        # Tab 4: Daily Bars
        daily_tab = QWidget()
        daily_layout = QVBoxLayout(daily_tab)
        self.daily_table = QTableWidget()
        self.daily_table.setColumnCount(5)
        self.daily_table.setHorizontalHeaderLabels(["Ticker", "Date Range", "Bars", "Status", "Progress"])
        self.daily_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        daily_layout.addWidget(self.daily_table)
        self.tab_widget.addTab(daily_tab, "📈 Daily Bars")
        
        # Tab 5: Market Data (Polygon.io integration)
        if POLYGON_DEPS_AVAILABLE:
            self.market_data_widget = MarketDataWidget()
            self.tab_widget.addTab(self.market_data_widget, "📈 Market Data")
        else:
            # Show placeholder if dependencies not installed
            placeholder_tab = QWidget()
            placeholder_layout = QVBoxLayout(placeholder_tab)
            placeholder_label = QLabel(
                "📈 Market Data Tab Disabled\n\n"
                "Install dependencies to enable live market data:\n\n"
                "python scripts/install_market_data_deps.py\n\n"
                "Features when enabled:\n"
                "• Real-time candlestick charts\n"
                "• Technical indicators (SMA, EMA, RSI, MACD, BB, VWAP)\n"
                "• Corporate actions (dividends, splits)\n"
                "• Live news feed\n"
                "• Reference data"
            )
            placeholder_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            placeholder_label.setStyleSheet("font-size: 14px; color: #888;")
            placeholder_layout.addWidget(placeholder_label)
            self.tab_widget.addTab(placeholder_tab, "📈 Market Data (Disabled)")
        
        main_layout.addWidget(self.tab_widget)
        
        # ============================================================
        # LOG OUTPUT (Bottom)
        # ============================================================
        log_group = QGroupBox("Live Log Output")
        log_layout = QVBoxLayout()
        
        # Log controls
        log_controls = QHBoxLayout()
        log_controls.addWidget(QLabel("Filter:"))
        self.log_filter = QComboBox()
        self.log_filter.addItems(["ALL", "INFO", "WARNING", "ERROR", "DEBUG"])
        self.log_filter.currentTextChanged.connect(self._apply_log_filter)
        log_controls.addWidget(self.log_filter)
        
        clear_log_btn = QPushButton("Clear Log")
        clear_log_btn.clicked.connect(lambda: self.log_output.clear())
        log_controls.addWidget(clear_log_btn)
        log_controls.addStretch()
        log_layout.addLayout(log_controls)
        
        # Log text area
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setMaximumHeight(200)
        self.log_output.setStyleSheet("background-color: #1e1e1e; color: #d4d4d4; font-family: 'Courier New';")
        log_layout.addWidget(self.log_output)
        
        log_group.setLayout(log_layout)
        main_layout.addWidget(log_group)
        
        # Status bar
        self.statusBar().showMessage("Ready")
    
    def _start_backfill(self):
        """Start the backfill process"""
        ticker = self.ticker_input.text().strip().upper()
        if not ticker:
            self.statusBar().showMessage("Error: Please enter a ticker symbol")
            return
        
        start = self.start_date.date().toString("yyyy-MM-dd")
        end = self.end_date.date().toString("yyyy-MM-dd")
        
        # Calculate years
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")
        years = max(1, (end_date - start_date).days // 365)
        
        # Clear previous data
        self.task_states.clear()
        self.phase1_data.clear()
        self.phase2_data.clear()
        self.phase3_data.clear()
        self.download_table.setRowCount(0)
        self.process_table.setRowCount(0)
        self.ref_table.setRowCount(0)
        self.corp_table.setRowCount(0)
        self.daily_table.setRowCount(0)
        self.log_output.clear()
        
        # Reset stats
        self.downloaded_label.setText("0")
        self.skipped_label.setText("0")
        self.failed_label.setText("0")
        self.processed_label.setText("0")
        self.progress_bar.setValue(0)
        
        # Always enable debug mode when running from GUI
        debug_enabled = True
        self.debug_mode = debug_enabled  # Track debug mode state
        
        # Check if backfill is enabled
        run_backfill = self.backfill_checkbox.isChecked()
        
        # Start worker thread
        self.worker = BackfillWorker(ticker, start, end, years, debug=debug_enabled, backfill=run_backfill)
        self.worker.log_signal.connect(self._append_log)
        self.worker.stats_signal.connect(self._update_stats)
        self.worker.task_update_signal.connect(self._update_task)
        self.worker.phase_signal.connect(self._update_phase)
        self.worker.finished_signal.connect(self._backfill_finished)
        self.worker.queue_signal.connect(self._update_queue_metrics)
        self.worker.start()
        
        # Update UI
        self.go_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self._update_status_bar()  # Show initial status
        
        backfill_status = "ENABLED (flat files + REST API gap filling)" if run_backfill else "DISABLED (flat files only)"
        self._append_log(f"=== Starting backfill for {ticker} ({start} to {end}) ===")
        self._append_log(f"🔍 Debug mode ENABLED - showing detailed queue/stack operations")
        self._append_log(f"🔄 Hybrid Backfill: {backfill_status}")
    
    def _stop_backfill(self):
        """Stop the running backfill"""
        if self.worker and self.worker.is_running:
            self.worker.stop()
            self.statusBar().showMessage("Stopping backfill...")
    
    def _purge_flatfiles(self):
        """Purge all downloaded flatpack files"""
        from PyQt6.QtWidgets import QMessageBox
        import glob
        from pathlib import Path
        
        # Scan for files first
        flatfiles_dir = Path(__file__).parent.parent / "data" / "flatfiles"
        
        if not flatfiles_dir.exists():
            QMessageBox.warning(
                self,
                'Directory Not Found',
                f'Flatfiles directory does not exist:\n{flatfiles_dir}'
            )
            return
        
        # Count files
        csv_files = list(flatfiles_dir.glob("**/*.csv"))
        gz_files = list(flatfiles_dir.glob("**/*.csv.gz"))
        parquet_files = list(flatfiles_dir.glob("**/*.parquet"))
        all_files = csv_files + gz_files + parquet_files
        
        if not all_files:
            QMessageBox.information(
                self,
                'No Files Found',
                f'No flatpack files found in:\n{flatfiles_dir}\n\nNothing to delete.'
            )
            return
        
        # Confirm action with actual file count
        total_size = sum(f.stat().st_size for f in all_files if f.exists())
        size_mb = total_size / 1024 / 1024
        size_str = f"{size_mb:.1f} MB" if size_mb < 1024 else f"{size_mb / 1024:.2f} GB"
        
        reply = QMessageBox.question(
            self,
            'Confirm Purge',
            f'Found {len(all_files)} files ({size_str}) in:\n{flatfiles_dir}\n\n'
            f'  • {len(csv_files)} .csv files\n'
            f'  • {len(gz_files)} .csv.gz files\n'
            f'  • {len(parquet_files)} .parquet files\n\n'
            'Delete all these files?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply != QMessageBox.StandardButton.Yes:
            return
        
        # Stop any running backfill first
        if self.worker and self.worker.is_running:
            self._stop_backfill()
            self._append_log("Stopping backfill before purge...")
        
        # Disable UI during purge
        self.purge_button.setEnabled(False)
        self.go_button.setEnabled(False)
        self.statusBar().showMessage("🗑️ Purging flatfiles...")
        self._append_log(f"=== Starting flatfile purge ({len(all_files)} files) ===")
        
        # Run purge in background thread
        self.purge_worker = PurgeWorker()
        self.purge_worker.progress_signal.connect(self._on_purge_progress)
        self.purge_worker.finished_signal.connect(self._on_purge_finished)
        self.purge_worker.start()
    
    def _on_purge_progress(self, message: str):
        """Handle purge progress messages"""
        self._append_log(message)
    
    def _on_purge_finished(self, files_deleted: int, bytes_freed: int):
        """Handle purge completion"""
        mb_freed = bytes_freed / 1024 / 1024
        gb_freed = mb_freed / 1024
        
        if gb_freed > 1:
            size_str = f"{gb_freed:.2f} GB"
        else:
            size_str = f"{mb_freed:.2f} MB"
        
        summary = f"✓ Purge complete: {files_deleted} files deleted, {size_str} freed"
        self._append_log(f"=== {summary} ===")
        self.statusBar().showMessage(summary)
        
        # Re-enable UI
        self.purge_button.setEnabled(True)
        self.go_button.setEnabled(True)
        
        # Reset queue counters since flatfiles are gone
        self.download_queue_bar.setValue(0)
        self.process_queue_bar.setValue(0)
        self.downloaded_label.setText("0")
        self.skipped_label.setText("0")
        self.failed_label.setText("0")
        self.processed_label.setText("0")
    
    def _backfill_finished(self, success: bool, message: str):
        """Handle backfill completion"""
        self.go_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        
        if success:
            self.statusBar().showMessage(f"✓ {message}")
            self._append_log(f"=== {message} ===")
        else:
            self.statusBar().showMessage(f"✗ {message}")
            self._append_log(f"=== ERROR: {message} ===")
    
    def _append_log(self, text: str):
        """Append text to log output"""
        # Color code based on log level
        if "ERROR" in text or "FAILED" in text:
            color = "#f44336"
        elif "WARNING" in text:
            color = "#FF9800"
        elif "INFO" in text or "✓" in text:
            color = "#4CAF50"
        elif "DEBUG" in text:
            color = "#9E9E9E"
        else:
            color = "#d4d4d4"
        
        self.log_output.append(f'<span style="color: {color}">{text}</span>')
        
        # Auto-scroll to bottom
        scrollbar = self.log_output.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
    
    def _apply_log_filter(self):
        """Apply log level filter (placeholder - would need log buffering)"""
        pass
    
    def _update_queue_metrics(self, download_qsize: int, process_qsize: int, 
                             download_maxsize: int = 100, process_maxsize: int = 100):
        """Update real-time queue depth indicators (progress bars, chart, AND tables)"""
        # Update progress bars with dynamic maxsize
        self.download_queue_bar.setMaximum(download_maxsize)
        self.download_queue_bar.setValue(download_qsize)
        self.download_queue_bar.setFormat(f"%v/{download_maxsize}")
        
        self.process_queue_bar.setMaximum(process_maxsize)
        self.process_queue_bar.setValue(process_qsize)
        self.process_queue_bar.setFormat(f"%v/{process_maxsize}")
        
        # Calculate state counts from task_states
        downloading_count = sum(1 for task in self.task_states.values() if task['state'] == 'DOWNLOADING')
        processing_count = sum(1 for task in self.task_states.values() if task['state'] == 'PROCESSING')
        waiting_count = sum(1 for task in self.task_states.values() if task['state'] == 'WAITING')
        retrying_count = sum(1 for task in self.task_states.values() if task.get('attempts', 0) > 1)
        
        # Update live multi-line chart with all current metrics
        self.queue_chart.update_data(
            downloaded=self.current_stats['downloaded'],
            processed=self.current_stats['processed'],
            skipped=self.current_stats['skipped'],
            failed=self.current_stats['failed'],
            retrying=retrying_count,
            downloading=downloading_count,
            processing=processing_count,
            waiting=waiting_count,
            download_latency_ms=self.current_stats['download_latency_ms'],
            process_latency_ms=self.current_stats['process_latency_ms'],
            process_qsize=process_qsize
        )
        
        # Force table refresh to show current queue states
        self._refresh_task_tables()
    
    def _update_stats(self, stats: Dict):
        """Update statistics display and current_stats for charts"""
        if 'downloaded' in stats:
            self.current_stats['downloaded'] = stats['downloaded']
            self.downloaded_label.setText(str(stats['downloaded']))
        if 'skipped' in stats:
            self.current_stats['skipped'] = stats['skipped']
            self.skipped_label.setText(str(stats['skipped']))
        if 'failed' in stats:
            self.current_stats['failed'] = stats['failed']
            self.failed_label.setText(str(stats['failed']))
        if 'processed' in stats:
            self.current_stats['processed'] = stats['processed']
            self.processed_label.setText(str(stats['processed']))
        
        # Update performance metrics from Pipeline Metrics log
        if 'download_mbps' in stats:
            self.current_stats['download_mbps'] = stats['download_mbps']
        if 'download_latency_ms' in stats:
            self.current_stats['download_latency_ms'] = stats['download_latency_ms']
        if 'process_latency_ms' in stats:
            self.current_stats['process_latency_ms'] = stats['process_latency_ms']
        
        # Store total for progress calculation
        if 'total' in stats:
            self.total_files = stats['total']
        
        # Update progress
        if hasattr(self, 'total_files') and self.total_files > 0:
            try:
                downloaded = int(self.downloaded_label.text())
                skipped = int(self.skipped_label.text())
                failed = int(self.failed_label.text())
                completed = downloaded + skipped + failed
                progress = min(100, int((completed / self.total_files) * 100))
                self.progress_bar.setValue(progress)
            except:
                pass
        
        # Update status bar with pipeline statistics
        self._update_status_bar()
    
    def _update_status_bar(self):
        """Update status bar with pipeline statistics in inline format"""
        try:
            ticker = self.ticker_input.text().strip().upper() if hasattr(self, 'ticker_input') else "..."
            downloaded = int(self.downloaded_label.text())
            skipped = int(self.skipped_label.text())
            failed = int(self.failed_label.text())
            processed = int(self.processed_label.text())
            
            # Calculate MB/min from MB/s
            mb_per_min = self.current_stats.get('download_mbps', 0.0) * 60
            
            # Build status message
            status_msg = f"Running backfill for {ticker} | Downloaded: {downloaded} | Skipped: {skipped} | Failed: {failed} | Processed: {processed}"
            
            # Add progress if we have total
            if hasattr(self, 'total_files') and self.total_files > 0:
                completed = downloaded + skipped + failed
                progress = min(100, int((completed / self.total_files) * 100))
                status_msg += f" | Progress: {progress}%"
            
            # Add throughput
            status_msg += f" | {mb_per_min:.1f} MB/min"
            
            self.statusBar().showMessage(status_msg)
        except:
            pass  # Silently ignore status bar update errors
    
    def _update_task(self, task_id: str, state: str, info: Dict):
        """Update task state in tables"""
        if task_id not in self.task_states:
            self.task_states[task_id] = {
                'id': task_id,
                'state': state,
                'date': info.get('date', ''),
                'attempts': 0,
                'error': info.get('error', ''),
                'filename': info.get('filename', ''),
                'old_state': info.get('old_state', ''),
                'bars': 0
            }
        else:
            # Update existing task
            self.task_states[task_id]['state'] = state
            if info.get('error'):
                self.task_states[task_id]['error'] = info['error']
            if info.get('old_state'):
                self.task_states[task_id]['old_state'] = info['old_state']
            
            # Increment attempts on retry
            if state == 'RETRYING':
                self.task_states[task_id]['attempts'] += 1
        
        # Force immediate table refresh
        self._refresh_task_tables()
    
    def _refresh_task_tables(self):
        """Refresh download and process tables with batch grouping every 100 items"""
        # Download table (WAITING, DOWNLOADING, RETRYING, FAILED states)
        download_tasks = [(k, v) for k, v in self.task_states.items() 
                         if v['state'] in ['WAITING', 'DOWNLOADING', 'RETRYING', 'FAILED']]
        
        # Calculate rows needed (tasks + batch separators)
        batch_separators = len(download_tasks) // 100
        total_rows = len(download_tasks) + batch_separators
        
        self.download_table.setRowCount(total_rows)
        
        table_row = 0
        for idx, (task_id, task) in enumerate(sorted(download_tasks, key=lambda x: x[0])):
            # Insert batch separator every 100 items
            if idx > 0 and idx % 100 == 0:
                batch_start = idx
                batch_end = min(idx + 99, len(download_tasks) - 1)
                separator = QTableWidgetItem(f"━━━ Batch {batch_start+1}-{batch_end+1} ━━━")
                separator.setBackground(QColor("#555"))
                separator.setForeground(QColor("#FFF"))
                font = QFont()
                font.setBold(True)
                separator.setFont(font)
                self.download_table.setItem(table_row, 0, separator)
                for col in range(1, 6):
                    self.download_table.setItem(table_row, col, QTableWidgetItem(""))
                    self.download_table.item(table_row, col).setBackground(QColor("#555"))
                table_row += 1
            
            # Task ID
            id_item = QTableWidgetItem(task_id)
            self.download_table.setItem(table_row, 0, id_item)
            
            # Date
            date_item = QTableWidgetItem(task.get('date', ''))
            self.download_table.setItem(table_row, 1, date_item)
            
            # State - Color-coded
            state_item = QTableWidgetItem(task['state'])
            state_item.setForeground(QColor(255, 255, 255))  # White text
            
            if task['state'] == 'DOWNLOADING':
                state_item.setBackground(QColor('#2196F3'))  # Blue
            elif task['state'] == 'RETRYING':
                state_item.setBackground(QColor('#FF9800'))  # Orange
            elif task['state'] == 'FAILED':
                state_item.setBackground(QColor('#f44336'))  # Red
            elif task['state'] == 'WAITING':
                state_item.setBackground(QColor('#9E9E9E'))  # Gray
            
            self.download_table.setItem(table_row, 2, state_item)
            
            # Attempts
            attempts_item = QTableWidgetItem(str(task.get('attempts', 0)))
            self.download_table.setItem(table_row, 3, attempts_item)
            
            # Error
            error_item = QTableWidgetItem(task.get('error', ''))
            if task.get('error'):
                error_item.setForeground(QColor('#f44336'))  # Red text for errors
            self.download_table.setItem(table_row, 4, error_item)
            
            # Filename
            filename_item = QTableWidgetItem(task.get('filename', ''))
            self.download_table.setItem(table_row, 5, filename_item)
            
            table_row += 1
        
        # Process table (DOWNLOADED, PROCESSING, COMPLETED states)
        process_tasks = [(k, v) for k, v in self.task_states.items() 
                        if v['state'] in ['DOWNLOADED', 'PROCESSING', 'COMPLETED']]
        
        # Calculate rows needed (tasks + batch separators)
        batch_separators_pr = len(process_tasks) // 100
        total_rows_pr = len(process_tasks) + batch_separators_pr
        
        self.process_table.setRowCount(total_rows_pr)
        
        table_row_pr = 0
        for idx, (task_id, task) in enumerate(sorted(process_tasks, key=lambda x: x[0])):
            # Insert batch separator every 100 items
            if idx > 0 and idx % 100 == 0:
                batch_start = idx
                batch_end = min(idx + 99, len(process_tasks) - 1)
                separator = QTableWidgetItem(f"━━━ Batch {batch_start+1}-{batch_end+1} ━━━")
                separator.setBackground(QColor("#555"))
                separator.setForeground(QColor("#FFF"))
                font = QFont()
                font.setBold(True)
                separator.setFont(font)
                self.process_table.setItem(table_row_pr, 0, separator)
                for col in range(1, 5):
                    self.process_table.setItem(table_row_pr, col, QTableWidgetItem(""))
                    item = self.process_table.item(table_row_pr, col)
                    if item:
                        item.setBackground(QColor("#555"))
                table_row_pr += 1
            
            # Task ID
            id_item = QTableWidgetItem(task_id)
            self.process_table.setItem(table_row_pr, 0, id_item)
            
            # Date
            date_item = QTableWidgetItem(task.get('date', ''))
            self.process_table.setItem(table_row_pr, 1, date_item)
            
            # State - Color-coded
            state_item = QTableWidgetItem(task['state'])
            state_item.setForeground(QColor(255, 255, 255))  # White text
            
            if task['state'] == 'PROCESSING':
                state_item.setBackground(QColor('#FF9800'))  # Orange
            elif task['state'] == 'COMPLETED':
                state_item.setBackground(QColor('#4CAF50'))  # Green
            elif task['state'] == 'DOWNLOADED':
                state_item.setBackground(QColor('#2196F3'))  # Blue
            
            self.process_table.setItem(table_row_pr, 2, state_item)
            
            # Bars Written
            bars_item = QTableWidgetItem(str(task.get('bars', 0)))
            self.process_table.setItem(table_row_pr, 3, bars_item)
            
            # Status
            status = 'OK' if task['state'] == 'COMPLETED' else 'In Progress'
            status_item = QTableWidgetItem(status)
            if task['state'] == 'COMPLETED':
                status_item.setForeground(QColor('#4CAF50'))  # Green
            self.process_table.setItem(table_row_pr, 4, status_item)
            
            table_row_pr += 1
    
    def _update_phase(self, phase_name: str, data: Dict):
        """Handle phase-specific updates"""
        # ====================================================================
        # PHASE 1: Reference Data
        # ====================================================================
        if phase_name == 'phase1_start':
            self.phase1_data.clear()
            self.ref_table.setRowCount(0)
            self._append_log("Phase 1: Reference Data started")
        
        elif phase_name == 'phase1_ticker':
            ticker = data.get('ticker', '')
            
            if ticker:
                self.phase1_data[ticker] = {
                    'status': data.get('status', 'Unknown'),
                    'name': data.get('name', ''),
                    'exchange': data.get('exchange', ''),
                    'type': data.get('type', ''),
                    'market_cap': data.get('market_cap', '')
                }
                self._refresh_ref_table()
        
        elif phase_name == 'phase1_complete':
            success = data.get('success', 0)
            total = data.get('total', 0)
            self._append_log(f"Phase 1: Complete ({success}/{total} successful)")
            self._refresh_ref_table()
        
        # ====================================================================
        # PHASE 2: Corporate Actions
        # ====================================================================
        elif phase_name == 'phase2_start':
            self.phase2_data.clear()
            self.corp_table.setRowCount(0)
            self._append_log("Phase 2: Corporate Actions started")
        
        elif phase_name == 'phase2_dividend':
            ticker = data.get('ticker', '')
            count = data.get('count', 0)
            
            if ticker:
                if ticker not in self.phase2_data:
                    self.phase2_data[ticker] = {
                        'dividends': 0,
                        'splits': 0,
                        'div_details': [],
                        'split_details': []
                    }
                
                self.phase2_data[ticker]['dividends'] = count
                self.current_ticker_p2 = ticker
                self._refresh_corp_table()
        
        elif phase_name == 'phase2_dividend_detail':
            if self.current_ticker_p2 and self.current_ticker_p2 in self.phase2_data:
                self.phase2_data[self.current_ticker_p2]['div_details'].append({
                    'amount': data.get('amount', 0),
                    'date': data.get('date', '')
                })
                self._refresh_corp_table()
        
        elif phase_name == 'phase2_split':
            ticker = data.get('ticker', '')
            count = data.get('count', 0)
            
            if ticker:
                if ticker not in self.phase2_data:
                    self.phase2_data[ticker] = {
                        'dividends': 0,
                        'splits': 0,
                        'div_details': [],
                        'split_details': []
                    }
                
                self.phase2_data[ticker]['splits'] = count
                self.current_ticker_p2 = ticker
                self._refresh_corp_table()
        
        elif phase_name == 'phase2_split_detail':
            if self.current_ticker_p2 and self.current_ticker_p2 in self.phase2_data:
                self.phase2_data[self.current_ticker_p2]['split_details'].append({
                    'ratio': data.get('ratio', 1.0),
                    'date': data.get('date', '')
                })
                self._refresh_corp_table()
        
        elif phase_name == 'phase2_complete':
            self._append_log("Phase 2: Corporate Actions complete")
            self._refresh_corp_table()
        
        # ====================================================================
        # PHASE 3: Daily Bars
        # ====================================================================
        elif phase_name == 'phase3_start':
            self.phase3_data.clear()
            self.daily_table.setRowCount(0)
            self._append_log("Phase 3: Daily Bars started")
        
        elif phase_name == 'phase3_ticker':
            ticker = data.get('ticker', '')
            bars = data.get('bars', 0)
            date_range = data.get('date_range', '')
            
            if ticker:
                self.phase3_data[ticker] = {
                    'bars': bars,
                    'status': 'Complete',
                    'progress': 100,
                    'date_range': date_range
                }
                self._refresh_daily_table()
        
        elif phase_name == 'phase3_complete':
            bars = data.get('bars', 0)
            self._append_log(f"Phase 3: Complete ({bars:,} bars)")
            self._refresh_daily_table()
        
        # ====================================================================
        # PHASE 4: FlatFiles (in debug mode, shows individual dates)
        # ====================================================================
        elif phase_name == 'phase4_bars_written':
            date_str = data.get('date', '')
            bars = data.get('bars', 0)
            tickers_str = data.get('tickers', '')
            
            if date_str:
                self.phase4_data[date_str] = {
                    'bars': bars,
                    'tickers': tickers_str,
                    'status': 'Complete'
                }
                
                # In debug mode, show Phase 4 data in daily bars table
                if self.debug_mode:
                    self._refresh_daily_table()
    
    def _refresh_ref_table(self):
        """Refresh Reference Data table"""
        self.ref_table.setRowCount(len(self.phase1_data))
        
        for row, (ticker, info) in enumerate(sorted(self.phase1_data.items())):
            # Ticker
            ticker_item = QTableWidgetItem(ticker)
            ticker_item.setFont(QFont("Courier New", weight=QFont.Weight.Bold))
            self.ref_table.setItem(row, 0, ticker_item)
            
            # Name
            name_item = QTableWidgetItem(info.get('name', ''))
            self.ref_table.setItem(row, 1, name_item)
            
            # Exchange
            exchange_item = QTableWidgetItem(info.get('exchange', ''))
            self.ref_table.setItem(row, 2, exchange_item)
            
            # Type
            type_item = QTableWidgetItem(info.get('type', ''))
            self.ref_table.setItem(row, 3, type_item)
            
            # Market Cap
            market_cap_item = QTableWidgetItem(info.get('market_cap', ''))
            self.ref_table.setItem(row, 4, market_cap_item)
            
            # Status
            status = info.get('status', 'Unknown')
            status_item = QTableWidgetItem(status)
            
            if status == 'Success':
                status_item.setForeground(QColor('#4CAF50'))  # Green
            elif status == 'Missing':
                status_item.setForeground(QColor('#f44336'))  # Red
            else:
                status_item.setForeground(QColor('#FF9800'))  # Orange
            
            self.ref_table.setItem(row, 5, status_item)
    
    def _refresh_corp_table(self):
        """Refresh Corporate Actions table - show both summaries and details"""
        # Count total rows needed (one row per ticker summary + detail rows)
        total_rows = 0
        for ticker, info in self.phase2_data.items():
            total_rows += 1  # Summary row for dividends
            if info['dividends'] > 0:
                total_rows += min(3, len(info['div_details']))  # Show up to 3 dividend details
            
            if info['splits'] > 0:
                total_rows += 1  # Summary row for splits
                total_rows += len(info['split_details'])  # All split details
        
        self.corp_table.setRowCount(total_rows)
        
        current_row = 0
        for ticker in sorted(self.phase2_data.keys()):
            info = self.phase2_data[ticker]
            
            # Dividend summary row
            if info['dividends'] > 0 or True:  # Always show even if 0
                ticker_item = QTableWidgetItem(ticker)
                ticker_item.setFont(QFont("Courier New", weight=QFont.Weight.Bold))
                self.corp_table.setItem(current_row, 0, ticker_item)
                
                type_item = QTableWidgetItem("Dividends")
                type_item.setForeground(QColor('#2196F3'))  # Blue
                self.corp_table.setItem(current_row, 1, type_item)
                
                self.corp_table.setItem(current_row, 2, QTableWidgetItem("Summary"))
                self.corp_table.setItem(current_row, 3, QTableWidgetItem("—"))
                
                count_item = QTableWidgetItem(f"{info['dividends']} found")
                font = QFont("Courier New")
                font.setWeight(QFont.Weight.Bold)
                count_item.setFont(font)
                self.corp_table.setItem(current_row, 4, count_item)
                
                status_item = QTableWidgetItem("OK" if info['dividends'] > 0 else "None")
                status_item.setForeground(QColor('#4CAF50') if info['dividends'] > 0 else QColor('#9E9E9E'))
                self.corp_table.setItem(current_row, 5, status_item)
                
                current_row += 1
                
                # Show first 3 dividend details
                for div in info['div_details'][:3]:
                    self.corp_table.setItem(current_row, 0, QTableWidgetItem("  ↳"))
                    self.corp_table.setItem(current_row, 1, QTableWidgetItem("Dividend"))
                    self.corp_table.setItem(current_row, 2, QTableWidgetItem(div['date']))
                    
                    amount_item = QTableWidgetItem(f"${div['amount']:.4f}")
                    amount_item.setForeground(QColor('#4CAF50'))
                    self.corp_table.setItem(current_row, 3, amount_item)
                    
                    self.corp_table.setItem(current_row, 4, QTableWidgetItem("—"))
                    self.corp_table.setItem(current_row, 5, QTableWidgetItem("✓"))
                    
                    current_row += 1
            
            # Split summary and details
            if info['splits'] > 0:
                ticker_item = QTableWidgetItem(ticker)
                ticker_item.setFont(QFont("Courier New", weight=QFont.Weight.Bold))
                self.corp_table.setItem(current_row, 0, ticker_item)
                
                type_item = QTableWidgetItem("Splits")
                type_item.setForeground(QColor('#FF9800'))  # Orange
                self.corp_table.setItem(current_row, 1, type_item)
                
                self.corp_table.setItem(current_row, 2, QTableWidgetItem("Summary"))
                self.corp_table.setItem(current_row, 3, QTableWidgetItem("—"))
                
                count_item = QTableWidgetItem(f"{info['splits']} found")
                font = QFont("Courier New")
                font.setWeight(QFont.Weight.Bold)
                count_item.setFont(font)
                self.corp_table.setItem(current_row, 4, count_item)
                
                status_item = QTableWidgetItem("OK")
                status_item.setForeground(QColor('#4CAF50'))
                self.corp_table.setItem(current_row, 5, status_item)
                
                current_row += 1
                
                # Show all split details
                for split in info['split_details']:
                    self.corp_table.setItem(current_row, 0, QTableWidgetItem("  ↳"))
                    self.corp_table.setItem(current_row, 1, QTableWidgetItem("Split"))
                    self.corp_table.setItem(current_row, 2, QTableWidgetItem(split['date']))
                    
                    ratio_item = QTableWidgetItem(f"{split['ratio']:.2f}:1")
                    ratio_item.setForeground(QColor('#FF9800'))
                    self.corp_table.setItem(current_row, 3, ratio_item)
                    
                    self.corp_table.setItem(current_row, 4, QTableWidgetItem("—"))
                    self.corp_table.setItem(current_row, 5, QTableWidgetItem("✓"))
                    
                    current_row += 1
    
    def _refresh_daily_table(self):
        """Refresh Daily Bars table - shows Phase 3 (aggregate) or Phase 4 (per-file debug) data"""
        
        # In debug mode, show Phase 4 flatfile data (individual dates)
        if self.debug_mode and self.phase4_data:
            # Update column headers for debug mode
            self.daily_table.setHorizontalHeaderLabels(["Date", "Tickers", "Bars", "Status", "Progress"])
            
            self.daily_table.setRowCount(len(self.phase4_data))
            
            for row, (date_str, info) in enumerate(sorted(self.phase4_data.items())):
                # Date (instead of ticker)
                date_item = QTableWidgetItem(date_str)
                date_item.setFont(QFont("Courier New", weight=QFont.Weight.Bold))
                self.daily_table.setItem(row, 0, date_item)
                
                # Tickers
                tickers_item = QTableWidgetItem(info.get('tickers', ''))
                self.daily_table.setItem(row, 1, tickers_item)
                
                # Bars
                bars = info.get('bars', 0)
                bars_item = QTableWidgetItem(f"{bars:,}")
                self.daily_table.setItem(row, 2, bars_item)
                
                # Status
                status = info.get('status', 'Processing')
                status_item = QTableWidgetItem(status)
                
                if status == 'Complete':
                    status_item.setForeground(QColor('#4CAF50'))  # Green
                else:
                    status_item.setForeground(QColor('#FF9800'))  # Orange
                
                self.daily_table.setItem(row, 3, status_item)
                
                # Progress (always 100% for completed files)
                progress_item = QTableWidgetItem("100%")
                self.daily_table.setItem(row, 4, progress_item)
        
        # Normal mode: show Phase 3 data (aggregate daily bars per ticker)
        else:
            # Update column headers for normal mode
            self.daily_table.setHorizontalHeaderLabels(["Ticker", "Date Range", "Bars", "Status", "Progress"])
            
            self.daily_table.setRowCount(len(self.phase3_data))
            
            for row, (ticker, info) in enumerate(sorted(self.phase3_data.items())):
                # Ticker
                ticker_item = QTableWidgetItem(ticker)
                ticker_item.setFont(QFont("Courier New", weight=QFont.Weight.Bold))
                self.daily_table.setItem(row, 0, ticker_item)
                
                # Date Range
                range_item = QTableWidgetItem(info.get('date_range', ''))
                self.daily_table.setItem(row, 1, range_item)
                
                # Bars
                bars = info.get('bars', 0)
                bars_item = QTableWidgetItem(f"{bars:,}")
                self.daily_table.setItem(row, 2, bars_item)
                
                # Status
                status = info.get('status', 'In Progress')
                status_item = QTableWidgetItem(status)
                
                if status == 'Complete':
                    status_item.setForeground(QColor('#4CAF50'))  # Green
                else:
                    status_item.setForeground(QColor('#FF9800'))  # Orange
                
                self.daily_table.setItem(row, 3, status_item)
                
                # Progress
                progress = info.get('progress', 0)
                progress_item = QTableWidgetItem(f"{progress}%")
                self.daily_table.setItem(row, 4, progress_item)


def main():
    """Main entry point"""
    app = QApplication(sys.argv)
    
    # Set dark theme
    app.setStyle("Fusion")
    palette = QPalette()
    palette.setColor(QPalette.ColorRole.Window, QColor(53, 53, 53))
    palette.setColor(QPalette.ColorRole.WindowText, Qt.GlobalColor.white)
    palette.setColor(QPalette.ColorRole.Base, QColor(25, 25, 25))
    palette.setColor(QPalette.ColorRole.AlternateBase, QColor(53, 53, 53))
    palette.setColor(QPalette.ColorRole.ToolTipBase, Qt.GlobalColor.white)
    palette.setColor(QPalette.ColorRole.ToolTipText, Qt.GlobalColor.white)
    palette.setColor(QPalette.ColorRole.Text, Qt.GlobalColor.white)
    palette.setColor(QPalette.ColorRole.Button, QColor(53, 53, 53))
    palette.setColor(QPalette.ColorRole.ButtonText, Qt.GlobalColor.white)
    palette.setColor(QPalette.ColorRole.BrightText, Qt.GlobalColor.red)
    palette.setColor(QPalette.ColorRole.Link, QColor(42, 130, 218))
    palette.setColor(QPalette.ColorRole.Highlight, QColor(42, 130, 218))
    palette.setColor(QPalette.ColorRole.HighlightedText, Qt.GlobalColor.black)
    app.setPalette(palette)
    
    window = BackfillVisualizer()
    window.show()
    
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
