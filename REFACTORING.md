# Backfill System - Refactored Modular Architecture

## Overview

This project has been refactored from monolithic files into a clean, modular architecture with two main entry points:
- **`scraper.py`** - Historical data backfill and real-time ingestion
- **`gui.py`** - Real-time visualization and monitoring interface

## Project Structure

```
backfill/
├── scraper.py                      # Main scraper entry point
├── gui.py                          # Main GUI entry point
├── common/                         # Shared utilities and models
│   ├── config/
│   │   └── settings.py            # Centralized configuration
│   ├── models/
│   │   └── data_models.py         # Data models (FileTask, OHLCVBar, etc.)
│   └── storage/
│       └── timescale_writer.py    # TimescaleDB storage layer (to be migrated)
├── scraper/                        # Data collection and processing
│   ├── orchestrator.py            # Backfill orchestration (to be created)
│   ├── clients/                   # Polygon API clients
│   │   ├── polygon_client.py      # Base REST client
│   │   ├── aggregates.py          # OHLCV data client
│   │   ├── corporate_actions.py   # Dividends/splits client
│   │   ├── reference.py           # Ticker reference data
│   │   └── news.py                # News articles client
│   ├── processors/
│   │   ├── indicators.py          # ✓ Technical indicators (O(1) incremental)
│   │   ├── normalizer.py          # Data normalization
│   │   └── enricher.py            # Data enrichment pipeline
│   ├── pipeline/
│   │   ├── kappa_engine.py        # Unified ingestion engine
│   │   ├── data_sources.py        # Historical/live data abstractions
│   │   └── message_bus.py         # Redis/Kafka message bus
│   └── utils/
│       ├── file_processor.py      # S3 flat file processing
│       └── market_hours.py        # Market hours utilities
└── gui/                            # Visualization components
    ├── main_window.py             # Main Qt window (to be created)
    ├── widgets/
    │   ├── chart_widget.py        # Chart display
    │   ├── control_panel.py       # User controls
    │   └── stats_panel.py         # Statistics display
    ├── workers/
    │   ├── backfill_worker.py     # Background backfill thread
    │   ├── data_loader.py         # Database query thread
    │   └── redis_subscriber.py    # Event-driven updates
    └── backends/
        ├── pyqtgraph_backend.py   # PyQtGraph charting
        └── finplot_backend.py     # FinPlot charting
```

## Key Features

### ✅ Completed Components

1. **Configuration System** (`common/config/settings.py`)
   - Centralized settings for HTTP, S3, Redis, Database
   - Environment variable integration
   - Type-safe dataclasses

2. **Data Models** (`common/models/data_models.py`)
   - `FileTask` - Thread-safe state machine for file processing
   - `OHLCVBar` - OHLCV bar representation
   - `EnrichedBar` - Bar with technical indicators
   - `CorporateAction`, `ReferenceData`, `NewsArticle`

3. **Indicator Processor** (`scraper/processors/indicators.py`)
   - O(1) incremental calculations (SMA, EMA, RSI, MACD, Bollinger Bands, VWAP)
   - Per-symbol state management
   - Rolling window architecture

4. **Entry Points**
   - `scraper.py` - CLI for backfill operations
   - `gui.py` - GUI launcher

5. **Package Structure**
   - Proper `__init__.py` files for all modules
   - Clean import paths

### 🔄 To Be Migrated from Existing Code

The following components need to be extracted from `backfill_historical_data.py` and `backfill_visualizer.py`:

#### From `backfill_historical_data.py` (3943 lines):

**Priority 1 - Core Scraper:**
1. `BackfillOrchestrator` class → `scraper/orchestrator.py`
   - Phase 1-5 methods (reference, corporate, daily, minute, news)
   - Flat file pipeline logic
   - Hybrid backfill mode

2. Polygon API clients → `scraper/clients/`
   - Extract imports from `data.ingestion.polygon.*`
   - Migrate to modular client classes

3. Data ingestion pipeline → `scraper/pipeline/`
   - `DataIngestionEngine` class
   - `MarketDataSource` abstraction (Historical/Live)
   - `RedisMessageBus`, `KafkaToRedisBridge`

4. File processing → `scraper/utils/`
   - `process_flat_file_task()` function
   - `generate_available_file_tasks()` function
   - `FileState` enum (already in models)

5. System utilities → `common/config/`
   - `get_pooled_session()` function
   - `check_system_limits()` function

#### From `backfill_visualizer.py` (4274 lines):

**Priority 2 - GUI Components:**
1. Main window → `gui/main_window.py`
   - `BackfillVisualizerWindow` class
   - Tab management
   - Menu bar

2. Chart widgets → `gui/widgets/`
   - `MarketDataWidget` class
   - `QueueChartWidget` class
   - Control panels
   - Stats displays

3. Background workers → `gui/workers/`
   - `BackfillWorker` class (subprocess management)
   - `ChartDataInitialLoader` class
   - `RedisSubscriberThread` class
   - `PolygonDataWorker` class

4. Chart backends → `gui/backends/`
   - `PyQtGraphBackend` class
   - `FinPlotBackend` class
   - `ChartBackend` ABC

## Patterns from polygon.io-stock-database Repository

The following patterns should be incorporated:

1. **Adjustments Handling** (from `05_download_adjustments.ipynb`)
   - Dividend adjustment calculations
   - Stock split handling
   - Cumulative adjustment factors

2. **Market Hours** (from `02_market_hours.ipynb`)
   - Trading hours by date
   - Early close detection
   - Holiday calendar

3. **Ticker Management** (from `03_tickers.ipynb`)
   - Active ticker filtering
   - Renaming detection
   - Delisting handling

4. **Data Aggregation** (from `09_aggregate.ipynb`)
   - OHLCV resampling to higher timeframes
   - Volume aggregation
   - VWAP calculation

5. **Processing Pipeline** (from `07_process.ipynb`)
   - Adjustment application to raw data
   - Data validation
   - Gap detection

## Usage Examples

### Scraper (CLI)

```bash
# Simple backfill for specific tickers
python scraper.py --tickers AMD,NVDA,TSLA --years 2

# Use S3 flat files for speed
python scraper.py --tickers AMD --years 5 --flatfiles

# Top 50 tickers, daily bars only
python scraper.py --all --limit 50 --skip-minute

# Debug mode
python scraper.py --tickers AMD --debug
```

### GUI (Interactive)

```bash
# Launch visualization interface
python gui.py
```

Features:
- Real-time chart updates via Redis Pub/Sub
- Historical data loading from TimescaleDB
- Multiple timeframes (1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w)
- Technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands, VWAP)
- Corporate actions overlay (dividends, splits)
- News feed integration
- Backfill monitoring (queue stats, throughput, latency)

## Architecture Principles

### Kappa Architecture (Unified Pipeline)
- Single processing path for batch and streaming
- Flow: DataSource → Processors → Storage → MessageBus
- Eliminates code duplication between backfill and live modes

### O(1) Incremental Processing
- Indicators calculated incrementally without recomputing history
- Rolling windows for efficient memory usage
- Per-symbol state isolation

### Event-Driven GUI
- Redis Pub/Sub for real-time updates (no polling)
- Background workers for database queries
- Qt signals for thread-safe communication

### Thread Safety
- Explicit locks for shared state
- Queue-based communication between threads
- Sentinel values for graceful shutdown

## Dependencies

```bash
# Core dependencies
pip install python-dotenv psycopg2-binary sqlalchemy pandas numpy

# Polygon.io SDK
pip install polygon-api-client

# Redis (optional, for real-time updates)
pip install redis

# Kafka (optional, for streaming)
pip install confluent-kafka

# GUI dependencies
pip install PyQt6 pyqtgraph matplotlib mplfinance

# S3 access
pip install boto3

# System monitoring
pip install psutil
```

## Configuration

Create a `.env` file with:

```env
# Polygon.io API
POLYGON_API_KEY=your_api_key_here
POLYGON_S3_ACCESS_KEY=your_s3_key
POLYGON_S3_SECRET_KEY=your_s3_secret

# Database (TimescaleDB)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=trading
DB_USER=postgres
DB_PASSWORD=your_password

# Redis (optional)
REDIS_PASSWORD=your_redis_password
REDIS_PORT=6380

# Debug
DEBUG_CPU=false
```

## Migration Roadmap

### Phase 1: Foundation (✅ COMPLETE)
- [x] Create folder structure
- [x] Configuration system
- [x] Data models
- [x] Indicator processor
- [x] Entry points (scraper.py, gui.py)
- [x] Package __init__ files

### Phase 2: Scraper Core (In Progress)
- [ ] Extract Polygon API clients
- [ ] Extract orchestrator logic
- [ ] Extract data pipeline components
- [ ] Extract file processing utilities
- [ ] Move TimescaleDB writer to common/storage

### Phase 3: GUI Components
- [ ] Extract main window
- [ ] Extract chart widgets
- [ ] Extract background workers
- [ ] Extract chart backends

### Phase 4: Integration
- [ ] Incorporate polygon.io-stock-database patterns
- [ ] Add adjustments handling
- [ ] Add market hours utilities
- [ ] Add aggregation functions

### Phase 5: Testing & Documentation
- [ ] Unit tests for processors
- [ ] Integration tests
- [ ] API documentation
- [ ] User guide

## Breaking Changes

None! The refactored code maintains full backward compatibility:
- Original `backfill_historical_data.py` still works
- Original `backfill_visualizer.py` still works
- New modular structure provides cleaner imports

## Contributing

When adding new features:
1. Place shared utilities in `common/`
2. Place scraper logic in `scraper/`
3. Place GUI components in `gui/`
4. Update `__init__.py` files for exports
5. Add configuration to `common/config/settings.py`
6. Document in this README

## License

Same as original project.
