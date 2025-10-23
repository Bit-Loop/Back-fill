# ChronoX Ingestion Service - Registry-Based Architecture

## Overview

The ChronoX Ingestion Service has been transformed from a CLI-driven scraper into an API-first, registry-driven data ingestion platform with flexible strategies and automated scheduling.

## Architecture Components

### 1. Symbol Registry (`symbol_registry` table)

Central configuration database that stores ingestion strategies per symbol:

```sql
CREATE TABLE symbol_registry (
    symbol TEXT PRIMARY KEY,
    strategy TEXT NOT NULL,  -- Ingestion strategy (see below)
    timeframe TEXT DEFAULT '1m',
    enabled BOOLEAN DEFAULT TRUE,
    last_backfill TIMESTAMPTZ,
    last_snapshot TIMESTAMPTZ,
    last_stream TIMESTAMPTZ,
    status TEXT DEFAULT 'idle',  -- idle|running|error|completed|scheduled
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 2. Ingestion Strategies

Six hybrid strategies available per symbol:

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `FLATPACK` | S3 flat files only | Historical bulk data (years) |
| `SNAPSHOT` | Real-time snapshots only | Current market state |
| `STREAM` | WebSocket streaming only | Real-time tick data |
| `FLATPACK_API` | Flat files + REST API gap fill | Historical + gap detection |
| `SNAPSHOT_STREAM` | Snapshots + streaming | Recent + real-time data |
| `FLATPACK_SNAPSHOT_STREAM` | Complete hybrid | Full historical + real-time |

### 3. Kafka-First Publishing

All data flows publish to symbol-specific Kafka topics:

- `chronox.backfill.<symbol>` - Historical data completions
- `chronox.snapshot.<symbol>` - Snapshot ingestion events
- `chronox.stream.<symbol>` - Real-time stream data

**Architecture**: Kafka = source of truth, TimescaleDB = consumer

### 4. APScheduler Integration

Automated periodic tasks:

- **Nightly Gap Detection** (00:00 UTC): Scans TimescaleDB for missing candles, triggers auto-backfill
- **Snapshot Refresh** (*/5m): Periodic snapshot ingestion for configured symbols
- **Stream Monitoring** (*/1m): Health checks for active WebSocket streams

## API Endpoints

### Core Service Endpoints

```http
GET  /health                      # Service health check
POST /backfill                    # Legacy backfill (CLI-style)
GET  /jobs                        # List all jobs
GET  /jobs/{job_id}               # Get job status
DELETE /jobs/{job_id}             # Cancel job
POST /stream                      # Start streaming
DELETE /stream                    # Stop streaming
GET  /stream                      # Stream status
```

### Symbol Registry Endpoints

```http
POST   /api/v1/symbols            # Add symbol to registry
GET    /api/v1/symbols            # List all symbols
GET    /api/v1/symbols/{symbol}   # Get symbol details
PATCH  /api/v1/symbols/{symbol}   # Update symbol config
DELETE /api/v1/symbols/{symbol}   # Remove symbol
POST   /api/v1/backfill/{symbol}  # Manual backfill trigger
GET    /api/v1/status             # Overall orchestrator status
```

### Registry Execution

```http
POST /api/v1/registry/execute     # Execute registry strategies
```

**Request body**:
```json
{
  "symbols": ["AAPL", "NVDA"]  // Optional, null = all enabled
}
```

### Scheduler Endpoints

```http
GET  /api/v1/scheduler/status          # Get scheduler status & jobs
POST /api/v1/scheduler/trigger/{job_id} # Manually trigger job
```

## Usage Examples

### 1. Add Symbols to Registry

```bash
# Add AAPL with flatpack strategy (historical data)
curl -X POST http://localhost:9000/api/v1/symbols \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "AAPL",
    "strategy": "flatpack",
    "timeframe": "1m",
    "enabled": true
  }'

# Add NVDA with snapshot+stream (real-time)
curl -X POST http://localhost:9000/api/v1/symbols \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "NVDA",
    "strategy": "snapshot_stream",
    "timeframe": "1m"
  }'

# Add SPY with full hybrid strategy
curl -X POST http://localhost:9000/api/v1/symbols \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "SPY",
    "strategy": "flatpack_snapshot_stream",
    "timeframe": "1m"
  }'
```

### 2. Execute Registry Strategies

```bash
# Execute all enabled symbols
curl -X POST http://localhost:9000/api/v1/registry/execute \
  -H "Content-Type: application/json" \
  -d '{"symbols": null}'

# Execute specific symbols
curl -X POST http://localhost:9000/api/v1/registry/execute \
  -H "Content-Type: application/json" \
  -d '{"symbols": ["AAPL", "NVDA"]}'
```

### 3. Monitor Execution

```bash
# Check job status
curl http://localhost:9000/jobs/{job_id}

# Check overall status
curl http://localhost:9000/api/v1/status

# Check scheduler status
curl http://localhost:9000/api/v1/scheduler/status
```

### 4. List Symbols

```bash
# All symbols
curl http://localhost:9000/api/v1/symbols

# Enabled only
curl http://localhost:9000/api/v1/symbols?enabled_only=true

# Get specific symbol
curl http://localhost:9000/api/v1/symbols/AAPL
```

### 5. Update Symbol Configuration

```bash
# Change strategy
curl -X PATCH http://localhost:9000/api/v1/symbols/AAPL \
  -H "Content-Type: application/json" \
  -d '{"strategy": "flatpack_snapshot_stream"}'

# Disable symbol
curl -X PATCH http://localhost:9000/api/v1/symbols/NVDA \
  -H "Content-Type: application/json" \
  -d '{"enabled": false}'
```

## Running the Service

### Start FastAPI Daemon

```bash
# Development mode
python scraper.py --daemon --host 127.0.0.1 --port 9000

# With scheduler enabled (default)
ENABLE_SCHEDULER=true python scraper.py --daemon

# Custom snapshot interval (default 5 minutes)
SNAPSHOT_INTERVAL_MINUTES=10 python scraper.py --daemon
```

### Environment Variables

```bash
# .env file configuration
POLYGON_API_KEY=your_api_key

# Database
TIMESCALE_HOST=localhost
TIMESCALE_PORT=5432
TIMESCALE_DB=chronox
TIMESCALE_USER=postgres
TIMESCALE_PASSWORD=password

# Redis (optional for pub/sub)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

# Kafka (optional)
KAFKA_BROKERS=localhost:9092

# Scheduler
ENABLE_SCHEDULER=true
SNAPSHOT_INTERVAL_MINUTES=5

# API
SCRAPER_HOST=127.0.0.1
SCRAPER_PORT=9000
```

## Strategy Execution Flow

### FLATPACK Strategy
1. Reference data ingestion
2. Corporate actions (dividends, splits)
3. Daily bars (OHLCV)
4. Minute bars from S3 flat files
5. Kafka publish to `chronox.backfill.<symbol>`
6. Update `last_backfill` timestamp

### SNAPSHOT Strategy
1. Fetch current market snapshot
2. Normalize and store in TimescaleDB
3. Kafka publish to `chronox.snapshot.<symbol>`
4. Update `last_snapshot` timestamp

### FLATPACK_SNAPSHOT_STREAM Strategy (Complete Hybrid)
1. Execute FLATPACK phases (reference, corporate, daily, minute)
2. Execute SNAPSHOT phase (current state)
3. Activate STREAM (requires separate streaming daemon)
4. Kafka publish to all three topics
5. Update all timestamps

## Scheduled Tasks

### Nightly Gap Detection (00:00 UTC)

Automatically detects missing candles:

```sql
-- Query to find gaps
WITH expected AS (
    SELECT generate_series(
        MIN(timestamp), MAX(timestamp), '1 minute'
    ) as ts FROM ohlcv_1m WHERE symbol = 'AAPL'
),
actual AS (
    SELECT timestamp FROM ohlcv_1m WHERE symbol = 'AAPL'
)
SELECT COUNT(*) FROM expected WHERE ts NOT IN (SELECT * FROM actual);
```

Triggers backfill for symbols with significant gaps.

### Snapshot Refresh (Configurable Interval)

Refreshes snapshots for symbols with strategies:
- `SNAPSHOT`
- `SNAPSHOT_STREAM`
- `FLATPACK_SNAPSHOT_STREAM`

### Stream Monitoring (Every Minute)

Checks `last_stream` timestamp for symbols with stream strategies.
Logs warnings for stale streams (>5 minutes).

## Database Schema

### Symbol Registry
```sql
-- Core registry table
symbol_registry (
    symbol, strategy, timeframe, enabled,
    last_backfill, last_snapshot, last_stream,
    status, error_message, created_at, updated_at
)

-- Index for efficient queries
CREATE INDEX idx_symbol_registry_enabled 
ON symbol_registry (enabled, status);
```

### OHLCV Data
```sql
-- 1-minute candles (TimescaleDB hypertable)
ohlcv_1m (
    symbol, timestamp, open, high, low, close, volume
)

-- Continuous aggregates for higher timeframes
ohlcv_5m, ohlcv_15m, ohlcv_1h, ohlcv_1d
```

## Kafka Topic Schema

### Backfill Completion Event
```json
{
  "symbol": "AAPL",
  "strategy": "flatpack",
  "phase": "completed",
  "timestamp": "2025-10-22T12:00:00Z",
  "results": {
    "reference": {"AAPL": true},
    "corporate_actions": {"dividends": 10, "splits": 1},
    "daily_bars": {"AAPL": 1260},
    "minute_bars": {"AAPL": 756000}
  }
}
```

### Snapshot Event
```json
{
  "symbol": "NVDA",
  "strategy": "snapshot",
  "phase": "completed",
  "timestamp": "2025-10-22T12:05:00Z",
  "count": 1
}
```

### Stream Activation Event
```json
{
  "symbol": "SPY",
  "strategy": "stream",
  "action": "activate",
  "timestamp": "2025-10-22T12:00:00Z"
}
```

## Migration from CLI to Registry

### Old CLI Approach
```bash
python scraper.py --tickers AAPL,NVDA,SPY --years 5 --skip-news
```

### New Registry Approach

1. **Add symbols to registry:**
```bash
for symbol in AAPL NVDA SPY; do
  curl -X POST http://localhost:9000/api/v1/symbols \
    -d "{\"symbol\": \"$symbol\", \"strategy\": \"flatpack\"}"
done
```

2. **Execute:**
```bash
curl -X POST http://localhost:9000/api/v1/registry/execute \
  -d '{"symbols": null}'
```

3. **Monitor:**
```bash
curl http://localhost:9000/api/v1/status
```

## Benefits of Registry Architecture

1. **Configuration as Code**: Symbol strategies stored in database, versioned via migrations
2. **Flexible Strategies**: Per-symbol control over data ingestion approach
3. **Automated Scheduling**: Set-and-forget periodic tasks
4. **Gap Detection**: Automatic identification and backfill of missing data
5. **Kafka-First**: Publish-subscribe architecture for downstream consumers
6. **API-Driven**: RESTful control plane for orchestration
7. **Monitoring**: Real-time status tracking via API endpoints

## Future Enhancements

- [ ] WebSocket API for real-time job progress
- [ ] Grafana dashboard for scheduler metrics
- [ ] Kafka consumer for TimescaleDB writes (full event-sourcing)
- [ ] Auto-scaling based on queue depth
- [ ] Multi-tenant symbol isolation
- [ ] Strategy templates (crypto, forex, options)

## Dependencies

```txt
# Core
python>=3.11
polygon-api-client>=1.12.0
psycopg2-binary>=2.9.9
tqdm>=4.66.1

# API (daemon mode)
fastapi>=0.104.0
uvicorn>=0.24.0
pydantic>=2.5.0

# Scheduler
apscheduler>=3.10.4

# Optional: Kafka
confluent-kafka>=2.3.0

# Optional: Redis
redis>=5.0.1
```

## Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Run migrations (if using Alembic)
alembic upgrade head

# Start service
python scraper.py --daemon
```

## Testing

```bash
# Test registry operations
python -c "
from common.storage.timescale_writer import TimescaleWriter
from scraper.registry.manager import RegistryManager
from scraper.registry.models import IngestionStrategy

writer = TimescaleWriter(
    host='localhost', port=5432, database='chronox',
    user='postgres', password='password'
)
manager = RegistryManager(writer)

# Add test symbol
manager.add_symbol('AAPL', IngestionStrategy.FLATPACK, '1m')

# List symbols
for s in manager.list_symbols():
    print(f'{s.symbol}: {s.strategy.value} ({s.status.value})')
"
```

## Troubleshooting

### Scheduler Not Running
```bash
# Check scheduler status
curl http://localhost:9000/api/v1/scheduler/status

# Enable scheduler
export ENABLE_SCHEDULER=true
python scraper.py --daemon
```

### Symbols Not Executing
```bash
# Check symbol is enabled
curl http://localhost:9000/api/v1/symbols/AAPL

# Enable symbol if needed
curl -X PATCH http://localhost:9000/api/v1/symbols/AAPL \
  -d '{"enabled": true}'
```

### Gap Detection Not Working
```bash
# Manually trigger gap detection
curl -X POST http://localhost:9000/api/v1/scheduler/trigger/nightly_gap_detection

# Check logs
tail -f logs/backfill.log | grep -i "gap detection"
```

## License

MIT License - See LICENSE file for details
