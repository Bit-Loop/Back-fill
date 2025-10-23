# ChronoX Historical Data Ingestion Service

Production-grade service for ingesting historical market data from Polygon.io with:
- **Symbol Registry**: Flexible ingestion strategies (flatpack, snapshot, stream, hybrid)
- **Kafka-First Architecture**: Publish to Kafka topics, consume into TimescaleDB
- **Structured Logging**: JSON format for easy parsing and monitoring
- **Error Recovery**: Retry logic, status tracking, gap detection
- **APScheduler Integration**: Nightly gap detection, periodic snapshot refresh
- **REST API**: FastAPI endpoints for registry management and orchestration

## Architecture

```
┌─────────────────┐
│ CLI Entry Point │  chronox_ingestion.py
└────────┬────────┘
         │
         ├──► --test-snapshot AAPL    (validate snapshot fetch)
         ├──► --daemon                (run with scheduler)
         ├──► --dry-run               (simulate without writes)
         └──► (default)               (one-time registry execution)
         │
         ▼
┌────────────────────┐
│ UnifiedOrchestrator│  Reads symbol_registry, executes strategies
└────────┬───────────┘
         │
         ├──► Flatpack Phase:   Historical backfill (REST/S3)
         ├──► Snapshot Phase:   Current market snapshot
         └──► Stream Phase:     Real-time WebSocket stream
         │
         ▼
┌────────────────────┐
│  Kafka Producer    │  Publish to chronox.{backfill|snapshot|stream}.<symbol>
└────────────────────┘
         │
         ▼
┌────────────────────┐
│  Kafka Consumer    │  (External) Write to TimescaleDB
└────────────────────┘
```

## Installation

### Prerequisites
- Python 3.11+
- PostgreSQL with TimescaleDB extension
- Kafka (optional, for Kafka publishing)
- Redis (optional, for real-time pub/sub)

### Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export POLYGON_API_KEY="your_polygon_api_key"
export TIMESCALE_HOST="localhost"
export TIMESCALE_PORT="5432"
export TIMESCALE_DB="chronox"
export TIMESCALE_USER="chronox"
export TIMESCALE_PASSWORD="your_password"

# Initialize database schema
python scripts/init_registry_schema.py
```

## Usage

### CLI Flags

| Flag | Description | Example |
|------|-------------|---------|
| `--test-snapshot SYMBOL` | Test snapshot fetch for symbol (exits after test) | `--test-snapshot AAPL` |
| `--daemon` | Run in daemon mode with scheduler | `--daemon` |
| `--dry-run` | Simulate execution without writes | `--dry-run` |
| `--log-level LEVEL` | Set log level (DEBUG, INFO, WARN, ERROR) | `--log-level DEBUG` |

### Examples

**Test snapshot fetch for AAPL:**
```bash
python chronox_ingestion.py --test-snapshot AAPL
```

**Run one-time registry execution:**
```bash
python chronox_ingestion.py
```

**Run in daemon mode with gap detection:**
```bash
python chronox_ingestion.py --daemon
```

**Dry run with debug logging:**
```bash
python chronox_ingestion.py --dry-run --log-level DEBUG
```

### Symbol Registry Management

**Add symbol with strategy:**
```python
from scraper.registry.manager import RegistryManager
from scraper.registry.models import IngestionStrategy, SymbolRegistry

manager = RegistryManager(db_writer)

# Add AAPL with flatpack + snapshot + stream strategy
manager.add_symbol(SymbolRegistry(
    symbol='AAPL',
    strategy=IngestionStrategy.FLATPACK_SNAPSHOT_STREAM,
    enabled=True,
    description='Apple Inc.'
))
```

**List enabled symbols:**
```python
symbols = manager.list_symbols(enabled_only=True)
for entry in symbols:
    print(f"{entry.symbol}: {entry.strategy.value} (status={entry.status.value})")
```

**Update symbol status:**
```python
# Disable symbol
manager.update_status('AAPL', IngestionStatus.DISABLED)

# Set error status
manager.update_status('NVDA', IngestionStatus.ERROR, error_message='Rate limit exceeded')
```

## REST API

Start the FastAPI server:
```bash
uvicorn scraper.service.api:app --reload --port 8000
```

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/api/v1/symbols` | List all symbols |
| `POST` | `/api/v1/symbols` | Add new symbol |
| `PUT` | `/api/v1/symbols/{symbol}/status` | Update symbol status |
| `DELETE` | `/api/v1/symbols/{symbol}` | Delete symbol |
| `POST` | `/api/v1/registry/execute` | Execute registry strategies |
| `GET` | `/api/v1/scheduler/status` | Get scheduler status |
| `POST` | `/api/v1/scheduler/start` | Start scheduler |
| `POST` | `/api/v1/scheduler/stop` | Stop scheduler |

**Example: Add symbol via API:**
```bash
curl -X POST http://localhost:8000/api/v1/symbols \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "AAPL",
    "strategy": "flatpack_snapshot_stream",
    "enabled": true,
    "description": "Apple Inc."
  }'
```

**Example: Execute registry:**
```bash
curl -X POST http://localhost:8000/api/v1/registry/execute
```

## Ingestion Strategies

| Strategy | Backfill | Snapshot | Stream | Use Case |
|----------|----------|----------|--------|----------|
| `FLATPACK` | ✓ | ✗ | ✗ | Historical data only |
| `SNAPSHOT` | ✗ | ✓ | ✗ | Current market data |
| `STREAM` | ✗ | ✗ | ✓ | Real-time updates only |
| `FLATPACK_API` | ✓ (REST) | ✗ | ✗ | Historical via REST API |
| `SNAPSHOT_STREAM` | ✗ | ✓ | ✓ | Current + real-time |
| `FLATPACK_SNAPSHOT_STREAM` | ✓ | ✓ | ✓ | Complete data pipeline |

## Scheduler Jobs

| Job | Trigger | Description |
|-----|---------|-------------|
| **Gap Detection** | Daily at 00:00 UTC | Scans TimescaleDB for missing candles, triggers backfill |
| **Snapshot Refresh** | Every 5 minutes | Refreshes snapshots for configured symbols |
| **Stream Monitoring** | Every 1 minute | Monitors active WebSocket streams for health |

## Structured Logging

All log entries are JSON-formatted:
```json
{
  "time": "2025-01-13T14:00:00.000Z",
  "level": "INFO",
  "message": "symbol_execution_started",
  "symbol": "AAPL",
  "strategy": "flatpack_snapshot_stream",
  "phase": "snapshot",
  "records": 1,
  "kafka": true,
  "redis": false,
  "elapsed": 2.35
}
```

## Testing

Run the test suite:
```bash
# Install pytest
pip install pytest pytest-asyncio

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_registry_crud.py -v

# Run with coverage
pytest tests/ --cov=scraper --cov-report=html
```

### Test Files

- `tests/test_registry_crud.py`: Registry CRUD operations (11 tests)
- `tests/test_snapshot_fetch.py`: Snapshot validation and Kafka publishing (10 tests)
- `tests/test_orchestrator_loop.py`: Orchestrator strategy execution (10 tests)

## Error Recovery

The orchestrator automatically:
1. **Clears stale locks** on startup (symbols stuck in "running" status)
2. **Retries failed operations** with exponential backoff
3. **Updates error status** with descriptive messages
4. **Logs structured errors** with symbol, phase, and error details

**Example error log:**
```json
{
  "time": "2025-01-13T14:05:00.000Z",
  "level": "ERROR",
  "message": "symbol_execution_failed",
  "symbol": "NVDA",
  "strategy": "snapshot",
  "error": "HTTP 429: Rate limit exceeded",
  "elapsed": 0.52
}
```

## Gap Detection

The scheduler runs nightly gap detection:
1. Queries TimescaleDB for symbols with missing candles
2. Compares expected vs. actual 1-minute candles
3. Logs gap information (missing count, duration)
4. (TODO) Auto-triggers backfill for gap periods

**SQL query for gap detection:**
```sql
WITH bounds AS (
  SELECT MIN(timestamp) as first_ts, MAX(timestamp) as last_ts
  FROM ohlcv_1m WHERE symbol = 'AAPL'
),
expected AS (
  SELECT generate_series(
    date_trunc('minute', first_ts),
    date_trunc('minute', last_ts),
    '1 minute'::interval
  ) as ts FROM bounds WHERE first_ts IS NOT NULL
),
actual AS (
  SELECT date_trunc('minute', timestamp) as ts
  FROM ohlcv_1m WHERE symbol = 'AAPL'
)
SELECT COUNT(*) as missing_count
FROM expected WHERE ts NOT IN (SELECT ts FROM actual)
```

## Configuration

### Database Schema

**`symbol_registry` table:**
```sql
CREATE TABLE symbol_registry (
    symbol VARCHAR(10) PRIMARY KEY,
    strategy VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'idle',
    enabled BOOLEAN DEFAULT true,
    last_backfill TIMESTAMPTZ,
    last_snapshot TIMESTAMPTZ,
    last_stream TIMESTAMPTZ,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POLYGON_API_KEY` | (required) | Polygon.io API key |
| `TIMESCALE_HOST` | `localhost` | TimescaleDB host |
| `TIMESCALE_PORT` | `5432` | TimescaleDB port |
| `TIMESCALE_DB` | `chronox` | Database name |
| `TIMESCALE_USER` | `chronox` | Database user |
| `TIMESCALE_PASSWORD` | (empty) | Database password |
| `KAFKA_BOOTSTRAP_SERVERS` | (optional) | Kafka bootstrap servers |
| `REDIS_HOST` | (optional) | Redis host |
| `REDIS_PORT` | (optional) | Redis port |

## Production Deployment

### Docker Compose

```yaml
version: '3.8'

services:
  chronox-ingestion:
    build: .
    command: python chronox_ingestion.py --daemon
    environment:
      - POLYGON_API_KEY=${POLYGON_API_KEY}
      - TIMESCALE_HOST=timescaledb
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    depends_on:
      - timescaledb
      - kafka
    restart: always

  timescaledb:
    image: timescale/timescaledb:latest-pg16
    environment:
      - POSTGRES_DB=chronox
      - POSTGRES_USER=chronox
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
    volumes:
      - timescale_data:/var/lib/postgresql/data

  kafka:
    image: confluentinc/cp-kafka:latest
    environment:
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
    depends_on:
      - zookeeper

volumes:
  timescale_data:
```

### Systemd Service

```ini
[Unit]
Description=ChronoX Ingestion Service
After=network.target

[Service]
Type=simple
User=chronox
WorkingDirectory=/opt/chronox
Environment="POLYGON_API_KEY=your_key"
ExecStart=/opt/chronox/venv/bin/python chronox_ingestion.py --daemon
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Monitoring

### Metrics to Monitor

1. **Ingestion Rate**: Records ingested per symbol per minute
2. **Error Rate**: Percentage of failed executions
3. **Gap Count**: Number of missing candles detected
4. **Latency**: Time from snapshot fetch to Kafka publish
5. **Queue Depth**: Kafka topic lag

### Log Queries

**Find errors in last hour:**
```bash
cat logs/chronox.log | jq 'select(.level == "ERROR" and .time > "2025-01-13T13:00:00Z")'
```

**Count executions by symbol:**
```bash
cat logs/chronox.log | jq 'select(.message == "symbol_execution_completed")' | jq -r '.symbol' | sort | uniq -c
```

## Troubleshooting

### Issue: Symbol stuck in "running" status

**Solution:** Restart service to clear stale locks (automatic on startup)

### Issue: Snapshot validation fails

**Solution:** Check Polygon API response format:
```bash
python chronox_ingestion.py --test-snapshot AAPL --log-level DEBUG
```

### Issue: Gap detection finds many gaps

**Solution:** Trigger manual backfill:
```python
from scraper.unified_orchestrator import run_backfill
await run_backfill('AAPL', start_time, end_time, db_writer)
```

### Issue: Kafka publishing fails

**Solution:** Check Kafka connectivity and topic creation:
```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

## Development

### Project Structure

```
backfill/
├── chronox_ingestion.py          # CLI entry point
├── scraper/
│   ├── unified_orchestrator.py   # Main orchestration logic
│   ├── registry/
│   │   ├── models.py             # Data models (SymbolRegistry, enums)
│   │   └── manager.py            # CRUD operations
│   ├── scheduler.py              # APScheduler integration
│   ├── pipeline/
│   │   └── snapshot_validator.py # Validation and Kafka publishing
│   ├── service/
│   │   ├── api.py                # FastAPI endpoints
│   │   └── core.py               # Service lifecycle management
│   └── utils/
│       └── structured_logging.py # JSON logger
└── tests/
    ├── conftest.py               # Pytest fixtures
    ├── test_registry_crud.py     # Registry tests
    ├── test_snapshot_fetch.py    # Snapshot tests
    └── test_orchestrator_loop.py # Orchestrator tests
```

### Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/my-feature`
3. Add tests for new functionality
4. Run test suite: `pytest tests/ -v`
5. Commit changes: `git commit -am 'Add feature'`
6. Push to branch: `git push origin feature/my-feature`
7. Submit pull request

## License

MIT License - See LICENSE file for details

## Support

For issues or questions:
- GitHub Issues: https://github.com/your-org/chronox/issues
- Documentation: https://docs.chronox.io
- Email: support@chronox.io
