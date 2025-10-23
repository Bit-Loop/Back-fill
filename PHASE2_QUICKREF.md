# Phase 2 Quick Reference

## Files Modified (Production Code)

### 1. scraper/registry/models.py
```python
# Added 2 new strategy enums
class IngestionStrategy(Enum):
    # ... existing strategies ...
    FLATPACK_API_SNAPSHOT_STREAM = "flatpack+api+snapshot+stream"
    API = "api"
```

### 2. scraper/unified_orchestrator.py
**Changes:**
- Enhanced `__init__` with 4 new parameters: `publish_mode`, `throttle_ms`, `snapshot_method`, `enable_stream`
- Replaced `_execute_symbol()` with hybrid strategy parsing logic
- Added `_detect_gaps(symbol, timeframe, last_backfill)` method
- Added `_execute_api_phase(symbol, timeframe, gaps, log)` method
- Updated all phase methods to accept `timeframe` parameter
- Replaced `_publish_kafka()` with `_publish_envelope()` implementing standard format

**Key Methods:**
```python
async def _detect_gaps(symbol: str, timeframe: str, last_backfill: datetime) -> List[Dict]:
    """Detect gaps in backfill coverage (7-day threshold)."""
    
async def _execute_api_phase(symbol: str, timeframe: str, gaps: List[Dict], log: Logger):
    """Execute API backfill for detected gaps."""
    
async def _publish_envelope(bar: Dict, symbol: str, timeframe: str, source: str):
    """Publish standardized envelope to Kafka/Redis."""
```

### 3. chronox_ingestion.py
**Added CLI Flags:**
```python
parser.add_argument("--strategy", help="Override registry strategy")
parser.add_argument("--snapshot-method", choices=["rest", "agg", "snapshot"])
parser.add_argument("--publish-mode", choices=["kafka-first", "db-first"])
parser.add_argument("--no-stream", action="store_true")
parser.add_argument("--symbols", nargs="+")
parser.add_argument("--timeframe", default="1m")
parser.add_argument("--throttle-ms", type=int, default=0)
```

**Updated Signature:**
```python
async def run_one_cycle(
    dry_run: bool,
    strategy_override: Optional[str] = None,
    snapshot_method: str = "snapshot",
    publish_mode: str = "kafka-first",
    enable_stream: bool = True,
    symbol_override: Optional[List[str]] = None,
    timeframe_override: str = "1m",
    throttle_ms: int = 0
):
```

## Files Created (Test Code)

### 1. tests/test_registry_strategies.py (134 lines)
**Tests:**
- `test_flatpack_api_strategy()` - Verifies flatpack+api hybrid execution
- `test_flatpack_api_snapshot_stream_full_hybrid()` - Tests all 4 phases
- `test_timestamp_updates_per_phase()` - Validates registry timestamps
- `test_strategy_parsing()` - Tests strategy string parsing
- `test_strategy_validation()` - Validates invalid strategies rejected

### 2. tests/test_snapshot_flags.py (145 lines)
**Tests:**
- `test_snapshot_method_rest()` - REST API endpoint variant
- `test_snapshot_method_agg()` - Aggregates API endpoint variant
- `test_snapshot_method_snapshot()` - Snapshot API endpoint variant
- `test_envelope_published_with_snapshot()` - Envelope structure validation

### 3. tests/test_publish_modes.py (228 lines)
**Tests:**
- `test_kafka_first_mode()` - Kafka-first publishing order
- `test_db_first_mode()` - DB-first publishing order
- `test_envelope_dedup_key_format()` - Dedup key validation
- `test_redis_mirror_kafka_first()` - Redis pub/sub mirroring
- `test_throttle_applied_between_publishes()` - Throttle delays

### 4. tests/test_gap_detection.py (267 lines)
**Tests:**
- `test_gap_detected_when_last_backfill_old()` - 7-day threshold detection
- `test_no_gaps_when_last_backfill_recent()` - No false positives
- `test_api_backfill_scheduled_for_gaps()` - API phase scheduled correctly
- `test_multiple_gap_windows_handled()` - Large gaps split properly
- `test_gap_detection_updates_registry_timestamp()` - Registry updates
- `test_flatpack_api_gap_combo()` - Hybrid strategy + gaps

### 5. tests/conftest.py (Modified)
**Added Mock Classes:**
- `MockKafkaProducer` - Mock Kafka publishing
- `MockRedisClient` - Mock Redis pub/sub
- `MockPolygonClient` - Mock Polygon API calls

## Standard Envelope Format

```json
{
  "event": "bar",
  "symbol": "AAPL",
  "tf": "1m",
  "ts": "2024-01-15T10:30:00Z",
  "ohlcv": {
    "open": 150.0,
    "high": 151.0,
    "low": 149.5,
    "close": 150.5,
    "volume": 1000000
  },
  "features": {},
  "source": "flatpack|api|snapshot|stream",
  "version": "v1",
  "dedup_key": "AAPL|1m|2024-01-15T10:30:00Z"
}
```

## CLI Usage Patterns

### Pattern 1: Registry-Driven (Production)
```bash
# Let registry control everything
python chronox_ingestion.py
```

### Pattern 2: Ad-Hoc Symbol Override
```bash
# Bypass registry, backfill specific symbols
python chronox_ingestion.py \
  --symbols AAPL NVDA AMD \
  --strategy flatpack+api \
  --timeframe 5m \
  --throttle-ms 100
```

### Pattern 3: Custom Snapshot Method
```bash
# Use different snapshot API endpoint
python chronox_ingestion.py \
  --snapshot-method agg \
  --publish-mode kafka-first
```

### Pattern 4: DB-First Compliance Mode
```bash
# Write DB before Kafka (audit trail)
python chronox_ingestion.py \
  --publish-mode db-first
```

### Pattern 5: Dry Run Testing
```bash
# Test without writes
python chronox_ingestion.py \
  --symbols SPY \
  --strategy flatpack+api+snapshot+stream \
  --dry-run
```

## Gap Detection Logic

```python
# Threshold: 7 days
if datetime.now() - last_backfill > timedelta(days=7):
    # Split into 7-day windows
    gaps = []
    start = last_backfill
    while start < datetime.now():
        end = min(start + timedelta(days=7), datetime.now())
        gaps.append({"start": start, "end": end})
        start = end
    
    # Execute API backfill for each window
    for gap in gaps:
        await _execute_api_phase(symbol, timeframe, gap)
```

## Publish Mode Decision Tree

```
┌─────────────────────────────┐
│ publish_mode = ?            │
└─────────────────────────────┘
               │
       ┌───────┴───────┐
       │               │
  kafka-first     db-first
       │               │
       ▼               ▼
┌─────────────┐  ┌─────────────┐
│ 1. Kafka    │  │ 1. DB Write │
│ 2. Redis    │  │ 2. Kafka    │
│ 3. DB Write │  │ 3. Redis    │
└─────────────┘  └─────────────┘
```

## Testing Commands

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_gap_detection.py -v

# Run with coverage
python -m pytest tests/ --cov=scraper --cov-report=html

# Run tests matching pattern
python -m pytest tests/ -k "gap_detection" -v
```

## Performance Metrics

**Lines of Code:**
- Production changes: ~190 lines
- Test code: ~774 lines
- Total Phase 2: ~964 lines

**Test Coverage:**
- 21 test functions
- 4 new test files
- 3 mock classes

**Execution Time:**
- Gap detection: O(1) query + O(n) windows
- Envelope serialization: O(1) per bar
- Test suite: ~5-10 seconds (all tests)

## Rollback Plan

**If issues arise:**
1. Revert `scraper/registry/models.py` enum additions
2. Revert `scraper/unified_orchestrator.py` changes
3. Revert `chronox_ingestion.py` CLI flag additions
4. Keep existing tests, mark as skipped

**No database migrations required** - all changes backward compatible.

## Next Phase (Phase 3 - Optional)

**Scheduler Daemon:**
- Poll registry every N seconds
- Auto-detect and backfill gaps
- Maintain long-lived WebSocket streams
- Health checks and auto-restart

**Estimated effort:** 100-150 lines of code
