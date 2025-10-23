# Phase 2 Implementation Plan

## Files to Modify

### 1. `scraper/unified_orchestrator.py` - Hybrid orchestration + gap detection
- Add hybrid strategy parsing (flatpack+api+snapshot+stream)
- Add gap detection with API backfill
- Add publish_mode flag (kafka-first vs db-first)
- Add standard envelope format
- Add throttle support

### 2. `chronox_ingestion.py` - CLI flags extension
- Add --strategy override
- Add --snapshot-method flag
- Add --publish-mode flag
- Add --no-stream flag
- Add --symbols flag
- Add --timeframe override
- Add --throttle-ms flag
- Wire flags to orchestrator

### 3. `scraper/pipeline/kafka_integration.py` - Standard envelope
- Ensure envelope format matches spec
- Add dedup_key support

### 4. `scraper/pipeline/message_bus.py` - Redis envelope
- Mirror standard envelope to Redis

### 5. `scraper/scheduler.py` - Daemon loop enhancements
- Add configurable poll interval
- Add gap detection integration

### 6. New test files
- `tests/test_registry_strategies.py`
- `tests/test_snapshot_flags.py`
- `tests/test_publish_modes.py`
- `tests/test_gap_detection.py`

## Implementation Order

1. Update registry models (add flatpack+api+snapshot+stream strategy)
2. Enhance unified_orchestrator with hybrid logic
3. Add CLI flags to chronox_ingestion.py
4. Standardize Kafka/Redis envelopes
5. Add tests
6. Update scheduler for daemon mode

---

## Acceptance Tests

```bash
# Test 1: Hybrid strategy
python chronox_ingestion.py --symbols AAPL --strategy flatpack+api --publish-mode kafka-first --dry-run

# Test 2: Snapshot only
python chronox_ingestion.py --symbols NVDA --strategy snapshot+stream --no-stream --dry-run

# Test 3: Run tests
python -m pytest tests/ -v

# Test 4: Daemon mode
python chronox_ingestion.py --daemon --throttle-ms 100
```

## Rollback

If CI fails, revert these commits:
- `git revert HEAD` (unified_orchestrator changes)
- `git revert HEAD~1` (chronox_ingestion changes)
- `git revert HEAD~2` (envelope standardization)
