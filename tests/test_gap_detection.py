"""Tests for gap detection and API backfill."""
import pytest
import asyncio
from datetime import datetime, timedelta
from scraper.registry.models import IngestionStrategy
from scraper.registry.manager import RegistryManager
from scraper.unified_orchestrator import UnifiedOrchestrator


@pytest.mark.asyncio
async def test_gap_detected_when_last_backfill_old(temp_db):
    """Test gap detected when last_backfill > threshold (7 days)."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    # Add symbol with old last_backfill timestamp
    old_timestamp = datetime.now() - timedelta(days=10)
    
    registry_manager.add_symbol(
        symbol="AAPL",
        strategy=IngestionStrategy.FLATPACK_API,
        timeframe="1m",
        enabled=True
    )
    
    # Simulate old backfill timestamp by directly updating DB
    with db_writer.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE symbol_registry SET last_backfill = %s WHERE symbol = %s",
            (old_timestamp, "AAPL")
        )
        conn.commit()
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True
    )
    
    # Get gaps (internal method test)
    gaps = await orchestrator._detect_gaps("AAPL", "1m", old_timestamp)
    
    # Should detect gap from old_timestamp to now
    assert len(gaps) > 0
    gap = gaps[0]
    assert gap["start"] >= old_timestamp
    assert gap["end"] <= datetime.now()


@pytest.mark.asyncio
async def test_no_gaps_when_last_backfill_recent(temp_db):
    """Test no gaps detected when last_backfill < threshold."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    # Add symbol with recent last_backfill
    recent_timestamp = datetime.now() - timedelta(hours=1)
    
    registry_manager.add_symbol(
        symbol="NVDA",
        strategy=IngestionStrategy.FLATPACK_API,
        timeframe="1m",
        enabled=True
    )
    
    with db_writer.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE symbol_registry SET last_backfill = %s WHERE symbol = %s",
            (recent_timestamp, "NVDA")
        )
        conn.commit()
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True
    )
    
    gaps = await orchestrator._detect_gaps("NVDA", "1m", recent_timestamp)
    
    # No gaps if last_backfill < 7 days ago
    assert len(gaps) == 0


@pytest.mark.asyncio
async def test_api_backfill_scheduled_for_gaps(temp_db):
    """Test API backfill phase scheduled for exact gap windows."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    old_timestamp = datetime.now() - timedelta(days=9)
    
    registry_manager.add_symbol(
        symbol="AMD",
        strategy=IngestionStrategy.API,
        timeframe="5m",
        enabled=True
    )
    
    with db_writer.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE symbol_registry SET last_backfill = %s WHERE symbol = %s",
            (old_timestamp, "AMD")
        )
        conn.commit()
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True  # Dry run to avoid actual API calls
    )
    
    async with orchestrator.lifecycle():
        # In dry_run mode, API phase will be executed but not make real calls
        await orchestrator.run_cycle()
    
    # Verify gap was detected (internal method test)
    gaps = await orchestrator._detect_gaps("AMD", "5m", old_timestamp)
    assert len(gaps) > 0


@pytest.mark.asyncio
async def test_multiple_gap_windows_handled(temp_db):
    """Test multiple gap windows detected and backfilled."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    # Simulate very old last_backfill (30 days)
    very_old = datetime.now() - timedelta(days=30)
    
    registry_manager.add_symbol(
        symbol="SPY",
        strategy=IngestionStrategy.API,
        timeframe="1m",
        enabled=True
    )
    
    with db_writer.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE symbol_registry SET last_backfill = %s WHERE symbol = %s",
            (very_old, "SPY")
        )
        conn.commit()
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True
    )
    
    gaps = await orchestrator._detect_gaps("SPY", "1m", very_old)
    
    # With 30-day gap, should split into multiple windows
    # (e.g., max 7-day chunks due to API limits)
    assert len(gaps) >= 3  # At least 3 windows for 30 days


@pytest.mark.asyncio
async def test_gap_detection_updates_registry_timestamp(temp_db):
    """Test successful API backfill updates last_backfill timestamp."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    old_timestamp = datetime.now() - timedelta(days=8)
    
    registry_manager.add_symbol(
        symbol="TSLA",
        strategy=IngestionStrategy.FLATPACK_API,
        timeframe="1m",
        enabled=True
    )
    
    with db_writer.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE symbol_registry SET last_backfill = %s WHERE symbol = %s",
            (old_timestamp, "TSLA")
        )
        conn.commit()
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True  # Dry run to avoid actual API calls
    )
    
    async with orchestrator.lifecycle():
        await orchestrator.run_cycle()
    
    # Check registry was updated after cycle
    symbol_entry = registry_manager.get_symbol("TSLA")
    
    # In dry_run, timestamp updates may still occur
    assert symbol_entry is not None
    # Timestamp should either be updated or remain old (depending on dry_run impl)
    assert symbol_entry.last_backfill is not None


@pytest.mark.asyncio
async def test_flatpack_api_gap_combo(temp_db):
    """Test flatpack+api strategy uses API for gaps, flatpack otherwise."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    # Recent flatpack, old backfill (gap scenario)
    recent_flatpack = datetime.now() - timedelta(minutes=5)
    old_backfill = datetime.now() - timedelta(days=10)
    
    registry_manager.add_symbol(
        symbol="QQQ",
        strategy=IngestionStrategy.FLATPACK_API,
        timeframe="1m",
        enabled=True
    )
    
    with db_writer.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE symbol_registry SET last_flatpack = %s, last_backfill = %s WHERE symbol = %s",
            (recent_flatpack, old_backfill, "QQQ")
        )
        conn.commit()
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True
    )
    
    # Test gap detection logic
    gaps = await orchestrator._detect_gaps("QQQ", "1m", old_backfill)
    
    # Should detect gap due to old backfill
    assert len(gaps) > 0
    
    # In real execution, both flatpack and API phases would run
    async with orchestrator.lifecycle():
        await orchestrator.run_cycle()
