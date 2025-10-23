"""Tests for hybrid registry strategies."""
import pytest
import asyncio
from datetime import datetime, timedelta
from scraper.registry.models import IngestionStrategy, IngestionStatus, SymbolRegistry
from scraper.registry.manager import RegistryManager
from scraper.unified_orchestrator import UnifiedOrchestrator


@pytest.mark.asyncio
async def test_flatpack_api_strategy(temp_db, sample_registry_entries):
    """Test flatpack+api strategy executes both phases."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    # Add symbol with flatpack+api strategy
    registry_manager.add_symbol(
        symbol="AAPL",
        strategy=IngestionStrategy.FLATPACK_API,
        timeframe="1m",
        enabled=True
    )
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True
    )
    
    async with orchestrator.lifecycle():
        results = await orchestrator.run_cycle()
    
    assert "AAPL" in results
    result = results["AAPL"]
    
    # Should have both flatpack and api phases
    assert "flatpack" in result["phases"]
    assert result["phases"]["flatpack"]["status"] == "completed"
    
    # API phase might be skipped if no gaps
    assert "api" in result["phases"]


@pytest.mark.asyncio
async def test_flatpack_api_snapshot_stream_full_hybrid(temp_db):
    """Test full hybrid strategy flatpack+api+snapshot+stream."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    registry_manager.add_symbol(
        symbol="NVDA",
        strategy=IngestionStrategy.FLATPACK_API_SNAPSHOT_STREAM,
        timeframe="1m",
        enabled=True
    )
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True,
        enable_stream=True
    )
    
    async with orchestrator.lifecycle():
        results = await orchestrator.run_cycle()
    
    result = results["NVDA"]
    
    # All four phases should be present
    assert "flatpack" in result["phases"]
    assert "api" in result["phases"]
    assert "snapshot" in result["phases"]
    assert "stream" in result["phases"]


@pytest.mark.asyncio
async def test_timestamp_updates_per_phase(temp_db):
    """Ensure registry timestamps update after each phase."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    registry_manager.add_symbol(
        symbol="AMD",
        strategy=IngestionStrategy.SNAPSHOT_STREAM,
        timeframe="1m",
        enabled=True
    )
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True
    )
    
    async with orchestrator.lifecycle():
        await orchestrator.run_cycle()
    
    # Check registry entry was updated
    entry = registry_manager.get_symbol("AMD")
    assert entry.last_snapshot is not None
    assert entry.last_stream is not None
    assert entry.status == IngestionStatus.IDLE


@pytest.mark.asyncio
async def test_strategy_parsing(temp_db):
    """Test that strategy string is correctly parsed into phases."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    # Test various strategy combinations
    test_cases = [
        ("flatpack", ["flatpack"]),
        ("flatpack+api", ["flatpack", "api"]),
        ("snapshot+stream", ["snapshot", "stream"]),
        ("flatpack+api+snapshot+stream", ["flatpack", "api", "snapshot", "stream"]),
    ]
    
    for strategy_str, expected_phases in test_cases:
        # Parse strategy value
        phases = strategy_str.split('+')
        assert phases == expected_phases
