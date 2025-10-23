"""Tests for snapshot method flags."""
import pytest
import asyncio
from scraper.registry.models import IngestionStrategy
from scraper.registry.manager import RegistryManager
from scraper.unified_orchestrator import UnifiedOrchestrator


@pytest.mark.asyncio
async def test_snapshot_method_rest(temp_db):
    """Test --snapshot-method rest flag."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    registry_manager.add_symbol(
        symbol="AAPL",
        strategy=IngestionStrategy.SNAPSHOT,
        timeframe="1m",
        enabled=True
    )
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True,
        snapshot_method="rest"
    )
    
    async with orchestrator.lifecycle():
        results = await orchestrator.run_cycle()
    
    assert results["AAPL"]["phases"]["snapshot"]["status"] == "completed"


@pytest.mark.asyncio
async def test_snapshot_method_agg(temp_db):
    """Test --snapshot-method agg flag."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    registry_manager.add_symbol(
        symbol="NVDA",
        strategy=IngestionStrategy.SNAPSHOT,
        timeframe="1m",
        enabled=True
    )
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True,
        snapshot_method="agg"
    )
    
    async with orchestrator.lifecycle():
        results = await orchestrator.run_cycle()
    
    result = results["NVDA"]
    assert result["phases"]["snapshot"]["status"] == "completed"


@pytest.mark.asyncio
async def test_snapshot_method_snapshot(temp_db):
    """Test --snapshot-method snapshot flag (default)."""
    from common.storage.timescale_writer import TimescaleWriter
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    registry_manager.add_symbol(
        symbol="AMD",
        strategy=IngestionStrategy.SNAPSHOT,
        timeframe="1m",
        enabled=True
    )
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=True,
        snapshot_method="snapshot"
    )
    
    async with orchestrator.lifecycle():
        results = await orchestrator.run_cycle()
    
    result = results["AMD"]
    assert result["phases"]["snapshot"]["status"] == "completed"


@pytest.mark.asyncio
async def test_envelope_published_with_snapshot(temp_db, mock_kafka_producer):
    """Test that snapshot publishes standard envelope."""
    from common.storage.timescale_writer import TimescaleWriter
    from tests.conftest import MockKafkaProducer
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    registry_manager.add_symbol(
        symbol="SPY",
        strategy=IngestionStrategy.SNAPSHOT,
        timeframe="1m",
        enabled=True
    )
    
    kafka_producer = MockKafkaProducer()
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        kafka_producer=kafka_producer,
        dry_run=True,
        publish_mode="kafka-first"
    )
    
    async with orchestrator.lifecycle():
        await orchestrator.run_cycle()
    
    # Check envelope was published
    assert len(kafka_producer.messages) > 0
    
    # Verify envelope structure
    message = kafka_producer.messages[0]
    import json
    envelope = json.loads(message['value'].decode('utf-8'))
    
    assert envelope["event"] == "bar"
    assert envelope["symbol"] == "SPY"
    assert envelope["tf"] == "1m"
    assert envelope["source"] == "snapshot"
    assert envelope["version"] == "v1"
    assert "dedup_key" in envelope
    assert "ohlcv" in envelope
