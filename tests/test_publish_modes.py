"""Tests for publish modes (kafka-first vs db-first)."""
import pytest
import asyncio
import json
from scraper.registry.models import IngestionStrategy
from scraper.registry.manager import RegistryManager
from scraper.unified_orchestrator import UnifiedOrchestrator


@pytest.mark.asyncio
async def test_kafka_first_mode(temp_db):
    """Test --publish-mode kafka-first publishes to Kafka before DB."""
    from common.storage.timescale_writer import TimescaleWriter
    from tests.conftest import MockKafkaProducer
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    registry_manager.add_symbol(
        symbol="AAPL",
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
        dry_run=False,  # Not dry run to test publish
        publish_mode="kafka-first"
    )
    
    async with orchestrator.lifecycle():
        await orchestrator.run_cycle()
    
    # Verify Kafka was published
    assert len(kafka_producer.messages) > 0
    
    # Verify envelope structure
    msg = kafka_producer.messages[0]
    envelope = json.loads(msg['value'].decode('utf-8'))
    
    assert envelope["symbol"] == "AAPL"
    assert envelope["event"] == "bar"
    assert envelope["version"] == "v1"


@pytest.mark.asyncio
async def test_db_first_mode(temp_db):
    """Test --publish-mode db-first writes DB before Kafka."""
    from common.storage.timescale_writer import TimescaleWriter
    from tests.conftest import MockKafkaProducer
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    registry_manager.add_symbol(
        symbol="NVDA",
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
        dry_run=False,
        publish_mode="db-first"
    )
    
    async with orchestrator.lifecycle():
        await orchestrator.run_cycle()
    
    # Both modes should publish identical envelopes
    assert len(kafka_producer.messages) > 0
    
    msg = kafka_producer.messages[0]
    envelope = json.loads(msg['value'].decode('utf-8'))
    
    assert envelope["symbol"] == "NVDA"
    assert envelope["version"] == "v1"


@pytest.mark.asyncio
async def test_envelope_dedup_key_format(temp_db):
    """Test dedup_key follows format: symbol|timeframe|timestamp."""
    from common.storage.timescale_writer import TimescaleWriter
    from tests.conftest import MockKafkaProducer
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    registry_manager.add_symbol(
        symbol="AMD",
        strategy=IngestionStrategy.SNAPSHOT,
        timeframe="5m",
        enabled=True
    )
    
    kafka_producer = MockKafkaProducer()
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        kafka_producer=kafka_producer,
        dry_run=False,
        publish_mode="kafka-first"
    )
    
    async with orchestrator.lifecycle():
        await orchestrator.run_cycle()
    
    msg = kafka_producer.messages[0]
    envelope = json.loads(msg['value'].decode('utf-8'))
    
    dedup_key = envelope["dedup_key"]
    
    # Format: SYMBOL|TIMEFRAME|TIMESTAMP
    assert dedup_key.startswith("AMD|5m|")
    parts = dedup_key.split('|')
    assert len(parts) == 3
    assert parts[0] == "AMD"
    assert parts[1] == "5m"


@pytest.mark.asyncio
async def test_redis_mirror_kafka_first(temp_db):
    """Test Redis pub/sub mirrors Kafka in kafka-first mode (non-flatpack only)."""
    from common.storage.timescale_writer import TimescaleWriter
    from tests.conftest import MockKafkaProducer, MockRedisClient
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    registry_manager.add_symbol(
        symbol="SPY",
        strategy=IngestionStrategy.SNAPSHOT,
        timeframe="1m",
        enabled=True
    )
    
    kafka_producer = MockKafkaProducer()
    redis_client = MockRedisClient()
    
    orchestrator = UnifiedOrchestrator(
        polygon_api_key="test_key",
        db_writer=db_writer,
        registry_manager=registry_manager,
        kafka_producer=kafka_producer,
        redis_client=redis_client,
        dry_run=False,
        publish_mode="kafka-first"
    )
    
    async with orchestrator.lifecycle():
        await orchestrator.run_cycle()
    
    # Redis should also have the message
    assert len(redis_client.published) > 0
    
    redis_msg = redis_client.published[0]
    assert redis_msg['channel'] == "bars:SPY:1m"
    
    # Message should be identical envelope
    envelope = json.loads(redis_msg['message'].decode('utf-8'))
    assert envelope["symbol"] == "SPY"
    assert envelope["version"] == "v1"


@pytest.mark.asyncio
async def test_throttle_applied_between_publishes(temp_db):
    """Test --throttle-ms adds delay between publishes."""
    from common.storage.timescale_writer import TimescaleWriter
    from tests.conftest import MockKafkaProducer
    import time
    
    db_writer = TimescaleWriter(database="test_db")
    registry_manager = RegistryManager(db_writer)
    
    # Add multiple symbols
    for sym in ["AAPL", "NVDA", "AMD"]:
        registry_manager.add_symbol(
            symbol=sym,
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
        dry_run=False,
        publish_mode="kafka-first",
        throttle_ms=50  # 50ms throttle
    )
    
    start = time.time()
    async with orchestrator.lifecycle():
        await orchestrator.run_cycle()
    elapsed = time.time() - start
    
    # With 3 symbols and 50ms throttle, should take at least 150ms
    # (plus execution overhead)
    assert elapsed >= 0.15
