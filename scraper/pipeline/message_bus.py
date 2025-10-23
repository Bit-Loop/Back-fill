"""
Message Bus Implementations
Abstractions for publishing/subscribing to market data updates.
Supports Redis Pub/Sub, Kafka bridge, and in-memory testing.
"""
import json
import logging
import os
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from datetime import datetime
import numpy as np
import logging
import json
import os
import threading
from abc import ABC, abstractmethod
from typing import Dict, List
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)


class MessageBus(ABC):
    """
    Abstract message bus interface.
    Allows swapping between Redis, Kafka, or in-memory implementations.
    """
    
    @abstractmethod
    def publish(self, topic: str, message: Dict):
        """Publish message to topic"""
        pass
    
    @abstractmethod
    def subscribe(self, topic: str, callback):
        """Subscribe to topic with callback"""
        pass


class RedisMessageBus(MessageBus):
    """
    Redis-based message bus using Pub/Sub for real-time GUI updates.
    Publishes to chronox:gui:updates:{symbol}:{tf} for frontend consumption.
    Supports password authentication.
    """
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, password: Optional[str] = None):
        """
        Initialize Redis message bus.
        
        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            password: Redis password (optional)
        """
        try:
            import redis
        except ImportError:
            logger.error("redis package not installed. Install with: pip install redis")
            raise
        
        self.redis_client = redis.Redis(
            host=host, 
            port=port, 
            db=db, 
            password=password,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        self.pubsub = None
        
        # Test connection
        self.redis_client.ping()
        logger.info(f"✓ Redis connected: {host}:{port}")
    
    def _serialize_value(self, obj):
        """Convert datetime and other non-serializable objects to JSON-compatible types"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif hasattr(obj, '__dict__'):
            return str(obj)
        return obj
    
    def _prepare_message(self, message: Dict) -> Dict:
        """Recursively convert message values to JSON-serializable types"""
        cleaned = {}
        for key, value in message.items():
            if isinstance(value, dict):
                cleaned[key] = self._prepare_message(value)
            elif isinstance(value, (list, tuple)):
                cleaned[key] = [self._serialize_value(item) for item in value]
            else:
                cleaned[key] = self._serialize_value(value)
        return cleaned
        
    def publish(self, topic: str, message: Dict):
        """Publish JSON message to Redis channel with GUI-compatible naming"""
        try:
            # Extract symbol and timeframe from topic
            if topic.startswith('bars:'):
                parts = topic.split(':')
                if len(parts) >= 3:
                    symbol = parts[1]
                    tf = parts[2]
                    gui_channel = f"chronox:gui:updates:{symbol}:{tf}"
                else:
                    gui_channel = topic
            else:
                gui_channel = topic
            
            # Clean message for JSON serialization
            cleaned_message = self._prepare_message(message)
            
            # Add composite key for idempotency
            if 'symbol' in cleaned_message and 'timeframe' in cleaned_message and 'timestamp' in cleaned_message:
                cleaned_message['k'] = f"{cleaned_message['symbol']}|{cleaned_message['timeframe']}|{cleaned_message['timestamp']}"
            
            self.redis_client.publish(gui_channel, json.dumps(cleaned_message))
        except Exception as e:
            logger.error(f"Redis publish error: {e}")
            logger.debug(f"Failed message: {message}")
    
    def subscribe(self, topic: str, callback):
        """Subscribe to Redis channel (blocking)"""
        if self.pubsub is None:
            import redis
            self.pubsub = self.redis_client.pubsub()
        
        self.pubsub.subscribe(topic)
        
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    callback(data)
                except Exception as e:
                    logger.error(f"Redis subscribe callback error: {e}")


class KafkaToRedisBridge(threading.Thread):
    """
    Background bridge thread: Kafka → Redis
    
    Consumes enriched market data from Kafka topic 'chronox.market.enriched'
    and republishes to Redis Pub/Sub for GUI consumption.
    
    Features:
    - Offset checkpointing to ./.chronox_offsets.json (resume on restart)
    - Idempotent DB writes (ON CONFLICT DO UPDATE)
    - Concurrent backfill + streaming (independent of backfill jobs)
    - Manual offset commit after successful DB write
    
    Channel naming: chronox:gui:updates:{symbol}:{timeframe}
    Message schema includes composite key 'k' for idempotency
    """
    
    def __init__(self, kafka_brokers: str = 'localhost:9092', 
                 redis_bus: RedisMessageBus = None,
                 storage = None,
                 checkpoint_path: str = './.chronox_offsets.json'):
        """
        Initialize Kafka to Redis bridge.
        
        Args:
            kafka_brokers: Kafka broker addresses
            redis_bus: RedisMessageBus instance
            storage: TimescaleWriter instance (optional)
            checkpoint_path: Path to offset checkpoint file
        """
        super().__init__(daemon=True)
        self.kafka_brokers = kafka_brokers
        self.redis_bus = redis_bus
        self.storage = storage
        self.checkpoint_path = checkpoint_path
        self.running = False
        self.consumer = None
        self.offsets = {}
        
    def _load_offsets(self):
        """Load offset checkpoint from disk"""
        try:
            if os.path.exists(self.checkpoint_path):
                with open(self.checkpoint_path, 'r') as f:
                    data = json.load(f)
                    self.offsets = {tuple(k.split('|')): v for k, v in data.items()}
                    logger.info(f"✓ Loaded {len(self.offsets)} offset checkpoints")
        except Exception as e:
            logger.warning(f"Failed to load offsets: {e}")
            self.offsets = {}
    
    def _save_offsets(self):
        """Persist offset checkpoint to disk"""
        try:
            data = {'|'.join(k): v for k, v in self.offsets.items()}
            with open(self.checkpoint_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save offsets: {e}")
    
    def run(self):
        """Main Kafka consumer loop"""
        try:
            from confluent_kafka import Consumer, KafkaError, TopicPartition
        except ImportError:
            logger.warning("Kafka bridge not started - confluent-kafka not installed")
            return
        
        self.running = True
        self._load_offsets()
        
        conf = {
            'bootstrap.servers': self.kafka_brokers,
            'group.id': 'chronox_gui_bridge',
            'enable.auto.commit': False,
            'auto.offset.reset': 'latest',
            'session.timeout.ms': 6000,
            'api.version.request': True
        }
        
        try:
            self.consumer = Consumer(conf)
            topic = 'chronox.market.enriched'
            self.consumer.subscribe([topic])
            
            logger.info(f"✓ Kafka→Redis bridge started: {topic}")
            
            # Seek to checkpointed offsets
            if self.offsets:
                assignment = self.consumer.assignment()
                if assignment:
                    for tp in assignment:
                        key = (tp.topic, tp.partition)
                        if key in self.offsets:
                            offset = self.offsets[key] + 1
                            self.consumer.seek(TopicPartition(tp.topic, tp.partition, offset))
                            logger.info(f"  Resuming {tp.topic}[{tp.partition}] at offset {offset}")
            
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue
                
                try:
                    bar_data = json.loads(msg.value().decode('utf-8'))
                    
                    # Idempotent DB write
                    if self.storage:
                        self.storage.write_ohlcv_bars(
                            bar_data.get('symbol'),
                            [bar_data],
                            timeframe=bar_data.get('timeframe', '1m')
                        )
                    
                    # Publish to Redis
                    if self.redis_bus:
                        symbol = bar_data.get('symbol', 'UNKNOWN')
                        tf = bar_data.get('timeframe', '1m')
                        ts = bar_data.get('timestamp', '')
                        
                        bar_data['k'] = f"{symbol}|{tf}|{ts}"
                        gui_channel = f"chronox:gui:updates:{symbol}:{tf}"
                        self.redis_bus.publish(gui_channel, bar_data)
                    
                    # Checkpoint offset
                    tp_key = (msg.topic(), msg.partition())
                    self.offsets[tp_key] = msg.offset()
                    self.consumer.commit(asynchronous=False)
                    
                    if msg.offset() % 100 == 0:
                        self._save_offsets()
                
                except Exception as e:
                    logger.error(f"Bridge processing error: {e}", exc_info=True)
        
        except Exception as e:
            logger.error(f"Kafka bridge error: {e}", exc_info=True)
        
        finally:
            if self.consumer:
                self.consumer.close()
            self._save_offsets()
            logger.info("✓ Kafka→Redis bridge stopped")
    
    def stop(self):
        """Stop bridge gracefully"""
        self.running = False
        self.join(timeout=5)


class InMemoryMessageBus(MessageBus):
    """
    In-memory message bus for testing or fallback.
    Uses Python lists for inter-thread communication.
    """
    
    def __init__(self):
        """Initialize in-memory message bus"""
        self.subscribers = {}
        
    def publish(self, topic: str, message: Dict):
        """Publish to in-memory subscribers"""
        if topic in self.subscribers:
            for callback in self.subscribers[topic]:
                try:
                    callback(message)
                except Exception as e:
                    logger.error(f"InMemory callback error: {e}")
    
    def subscribe(self, topic: str, callback):
        """Subscribe callback to topic"""
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        self.subscribers[topic].append(callback)
