"""
Kafka Integration for Market Data Streaming
Producer/consumer implementation for real-time and batch data processing.
"""
import json
import logging
import time
import threading
from typing import Dict, List, Optional, Callable
from datetime import datetime

logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    Kafka producer for publishing market data events.
    Handles serialization, batching, and error recovery.
    """
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092',
                 topic: str = 'market.data.raw',
                 batch_size: int = 1000,
                 linger_ms: int = 100):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Default topic for publishing
            batch_size: Batch size for batching messages
            linger_ms: Linger time in ms before sending batch
        """
        try:
            from confluent_kafka import Producer
        except ImportError:
            logger.error("confluent-kafka not installed. Install with: pip install confluent-kafka")
            raise
        
        self.topic = topic
        self.batch_size = batch_size
        
        # Producer configuration
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'market-data-producer',
            'acks': 'all',  # Wait for all replicas
            'compression.type': 'snappy',  # Compress messages
            'linger.ms': linger_ms,  # Batch delay
            'batch.size': batch_size,
            'max.in.flight.requests.per.connection': 5,
            'enable.idempotence': True,  # Exactly-once semantics
        }
        
        self.producer = Producer(conf)
        self.pending = 0
        self.lock = threading.Lock()
        
        logger.info(f"Kafka producer initialized: {bootstrap_servers} -> {topic}")
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        with self.lock:
            self.pending -= 1
        
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def publish(self, message: Dict, topic: Optional[str] = None, key: Optional[str] = None):
        """
        Publish message to Kafka topic.
        
        Args:
            message: Message dictionary
            topic: Topic override (default: self.topic)
            key: Message key for partitioning
        """
        target_topic = topic or self.topic
        
        try:
            # Serialize message
            value = json.dumps(message, default=str).encode('utf-8')
            key_bytes = key.encode('utf-8') if key else None
            
            # Produce message
            with self.lock:
                self.pending += 1
            
            self.producer.produce(
                target_topic,
                value=value,
                key=key_bytes,
                callback=self._delivery_callback
            )
            
            # Poll for delivery reports (non-blocking)
            self.producer.poll(0)
            
        except BufferError:
            # Queue is full, wait and retry
            logger.warning("Producer queue full, waiting...")
            self.producer.flush(timeout=10)
            self.publish(message, topic, key)
        
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            with self.lock:
                self.pending -= 1
    
    def flush(self, timeout: float = 30):
        """
        Flush pending messages.
        
        Args:
            timeout: Flush timeout in seconds
        """
        logger.info(f"Flushing {self.pending} pending messages...")
        self.producer.flush(timeout)
        logger.info("Flush complete")
    
    def close(self):
        """Close producer"""
        self.flush()
        logger.info("Kafka producer closed")


class KafkaConsumer:
    """
    Kafka consumer for processing market data events.
    Handles deserialization, offset management, and error recovery.
    """
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092',
                 topics: List[str] = None,
                 group_id: str = 'market-data-consumer',
                 auto_offset_reset: str = 'earliest'):
        """
        Initialize Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topics: List of topics to subscribe
            group_id: Consumer group ID
            auto_offset_reset: Offset reset policy ('earliest' or 'latest')
        """
        try:
            from confluent_kafka import Consumer, KafkaError
        except ImportError:
            logger.error("confluent-kafka not installed. Install with: pip install confluent-kafka")
            raise
        
        self.topics = topics or ['market.data.raw']
        self.KafkaError = KafkaError
        
        # Consumer configuration
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'client.id': f'{group_id}-client',
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': False,  # Manual commit
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 10000,  # 10 seconds
        }
        
        self.consumer = Consumer(conf)
        self.consumer.subscribe(self.topics)
        self.running = False
        
        logger.info(f"Kafka consumer initialized: {bootstrap_servers} <- {self.topics}")
    
    def consume(self, callback: Callable[[Dict], None], timeout: float = 1.0):
        """
        Consume messages and process with callback.
        
        Args:
            callback: Function to process each message
            timeout: Poll timeout in seconds
        """
        self.running = True
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == self.KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    # Deserialize message
                    value = json.loads(msg.value().decode('utf-8'))
                    
                    # Process message
                    callback(value)
                    
                    # Commit offset after successful processing
                    self.consumer.commit(asynchronous=False)
                
                except Exception as e:
                    logger.error(f"Message processing error: {e}", exc_info=True)
                    # Don't commit on error - message will be reprocessed
        
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        
        finally:
            self.close()
    
    def stop(self):
        """Stop consuming messages"""
        self.running = False
    
    def close(self):
        """Close consumer"""
        self.consumer.close()
        logger.info("Kafka consumer closed")


class StreamProcessor:
    """
    Stream processor for real-time market data pipeline.
    Coordinates producers, consumers, and processing logic.
    """
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        """
        Initialize stream processor.
        
        Args:
            bootstrap_servers: Kafka broker addresses
        """
        self.bootstrap_servers = bootstrap_servers
        self.producers = {}
        self.consumers = {}
        self.threads = []
        
    def create_producer(self, name: str, topic: str) -> KafkaProducer:
        """
        Create and register a producer.
        
        Args:
            name: Producer name
            topic: Default topic
        
        Returns:
            KafkaProducer instance
        """
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=topic
        )
        self.producers[name] = producer
        return producer
    
    def create_consumer(self, name: str, topics: List[str], 
                       callback: Callable[[Dict], None],
                       group_id: Optional[str] = None) -> threading.Thread:
        """
        Create and register a consumer thread.
        
        Args:
            name: Consumer name
            topics: Topics to subscribe
            callback: Message processing function
            group_id: Consumer group ID
        
        Returns:
            Consumer thread
        """
        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            topics=topics,
            group_id=group_id or f'processor-{name}'
        )
        self.consumers[name] = consumer
        
        # Create consumer thread
        thread = threading.Thread(
            target=consumer.consume,
            args=(callback,),
            name=f'kafka-consumer-{name}',
            daemon=True
        )
        self.threads.append(thread)
        
        return thread
    
    def start(self):
        """Start all consumer threads"""
        for thread in self.threads:
            thread.start()
            logger.info(f"Started {thread.name}")
    
    def stop(self):
        """Stop all consumers and flush producers"""
        # Stop consumers
        for name, consumer in self.consumers.items():
            logger.info(f"Stopping consumer: {name}")
            consumer.stop()
        
        # Flush producers
        for name, producer in self.producers.items():
            logger.info(f"Flushing producer: {name}")
            producer.flush()
        
        # Wait for threads
        for thread in self.threads:
            thread.join(timeout=5)
        
        logger.info("Stream processor stopped")
