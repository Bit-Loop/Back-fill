"""
Data Ingestion Engine
Unified Kappa architecture pipeline for both historical and live data.
Single processing path ensures consistency between backfill and streaming.
"""
import logging
from typing import List, Dict

from .data_sources import MarketDataSource
from .message_bus import MessageBus

logger = logging.getLogger(__name__)


class DataIngestionEngine:
    """
    Unified ingestion pipeline for both historical and live data (Kappa architecture).
    Single processing path ensures consistency between backfill and streaming.
    
    Flow: DataSource → Processor Chain → Storage (idempotent upsert) → MessageBus
    """
    
    def __init__(self, processors: List, storage, 
                 message_bus: MessageBus, batch_size: int = 1000):
        """
        Initialize data ingestion engine.
        
        Args:
            processors: List of processor instances (e.g., IncrementalIndicatorProcessor)
            storage: TimescaleWriter instance
            message_bus: MessageBus instance for publishing updates
            batch_size: Batch size for database writes
        """
        self.processors = processors
        self.storage = storage
        self.message_bus = message_bus
        self.batch_size = batch_size
        self.batch_buffer = []
        
    def ingest(self, data_source: MarketDataSource, mode: str = 'batch'):
        """
        Ingest data from source through unified pipeline.
        
        Args:
            data_source: MarketDataSource instance (Historical or Live)
            mode: 'batch' for historical backfill, 'stream' for live data
        """
        logger.info(f"Starting ingestion in {mode} mode...")
        
        bars_processed = 0
        
        try:
            for bar in data_source:
                # Process through indicator chain
                enriched_bar = bar
                for processor in self.processors:
                    enriched_bar = processor.process(enriched_bar)
                
                # Add to batch buffer
                self.batch_buffer.append(enriched_bar)
                
                # Flush batch when full
                if len(self.batch_buffer) >= self.batch_size:
                    self._flush_batch()
                    bars_processed += self.batch_size
                    
                    if mode == 'batch' and bars_processed % 10000 == 0:
                        logger.info(f"Processed {bars_processed:,} bars...")
                
                # In stream mode, publish immediately
                if mode == 'stream':
                    self._publish_bar(enriched_bar)
            
            # Flush remaining bars
            if self.batch_buffer:
                self._flush_batch()
                bars_processed += len(self.batch_buffer)
            
            logger.info(f"Ingestion complete: {bars_processed:,} bars processed")
        
        except Exception as e:
            logger.error(f"Ingestion error: {e}", exc_info=True)
            raise
    
    def _flush_batch(self):
        """Write buffered bars to database"""
        if not self.batch_buffer:
            return
        
        try:
            # Group by symbol and timeframe
            grouped = {}
            for bar in self.batch_buffer:
                key = (bar['symbol'], bar['timeframe'])
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(bar)
            
            # Write each group
            for (symbol, timeframe), bars in grouped.items():
                self.storage.write_ohlcv_bars(symbol, bars, timeframe=timeframe)
            
            # Clear buffer
            self.batch_buffer.clear()
        
        except Exception as e:
            logger.error(f"Batch flush error: {e}")
            raise
    
    def _publish_bar(self, bar: Dict):
        """Publish bar to message bus"""
        try:
            topic = f"bars:{bar['symbol']}:{bar['timeframe']}"
            self.message_bus.publish(topic, bar)
        except Exception as e:
            logger.error(f"Publish error: {e}")
