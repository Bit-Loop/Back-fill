"""Streaming utilities used by the scraper service."""
from __future__ import annotations

import logging
import threading
from typing import Callable, Dict, List, Optional

from scraper.pipeline import DataIngestionEngine
from scraper.pipeline.message_bus import MessageBus
from scraper.pipeline.data_sources import LiveDataSource
from scraper.processors import IncrementalIndicatorProcessor

logger = logging.getLogger(__name__)


class LiveStreamCoordinator:
    """Coordinates live streaming ingestion for one or more symbols."""

    def __init__(
        self,
        symbols: List[str],
        timeframe: str,
        api_key: str,
        db_writer,
        message_bus: MessageBus,
        ownership_callback: Optional[Callable] = None,
    ) -> None:
        self.symbols = symbols
        self.timeframe = timeframe
        self.api_key = api_key
        self.db_writer = db_writer
        self.message_bus = message_bus
        self.ownership_callback = ownership_callback

        self._threads: List[threading.Thread] = []
        self._sources: Dict[str, LiveDataSource] = {}
        self._lock = threading.RLock()
        self._shutdown = threading.Event()

    def start(self) -> None:
        if not self.symbols:
            raise ValueError("No symbols provided for live streaming")

        for symbol in self.symbols:
            source = LiveDataSource(symbol, self.api_key, timeframe=self.timeframe)
            with self._lock:
                self._sources[symbol] = source

            thread = threading.Thread(
                target=self._run_stream,
                args=(symbol, source),
                name=f"stream-{symbol}-{self.timeframe}",
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()

        logger.info("Live streaming started for %s (%s)", self.symbols, self.timeframe)

    def stop(self) -> None:
        self._shutdown.set()
        with self._lock:
            sources = list(self._sources.values())

        for source in sources:
            try:
                source.stop()
            except Exception:
                logger.debug("Error stopping live source", exc_info=True)

        for thread in self._threads:
            thread.join(timeout=5)

        if self.ownership_callback:
            try:
                self.ownership_callback(self.db_writer)
            except Exception:
                logger.debug("Writer release callback failed", exc_info=True)
        logger.info("Live streaming stopped")

    def _run_stream(self, symbol: str, source: LiveDataSource) -> None:
        processor = IncrementalIndicatorProcessor()
        engine = DataIngestionEngine(
            processors=[processor],
            storage=self.db_writer,
            message_bus=self.message_bus,
            batch_size=1,
        )

        try:
            engine.ingest(source, mode='stream')
        except Exception:
            if not self._shutdown.is_set():
                logger.exception("Live stream failed for %s", symbol)
        finally:
            source.stop()
