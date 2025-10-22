"""
Scraper Main Entry Point
Orchestrates historical data backfill and real-time data ingestion.

Usage:
    python scraper.py --tickers AMD,NVDA --years 2 --flatfiles
    python scraper.py --all --limit 50
"""
import os
import sys
import argparse
import logging
import threading
import time
from pathlib import Path
from typing import Dict, List

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from common.config.settings import BackfillConfig
from dotenv import load_dotenv
from scraper.pipeline import DataIngestionEngine, InMemoryMessageBus, RedisMessageBus
from scraper.pipeline.data_sources import LiveDataSource
from scraper.processors import IncrementalIndicatorProcessor

# Load environment as early as possible so logging picks up overrides
load_dotenv()

# Create logs directory if needed
LOG_DIR = Path(os.getenv('LOG_DIR', './logs')).resolve()
LOG_DIR.mkdir(exist_ok=True)

# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s [%(threadName)-15s] %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Backfill historical market data')
    parser.add_argument('--tickers', type=str, help='Comma-separated list of tickers')
    parser.add_argument('--all', action='store_true', help='Backfill all tickers')
    parser.add_argument('--limit', type=int, help='Limit number of tickers (with --all)')
    parser.add_argument('--years', type=int, default=5, help='Years of historical data')
    parser.add_argument('--flatfiles', action='store_true', help='Use S3 flat files')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--skip-reference', action='store_true', help='Skip Phase 1')
    parser.add_argument('--skip-corporate', action='store_true', help='Skip Phase 2')
    parser.add_argument('--skip-daily', action='store_true', help='Skip Phase 3')
    parser.add_argument('--skip-minute', action='store_true', help='Skip Phase 4')
    parser.add_argument('--skip-news', action='store_true', help='Skip Phase 5')
    parser.add_argument('--stream', action='store_true', help='Stream live data while backfilling')
    parser.add_argument('--stream-only', action='store_true', help='Only stream live data without backfill')
    parser.add_argument('--stream-tickers', type=str, help='Comma-separated tickers to stream (defaults to backfill tickers)')
    parser.add_argument('--stream-timeframe', type=str, default='1m', help='Timeframe for live streaming (default: 1m)')
    return parser.parse_args()


class LiveStreamCoordinator:
    """Manage live Polygon streams alongside backfill jobs."""

    def __init__(
        self,
        symbols: List[str],
        timeframe: str,
        api_key: str,
        db_writer,
        message_bus
    ) -> None:
        self.symbols = symbols
        self.timeframe = timeframe
        self.api_key = api_key
        self.db_writer = db_writer
        self.message_bus = message_bus
        self._threads: List[threading.Thread] = []
        self._sources: Dict[str, LiveDataSource] = {}
        self._lock = threading.Lock()
        self._shutdown = threading.Event()

    def start(self) -> None:
        if not self.symbols:
            logger.warning("No symbols provided for live streaming; skipping stream startup")
            return

        for symbol in self.symbols:
            source = LiveDataSource(symbol, self.api_key, timeframe=self.timeframe)
            with self._lock:
                self._sources[symbol] = source

            thread = threading.Thread(
                target=self._run_stream,
                args=(symbol, source),
                name=f'stream-{symbol}-{self.timeframe}',
                daemon=True
            )
            self._threads.append(thread)
            thread.start()

        logger.info(
            "Live streaming started for %s symbols on timeframe %s",
            len(self.symbols),
            self.timeframe
        )

    def _run_stream(self, symbol: str, source: LiveDataSource) -> None:
        processor = IncrementalIndicatorProcessor()
        engine = DataIngestionEngine(
            processors=[processor],
            storage=self.db_writer,
            message_bus=self.message_bus,
            batch_size=1  # Flush every live bar so DB and GUI stay current
        )

        try:
            engine.ingest(source, mode='stream')
        except Exception as exc:  # pragma: no cover - defensive logging
            if not self._shutdown.is_set():
                logger.error("Live stream for %s failed: %s", symbol, exc, exc_info=True)
        finally:
            source.stop()

    def stop(self) -> None:
        self._shutdown.set()
        with self._lock:
            sources = list(self._sources.values())

        for source in sources:
            try:
                source.stop()
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("Error stopping live source: %s", exc, exc_info=True)

        for thread in self._threads:
            thread.join(timeout=5)

        logger.info("Live streaming coordinator stopped")


def main():
    """Main scraper entry point"""
    # Parse arguments
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create configuration
    config = BackfillConfig.default()
    config.debug = args.debug
    
    # Import after configuration
    from scraper.orchestrator import BackfillOrchestrator
    from common.storage import TimescaleWriter

    need_backfill = not args.stream_only
    enable_stream = args.stream or args.stream_only

    # Determine tickers for backfill
    backfill_tickers: List[str] = []
    if args.all:
        logger.info("Fetching all active tickers...")
        backfill_tickers = []  # TODO: Implement retrieval from reference data
        if args.limit:
            backfill_tickers = backfill_tickers[:args.limit]
    elif args.tickers:
        backfill_tickers = [t.strip().upper() for t in args.tickers.split(',') if t.strip()]
    elif need_backfill:
        logger.error("Must specify --tickers or --all when running a backfill")
        sys.exit(1)

    # Determine symbols to stream
    stream_symbols: List[str] = []
    if enable_stream:
        if args.stream_tickers:
            stream_symbols = [t.strip().upper() for t in args.stream_tickers.split(',') if t.strip()]
        else:
            stream_symbols = list(backfill_tickers)

        seen = set()
        ordered_symbols: List[str] = []
        for sym in stream_symbols:
            if sym not in seen:
                seen.add(sym)
                ordered_symbols.append(sym)
        stream_symbols = ordered_symbols

        if not stream_symbols:
            logger.error("Streaming requested but no tickers provided. Use --stream-tickers or supply --tickers/--all.")
            sys.exit(1)

    # Validate API key for both backfill and streaming
    if not config.polygon.api_key:
        logger.error("POLYGON_API_KEY not set in environment")
        sys.exit(1)

    db_writer = None
    orchestrator = None
    stream_coordinator = None
    exit_code = 0

    try:
        db_writer = TimescaleWriter(
            host=config.database.host,
            port=config.database.port,
            database=config.database.database,
            user=config.database.user,
            password=config.database.password
        )

        if need_backfill:
            orchestrator = BackfillOrchestrator(
                polygon_api_key=config.polygon.api_key,
                db_writer=db_writer,
                years_back=args.years,
                use_flatfiles=args.flatfiles,
                debug=args.debug,
                skip_reference=args.skip_reference,
                skip_corporate=args.skip_corporate,
                skip_daily=args.skip_daily,
                skip_minute=args.skip_minute,
                skip_news=args.skip_news
            )
            logger.info("Starting backfill for %s tickers: %s", len(backfill_tickers), backfill_tickers)

        if enable_stream:
            try:
                message_bus = RedisMessageBus(
                    host=config.redis.host,
                    port=config.redis.port,
                    db=config.redis.db,
                    password=config.redis.password
                )
            except Exception as exc:
                logger.warning(
                    "Redis message bus unavailable (%s); using in-memory message bus instead.",
                    exc
                )
                message_bus = InMemoryMessageBus()

            stream_coordinator = LiveStreamCoordinator(
                symbols=stream_symbols,
                timeframe=args.stream_timeframe,
                api_key=config.polygon.api_key,
                db_writer=db_writer,
                message_bus=message_bus
            )
            stream_coordinator.start()

        if orchestrator:
            results = orchestrator.run_full_backfill(backfill_tickers)
            logger.info("Backfill complete: %s", results)

        if enable_stream:
            if orchestrator:
                logger.info("Backfill finished; live streaming continues. Press Ctrl+C to stop.")
            else:
                logger.info("Live streaming running. Press Ctrl+C to stop.")

            while True:
                time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("Scraper failed: %s", exc, exc_info=True)
        exit_code = 1
    finally:
        if stream_coordinator:
            stream_coordinator.stop()
        if orchestrator:
            orchestrator.close()
        if db_writer:
            db_writer.close()

    if exit_code:
        sys.exit(exit_code)


if __name__ == '__main__':
    main()
