"""Scraper entry point supporting CLI execution and a daemonised API."""
from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional, cast

from dotenv import load_dotenv

# Ensure project root is available for imports
sys.path.insert(0, str(Path(__file__).parent))

from common.config.settings import BackfillConfig
from scraper.clients.polygon_client import PolygonClient
from scraper.clients.reference_client import ReferenceClient
from scraper.service import ScraperService
from scraper.service.models import BackfillRequest, BackfillSource, JobState, StreamRequest

# ---------------------------------------------------------------------------
# Environment & logging configuration
# All configuration loaded from .env file:
#   - POLYGON_API_KEY: Polygon.io API key (required)
#   - TIMESCALE_HOST/PORT/DB/USER/PASSWORD: Database connection
#   - REDIS_HOST/PORT/PASSWORD: Redis connection for streaming
#   - LOG_DIR: Directory for log files (default: ./logs)
#   - LOG_LEVEL: Logging level (default: INFO)
# ---------------------------------------------------------------------------
load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "./logs")).resolve()
LOG_DIR.mkdir(exist_ok=True)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(threadName)-15s] %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "backfill.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill and streaming controller")
    parser.add_argument("--tickers", type=str, help="Comma-separated list of tickers")
    parser.add_argument("--all", action="store_true", help="Backfill all active tickers")
    parser.add_argument("--limit", type=int, help="Limit number of tickers when using --all")
    parser.add_argument("--years", type=int, default=5, help="Years of historical data")
    parser.add_argument("--flatfiles", action="store_true", help="Use Polygon flat files when available")
    parser.add_argument(
        "--source",
        type=str,
        choices=["rest", "flatfile", "snapshot", "hybrid"],
        default=os.getenv("SCRAPER_SOURCE", "rest"),
        help="Backfill data source strategy",
    )
    parser.add_argument("--publish-kafka", action="store_true", help="Publish ingested data to Kafka")
    parser.add_argument("--kafka-topic", type=str, default=os.getenv("KAFKA_TOPIC", "chronox.market.snapshots"), help="Kafka topic for publishing")
    parser.add_argument("--kafka-brokers", type=str, default=os.getenv("KAFKA_BROKERS", ""), help="Kafka bootstrap servers")
    parser.add_argument("--no-redis-publish", action="store_true", help="Disable Redis publication during backfill")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--skip-reference", action="store_true", help="Skip reference data phase")
    parser.add_argument("--skip-corporate", action="store_true", help="Skip corporate actions phase")
    parser.add_argument("--skip-daily", action="store_true", help="Skip daily bars phase")
    parser.add_argument("--skip-minute", action="store_true", help="Skip minute bars phase")
    parser.add_argument("--skip-news", action="store_true", help="Skip news phase")
    parser.add_argument("--stream", action="store_true", help="Stream live data while backfilling")
    parser.add_argument("--stream-only", action="store_true", help="Run streaming without backfill")
    parser.add_argument("--stream-tickers", type=str, help="Tickers to stream (overrides backfill tickers)")
    parser.add_argument("--stream-timeframe", type=str, default="1m", help="Streaming timeframe")
    parser.add_argument("--daemon", action="store_true", help="Run as FastAPI daemon")
    parser.add_argument("--host", type=str, default=os.getenv("SCRAPER_HOST", "127.0.0.1"), help="Daemon host")
    parser.add_argument("--port", type=int, default=int(os.getenv("SCRAPER_PORT", "9000")), help="Daemon port")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for item in values:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def _fetch_all_tickers(config: BackfillConfig, limit: Optional[int]) -> List[str]:
    client = PolygonClient(cast(str, config.polygon.api_key))
    reference = ReferenceClient(client)
    try:
        tickers = reference.get_tickers(limit=limit or 1000)
        return [item["ticker"] for item in tickers if item.get("ticker")]
    finally:
        client.close()


def _wait_for_job_completion(service: ScraperService, job_id: str) -> None:
    while True:
        job = service.get_job(job_id)
        if not job:
            logger.error("Job %s not found", job_id)
            break

        if job.state in {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}:
            if job.state is JobState.COMPLETED:
                logger.info("Backfill job %s completed", job_id)
            elif job.state is JobState.FAILED:
                logger.error("Backfill job %s failed: %s", job_id, job.error)
            else:
                logger.warning("Backfill job %s cancelled", job_id)
            break

        time.sleep(1)


def _run_daemon(host: str, port: int) -> None:
    try:
        uvicorn = importlib.import_module("uvicorn")
    except ModuleNotFoundError as exc:
        logger.error("uvicorn is required for daemon mode. Install via 'pip install uvicorn'.")
        raise SystemExit(1) from exc

    from scraper.service.api import app

    uvicorn.run(app, host=host, port=port, log_level=LOG_LEVEL.lower())


# ---------------------------------------------------------------------------
# Main routine
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.daemon:
        _run_daemon(args.host, args.port)
        return

    config = BackfillConfig.default()

    if not config.polygon.api_key:
        logger.error("POLYGON_API_KEY not configured; aborting")
        sys.exit(1)

    need_backfill = not args.stream_only
    enable_stream = args.stream or args.stream_only

    backfill_tickers: List[str] = []
    if args.all:
        logger.info("Discovering active tickers from Polygon API...")
        backfill_tickers = _fetch_all_tickers(config, args.limit)
        if args.limit:
            backfill_tickers = backfill_tickers[: args.limit]
    elif args.tickers:
        backfill_tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif need_backfill:
        logger.error("Specify --tickers or --all when running a backfill")
        sys.exit(1)

    stream_symbols: List[str] = []
    if enable_stream:
        if args.stream_tickers:
            stream_symbols = [t.strip().upper() for t in args.stream_tickers.split(",") if t.strip()]
        else:
            stream_symbols = list(backfill_tickers)

        stream_symbols = _dedupe_preserve_order(stream_symbols)
        if not stream_symbols:
            logger.error("Streaming requested but no tickers available")
            sys.exit(1)

    source_choice = args.source
    if args.flatfiles:
        source_choice = "flatfile"

    service = ScraperService(config=config)
    job_id: Optional[str] = None

    try:
        if enable_stream:
            stream_req = StreamRequest(tickers=stream_symbols, timeframe=args.stream_timeframe)
            service.start_stream(stream_req)

        if need_backfill:
            request = BackfillRequest(
                tickers=backfill_tickers,
                years=args.years,
                use_flatfiles=args.flatfiles or source_choice == "flatfile",
                debug=args.debug,
                skip_reference=args.skip_reference,
                skip_corporate=args.skip_corporate,
                skip_daily=args.skip_daily,
                skip_minute=args.skip_minute,
                skip_news=args.skip_news,
                source=BackfillSource(source_choice),
                publish_kafka=args.publish_kafka,
                publish_redis=not args.no_redis_publish,
                kafka_topic=args.kafka_topic,
                kafka_bootstrap=args.kafka_brokers or None,
            )
            status = service.start_backfill(request)
            job_id = status.job_id
            _wait_for_job_completion(service, job_id)

        if enable_stream:
            logger.info("Streaming active. Press Ctrl+C to stop.")
            while True:
                status = service.stream_status()
                if not status.active:
                    logger.warning("Streaming stopped unexpectedly")
                    break
                time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    finally:
        if job_id:
            job = service.get_job(job_id)
            if job and job.state is JobState.RUNNING:
                service.cancel_job(job_id)
        service.shutdown()


if __name__ == "__main__":
    main()
