#!/usr/bin/env python3
"""
ChronoX Ingestion Service - CLI Entry Point

Production-grade historical data ingestion with:
- Symbol registry-driven execution
- Kafka-first, DB-consumer architecture
- Structured JSON logging
- Error recovery and gap detection
"""
import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

from storage.timescale.writer import TimescaleWriter
from scraper.registry.manager import RegistryManager
from scraper.registry.models import IngestionStrategy, IngestionStatus, SymbolRegistry
from core.orchestrator.unified import UnifiedOrchestrator
from scraper.utils.structured_logging import get_logger


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description='ChronoX Historical Data Ingestion Service',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run snapshot test for single symbol
  %(prog)s --test-snapshot AAPL
  
  # Run in daemon mode with gap detection
  %(prog)s --daemon
  
  # Dry run with debug logging
  %(prog)s --dry-run --log-level DEBUG
  
  # Execute registry strategies once
  %(prog)s
        """
    )
    
    parser.add_argument(
        '--test-snapshot',
        type=str,
        metavar='SYMBOL',
        help='Test snapshot fetch and validation for symbol (exits after test)'
    )
    
    parser.add_argument(
        '--daemon',
        action='store_true',
        help='Run in daemon mode with scheduler (gap detection, periodic refresh)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate execution without writes (useful for testing)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--db-host',
        type=str,
        default=os.getenv('TIMESCALE_HOST', 'localhost'),
        help='TimescaleDB host (default: $TIMESCALE_HOST or localhost)'
    )
    
    parser.add_argument(
        '--db-port',
        type=int,
        default=int(os.getenv('TIMESCALE_PORT', '5432')),
        help='TimescaleDB port (default: $TIMESCALE_PORT or 5432)'
    )
    
    parser.add_argument(
        '--db-name',
        type=str,
        default=os.getenv('TIMESCALE_DB', 'chronox'),
        help='TimescaleDB database (default: $TIMESCALE_DB or chronox)'
    )
    
    parser.add_argument(
        '--db-user',
        type=str,
        default=os.getenv('TIMESCALE_USER', 'chronox'),
        help='TimescaleDB user (default: $TIMESCALE_USER or chronox)'
    )
    
    parser.add_argument(
        '--db-password',
        type=str,
        default=os.getenv('TIMESCALE_PASSWORD', ''),
        help='TimescaleDB password (default: $TIMESCALE_PASSWORD)'
    )
    
    parser.add_argument(
        '--polygon-api-key',
        type=str,
        default=os.getenv('POLYGON_API_KEY', ''),
        help='Polygon.io API key (default: $POLYGON_API_KEY)'
    )
    
    parser.add_argument(
        '--strategy',
        type=str,
        help='Override registry strategy (e.g., flatpack+api, snapshot+stream, flatpack+api+snapshot+stream)'
    )
    
    parser.add_argument(
        '--snapshot-method',
        type=str,
        choices=['rest', 'agg', 'snapshot'],
        default='snapshot',
        help='Snapshot fetch method (default: snapshot)'
    )
    
    parser.add_argument(
        '--publish-mode',
        type=str,
        choices=['kafka-first', 'db-first'],
        default='kafka-first',
        help='Publishing order: kafka-first (default) or db-first'
    )
    
    parser.add_argument(
        '--no-stream',
        action='store_true',
        help='Disable stream phase (run history + snapshot only)'
    )
    
    parser.add_argument(
        '--symbols',
        type=str,
        help='Comma-separated symbol list (overrides registry, e.g., AAPL,NVDA,AMD)'
    )
    
    parser.add_argument(
        '--timeframe',
        type=str,
        default='1m',
        help='Override timeframe (default: 1m)'
    )
    
    parser.add_argument(
        '--throttle-ms',
        type=int,
        default=0,
        help='Throttle publish rate in milliseconds (default: 0 = no throttle)'
    )
    
    return parser.parse_args()


async def test_snapshot_fetch(
    symbol: str,
    polygon_api_key: str,
    db_writer: TimescaleWriter,
    logger: Any
) -> int:
    """Test snapshot fetch and validation for symbol."""
    from scraper.clients import PolygonClient
    from scraper.pipeline.snapshot_validator import (
        validate_snapshot,
        validate_snapshot_timestamp
    )
    
    logger.info("snapshot_test_started", symbol=symbol)
    
    try:
        from scraper.clients.snapshot_client import SnapshotClient
        
        polygon_client = PolygonClient(polygon_api_key)
        snapshot_client = SnapshotClient(polygon_client)
        
        snapshots = snapshot_client.get_snapshots_for_tickers([symbol])
        
        if not snapshots:
            logger.error("snapshot_fetch_failed", symbol=symbol, error="Empty response")
            return 1
        
        snapshot = snapshots[0]
        
        validation = validate_snapshot({'ticker': snapshot})
        if not validation.get('valid'):
            logger.error("snapshot_validation_failed", symbol=symbol, error=validation.get('error'))
            return 1
        
        day = snapshot.get('day', {})
        if not day:
            logger.error("snapshot_missing_day_data", symbol=symbol)
            return 1
        logger.info(
            "snapshot_test_passed",
            symbol=symbol,
            open=day.get('o'),
            high=day.get('h'),
            low=day.get('l'),
            close=day.get('c'),
            volume=day.get('v')
        )
        
        polygon_client.close()
        
        return 0
        
    except Exception as e:
        logger.error("snapshot_test_exception", symbol=symbol, error=str(e))
        return 1


async def run_one_cycle(
    polygon_api_key: str,
    db_writer: TimescaleWriter,
    registry_manager: RegistryManager,
    dry_run: bool,
    publish_mode: str,
    throttle_ms: int,
    snapshot_method: str,
    enable_stream: bool,
    symbol_override: Optional[List[str]],
    strategy_override: Optional[str],
    timeframe_override: str,
    logger: Any
) -> None:
    """Execute one orchestration cycle with flag overrides."""
    orchestrator = UnifiedOrchestrator(
        polygon_api_key=polygon_api_key,
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=dry_run,
        publish_mode=publish_mode,
        throttle_ms=throttle_ms,
        snapshot_method=snapshot_method,
        enable_stream=enable_stream
    )
    
    await orchestrator.resume_failed_jobs()
    
    async with orchestrator.lifecycle():
        if symbol_override:
            filtered_symbols = []
            for sym in symbol_override:
                entry = registry_manager.get_symbol(sym)
                if entry and entry.enabled:
                    filtered_symbols.append(entry)
                else:
                    logger.warn("symbol_not_found_or_disabled", symbol=sym)
            
            results = {}
            for entry in filtered_symbols:
                result = await orchestrator._execute_symbol(entry)
                results[entry.symbol] = result
        else:
            results = await orchestrator.run_cycle()
        
        success_count = sum(1 for r in results.values() if r.get('status') == 'success')
        error_count = sum(1 for r in results.values() if r.get('status') == 'error')
        
        logger.info(
            "cycle_completed",
            total=len(results),
            success=success_count,
            errors=error_count,
            symbol_override=symbol_override is not None
        )


async def run_daemon_mode(
    polygon_api_key: str,
    db_writer: TimescaleWriter,
    registry_manager: RegistryManager,
    dry_run: bool,
    logger: Any
) -> None:
    """Run in daemon mode with scheduler."""
    from core.orchestrator.legacy import BackfillOrchestrator
    from scraper.scheduler import IngestionScheduler
    
    logger.info("daemon_mode_started")
    
    orchestrator_base = BackfillOrchestrator(polygon_api_key, db_writer)
    
    scheduler = IngestionScheduler(
        orchestrator=orchestrator_base,
        registry_manager=registry_manager
    )
    
    scheduler.start()
    logger.info("scheduler_started", jobs=len(scheduler.scheduler.get_jobs()))
    
    try:
        while True:
            await asyncio.sleep(60)
            
            jobs = scheduler.scheduler.get_jobs()
            logger.debug("scheduler_heartbeat", active_jobs=len(jobs))
            
    except KeyboardInterrupt:
        logger.info("daemon_shutdown_requested")
        scheduler.stop()
        logger.info("daemon_shutdown_completed")


async def main() -> int:
    """Main entry point."""
    args = parse_args()
    
    log_level = getattr(logging, args.log_level)
    logger = get_logger(__name__, level=log_level)
    
    logger.info(
        "chronox_service_starting",
        test_snapshot=args.test_snapshot,
        daemon=args.daemon,
        dry_run=args.dry_run,
        log_level=args.log_level
    )
    
    if not args.polygon_api_key:
        logger.error("missing_polygon_api_key", error="Set POLYGON_API_KEY environment variable")
        return 1
    
    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_password
    }
    
    try:
        db_writer = TimescaleWriter(**db_config)
        logger.info("database_connected", **{k: v for k, v in db_config.items() if k != 'password'})
    except Exception as e:
        logger.error("database_connection_failed", error=str(e))
        return 1
    
    registry_manager = RegistryManager(db_writer)
    
    if args.test_snapshot:
        exit_code = await test_snapshot_fetch(
            symbol=args.test_snapshot.upper(),
            polygon_api_key=args.polygon_api_key,
            db_writer=db_writer,
            logger=logger
        )
        return exit_code
    
    if args.daemon:
        await run_daemon_mode(
            polygon_api_key=args.polygon_api_key,
            db_writer=db_writer,
            registry_manager=registry_manager,
            dry_run=args.dry_run,
            logger=logger
        )
        return 0
    
    symbol_override = None
    if args.symbols:
        symbol_override = [s.strip().upper() for s in args.symbols.split(',')]
        logger.info("symbol_override_active", symbols=symbol_override)
    
    if symbol_override and args.strategy:
        from scraper.registry.models import IngestionStrategy
        for sym in symbol_override:
            try:
                registry_manager.add_symbol(
                    symbol=sym,
                    strategy=IngestionStrategy(args.strategy),
                    timeframe=args.timeframe,
                    enabled=True
                )
                logger.info("symbol_added_for_override", symbol=sym, strategy=args.strategy)
            except Exception as e:
                logger.warn("symbol_override_failed", symbol=sym, error=str(e))
    
    await run_one_cycle(
        polygon_api_key=args.polygon_api_key,
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=args.dry_run,
        publish_mode=args.publish_mode,
        throttle_ms=args.throttle_ms,
        snapshot_method=args.snapshot_method,
        enable_stream=not args.no_stream,
        symbol_override=symbol_override,
        strategy_override=args.strategy,
        timeframe_override=args.timeframe,
        logger=logger
    )
    
    return 0


if __name__ == '__main__':
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)
