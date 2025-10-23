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
from typing import Any, Optional

from common.storage.timescale_writer import TimescaleWriter
from scraper.registry.manager import RegistryManager
from scraper.registry.models import IngestionStrategy, IngestionStatus, SymbolRegistry
from scraper.unified_orchestrator import UnifiedOrchestrator
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
    
    return parser.parse_args()


async def test_snapshot_fetch(
    symbol: str,
    polygon_api_key: str,
    db_writer: TimescaleWriter,
    logger: Any
) -> int:
    """
    Test snapshot fetch and validation for symbol.
    
    Args:
        symbol: Symbol ticker
        polygon_api_key: Polygon API key
        db_writer: Database writer
        logger: Logger instance
    
    Returns:
        Exit code (0=success, 1=failure)
    """
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
    logger: Any
) -> None:
    """
    Execute one orchestration cycle.
    
    Args:
        polygon_api_key: Polygon API key
        db_writer: Database writer
        registry_manager: Registry manager
        dry_run: If True, simulate without writes
        logger: Logger instance
    """
    orchestrator = UnifiedOrchestrator(
        polygon_api_key=polygon_api_key,
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=dry_run
    )
    
    await orchestrator.resume_failed_jobs()
    
    async with orchestrator.lifecycle():
        results = await orchestrator.run_cycle()
        
        success_count = sum(1 for r in results.values() if r.get('status') == 'success')
        error_count = sum(1 for r in results.values() if r.get('status') == 'error')
        
        logger.info(
            "cycle_completed",
            total=len(results),
            success=success_count,
            errors=error_count
        )


async def run_daemon_mode(
    polygon_api_key: str,
    db_writer: TimescaleWriter,
    registry_manager: RegistryManager,
    dry_run: bool,
    logger: Any
) -> None:
    """
    Run in daemon mode with scheduler.
    
    Args:
        polygon_api_key: Polygon API key
        db_writer: Database writer
        registry_manager: Registry manager
        dry_run: If True, simulate without writes
        logger: Logger instance
    """
    from scraper.orchestrator import BackfillOrchestrator
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
    """
    Main entry point.
    
    Returns:
        Exit code
    """
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
    
    await run_one_cycle(
        polygon_api_key=args.polygon_api_key,
        db_writer=db_writer,
        registry_manager=registry_manager,
        dry_run=args.dry_run,
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
