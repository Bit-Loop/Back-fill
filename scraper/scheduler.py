"""
APScheduler integration for periodic ingestion tasks.

Provides scheduled execution of:
- Nightly gap detection and backfill (00:00 UTC)
- Periodic snapshot refresh (configurable interval)
- Stream heartbeat monitoring
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from common.storage.timescale_writer import TimescaleWriter
from scraper.orchestrator import BackfillOrchestrator
from scraper.registry.manager import RegistryManager
from scraper.registry.models import IngestionStrategy

logger = logging.getLogger(__name__)


class IngestionScheduler:
    """
    Manages scheduled ingestion tasks using APScheduler.
    
    Features:
    - Nightly gap detection: Scans TimescaleDB for missing candles, triggers backfill
    - Snapshot refresh: Periodic snapshot ingestion for configured symbols
    - Stream monitoring: Health checks for active WebSocket streams
    """
    
    def __init__(
        self,
        orchestrator: BackfillOrchestrator,
        registry_manager: RegistryManager,
        snapshot_interval_minutes: int = 5,
    ):
        """
        Initialize scheduler.
        
        Args:
            orchestrator: BackfillOrchestrator instance for executing strategies
            registry_manager: RegistryManager for querying symbol registry
            snapshot_interval_minutes: Interval for snapshot refresh (default: 5 minutes)
        """
        self.orchestrator = orchestrator
        self.registry_manager = registry_manager
        self.snapshot_interval_minutes = snapshot_interval_minutes
        
        # Initialize APScheduler
        self.scheduler = BackgroundScheduler(
            job_defaults={
                'coalesce': True,  # Combine missed runs into one
                'max_instances': 1,  # Prevent concurrent execution
                'misfire_grace_time': 300,  # 5 minutes grace period
            }
        )
        
        self._setup_jobs()
        logger.info("IngestionScheduler initialized")
    
    def _setup_jobs(self) -> None:
        """Configure scheduled jobs."""
        
        # Job 1: Nightly gap detection at 00:00 UTC
        self.scheduler.add_job(
            func=self._run_gap_detection,
            trigger=CronTrigger(hour=0, minute=0, timezone='UTC'),
            id='nightly_gap_detection',
            name='Nightly Gap Detection',
            replace_existing=True,
        )
        logger.info("Scheduled: Nightly gap detection at 00:00 UTC")
        
        # Job 2: Periodic snapshot refresh
        self.scheduler.add_job(
            func=self._run_snapshot_refresh,
            trigger=IntervalTrigger(minutes=self.snapshot_interval_minutes),
            id='snapshot_refresh',
            name='Snapshot Refresh',
            replace_existing=True,
        )
        logger.info(f"Scheduled: Snapshot refresh every {self.snapshot_interval_minutes} minutes")
        
        # Job 3: Stream heartbeat monitoring (every minute)
        self.scheduler.add_job(
            func=self._run_stream_monitoring,
            trigger=IntervalTrigger(minutes=1),
            id='stream_monitoring',
            name='Stream Heartbeat Monitor',
            replace_existing=True,
        )
        logger.info("Scheduled: Stream monitoring every 1 minute")
    
    def _run_gap_detection(self) -> None:
        """
        Detect missing candles and trigger backfill.
        
        This job:
        1. Queries TimescaleDB for symbols with data gaps
        2. Identifies missing candles between first and last timestamp
        3. Triggers backfill for symbols with significant gaps
        """
        logger.info("=== STARTING NIGHTLY GAP DETECTION ===")
        
        try:
            # Get all symbols with backfill strategies
            symbols = self.registry_manager.list_symbols(enabled_only=True)
            backfill_symbols = [
                s.symbol for s in symbols 
                if s.strategy in {
                    IngestionStrategy.FLATPACK,
                    IngestionStrategy.FLATPACK_API,
                    IngestionStrategy.FLATPACK_SNAPSHOT_STREAM,
                }
            ]
            
            if not backfill_symbols:
                logger.info("No symbols configured for backfill strategies")
                return
            
            logger.info(f"Checking gaps for {len(backfill_symbols)} symbols")
            
            # Query for gaps (simplified - needs full implementation)
            gaps = self._detect_gaps(backfill_symbols)
            
            if gaps:
                logger.info(f"Found {len(gaps)} symbols with data gaps")
                for symbol, gap_info in gaps.items():
                    logger.info(f"  {symbol}: {gap_info['missing_count']} missing candles")
                    # TODO: Trigger backfill for gap periods
            else:
                logger.info("No data gaps detected")
        
        except Exception as e:
            logger.error(f"Gap detection failed: {e}", exc_info=True)
        
        logger.info("=== GAP DETECTION COMPLETE ===")
    
    def _detect_gaps(self, symbols: list[str]) -> dict:
        """
        Detect missing candles in TimescaleDB.
        
        Args:
            symbols: List of symbols to check
        
        Returns:
            Dict mapping symbol to gap information
        """
        gaps = {}
        
        with self.orchestrator.db_writer.get_connection() as conn:
            cur = conn.cursor()
            
            for symbol in symbols:
                try:
                    # Query to find gaps in 1-minute candles
                    # Expected: one row per minute between first and last timestamp
                    cur.execute("""
                        WITH bounds AS (
                            SELECT 
                                MIN(timestamp) as first_ts,
                                MAX(timestamp) as last_ts
                            FROM ohlcv_1m
                            WHERE symbol = %s
                        ),
                        expected AS (
                            SELECT generate_series(
                                date_trunc('minute', first_ts),
                                date_trunc('minute', last_ts),
                                '1 minute'::interval
                            ) as ts
                            FROM bounds
                            WHERE first_ts IS NOT NULL
                        ),
                        actual AS (
                            SELECT date_trunc('minute', timestamp) as ts
                            FROM ohlcv_1m
                            WHERE symbol = %s
                        )
                        SELECT COUNT(*) as missing_count
                        FROM expected
                        WHERE ts NOT IN (SELECT ts FROM actual)
                    """, (symbol, symbol))
                    
                    result = cur.fetchone()
                    missing_count = result[0] if result else 0
                    
                    if missing_count > 0:
                        gaps[symbol] = {
                            'missing_count': missing_count,
                            'checked_at': datetime.utcnow().isoformat(),
                        }
                
                except Exception as e:
                    logger.error(f"Gap detection failed for {symbol}: {e}")
        
        return gaps
    
    def _run_snapshot_refresh(self) -> None:
        """
        Refresh snapshots for configured symbols.
        
        This job:
        1. Finds symbols with SNAPSHOT or hybrid strategies
        2. Executes snapshot ingestion
        3. Updates last_snapshot timestamp
        """
        logger.info("=== STARTING SNAPSHOT REFRESH ===")
        
        try:
            # Get symbols with snapshot strategies
            symbols = self.registry_manager.list_symbols(enabled_only=True)
            snapshot_symbols = [
                s.symbol for s in symbols
                if s.strategy in {
                    IngestionStrategy.SNAPSHOT,
                    IngestionStrategy.SNAPSHOT_STREAM,
                    IngestionStrategy.FLATPACK_SNAPSHOT_STREAM,
                }
            ]
            
            if not snapshot_symbols:
                logger.info("No symbols configured for snapshot ingestion")
                return
            
            logger.info(f"Refreshing snapshots for {len(snapshot_symbols)} symbols")
            
            # Execute snapshot ingestion
            count = self.orchestrator.phase_snapshots(snapshot_symbols)
            
            logger.info(f"Snapshot refresh complete: {count} records ingested")
        
        except Exception as e:
            logger.error(f"Snapshot refresh failed: {e}", exc_info=True)
        
        logger.info("=== SNAPSHOT REFRESH COMPLETE ===")
    
    def _run_stream_monitoring(self) -> None:
        """
        Monitor active WebSocket streams for health.
        
        This job:
        1. Checks symbols with STREAM strategies
        2. Verifies last_stream timestamp is recent
        3. Logs warnings for stale streams
        """
        logger.debug("Checking stream health...")
        
        try:
            # Get symbols with stream strategies
            symbols = self.registry_manager.list_symbols(enabled_only=True)
            stream_symbols = [
                s for s in symbols
                if s.strategy in {
                    IngestionStrategy.STREAM,
                    IngestionStrategy.SNAPSHOT_STREAM,
                    IngestionStrategy.FLATPACK_SNAPSHOT_STREAM,
                }
            ]
            
            if not stream_symbols:
                return
            
            # Check for stale streams (no update in last 5 minutes)
            now = datetime.utcnow()
            stale_threshold_seconds = 300  # 5 minutes
            
            for symbol_entry in stream_symbols:
                if symbol_entry.last_stream:
                    age = (now - symbol_entry.last_stream).total_seconds()
                    if age > stale_threshold_seconds:
                        logger.warning(
                            f"Stream for {symbol_entry.symbol} is stale "
                            f"(last update: {int(age/60)} minutes ago)"
                        )
        
        except Exception as e:
            logger.error(f"Stream monitoring failed: {e}", exc_info=True)
    
    def start(self) -> None:
        """Start the scheduler."""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")
        else:
            logger.warning("Scheduler already running")
    
    def stop(self) -> None:
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            logger.info("Scheduler stopped")
        else:
            logger.warning("Scheduler not running")
    
    def get_jobs(self) -> list:
        """Get list of scheduled jobs."""
        return [
            {
                'id': job.id,
                'name': job.name,
                'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger),
            }
            for job in self.scheduler.get_jobs()
        ]
