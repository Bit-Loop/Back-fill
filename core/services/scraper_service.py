"""Core implementation for the scraper daemon service."""
from __future__ import annotations

import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime
from typing import Dict, List, Optional

from common.config.settings import BackfillConfig
from storage.timescale.writer import TimescaleWriter
from core.orchestrator.legacy import BackfillOrchestrator
from scraper.registry.manager import RegistryManager

from scraper.service.models import (
    BackfillRequest,
    JobState,
    JobStatus,
    StreamRequest,
    StreamStatus,
)
from scraper.service.stream import LiveStreamCoordinator

logger = logging.getLogger(__name__)


class ScraperService:
    """High-level façade exposing backfill and streaming capabilities as a service."""

    def __init__(
        self,
        config: Optional[BackfillConfig] = None,
        max_workers: int = 2,
        enable_scheduler: bool = False,
        snapshot_interval_minutes: int = 5,
    ) -> None:
        self.config = config or BackfillConfig.default()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._jobs: Dict[str, JobStatus] = {}
        self._job_futures: Dict[str, Future] = {}
        self._jobs_lock = threading.RLock()

        api_key = self.config.polygon.api_key
        if not api_key:
            raise RuntimeError("POLYGON_API_KEY is required to run the scraper service")
        self._api_key: str = api_key

        self._stream_lock = threading.RLock()
        self._stream_coordinator: Optional[LiveStreamCoordinator] = None
        self._stream_status = StreamStatus(active=False)
        self._stream_writer: Optional[TimescaleWriter] = None
        
        # Scheduler support (optional) - only create once globally
        self._scheduler = None
        self._enable_scheduler = enable_scheduler
        self._snapshot_interval_minutes = snapshot_interval_minutes

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def initialize_scheduler(self) -> None:
        """Initialize scheduler if enabled and not already running."""
        if not self._enable_scheduler or self._scheduler is not None:
            return
        
        try:
            from scraper.scheduler import IngestionScheduler
            
            writer = self._create_writer()
            registry_manager = RegistryManager(writer)
            orchestrator = BackfillOrchestrator(
                polygon_api_key=self._api_key,
                db_writer=writer,
                years_back=5,
                redis_config=self.config.redis,
            )
            
            self._scheduler = IngestionScheduler(
                orchestrator=orchestrator,
                registry_manager=registry_manager,
                snapshot_interval_minutes=self._snapshot_interval_minutes,
            )
            self._scheduler.start()
            logger.info("Scheduler enabled with %d minute snapshot interval", self._snapshot_interval_minutes)
        except ImportError as exc:
            logger.warning("APScheduler not installed, scheduler disabled: %s", exc)
        except Exception as exc:
            logger.error("Failed to initialize scheduler: %s", exc)
    
    def start_backfill(self, request: BackfillRequest) -> JobStatus:
        """Submit a backfill job to the executor."""

        job_id = str(uuid.uuid4())
        status = JobStatus(job_id=job_id, request=request, state=JobState.PENDING)

        with self._jobs_lock:
            self._jobs[job_id] = status

        future = self._executor.submit(self._run_backfill_job, status)
        future.add_done_callback(lambda fut, jid=job_id: self._finalize_job(jid, fut))

        with self._jobs_lock:
            self._job_futures[job_id] = future

        logger.info("Backfill job %s queued for tickers=%s", job_id, request.tickers)
        return status
    
    def execute_registry(self, symbols: Optional[List[str]] = None) -> JobStatus:
        """
        Execute ingestion strategies from symbol registry.
        
        This is the registry-driven execution method that processes symbols
        based on their configured strategies in the database.
        
        Args:
            symbols: Optional list of symbols to process. If None, processes all enabled symbols.
        
        Returns:
            JobStatus with job tracking information
        """
        job_id = str(uuid.uuid4())
        # Create a synthetic request for registry execution
        request = BackfillRequest(
            tickers=symbols or [],
            years=0,  # Not used in registry mode
            use_flatfiles=False,
            debug=False,
        )
        status = JobStatus(job_id=job_id, request=request, state=JobState.PENDING)
        status.message = f"Registry execution for {len(symbols) if symbols else 'all enabled'} symbols"

        with self._jobs_lock:
            self._jobs[job_id] = status

        future = self._executor.submit(self._run_registry_job, status, symbols)
        future.add_done_callback(lambda fut, jid=job_id: self._finalize_job(jid, fut))

        with self._jobs_lock:
            self._job_futures[job_id] = future

        logger.info("Registry execution job %s queued", job_id)
        return status

    def list_jobs(self) -> List[JobStatus]:
        with self._jobs_lock:
            return list(self._jobs.values())

    def get_job(self, job_id: str) -> Optional[JobStatus]:
        with self._jobs_lock:
            return self._jobs.get(job_id)

    def cancel_job(self, job_id: str) -> bool:
        with self._jobs_lock:
            future = self._job_futures.get(job_id)
            status = self._jobs.get(job_id)

        if not future or not status:
            return False

        cancelled = future.cancel()
        if cancelled:
            status.state = JobState.CANCELLED
            status.completed_at = datetime.utcnow()
            status.message = "Job cancelled by user"
            logger.info("Backfill job %s cancelled", job_id)
        return cancelled

    def start_stream(self, request: StreamRequest) -> StreamStatus:
        """Start streaming updates via Redis + lightweight-charts feed."""

        with self._stream_lock:
            if self._stream_coordinator:
                raise RuntimeError("Streaming already active")

            db_writer = self._create_writer()
            coordinator = LiveStreamCoordinator(
                symbols=request.tickers,
                timeframe=request.timeframe,
                api_key=self._api_key,
                db_writer=db_writer,
                message_bus=self._create_message_bus(),
            )
            coordinator.start()
            self._stream_coordinator = coordinator
            self._stream_writer = db_writer
            self._stream_status = StreamStatus(
                active=True,
                tickers=request.tickers,
                timeframe=request.timeframe,
                started_at=datetime.utcnow(),
            )
            logger.info("Streaming started for %s (%s)", request.tickers, request.timeframe)
            return self._stream_status

    def stop_stream(self) -> StreamStatus:
        """Stop the active stream if running."""

        with self._stream_lock:
            if not self._stream_coordinator:
                return self._stream_status

            self._stream_coordinator.stop()
            self._stream_coordinator = None
            if self._stream_writer:
                self._release_writer(self._stream_writer)
                self._stream_writer = None
            self._stream_status = StreamStatus(active=False)
            logger.info("Streaming stopped")
            return self._stream_status

    def stream_status(self) -> StreamStatus:
        with self._stream_lock:
            return self._stream_status

    def shutdown(self) -> None:
        logger.info("Shutting down scraper service")
        if self._scheduler:
            self._scheduler.stop()
        self.stop_stream()
        self._executor.shutdown(wait=False)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _run_backfill_job(self, status: JobStatus) -> None:
        status.state = JobState.RUNNING
        status.started_at = datetime.utcnow()
        logger.info("Backfill job %s started", status.job_id)

        writer = None
        orchestrator = None
        try:
            writer = self._create_writer()
            orchestrator = BackfillOrchestrator(
                polygon_api_key=self._api_key,
                db_writer=writer,
                years_back=status.request.years,
                use_flatfiles=status.request.use_flatfiles,
                debug=status.request.debug,
                skip_reference=status.request.skip_reference,
                skip_corporate=status.request.skip_corporate,
                skip_daily=status.request.skip_daily,
                skip_minute=status.request.skip_minute,
                skip_news=status.request.skip_news,
                source=status.request.source,
                publish_kafka=status.request.publish_kafka,
                publish_redis=status.request.publish_redis,
                kafka_topic=status.request.kafka_topic,
                kafka_bootstrap=status.request.kafka_bootstrap,
                redis_config=self.config.redis,
            )

            result = orchestrator.run_full_backfill(status.request.tickers)
            status.state = JobState.COMPLETED
            status.completed_at = datetime.utcnow()
            status.result = result
            status.message = "Backfill completed"
            logger.info(
                "Backfill job %s completed: %s symbols", status.job_id, len(status.request.tickers)
            )
        except Exception as exc:
            status.state = JobState.FAILED
            status.completed_at = datetime.utcnow()
            status.error = str(exc)
            status.message = "Backfill failed"
            logger.exception("Backfill job %s failed", status.job_id)
        finally:
            if orchestrator:
                orchestrator.close()
            if writer:
                writer.close()
    
    def _run_registry_job(self, status: JobStatus, symbols: Optional[List[str]]) -> None:
        """Execute registry-based ingestion strategies."""
        status.state = JobState.RUNNING
        status.started_at = datetime.utcnow()
        logger.info("Registry execution job %s started", status.job_id)

        writer = None
        orchestrator = None
        try:
            writer = self._create_writer()
            orchestrator = BackfillOrchestrator(
                polygon_api_key=self._api_key,
                db_writer=writer,
                years_back=5,  # Default for registry mode
                redis_config=self.config.redis,
            )

            result = orchestrator.execute_registry_strategies(symbols=symbols)
            status.state = JobState.COMPLETED
            status.completed_at = datetime.utcnow()
            status.result = result
            status.message = f"Registry execution completed for {len(result)} symbols"
            logger.info("Registry execution job %s completed: %s symbols", status.job_id, len(result))
        except Exception as exc:
            status.state = JobState.FAILED
            status.completed_at = datetime.utcnow()
            status.error = str(exc)
            status.message = "Registry execution failed"
            logger.exception("Registry execution job %s failed", status.job_id)
        finally:
            if orchestrator:
                orchestrator.close()
            if writer:
                writer.close()

    def _finalize_job(self, job_id: str, future: Future) -> None:
        with self._jobs_lock:
            status = self._jobs.get(job_id)
            if status and status.state == JobState.RUNNING:
                # The job completed without exception but handlers did not update state
                if future.cancelled():
                    status.state = JobState.CANCELLED
                    status.completed_at = datetime.utcnow()
                    status.message = "Job cancelled"
                elif future.exception():
                    status.state = JobState.FAILED
                    status.completed_at = datetime.utcnow()
                    status.error = str(future.exception())
                    status.message = "Job failed"
                else:
                    status.state = JobState.COMPLETED
                    status.completed_at = datetime.utcnow()
                    status.message = "Backfill completed"

    def _create_writer(self) -> TimescaleWriter:
        db = self.config.database
        return TimescaleWriter(
            host=db.host,
            port=db.port,
            database=db.database,
            user=db.user,
            password=db.password,
        )

    def _create_message_bus(self):
        try:
            from scraper.pipeline import RedisMessageBus

            redis_cfg = self.config.redis
            logger.debug("Attempting Redis connection at %s:%s", redis_cfg.host, redis_cfg.port)
            return RedisMessageBus(
                host=redis_cfg.host,
                port=redis_cfg.port,
                db=redis_cfg.db,
                password=redis_cfg.password,
            )
        except Exception as exc:  # pragma: no cover - fallback path
            logger.warning("Redis unavailable, using in-memory message bus: %s", exc)
            from scraper.pipeline import InMemoryMessageBus

            return InMemoryMessageBus()

    def _release_writer(self, writer: TimescaleWriter) -> None:
        try:
            writer.close()
        except Exception:  # pragma: no cover - defensive cleanup
            logger.debug("Writer close failed during stream shutdown", exc_info=True)
