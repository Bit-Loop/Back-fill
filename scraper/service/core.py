"""Core implementation for the scraper daemon service."""
from __future__ import annotations

import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime
from typing import Dict, List, Optional

from common.config.settings import BackfillConfig
from common.storage import TimescaleWriter
from scraper.orchestrator import BackfillOrchestrator

from .models import (
    BackfillRequest,
    JobState,
    JobStatus,
    StreamRequest,
    StreamStatus,
)
from .stream import LiveStreamCoordinator

logger = logging.getLogger(__name__)


class ScraperService:
    """High-level façade exposing backfill and streaming capabilities as a service."""

    def __init__(self, config: Optional[BackfillConfig] = None, max_workers: int = 2) -> None:
        self.config = config or BackfillConfig.default()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._jobs: Dict[str, JobStatus] = {}
        self._job_futures: Dict[str, Future] = {}
        self._jobs_lock = threading.RLock()

        self._stream_lock = threading.RLock()
        self._stream_coordinator: Optional[LiveStreamCoordinator] = None
        self._stream_status = StreamStatus(active=False)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
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
                api_key=self.config.polygon.api_key,
                db_writer=db_writer,
                message_bus=self._create_message_bus(),
                ownership_callback=self._release_writer
            )
            coordinator.start()
            self._stream_coordinator = coordinator
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
            self._stream_status = StreamStatus(active=False)
            logger.info("Streaming stopped")
            return self._stream_status

    def stream_status(self) -> StreamStatus:
        with self._stream_lock:
            return self._stream_status

    def shutdown(self) -> None:
        logger.info("Shutting down scraper service")
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
                polygon_api_key=self.config.polygon.api_key,
                db_writer=writer,
                years_back=status.request.years,
                use_flatfiles=status.request.use_flatfiles,
                debug=status.request.debug,
                skip_reference=status.request.skip_reference,
                skip_corporate=status.request.skip_corporate,
                skip_daily=status.request.skip_daily,
                skip_minute=status.request.skip_minute,
                skip_news=status.request.skip_news,
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
            return RedisMessageBus(
                host=redis_cfg.host,
                port=redis_cfg.port,
                db=redis_cfg.db,
                password=redis_cfg.password,
            )
        except Exception as exc:  # pragma: no cover - fallback path
            logger.warning("Falling back to in-memory message bus: %s", exc)
            from scraper.pipeline import InMemoryMessageBus

            return InMemoryMessageBus()

    def _release_writer(self, writer: TimescaleWriter) -> None:
        try:
            writer.close()
        except Exception:  # pragma: no cover - defensive cleanup
            logger.debug("Writer close failed during stream shutdown", exc_info=True)
