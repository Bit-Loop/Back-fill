"""FastAPI application exposing the scraper service."""
from __future__ import annotations

import logging
from typing import Dict

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse

from common.config.settings import BackfillConfig

from .core import ScraperService
from .models import BackfillRequest, StreamRequest

logger = logging.getLogger(__name__)


def get_service() -> ScraperService:
    service = getattr(get_service, "_instance", None)
    if service is None:
        config = BackfillConfig.default()
        service = ScraperService(config=config)
        setattr(get_service, "_instance", service)
    return service


app = FastAPI(title="Backfill Scraper Service", version="1.0.0")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    service = get_service()
    service.shutdown()


@app.get("/health")
def health(service: ScraperService = Depends(get_service)) -> Dict[str, str]:
    status = {
        "status": "ok",
        "stream_active": service.stream_status().active,
        "jobs": len(service.list_jobs()),
    }
    return status


@app.post("/backfill")
def start_backfill(request: BackfillRequest, service: ScraperService = Depends(get_service)):
    if not request.tickers:
        raise HTTPException(status_code=400, detail="No tickers supplied")
    status = service.start_backfill(request)
    return JSONResponse(status_code=202, content=status.to_dict())


@app.get("/jobs")
def list_jobs(service: ScraperService = Depends(get_service)):
    return [job.to_dict() for job in service.list_jobs()]


@app.get("/jobs/{job_id}")
def get_job(job_id: str, service: ScraperService = Depends(get_service)):
    job = service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.to_dict()


@app.delete("/jobs/{job_id}")
def cancel_job(job_id: str, service: ScraperService = Depends(get_service)):
    cancelled = service.cancel_job(job_id)
    if not cancelled:
        raise HTTPException(status_code=404, detail="Job not found or already finished")
    return {"status": "cancelled", "job_id": job_id}


@app.post("/stream")
def start_stream(request: StreamRequest, service: ScraperService = Depends(get_service)):
    if not request.tickers:
        raise HTTPException(status_code=400, detail="No tickers supplied")
    status = service.start_stream(request)
    return status.to_dict()


@app.delete("/stream")
def stop_stream(service: ScraperService = Depends(get_service)):
    status = service.stop_stream()
    return status.to_dict()


@app.get("/stream")
def stream_status(service: ScraperService = Depends(get_service)):
    return service.stream_status().to_dict()
