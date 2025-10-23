"""FastAPI application exposing the scraper service."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common.config.settings import BackfillConfig
from scraper.registry import IngestionStrategy, RegistryManager, SymbolRegistry

from .core import ScraperService
from .models import BackfillRequest, StreamRequest

logger = logging.getLogger(__name__)


# Pydantic models for API requests
class SymbolRegistryCreate(BaseModel):
    symbol: str
    strategy: str
    timeframe: str = "1m"
    enabled: bool = True


class SymbolRegistryUpdate(BaseModel):
    strategy: str | None = None
    timeframe: str | None = None
    enabled: bool | None = None


def get_service() -> ScraperService:
    service = getattr(get_service, "_instance", None)
    if service is None:
        config = BackfillConfig.default()
        # Enable scheduler in daemon mode
        import os
        enable_scheduler = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"
        snapshot_interval = int(os.getenv("SNAPSHOT_INTERVAL_MINUTES", "5"))
        service = ScraperService(
            config=config,
            enable_scheduler=enable_scheduler,
            snapshot_interval_minutes=snapshot_interval,
        )
        setattr(get_service, "_instance", service)
    return service


def get_registry_manager(service: ScraperService = Depends(get_service)) -> RegistryManager:
    """Get or create registry manager instance."""
    manager = getattr(get_registry_manager, "_manager", None)
    if manager is None:
        from common.storage.timescale_writer import TimescaleWriter

        db = service.config.database
        writer = TimescaleWriter(
            host=db.host,
            port=db.port,
            database=db.database,
            user=db.user,
            password=db.password,
        )
        manager = RegistryManager(writer)
        setattr(get_registry_manager, "_manager", manager)
    return manager


app = FastAPI(title="ChronoX Ingestion Service", version="2.0.0")


@app.on_event("startup")
async def startup_event() -> None:
    service = get_service()
    service.initialize_scheduler()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    service = get_service()
    service.shutdown()


@app.get("/health")
def health(service: ScraperService = Depends(get_service)) -> Dict:
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


# ============================================================================
# Symbol Registry Endpoints
# ============================================================================


@app.post("/api/v1/symbols", status_code=201)
def create_symbol(
    data: SymbolRegistryCreate,
    registry: RegistryManager = Depends(get_registry_manager),
):
    """Add symbol to registry with ingestion strategy."""
    try:
        strategy = IngestionStrategy(data.strategy)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid strategy. Must be one of: {[s.value for s in IngestionStrategy]}",
        )

    entry = registry.add_symbol(
        symbol=data.symbol.upper(),
        strategy=strategy,
        timeframe=data.timeframe,
        enabled=data.enabled,
    )
    return entry.to_dict()


@app.get("/api/v1/symbols")
def list_symbols(
    enabled_only: bool = False,
    registry: RegistryManager = Depends(get_registry_manager),
):
    """List all symbols in registry."""
    symbols = registry.list_symbols(enabled_only=enabled_only)
    return [s.to_dict() for s in symbols]


@app.get("/api/v1/symbols/{symbol}")
def get_symbol(
    symbol: str,
    registry: RegistryManager = Depends(get_registry_manager),
):
    """Get symbol registry entry."""
    entry = registry.get_symbol(symbol.upper())
    if not entry:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
    return entry.to_dict()


@app.patch("/api/v1/symbols/{symbol}")
def update_symbol(
    symbol: str,
    data: SymbolRegistryUpdate,
    registry: RegistryManager = Depends(get_registry_manager),
):
    """Update symbol configuration."""
    existing = registry.get_symbol(symbol.upper())
    if not existing:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")

    # Merge updates
    if data.strategy:
        try:
            existing.strategy = IngestionStrategy(data.strategy)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid strategy: {data.strategy}")

    if data.timeframe:
        existing.timeframe = data.timeframe
    if data.enabled is not None:
        existing.enabled = data.enabled

    # Save back
    updated = registry.add_symbol(
        symbol=existing.symbol,
        strategy=existing.strategy,
        timeframe=existing.timeframe,
        enabled=existing.enabled,
    )
    return updated.to_dict()


@app.delete("/api/v1/symbols/{symbol}")
def delete_symbol(
    symbol: str,
    registry: RegistryManager = Depends(get_registry_manager),
):
    """Remove symbol from registry."""
    deleted = registry.delete_symbol(symbol.upper())
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
    return {"status": "deleted", "symbol": symbol.upper()}


@app.post("/api/v1/backfill/{symbol}")
def trigger_backfill(
    symbol: str,
    service: ScraperService = Depends(get_service),
    registry: RegistryManager = Depends(get_registry_manager),
):
    """Manually trigger backfill for a symbol."""
    entry = registry.get_symbol(symbol.upper())
    if not entry:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not in registry")

    # TODO: Implement manual backfill trigger
    # For now, use existing backfill flow
    request = BackfillRequest(
        tickers=[symbol.upper()],
        years=5,
        skip_news=True,
    )
    status = service.start_backfill(request)
    return JSONResponse(status_code=202, content=status.to_dict())


@app.get("/api/v1/status")
def orchestrator_status(
    service: ScraperService = Depends(get_service),
    registry: RegistryManager = Depends(get_registry_manager),
):
    """Get overall orchestrator status."""
    symbols = registry.list_symbols(enabled_only=True)
    return {
        "stream_active": service.stream_status().active,
        "active_jobs": len([j for j in service.list_jobs() if j.state.value == "running"]),
        "total_symbols": len(symbols),
        "symbols_by_status": {
            "idle": len([s for s in symbols if s.status.value == "idle"]),
            "running": len([s for s in symbols if s.status.value == "running"]),
            "error": len([s for s in symbols if s.status.value == "error"]),
        },
    }


class RegistryExecuteRequest(BaseModel):
    """Request model for registry execution."""
    symbols: List[str] | None = None  # If None, execute all enabled symbols


@app.post("/api/v1/registry/execute", status_code=202)
def execute_registry(
    request: RegistryExecuteRequest,
    service: ScraperService = Depends(get_service),
):
    """
    Execute ingestion strategies from symbol registry.
    
    This triggers registry-driven orchestration where each symbol's
    configured strategy determines how data is fetched and stored.
    
    Args:
        request: Optional list of symbols. If omitted, processes all enabled symbols.
    
    Returns:
        Job status for tracking execution progress
    """
    symbols = [s.upper() for s in request.symbols] if request.symbols else None
    status = service.execute_registry(symbols=symbols)
    return status.to_dict()


@app.get("/api/v1/scheduler/status")
def scheduler_status(service: ScraperService = Depends(get_service)):
    """Get scheduler status and configured jobs."""
    if not service._scheduler:
        return {
            "enabled": False,
            "message": "Scheduler not enabled. Set ENABLE_SCHEDULER=true to enable.",
        }
    
    return {
        "enabled": True,
        "running": service._scheduler.scheduler.running,
        "jobs": service._scheduler.get_jobs(),
    }


@app.post("/api/v1/scheduler/trigger/{job_id}")
def trigger_scheduler_job(job_id: str, service: ScraperService = Depends(get_service)):
    """Manually trigger a scheduled job."""
    if not service._scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not enabled")
    
    try:
        job = service._scheduler.scheduler.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        # Trigger job immediately
        job.modify(next_run_time=datetime.utcnow())
        return {
            "status": "triggered",
            "job_id": job_id,
            "job_name": job.name,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
