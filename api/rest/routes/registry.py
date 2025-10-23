"""Registry API routes using service layer."""
from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
import logging

from core.services import RegistryService
from storage.interfaces import MarketDataRepository


logger = logging.getLogger(__name__)
registry_router = APIRouter(prefix="/registry", tags=["registry"])


# Request/Response models
class RegisterTickerRequest(BaseModel):
    """Request model for registering a ticker."""
    symbol: str
    details: Optional[Dict[str, Any]] = None


class BatchRegisterRequest(BaseModel):
    """Request model for batch registering tickers."""
    symbols: List[str]


# Dependency injection placeholder
# TODO: Wire up actual repository instance via dependency injection
_registry_service: Optional[RegistryService] = None


def get_registry_service() -> RegistryService:
    """
    Dependency provider for RegistryService.
    
    In production, this should be configured via app startup to inject
    the actual repository instance. For now, this is a placeholder.
    """
    if _registry_service is None:
        raise HTTPException(
            status_code=500,
            detail="Registry service not initialized. Configure dependency injection on app startup."
        )
    return _registry_service


def configure_registry_service(service: RegistryService):
    """
    Configure the registry service for dependency injection.
    
    Call this during app startup to set the service instance.
    
    Args:
        service: Initialized RegistryService instance
    """
    global _registry_service
    _registry_service = service
    logger.info("Configured RegistryService for dependency injection")


@registry_router.post("/ticker")
async def register_ticker(
    request: RegisterTickerRequest,
    service: RegistryService = Depends(get_registry_service)
) -> Dict[str, Any]:
    """
    Register a single ticker with optional details.
    
    Args:
        request: Ticker registration request
        service: Injected RegistryService
    
    Returns:
        Registration status
    """
    result = service.register_ticker(request.symbol, request.details)
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "Registration failed"))
    
    return result


@registry_router.post("/ticker/batch")
async def batch_register_tickers(
    request: BatchRegisterRequest,
    service: RegistryService = Depends(get_registry_service)
) -> Dict[str, Any]:
    """
    Register multiple tickers in batch.
    
    Args:
        request: Batch registration request
        service: Injected RegistryService
    
    Returns:
        Batch registration status with counts
    """
    result = service.batch_register_tickers(request.symbols)
    return result


@registry_router.get("/ticker/{symbol}")
async def get_ticker_info(
    symbol: str,
    service: RegistryService = Depends(get_registry_service)
) -> Dict[str, Any]:
    """
    Get ticker information.
    
    Args:
        symbol: Ticker symbol
        service: Injected RegistryService
    
    Returns:
        Ticker information
    """
    result = service.get_ticker_info(symbol)
    
    if not result.get("status") or result.get("status") == "placeholder_implementation":
        raise HTTPException(status_code=501, detail="Query implementation pending")
    
    return result
