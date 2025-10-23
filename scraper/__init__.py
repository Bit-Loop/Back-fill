"""
Scraper package - data collection and processing

This package provides backward compatibility shims for relocated modules.
New code should import from core.orchestrator and core.services
"""
import warnings

__version__ = '1.0.0'

# Backward compatibility shims
def _warn_deprecated_import(old_path: str, new_path: str):
    warnings.warn(
        f"{old_path} is deprecated. Use {new_path} instead.",
        DeprecationWarning,
        stacklevel=3
    )

# Re-export from new locations for backward compatibility
def __getattr__(name):
    if name == "BackfillOrchestrator":
        _warn_deprecated_import("scraper.orchestrator", "core.orchestrator.legacy")
        from core.orchestrator.legacy import BackfillOrchestrator
        return BackfillOrchestrator
    elif name == "UnifiedOrchestrator":
        _warn_deprecated_import("scraper.unified_orchestrator", "core.orchestrator.unified")
        from core.orchestrator.unified import UnifiedOrchestrator
        return UnifiedOrchestrator
    elif name == "ScraperService":
        _warn_deprecated_import("scraper.service.core", "core.services.scraper_service")
        from core.services.scraper_service import ScraperService
        return ScraperService
    raise AttributeError(f"module 'scraper' has no attribute '{name}'")
