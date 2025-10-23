"""
Storage module for database connections and writers - DEPRECATED

This module provides backward compatibility shims.
New code should import from storage.timescale.writer
"""
import warnings

# Re-export from new location for backward compatibility
from storage.timescale.writer import TimescaleWriter

warnings.warn(
    "common.storage is deprecated. Use storage.timescale.writer instead.",
    DeprecationWarning,
    stacklevel=2
)

__all__ = ['TimescaleWriter']
