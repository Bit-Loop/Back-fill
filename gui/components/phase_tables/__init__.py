"""
Phase Tables Package - Data tables for backfill phases

Provides:
- Phase1ReferenceTable: Reference data (ticker metadata)
- Phase2CorporateTable: Corporate actions (dividends, splits)
- Phase3DailyTable: Daily bars (aggregate or per-flatfile debug)
"""

from .phase1_reference import Phase1ReferenceTable
from .phase2_corporate import Phase2CorporateTable
from .phase3_daily import Phase3DailyTable

__all__ = [
    'Phase1ReferenceTable',
    'Phase2CorporateTable',
    'Phase3DailyTable'
]
