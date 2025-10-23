"""Polygon full-market snapshot client."""
from __future__ import annotations

import logging
from typing import Dict, Iterable, Iterator, List, Optional

from .polygon_client import PolygonClient

logger = logging.getLogger(__name__)


class SnapshotClient:
    """Client wrapper for Polygon's full market snapshot endpoints."""

    SNAPSHOT_ENDPOINT = "/v2/snapshot/locale/us/markets/stocks/tickers"

    def __init__(self, client: PolygonClient):
        self.client = client

    def iter_full_market_snapshots(
        self,
        tickers: Optional[Iterable[str]] = None,
        limit: int = 1000,
        include_otc: bool = False,
    ) -> Iterator[List[Dict]]:
        """Yield successive pages of full-market snapshots.

        Args:
            tickers: Optional iterable of tickers to filter. If omitted the entire market is returned.
            limit: Page size (Polygon max is 1000).
            include_otc: Whether to include OTC tickers (requires entitlement).

        Yields:
            Lists of snapshot dictionaries matching Polygon's schema.
        """
        params: Dict[str, str] = {"limit": str(limit)}
        if tickers:
            params["tickers"] = ",".join(sorted({t.upper() for t in tickers}))
        if include_otc:
            params["include_otc"] = "true"

        next_path: Optional[str] = self.SNAPSHOT_ENDPOINT
        next_params: Optional[Dict[str, str]] = params

        while next_path:
            payload = self.client._make_request(next_path, next_params)
            page = payload.get("tickers", [])
            if page:
                yield page

            next_url = payload.get("next_url")
            if not next_url:
                break

            if next_url.startswith(self.client.BASE_URL):
                next_path = next_url[len(self.client.BASE_URL) :]
            else:
                next_path = next_url
            next_params = None  # already encoded in next_url

    def get_snapshots_for_tickers(self, tickers: List[str]) -> List[Dict]:
        """Convenience helper returning a single batch of snapshots for specific tickers."""
        tickers = [t.strip().upper() for t in tickers if t.strip()]
        if not tickers:
            return []

        try:
            iterator = self.iter_full_market_snapshots(tickers=tickers, limit=min(len(tickers), 1000))
            return next(iterator, [])
        except StopIteration:
            return []
        except Exception as exc:  # pragma: no cover - network failure
            logger.error("Snapshot fetch failed: %s", exc)
            return []
