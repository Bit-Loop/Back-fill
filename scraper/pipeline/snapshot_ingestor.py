"""Snapshot ingestion pipeline bridging Polygon snapshots to storage/Kafka/Redis."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from storage.timescale.writer import TimescaleWriter
from scraper.clients.snapshot_client import SnapshotClient

logger = logging.getLogger(__name__)


class SnapshotIngestor:
    """High-level ingest coordinator for full-market snapshots."""

    def __init__(
        self,
        snapshot_client: SnapshotClient,
        db_writer: TimescaleWriter,
        kafka_producer=None,
        kafka_topic: str = "chronox.market.snapshots",
        message_bus=None,
    ) -> None:
        self.snapshot_client = snapshot_client
        self.db_writer = db_writer
        self.kafka_producer = kafka_producer
        self.kafka_topic = kafka_topic
        self.message_bus = message_bus

    def ingest(
        self,
        tickers: Optional[Iterable[str]] = None,
        batch_limit: int = 1000,
        include_otc: bool = False,
    ) -> int:
        """Fetch snapshots from Polygon and fan-out to sinks.

        Returns number of snapshot records stored.
        """
        total_records = 0

        for page in self.snapshot_client.iter_full_market_snapshots(
            tickers=tickers, limit=batch_limit, include_otc=include_otc
        ):
            normalized = [self._normalize_snapshot(item) for item in page if item.get("ticker")]
            if not normalized:
                continue

            stored = self.db_writer.write_snapshots(normalized)
            total_records += stored

            self._broadcast(normalized)

            logger.debug("Processed %s snapshots", stored)

        logger.info("Snapshot ingestion complete: %s records", total_records)
        return total_records

    def _broadcast(self, snapshots: List[Dict]) -> None:
        if self.kafka_producer:
            for snapshot in snapshots:
                key = snapshot.get("symbol")
                try:
                    self.kafka_producer.publish(snapshot, topic=self.kafka_topic, key=key)
                except Exception as exc:  # pragma: no cover - network failure
                    logger.error("Kafka publish failed for %s: %s", key, exc)

        if self.message_bus:
            for snapshot in snapshots:
                channel = f"chronox:gui:snapshots:{snapshot.get('symbol')}"
                try:
                    self.message_bus.publish(channel, snapshot)
                except Exception as exc:  # pragma: no cover - redis failure
                    logger.error("Redis publish failed for %s: %s", snapshot.get("symbol"), exc)

    @staticmethod
    def _normalize_snapshot(raw: Dict) -> Dict:
        ticker = raw.get("ticker") or raw.get("T")
        day = raw.get("day", {})
        last_trade = raw.get("lastTrade", {})
        last_quote = raw.get("lastQuote", {})
        min_snapshot = raw.get("min", {})
        prev_day = raw.get("prevDay", {})

        updated = (
            raw.get("updated")
            or raw.get("t")
            or last_trade.get("t")
            or int(datetime.now(timezone.utc).timestamp() * 1000)
        )
        # Polygon timestamps can be milliseconds or nanoseconds
        # If > 1e12, it's likely nanoseconds (after year ~2001 in ms, ~33658 in ns)
        ts_int = int(updated)
        if ts_int > 1e12:
            updated_dt = datetime.fromtimestamp(ts_int / 1e9, tz=timezone.utc)
        else:
            updated_dt = datetime.fromtimestamp(ts_int / 1000, tz=timezone.utc)

        snapshot = {
            "symbol": ticker,
            "updated_at": updated_dt,
            "last_trade_price": last_trade.get("p"),
            "last_trade_size": last_trade.get("s"),
            "last_trade_exchange": last_trade.get("x"),
            "last_quote_bid": last_quote.get("bp"),
            "last_quote_ask": last_quote.get("ap"),
            "last_quote_bid_size": last_quote.get("bs"),
            "last_quote_ask_size": last_quote.get("as"),
            "day_open": day.get("o"),
            "day_high": day.get("h"),
            "day_low": day.get("l"),
            "day_close": day.get("c"),
            "day_volume": day.get("v"),
            "prev_close": prev_day.get("c"),
            "prev_volume": prev_day.get("v"),
            "todays_change": raw.get("todaysChange"),
            "todays_change_percent": raw.get("todaysChangePerc"),
            "minute_vwap": min_snapshot.get("av"),
            "minute_volume": min_snapshot.get("v"),
            "raw": raw,
        }
        return snapshot
