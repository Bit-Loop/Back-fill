Refactor Roadmap (Copy/Paste Ready)

Phase 1 — RE-ORGANIZE (Status: ❌)

Why change?
- chronox_ingestion.py, scraper.py, and service modules sit in the repo root, forcing absolute imports and making it hard to reason about boundaries.
- Tests and tools expect cli/, core/, storage/, api/ (see _REFACTOR_PLANNING.md), but those folders do not exist—new teammates hunt for files.
- Current layout blocks incremental modularization (Phase 2+) because business logic, infra code, and CLIs are interleaved.

How to execute safely
1. Create package scaffolding (no logic change yet):
   cli/
     __init__.py
     chronox_cli.py        # new home for chronox_ingestion.py
     legacy_cli.py         # new home for scraper.py (will deprecate in Phase 4)
   core/
     __init__.py
     orchestrator/
       __init__.py
       unified.py          # new home for scraper/unified_orchestrator.py
       legacy.py           # new home for scraper/orchestrator.py
     services/
       __init__.py
       scraper_service.py  # new home for scraper/core.py
   storage/
     __init__.py
     timescale/
       __init__.py
       writer.py           # common/storage/timescale_writer.py
       schema.py           # placeholder for Phase 2 extraction
   api/
     __init__.py
     rest/
       __init__.py
       app.py              # new home for api.py (will wrap FastAPI)
   gui/
     __init__.py
     legacy_visualizer.py  # temp name for backfill_visualizer.py until breakout

2. Move files with git mv (preserves history). Example:
   git mv chronox_ingestion.py cli/chronox_cli.py
   git mv scraper/unified_orchestrator.py core/orchestrator/unified.py
   git mv common/storage/timescale_writer.py storage/timescale/writer.py
   git mv api.py api/rest/app.py
   git mv backfill_visualizer.py gui/legacy_visualizer.py
   ⚠️ If VS Code reports “file already exists”, ensure destination is new path or remove stale file before moving.

3. Update imports (search-replace, run tests after each batch):
   BEFORE
   from scraper.unified_orchestrator import UnifiedOrchestrator
   AFTER
   from core.orchestrator.unified import UnifiedOrchestrator

4. Temporary shim for backward compatibility:
   # scraper/__init__.py (new file to avoid breaking legacy code)
   from core.orchestrator.unified import UnifiedOrchestrator  # noqa: F401
   from core.orchestrator.legacy import BackfillOrchestrator  # noqa: F401
   This lets any existing from scraper import UnifiedOrchestrator continue to work until we rewrite callers.

5. Smoke test:
   python -m cli.chronox_cli --help
   python -m cli.legacy_cli --help
   python -m pytest tests/ -q

Deliverable for Phase 1
- New folder structure checked in
- All imports green
- No functional change, only relocation

Phase 2 — ISOLATE (Status: ❌)

Why change?
- storage/timescale/writer.py is a 600+ line god object doing pooling, schema migrations, and persistence. This blocks mocking and unit testing.
- gui/legacy_visualizer.py (≈3,900 LOC) handles Qt widgets, Redis, Timescale queries, indicator math, and threading inside one file—impossible to test or maintain.
- GUI still queries TimescaleDB directly (TimescaleWriter usage), violating separation of concerns.

How to execute safely

2.1 Split TimescaleWriter
Create new modules (pure refactor, no behavior changes):

storage/timescale/pool.py
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager

class PostgresConnectionPool:
    def __init__(self, dsn: str, min_conn: int = 1, max_conn: int = 20):
        self._pool = ThreadedConnectionPool(min_conn, max_conn, dsn)

    @contextmanager
    def connection(self):
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def close(self) -> None:
        self._pool.closeall()

storage/timescale/schema.py
import logging
from psycopg2.extensions import connection

logger = logging.getLogger(__name__)

class SchemaManager:
    def __init__(self, conn: connection):
        self._conn = conn

    def ensure_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            # ... extract existing CREATE TABLE / ALTER TABLE logic here ...
        logger.info("Timescale schema ensured")

storage/timescale/writer.py (new façade calling pool + schema + repositories)
from storage.timescale.pool import PostgresConnectionPool
from storage.timescale.schema import SchemaManager
from storage.timescale.repositories.ohlcv import OHLCVRepository
# ... other repositories ...

class TimescaleWriter:
    def __init__(self, dsn: str, min_conn: int = 1, max_conn: int = 20):
        self._pool = PostgresConnectionPool(dsn, min_conn, max_conn)
        with self._pool.connection() as conn:
            SchemaManager(conn).ensure_schema()
        self._ohlcv = OHLCVRepository(self._pool)
        # ... instantiate other repositories ...

    def write_ohlcv_bars(self, symbol: str, bars: list[dict], timeframe: str = "1m") -> int:
        return self._ohlcv.insert(symbol, bars, timeframe)

    def close(self) -> None:
        self._pool.close()

storage/timescale/repositories/ohlcv.py
from psycopg2 import extras
from datetime import datetime

class OHLCVRepository:
    def __init__(self, pool):
        self._pool = pool

    def insert(self, symbol: str, bars: list[dict], timeframe: str) -> int:
        if not bars:
            return 0
        table_name = self._map_timeframe(timeframe)
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                payload = [
                    (
                        datetime.fromtimestamp(item.get("t", 0) / 1000),
                        symbol,
                        item.get("o"),
                        item.get("h"),
                        item.get("l"),
                        item.get("c"),
                        item.get("v"),
                        item.get("vw"),
                        item.get("n"),
                    )
                    for item in bars
                ]
                extras.execute_batch(
                    cur,
                    """
                    INSERT INTO {table_name}
                    (time, symbol, open, high, low, close, volume, vwap, transactions)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (time, symbol) DO NOTHING
                    """,
                    payload,
                )
                return len(payload)

    @staticmethod
    def _map_timeframe(timeframe: str) -> str:
        mapping = {"1m": "market_data_1m", "5m": "market_data_5m", "1d": "market_data_1d"}
        return mapping.get(timeframe, "market_data_1m")

Migration plan
1. Extract pool & schema classes.
2. Move insert/update logic into repository modules (one per table group).
3. TimescaleWriter becomes thin façade; all existing call sites keep same import.
4. Add unit tests around repositories using psycopg2 extras.execute_values mocks.

2.2 Decompose GUI monolith
Target file: gui/legacy_visualizer.py
Breakdown:
- RedisSubscriberThread → move into gui/services/redis.py
- Database queries → new REST client (api_client = ApiClient(base_url=...))
- Charts → gui/components/candlestick.py, gui/components/order_book.py, etc.
- App entry point → gui/app.py orchestrates dependencies.

Example extraction:
gui/services/redis.py
import asyncio
import json
import logging

class RedisSubscriber:
    def __init__(self, client, channel: str, callback):
        self._client = client
        self._channel = channel
        self._callback = callback
        self._log = logging.getLogger(__name__)

    async def run(self) -> None:
        pubsub = self._client.pubsub()
        await pubsub.subscribe(self._channel)
        async for message in pubsub.listen():
            if message["type"] == "message":
                payload = json.loads(message["data"])
                self._callback(payload)

gui/components/candlestick.py
from PyQt6.QtWidgets import QWidget

class CandlestickChart(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        # init UI components here

    def render_bars(self, bars):
        # translate envelope into series
        ...

After extraction:
gui/app.py
from gui.services.redis import RedisSubscriber
from gui.components.candlestick import CandlestickChart
from gui.data.providers import BarsProvider

class BackfillVisualizer(QMainWindow):
    def __init__(self, redis_client, api_client):
        super().__init__()
        self.chart = CandlestickChart(self)
        self.provider = BarsProvider(api_client)
        self.subscriber = RedisSubscriber(redis_client, "bars:SPY:1m", self._on_bar)

    def _on_bar(self, envelope):
        self.chart.render_bars([envelope])

Remove direct DB access
- Replace TimescaleWriter in GUI with BarsProvider that invokes a new REST endpoint (see Phase 3 service layer).
- Ensure GUI uses asynchronous tasks instead of blocking queries.

Testing plan
- Introduce tests/gui/test_redis_subscriber.py mocking Redis client to ensure callbacks fire.
- Integration test: pytest gui/tests/test_visualizer_startup.py launching app with Qt test harness (PyQt6 QTest).

Phase 3 — INTERFACE (Status: ❌)

Why change?
- Two orchestrators (core/orchestrator/unified.py and legacy.py) lead to drift.
- Registry manager and other services call TimescaleWriter directly, preventing mocking.
- FastAPI routes in api/rest/app.py contain business logic, making CLI, scheduler, and API diverge.

How to execute safely

3.1 OrchestratorFacade
core/orchestrator/facade.py
from typing import Protocol

class IngestionOrchestrator(Protocol):
    async def run_cycle(self) -> None: ...
    async def run_symbol(self, symbol: str) -> None: ...

class OrchestratorFacade:
    def __init__(self, registry_manager, polygon_client, writer, config):
        self._registry = registry_manager
        if config.use_unified:
            from core.orchestrator.unified import UnifiedOrchestrator
            self._impl: IngestionOrchestrator = UnifiedOrchestrator(...)
        else:
            from core.orchestrator.legacy import BackfillOrchestrator
            self._impl = BackfillOrchestrator(...)

    async def run_cycle(self) -> None:
        await self._impl.run_cycle()
- cli/chronox_cli.py, scheduler, and API call the façade, not specific implementations.
- Feature flags/mailbox determine which orchestrator runs; we can phase out legacy later.

3.2 Database adapter interface
storage/interfaces.py
from typing import Protocol, Sequence, Mapping

class MarketDataRepository(Protocol):
    def insert_bars(self, symbol: str, bars: Sequence[Mapping], timeframe: str) -> int: ...

storage/timescale/adapter.py
from storage.interfaces import MarketDataRepository
from storage.timescale.repositories.ohlcv import OHLCVRepository

class TimescaleMarketDataAdapter(MarketDataRepository):
    def __init__(self, pool):
        self._repo = OHLCVRepository(pool)

    def insert_bars(self, symbol, bars, timeframe):
        return self._repo.insert(symbol, bars, timeframe)
- Registry, orchestrators, and services depend on MarketDataRepository protocol; Timescale implementation injected via DI.
- Enables writing unit tests with in-memory adapters.

3.3 Service layer for API routes
core/services/registry_service.py
class RegistryService:
    def __init__(self, registry_manager, orchestrator_facade):
        self._registry = registry_manager
        self._orchestrator = orchestrator_facade

    def list_symbols(self):
        return self._registry.list_symbols()

    async def trigger_cycle(self):
        await self._orchestrator.run_cycle()

api/rest/routes/registry.py
from fastapi import APIRouter, Depends
from core.services.registry_service import RegistryService

router = APIRouter()

@router.get("/registry")
def get_registry(service: RegistryService = Depends(get_registry_service)):
    return service.list_symbols()

@router.post("/backfill")
async def trigger_backfill(service: RegistryService = Depends(get_registry_service)):
    await service.trigger_cycle()
    return {"status": "queued"}
- FastAPI app now just wires dependencies.
- CLI/scheduler can reuse RegistryService for consistent behavior.

Phase 4 — CLEANUP (Status: ❌)

Why change?
- Dead modules (data_sources.py, ingestion_engine.py) confuse new contributors.
- Empty folders (backends, widgets, workers) suggest incomplete refactors.
- scraper.py duplicates CLI logic and blocks deprecation.
- Legacy code (core/orchestrator/legacy.py, core/services/scraper_service.py, etc.) lacks tests, preventing safe edits.

How to execute safely
1. Delete unused modules after verifying grep results:
   git grep -l "data_sources" | grep -v data_sources.py
   git rm data_sources.py ingestion_engine.py

2. Remove empty packages if no __init__.py logic:
   git rm -r gui/backends gui/widgets gui/workers

3. Deprecate legacy CLI:
   # cli/legacy_cli.py
   import warnings
   warnings.warn(
       "cli.legacy_cli is deprecated; please use cli.chronox_cli",
       DeprecationWarning,
       stacklevel=2,
   )
   - Add console banner when executed.
   - Update README and onboarding docs.

4. Add missing tests (focus on the most fragile code first):
   - tests/core/test_backfill_orchestrator.py: integration test using fixtures from tests/conftest.py.
   - tests/core/test_scraper_service.py: mock ThreadPoolExecutor to verify job lifecycle.
   - tests/api/test_routes.py: FastAPI TestClient verifying 200/400 cases.
   - Aim for >70% coverage on critical legacy components before altering logic.

5. Documentation refresh:
   - Update README.md and PRODUCTION_README.md with new tree layout.
   - Add architecture diagram showing CLI → Service Layer → Orchestrator → Storage.

Additional Ideas & Future Planning
1. Async-first storage: Once repositories are extracted, evaluate asyncpg + databases library to remove threadpool overhead (separate ticket).
2. Config normalization: centralize environment variable parsing in core/config.py, inject typed config objects everywhere (makes testing easier).
3. Metrics & Observability: after service layer extraction, add Prometheus counters for “bars_ingested”, “gap_windows_closed”, etc.
4. Migrate GUI to web: after Phase 2, consider replacing PyQt GUI with a thin web dashboard using FastAPI + WebSockets; current plan makes API the single data source.
5. Terraform / Flyway: schema manager currently runs DDL at runtime; Phase 2 extraction allows delegating to external migration tooling later.

Summary Checklist (All ❌ right now)
- Phase 1: folder structure migrated, imports updated.
- Phase 2: TimescaleWriter split; GUI decomposed; GUI no longer touches DB.
- Phase 3: façade + interfaces + service layer extracted.
- Phase 4: dead code removed, legacy CLI deprecated, tests added.

Complete these phases sequentially to transform the project from “big ball of mud” into a modular, testable system aligned with the refactor plan.