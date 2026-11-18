import asyncio
from contextlib import closing
import json
import sqlite3
import time

from fastapi import FastAPI
import httpx
import pytest
from sqlalchemy.engine import URL

from enterprise.control.edge_client import EdgeReplayClient
from enterprise.control.edge_http import create_edge_router
from enterprise.control.edge_journal import EdgeJournal
from enterprise.control.edge_manager import EdgeManager
from enterprise.control.edge_receiver import EdgeBinding, EdgeReplayReceiver
from enterprise.control.edge_service import (
    BoundedEdgeStore, EdgeStorageLimitExceeded, _process_lock, run_edge_service,
)
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.config import ColumnDef, TableDef
from fabric_shortcut_proxy.db import executor
from fabric_shortcut_proxy.security.agent_auth import AgentAuthMiddleware
from tests.enterprise.test_federated_publication_runtime import _runtime


async def _until(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError("edge test condition was not reached")
        await asyncio.sleep(0.05)


async def test_service_refreshes_offline_then_replays_in_order(tmp_path, monkeypatch):
    source = tmp_path / "source.db"
    with closing(sqlite3.connect(source)) as db, db:
        db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO sales VALUES (1, 'offline')")
    monkeypatch.setattr(config, "DB_URL", str(URL.create("sqlite+aiosqlite", database=str(source))))
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg")
    monkeypatch.setattr(config, "BUCKET_NAME", "test-bucket")
    monkeypatch.setattr(config, "WAREHOUSE_PREFIX", "warehouse")
    monkeypatch.setattr(config, "OBJECT_PATH_LAYOUT", "legacy")
    monkeypatch.setattr(config, "ARTIFACT_STORE_SERVING", False)
    monkeypatch.setattr(config, "PARQUET_DISK_CACHE", False)
    monkeypatch.setattr(config, "PIN_MATERIALIZED_SPLITS", False)
    monkeypatch.setattr(config, "ICEBERG_MANIFEST_STATS", False)
    monkeypatch.setattr(executor, "_engine", None)
    monkeypatch.setenv("AGENT_AUTH_MODE", "required")
    monkeypatch.setenv("AGENT_TOKEN", "site-token-" + "x" * 32)
    monkeypatch.setenv("AGENT_IDENTITY_TOKENS", json.dumps({"site-1": "site-token-" + "x" * 32}))
    table = TableDef(
        name="sales", source_table="sales", num_splits=1, key_column="id",
        schema=[ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
                ColumnDef(field_id=2, name="name", iceberg_type="string", nullable=True)],
        split_strategy="modulo",
    )
    store = BoundedEdgeStore(str(tmp_path / "store"), 10_000_000)
    journal = EdgeJournal(tmp_path / "edge.db", dataset_id="default::sales")
    manager = EdgeManager(table, journal, store)
    catalog, _, _, primary, _ = _runtime()
    # Use the same configured source identity as the edge's default connection.
    primary.catalog.placement.connections["default"] = primary.catalog.placement.connections["source"]
    primary.catalog.create(
        "default::sales", owner_id="manager-swc", owner_location="azure:swedencentral",
        lease_ms=100_000, freshness_target_ms=5_000,
    )
    app = FastAPI()
    app.add_middleware(AgentAuthMiddleware)
    app.include_router(create_edge_router(EdgeReplayReceiver(
        primary, {"site-1": EdgeBinding("default::sales", "sales", "warehouse/sales")}
    )))

    class SwitchableTransport(httpx.AsyncBaseTransport):
        def __init__(self):
            self.online = False
            self.transport = httpx.ASGITransport(app=app)

        async def handle_async_request(self, request):
            if not self.online:
                raise httpx.ConnectError("site disconnected", request=request)
            return await self.transport.handle_async_request(request)

        async def aclose(self):
            await self.transport.aclose()

    transport = SwitchableTransport()
    replay = EdgeReplayClient("https://testserver", "site-1", transport=transport)
    stop = asyncio.Event()
    task = asyncio.create_task(run_edge_service(
        manager, replay, refresh_seconds=0.1, retry_seconds=0.1,
        max_pending=3, stop=stop,
    ))
    try:
        await _until(lambda: journal.pending_count() == 3)
        assert catalog.get("default::sales")["replicas"] == {}
        transport.online = True
        await _until(lambda: catalog.get("default::sales").get("edge_replay", {}).get("sequence", 0) >= 3)
        stop.set()
        await asyncio.wait_for(task, 10)
        assert catalog.get("default::sales")["edge_replay"]["sequence"] >= 3
    finally:
        stop.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await replay.aclose()
        await executor.dispose_engines()


def test_service_locks_and_store_limit(tmp_path):
    lock = tmp_path / "worker.lock"
    with _process_lock(lock):
        with pytest.raises(RuntimeError, match="lock unavailable"):
            with _process_lock(lock):
                pass
    with _process_lock(lock):
        pass
    store = BoundedEdgeStore(str(tmp_path / "store"), 5)
    store.put_stream_if_absent("data/one", [b"1234"], length=4)
    with pytest.raises(EdgeStorageLimitExceeded):
        store.put_stream_if_absent("data/two", [b"12"], length=2)
    assert store.head("data/two") is None
