"""Manager-mediated on-demand materialization for stateless / C++ Agents (lazy).

A C++ Agent that hits a store miss under lazy mode posts the object key to the
Manager's ``/control/materialize``; the Manager materializes that table into the
shared artifact store (data + metadata) so the Agent can serve it. These tests
exercise the Manager side directly (the C++ client is validated by a build).
"""
from __future__ import annotations

import os
import pathlib

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import httpx

import config


async def _seed_and_configure(monkeypatch, db_path: pathlib.Path, store_dir: pathlib.Path,
                              table_format: str = "iceberg"):
    import db.executor as _executor
    import runtime.materializer as materializer
    from enterprise.control import materialize_service
    from iceberg.state_store import _snapshots, _history
    from delta import log as delta_log
    from demo.seed_db import seed_demo_database

    monkeypatch.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{db_path.as_posix()}")
    monkeypatch.setattr(config, "NUM_SPLITS", 4)
    monkeypatch.setattr(config, "BUCKET_NAME", "cpp-bucket")
    monkeypatch.setattr(config, "DB_SOURCE_TABLE", "sales")
    monkeypatch.setattr(config, "TABLE_FORMAT", table_format)
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "lazy")
    monkeypatch.setattr(config, "ARTIFACT_STORE_BACKEND", "local")
    monkeypatch.setattr(config, "ARTIFACT_STORE_DIR", store_dir.as_posix())
    monkeypatch.setattr(config, "TABLES", [config.TableDef(
        name="sales", source_table="sales", schema=config.TABLE_SCHEMA, num_splits=4)])

    await seed_demo_database()
    _executor._engine = None
    materializer._locks.clear()
    materialize_service.reset()
    _snapshots.clear()
    _history.clear()
    delta_log.reset()


def _teardown():
    import db.executor as _executor
    from iceberg.state_store import _snapshots, _history
    from delta import log as delta_log
    _snapshots.clear()
    _history.clear()
    delta_log.reset()
    return _executor


async def test_materialize_service_populates_store_iceberg(tmp_path, monkeypatch):
    from enterprise.control import materialize_service
    from iceberg.state_store import build_snapshot

    db = tmp_path / "src.db"
    store = tmp_path / "store"
    await _seed_and_configure(monkeypatch, db, store, "iceberg")

    # Deterministic keys (the service rebuilds the same registry internally).
    snap = build_snapshot(table_name="sales", num_splits=4, bucket="cpp-bucket",
                          warehouse_prefix=config.WAREHOUSE_PREFIX)
    data_keys = [s.object_key for s in snap.splits]

    result = await materialize_service.materialize_for_key(snap.metadata_key)
    assert result["ok"] is True and result["materialized"] is True

    # The shared store now holds data splits + Iceberg metadata for the C++ Agent.
    for key in data_keys + [snap.metadata_key, snap.manifest_file_key,
                            snap.manifest_list_key, snap.version_hint_key]:
        assert (store / key).exists(), key

    _executor = _teardown()
    if _executor._engine is not None:
        await _executor._engine.dispose()
        _executor._engine = None


async def test_materialize_service_populates_store_delta(tmp_path, monkeypatch):
    from enterprise.control import materialize_service
    from iceberg.state_store import build_snapshot

    db = tmp_path / "src.db"
    store = tmp_path / "store"
    await _seed_and_configure(monkeypatch, db, store, "delta")

    snap = build_snapshot(table_name="sales", num_splits=4, bucket="cpp-bucket",
                          warehouse_prefix=config.WAREHOUSE_PREFIX)
    log_key = f"{snap.table_path}/_delta_log/{0:020d}.json"

    result = await materialize_service.materialize_for_key(log_key)
    assert result["ok"] is True

    # Data splits and the _delta_log commit are in the store.
    for s in snap.splits:
        assert (store / s.object_key).exists()
    assert (store / log_key).exists()

    _executor = _teardown()
    if _executor._engine is not None:
        await _executor._engine.dispose()
        _executor._engine = None


async def test_materialize_service_unknown_key(tmp_path, monkeypatch):
    from enterprise.control import materialize_service

    db = tmp_path / "src.db"
    store = tmp_path / "store"
    await _seed_and_configure(monkeypatch, db, store, "iceberg")

    result = await materialize_service.materialize_for_key("cpp-bucket/nope/not/a/key.parquet")
    assert result["ok"] is False

    _executor = _teardown()
    if _executor._engine is not None:
        await _executor._engine.dispose()
        _executor._engine = None


async def test_control_materialize_endpoint(tmp_path, monkeypatch):
    from enterprise.control.manager_app import create_manager_app
    from iceberg.state_store import build_snapshot

    db = tmp_path / "src.db"
    store = tmp_path / "store"
    await _seed_and_configure(monkeypatch, db, store, "iceberg")
    monkeypatch.setattr(config, "ENABLE_GATEWAY", False, raising=False)

    snap = build_snapshot(table_name="sales", num_splits=4, bucket="cpp-bucket",
                          warehouse_prefix=config.WAREHOUSE_PREFIX)

    app = create_manager_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://mgr") as c:
        r = await c.post("/control/materialize", json={"key": snap.metadata_key})
        assert r.status_code == 200
        assert r.json()["ok"] is True

        missing = await c.post("/control/materialize", json={})
        assert missing.status_code == 400

    for s in snap.splits:
        assert (store / s.object_key).exists()

    _executor = _teardown()
    if _executor._engine is not None:
        await _executor._engine.dispose()
        _executor._engine = None
