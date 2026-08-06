"""MATERIALIZE_MODE=lazy — deferred, per-table materialization on first metadata read.

Verifies the lazy gate materializes a table's splits on demand and that the
manifest-declared size matches the served bytes, plus fail-closed validation for
the unsupported lazy combinations.
"""
from __future__ import annotations

import pathlib

import httpx
import pytest

import config
from main import app

_DB = pathlib.Path(__file__).parent / "test_lazy.db"


@pytest.fixture
async def lazy_client(monkeypatch):
    import db.executor as _executor
    import runtime.materializer as materializer
    from iceberg.state_store import build_snapshot, _snapshots, _history
    from demo.seed_db import seed_demo_database

    monkeypatch.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{_DB.as_posix()}")
    monkeypatch.setattr(config, "NUM_SPLITS", 4)
    monkeypatch.setattr(config, "BUCKET_NAME", "lazy-bucket")
    monkeypatch.setattr(config, "TABLE_NAME", "sales")
    monkeypatch.setattr(config, "DB_SOURCE_TABLE", "sales")
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg")
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "lazy")

    await seed_demo_database()
    _executor._engine = None
    materializer._locks.clear()

    snap = build_snapshot(
        table_name="sales", num_splits=4,
        bucket="lazy-bucket", warehouse_prefix=config.WAREHOUSE_PREFIX,
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c, snap

    if _executor._engine is not None:
        await _executor._engine.dispose()
        _executor._engine = None
    _snapshots.clear()
    _history.clear()
    if _DB.exists():
        _DB.unlink(missing_ok=True)


async def test_splits_unmaterialized_until_metadata_read(lazy_client):
    c, snap = lazy_client
    # Before any request the lazy snapshot carries no declared sizes.
    assert all(s.file_size_in_bytes is None for s in snap.splits)

    r = await c.get(f"/lazy-bucket/{snap.metadata_key}")
    assert r.status_code == 200

    # The metadata read triggered materialization of every split.
    assert all(s.file_size_in_bytes is not None for s in snap.splits)
    assert snap.total_records and snap.total_records > 0


async def test_manifest_size_matches_served_bytes(lazy_client):
    c, snap = lazy_client
    # Trigger lazy materialization via the metadata entry point.
    assert (await c.get(f"/lazy-bucket/{snap.metadata_key}")).status_code == 200

    # Each split's served bytes must match its declared file_size_in_bytes
    # (the value the Iceberg manifest publishes for footer-offset reads).
    for split in snap.splits:
        body = (await c.get(f"/lazy-bucket/{split.object_key}")).content
        assert len(body) == split.file_size_in_bytes


async def test_data_read_alone_materializes_table(lazy_client):
    c, snap = lazy_client
    # A direct data GET (no prior metadata read) also gates materialization so
    # the table's declared sizes are consistent afterwards.
    first = snap.splits[0]
    r = await c.get(f"/lazy-bucket/{first.object_key}")
    assert r.status_code == 200
    assert all(s.file_size_in_bytes is not None for s in snap.splits)


async def test_lazy_gate_is_idempotent(lazy_client):
    c, snap = lazy_client
    assert (await c.get(f"/lazy-bucket/{snap.metadata_key}")).status_code == 200
    sizes = [s.file_size_in_bytes for s in snap.splits]
    total = snap.total_records
    # A second read must not re-materialize or change the declared sizes.
    assert (await c.get(f"/lazy-bucket/{snap.metadata_key}")).status_code == 200
    assert [s.file_size_in_bytes for s in snap.splits] == sizes
    assert snap.total_records == total


# ---------------------------------------------------------------------------
# Fail-closed validation for unsupported lazy combinations
# ---------------------------------------------------------------------------

def test_lazy_rejects_delta(monkeypatch):
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "lazy")
    monkeypatch.setattr(config, "TABLE_FORMAT", "delta")
    with pytest.raises(ValueError, match="requires TABLE_FORMAT 'iceberg'"):
        config.validate_config()


def test_lazy_rejects_multi_shard(monkeypatch):
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "lazy")
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg")
    monkeypatch.setattr(config, "AGENT_SHARD_COUNT", 2)
    monkeypatch.setattr(config, "AGENT_SHARD_INDEX", 0)
    with pytest.raises(ValueError, match="requires a single shard"):
        config.validate_config()


def test_lazy_rejects_auto_refresh(monkeypatch):
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "lazy")
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg")
    monkeypatch.setattr(config, "AUTO_REFRESH", True)
    with pytest.raises(ValueError, match="incompatible with AUTO_REFRESH"):
        config.validate_config()


def test_invalid_materialize_mode_rejected(monkeypatch):
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "sometimes")
    with pytest.raises(ValueError, match="MATERIALIZE_MODE must be"):
        config.validate_config()
