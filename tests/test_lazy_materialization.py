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
_DDB = pathlib.Path(__file__).parent / "test_lazy_delta.db"
_VDB = pathlib.Path(__file__).parent / "test_virtual.db"


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
# Delta lazy (phase 2)
# ---------------------------------------------------------------------------

@pytest.fixture
async def lazy_delta_client(monkeypatch):
    import db.executor as _executor
    import runtime.materializer as materializer
    from iceberg.state_store import build_snapshot, _snapshots, _history
    from demo.seed_db import seed_demo_database
    from delta import log as delta_log

    monkeypatch.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{_DDB.as_posix()}")
    monkeypatch.setattr(config, "NUM_SPLITS", 4)
    monkeypatch.setattr(config, "BUCKET_NAME", "lazy-bucket")
    monkeypatch.setattr(config, "TABLE_NAME", "sales")
    monkeypatch.setattr(config, "DB_SOURCE_TABLE", "sales")
    monkeypatch.setattr(config, "TABLE_FORMAT", "delta")
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "lazy")

    await seed_demo_database()
    _executor._engine = None
    materializer._locks.clear()
    delta_log.reset()

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
    delta_log.reset()
    if _DDB.exists():
        _DDB.unlink(missing_ok=True)


async def test_delta_lazy_materializes_on_log_read(lazy_delta_client):
    c, snap = lazy_delta_client
    assert all(s.file_size_in_bytes is None for s in snap.splits)

    log_key = f"{snap.table_path}/_delta_log/{0:020d}.json"
    r = await c.get(f"/lazy-bucket/{log_key}")
    assert r.status_code == 200
    assert all(s.file_size_in_bytes is not None for s in snap.splits)


async def test_delta_lazy_commit_size_matches_served_bytes(lazy_delta_client):
    import json

    c, snap = lazy_delta_client
    log_key = f"{snap.table_path}/_delta_log/{0:020d}.json"
    commit = (await c.get(f"/lazy-bucket/{log_key}")).text

    adds = []
    for line in commit.splitlines():
        if not line.strip():
            continue
        action = json.loads(line)
        if "add" in action:
            adds.append(action["add"])
    # Commit 0 declares every split as an add with its true size.
    assert len(adds) == len(snap.splits)
    for add in adds:
        body = (await c.get(f"/lazy-bucket/{snap.table_path}/{add['path']}")).content
        assert len(body) == add["size"]


# ---------------------------------------------------------------------------
# Virtual (zero-persistence) — regenerate deterministically on demand
# ---------------------------------------------------------------------------

@pytest.fixture
async def virtual_client(monkeypatch):
    import db.executor as _executor
    import runtime.materializer as materializer
    import cache.lru_cache as cache
    from iceberg.state_store import build_snapshot, _snapshots, _history
    from demo.seed_db import seed_demo_database

    monkeypatch.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{_VDB.as_posix()}")
    monkeypatch.setattr(config, "NUM_SPLITS", 4)
    monkeypatch.setattr(config, "BUCKET_NAME", "virtual-bucket")
    monkeypatch.setattr(config, "TABLE_NAME", "sales")
    monkeypatch.setattr(config, "DB_SOURCE_TABLE", "sales")
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg")
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "virtual")

    await seed_demo_database()
    _executor._engine = None
    materializer._locks.clear()
    cache._pinned.clear()

    snap = build_snapshot(
        table_name="sales", num_splits=4,
        bucket="virtual-bucket", warehouse_prefix=config.WAREHOUSE_PREFIX,
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
    if _VDB.exists():
        _VDB.unlink(missing_ok=True)


async def test_virtual_materializes_without_pinning(virtual_client):
    import cache.lru_cache as cache
    c, snap = virtual_client
    assert (await c.get(f"/virtual-bucket/{snap.metadata_key}")).status_code == 200
    # Sizes are known (the manifest is correct) but NO split is pinned at rest.
    assert all(s.file_size_in_bytes is not None for s in snap.splits)
    assert all(s.object_key not in cache._pinned for s in snap.splits)


async def test_virtual_manifest_size_matches_served_bytes(virtual_client):
    c, snap = virtual_client
    assert (await c.get(f"/virtual-bucket/{snap.metadata_key}")).status_code == 200
    for split in snap.splits:
        body = (await c.get(f"/virtual-bucket/{split.object_key}")).content
        assert len(body) == split.file_size_in_bytes


async def test_virtual_regenerates_byte_identical_after_eviction(virtual_client):
    import cache.lru_cache as cache
    c, snap = virtual_client
    assert (await c.get(f"/virtual-bucket/{snap.metadata_key}")).status_code == 200
    split = snap.splits[0]
    first = (await c.get(f"/virtual-bucket/{split.object_key}")).content
    # Evict so the next read regenerates the split from SQL.
    cache._parquet_cache._evict(split.object_key)
    cache._pinned.pop(split.object_key, None)
    second = (await c.get(f"/virtual-bucket/{split.object_key}")).content
    assert first == second
    assert len(first) == split.file_size_in_bytes


# ---------------------------------------------------------------------------
# Fail-closed validation for unsupported lazy combinations
# ---------------------------------------------------------------------------

def _validation_problems() -> str:
    try:
        config.validate_config()
        return ""
    except ValueError as exc:
        return str(exc)


def test_lazy_accepts_delta(monkeypatch):
    # Phase 2: Delta lazy is supported; no MATERIALIZE_MODE problem should appear.
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "lazy")
    monkeypatch.setattr(config, "TABLE_FORMAT", "delta")
    assert "MATERIALIZE_MODE" not in _validation_problems()


def test_lazy_rejects_multi_shard_without_shared_store(monkeypatch):
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "lazy")
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg")
    monkeypatch.setattr(config, "AGENT_SHARD_COUNT", 2)
    monkeypatch.setattr(config, "AGENT_SHARD_INDEX", 0)
    monkeypatch.setattr(config, "ARTIFACT_STORE_SERVING", False)
    with pytest.raises(ValueError, match="requires ARTIFACT_STORE_SERVING"):
        config.validate_config()


def test_lazy_accepts_multi_shard_with_shared_store(monkeypatch):
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "lazy")
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg")
    monkeypatch.setattr(config, "AGENT_SHARD_COUNT", 2)
    monkeypatch.setattr(config, "AGENT_SHARD_INDEX", 0)
    monkeypatch.setattr(config, "ARTIFACT_STORE_SERVING", True)
    assert "ARTIFACT_STORE_SERVING" not in _validation_problems()


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


def test_virtual_accepts_iceberg_and_delta(monkeypatch):
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "virtual")
    for fmt in ("iceberg", "delta"):
        monkeypatch.setattr(config, "TABLE_FORMAT", fmt)
        assert "MATERIALIZE_MODE" not in _validation_problems()


def test_virtual_multishard_needs_no_shared_store(monkeypatch):
    # Virtual regenerates deterministically per agent, so multi-shard is consistent
    # without a shared store (unlike lazy).
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "virtual")
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg")
    monkeypatch.setattr(config, "AGENT_SHARD_COUNT", 2)
    monkeypatch.setattr(config, "AGENT_SHARD_INDEX", 0)
    monkeypatch.setattr(config, "ARTIFACT_STORE_SERVING", False)
    assert "ARTIFACT_STORE_SERVING" not in _validation_problems()


def test_virtual_rejects_auto_refresh(monkeypatch):
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "virtual")
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg")
    monkeypatch.setattr(config, "AUTO_REFRESH", True)
    with pytest.raises(ValueError, match="incompatible with AUTO_REFRESH"):
        config.validate_config()


# ---------------------------------------------------------------------------
# Cluster ownership (pure)
# ---------------------------------------------------------------------------

def test_owns_split_single_shard(monkeypatch):
    import runtime.materializer as m

    class _S:
        def __init__(self, i): self.split_index = i

    monkeypatch.setattr(config, "AGENT_SHARD_COUNT", 1)
    assert m._owns_split(_S(0)) and m._owns_split(_S(3))


def test_owns_split_modulo(monkeypatch):
    import runtime.materializer as m

    class _S:
        def __init__(self, i): self.split_index = i

    monkeypatch.setattr(config, "AGENT_SHARD_COUNT", 2)
    monkeypatch.setattr(config, "AGENT_SHARD_INDEX", 0)
    assert m._owns_split(_S(0)) and not m._owns_split(_S(1))
    monkeypatch.setattr(config, "AGENT_SHARD_INDEX", 1)
    assert not m._owns_split(_S(0)) and m._owns_split(_S(1))
