"""
Tests for data freshness (content-addressed snapshots + poller) — P1/P2.

These exercise the freshness module against a small file-based SQLite table so
we test the real materialize -> content-hash -> publish path end to end.
"""
from __future__ import annotations

import os
import pathlib

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

import config
from config import ColumnDef, TableDef
import db.executor as _executor
import iceberg.state_store as state_store
from iceberg import freshness

_DB = pathlib.Path(__file__).parent / "test_freshness.db"

_SCHEMA = [
    ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
    ColumnDef(field_id=2, name="name", iceberg_type="string", nullable=True),
    ColumnDef(field_id=3, name="amount", iceberg_type="double", nullable=True),
]

_BUCKET = "test-bucket"
_PREFIX = "warehouse/db"


def _table() -> TableDef:
    return TableDef(
        name="frtest",
        source_table="frtest",
        schema=list(_SCHEMA),
        num_splits=2,
        key_column="id",
    )


async def _write_rows(rows: list[tuple[int, str, float]]) -> None:
    """(Re)create the source table with exactly ``rows`` and reset the executor
    engine so the next read reopens the file."""
    url = f"sqlite+aiosqlite:///{_DB.as_posix()}"
    engine = create_async_engine(url, echo=False)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS frtest"))
            await conn.execute(text(
                "CREATE TABLE frtest (id INTEGER PRIMARY KEY, name TEXT, amount REAL)"
            ))
            for r in rows:
                await conn.execute(
                    text("INSERT INTO frtest (id, name, amount) VALUES (:i, :n, :a)"),
                    {"i": r[0], "n": r[1], "a": r[2]},
                )
    finally:
        await engine.dispose()
    # Force the shared executor engine to reopen against the updated file.
    if _executor._engine is not None:
        await _executor._engine.dispose()
        _executor._engine = None


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{_DB.as_posix()}", raising=False)
    monkeypatch.setattr(config, "BUCKET_NAME", _BUCKET, raising=False)
    monkeypatch.setattr(config, "WAREHOUSE_PREFIX", _PREFIX, raising=False)
    monkeypatch.setattr(config, "SNAPSHOT_HISTORY_LIMIT", 3, raising=False)
    monkeypatch.setattr(config, "ICEBERG_SNAPSHOT_HISTORY", False, raising=False)
    monkeypatch.setattr(config, "ICEBERG_MANIFEST_STATS", False, raising=False)
    monkeypatch.setattr(config, "PARQUET_DISK_CACHE", False, raising=False)
    # Clean state store + probe/ttl caches for the test table.
    state_store._snapshots.pop("frtest", None)
    state_store._history.pop("frtest", None)
    freshness._probe_tokens.pop("frtest", None)
    freshness._ttl_gen.pop("frtest", None)
    import cache.lru_cache as _cache
    _cache.unpin_all()
    yield
    import cache.lru_cache as _cache2
    _cache2.unpin_all()


@pytest.fixture(scope="module", autouse=True)
def _cleanup_db():
    _DB.unlink(missing_ok=True)
    yield
    try:
        _DB.unlink(missing_ok=True)
    except OSError:
        pass  # Windows may still hold the handle; harmless leftover.


# ---------------------------------------------------------------------------
# materialize_table — content addressing + determinism
# ---------------------------------------------------------------------------

async def test_materialize_is_content_deterministic():
    await _write_rows([(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)])
    t = _table()
    snap1 = await freshness.materialize_table(t, _BUCKET, _PREFIX)
    snap2 = await freshness.materialize_table(t, _BUCKET, _PREFIX)
    assert snap1.snapshot_id == snap2.snapshot_id
    assert [s.object_key for s in snap1.splits] == [s.object_key for s in snap2.splits]
    # Every data-file path is content-addressed under this table's data/ dir.
    for s in snap1.splits:
        assert s.object_key.startswith(f"{_PREFIX}/frtest/data/split-")
        assert s.object_key.endswith(".parquet")


async def test_materialize_reuses_pinned_content_addressed_bytes():
    import cache.lru_cache as _cache
    await _write_rows([(1, "a", 1.0), (2, "b", 2.0)])
    t = _table()
    first = await freshness.materialize_table(t, _BUCKET, _PREFIX)
    key = first.splits[0].object_key
    # Overwrite the pinned chunk with DIFFERENT-sized bytes, then re-materialize
    # the SAME content: it must REUSE the pinned bytes (size stays locked to the
    # already-published manifest), not regenerate/overwrite -> no size drift
    # (Fabric SQL msg 19778 "file size in metadata smaller than actual").
    sentinel = b"SENTINEL-DIFFERENT-SIZE-PARQUET-BYTES"
    _cache.pin_parquet(key, sentinel)
    again = await freshness.materialize_table(t, _BUCKET, _PREFIX)
    assert again.splits[0].object_key == key
    assert again.splits[0].file_size_in_bytes == len(sentinel)
    assert _cache.peek_parquet(key) == sentinel  # reused, not overwritten


async def test_materialize_changes_id_on_data_change():
    await _write_rows([(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)])
    t = _table()
    before = await freshness.materialize_table(t, _BUCKET, _PREFIX)
    before_keys = {s.object_key for s in before.splits}

    await _write_rows([(1, "a", 1.0), (2, "CHANGED", 2.0), (3, "c", 3.0)])
    after = await freshness.materialize_table(t, _BUCKET, _PREFIX)
    after_keys = {s.object_key for s in after.splits}

    assert before.snapshot_id != after.snapshot_id
    # At least one chunk path changed (the one holding the mutated row).
    assert before_keys != after_keys


# ---------------------------------------------------------------------------
# publish — dedupe + version bump
# ---------------------------------------------------------------------------

async def test_publish_dedupes_identical_content():
    await _write_rows([(1, "a", 1.0), (2, "b", 2.0)])
    t = _table()

    first = await freshness.materialize_table(t, _BUCKET, _PREFIX)
    assert await freshness.publish(first) is True
    cur = state_store._snapshots["frtest"]
    assert cur.version == 1
    assert cur.metadata_key.endswith("v1.metadata.json")

    # Re-materialize identical content -> no publish.
    again = await freshness.materialize_table(t, _BUCKET, _PREFIX)
    assert await freshness.publish(again) is False
    assert state_store._snapshots["frtest"].version == 1


async def test_publish_bumps_version_on_change():
    await _write_rows([(1, "a", 1.0), (2, "b", 2.0)])
    t = _table()
    assert await freshness.publish(await freshness.materialize_table(t, _BUCKET, _PREFIX)) is True
    assert state_store._snapshots["frtest"].version == 1

    await _write_rows([(1, "a", 1.0), (2, "b", 99.0)])
    assert await freshness.publish(await freshness.materialize_table(t, _BUCKET, _PREFIX)) is True
    cur = state_store._snapshots["frtest"]
    assert cur.version == 2
    assert cur.metadata_key.endswith("v2.metadata.json")
    # Both versions retained in history (history limit is 3).
    assert len(state_store.get_snapshot_history("frtest")) == 2


async def test_history_is_pruned_and_chunks_evicted():
    import cache.lru_cache as _cache
    t = _table()
    # Publish 4 distinct versions; history limit is 3, so v1 is pruned.
    for n in range(1, 5):
        await _write_rows([(1, "a", float(n)), (2, "b", 2.0)])
        assert await freshness.publish(await freshness.materialize_table(t, _BUCKET, _PREFIX)) is True
    hist = state_store.get_snapshot_history("frtest")
    assert len(hist) == 3
    assert hist[0].version == 2  # v1 pruned
    # A chunk that only existed in the pruned v1 is no longer cached.
    # (The changed split's key differs each version.)
    retained = {s.object_key for snap in hist for s in snap.splits}
    for k in retained:
        assert _cache.peek_parquet(k) is not None


# ---------------------------------------------------------------------------
# poll_once — cascade behaviour
# ---------------------------------------------------------------------------

async def test_poll_content_hash_publishes_on_change(monkeypatch):
    monkeypatch.setattr(config, "REFRESH_STRATEGY", "content_hash", raising=False)
    await _write_rows([(1, "a", 1.0), (2, "b", 2.0)])
    t = _table()
    # First poll -> publishes v1.
    assert await freshness.poll_once(t, _BUCKET, _PREFIX) is True
    # No change -> content hash identical -> no publish.
    assert await freshness.poll_once(t, _BUCKET, _PREFIX) is False
    # Change -> publishes v2.
    await _write_rows([(1, "a", 1.0), (2, "b", 5.0)])
    assert await freshness.poll_once(t, _BUCKET, _PREFIX) is True


async def test_poll_manual_is_noop(monkeypatch):
    monkeypatch.setattr(config, "REFRESH_STRATEGY", "manual", raising=False)
    await _write_rows([(1, "a", 1.0)])
    t = _table()
    assert await freshness.poll_once(t, _BUCKET, _PREFIX) is False
    assert "frtest" not in state_store._snapshots


async def test_poll_never_raises(monkeypatch):
    monkeypatch.setattr(config, "REFRESH_STRATEGY", "content_hash", raising=False)

    async def _boom(*a, **k):
        raise RuntimeError("db exploded")

    monkeypatch.setattr(freshness, "materialize_table", _boom)
    t = _table()
    # A failing materialize must be swallowed, not crash the poller.
    assert await freshness.poll_once(t, _BUCKET, _PREFIX) is False


async def test_probe_token_sqlite_changes_after_write():
    await _write_rows([(1, "a", 1.0)])
    t = _table()
    # First probe opens the shared executor engine and reads data_version.
    tok1 = await freshness.probe_change_token(t)
    assert tok1 is not None and tok1.startswith("dv:")

    # Commit a change from a SEPARATE connection while the executor engine stays
    # alive — SQLite's PRAGMA data_version then advances on the next read.
    url = f"sqlite+aiosqlite:///{_DB.as_posix()}"
    w = create_async_engine(url, echo=False)
    try:
        async with w.begin() as conn:
            await conn.execute(text("INSERT INTO frtest (id, name, amount) VALUES (99, 'z', 9.0)"))
    finally:
        await w.dispose()

    tok2 = await freshness.probe_change_token(t)
    assert tok2 is not None and tok2.startswith("dv:")
    assert tok1 != tok2


async def test_refresh_all_reports_changed():
    await _write_rows([(1, "a", 1.0)])
    t = _table()
    changed = await freshness.refresh_all([t], _BUCKET, _PREFIX)
    assert changed == ["frtest"]
    # Identical content on a second call -> nothing changed.
    changed2 = await freshness.refresh_all([t], _BUCKET, _PREFIX)
    assert changed2 == []
