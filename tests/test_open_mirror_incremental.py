"""Phase 3 + 5 — incremental change stream, drop, quarantine, dry-run, scheduler.

Uses a real seeded SQLite source and a local landing zone (no mocks). Verifies the
diff engine, the state store, retry-safe publishing, table drops, per-table
quarantine, dry-run, and the background scheduler's one-shot cycle.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import pathlib

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

import config
from config import ColumnDef
from db import executor
from open_mirror import source as om_source
from open_mirror import target_from_dict
from open_mirror.changes import RowMarker, compute_changes
from open_mirror.cleanup import cleanup_target, inspect_cleanup
from open_mirror.landing_zone import LocalLandingZone
from open_mirror.publisher import ROW_MARKER_COLUMN, LandingZonePublisher
from open_mirror.state import (
    CommittedCursor,
    PendingBatch,
    PublishState,
    build_state_from_rows,
    load_state,
    save_state,
    state_file_path,
)

_COLUMNS = [
    ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
    ColumnDef(field_id=2, name="name", iceberg_type="string", nullable=True),
    ColumnDef(field_id=3, name="status", iceberg_type="string", nullable=True),
]


# --- diff engine (pure) ----------------------------------------------------

def _row(i, name, status="active"):
    return {"id": i, "name": name, "status": status}


def test_compute_changes_insert_update_delete():
    prev = build_state_from_rows([_row(1, "a"), _row(2, "b"), _row(3, "c")], _COLUMNS, ["id"])
    current = [_row(1, "a"), _row(2, "b2"), _row(4, "d")]  # 2 changed, 3 deleted, 4 inserted
    batch = compute_changes(prev, current, _COLUMNS, ["id"])
    assert batch.inserts == 1 and batch.updates == 1 and batch.deletes == 1
    by_id = {r["id"]: m for r, m in zip(batch.rows, batch.markers)}
    assert by_id[4] == RowMarker.INSERT
    assert by_id[2] == RowMarker.UPDATE
    assert by_id[3] == RowMarker.DELETE


def test_compute_changes_no_changes():
    rows = [_row(1, "a"), _row(2, "b")]
    prev = build_state_from_rows(rows, _COLUMNS, ["id"])
    batch = compute_changes(prev, rows, _COLUMNS, ["id"])
    assert not batch.has_changes


def test_compute_changes_default_upsert():
    prev = PublishState()
    batch = compute_changes(prev, [_row(1, "a")], _COLUMNS, ["id"], default_upsert=True)
    assert batch.markers == [RowMarker.UPSERT]


# --- publisher: rowMarker + drop -------------------------------------------

def _target(root):
    return target_from_dict({
        "id": "t1", "connection": "default", "landing_zone_root": str(root), "source_type": "SQL",
        "tables": [{"name": "sales", "source_table": "sales", "target_table": "sales",
                    "key_column": "id", "schema": "dbo"}],
    })


def test_publish_changes_writes_marker_file(tmp_path):
    target = _target(tmp_path)
    pub = LandingZonePublisher(LocalLandingZone(str(tmp_path)), target)
    pub.publish_changes(target.tables[0], [_row(1, "a"), {"id": 2}], [RowMarker.UPDATE, RowMarker.DELETE], _COLUMNS)
    table = pq.read_table(tmp_path / "dbo.schema" / "sales" / "00000000000000000001.parquet")
    assert table.schema.names[-1] == ROW_MARKER_COLUMN
    assert table.column(ROW_MARKER_COLUMN).to_pylist() == [1, 2]


def test_drop_table_removes_folder(tmp_path):
    target = _target(tmp_path)
    pub = LandingZonePublisher(LocalLandingZone(str(tmp_path)), target)
    pub.publish_initial_load(target.tables[0], [_row(1, "a")], _COLUMNS)
    assert (tmp_path / "dbo.schema" / "sales").is_dir()
    pub.drop_table(target.tables[0])
    assert not (tmp_path / "dbo.schema" / "sales").exists()


def test_cleanup_inspects_and_deletes_only_expired_ready_files(tmp_path):
    target = _target(tmp_path)
    ready = tmp_path / "dbo.schema" / "sales" / "_FilesReadyToDelete"
    ready.mkdir(parents=True)
    old = ready / "old.parquet"
    old.write_bytes(b"old")
    old_time = (dt.datetime.now(dt.UTC) - dt.timedelta(days=8)).timestamp()
    os.utime(old, (old_time, old_time))

    candidate = inspect_cleanup(target, target.tables[0])
    assert candidate.eligible is True
    assert candidate.eligible_file_paths == ("dbo.schema/sales/_FilesReadyToDelete/old.parquet",)
    result = cleanup_target(target, execute=True)
    assert result["deleted"] == ["dbo.schema/sales/_FilesReadyToDelete/old.parquet"]
    assert not ready.exists()


def test_cleanup_dry_run_preserves_recent_ready_files(tmp_path):
    target = _target(tmp_path)
    ready = tmp_path / "dbo.schema" / "sales" / "_FilesReadyToDelete"
    ready.mkdir(parents=True)
    (ready / "recent.parquet").write_bytes(b"recent")

    result = cleanup_target(target)
    assert result["execute"] is False
    assert result["deleted"] == []
    assert ready.exists()


def test_cleanup_deletes_nested_ready_files(tmp_path):
    target = _target(tmp_path)
    ready = tmp_path / "dbo.schema" / "sales" / "_FilesReadyToDelete" / "batch"
    ready.mkdir(parents=True)
    old = ready / "old.parquet"
    old.write_bytes(b"old")
    old_time = (dt.datetime.now(dt.UTC) - dt.timedelta(days=8)).timestamp()
    os.utime(old, (old_time, old_time))

    result = cleanup_target(target, execute=True)
    assert result["deleted"] == ["dbo.schema/sales/_FilesReadyToDelete/batch/old.parquet"]
    assert not (tmp_path / "dbo.schema" / "sales" / "_FilesReadyToDelete").exists()


def test_cleanup_deletes_old_files_and_keeps_recent_files(tmp_path):
    target = _target(tmp_path)
    ready = tmp_path / "dbo.schema" / "sales" / "_FilesReadyToDelete"
    ready.mkdir(parents=True)
    old = ready / "old.parquet"
    recent = ready / "recent.parquet"
    old.write_bytes(b"old")
    recent.write_bytes(b"recent")
    now = dt.datetime(2026, 8, 19, 17, 0, tzinfo=dt.UTC)
    old_time = (now - dt.timedelta(days=8)).timestamp()
    recent_time = (now - dt.timedelta(hours=1)).timestamp()
    os.utime(old, (old_time, old_time))
    os.utime(recent, (recent_time, recent_time))

    candidate = inspect_cleanup(target, target.tables[0], now=now)
    assert candidate.eligible is True
    assert candidate.retained_file_count == 1
    assert candidate.reason == "retention_elapsed"

    result = cleanup_target(target, execute=True)
    assert result["deleted"] == ["dbo.schema/sales/_FilesReadyToDelete/old.parquet"]
    assert not old.exists()
    assert recent.exists()
    assert ready.exists()


def test_cleanup_keeps_files_without_timestamps(tmp_path):
    target = _target(tmp_path)

    class Backend:
        def __init__(self):
            self.entries = {
                "dbo.schema/sales/_FilesReadyToDelete": [
                    {"name": "unknown.parquet", "is_directory": False,
                     "last_modified": None, "content_length": 1},
                ],
            }
            self.deleted = []

        def list_entries(self, path):
            return self.entries.get(path, [])

        def delete(self, path):
            self.deleted.append(path)

        def remove_tree(self, path):
            self.entries.pop(path, None)

        def exists(self, path):
            return path in self.entries

    backend = Backend()
    candidate = inspect_cleanup(target, target.tables[0], backend=backend)
    assert candidate.eligible is False
    assert candidate.retained_file_count == 1
    assert candidate.reason == "file_timestamp_unavailable"
    assert backend.deleted == []


# --- end-to-end incremental against a live SQLite source -------------------

def _seed(path: pathlib.Path, rows):
    eng = create_engine(f"sqlite:///{path.as_posix()}")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE sales (id INTEGER PRIMARY KEY, name TEXT, status TEXT)"))
        for r in rows:
            c.execute(text("INSERT INTO sales (id,name,status) VALUES (:i,:n,:s)"),
                      {"i": r[0], "n": r[1], "s": r[2]})
    eng.dispose()


def _apply(path: pathlib.Path, sql, params=None):
    eng = create_engine(f"sqlite:///{path.as_posix()}")
    with eng.begin() as c:
        c.execute(text(sql), params or {})
    eng.dispose()


@pytest.fixture
async def sqlite_src(tmp_path, monkeypatch):
    db = tmp_path / "src.db"
    _seed(db, [(1, "a", "active"), (2, "b", "active"), (3, "c", "active")])
    monkeypatch.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{db.as_posix()}", raising=False)
    monkeypatch.setattr(config, "OPEN_MIRROR_STATE_DIR", str(tmp_path / "state"), raising=False)
    executor._engine = None
    executor._sync_engine = None
    yield tmp_path, db
    await executor.dispose_engines()


async def test_incremental_cycle_initial_then_diff(sqlite_src):
    tmp_path, db = sqlite_src
    landing = tmp_path / "lz"
    target = _target(landing)
    table = target.tables[0]

    first = await om_source.publish_table(target, table, mode="incremental")
    assert first.action == "initial" and first.rows == 3
    state = load_state(str(tmp_path / "state"), target, table).state
    assert state.published_rows_total == 3
    assert state.last_batch_rows == 3
    assert state.last_published_at

    # No source change -> no-op, no new file.
    noop = await om_source.publish_table(target, table, mode="incremental")
    assert noop.action == "noop"

    # Mutate the source: update id=2, delete id=3, insert id=4.
    _apply(db, "UPDATE sales SET status='inactive' WHERE id=2")
    _apply(db, "DELETE FROM sales WHERE id=3")
    _apply(db, "INSERT INTO sales (id,name,status) VALUES (4,'d','active')")

    inc = await om_source.publish_table(target, table, mode="incremental")
    assert inc.action == "incremental"
    assert inc.inserts == 1 and inc.updates == 1 and inc.deletes == 1
    state = load_state(str(tmp_path / "state"), target, table).state
    assert state.published_rows_total == 6
    assert state.last_batch_rows == 3
    from monitor.router import _landing_zone_rows
    assert _landing_zone_rows(target, table) == 6

    change_file = landing / "dbo.schema" / "sales" / "00000000000000000002.parquet"
    t = pq.read_table(change_file)
    assert t.schema.names[-1] == ROW_MARKER_COLUMN


async def test_dry_run_writes_nothing(sqlite_src):
    tmp_path, db = sqlite_src
    landing = tmp_path / "lz"
    target = _target(landing)
    res = await om_source.publish_table(target, target.tables[0], dry_run=True)
    assert res.action == "dry_run" and res.rows == 3
    assert not (landing / "dbo.schema" / "sales").exists()


async def test_publish_target_quarantines_bad_table(sqlite_src):
    tmp_path, db = sqlite_src
    landing = tmp_path / "lz"
    target = target_from_dict({
        "id": "t1", "connection": "default", "landing_zone_root": str(landing), "source_type": "SQL",
        "tables": [
            {"name": "sales", "source_table": "sales", "target_table": "sales", "key_column": "id", "schema": "dbo"},
            {"name": "ghost", "source_table": "does_not_exist", "target_table": "ghost", "key_column": "id"},
        ],
    })
    result = await om_source.publish_target(target)
    actions = {r.table: r.action for r in result.results}
    assert actions["sales"] == "initial"
    assert actions["ghost"] == "error"
    assert not result.ok  # one table failed, but sales still published
    assert (landing / "dbo.schema" / "sales").is_dir()


async def test_publish_target_reconciles_drops(sqlite_src):
    tmp_path, db = sqlite_src
    landing = tmp_path / "lz"
    state_dir = str(tmp_path / "state")

    two = target_from_dict({
        "id": "t1", "connection": "default", "landing_zone_root": str(landing), "source_type": "SQL",
        "tables": [
            {"name": "sales", "source_table": "sales", "target_table": "sales", "key_column": "id", "schema": "dbo"},
            {"name": "sales2", "source_table": "sales", "target_table": "sales2", "key_column": "id", "schema": "dbo"},
        ],
    })
    await om_source.publish_target(two, state_dir=state_dir)
    assert (landing / "dbo.schema" / "sales2").is_dir()

    one = _target(landing)  # sales2 removed
    result = await om_source.publish_target(one, state_dir=state_dir)
    assert any("sales2" in d for d in result.dropped)
    assert not (landing / "dbo.schema" / "sales2").exists()
    assert (landing / "dbo.schema" / "sales").is_dir()


async def test_scheduler_run_cycle(sqlite_src, monkeypatch):
    tmp_path, db = sqlite_src
    landing = tmp_path / "lz"
    target = _target(landing)
    monkeypatch.setattr("open_mirror.config.load_targets", lambda *a, **k: [target])
    from open_mirror.scheduler import run_cycle
    results = await run_cycle()
    assert results[0].results[0].action == "initial"
    assert (landing / "dbo.schema" / "sales" / "00000000000000000001.parquet").exists()


# --- watermark mode --------------------------------------------------------

import datetime as _dt

from open_mirror.source import _max_watermark, _select_ordered_sql, _select_since_sql
from open_mirror.state import decode_watermark, encode_watermark
from planner.dialects import _MSSQL, _ORACLE, _SQLITE


def test_watermark_encode_decode_roundtrip():
    for v in [7, 3.5, "abc", _dt.datetime(2026, 8, 12, 9, 30), _dt.date(2026, 8, 12), b"\x00\x08"]:
        assert decode_watermark(encode_watermark(v)) == v
    assert encode_watermark(None) is None
    assert decode_watermark(None) is None


def test_max_watermark_ignores_nulls():
    rows = [{"ver": 3}, {"ver": None}, {"ver": 9}, {"ver": 5}]
    assert _max_watermark(rows, "ver") == 9
    assert _max_watermark([{"ver": None}], "ver") is None


def test_select_since_sql_per_dialect():
    assert "WHERE \"ver\" > :wm" in _select_since_sql(_SQLITE, '"id"', '"t"', '"ver"', wm_param="wm")
    assert _select_since_sql(_SQLITE, '"id"', '"t"', '"ver"', wm_param="wm").endswith("LIMIT :max_rows")
    assert _select_since_sql(_MSSQL, "[id]", "[t]", "[ver]", wm_param="wm").startswith("SELECT TOP (:max_rows)")
    assert "FETCH FIRST :max_rows ROWS ONLY" in _select_ordered_sql(_ORACLE, '"id"', '"t"', '"ver"')


def test_mode_precedence_invocation_then_table_then_global(monkeypatch):
    global_table = target_from_dict({
        "id": "t", "landing_zone_root": "lz",
        "tables": [{
            "name": "sales", "source_table": "sales",
            "target_table": "sales", "key_column": "id",
        }],
    }).tables[0]
    table_override = target_from_dict({
        "id": "t", "landing_zone_root": "lz",
        "tables": [{
            "name": "sales", "source_table": "sales",
            "target_table": "sales", "key_column": "id", "mode": "snapshot",
        }],
    }).tables[0]
    monkeypatch.setattr(config, "OPEN_MIRROR_MODE", "watermark", raising=False)

    assert om_source._effective_mode(global_table, None) == "watermark"
    assert om_source._effective_mode(table_override, None) == "snapshot"
    assert om_source._effective_mode(table_override, "initial") == "initial"


def _seed_wm(path, rows):
    eng = create_engine(f"sqlite:///{path.as_posix()}")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE sales (id INTEGER PRIMARY KEY, name TEXT, ver INTEGER)"))
        for r in rows:
            c.execute(text("INSERT INTO sales (id,name,ver) VALUES (:i,:n,:v)"),
                      {"i": r[0], "n": r[1], "v": r[2]})
    eng.dispose()


@pytest.fixture
async def sqlite_wm(tmp_path, monkeypatch):
    db = tmp_path / "wm.db"
    _seed_wm(db, [(1, "a", 1), (2, "b", 2), (3, "c", 3)])
    monkeypatch.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{db.as_posix()}", raising=False)
    monkeypatch.setattr(config, "OPEN_MIRROR_STATE_DIR", str(tmp_path / "state"), raising=False)
    executor._engine = None
    executor._sync_engine = None
    yield tmp_path, db
    await executor.dispose_engines()


def _wm_target(landing):
    return target_from_dict({
        "id": "t1", "connection": "default", "landing_zone_root": str(landing), "source_type": "SQL",
        "tables": [{"name": "sales", "source_table": "sales", "target_table": "sales",
                    "key_column": "id", "schema": "dbo", "watermark_column": "ver"}],
    })


async def test_watermark_initial_then_incremental_upserts(sqlite_wm):
    tmp_path, db = sqlite_wm
    landing = tmp_path / "lz"
    state_dir = str(tmp_path / "state")
    target = _wm_target(landing)
    table = target.tables[0]

    first = await om_source.publish_table(target, table, mode="incremental")
    assert first.action == "initial" and first.rows == 3
    assert decode_watermark(load_state(state_dir, target, table).watermark) == 3

    # Bump an existing row's watermark and insert a new higher-watermark row.
    _apply(db, "UPDATE sales SET name='a2', ver=10 WHERE id=1")
    _apply(db, "INSERT INTO sales (id,name,ver) VALUES (4,'d',11)")

    inc = await om_source.publish_table(target, table, mode="incremental")
    assert inc.action == "incremental" and inc.rows == 2  # only ver>3

    change = pq.read_table(landing / "dbo.schema" / "sales" / "00000000000000000002.parquet")
    assert change.schema.names[-1] == ROW_MARKER_COLUMN
    assert set(change.column(ROW_MARKER_COLUMN).to_pylist()) == {RowMarker.UPSERT}
    assert decode_watermark(load_state(state_dir, target, table).watermark) == 11


async def test_watermark_no_new_rows_is_noop(sqlite_wm):
    tmp_path, db = sqlite_wm
    landing = tmp_path / "lz"
    target = _wm_target(landing)
    table = target.tables[0]
    await om_source.publish_table(target, table)          # initial (watermark=3)
    noop = await om_source.publish_table(target, table)   # nothing past 3
    assert noop.action == "noop"


async def test_watermark_does_not_detect_deletes(sqlite_wm):
    tmp_path, db = sqlite_wm
    landing = tmp_path / "lz"
    target = _wm_target(landing)
    table = target.tables[0]
    await om_source.publish_table(target, table)          # initial, watermark=3
    _apply(db, "DELETE FROM sales WHERE id=2")            # a delete a watermark query can't see
    res = await om_source.publish_table(target, table)
    assert res.action == "noop"                           # no delete emitted
    assert not (landing / "dbo.schema" / "sales" / "00000000000000000002.parquet").exists()


async def test_watermark_invalid_column_errors(sqlite_wm):
    tmp_path, db = sqlite_wm
    landing = tmp_path / "lz"
    target = target_from_dict({
        "id": "t1", "connection": "default", "landing_zone_root": str(landing), "source_type": "SQL",
        "tables": [{"name": "sales", "source_table": "sales", "target_table": "sales",
                    "key_column": "id", "watermark_column": "nope"}],
    })
    result = await om_source.publish_target(target)
    assert result.results[0].action == "error"
    assert "nope" in (result.results[0].error or "")


async def test_corrupt_state_fails_closed_without_source_publish(sqlite_wm):
    tmp_path, _ = sqlite_wm
    target = _wm_target(tmp_path / "lz")
    table = target.tables[0]
    path = pathlib.Path(state_file_path(str(tmp_path / "state"), target, table))
    path.parent.mkdir(parents=True)
    path.write_text("{truncated", encoding="utf-8")

    result = await om_source.publish_target(target)

    assert result.results[0].action == "error"
    assert result.results[0].state_status == "corrupt"
    assert "refusing an implicit full load" in result.results[0].error
    assert not (tmp_path / "lz" / "dbo.schema" / "sales").exists()


async def test_empty_initial_is_initialized_once(sqlite_wm):
    tmp_path, db = sqlite_wm
    _apply(db, "DELETE FROM sales")
    target = _wm_target(tmp_path / "lz")
    table = target.tables[0]

    first = await om_source.publish_table(target, table)
    second = await om_source.publish_table(target, table)

    assert first.action == "initial"
    assert first.reason == "state_missing"
    assert second.action == "noop"
    assert load_state(str(tmp_path / "state"), target, table).initialized is True


async def test_tied_watermark_pages_use_keyset_cursor(sqlite_wm):
    tmp_path, db = sqlite_wm
    _apply(db, "DELETE FROM sales")
    for row_id in range(1, 6):
        _apply(
            db,
            "INSERT INTO sales (id,name,ver) VALUES (:id,:name,7)",
            {"id": row_id, "name": f"r{row_id}"},
        )
    target = _wm_target(tmp_path / "lz")
    table = target.tables[0]

    result = await om_source.publish_table(target, table, max_rows=2)

    assert result.action == "initial"
    assert result.rows_published == 5
    assert result.pages_read == 3
    files = sorted((tmp_path / "lz" / "dbo.schema" / "sales").glob("*.parquet"))
    assert len(files) == 3
    ids = [
        value
        for file in files
        for value in pq.read_table(file).column("id").to_pylist()
    ]
    assert ids == [1, 2, 3, 4, 5]
    committed = load_state(str(tmp_path / "state"), target, table).committed
    assert decode_watermark(committed.watermark) == 7
    assert [decode_watermark(value) for value in committed.keys] == [5]


async def test_restart_finalizes_uploaded_pending_batch(sqlite_wm, monkeypatch):
    tmp_path, _ = sqlite_wm
    target = _wm_target(tmp_path / "lz")
    table = target.tables[0]
    real_save = om_source.save_state
    calls = 0

    def fail_commit(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated cursor commit failure")
        return real_save(*args)

    monkeypatch.setattr(om_source, "save_state", fail_commit)
    with pytest.raises(OSError, match="cursor commit"):
        await om_source.publish_table(target, table)
    monkeypatch.setattr(om_source, "save_state", real_save)

    recovered = await om_source.publish_table(target, table)

    assert recovered.action == "recovery"
    assert recovered.recovery == "finalized_existing_file"
    assert len(list((tmp_path / "lz" / "dbo.schema" / "sales").glob("*.parquet"))) == 1
    assert load_state(str(tmp_path / "state"), target, table).pending is None


async def test_pending_source_mismatch_has_recovery_reason(sqlite_wm):
    tmp_path, _ = sqlite_wm
    target = _wm_target(tmp_path / "lz")
    table = target.tables[0]
    prior = CommittedCursor(
        watermark=encode_watermark(1), keys=[encode_watermark(1)]
    )
    state = PublishState(
        strategy="watermark",
        initialized=True,
        committed=prior,
        pending=PendingBatch(
            prior=prior,
            next=CommittedCursor(
                watermark=encode_watermark(2), keys=[encode_watermark(2)]
            ),
            path="dbo.schema/sales/00000000000000000001.parquet",
            row_count=99,
            content_hash="0" * 64,
        ),
    )
    save_state(str(tmp_path / "state"), target, table, state)

    result = await om_source.publish_target(target)

    assert result.results[0].action == "error"
    assert result.results[0].reason == "state_pending_invalid"


def test_version_one_watermark_state_migrates_in_memory(tmp_path):
    target = _wm_target(tmp_path / "lz")
    table = target.tables[0]
    path = pathlib.Path(state_file_path(str(tmp_path / "state"), target, table))
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps({"version": 1, "keys": {}, "watermark": encode_watermark(9)}),
        encoding="utf-8",
    )

    loaded = load_state(str(tmp_path / "state"), target, table)

    assert loaded.status == "valid"
    assert loaded.state.initialized is True
    assert decode_watermark(loaded.state.committed.watermark) == 9


async def test_version_one_watermark_resumes_without_key_bind(sqlite_wm):
    tmp_path, _ = sqlite_wm
    target = _wm_target(tmp_path / "lz")
    table = target.tables[0]
    path = pathlib.Path(state_file_path(str(tmp_path / "state"), target, table))
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps({"version": 1, "keys": {}, "watermark": encode_watermark(1)}),
        encoding="utf-8",
    )

    result = await om_source.publish_table(target, table)

    assert result.action == "incremental"
    assert result.rows == 2
    loaded = load_state(str(tmp_path / "state"), target, table)
    assert [decode_watermark(value) for value in loaded.committed.keys] == [3]
