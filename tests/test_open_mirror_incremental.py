"""Phase 3 + 5 — incremental change stream, drop, quarantine, dry-run, scheduler.

Uses a real seeded SQLite source and a local landing zone (no mocks). Verifies the
diff engine, the state store, retry-safe publishing, table drops, per-table
quarantine, dry-run, and the background scheduler's one-shot cycle.
"""
from __future__ import annotations

import io
import os
import pathlib

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

import config
import db.executor as executor
from config import ColumnDef
from open_mirror import target_from_dict
from open_mirror.changes import RowMarker, compute_changes
from open_mirror.landing_zone import LocalLandingZone
from open_mirror.publisher import ROW_MARKER_COLUMN, LandingZonePublisher
from open_mirror.state import PublishState, build_state_from_rows
from open_mirror import source as om_source

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
    rel = pub.publish_changes(target.tables[0], [_row(1, "a"), {"id": 2}], [RowMarker.UPDATE, RowMarker.DELETE], _COLUMNS)
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
