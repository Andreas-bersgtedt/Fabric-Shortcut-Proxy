from __future__ import annotations

import pathlib

import pytest
from sqlalchemy import text

import config
import db.executor as executor


_DB = pathlib.Path(__file__).parent / "test_sync_fallback.db"


@pytest.fixture
async def sync_sqlite(monkeypatch):
    url = f"sqlite:///{_DB.as_posix()}"
    monkeypatch.setattr(config, "DB_URL", url, raising=False)

    # Reset both engine caches so the new URL is picked up.
    executor._engine = None
    executor._sync_engine = None

    eng = executor.get_sync_engine()
    with eng.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS t_sync"))
        conn.execute(text("CREATE TABLE t_sync (id INTEGER PRIMARY KEY, v TEXT)"))
        conn.execute(text("INSERT INTO t_sync (id, v) VALUES (1, 'a'), (2, 'b')"))

    yield

    await executor.dispose_engines()
    if _DB.exists():
        _DB.unlink(missing_ok=True)


async def test_execute_scalar_sync_fallback(sync_sqlite):
    v = await executor.execute_scalar("SELECT COUNT(*) FROM t_sync")
    assert int(v) == 2


async def test_execute_split_query_sync_fallback(sync_sqlite):
    sql = "SELECT id, v FROM t_sync WHERE id >= :lo ORDER BY id"
    rows = await executor.execute_split_query(sql, {"lo": 1}, split_index=0, max_retries=0)
    assert [r["id"] for r in rows] == [1, 2]
