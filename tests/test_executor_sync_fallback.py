from __future__ import annotations

import pathlib
import time

import pytest
from sqlalchemy import text

from fabric_shortcut_proxy import config
import fabric_shortcut_proxy.db.executor as executor


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


async def test_sync_query_timeout_is_reported(sync_sqlite, monkeypatch):
    class _Result:
        def keys(self):
            return ["value"]

        def fetchall(self):
            return [(1,)]

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args):
            time.sleep(0.05)
            return _Result()

    class _Engine:
        def connect(self):
            return _Connection()

    monkeypatch.setattr(config, "QUERY_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(executor, "_sync_engine_for", lambda _connection: _Engine())

    with pytest.raises(executor.SourceUnavailable, match="TimeoutError"):
        await executor.execute_split_query(
            "SELECT 1", {}, split_index=0, max_retries=0
        )
