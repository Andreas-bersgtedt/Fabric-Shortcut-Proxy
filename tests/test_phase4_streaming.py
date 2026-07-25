"""Phase 4 scale engine: streaming (bounded-memory) Parquet materialization."""
from __future__ import annotations

import io
import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import pyarrow.parquet as pq
import pytest
from sqlalchemy import text as _text
from sqlalchemy.ext.asyncio import create_async_engine

import config
from config import ColumnDef
from parquet.generator import rows_to_parquet, stream_rows_to_parquet

_COLS = [
    ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
    ColumnDef(field_id=2, name="val", iceberg_type="string", nullable=True),
]


async def _abatches(batches):
    for b in batches:
        yield b


def _read(pq_bytes: bytes):
    t = pq.read_table(io.BytesIO(pq_bytes))
    return t.num_rows, t.column("id").to_pylist()


# ---------------------------------------------------------------------------
# Streaming generator produces the same data as the single-shot generator.
# ---------------------------------------------------------------------------

async def test_stream_matches_single_shot():
    rows = [{"id": i, "val": f"v{i}"} for i in range(1, 21)]
    single = rows_to_parquet(rows, split_index=0, columns=_COLS)
    # stream the same rows in batches of 7
    batches = [rows[i:i + 7] for i in range(0, len(rows), 7)]
    streamed, n = await stream_rows_to_parquet(_abatches(batches), split_index=0, columns=_COLS)

    assert n == 20
    assert _read(streamed) == _read(single) == (20, list(range(1, 21)))


async def test_stream_empty_writes_schema_only():
    streamed, n = await stream_rows_to_parquet(_abatches([]), split_index=0, columns=_COLS)
    assert n == 0
    t = pq.read_table(io.BytesIO(streamed))
    assert t.num_rows == 0
    assert t.schema.names == ["id", "val"]


async def test_stream_skips_empty_batches():
    batches = [[{"id": 1, "val": "a"}], [], [{"id": 2, "val": "b"}]]
    streamed, n = await stream_rows_to_parquet(_abatches(batches), split_index=0, columns=_COLS)
    assert n == 2
    assert _read(streamed) == (2, [1, 2])


# ---------------------------------------------------------------------------
# stream_split_query batches rows off SQLite and covers everything.
# ---------------------------------------------------------------------------

@pytest.fixture
async def stream_db(tmp_path, monkeypatch):
    import db.executor as ex
    url = f"sqlite+aiosqlite:///{(tmp_path / 'stream.db').as_posix()}"
    monkeypatch.setattr(config, "DB_URL", url, raising=False)
    old_engine = ex._engine
    ex._engine = None
    eng = create_async_engine(url)
    async with eng.begin() as conn:
        await conn.execute(_text("CREATE TABLE s (id INTEGER PRIMARY KEY, val TEXT)"))
        await conn.execute(_text(
            "INSERT INTO s (id, val) VALUES " +
            ",".join(f"({i},'v{i}')" for i in range(1, 101))
        ))
    await eng.dispose()
    yield url
    if ex._engine is not None:
        await ex._engine.dispose()
    ex._engine = old_engine


async def test_stream_split_query_batches_all_rows(stream_db):
    from db.executor import stream_split_query
    seen: list[int] = []
    batch_sizes: list[int] = []
    gen = stream_split_query("SELECT id, val FROM s ORDER BY id", {},
                             split_index=0, batch_rows=30)
    async for batch in gen:
        batch_sizes.append(len(batch))
        seen += [r["id"] for r in batch]
    assert sorted(seen) == list(range(1, 101))      # all rows
    assert max(batch_sizes) <= 30                    # bounded batches
    assert len(batch_sizes) >= 4                     # 100 / 30 -> 4 batches


async def test_stream_end_to_end_materialize(stream_db):
    from db.executor import stream_split_query
    gen = stream_split_query("SELECT id, val FROM s ORDER BY id", {},
                             split_index=0, batch_rows=25)
    pq_bytes, n = await stream_rows_to_parquet(gen, split_index=0, columns=_COLS)
    assert n == 100
    assert _read(pq_bytes) == (100, list(range(1, 101)))


# ---------------------------------------------------------------------------
# Source backpressure: cap concurrent source queries per Agent.
# ---------------------------------------------------------------------------

import asyncio


async def _peak_concurrency(n_workers: int) -> int:
    import db.executor as ex
    ex._source_sem = None                       # rebuild for this loop/limit
    peak = cur = 0
    lock = asyncio.Lock()

    async def worker():
        nonlocal peak, cur
        async with ex._source_gate():
            async with lock:
                cur += 1
                peak = max(peak, cur)
            await asyncio.sleep(0.02)
            async with lock:
                cur -= 1

    await asyncio.gather(*(worker() for _ in range(n_workers)))
    return peak


async def test_source_gate_caps_concurrency(monkeypatch):
    monkeypatch.setattr(config, "SOURCE_MAX_CONCURRENCY", 2, raising=False)
    assert await _peak_concurrency(6) == 2       # never more than 2 at once


async def test_source_gate_unlimited_by_default(monkeypatch):
    monkeypatch.setattr(config, "SOURCE_MAX_CONCURRENCY", 0, raising=False)
    assert await _peak_concurrency(6) == 6       # null gate -> all concurrent
