"""
F1 — multi-table integration tests.

Registers two virtual Iceberg tables (the demo ``sales`` table plus an extra
``products`` table backed by its own source SQL table) and verifies that both
are served independently: metadata.json, manifest, and Parquet data files for
each table are correct and isolated.
"""
from __future__ import annotations

import io
import json
import os
import pathlib

import pytest
import pyarrow.parquet as pq

_TEST_DB = pathlib.Path(__file__).parent / "test_multitable.db"
os.environ["DB_URL"] = f"sqlite+aiosqlite:///{_TEST_DB.as_posix()}"
os.environ["S3_BUCKET"] = "mt-bucket"

import httpx
import config
from config import ColumnDef, TableDef

config.DB_URL = f"sqlite+aiosqlite:///{_TEST_DB.as_posix()}"
config.BUCKET_NAME = "mt-bucket"

from main import app

_PRODUCTS_SCHEMA = [
    ColumnDef(field_id=1, name="product_id", iceberg_type="long", nullable=False),
    ColumnDef(field_id=2, name="sku", iceberg_type="string", nullable=True),
    ColumnDef(field_id=3, name="price", iceberg_type="double", nullable=True),
]


async def _seed_products():
    """Create and populate a second source table ``products``."""
    import db.executor as _executor
    from sqlalchemy import text

    engine = _executor.get_engine()
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS products"))
        await conn.execute(text(
            "CREATE TABLE products ("
            "product_id INTEGER PRIMARY KEY, sku TEXT, price REAL)"
        ))
        for i in range(20):
            await conn.execute(
                text("INSERT INTO products (product_id, sku, price) "
                     "VALUES (:pid, :sku, :price)"),
                {"pid": i, "sku": f"SKU-{i:03d}", "price": float(i) * 1.5},
            )


@pytest.fixture(scope="module")
async def client():
    from demo.seed_db import seed_demo_database
    import db.executor as _executor
    from iceberg.state_store import build_all_snapshots

    # Module-scoped pin: other test modules mutate these globals at import time,
    # so set them here (and restore on teardown) to keep this module isolated.
    mp = pytest.MonkeyPatch()
    mp.setattr(config, "BUCKET_NAME", "mt-bucket")
    mp.setattr(config, "OBJECT_PATH_LAYOUT", "legacy")   # tests assert legacy warehouse/db/<table> paths
    mp.setattr(config, "ENABLE_LEGACY_PATH_ALIASES", True)   # accept the 'warehouse/' request prefix

    _executor._engine = None  # pick up DB_URL set above

    await seed_demo_database()
    await _seed_products()

    sales_table = TableDef(
        name="sales",
        source_table="sales",
        schema=config.TABLE_SCHEMA,
        num_splits=2,
    )
    products_table = TableDef(
        name="products",
        source_table="products",
        schema=_PRODUCTS_SCHEMA,
        num_splits=2,
    )
    mp.setattr(config, "TABLES", [sales_table, products_table])

    snaps = build_all_snapshots(
        config.TABLES,
        bucket=config.BUCKET_NAME,
        warehouse_prefix=config.WAREHOUSE_PREFIX,
    )
    # Materialize each split so manifest stats + parquet cache are accurate.
    import cache.lru_cache as _cache
    from planner.split_planner import build_split_query
    from parquet.generator import rows_to_parquet

    for snap in snaps:
        total = 0
        for split in snap.splits:
            sql, params = build_split_query(split)
            rows = await _executor.execute_split_query(
                sql, params, split_index=split.split_index
            )
            data = rows_to_parquet(rows, split_index=split.split_index,
                                   columns=split.table.schema)
            _cache.put_parquet(split.object_key, data)
            split.record_count = len(rows)
            split.file_size_in_bytes = len(data)
            total += len(rows)
        snap.total_records = total

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    mp.undo()
    if _executor._engine is not None:
        await _executor._engine.dispose()
        _executor._engine = None
    if _TEST_DB.exists():
        _TEST_DB.unlink(missing_ok=True)


async def test_both_tables_listed(client):
    r = await client.get("/mt-bucket?list-type=2&prefix=warehouse/db/&delimiter=/")
    assert r.status_code == 200
    body = r.text
    assert "warehouse/db/sales/" in body
    assert "warehouse/db/products/" in body


async def test_sales_metadata(client):
    r = await client.get("/mt-bucket/warehouse/db/sales/metadata/v1.metadata.json")
    assert r.status_code == 200
    meta = json.loads(r.content)
    assert meta["location"].endswith("/sales")
    names = {f["name"] for f in meta["schemas"][0]["fields"]}
    assert {"id", "region", "product"} <= names
    assert "sku" not in names


async def test_products_metadata(client):
    r = await client.get("/mt-bucket/warehouse/db/products/metadata/v1.metadata.json")
    assert r.status_code == 200
    meta = json.loads(r.content)
    assert meta["location"].endswith("/products")
    names = {f["name"] for f in meta["schemas"][0]["fields"]}
    assert names == {"product_id", "sku", "price"}
    assert meta["last-column-id"] == 3


async def test_products_parquet_schema_and_rows(client):
    # Discover the products data split keys via ListObjectsV2.
    r = await client.get("/mt-bucket?list-type=2&prefix=warehouse/db/products/data/")
    assert r.status_code == 200
    assert ".parquet" in r.text

    # Fetch split 0 directly (deterministic key layout).
    lr = await client.get("/mt-bucket?list-type=2&prefix=warehouse/db/products/data/")
    import xml.etree.ElementTree as ET
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    root = ET.fromstring(lr.text)
    keys = [e.text for e in root.findall(".//s3:Contents/s3:Key", ns)]
    assert keys, "expected at least one products parquet key"

    total_rows = 0
    seen_columns = None
    for key in keys:
        pr = await client.get(f"/mt-bucket/{key}")
        assert pr.status_code == 200
        table = pq.read_table(io.BytesIO(pr.content))
        seen_columns = table.column_names
        total_rows += table.num_rows

    assert seen_columns == ["product_id", "sku", "price"]
    assert total_rows == 20  # all seeded products rows across splits


async def test_sales_parquet_still_isolated(client):
    r = await client.get("/mt-bucket?list-type=2&prefix=warehouse/db/sales/data/")
    import xml.etree.ElementTree as ET
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    root = ET.fromstring(r.text)
    keys = [e.text for e in root.findall(".//s3:Contents/s3:Key", ns)]
    assert keys
    pr = await client.get(f"/mt-bucket/{keys[0]}")
    assert pr.status_code == 200
    table = pq.read_table(io.BytesIO(pr.content))
    # Sales schema columns — NOT products columns.
    assert "region" in table.column_names
    assert "sku" not in table.column_names
