"""Native materializer bridge: URL translation, argv build, and fallback guards."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import config
from config import ColumnDef, TableDef
from runtime import native_materializer as nm
from runtime.artifact_store import MemoryStore


def _orders_table(**kw) -> TableDef:
    return TableDef(
        name=kw.get("name", "orders"),
        source_table=kw.get("source_table", "sales"),
        schema=[ColumnDef(f, n, t, nl) for f, n, t, nl in (
            (1, "id", "long", False),
            (2, "order_date", "date", True),
            (3, "customer_id", "long", True),
            (4, "product", "string", True),
            (5, "quantity", "int", True),
            (6, "unit_price", "double", True),
            (7, "total", "double", True),
            (8, "region", "string", True),
        )],
        num_splits=kw.get("num_splits", 8),
        key_column=kw.get("key_column"),
        connection_id="default",
    )


def test_source_args_sqlite():
    args = nm.source_args("sqlite+aiosqlite:///./demo.db")
    assert args == ["--sqlite", os.path.abspath("./demo.db")]


def test_source_args_sqlite_memory_unsupported():
    assert nm.source_args("sqlite:///:memory:") is None


def test_source_args_postgres():
    args = nm.source_args("postgresql+asyncpg://bob:secret@dbhost:5433/mydb")
    assert args == ["--postgres", "host=dbhost port=5433 user=bob password=secret dbname=mydb"]


def test_source_args_mssql_odbc_connect():
    conn = "Driver%3D%7BODBC%20Driver%2018%20for%20SQL%20Server%7D%3BServer%3Dlocalhost"
    args = nm.source_args(f"mssql+pyodbc:///?odbc_connect={conn}")
    assert args == ["--odbc", "Driver={ODBC Driver 18 for SQL Server};Server=localhost",
                    "--db-kind", "mssql"]


def test_source_args_mssql_without_odbc_connect_unsupported():
    assert nm.source_args("mssql+pyodbc://sa:pw@host/db") is None


def test_source_args_oracle_unsupported():
    assert nm.source_args("oracle+oracledb://u:p@host:1521/?service_name=orcl") is None


def test_schema_supported_default():
    assert nm.schema_supported(_orders_table()) is True


def test_schema_none_unsupported():
    t = _orders_table()
    t.schema = None
    assert nm.schema_supported(t) is False


def test_schema_mismatch_unsupported():
    t = _orders_table()
    t.schema = t.schema[:-1]  # drop 'region'
    assert nm.schema_supported(t) is False


def test_build_argv_sqlite(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "sqlite:///./demo.db", raising=False)
    monkeypatch.setattr(config, "WAREHOUSE_PREFIX", "warehouse", raising=False)
    monkeypatch.setattr(config, "BUCKET_NAME", "buck", raising=False)
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg", raising=False)
    monkeypatch.setattr(config, "OBJECT_PATH_LAYOUT", "legacy", raising=False)

    argv = nm.build_argv("np", "/store", _orders_table(source_table="sales"))
    assert argv == [
        "np", "--sqlite", os.path.abspath("./demo.db"),
        "--store", "/store", "--table", "sales", "--splits", "8",
        "--bucket", "buck", "--table-path", "warehouse/orders", "--format", "iceberg",
    ]


def test_build_argv_includes_key_column(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "sqlite:///./demo.db", raising=False)
    monkeypatch.setattr(config, "OBJECT_PATH_LAYOUT", "legacy", raising=False)
    argv = nm.build_argv("np", "/store", _orders_table(key_column="cust_id"))
    assert "--key" in argv and argv[argv.index("--key") + 1] == "cust_id"


def test_build_argv_unsupported_schema_returns_none():
    t = _orders_table()
    t.schema = None
    assert nm.build_argv("np", "/store", t) is None


def test_materialize_serving_image_non_local_falls_back(monkeypatch):
    monkeypatch.setattr(config, "ARTIFACT_STORE_BACKEND", "memory", raising=False)
    res = nm.materialize_serving_image(MemoryStore())
    assert res["complete"] is False and res["reason"] == "store_not_local"


def test_materialize_serving_image_missing_binary_falls_back(monkeypatch):
    monkeypatch.setattr(config, "ARTIFACT_STORE_BACKEND", "local", raising=False)
    monkeypatch.setattr(config, "ARTIFACT_STORE_DIR", "./.artifacts", raising=False)
    monkeypatch.setattr(nm, "native_publish_binary", lambda: None)
    res = nm.materialize_serving_image(MemoryStore())
    assert res["complete"] is False and res["reason"] == "binary_missing"
