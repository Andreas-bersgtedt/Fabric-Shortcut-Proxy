"""Real-environment integration smoke tests for Oracle + Databricks.

These tests are opt-in and skipped unless the required environment variables are
present. They exercise end-to-end connectivity + reflection + query execution
against real services.

Run manually:
  .\\.venv\\Scripts\\python.exe -m pytest tests/test_integration_oracle_databricks.py -q
"""
from __future__ import annotations

import os

import pytest

import config
from db.reflect import build_url, SchemaReflector
import db.executor as executor


def _has_required(prefix: str, required: list[str]) -> bool:
    return all(os.environ.get(f"{prefix}_{k}") for k in required)


def _oracle_conn() -> dict:
    return {
        "dialect": "oracle",
        "host": os.environ.get("ORACLE_HOST"),
        "port": int(os.environ["ORACLE_PORT"]) if os.environ.get("ORACLE_PORT") else None,
        "database": os.environ.get("ORACLE_DATABASE") or os.environ.get("ORACLE_SERVICE"),
        "username": os.environ.get("ORACLE_USERNAME"),
        "password": os.environ.get("ORACLE_PASSWORD"),
    }


def _databricks_conn() -> dict:
    return {
        "dialect": "databricks",
        "host": os.environ.get("DATABRICKS_HOST"),
        "port": int(os.environ["DATABRICKS_PORT"]) if os.environ.get("DATABRICKS_PORT") else None,
        "username": "token",
        "password": os.environ.get("DATABRICKS_TOKEN"),
        "query": {
            "http_path": os.environ.get("DATABRICKS_HTTP_PATH"),
            **({"catalog": os.environ.get("DATABRICKS_CATALOG")} if os.environ.get("DATABRICKS_CATALOG") else {}),
            **({"schema": os.environ.get("DATABRICKS_SCHEMA")} if os.environ.get("DATABRICKS_SCHEMA") else {}),
        },
    }


async def _assert_reflection_smoke(conn: dict) -> None:
    url = build_url(**conn)
    async with SchemaReflector(url) as ref:
        # Any successful response proves connectivity + inspector wiring.
        _ = await ref.server_version()
        tables = await ref.list_tables()
        assert isinstance(tables, list)

        smoke_table = os.environ.get("INTEGRATION_SMOKE_TABLE")
        if smoke_table:
            cols = await ref.columns(smoke_table)
            assert isinstance(cols, list)


async def _assert_query_smoke(db_url: str, sql: str) -> None:
    old_url = config.DB_URL
    config.DB_URL = db_url
    executor._engine = None
    executor._sync_engine = None
    try:
        v = await executor.execute_scalar(sql)
        assert int(v) == 1
    finally:
        await executor.dispose_engines()
        config.DB_URL = old_url


@pytest.mark.asyncio
async def test_oracle_reflection_and_query_smoke():
    if not _has_required("ORACLE", ["HOST", "DATABASE", "USERNAME", "PASSWORD"]):
        pytest.skip("Oracle integration env vars not configured.")

    conn = _oracle_conn()
    await _assert_reflection_smoke(conn)
    url = build_url(**conn)
    await _assert_query_smoke(url.render_as_string(hide_password=False), "SELECT 1 FROM dual")


@pytest.mark.asyncio
async def test_databricks_reflection_and_query_smoke():
    if not _has_required("DATABRICKS", ["HOST", "TOKEN", "HTTP_PATH"]):
        pytest.skip("Databricks integration env vars not configured.")

    conn = _databricks_conn()
    await _assert_reflection_smoke(conn)
    url = build_url(**conn)
    await _assert_query_smoke(url.render_as_string(hide_password=False), "SELECT 1")
