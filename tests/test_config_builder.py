"""
Config Builder tests (M1 API + reflection helpers).

The router is exercised in isolation (its own FastAPI app) against a temp SQLite
DB, so it does not depend on main.py's ENABLE_CONFIG_BUILDER mount order.
"""
from __future__ import annotations

import pathlib

import pytest
import httpx
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from db.reflect import build_url, detect_key_column, UnsupportedDialect
from configbuilder.router import router as cb_router

_DB = pathlib.Path(__file__).parent / "test_cfgbuilder.db"


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def test_build_url_postgres_defaults_and_encoding():
    url = build_url(dialect="postgresql", host="h", database="db",
                    username="u", password="p@ss:w/rd")
    s = url.render_as_string(hide_password=False)
    assert s.startswith("postgresql+asyncpg://u:")
    assert "@h:5432/db" in s            # default port applied, host present


def test_build_url_mssql_adds_driver_and_cert():
    url = build_url(dialect="mssql", host="h", database="db", username="u", password="p")
    s = url.render_as_string(hide_password=False)
    assert s.startswith("mssql+aioodbc://")
    assert "ODBC" in s and "Driver" in s
    assert "TrustServerCertificate" in s


def test_build_url_rejects_unknown_dialect():
    with pytest.raises(UnsupportedDialect):
        build_url(dialect="oracle", host="h", database="db")


def test_detect_key_column():
    cols = [{"name": "id", "type": "long", "nullable": False},
            {"name": "n", "type": "string", "nullable": True}]
    key, ints = detect_key_column(cols, ["id"])
    assert key == "id" and ints == ["id"]

    cols2 = [{"name": "a", "type": "string", "nullable": False},
             {"name": "b", "type": "int", "nullable": False}]
    key2, ints2 = detect_key_column(cols2, [])
    assert key2 == "b" and ints2 == ["b"]

    key3, ints3 = detect_key_column([{"name": "s", "type": "string", "nullable": False}], [])
    assert key3 is None and ints3 == []


# ---------------------------------------------------------------------------
# API (isolated app + temp SQLite)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def app():
    a = FastAPI()
    a.include_router(cb_router)
    return a


@pytest.fixture(scope="module")
async def db_path():
    url = f"sqlite+aiosqlite:///{_DB.as_posix()}"
    eng = create_async_engine(url)
    async with eng.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS gadgets"))
        await conn.execute(text(
            "CREATE TABLE gadgets (gadget_id INTEGER PRIMARY KEY, label TEXT, price REAL)"
        ))
        await conn.execute(text("INSERT INTO gadgets VALUES (1,'a',1.0),(2,'b',2.0),(3,'c',3.0)"))
    await eng.dispose()
    yield _DB.as_posix()
    if _DB.exists():
        _DB.unlink(missing_ok=True)


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def test_index_serves_html(app):
    async with _client(app) as c:
        r = await c.get("/_config/")
    assert r.status_code == 200
    assert "Config Builder" in r.text


def test_settings_catalog_has_defaults():
    import config
    cat = config.settings_catalog()
    m = {s["key"]: s for s in cat}
    # A representative spread of settings with their built-in defaults.
    assert m["num_splits"]["default"] == 8
    assert m["pin_materialized_splits"]["default"] is True
    assert m["auto_refresh"]["default"] is False
    assert m["refresh_ttl_seconds"]["default"] == 1200
    # Secrets are flagged (so the builder never prefills their value).
    assert m["db_url"]["secret"] is True
    # Every entry carries the fields the UI needs.
    for s in cat:
        assert {"key", "env", "type", "default", "category", "help", "secret"} <= set(s)


async def test_settings_api(app):
    async with _client(app) as c:
        r = await c.get("/_config/api/settings")
    assert r.status_code == 200
    keys = {s["key"] for s in r.json()["settings"]}
    assert {"num_splits", "pin_materialized_splits", "auto_refresh", "require_sigv4"} <= keys


async def test_connect_lists_tables(app, db_path):
    async with _client(app) as c:
        r = await c.post("/_config/api/connect",
                         json={"dialect": "sqlite", "database": db_path})
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert "gadgets" in [t["name"] for t in data["tables"]]
    assert data["db_url"].startswith("sqlite+aiosqlite")


async def test_connect_bad_dialect(app):
    async with _client(app) as c:
        r = await c.post("/_config/api/connect", json={"dialect": "oracle", "database": "x"})
    assert r.status_code == 400
    assert r.json()["ok"] is False


async def test_inspect_reflects_and_detects_key(app, db_path):
    async with _client(app) as c:
        r = await c.post("/_config/api/inspect", json={
            "connection": {"dialect": "sqlite", "database": db_path},
            "tables": [{"name": "gadgets"}],
        })
    assert r.status_code == 200
    t = r.json()["tables"][0]
    assert t["source_table"] == "gadgets"
    assert t["detected_key"] == "gadget_id"
    assert "gadget_id" in t["integer_keys"]
    assert {c["name"] for c in t["columns"]} == {"gadget_id", "label", "price"}
    assert t["approx_rows"] == 3
