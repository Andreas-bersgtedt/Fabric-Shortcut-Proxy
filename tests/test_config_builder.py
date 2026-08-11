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

import db.reflect as reflect
from db.reflect import build_url, detect_key_column, UnsupportedDialect
from configbuilder.router import _clean_error, _conn_fields
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


def test_build_url_mssql_prefers_installed_driver(monkeypatch):
    monkeypatch.setattr(reflect, "_installed_sql_server_odbc_drivers",
                        lambda: ["ODBC Driver 17 for SQL Server"])
    url = build_url(dialect="mssql", host="h", database="db", username="u", password="p")
    assert url.query["driver"] == "ODBC Driver 17 for SQL Server"


# --- issue #19: SQL Server SPN + Windows auth --------------------------------

def test_build_url_mssql_sql_auth_unchanged():
    # Explicit auth_method="sql" is the default path: username/password, no auth keyword.
    url = build_url(dialect="mssql", host="h", database="db",
                    username="u", password="p", auth_method="sql")
    assert url.username == "u" and url.password == "p"
    assert "Authentication" not in url.query and "Trusted_Connection" not in url.query


def test_build_url_mssql_windows_auth():
    url = build_url(dialect="mssql", host="h", database="db", auth_method="windows")
    # Integrated Security: no UID/PWD, Trusted_Connection set.
    assert url.username is None and url.password is None
    assert url.query["Trusted_Connection"] == "yes"


def test_build_url_mssql_windows_ignores_any_credentials():
    url = build_url(dialect="mssql", host="h", database="db",
                    username="stray", password="stray", auth_method="windows")
    assert url.username is None and url.password is None
    assert url.query["Trusted_Connection"] == "yes"


def test_build_url_mssql_spn_auth():
    url = build_url(dialect="mssql", host="h", database="db", auth_method="spn",
                    client_id="app-guid", client_secret="sekret")
    assert url.username == "app-guid" and url.password == "sekret"
    assert url.query["Authentication"] == "ActiveDirectoryServicePrincipal"
    assert url.query["Encrypt"] == "yes"


def test_build_url_mssql_spn_requires_client_id_and_secret():
    with pytest.raises(ValueError, match="service-principal"):
        build_url(dialect="mssql", host="h", database="db", auth_method="spn",
                  client_id="app-guid")   # secret missing


def test_build_url_mssql_unknown_auth_method_errors():
    with pytest.raises(ValueError, match="auth method"):
        build_url(dialect="mssql", host="h", database="db", auth_method="nope")


def test_build_url_non_mssql_rejects_non_sql_auth_method():
    with pytest.raises(ValueError, match="only supported for SQL Server"):
        build_url(dialect="postgresql", host="h", database="db", auth_method="windows")


def test_conn_fields_passes_auth_mode_and_spn_creds():
    out = _conn_fields({"dialect": "mssql", "host": "h", "database": "db",
                        "auth_method": "SPN", "client_id": "app", "client_secret": "s"})
    assert out["auth_method"] == "spn"
    assert out["client_id"] == "app" and out["client_secret"] == "s"


def test_conn_fields_defaults_auth_method_to_sql():
    out = _conn_fields({"dialect": "mssql", "host": "h", "database": "db",
                        "username": "u", "password": "p"})
    assert out["auth_method"] == "sql"
    assert out["client_id"] is None and out["client_secret"] is None


def test_clean_error_im002_adds_driver_hint(monkeypatch):
    monkeypatch.setattr(
        "configbuilder.router._installed_sql_server_odbc_drivers",
        lambda: ["ODBC Driver 17 for SQL Server"],
    )
    err = Exception(
        "(pyodbc.InterfaceError) ('IM002', '[IM002] [Microsoft][ODBC Driver Manager] "
        "Data source name not found and no default driver specified (0) (SQLDriverConnect)')"
    )
    msg = _clean_error(err)
    assert "Hint:" in msg
    assert "ODBC Driver 17 for SQL Server" in msg


def test_build_url_oracle_defaults_port():
    url = build_url(dialect="oracle", host="orcl-host", database="ORCLPDB1",
                    username="u", password="p")
    s = url.render_as_string(hide_password=False)
    assert s.startswith("oracle+oracledb://u:")
    assert "@orcl-host:1521/ORCLPDB1" in s


def test_build_url_databricks_with_http_path():
    url = build_url(
        dialect="databricks",
        host="dbc.example.com",
        username="token",
        password="dapi-example",
        query={"http_path": "/sql/1.0/warehouses/abc"},
    )
    s = url.render_as_string(hide_password=False)
    assert s.startswith("databricks://token:")
    assert "@dbc.example.com:443" in s
    assert "http_path=%2Fsql%2F1.0%2Fwarehouses%2Fabc" in s


def test_build_url_redshift_defaults_port():
    url = build_url(dialect="redshift", host="rs-host", database="analytics",
                    username="u", password="p")
    s = url.render_as_string(hide_password=False)
    assert s.startswith("redshift+redshift_connector://u:")
    assert "@rs-host:5439/analytics" in s


def test_build_url_teradata_uses_connection_params():
    url = build_url(dialect="teradata", host="td-host", database="dbc",
                    username="u", password="p")
    s = url.render_as_string(hide_password=False)
    assert s.startswith("teradatasql://u:")
    assert "@td-host" in s
    # database + port travel as driver connection params, not URL path/port.
    assert url.query["database"] == "dbc"
    assert url.query["dbs_port"] == "1025"
    assert url.database is None
    assert url.port is None


def test_build_url_impala_defaults_port():
    url = build_url(dialect="impala", host="impala-host", database="default",
                    username="u", password="p")
    s = url.render_as_string(hide_password=False)
    assert s.startswith("impala://u:")
    assert "@impala-host:21050/default" in s


def test_build_url_rejects_unknown_dialect():
    with pytest.raises(UnsupportedDialect):
        build_url(dialect="not-a-real-dialect", host="h", database="db")


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
    assert "column policies" in r.text
    assert 'kind:"deterministic_hash"' in r.text
    assert 'kind:"random_token"' in r.text
    assert 'column.policy!=="remove"' in r.text
    assert '["mssql", "postgresql", "postgres", "oracle", "databricks"]' in r.text
    assert 'id="materializeMode"' in r.text


def test_settings_catalog_has_defaults():
    import config
    cat = config.settings_catalog()
    m = {s["key"]: s for s in cat}
    # A representative spread of settings with their built-in defaults.
    assert m["num_splits"]["default"] == 8
    assert m["pin_materialized_splits"]["default"] is True
    assert m["materialize_mode"]["default"] == "eager"
    assert m["materialize_mode"]["choices"] == ["eager", "lazy", "virtual"]
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


async def test_bootstrap_api_prefills_running_builder_config(app):
    async with _client(app) as c:
        r = await c.get("/_config/api/bootstrap")
    assert r.status_code == 200
    b = r.json()["builder"]
    assert isinstance(b.get("bucket"), str)
    assert isinstance(b.get("num_splits"), int)
    assert b.get("table_format") in ("iceberg", "delta")
    assert b.get("materialize_mode") in ("eager", "lazy")
    assert isinstance(b.get("tables"), list)
    assert isinstance(b.get("flavor"), str)


async def test_bootstrap_api_preserves_column_policies(app, monkeypatch):
    import config

    table = config.TableDef(
        name="customers_safe",
        source_table="dbo.customers",
        key_column="customer_id",
        schema=[
            config.ColumnDef(1, "customer_id", "long", nullable=False),
            config.ColumnDef(
                2,
                "email_token",
                "string",
                source="email",
                transform=config.ColumnTransform(
                    kind="deterministic_hash",
                    key_ref="customer-pii-v1",
                    domain="customer-email",
                    normalization="trim_lower",
                ),
            ),
        ],
    )
    monkeypatch.setattr(config, "TABLES", [table])
    async with _client(app) as c:
        response = await c.get("/_config/api/bootstrap")

    schema = response.json()["builder"]["tables"][0]["schema"]
    assert schema[1] == {
        "field_id": 2,
        "name": "email_token",
        "type": "string",
        "nullable": True,
        "source": "email",
        "transform": {
            "kind": "deterministic_hash",
            "normalization": "trim_lower",
            "key_ref": "customer-pii-v1",
            "domain": "customer-email",
        },
    }
    assert "uat-secret" not in response.text


async def test_connect_lists_tables(app, db_path):
    async with _client(app) as c:
        r = await c.post("/_config/api/connect",
                         json={"dialect": "sqlite", "database": db_path})
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert "gadgets" in [t["name"] for t in data["tables"]]
    assert data["db_url"].startswith("sqlite+aiosqlite")
    assert data["capabilities"]["flavor"] == "sqlite"
    assert data["capabilities"]["execution_mode"] == "async-native"


async def test_connect_bad_dialect(app):
    async with _client(app) as c:
        r = await c.post("/_config/api/connect", json={"dialect": "bad", "database": "x"})
    assert r.status_code == 400
    assert r.json()["ok"] is False


async def test_connect_databricks_requires_http_path(app):
    async with _client(app) as c:
        r = await c.post("/_config/api/connect", json={
            "dialect": "databricks",
            "host": "dbc.example.com",
            "token": "dapi-example",
        })
    assert r.status_code == 400
    assert "http_path" in r.json()["error"]


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
