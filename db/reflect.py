"""
Schema reflection against an *arbitrary* database (used by the config builder).

Unlike ``db/executor.py`` (which reflects the single configured source via the
global engine), this opens a throwaway async engine for a user-supplied
connection, lists tables/views, reflects columns/keys, and disposes cleanly.

Security: the connection is built from *structured* fields with an allowlisted
driver — never a raw URL from the client — and only inspector APIs are used
(no string interpolation into SQL).
"""
from __future__ import annotations

import asyncio

from sqlalchemy import create_engine
from sqlalchemy import inspect, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from db.capabilities import capabilities_for_db_url, missing_required_fields
from db.executor import sqlalchemy_type_to_iceberg, _split_qualified
from observability.logging import get_logger

log = get_logger(__name__)

_INTEGER_TYPES = ("int", "long")

# dialect (as the SPA sends it) -> SQLAlchemy async drivername
_DRIVERS: dict[str, str] = {
    "postgresql": "postgresql+asyncpg",
    "postgres": "postgresql+asyncpg",
    "mssql": "mssql+aioodbc",
    "sqlserver": "mssql+aioodbc",
    "oracle": "oracle+oracledb",
    "oraclesql": "oracle+oracledb",
    "databricks": "databricks",
    "sqlite": "sqlite+aiosqlite",   # tests / local files only
}

_DEFAULT_PORTS: dict[str, int] = {
    "postgresql": 5432, "postgres": 5432,
    "mssql": 1433, "sqlserver": 1433,
    "oracle": 1521, "oraclesql": 1521,
    "databricks": 443,
}

# Schemas that are never interesting to expose as tables.
_SYSTEM_SCHEMAS = {
    "information_schema", "pg_catalog", "pg_toast", "sys", "guest",
    "db_owner", "db_accessadmin", "db_securityadmin", "db_ddladmin",
    "db_backupoperator", "db_datareader", "db_datawriter",
    "db_denydatareader", "db_denydatawriter",
}


class UnsupportedDialect(ValueError):
    pass


def _installed_sql_server_odbc_drivers() -> list[str]:
    """Best-effort list of locally installed SQL Server ODBC drivers.

    Returns an empty list when ``pyodbc`` is unavailable or driver enumeration
    fails; callers should then fall back to a safe default.
    """
    try:
        import pyodbc  # type: ignore
        return [d for d in pyodbc.drivers() if "sql server" in d.lower()]
    except Exception:  # noqa: BLE001 - advisory only
        return []


def _default_mssql_odbc_driver() -> str:
    """Pick the best available SQL Server ODBC driver on this host."""
    preferred = (
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "ODBC Driver 11 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    )
    installed = _installed_sql_server_odbc_drivers()
    for name in preferred:
        if name in installed:
            return name
    if installed:
        return installed[-1]
    return "ODBC Driver 18 for SQL Server"


def build_url(
    *,
    dialect: str,
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    username: str | None = None,
    password: str | None = None,
    driver: str | None = None,
    trust_cert: bool = True,
    query: dict | None = None,
) -> URL:
    """Build a SQLAlchemy URL from structured fields (encoding-safe, allowlisted)."""
    key = (dialect or "").strip().lower()
    drivername = _DRIVERS.get(key)
    if not drivername:
        raise UnsupportedDialect(f"Unsupported dialect {dialect!r}.")

    q: dict = dict(query or {})
    req = missing_required_fields(key, q)
    if req:
        raise ValueError(
            f"Dialect {dialect!r} requires connection field(s): {', '.join(req)}."
        )

    if drivername.startswith("mssql"):
        q.setdefault("driver", driver or _default_mssql_odbc_driver())
        if trust_cert:
            q.setdefault("TrustServerCertificate", "yes")

    return URL.create(
        drivername,
        username=username or None,
        password=password or None,
        host=host or None,
        port=(port or _DEFAULT_PORTS.get(key)) if host else None,
        database=database or None,
        query=q,
    )


def _is_system_schema(schema: str | None) -> bool:
    return bool(schema) and schema.lower() in _SYSTEM_SCHEMAS


class SchemaReflector:
    """Async context manager wrapping a temporary engine for one connection."""

    def __init__(self, url: URL) -> None:
        self._url = url
        self._async_engine: AsyncEngine | None = None
        self._sync_engine: Engine | None = None

    @staticmethod
    def _is_async_driver(drivername: str) -> bool:
        # Current known async-native drivers in this project. Other drivers
        # run via a sync engine in a worker thread.
        return (
            drivername.startswith("postgresql+asyncpg")
            or drivername.startswith("mssql+aioodbc")
            or drivername.startswith("sqlite+aiosqlite")
        )

    async def __aenter__(self) -> "SchemaReflector":
        if self._is_async_driver(self._url.drivername):
            self._async_engine = create_async_engine(self._url, echo=False)
        else:
            self._sync_engine = create_engine(self._url, echo=False, pool_pre_ping=True)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._async_engine is not None:
            await self._async_engine.dispose()
            self._async_engine = None
        if self._sync_engine is not None:
            eng = self._sync_engine
            self._sync_engine = None
            await asyncio.to_thread(eng.dispose)

    async def _run(self, fn):
        if self._async_engine is not None:
            async with self._async_engine.connect() as conn:
                return await conn.run_sync(fn)

        if self._sync_engine is None:
            raise RuntimeError("SchemaReflector engine is not initialized.")

        def _work():
            with self._sync_engine.connect() as conn:
                return fn(conn)

        return await asyncio.to_thread(_work)

    async def server_version(self) -> str | None:
        try:
            info = await self._run(lambda conn: conn.dialect.server_version_info)
            return ".".join(str(p) for p in info) if info else None
        except Exception:  # noqa: BLE001
            return None

    async def list_tables(self) -> list[dict]:
        """Return [{schema, name, kind}] for user tables and views."""
        def _f(sync_conn):
            insp = inspect(sync_conn)
            try:
                schemas = insp.get_schema_names()
            except Exception:  # noqa: BLE001 - some backends don't support it
                schemas = []
            schemas = [s for s in schemas if not _is_system_schema(s)] or [None]
            out: list[dict] = []
            for s in schemas:
                for name in insp.get_table_names(schema=s):
                    out.append({"schema": s, "name": name, "kind": "table"})
                try:
                    for name in insp.get_view_names(schema=s):
                        out.append({"schema": s, "name": name, "kind": "view"})
                except Exception:  # noqa: BLE001
                    pass

            if out:
                return out

            # Fallback for engines that do not expose inspector schema/table
            # metadata consistently (for example Databricks connectors).
            try:
                rs = sync_conn.execute(text(
                    "SELECT table_schema, table_name, table_type "
                    "FROM information_schema.tables"
                ))
                for row in rs:
                    schema = row[0]
                    name = row[1]
                    ttype = (row[2] or "").upper()
                    if _is_system_schema(schema):
                        continue
                    kind = "view" if "VIEW" in ttype else "table"
                    out.append({"schema": schema, "name": name, "kind": kind})
            except Exception:  # noqa: BLE001
                pass
            return out
        return await self._run(_f)

    async def columns(self, source_table: str) -> list[dict]:
        schema, name = _split_qualified(source_table)
        cols = await self._run(lambda c: inspect(c).get_columns(name, schema=schema))
        return [
            {
                "name": col["name"],
                "type": sqlalchemy_type_to_iceberg(col["type"]),
                "nullable": bool(col.get("nullable", True)),
            }
            for col in cols
        ]

    async def primary_key(self, source_table: str) -> list[str]:
        schema, name = _split_qualified(source_table)
        try:
            pk = await self._run(lambda c: inspect(c).get_pk_constraint(name, schema=schema))
            return list(pk.get("constrained_columns") or [])
        except Exception:  # noqa: BLE001
            return []

    async def approx_row_count(self, source_table: str) -> int | None:
        """Fast row estimate from catalog stats; None if unavailable."""
        drv = self._url.drivername
        caps = capabilities_for_db_url(f"{drv}://")
        try:
            if drv.startswith("postgresql"):
                row = await self._run(
                    lambda conn: conn.execute(
                        text("SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass(:t)"),
                        {"t": source_table},
                    ).scalar()
                )
                return int(row) if row is not None and row >= 0 else None

            if drv.startswith("mssql"):
                row = await self._run(
                    lambda conn: conn.execute(
                        text(
                            "SELECT SUM(row_count) FROM sys.dm_db_partition_stats "
                            "WHERE object_id = OBJECT_ID(:t) AND index_id IN (0,1)"
                        ),
                        {"t": source_table},
                    ).scalar()
                )
                return int(row) if row is not None else None

            if drv.startswith("oracle"):
                schema, name = _split_qualified(source_table)
                owner = (schema or "").upper()
                table = name.upper()

                def _oracle_stats(conn):
                    if owner:
                        return conn.execute(
                            text("SELECT num_rows FROM all_tables WHERE owner = :o AND table_name = :t"),
                            {"o": owner, "t": table},
                        ).scalar()
                    return conn.execute(
                        text("SELECT num_rows FROM user_tables WHERE table_name = :t"),
                        {"t": table},
                    ).scalar()

                row = await self._run(_oracle_stats)
                if row is not None:
                    return int(row)

            if caps.supports_fast_row_estimate:
                return None

            # sqlite / oracle / databricks / other: fallback exact count
            schema, name = _split_qualified(source_table)
            ident = f'"{name}"' if not schema else f'"{schema}"."{name}"'
            row = await self._run(lambda conn: conn.execute(text(f"SELECT COUNT(*) FROM {ident}")).scalar())
            return int(row) if row is not None else None
        except Exception:  # noqa: BLE001
            return None


def detect_key_column(columns: list[dict], primary_key: list[str]) -> tuple[str | None, list[str]]:
    """Return (detected_key, integer_key_candidates).

    Prefers a single-column integer primary key, else the first non-nullable
    integer column, else any integer column.
    """
    integer_cols = [c["name"] for c in columns if c["type"] in _INTEGER_TYPES]
    if len(primary_key) == 1 and primary_key[0] in integer_cols:
        return primary_key[0], integer_cols
    for c in columns:
        if c["type"] in _INTEGER_TYPES and not c["nullable"]:
            return c["name"], integer_cols
    return (integer_cols[0] if integer_cols else None), integer_cols
