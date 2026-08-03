"""
Native materializer bridge.

Drives the compiled C++ ``native_publish`` binary (agent-cpp/native) to write a
complete serving image (Parquet splits plus Iceberg ``metadata.json``/manifests
or the Delta ``_delta_log``) straight into the local artifact store, so the C++
Agent serves a fully native-materialized table. Gated by ``NATIVE_MATERIALIZER``.

The native publisher currently materializes the default 8-column orders schema
only, from a SQLite, PostgreSQL, or (odbc_connect) SQL Server source into a local
store. Any table it does not support (different schema, non-local store,
untranslatable source URL, or a missing binary) is left to the Python publisher:
:func:`materialize_serving_image` reports ``complete=False`` and the caller runs
:func:`runtime.serving_image.publish_serving_image` so the image is never partial.
"""
from __future__ import annotations

import glob
import os
import subprocess
from urllib.parse import unquote

from sqlalchemy.engine import make_url

import config
from iceberg.state_store import active_table_path
from observability.logging import get_logger

log = get_logger(__name__)

# Publishing a large table can take a while; cap it so a wedged child cannot hang
# startup forever (the Python publisher still runs as the fallback).
_PUBLISH_TIMEOUT_S = 1800

# The (name, iceberg_type, nullable) columns native_publish hardcodes as
# default_schema(); a table must match this exactly to be materialized natively.
_NATIVE_SCHEMA: tuple[tuple[str, str, bool], ...] = (
    ("id", "long", False),
    ("order_date", "date", True),
    ("customer_id", "long", True),
    ("product", "string", True),
    ("quantity", "int", True),
    ("unit_price", "double", True),
    ("total", "double", True),
    ("region", "string", True),
)


def native_publish_binary() -> str | None:
    """Return the path to the built ``native_publish`` binary, or None if absent."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    build = os.path.join(root, "agent-cpp", "native", "build")
    for name in ("native_publish", "native_publish.exe"):
        path = os.path.join(build, name)
        if os.path.isfile(path):
            return path
    return None


def _native_env(binary: str) -> dict:
    """Environment for the child, adding the vcpkg shared-lib dir on POSIX.

    On Windows the co-located DLLs are found via the image directory; on Linux the
    Arrow/Avro shared objects live under ``build/vcpkg_installed/*/lib``.
    """
    env = dict(os.environ)
    if os.name != "nt":
        build = os.path.dirname(binary)
        libs = glob.glob(os.path.join(build, "vcpkg_installed", "*", "lib"))
        if libs:
            prev = env.get("LD_LIBRARY_PATH", "")
            env["LD_LIBRARY_PATH"] = os.pathsep.join(libs + ([prev] if prev else []))
    return env


def _table_schema(table) -> list[tuple[str, str, bool]] | None:
    schema = getattr(table, "schema", None)
    if not schema:
        return None
    return [(c.name, c.iceberg_type, bool(c.nullable)) for c in schema]


def schema_supported(table) -> bool:
    """True iff the table's explicit schema matches the native publisher's."""
    cols = _table_schema(table)
    return cols is not None and tuple(cols) == _NATIVE_SCHEMA


def _odbc_connect_from(url) -> str | None:
    """Extract an ODBC connection string from a pyodbc SQLAlchemy URL.

    Only the explicit ``odbc_connect=`` form is supported; component-built DSNs
    are left to the Python publisher.
    """
    raw = (url.query or {}).get("odbc_connect")
    if raw is None:
        return None
    if not isinstance(raw, str):
        raw = raw[0]
    return unquote(raw)


def source_args(db_url: str) -> list[str] | None:
    """Translate a SQLAlchemy URL into ``native_publish`` source args, or None
    when the flavor is not supported by the native publisher."""
    try:
        url = make_url(db_url)
    except Exception:  # noqa: BLE001 - malformed URL just means "unsupported here"
        return None

    backend = url.get_backend_name()
    if backend == "sqlite":
        path = url.database or ""
        if not path or path == ":memory:":
            return None
        return ["--sqlite", os.path.abspath(path)]
    if backend == "postgresql":
        parts = [f"host={url.host or 'localhost'}", f"port={url.port or 5432}"]
        if url.username:
            parts.append(f"user={url.username}")
        if url.password:
            parts.append(f"password={url.password}")
        if url.database:
            parts.append(f"dbname={url.database}")
        return ["--postgres", " ".join(parts)]
    if backend == "mssql":
        odbc = _odbc_connect_from(url)
        return ["--odbc", odbc, "--db-kind", "mssql"] if odbc else None
    return None


def build_argv(binary: str, store_dir: str, table) -> list[str] | None:
    """Build the ``native_publish`` argv for a table, or None if unsupported."""
    if not schema_supported(table):
        return None
    src = source_args(config.effective_db_url(table.connection_id))
    if src is None:
        return None
    table_path = active_table_path(table, config.WAREHOUSE_PREFIX)
    splits = table.num_splits or config.NUM_SPLITS
    argv = [
        binary, *src,
        "--store", store_dir,
        "--table", table.source_table,
        "--splits", str(splits),
        "--bucket", config.BUCKET_NAME,
        "--table-path", table_path,
        "--format", config.TABLE_FORMAT,
    ]
    if table.key_column:
        argv += ["--key", table.key_column]
    return argv


def _materialize_one(binary: str, env: dict, store_dir: str, table) -> dict:
    name = table.name
    argv = build_argv(binary, store_dir, table)
    if argv is None:
        reason = "schema_unsupported" if not schema_supported(table) else "source_unsupported"
        return {"table": name, "ok": False, "reason": reason}
    try:
        proc = subprocess.run(
            argv, env=env, capture_output=True, text=True,
            timeout=_PUBLISH_TIMEOUT_S, check=False,
        )
    except Exception as exc:  # noqa: BLE001 - any child failure falls back to Python
        log.warning("native_materialize_error", table=name, error=str(exc))
        return {"table": name, "ok": False, "reason": "invoke_failed"}
    if proc.returncode != 0:
        log.warning("native_materialize_nonzero", table=name, code=proc.returncode,
                    stderr=(proc.stderr or "")[-500:])
        return {"table": name, "ok": False, "reason": f"exit_{proc.returncode}"}
    log.info("native_materialize_table_ok", table=name,
             table_format=config.TABLE_FORMAT, splits=table.num_splits or config.NUM_SPLITS)
    return {"table": name, "ok": True, "reason": None}


def materialize_serving_image(store) -> dict:
    """Materialize every configured table's serving image natively into ``store``.

    Returns ``{"complete": bool, "reason": str|None, "tables": [...]}``.
    ``complete`` is True only when every table was materialized by the native
    publisher; when it is False the caller must run the Python publisher so the
    published image stays whole.
    """
    if config.ARTIFACT_STORE_BACKEND != "local" or not config.ARTIFACT_STORE_DIR:
        return {"complete": False, "reason": "store_not_local", "tables": []}
    binary = native_publish_binary()
    if not binary:
        return {"complete": False, "reason": "binary_missing", "tables": []}

    store_dir = getattr(store, "root", None) or os.path.abspath(config.ARTIFACT_STORE_DIR)
    env = _native_env(binary)
    results = [_materialize_one(binary, env, store_dir, t) for t in config.TABLES]
    complete = bool(results) and all(r["ok"] for r in results)
    log.info("native_materialize_done", complete=complete,
             native=[r["table"] for r in results if r["ok"]],
             fallback=[r["table"] for r in results if not r["ok"]])
    return {"complete": complete, "reason": None, "tables": results}
