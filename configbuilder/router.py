"""
Config Builder API + SPA (optional; mounted only when ENABLE_CONFIG_BUILDER).

Endpoints (all under ``/_config``):
  GET  /_config/            -> the single-page app
  POST /_config/api/connect -> test a connection, list tables/views
  POST /_config/api/inspect -> reflect columns + detect key column per table
  POST /_config/api/save    -> persist settings to config.json (restart to apply)
  POST /_config/api/apply   -> apply live + persist in one call (no restart needed
                               for live-applicable settings)

The generated ``config.json`` is assembled client-side from these responses.
"""
from __future__ import annotations

import pathlib

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

import config
from db.capabilities import capabilities_for_dialect, flavor_warnings
from db.reflect import (
    _installed_sql_server_odbc_drivers,
    build_url,
    SchemaReflector,
    detect_key_column,
    UnsupportedDialect,
)
from observability.logging import get_logger

log = get_logger(__name__)

router = APIRouter(prefix="/_config")
_HTML_PATH = pathlib.Path(__file__).parent / "index.html"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _conn_fields(d: dict) -> dict:
    dialect = str(d.get("dialect", "")).strip().lower()
    username = d.get("username") or None
    password = d.get("password") or None

    # Databricks commonly authenticates with username='token' and password=<PAT>.
    token = d.get("token") or None
    if dialect == "databricks" and token and not password:
        username = username or "token"
        password = token

    query: dict[str, str] = {}
    for k in ("http_path", "catalog", "schema"):
        v = d.get(k)
        if v:
            query[k] = str(v)

    return dict(
        dialect=dialect,
        host=d.get("host") or None,
        port=int(d["port"]) if d.get("port") else None,
        database=d.get("database") or None,
        username=username,
        password=password,
        driver=d.get("driver") or None,
        trust_cert=bool(d.get("trust_cert", True)),
        query=query or None,
    )


def _clean_error(exc: Exception) -> str:
    """Redact any embedded credential and trim driver noise from an error."""
    msg = config.redact_db_url(str(exc))
    low = msg.lower()
    if "can't load plugin: sqlalchemy.dialects:databricks" in low:
        hint = (
            "Hint: Databricks SQLAlchemy dialect is not installed in this environment. "
            "Install project dependencies again (for example `pip install -e .`) or install "
            "`databricks-sqlalchemy`."
        )
        msg = f"{msg}\n{hint}"
    if "im002" in low and "sqldriverconnect" in low:
        drivers = _installed_sql_server_odbc_drivers()
        if drivers:
            hint = (
                "Hint: SQL Server ODBC driver not found for this connection string. "
                "Pick an installed value in Advanced -> ODBC driver, e.g. "
                + ", ".join(drivers)
            )
        else:
            hint = (
                "Hint: SQL Server ODBC driver is missing. Install 'ODBC Driver 18 for SQL Server' "
                "(or 17) and retry, or set Advanced -> ODBC driver to an installed name."
            )
        msg = f"{msg}\n{hint}"
    return msg[:400]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("")
@router.get("/")
async def index() -> HTMLResponse:
    # no-store so operators always get the latest builder UI (not a stale cache).
    return HTMLResponse(_HTML_PATH.read_text(encoding="utf-8"),
                        headers={"Cache-Control": "no-store, max-age=0"})


@router.get("/api/settings")
async def settings() -> JSONResponse:
    """Full catalog of global settings (json key, env var, type, default,
    category, help) so the UI can show every setting with its default."""
    return JSONResponse({"settings": config.settings_catalog()})


@router.get("/api/current")
async def current_config() -> JSONResponse:
    """Phase 5.1: the CURRENT effective configuration — each setting's live value
    and its source (env / config.json / default), secrets redacted. Lets the
    builder show what's actually running, not just defaults."""
    return JSONResponse({
        "settings": config.effective_settings(),
        "config_file": config.config_file_path(),
    })


@router.get("/api/bootstrap")
async def bootstrap_builder() -> JSONResponse:
    """Runtime snapshot used by the builder UI to prefill the main form.

    Returns non-secret effective values plus the currently configured table
    mappings so opening ``/_config`` starts from what is running now.
    """
    tables = []
    for t in config.TABLES:
        tables.append({
            "name": t.name,
            "source_table": t.source_table,
            "connection": t.connection_id,
            "key_column": t.key_column,
            "num_splits": int(t.num_splits),
        })

    return JSONResponse({
        "builder": {
            "db_url_masked": config.redact_db_url(config.DB_URL),
            "has_db_url": bool(config.DB_URL),
            "bucket": config.BUCKET_NAME,
            "num_splits": int(config.NUM_SPLITS),
            "table_format": config.TABLE_FORMAT,
            "require_sigv4": bool(config.REQUIRE_SIGV4),
            "auto_refresh": bool(config.AUTO_REFRESH),
            "refresh_poll_seconds": int(config.REFRESH_POLL_SECONDS),
            "refresh_strategy": config.REFRESH_STRATEGY,
            "refresh_allow_full_pull": bool(config.REFRESH_ALLOW_FULL_PULL),
            "refresh_ttl_seconds": int(config.REFRESH_TTL_SECONDS),
            "tables": tables,
        }
    })


@router.post("/api/save")
async def save_config(request: Request) -> JSONResponse:
    """Phase 5.1: validate + persist a partial settings map to the on-disk
    ``config.json`` (atomic merge, preserving ``tables`` and unlisted keys).

    Config is import-time, so persisted changes apply on the next start
    (``restart_required``) — except ``agent_count``, which the Manager can also
    apply live via ``POST /_manager/api/scale``.
    """
    body = await request.json()
    updates = body.get("settings") if isinstance(body, dict) else None
    if not isinstance(updates, dict) or not updates:
        return JSONResponse({"ok": False, "error": "body must be {\"settings\": {key: value, ...}}"},
                            status_code=400)
    clean, errors = config.validate_setting_updates(updates)
    if errors:
        return JSONResponse({"ok": False, "errors": errors}, status_code=400)
    try:
        result = config.write_config_updates(clean)
    except (OSError, ValueError) as exc:
        log.warning("config_builder_save_failed", error=str(exc))
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    log.info("config_saved", path=result["path"], changed=result["changed"])
    return JSONResponse({
        "ok": True,
        "path": result["path"],
        "changed": result["changed"],
        "restart_required": True,
        "note": "Saved to config.json. Restart the Manager/Agents to apply "
                "(agent_count can also be applied live from the Manager: POST /_manager/api/scale).",
    })


@router.post("/api/apply")
async def apply_config(request: Request) -> JSONResponse:
    """Apply settings to the running process AND persist to config.json in one call.

    Live-applicable settings (see ``config.LIVE_SETTINGS``) take effect
    immediately — no restart needed. Structural settings (DB_URL, PORT, bucket,
    etc.) are persisted to config.json and reported as ``restart_required``.

    Request body: ``{"settings": {"key": value, ...}}``
    """
    body = await request.json()
    updates = body.get("settings") if isinstance(body, dict) else None
    if not isinstance(updates, dict) or not updates:
        return JSONResponse({"ok": False, "error": 'body must be {"settings": {key: value, ...}}'},
                            status_code=400)
    clean, errors = config.validate_setting_updates(updates)
    if errors:
        return JSONResponse({"ok": False, "errors": errors}, status_code=400)

    # 1. Apply live to this process.
    try:
        live_result = config.apply_live_settings(clean)
    except (ValueError, AttributeError) as exc:
        log.warning("config_builder_apply_failed", error=str(exc))
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    # 2. Persist all changes to config.json (both live and restart-required).
    try:
        saved = config.write_config_updates(clean)
    except (OSError, ValueError) as exc:
        log.warning("config_builder_save_failed", error=str(exc))
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    log.info("config_applied",
             applied_live=live_result["applied"],
             restart_required=live_result["restart_required"],
             path=saved["path"])

    return JSONResponse({
        "ok": True,
        "applied_live": live_result["applied"],
        "restart_required": live_result["restart_required"],
        "path": saved["path"],
        "note": (
            f"Applied {len(live_result['applied'])} setting(s) live."
            + (
                f" {len(live_result['restart_required'])} setting(s) saved to config.json "
                f"(restart required): {', '.join(live_result['restart_required'])}."
                if live_result["restart_required"] else ""
            )
        ),
    })


@router.post("/api/connect")
async def connect(request: Request) -> JSONResponse:
    body = await request.json()
    conn = _conn_fields(body)
    caps = capabilities_for_dialect(conn.get("dialect"))
    try:
        url = build_url(**conn)
    except (UnsupportedDialect, ValueError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    try:
        async with SchemaReflector(url) as ref:
            tables = await ref.list_tables()
            version = await ref.server_version()
    except Exception as exc:  # noqa: BLE001 - surface a clean message to the UI
        log.warning("config_builder_connect_failed", error=_clean_error(exc))
        return JSONResponse({"ok": False, "error": _clean_error(exc)}, status_code=400)

    return JSONResponse({
        "ok": True,
        "server_version": version,
        "db_url": url.render_as_string(hide_password=False),
        "db_url_masked": url.render_as_string(hide_password=True),
        "tables": tables,
        "capabilities": caps.to_dict(),
        "warnings": flavor_warnings(conn.get("dialect")),
    })


@router.post("/api/inspect")
async def inspect_tables(request: Request) -> JSONResponse:
    body = await request.json()
    conn = body.get("connection") or {}
    refs = body.get("tables") or []
    try:
        url = build_url(**_conn_fields(conn))
    except (UnsupportedDialect, ValueError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    out: list[dict] = []
    try:
        async with SchemaReflector(url) as ref:
            for r in refs:
                schema = r.get("schema") or None
                name = r.get("name")
                if not name:
                    continue
                source_table = f"{schema}.{name}" if schema else name
                cols = await ref.columns(source_table)
                pk = await ref.primary_key(source_table)
                detected, int_keys = detect_key_column(cols, pk)
                out.append({
                    "name": name,
                    "source_table": source_table,
                    "detected_key": detected,
                    "integer_keys": int_keys,
                    "columns": cols,
                    "approx_rows": await ref.approx_row_count(source_table),
                })
    except Exception as exc:  # noqa: BLE001
        log.warning("config_builder_inspect_failed", error=_clean_error(exc))
        return JSONResponse({"ok": False, "error": _clean_error(exc)}, status_code=400)

    return JSONResponse({"ok": True, "tables": out})
