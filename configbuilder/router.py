"""
Config Builder API + SPA (optional; mounted only when ENABLE_CONFIG_BUILDER).

Endpoints (all under ``/_config``):
  GET  /_config/            -> the single-page app
  POST /_config/api/connect -> test a connection, list tables/views
  POST /_config/api/inspect -> reflect columns + detect key column per table

The generated ``config.json`` is assembled client-side from these responses.
"""
from __future__ import annotations

import pathlib

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

import config
from db.capabilities import capabilities_for_dialect, flavor_warnings
from db.reflect import build_url, SchemaReflector, detect_key_column, UnsupportedDialect
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
