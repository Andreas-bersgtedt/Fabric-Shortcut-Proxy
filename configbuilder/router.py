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

import os
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
from security.credential_store import CredentialStore, env_var_for, looks_masked

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


def _flavor_from_url(db_url: str) -> str:
    """Best-effort dialect label from a SQLAlchemy URL scheme (for UI display)."""
    from db.capabilities import flavor_from_db_url
    return flavor_from_db_url(db_url)


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

    # Named source connections (exclude the reserved 'default', which is the db_url
    # above). Passwords are masked — the builder never receives raw secrets.
    connections = []
    for cid, conn in config.CONNECTIONS.items():
        if cid == "default":
            continue
        connections.append({
            "id": cid,
            "db_url_masked": config.redact_db_url(conn.db_url),
            "flavor": _flavor_from_url(conn.db_url),
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
            "connections": connections,
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


# ---------------------------------------------------------------------------
# Credential store (persist DB credentials so they survive a restart)
# ---------------------------------------------------------------------------

def _store() -> CredentialStore:
    return CredentialStore(config.CREDENTIAL_STORE_PATH or None)


async def _restart_agents(request: Request) -> int:
    """Restart supervised Agents so they re-read the updated environment.

    Returns the number restarted (0 if this app supervises no Agents).
    """
    sups = getattr(request.app.state, "supervisors", None) or []
    n = 0
    for sup in sups:
        try:
            await sup.stop()
            await sup.start()
            n += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("cred_apply_restart_failed", agent=getattr(sup, "name", "?"), error=str(exc))
    return n


@router.get("/api/credentials")
async def list_credentials() -> JSONResponse:
    """Non-secret status of the Manager credential store (never returns URLs)."""
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "enabled": False,
                             "error": "credential store is disabled (ENABLE_CREDENTIAL_STORE=0)"})
    return JSONResponse({"ok": True, "enabled": True, **_store().status()})


@router.post("/api/credentials")
async def save_credential(request: Request) -> JSONResponse:
    """Encrypt + persist a connection's DB URL so it survives a restart.

    Body: ``{connection_id, db_url}`` OR ``{connection_id, connection: {..fields}}``.
    Optional ``apply: true`` restarts the Agents so the new credential takes
    effect now (otherwise it applies on the next Manager start).
    """
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "error": "credential store is disabled (ENABLE_CREDENTIAL_STORE=0)"},
                            status_code=400)
    body = await request.json()
    if not isinstance(body, dict):
        return JSONResponse({"ok": False, "error": "body must be a JSON object"}, status_code=400)
    cid = str(body.get("connection_id") or "default").strip() or "default"

    db_url = str(body.get("db_url") or "").strip()
    if not db_url and isinstance(body.get("connection"), dict):
        try:
            db_url = build_url(**_conn_fields(body["connection"])).render_as_string(hide_password=False)
        except (UnsupportedDialect, ValueError) as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    if not db_url:
        return JSONResponse({"ok": False, "error": "provide db_url or connection fields"}, status_code=400)
    if looks_masked(db_url):
        return JSONResponse({"ok": False, "error": (
            "that looks like a masked URL (contains '***'). Enter the real password and "
            "Test the connection first, then save it.")}, status_code=400)

    st = _store()
    if not st.available:
        return JSONResponse({"ok": False, "backend": st.backend_name,
                             "error": "no encryption backend available; on non-Windows hosts install "
                                      "'cryptography' or set FSP_CRED_KEY"}, status_code=400)
    try:
        st.set_url(cid, db_url)
    except (RuntimeError, ValueError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    env_var = env_var_for(cid)
    # Update THIS process's env so a subsequent Agent restart inherits the new URL.
    os.environ[env_var] = db_url

    apply_now = bool(body.get("apply"))
    restarted = await _restart_agents(request) if apply_now else 0

    log.info("credential_saved", connection=cid, env_var=env_var,
             backend=st.backend_name, applied=apply_now, restarted=restarted)
    return JSONResponse({
        "ok": True,
        "connection_id": cid,
        "env_var": env_var,
        "backend": st.backend_name,
        "applied": apply_now,
        "restarted": restarted,
        "note": ("Saved and restarted Agents — the new credential is live."
                 if apply_now else
                 "Saved (encrypted). Restart the Manager/Agents to apply."),
    })


@router.delete("/api/credentials/{connection_id}")
async def delete_credential(connection_id: str) -> JSONResponse:
    """Remove a stored credential (running Agents keep it until the next restart)."""
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "error": "credential store is disabled"}, status_code=400)
    removed = _store().delete(connection_id)
    log.info("credential_deleted", connection=connection_id, removed=removed)
    return JSONResponse({"ok": True, "connection_id": connection_id, "removed": removed})


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
