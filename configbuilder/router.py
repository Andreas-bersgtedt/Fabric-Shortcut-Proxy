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
    for needle, extra, packages in (
        ("sqlalchemy.dialects:redshift", "redshift", "sqlalchemy-redshift + redshift-connector"),
        ("sqlalchemy.dialects:teradatasql", "teradata", "teradatasqlalchemy"),
        ("sqlalchemy.dialects:impala", "impala", "impyla"),
    ):
        if f"can't load plugin: {needle}" in low:
            msg = (
                f"{msg}\nHint: the {extra} source driver is not installed in this "
                f"environment. Install it with `pip install -e '.[{extra}]'` ({packages})."
            )
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
        schema = []
        for column in t.schema or []:
            item = {
                "field_id": column.field_id,
                "name": column.name,
                "type": column.iceberg_type,
                "nullable": column.nullable,
            }
            if column.source:
                item["source"] = column.source
            if column.transform:
                transform = {
                    "kind": column.transform.kind,
                    "normalization": column.transform.normalization,
                }
                if column.transform.key_ref:
                    transform["key_ref"] = column.transform.key_ref
                if column.transform.domain is not None:
                    transform["domain"] = column.transform.domain
                item["transform"] = transform
            schema.append(item)
        tables.append({
            "name": t.name,
            "source_table": t.source_table,
            "connection": t.connection_id,
            "key_column": t.key_column,
            "num_splits": int(t.num_splits),
            "split_target_rows": (int(t.split_target_rows) if t.split_target_rows is not None else None),
            "split_strategy": t.split_strategy,
            "split_balance": t.split_balance,
            "schema": schema or None,
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
            "flavor": _flavor_from_url(config.DB_URL),
            "bucket": config.BUCKET_NAME,
            "num_splits": int(config.NUM_SPLITS),
            "split_target_rows": int(config.SPLIT_TARGET_ROWS),
            "split_count_min": int(config.SPLIT_COUNT_MIN),
            "split_count_max": int(config.SPLIT_COUNT_MAX),
            "split_strategy": config.SPLIT_STRATEGY,
            "table_format": config.TABLE_FORMAT,
            "materialize_mode": config.MATERIALIZE_MODE,
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


def _s3_auth_blob(auth) -> dict:
    """Reduce a parsed S3AuthConfig to the minimal stored blob for its mode."""
    blob: dict = {"mode": auth.mode}
    if auth.mode in ("static", "session"):
        blob["access_key"] = auth.access_key
        blob["secret_key"] = auth.secret_key
        if auth.mode == "session":
            blob["session_token"] = auth.session_token
    elif auth.mode == "assume_role":
        blob["role_arn"] = auth.role_arn
        if auth.external_id:
            blob["external_id"] = auth.external_id
        if auth.session_name:
            blob["session_name"] = auth.session_name
        if auth.duration_seconds:
            blob["duration_seconds"] = auth.duration_seconds
    elif auth.mode == "web_identity":
        blob["role_arn"] = auth.role_arn
        blob["web_identity_token_file"] = auth.web_identity_token_file
        if auth.session_name:
            blob["session_name"] = auth.session_name
    elif auth.mode in ("profile", "sso"):
        blob["profile"] = auth.profile
    elif auth.mode == "process":
        blob["credential_process"] = auth.credential_process
    return blob


@router.get("/api/s3-credentials")
async def list_s3_credentials() -> JSONResponse:
    """Non-secret list of stored upstream S3 credential ids."""
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "enabled": False,
                             "error": "credential store is disabled (ENABLE_CREDENTIAL_STORE=0)"})
    st = _store()
    return JSONResponse({"ok": True, "enabled": True, "available": st.available,
                         "backend": st.backend_name, "ids": st.list_secret_ids()})


@router.post("/api/s3-credentials")
async def save_s3_credential(request: Request) -> JSONResponse:
    """Encrypt + persist an upstream S3 credential blob, keyed by an id.

    Body: ``{credential_id, auth: {mode, ...}}``. Secrets are validated per mode
    and stored encrypted; only the id is ever returned.
    """
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "error": "credential store is disabled (ENABLE_CREDENTIAL_STORE=0)"},
                            status_code=400)
    body = await request.json()
    if not isinstance(body, dict):
        return JSONResponse({"ok": False, "error": "body must be a JSON object"}, status_code=400)
    cid = str(body.get("credential_id") or "").strip()
    if not cid:
        return JSONResponse({"ok": False, "error": "credential_id is required"}, status_code=400)
    auth_in = body.get("auth")
    if not isinstance(auth_in, dict):
        return JSONResponse({"ok": False, "error": "auth object is required"}, status_code=400)

    from storage.s3_auth import parse_s3_auth, validate_s3_auth
    auth = parse_s3_auth(auth_in)
    problems = validate_s3_auth(auth)
    if problems:
        return JSONResponse({"ok": False, "errors": problems}, status_code=400)

    st = _store()
    if not st.available:
        return JSONResponse({"ok": False, "backend": st.backend_name,
                             "error": "no encryption backend available; on non-Windows hosts install "
                                      "'cryptography' or set FSP_CRED_KEY"}, status_code=400)
    try:
        st.set_secret(cid, _s3_auth_blob(auth))
    except (RuntimeError, ValueError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    log.info("s3_credential_saved", credential=cid, mode=auth.mode, backend=st.backend_name)
    return JSONResponse({"ok": True, "credential_id": cid, "mode": auth.mode,
                         "backend": st.backend_name,
                         "note": "Saved (encrypted). Restart the Manager/Agents to apply to serving."})


@router.delete("/api/s3-credentials/{credential_id}")
async def delete_s3_credential(credential_id: str) -> JSONResponse:
    """Remove a stored S3 credential (running Agents keep it until the next restart)."""
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "error": "credential store is disabled"}, status_code=400)
    removed = _store().delete_secret(credential_id)
    log.info("s3_credential_deleted", credential=credential_id, removed=removed)
    return JSONResponse({"ok": True, "credential_id": credential_id, "removed": removed})


def _azure_auth_blob(auth) -> dict:
    """Reduce a parsed AzureAuthConfig to the minimal stored blob for its mode."""
    blob: dict = {"mode": auth.mode}
    if auth.mode == "connection_string":
        blob["connection_string"] = auth.connection_string
    elif auth.mode == "account_key":
        blob["account_key"] = auth.account_key
    elif auth.mode == "sas":
        blob["sas_token"] = auth.sas_token
    elif auth.mode == "aad_client_secret":
        blob["tenant_id"] = auth.tenant_id
        blob["client_id"] = auth.client_id
        blob["client_secret"] = auth.client_secret
    elif auth.mode == "managed_identity":
        if auth.client_id:
            blob["client_id"] = auth.client_id
    return blob


@router.get("/api/azure-credentials")
async def list_azure_credentials() -> JSONResponse:
    """Non-secret list of stored upstream Azure credential ids."""
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "enabled": False,
                             "error": "credential store is disabled (ENABLE_CREDENTIAL_STORE=0)"})
    st = _store()
    return JSONResponse({"ok": True, "enabled": True, "available": st.available,
                         "backend": st.backend_name, "ids": st.list_secret_ids()})


@router.post("/api/azure-credentials")
async def save_azure_credential(request: Request) -> JSONResponse:
    """Encrypt + persist an upstream Azure credential blob, keyed by an id.

    Body: ``{credential_id, auth: {mode, ...}}``. Secrets are validated per mode
    and stored encrypted; only the id is ever returned.
    """
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "error": "credential store is disabled (ENABLE_CREDENTIAL_STORE=0)"},
                            status_code=400)
    body = await request.json()
    if not isinstance(body, dict):
        return JSONResponse({"ok": False, "error": "body must be a JSON object"}, status_code=400)
    cid = str(body.get("credential_id") or "").strip()
    if not cid:
        return JSONResponse({"ok": False, "error": "credential_id is required"}, status_code=400)
    auth_in = body.get("auth")
    if not isinstance(auth_in, dict):
        return JSONResponse({"ok": False, "error": "auth object is required"}, status_code=400)

    from storage.azure_auth import parse_azure_auth, validate_azure_auth
    auth = parse_azure_auth(auth_in)
    problems = validate_azure_auth(auth)
    if problems:
        return JSONResponse({"ok": False, "errors": problems}, status_code=400)

    st = _store()
    if not st.available:
        return JSONResponse({"ok": False, "backend": st.backend_name,
                             "error": "no encryption backend available; on non-Windows hosts install "
                                      "'cryptography' or set FSP_CRED_KEY"}, status_code=400)
    try:
        st.set_secret(cid, _azure_auth_blob(auth))
    except (RuntimeError, ValueError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    log.info("azure_credential_saved", credential=cid, mode=auth.mode, backend=st.backend_name)
    return JSONResponse({"ok": True, "credential_id": cid, "mode": auth.mode,
                         "backend": st.backend_name,
                         "note": "Saved (encrypted). Restart the Manager/Agents to apply to serving."})


@router.delete("/api/azure-credentials/{credential_id}")
async def delete_azure_credential(credential_id: str) -> JSONResponse:
    """Remove a stored Azure credential (running Agents keep it until the next restart)."""
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "error": "credential store is disabled"}, status_code=400)
    removed = _store().delete_secret(credential_id)
    log.info("azure_credential_deleted", credential=credential_id, removed=removed)
    return JSONResponse({"ok": True, "credential_id": credential_id, "removed": removed})


# ---------------------------------------------------------------------------
# Proxy access keys + per-key ACLs (Phase 4)
# ---------------------------------------------------------------------------

@router.get("/api/access-keys")
async def list_access_keys_endpoint() -> JSONResponse:
    """Non-secret list of proxy access keys + their scope."""
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "enabled": False,
                             "error": "credential store is disabled (ENABLE_CREDENTIAL_STORE=0)"})
    from security import access_keys as ak
    st = _store()
    return JSONResponse({"ok": True, "enabled": True, "available": st.available,
                         "backend": st.backend_name,
                         "enforce_mount_auth": bool(config.ENFORCE_MOUNT_AUTH),
                         "require_sigv4": bool(config.REQUIRE_SIGV4),
                         "keys": ak.list_access_keys(store=st)})


@router.post("/api/access-keys")
async def save_access_key_endpoint(request: Request) -> JSONResponse:
    """Create or update a proxy access key + ACL scope.

    Body: ``{access_key_id?, secret_key?, label, allowed_buckets, allowed_prefixes,
    permissions, enabled}``. Omitting ``access_key_id``/``secret_key`` on create
    generates a fresh pair (the secret is returned once, then never again).
    """
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "error": "credential store is disabled (ENABLE_CREDENTIAL_STORE=0)"},
                            status_code=400)
    body = await request.json()
    if not isinstance(body, dict):
        return JSONResponse({"ok": False, "error": "body must be a JSON object"}, status_code=400)

    from security import access_keys as ak
    st = _store()
    if not st.available:
        return JSONResponse({"ok": False, "backend": st.backend_name,
                             "error": "no encryption backend available; on non-Windows hosts install "
                                      "'cryptography' or set FSP_CRED_KEY"}, status_code=400)

    key_id = str(body.get("access_key_id") or "").strip()
    secret = str(body.get("secret_key") or "")
    generated = False
    if not key_id:
        key_id, secret = ak.generate_key()
        generated = True
    elif not secret:
        # Update of an existing key that keeps its secret.
        existing = ak.get_access_key(key_id, store=st)
        if existing is not None:
            secret = existing.secret_key

    record = ak.parse_access_key({**body, "access_key_id": key_id, "secret_key": secret})
    problems = ak.validate_access_key(record)
    if problems:
        return JSONResponse({"ok": False, "errors": problems}, status_code=400)
    try:
        ak.save_access_key(record, store=st)
    except (RuntimeError, ValueError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    log.info("access_key_saved", access_key_id=key_id, generated=generated,
             buckets=record.allowed_buckets)
    resp = {"ok": True, "access_key_id": key_id, "generated": generated,
            "backend": st.backend_name,
            "note": "Saved (encrypted). Restart the Manager/Agents to apply to serving."}
    # Return the secret exactly once, on generation, so the operator can copy it.
    if generated:
        resp["secret_key"] = secret
    return JSONResponse(resp)


@router.post("/api/access-keys/{access_key_id}/rotate")
async def rotate_access_key_endpoint(access_key_id: str) -> JSONResponse:
    """Rotate an existing key's secret (scope preserved). Returns the new secret once."""
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "error": "credential store is disabled"}, status_code=400)
    from security import access_keys as ak
    st = _store()
    existing = ak.get_access_key(access_key_id, store=st)
    if existing is None:
        return JSONResponse({"ok": False, "error": f"access key {access_key_id!r} not found"}, status_code=404)
    _new_id, new_secret = ak.generate_key()
    existing.secret_key = new_secret
    ak.save_access_key(existing, store=st)
    log.info("access_key_rotated", access_key_id=access_key_id)
    return JSONResponse({"ok": True, "access_key_id": access_key_id, "secret_key": new_secret,
                         "note": "Rotated. Update the client, then restart the Manager/Agents."})


@router.delete("/api/access-keys/{access_key_id}")
async def delete_access_key_endpoint(access_key_id: str) -> JSONResponse:
    if not config.ENABLE_CREDENTIAL_STORE:
        return JSONResponse({"ok": False, "error": "credential store is disabled"}, status_code=400)
    from security import access_keys as ak
    removed = ak.delete_access_key(access_key_id, store=_store())
    log.info("access_key_deleted", access_key_id=access_key_id, removed=removed)
    return JSONResponse({"ok": True, "access_key_id": access_key_id, "removed": removed})


@router.get("/api/audit")
async def recent_audit() -> JSONResponse:
    """Most recent storage-proxy audit events (in-memory ring)."""
    from observability import audit
    return JSONResponse({"ok": True, "enabled": bool(config.ENABLE_AUDIT_LOG),
                         "events": audit.recent(200)})


# ---------------------------------------------------------------------------
# Storage proxy mounts (config.mounts.json)
# ---------------------------------------------------------------------------

def _mounts_file_path() -> str:
    return os.environ.get("MOUNTS_CONFIG_FILE", "config.mounts.json")


def _write_mounts_file(mounts: list) -> str:
    import json
    import tempfile
    path = _mounts_file_path()
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".mounts-", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump({"mounts": mounts}, fh, indent=2)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
    return path


def _validate_mounts_payload(mounts) -> tuple[list, list[str]]:
    import storage.mounts as sm
    errors: list[str] = []
    clean: list[dict] = []
    seen: set[str] = set()
    if not isinstance(mounts, list):
        return [], ["'mounts' must be a list"]
    for i, e in enumerate(mounts):
        if not isinstance(e, dict):
            errors.append(f"mounts[{i}]: must be an object")
            continue
        bucket = str(e.get("bucket") or "").strip()
        backend = str(e.get("backend") or "local").strip().lower()
        root = str(e.get("root") or "").strip()
        prefix = str(e.get("prefix") or "").strip().strip("/")
        if not bucket:
            errors.append(f"mounts[{i}]: 'bucket' is required")
            continue
        if not sm._VALID_BUCKET.match(bucket):
            errors.append(f"mounts[{i}]: {bucket!r} is not a valid S3 bucket name")
            continue
        if bucket == config.BUCKET_NAME:
            errors.append(f"mounts[{i}]: {bucket!r} is reserved for the DB warehouse bucket")
            continue
        if bucket in seen:
            errors.append(f"mounts[{i}]: duplicate bucket {bucket!r}")
            continue
        if backend not in sm._SUPPORTED_BACKENDS:
            errors.append(f"mounts[{i}]: backend {backend!r} not supported (use one of {list(sm._SUPPORTED_BACKENDS)})")
            continue
        entry = {"bucket": bucket, "backend": backend, "root": root,
                 "prefix": prefix, "read_only": True}
        if backend == "local":
            if not root:
                errors.append(f"mounts[{i}]: local backend needs 'root'")
                continue
        elif backend == "s3":
            if not root:
                errors.append(f"mounts[{i}]: s3 backend needs 'root' (the upstream bucket)")
                continue
            credential = str(e.get("credential") or "").strip()
            auth = str(e.get("auth") or "").strip().lower()
            if not credential and not auth:
                errors.append(f"mounts[{i}]: s3 backend needs a 'credential' id or an explicit 'auth' "
                              "mode ('anonymous' or 'instance')")
                continue
            entry.update({
                "credential": credential,
                "auth": auth,
                "endpoint": str(e.get("endpoint") or "").strip(),
                "region": str(e.get("region") or "").strip(),
                "addressing_style": str(e.get("addressing_style") or "").strip().lower(),
                "signature_version": str(e.get("signature_version") or "").strip().lower(),
                "verify_tls": str(e.get("verify_tls") or "").strip(),
                "use_fips": bool(e.get("use_fips", False)),
                "use_dualstack": bool(e.get("use_dualstack", False)),
            })
        elif backend == "azure":
            if not root:
                errors.append(f"mounts[{i}]: azure backend needs 'root' (the container)")
                continue
            credential = str(e.get("credential") or "").strip()
            auth = str(e.get("auth") or "").strip().lower()
            if not credential and not auth:
                errors.append(f"mounts[{i}]: azure backend needs a 'credential' id or an explicit 'auth' "
                              "mode ('default', 'managed_identity', or 'anonymous')")
                continue
            account = str(e.get("account") or "").strip()
            endpoint = str(e.get("endpoint") or "").strip()
            conn_string_cred = credential and auth == ""
            # connection_string auth carries the account itself; otherwise we need
            # an account name or an explicit account URL to reach the endpoint.
            if not account and not endpoint and not conn_string_cred:
                errors.append(f"mounts[{i}]: azure backend needs 'account' (storage account name) or 'endpoint' (account URL)")
                continue
            entry.update({
                "credential": credential,
                "auth": auth,
                "account": account,
                "endpoint": endpoint,
                "endpoint_suffix": str(e.get("endpoint_suffix") or "").strip(),
            })
        seen.add(bucket)
        clean.append(entry)
    return clean, errors


@router.get("/api/mounts")
async def list_mounts() -> JSONResponse:
    """Current storage-proxy mount table + enable flag (config.mounts.json)."""
    import storage.mounts as sm
    mounts = []
    for m in sm.MOUNTS.values():
        entry = {"bucket": m.bucket, "backend": m.backend, "root": m.root,
                 "prefix": m.prefix.rstrip("/"), "read_only": m.read_only}
        if m.backend == "s3":
            entry.update({"credential": m.credential, "auth": m.auth,
                          "endpoint": m.endpoint, "region": m.region,
                          "addressing_style": m.addressing_style,
                          "signature_version": m.signature_version,
                          "verify_tls": m.verify_tls, "use_fips": m.use_fips,
                          "use_dualstack": m.use_dualstack})
        elif m.backend == "azure":
            entry.update({"credential": m.credential, "auth": m.auth,
                          "account": m.account, "endpoint": m.endpoint,
                          "endpoint_suffix": m.endpoint_suffix})
        mounts.append(entry)
    return JSONResponse({
        "ok": True,
        "enabled": bool(config.ENABLE_STORAGE_PROXY),
        "supported_backends": list(sm._SUPPORTED_BACKENDS),
        "reserved_bucket": config.BUCKET_NAME,
        "mounts": mounts,
    })


@router.post("/api/mounts")
async def save_mounts(request: Request) -> JSONResponse:
    """Validate + persist the mount table to config.mounts.json (restart to apply).

    Optionally also flips ``enable_storage_proxy`` in config.system.json.
    """
    body = await request.json()
    if not isinstance(body, dict):
        return JSONResponse({"ok": False, "error": "body must be a JSON object"}, status_code=400)
    clean, errors = _validate_mounts_payload(body.get("mounts"))
    if errors:
        return JSONResponse({"ok": False, "errors": errors}, status_code=400)

    enabled = body.get("enabled")
    enable_result = None
    if isinstance(enabled, bool):
        clean_s, errs = config.validate_setting_updates({"enable_storage_proxy": enabled})
        if not errs:
            try:
                config.write_config_updates(clean_s)
                enable_result = enabled
            except (OSError, ValueError) as exc:
                log.warning("mounts_enable_persist_failed", error=str(exc))

    try:
        path = _write_mounts_file(clean)
    except OSError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    log.info("mounts_saved", path=path, count=len(clean), enabled=enable_result)
    return JSONResponse({"ok": True, "path": path, "count": len(clean),
                         "enabled": enable_result, "restart_required": True,
                         "note": "Saved to config.mounts.json. Restart the Manager/Agents to apply."})


@router.post("/api/mounts/test")
async def test_mount(request: Request) -> JSONResponse:
    """Verify a mount's backend is reachable (local: a directory; s3: list a prefix)."""
    body = await request.json()
    backend = str(body.get("backend") or "local").strip().lower()
    root = str(body.get("root") or "").strip()
    prefix = str(body.get("prefix") or "").strip().strip("/")
    if backend == "s3":
        return await _test_s3_mount(body, root, prefix)
    if backend == "azure":
        return await _test_azure_mount(body, root, prefix)
    if backend != "local":
        return JSONResponse({"ok": False, "error": f"backend {backend!r} not testable (use 'local', 's3', or 'azure')"})
    if not root:
        return JSONResponse({"ok": False, "error": "root path is required"})
    base = os.path.join(root, *prefix.split("/")) if prefix else root
    if not os.path.isdir(base):
        return JSONResponse({"ok": False, "error": f"not a directory: {base!r} (mount the share first)"})
    entries: list[str] = []
    try:
        with os.scandir(base) as it:
            for i, ent in enumerate(it):
                if i >= 20:
                    break
                entries.append(ent.name + ("/" if ent.is_dir() else ""))
    except OSError as exc:
        return JSONResponse({"ok": False, "error": f"cannot read {base!r}: {exc}"})
    return JSONResponse({"ok": True, "path": base, "sample": entries, "sample_count": len(entries)})


async def _test_s3_mount(body: dict, root: str, prefix: str) -> JSONResponse:
    """Build the s3 backend from the posted mount and list one folder level."""
    import storage.mounts as sm
    from storage.s3_auth import resolve_s3_auth, validate_s3_auth
    from storage.s3_store import build_s3_store

    if not root:
        return JSONResponse({"ok": False, "error": "s3 backend needs 'root' (the upstream bucket)"})
    mount = sm._mount_from_json({**body, "backend": "s3", "prefix": prefix})
    try:
        auth = resolve_s3_auth(mount)
    except Exception as exc:  # noqa: BLE001 - never leak secret material
        return JSONResponse({"ok": False, "error": str(exc)})
    problems = validate_s3_auth(auth)
    if problems:
        return JSONResponse({"ok": False, "error": "; ".join(problems)})
    try:
        store = build_s3_store(mount)
        entries = store.list_dir(mount.prefix)
        sample = [name + ("/" if is_dir else "") for name, is_dir, *_ in entries[:20]]
    except Exception as exc:  # noqa: BLE001 - surface a clean, secret-free message
        return JSONResponse({"ok": False, "error": str(exc)})
    return JSONResponse({"ok": True, "bucket": root, "auth_mode": auth.mode,
                         "sample": sample, "sample_count": len(sample)})


async def _test_azure_mount(body: dict, root: str, prefix: str) -> JSONResponse:
    """Build the azure backend from the posted mount and list one folder level."""
    import storage.mounts as sm
    from storage.azure_auth import resolve_azure_auth, validate_azure_auth
    from storage.azure_store import build_azure_store

    if not root:
        return JSONResponse({"ok": False, "error": "azure backend needs 'root' (the container)"})
    mount = sm._mount_from_json({**body, "backend": "azure", "prefix": prefix})
    try:
        auth = resolve_azure_auth(mount)
    except Exception as exc:  # noqa: BLE001 - never leak secret material
        return JSONResponse({"ok": False, "error": str(exc)})
    problems = validate_azure_auth(auth)
    if problems:
        return JSONResponse({"ok": False, "error": "; ".join(problems)})
    try:
        store = build_azure_store(mount)
        entries = store.list_dir(mount.prefix)
        sample = [name + ("/" if is_dir else "") for name, is_dir, *_ in entries[:20]]
    except Exception as exc:  # noqa: BLE001 - surface a clean, secret-free message
        return JSONResponse({"ok": False, "error": str(exc)})
    return JSONResponse({"ok": True, "bucket": root, "auth_mode": auth.mode,
                         "sample": sample, "sample_count": len(sample)})


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
