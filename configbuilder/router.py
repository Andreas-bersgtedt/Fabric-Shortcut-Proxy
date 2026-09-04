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

import asyncio
from datetime import datetime, timezone
import hmac
import os
import pathlib
import uuid

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response

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
from security.backup import BackupError, create_backup, restore_backup
from security.credential_store import CredentialStore, env_var_for, looks_masked

log = get_logger(__name__)

router = APIRouter(prefix="/_config")
_HTML_PATH = pathlib.Path(__file__).parent / "index.html"
# Config objects are import-time snapshots. Keep deleted ids hidden from builder
# reads until restart, when the persisted files become the new snapshots.
_REMOVED_CONNECTION_IDS: set[str] = set()
_BUILDER_TABLES_OVERRIDE: list[dict] | None = None
_OPEN_MIRROR_JOBS: dict[str, dict] = {}
_OPEN_MIRROR_TASKS: set[asyncio.Task] = set()
_LATEST_OPEN_MIRROR_JOB_ID: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_proxy_entra_auth() -> tuple[str, str | None, str | None]:
    """Resolve the ``entra_proxy`` SQL Server auth method onto the proxy's OWN Entra
    identity — the one already configured for Key Vault (issue #16). Returns a concrete
    ``(auth_method, client_id, client_secret)`` for :func:`build_url`:

    - ``service_principal`` -> ``spn`` with the configured client id + the
      ``AZURE_CLIENT_SECRET`` env secret (never a config file).
    - ``managed_identity`` -> ``managed_identity`` (user-assigned client id when set).
    - anything else -> ``default`` (the ambient credential chain).
    """
    mode = (getattr(config, "AUTH_MODE", "default") or "default").strip().lower()
    client_id = (getattr(config, "AZURE_CLIENT_ID", "") or "").strip() or None
    if mode == "service_principal":
        secret = os.environ.get("AZURE_CLIENT_SECRET") or None
        if not client_id or not secret:
            raise ValueError(
                "Reuse-the-proxy-identity auth needs the proxy's Entra service principal "
                "configured (auth_mode=service_principal with azure_client_id and the "
                "AZURE_CLIENT_SECRET env var — see the issue #16 / Key Vault setup)."
            )
        return "spn", client_id, secret
    if mode == "managed_identity":
        return "managed_identity", client_id, None
    return "default", client_id, None


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

    auth_method = str(d.get("auth_method") or "sql").strip().lower()
    client_id = d.get("client_id") or None
    client_secret = d.get("client_secret") or None
    # "entra_proxy" reuses the proxy's own Entra identity (the Key Vault SPN/MI/default).
    if dialect == "mssql" and auth_method in ("entra_proxy", "proxy", "proxy_identity", "keyvault_spn"):
        auth_method, client_id, client_secret = _resolve_proxy_entra_auth()

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
        auth_method=auth_method,
        client_id=client_id,
        client_secret=client_secret,
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


@router.get("/api/modules/catalog")
async def module_catalog() -> JSONResponse:
    """Return the allowlisted optional module catalog and runtime status."""
    from module_registry import module_status
    return JSONResponse(module_status())


@router.get("/api/modules/status")
async def module_status_endpoint() -> JSONResponse:
    """Return desired, installed, and active module states."""
    from module_registry import module_status
    return JSONResponse(module_status())


@router.post("/api/modules/plan")
async def module_plan_endpoint(request: Request) -> JSONResponse:
    """Preview a desired module profile without writing or installing packages."""
    from module_registry import module_plan
    body = await request.json()
    desired = body.get("desired") if isinstance(body, dict) else None
    if not isinstance(desired, list):
        return JSONResponse({"ok": False, "error": 'body must be {"desired": [...]}'}, status_code=400)
    return JSONResponse({"ok": True, **module_plan(desired)})


@router.post("/api/modules/desired")
async def save_module_profile(request: Request) -> JSONResponse:
    """Persist an allowlisted desired module profile; installation is restart-bound."""
    from module_registry import save_desired_profile
    body = await request.json()
    desired = body.get("desired") if isinstance(body, dict) else None
    if not isinstance(desired, list):
        return JSONResponse({"ok": False, "error": 'body must be {"desired": [...]}'}, status_code=400)
    try:
        result = save_desired_profile(desired)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, **result, "restart_required": True,
                         "note": "Profile saved. Install dependencies and restart through the deployment workflow."})


@router.get("/api/modules/operation")
async def module_operation_status() -> JSONResponse:
    """Return the current controlled dependency operation state."""
    from module_installer import status
    return JSONResponse(status())


@router.post("/api/modules/install")
async def install_modules(request: Request) -> JSONResponse:
    """Start an allowlisted dependency install; never execute browser package input."""
    from module_installer import start_install
    body = await request.json()
    desired = body.get("desired") if isinstance(body, dict) else None
    if not isinstance(desired, list):
        return JSONResponse({"ok": False, "error": 'body must be {"desired": [...]}'}, status_code=400)
    try:
        result = start_install(desired)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=409)
    return JSONResponse({"ok": True, **result,
                         "note": "Installation started. Restart the Manager explicitly after it succeeds."}, status_code=202)


@router.get("/api/keyvault")
async def keyvault_status() -> JSONResponse:
    """Non-secret Key Vault / Entra ID config + live status for the builder panel."""
    from security import keyvault as kv
    st = kv.status_snapshot()
    st["sdk_installed"] = bool(st.get("enabled")) and kv.sdk_available()
    return JSONResponse(st)


@router.get("/api/tokenization-key-references")
async def tokenization_key_references() -> JSONResponse:
    """List configured tokenization key references without returning key values."""
    prefix = "FSP_TOKENIZATION_KEY_"
    references = sorted({
        name[len(prefix):].lower().replace("_", "-")
        for name, value in os.environ.items()
        if name.upper().startswith(prefix) and value
    })
    return JSONResponse({"ok": True, "references": references})


@router.get("/api/tokenization/policies")
async def tokenization_policies() -> JSONResponse:
    """Return the central policy catalog without returning secret values."""
    from tokenization import algorithm_specs, load_default_registry

    try:
        registry = load_default_registry()
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=503)
    return JSONResponse({
        "ok": True,
        "path": os.environ.get("TOKENIZATION_POLICY_FILE", "config.tokenization.json"),
        "algorithms": [spec.name for spec in algorithm_specs()],
        "policies": registry.list_public(),
    })


@router.post("/api/keyvault/test")
async def keyvault_test(request: Request) -> JSONResponse:
    """Live Key Vault connectivity test for the 'Test Key Vault' button.

    An optional body ``{vault_uri, auth_mode, tenant_id, client_id}`` tests
    unsaved values; otherwise the saved config is used. The service-principal
    client secret is read from ``AZURE_CLIENT_SECRET`` — never sent from the UI.
    """
    import asyncio
    from security import keyvault as kv
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - empty/invalid body => test the saved config
        body = {}
    if isinstance(body, dict) and str(body.get("vault_uri") or "").strip():
        cfg = kv.KeyVaultConfig(
            vault_uri=str(body.get("vault_uri")).strip(),
            auth_mode=(str(body.get("auth_mode") or "default").strip().lower() or "default"),
            tenant_id=str(body.get("tenant_id") or "").strip(),
            client_id=str(body.get("client_id") or "").strip(),
            client_secret=os.environ.get("AZURE_CLIENT_SECRET", ""),
        )
    else:
        cfg = kv.config_from_settings(config)
    if not cfg.enabled:
        return JSONResponse({"ok": False, "error": "no Key Vault URI configured"})
    source = kv.KeyVaultSecretSource(cfg)
    ok, detail = await asyncio.to_thread(source.probe)
    log.info("keyvault_test", vault=kv._vault_host(cfg.vault_uri), ok=ok)
    return JSONResponse({"ok": ok, "detail": detail,
                         "vault": kv._vault_host(cfg.vault_uri), "auth_mode": cfg.auth_mode})


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
        if t.connection_id in _REMOVED_CONNECTION_IDS:
            continue
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
            "enabled": t.enabled,
            "schema": schema or None,
        })
    if _BUILDER_TABLES_OVERRIDE is not None:
        tables = [dict(table) for table in _BUILDER_TABLES_OVERRIDE]

    # Named source connections (exclude the reserved 'default', which is the db_url
    # above). Passwords are masked — the builder never receives raw secrets.
    connections = []
    for cid, conn in config.CONNECTIONS.items():
        if cid == "default" or cid in _REMOVED_CONNECTION_IDS:
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
            "open_mirror_targets": _open_mirror_targets_payload(),
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
    # Phase 4 (issue #16): mirror config-file secrets (S3 secret / admin token /
    # manager password) to Key Vault when write-back is on (fail-soft).
    try:
        from security.keyvault import write_back_config_secrets
        write_back_config_secrets(clean)
    except Exception as exc:  # noqa: BLE001 - write-back must never break the save
        log.warning("config_builder_keyvault_writeback_failed", error=str(exc))
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
    # Phase 4 (issue #16): mirror config-file secrets to Key Vault (fail-soft).
    try:
        from security.keyvault import write_back_config_secrets
        write_back_config_secrets(clean)
    except Exception as exc:  # noqa: BLE001 - write-back must never break the apply
        log.warning("config_builder_keyvault_writeback_failed", error=str(exc))

    if isinstance(updates.get("connections"), list):
        restored_ids = {
            str(item.get("id") or "").strip()
            for item in updates["connections"] if isinstance(item, dict)
        }
        _REMOVED_CONNECTION_IDS.difference_update(restored_ids)
    if isinstance(updates.get("tables"), list):
        global _BUILDER_TABLES_OVERRIDE
        _BUILDER_TABLES_OVERRIDE = [dict(table) for table in updates["tables"]
                                    if isinstance(table, dict)]

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


@router.post("/api/manager/restart")
async def restart_manager(request: Request) -> JSONResponse:
    """Restart the Manager after re-authenticating with the current admin password."""
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - empty body is handled as a missing password
        body = {}
    password = str(body.get("password") or "") if isinstance(body, dict) else ""
    if not config.MANAGER_AUTH_ENABLED or not config.MANAGER_AUTH_PASSWORD:
        return JSONResponse({"ok": False, "error": "manager authentication is not configured"}, status_code=503)
    if not hmac.compare_digest(password, str(config.MANAGER_AUTH_PASSWORD)):
        return JSONResponse({"ok": False, "error": "current admin password is required"}, status_code=401)

    callback = getattr(request.app.state, "restart_manager", None)
    if callable(callback):
        result = callback()
        if asyncio.iscoroutine(result):
            result = await result
        return JSONResponse(result or {"ok": True, "action": "restart"})

    async def _do_restart() -> None:
        await asyncio.sleep(0.3)
        server = getattr(request.app.state, "uvicorn_server", None)
        if server is not None:
            server.should_exit = True
            return
        os._exit(0)

    asyncio.create_task(_do_restart())
    log.info("config_builder_manager_restart_requested")
    return JSONResponse({
        "ok": True,
        "action": "restart",
        "note": "Manager restart requested. Kubernetes or the service supervisor will start it again.",
    })


@router.delete("/api/connections/{connection_id}")
async def delete_connection(connection_id: str, request: Request) -> JSONResponse:
    """Persistently remove a named source, its table mappings, and its credential."""
    cid = str(connection_id or "").strip()
    if not cid or cid == "default":
        return JSONResponse(
            {"ok": False, "error": "the primary connection cannot be deleted"},
            status_code=400,
        )

    dependencies = [
        str(target.get("id") or "(unnamed)")
        for target in _open_mirror_targets_payload()
        if str(target.get("connection") or "default") == cid
    ]
    if dependencies:
        return JSONResponse({
            "ok": False,
            "error": (
                f"connection {cid!r} is used by Open Mirroring target(s): "
                + ", ".join(dependencies)
                + ". Remove or reassign those targets first."
            ),
        }, status_code=409)

    body = await request.json()
    connections = body.get("connections") if isinstance(body, dict) else None
    tables = body.get("tables") if isinstance(body, dict) else None
    if not isinstance(connections, list) or not isinstance(tables, list):
        return JSONResponse({
            "ok": False,
            "error": "body must contain the remaining connections and tables arrays",
        }, status_code=400)
    if any(str((item or {}).get("id") or "").strip() == cid
           for item in connections if isinstance(item, dict)):
        return JSONResponse({"ok": False, "error": "deleted connection remains in payload"},
                            status_code=400)

    clean, errors = config.validate_setting_updates({
        "connections": connections,
        "tables": tables,
    })
    if errors:
        return JSONResponse({"ok": False, "errors": errors}, status_code=400)
    try:
        saved = config.write_config_updates(clean)
    except (OSError, ValueError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    credential_removed = False
    environment_override = False
    if config.ENABLE_CREDENTIAL_STORE:
        store = _store()
        stored_url = store.get_url(cid) if cid in store.list_ids() else None
        credential_removed = store.delete(cid)
        env_name = env_var_for(cid)
        current_env = os.environ.get(env_name)
        if stored_url and current_env == stored_url:
            os.environ.pop(env_name, None)
        elif current_env:
            environment_override = True

    _REMOVED_CONNECTION_IDS.add(cid)
    log.info("connection_deleted", connection=cid, credential_removed=credential_removed,
             environment_override=environment_override)
    return JSONResponse({
        "ok": True,
        "connection_id": cid,
        "path": saved["path"],
        "credential_removed": credential_removed,
        "environment_override": environment_override,
        "restart_required": True,
    })


# ---------------------------------------------------------------------------
# Open Mirroring targets (config.open_mirror.json)
# ---------------------------------------------------------------------------

def _open_mirror_targets_payload() -> list[dict]:
    """Current Open Mirroring targets (fresh from config.open_mirror.json) for the builder."""
    try:
        from open_mirror.config import load_targets
        targets = load_targets()
    except Exception as exc:  # noqa: BLE001 - the tab must load even if the file is bad
        log.warning("open_mirror_bootstrap_failed", error=str(exc))
        return []

    def _column_payload(column) -> dict:
        payload = {
            "field_id": column.field_id,
            "name": column.name,
            "source": column.source,
            "type": column.iceberg_type,
            "nullable": column.nullable,
        }
        if column.transform:
            payload["transform"] = {
                "kind": column.transform.kind,
                "key_ref": column.transform.key_ref,
                "domain": column.transform.domain,
                "normalization": column.transform.normalization,
            }
        return payload

    out: list[dict] = []
    for t in targets:
        out.append({
            "id": t.id,
            "connection": t.connection_id,
            "landing_zone_root": t.landing_zone_root,
            "workspace_id": t.workspace_id,
            "mirrored_database_id": t.mirrored_database_id,
            "partner_name": t.partner_name,
            "source_type": t.source_type,
            "source_version": t.source_version,
            "enabled": t.enabled,
            "self_healing": t.self_healing,
            "fabric_retention_days": t.fabric_retention_days,
            "tables": [{
                "name": tb.name,
                "source_table": tb.source_table,
                "target_table": tb.target_table,
                "key_column": tb.key_column,
                "schema": tb.schema,
                "mode": tb.mode,
                "watermark_column": tb.watermark_column,
                "enabled": tb.enabled,
                "columns": (
                    [_column_payload(column) for column in tb.columns]
                    if tb.columns is not None else None
                ),
            } for tb in t.tables],
        })
    return out


@router.post("/api/open-mirror/save")
async def save_open_mirror(request: Request) -> JSONResponse:
    """Validate + persist Open Mirroring targets to config.open_mirror.json.

    Body: ``{"open_mirror_targets": [ {id, connection, landing_zone_root, tables[...]} ]}``.
    Applies on the next restart (config is import-time).
    """
    body = await request.json()
    targets = body.get("open_mirror_targets") if isinstance(body, dict) else None
    if not isinstance(targets, list):
        return JSONResponse({"ok": False, "error": 'body must be {"open_mirror_targets": [...]}'},
                            status_code=400)
    clean, errors = config.validate_setting_updates({"open_mirror_targets": targets})
    if errors:
        return JSONResponse({"ok": False, "errors": errors}, status_code=400)
    try:
        result = config.write_config_updates(clean)
    except (OSError, ValueError) as exc:
        log.warning("open_mirror_save_failed", error=str(exc))
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    log.info("open_mirror_saved", path=result["path"], count=len(targets))
    return JSONResponse({
        "ok": True,
        "path": result["path"],
        "restart_required": True,
        "note": "Saved to config.open_mirror.json. Restart the Manager/Agents to apply.",
    })


@router.post("/api/open-mirror/preview")
async def preview_open_mirror(request: Request) -> JSONResponse:
    """Validate one target and return the landing-zone folder layout it would write.

    No database access and no writes: this only computes the Fabric folder paths
    (table folders, ``_metadata.json``, first data file, ``_partnerEvents.json``)
    so the operator can confirm the target before saving.
    """
    body = await request.json()
    target = body.get("target") if isinstance(body, dict) else None
    if not isinstance(target, dict):
        return JSONResponse({"ok": False, "error": 'body must be {"target": {...}}'}, status_code=400)
    clean, errors = config.validate_setting_updates({"open_mirror_targets": [target]})
    if errors:
        return JSONResponse({"ok": False, "errors": errors}, status_code=400)

    from open_mirror.landing_zone import is_onelake_uri, table_relative_path
    from open_mirror.manifest import format_file_name
    from open_mirror.metadata import PARTNER_EVENTS_FILE, TABLE_METADATA_FILE

    root = str(target.get("landing_zone_root") or "").rstrip("/")
    layout: list[dict] = []
    for tb in (target.get("tables") or []):
        if not isinstance(tb, dict):
            continue
        target_table = str(tb.get("target_table") or tb.get("name") or "").strip()
        if not target_table:
            continue
        try:
            rel = table_relative_path(target_table, tb.get("schema"))
        except ValueError as exc:
            return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=400)
        keys = [c.strip() for c in str(tb.get("key_column") or "").split(",") if c.strip()]
        layout.append({
            "table": tb.get("name"),
            "folder": f"{root}/{rel}",
            "metadata_file": f"{root}/{rel}/{TABLE_METADATA_FILE}",
            "first_data_file": f"{root}/{rel}/{format_file_name(1)}",
            "key_columns": keys,
        })
    return JSONResponse({
        "ok": True,
        "is_onelake": is_onelake_uri(root),
        "partner_events_file": f"{root}/{PARTNER_EVENTS_FILE}",
        "layout": layout,
    })


@router.post("/api/open-mirror/health")
async def check_open_mirror_health(request: Request) -> JSONResponse:
    """Run read-only dependency checks for one edited Open Mirror target."""
    body = await request.json()
    raw_target = body.get("target") if isinstance(body, dict) else None
    if not isinstance(raw_target, dict):
        return JSONResponse(
            {"ok": False, "error": 'body must be {"target": {...}}'},
            status_code=400,
        )
    clean, errors = config.validate_setting_updates(
        {"open_mirror_targets": [raw_target]}
    )
    if errors:
        return JSONResponse({"ok": False, "errors": errors}, status_code=400)

    from sqlalchemy.engine import make_url
    from db.executor import execute_scalar
    from open_mirror.config import target_from_dict
    from open_mirror.fabric_api import get_mirroring_status
    from open_mirror.landing_zone import is_onelake_uri, open_landing_zone
    from open_mirror.source import derive_table_schema

    target_payload = clean["open_mirror_targets"][0]
    target = target_from_dict(target_payload)
    checks: list[dict] = []

    def add(check_id: str, label: str, status: str, detail: str) -> None:
        checks.append({
            "id": check_id, "label": label, "status": status, "detail": detail,
        })

    add("configuration", "Configuration", "ok", "Target configuration is valid.")

    effective_url = config.effective_db_url(target.connection_id)
    stored_url = None
    if config.ENABLE_CREDENTIAL_STORE:
        try:
            stored_url = _store().get_url(target.connection_id)
        except Exception as exc:  # noqa: BLE001 - health checks must continue
            add("credential_store", "Credential store", "warning", _clean_error(exc))
    if stored_url and stored_url != effective_url:
        add(
            "credential", "Runtime credential", "warning",
            "A newer saved credential exists. Restart the Manager before publishing.",
        )
    else:
        parsed_url = make_url(effective_url)
        auth_detail = (
            "Runtime connection includes a username and password."
            if parsed_url.username and parsed_url.password
            else "Runtime connection uses passwordless or driver-managed authentication."
        )
        add("credential", "Runtime credential", "ok", auth_detail)

    source_ready = False
    try:
        await execute_scalar("SELECT 1", connection=target.connection_id)
        source_ready = True
        add("source", "Source connection", "ok", "Source accepted a test query.")
    except Exception as exc:  # noqa: BLE001 - return the dependency failure
        add("source", "Source connection", "error", _clean_error(exc))

    for table in target.tables:
        if not table.enabled:
            add(
                f"table:{table.name}", f"Table {table.name}", "warning",
                "Table is disabled; schema check skipped.",
            )
            continue
        if not source_ready:
            add(
                f"table:{table.name}", f"Table {table.name}", "blocked",
                "Source connection failed; schema check not run.",
            )
            continue
        try:
            columns = await derive_table_schema(
                table.source_table, target.connection_id
            )
            by_name = {column.source_name: column for column in columns}
            missing = [name for name in table.key_columns if name not in by_name]
            if table.watermark_column and table.watermark_column not in by_name:
                missing.append(table.watermark_column)
            if missing:
                add(
                    f"table:{table.name}", f"Table {table.name}", "error",
                    "Missing control column(s): " + ", ".join(missing),
                )
                continue
            watermark = by_name.get(table.watermark_column or "")
            if watermark and (
                watermark.nullable or watermark.iceberg_type.lower() == "string"
            ):
                add(
                    f"table:{table.name}", f"Table {table.name}", "warning",
                    "Schema is readable, but the watermark should be non-null and "
                    "monotonic; nullable or string watermarks can stall pagination.",
                )
            else:
                add(
                    f"table:{table.name}", f"Table {table.name}", "ok",
                    f"Schema is readable; {len(columns)} column(s) reflected.",
                )
        except Exception as exc:  # noqa: BLE001 - isolate table checks
            add(
                f"table:{table.name}", f"Table {table.name}", "error",
                _clean_error(exc),
            )

    onelake = is_onelake_uri(target.landing_zone_root)
    if onelake:
        if not target.workspace_id or not target.mirrored_database_id:
            add(
                "fabric", "Fabric mirroring", "error",
                "Workspace and mirrored database IDs are required.",
            )
        else:
            try:
                payload = await asyncio.to_thread(
                    get_mirroring_status,
                    target.workspace_id,
                    target.mirrored_database_id,
                )
                status = payload.get("status") or payload.get("mirroringStatus")
                if isinstance(status, dict):
                    status = status.get("status")
                status = str(status or "Unknown")
                add(
                    "fabric", "Fabric mirroring",
                    "ok" if status == "Running" else "warning",
                    f"Mirroring status is {status}.",
                )
            except Exception as exc:  # noqa: BLE001 - continue to OneLake
                add("fabric", "Fabric mirroring", "error", _clean_error(exc))
    else:
        add(
            "fabric", "Fabric mirroring", "skipped",
            "Local landing-zone targets do not use Fabric mirroring status.",
        )

    try:
        backend = open_landing_zone(target.landing_zone_root)
        await asyncio.to_thread(backend.list_dir, "")
        add(
            "landing_zone", "Landing zone", "ok",
            "Landing-zone root is accessible for listing.",
        )
    except Exception as exc:  # noqa: BLE001 - final dependency result
        add("landing_zone", "Landing zone", "error", _clean_error(exc))

    overall = (
        "error" if any(item["status"] in {"error", "blocked"} for item in checks)
        else "warning" if any(item["status"] == "warning" for item in checks)
        else "ok"
    )
    return JSONResponse({
        "ok": overall != "error",
        "status": overall,
        "target_id": target.id,
        "checked_at": _utc_now(),
        "checks": checks,
    })


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _publish_target_payload(result) -> dict:
    return {
        "target_id": result.target_id,
        "skipped": result.skipped,
        "error": result.error,
        "dropped": result.dropped,
        "replication_status": result.replication_status,
        "replication_action": result.replication_action,
        "tables": [{
            "table": item.table, "action": item.action, "rows": item.rows,
            "inserts": item.inserts, "updates": item.updates, "deletes": item.deletes,
            "path": item.path, "error": item.error,
            "strategy": item.strategy, "reason": item.reason,
            "input_cursor": item.input_cursor, "output_cursor": item.output_cursor,
            "pages_read": item.pages_read, "rows_scanned": item.rows_scanned,
            "rows_published": item.rows_published,
            "state_status": item.state_status, "state_path": item.state_path,
            "recovery": item.recovery, "query_mode": item.query_mode,
        } for item in result.results],
    }


async def _run_open_mirror_job(job_id: str, targets: list, *, dry_run: bool, mode: str | None) -> None:
    from open_mirror.scheduler import publish_targets_with_preflight

    job = _OPEN_MIRROR_JOBS[job_id]
    job["status"] = "running"
    job["started_at"] = _utc_now()
    try:
        for target in targets:
            target_status = job["targets"][target.id]
            target_status["status"] = "running"
            target_status["started_at"] = _utc_now()
            try:
                result = (await publish_targets_with_preflight(
                    [target], dry_run=dry_run, mode=mode
                ))[0]
                payload = _publish_target_payload(result)
                failed = bool(result.error or any(item.action == "error" for item in result.results))
                target_status.update(payload)
                target_status["status"] = "error" if failed else ("skipped" if result.skipped else "completed")
            except Exception as exc:  # noqa: BLE001 - isolate each mirror in the job
                log.warning("open_mirror_publish_target_failed", target=target.id, error=str(exc))
                target_status.update({"status": "error", "error": str(exc), "tables": []})
            target_status["completed_at"] = _utc_now()
        job["status"] = (
            "completed_with_errors"
            if any(item["status"] == "error" for item in job["targets"].values())
            else "completed"
        )
    except asyncio.CancelledError:
        job["status"] = "cancelled"
        raise
    except Exception as exc:  # noqa: BLE001 - retain a queryable terminal job state
        job["status"] = "error"
        job["error"] = str(exc)
        log.warning("open_mirror_publish_job_failed", job_id=job_id, error=str(exc))
    finally:
        job["completed_at"] = _utc_now()
        log.info("open_mirror_publish_job_finished", job_id=job_id, status=job["status"])


def _open_mirror_job_response(job: dict) -> JSONResponse:
    return JSONResponse({"ok": True, "job": job})


@router.get("/api/open-mirror/publish/jobs/latest")
async def latest_open_mirror_publish_job() -> JSONResponse:
    if not _LATEST_OPEN_MIRROR_JOB_ID:
        return JSONResponse({"ok": True, "job": None})
    return _open_mirror_job_response(_OPEN_MIRROR_JOBS[_LATEST_OPEN_MIRROR_JOB_ID])


@router.get("/api/open-mirror/publish/jobs/{job_id}")
async def get_open_mirror_publish_job(job_id: str) -> JSONResponse:
    job = _OPEN_MIRROR_JOBS.get(job_id)
    if not job:
        return JSONResponse({"ok": False, "error": "publish job not found"}, status_code=404)
    return _open_mirror_job_response(job)


@router.post("/api/open-mirror/publish")
async def publish_open_mirror(request: Request) -> JSONResponse:
    """Start a background publish job and return its queryable status immediately.

    Body (all optional): ``{"target_id": "...", "dry_run": true}``. With no
    ``target_id`` every configured target is published. Failures are quarantined
    per target/table and returned in the response rather than raising.
    """
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - empty body => publish all
        body = {}
    body = body if isinstance(body, dict) else {}
    target_id = str(body.get("target_id") or "").strip() or None
    dry_run = bool(body.get("dry_run", False))
    mode = str(body.get("mode") or "").strip().lower() or None

    from open_mirror.config import load_targets
    targets = load_targets()
    if target_id:
        targets = [t for t in targets if t.id == target_id]
        if not targets:
            return JSONResponse({"ok": False, "error": f"no target with id {target_id!r}"},
                                status_code=404)
    active = next((job for job in _OPEN_MIRROR_JOBS.values()
                   if job["status"] in {"queued", "running"}), None)
    if active:
        return JSONResponse({"ok": False, "error": "a publish job is already running",
                             "job": active}, status_code=409)

    global _LATEST_OPEN_MIRROR_JOB_ID
    job_id = uuid.uuid4().hex
    job = {
        "id": job_id,
        "status": "queued",
        "dry_run": dry_run,
        "created_at": _utc_now(),
        "started_at": None,
        "completed_at": None,
        "error": None,
        "targets": {
            target.id: {"target_id": target.id, "status": "queued", "error": None,
                        "started_at": None, "completed_at": None, "tables": []}
            for target in targets
        },
    }
    _OPEN_MIRROR_JOBS[job_id] = job
    _LATEST_OPEN_MIRROR_JOB_ID = job_id
    task = asyncio.create_task(
        _run_open_mirror_job(job_id, targets, dry_run=dry_run, mode=mode),
        name=f"open-mirror-publish-{job_id[:8]}",
    )
    _OPEN_MIRROR_TASKS.add(task)
    task.add_done_callback(_OPEN_MIRROR_TASKS.discard)
    log.info("open_mirror_publish_job_started", job_id=job_id,
             target=target_id or "*", dry_run=dry_run)
    return JSONResponse({"ok": True, "job": job}, status_code=202)


@router.post("/api/open-mirror/reset")
async def reset_open_mirror_table(request: Request) -> JSONResponse:
    """Explicitly remove one table's local cursor after operator confirmation."""
    body = await request.json()
    body = body if isinstance(body, dict) else {}
    target_id = str(body.get("target_id") or "").strip()
    table_name = str(body.get("table") or "").strip()
    if not target_id or not table_name:
        return JSONResponse(
            {"ok": False, "error": "target_id and table are required"},
            status_code=400,
        )
    if body.get("confirm") is not True:
        return JSONResponse(
            {"ok": False, "error": "reset requires confirm=true"},
            status_code=400,
        )

    from open_mirror.config import load_targets
    from open_mirror.state import delete_state, load_state

    target = next((item for item in load_targets() if item.id == target_id), None)
    if target is None:
        return JSONResponse(
            {"ok": False, "error": f"no target with id {target_id!r}"},
            status_code=404,
        )
    table = next(
        (
            item for item in target.tables
            if item.name == table_name or item.target_table == table_name
        ),
        None,
    )
    if table is None:
        return JSONResponse(
            {"ok": False, "error": f"no table {table_name!r} in target {target_id!r}"},
            status_code=404,
        )
    state_dir = getattr(config, "OPEN_MIRROR_STATE_DIR", "./.open_mirror_state")
    loaded = load_state(state_dir, target, table)
    previous = None
    if loaded.state:
        previous = {
            "version": 2,
            "strategy": loaded.state.strategy,
            "initialized": loaded.state.initialized,
            "committed": (
                loaded.state.committed.to_json()
                if loaded.state.committed else None
            ),
            "pending": (
                {
                    "prior": (
                        loaded.state.pending.prior.to_json()
                        if loaded.state.pending.prior else None
                    ),
                    "next": loaded.state.pending.next.to_json(),
                    "path": loaded.state.pending.path,
                    "row_count": loaded.state.pending.row_count,
                    "content_hash": loaded.state.pending.content_hash,
                    "initial": loaded.state.pending.initial,
                }
                if loaded.state.pending else None
            ),
        }
    delete_state(state_dir, target, table)
    log.warning(
        "open_mirror_table_reset", target=target_id, table=table_name,
        previous_status=loaded.status, state_path=loaded.path,
    )
    return JSONResponse({
        "ok": True, "target_id": target_id, "table": table_name,
        "previous_status": loaded.status, "previous_state": previous,
        "state_path": loaded.path, "reason": "table_reset",
    })


def _connection_url_for(connection_id: str):
    """SQLAlchemy URL for a saved connection id (uses the stored/effective URL)."""
    from sqlalchemy.engine import make_url
    return make_url(config.effective_db_url(connection_id or "default"))


def _source_ref(source_id: str) -> tuple[str, str]:
    """Split a namespaced builder source id into its kind and runtime id."""
    kind, separator, runtime_id = str(source_id or "").partition(":")
    if separator and kind in {"database", "storage"} and runtime_id:
        return kind, runtime_id
    raise ValueError("source id must be database:<connection> or storage:<mount>")


@router.get("/api/sources")
async def list_sources() -> JSONResponse:
    """Return saved database connections and storage mounts without secrets."""
    import storage.mounts as sm

    stored = set()
    if config.ENABLE_CREDENTIAL_STORE:
        try:
            stored = {str(item.get("id")) for item in _store().status().get("connections", [])}
        except Exception:  # noqa: BLE001 - source listing must survive store failures
            stored = set()

    sources = [{
        "id": "database:default",
        "runtime_id": "default",
        "label": "Primary database",
        "kind": "database",
        "type": _flavor_from_url(config.DB_URL),
        "credential_stored": "default" in stored,
    }]
    for connection_id, connection in config.CONNECTIONS.items():
        if connection_id == "default" or connection_id in _REMOVED_CONNECTION_IDS:
            continue
        sources.append({
            "id": f"database:{connection_id}",
            "runtime_id": connection_id,
            "label": connection_id,
            "kind": "database",
            "type": _flavor_from_url(connection.db_url),
            "credential_stored": connection_id in stored,
        })
    for mount in sm.MOUNTS.values():
        sources.append({
            "id": f"storage:{mount.bucket}",
            "runtime_id": mount.bucket,
            "label": mount.bucket,
            "kind": "storage",
            "type": mount.backend,
            "format": getattr(mount, "format", "") or None,
            "credential_stored": bool(getattr(mount, "credential", "")),
        })
    return JSONResponse({"ok": True, "sources": sources})


@router.post("/api/sources/{source_id}/discover")
async def discover_source_objects(source_id: str) -> JSONResponse:
    """List selectable objects from one saved source."""
    import storage.mounts as sm

    try:
        kind, runtime_id = _source_ref(source_id)
        if kind == "database":
            async with SchemaReflector(_connection_url_for(runtime_id)) as reflector:
                objects = await reflector.list_tables()
        else:
            mount = sm.MOUNTS.get(runtime_id)
            if mount is None:
                raise ValueError(f"unknown storage source {runtime_id!r}")
            fmt = getattr(mount, "format", "") or ""
            objects = ([{"schema": None, "name": mount.bucket, "kind": fmt}]
                       if fmt in {"delta", "iceberg"} else [])
    except Exception as exc:  # noqa: BLE001 - return a clean builder error
        return JSONResponse({"ok": False, "error": _clean_error(exc)}, status_code=400)
    return JSONResponse({"ok": True, "source_id": source_id, "objects": objects})


@router.post("/api/sources/{source_id}/inspect")
async def inspect_source_object(source_id: str, request: Request) -> JSONResponse:
    """Return normalized metadata for one object in a saved source."""
    import storage.mounts as sm

    body = await request.json()
    body = body if isinstance(body, dict) else {}
    try:
        kind, runtime_id = _source_ref(source_id)
        name = str(body.get("name") or "").strip()
        schema = str(body.get("schema") or "").strip() or None
        if not name:
            raise ValueError("name is required")
        if kind == "database":
            source_table = f"{schema}.{name}" if schema else name
            async with SchemaReflector(_connection_url_for(runtime_id)) as reflector:
                available = await reflector.list_tables()
                if not any(item.get("name") == name and (item.get("schema") or None) == schema
                           for item in available):
                    raise ValueError("object is not part of the selected database source")
                columns = await reflector.columns(source_table)
                primary_key = await reflector.primary_key(source_table)
                detected_key, integer_keys = detect_key_column(columns, primary_key)
                approx_rows = await reflector.approx_row_count(source_table)
            result = {
                "name": name, "schema": schema, "source_table": source_table,
                "columns": columns, "primary_key": primary_key,
                "detected_key": detected_key, "integer_keys": integer_keys,
                "approx_rows": approx_rows,
            }
        else:
            mount = sm.MOUNTS.get(runtime_id)
            if mount is None or name != runtime_id:
                raise ValueError("object is not part of the selected storage source")
            from storage.objectstore_reader import reader_for_mount
            arrow_schema = reader_for_mount(mount).schema()
            columns = [{"name": field.name, "type": _arrow_to_iceberg_type(field.type),
                        "nullable": bool(field.nullable)} for field in arrow_schema]
            integer_keys = [column["name"] for column in columns
                            if column["type"] in {"int", "long"}]
            result = {
                "name": name, "schema": None, "source_table": name,
                "columns": columns, "primary_key": [],
                "detected_key": getattr(mount, "key_column", "") or (integer_keys[0] if integer_keys else None),
                "integer_keys": integer_keys, "approx_rows": None,
                "format": getattr(mount, "format", "") or None,
            }
    except Exception as exc:  # noqa: BLE001 - return a clean builder error
        return JSONResponse({"ok": False, "error": _clean_error(exc)}, status_code=400)
    return JSONResponse({"ok": True, "source_id": source_id, "object": result})


@router.post("/api/open-mirror/list-tables")
async def open_mirror_list_tables(request: Request) -> JSONResponse:
    """List tables/views for a SAVED connection id (for the Open Mirror table picker).

    Body: ``{"connection": "source_1"}``. Reflects using the connection's stored
    URL — no credentials are entered or returned.
    """
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - empty body => default connection
        body = {}
    connection = str((body or {}).get("connection") or "default").strip() or "default"
    try:
        url = _connection_url_for(connection)
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    try:
        async with SchemaReflector(url) as ref:
            tables = await ref.list_tables()
    except Exception as exc:  # noqa: BLE001 - surface a clean message to the UI
        log.warning("open_mirror_list_tables_failed", connection=connection, error=_clean_error(exc))
        return JSONResponse({"ok": False, "error": _clean_error(exc)}, status_code=400)
    return JSONResponse({"ok": True, "connection": connection, "tables": tables})


@router.post("/api/open-mirror/inspect-table")
async def open_mirror_inspect_table(request: Request) -> JSONResponse:
    """Detect the key column + columns for one table on a saved connection id.

    Body: ``{"connection": "source_1", "schema": "SalesLT", "name": "Customer"}``.
    """
    body = await request.json()
    body = body if isinstance(body, dict) else {}
    connection = str(body.get("connection") or "default").strip() or "default"
    schema = body.get("schema") or None
    name = body.get("name")
    if not name:
        return JSONResponse({"ok": False, "error": "name is required"}, status_code=400)
    source_table = f"{schema}.{name}" if schema else str(name)
    try:
        url = _connection_url_for(connection)
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    try:
        async with SchemaReflector(url) as ref:
            cols = await ref.columns(source_table)
            pk = await ref.primary_key(source_table)
            detected, int_keys = detect_key_column(cols, pk)
    except Exception as exc:  # noqa: BLE001
        log.warning("open_mirror_inspect_table_failed", connection=connection, error=_clean_error(exc))
        return JSONResponse({"ok": False, "error": _clean_error(exc)}, status_code=400)
    return JSONResponse({
        "ok": True, "source_table": source_table, "detected_key": detected,
        "integer_keys": int_keys, "columns": cols,
    })


def _clean_fabric_error(exc: Exception) -> str:
    """Redact secrets and add an install hint when azure-identity is missing."""
    msg = config.redact_db_url(str(exc))
    low = msg.lower()
    if "azure-identity" in low or "azure.identity" in low or "no module named 'azure" in low:
        msg += ("\nHint: install the OneLake extra so the proxy can call Fabric: "
                "pip install 'fabric-shortcut-proxy[onelake]'.")
    return msg[:400]


@router.get("/api/open-mirror/fabric/workspaces")
async def open_mirror_fabric_workspaces() -> JSONResponse:
    """List Fabric workspaces visible to the proxy's Entra identity (Open Mirror picker)."""
    from open_mirror.fabric_api import FabricApiError, list_workspaces
    try:
        workspaces = await asyncio.to_thread(list_workspaces)
    except FabricApiError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    except Exception as exc:  # noqa: BLE001 - surface a clean message (e.g. SDK missing)
        log.warning("open_mirror_fabric_workspaces_failed", error=str(exc))
        return JSONResponse({"ok": False, "error": _clean_fabric_error(exc)}, status_code=400)
    return JSONResponse({"ok": True, "workspaces": workspaces})


@router.get("/api/open-mirror/fabric/workspaces/{workspace_id}/mirrored-databases")
async def open_mirror_fabric_mirrored_dbs(workspace_id: str) -> JSONResponse:
    """List mirrored databases in a workspace, each with its computed landing-zone root."""
    from open_mirror.fabric_api import FabricApiError, list_mirrored_databases
    wsid = (workspace_id or "").strip()
    if not wsid:
        return JSONResponse({"ok": False, "error": "workspace_id is required"}, status_code=400)
    try:
        dbs = await asyncio.to_thread(list_mirrored_databases, wsid)
    except FabricApiError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    except Exception as exc:  # noqa: BLE001
        log.warning("open_mirror_fabric_mirrored_dbs_failed", workspace=wsid, error=str(exc))
        return JSONResponse({"ok": False, "error": _clean_fabric_error(exc)}, status_code=400)
    return JSONResponse({"ok": True, "workspace_id": wsid, "mirrored_databases": dbs})


# ---------------------------------------------------------------------------
# Credential store (persist DB credentials so they survive a restart)
# ---------------------------------------------------------------------------

def _store() -> CredentialStore:
    st = CredentialStore(config.CREDENTIAL_STORE_PATH or None)
    # Phase 4 (issue #16): when keyvault_write_back is on, also persist saved
    # credentials into Key Vault (fail-soft — never breaks the local save).
    try:
        from security.keyvault import attach_write_back
        attach_write_back(st)
    except Exception:  # noqa: BLE001 - write-back must never break the save path
        pass
    return st


@router.post("/api/backup")
async def download_backup(request: Request) -> Response:
    """Create a portable password-encrypted backup of all FSP-managed state."""
    body = await request.json()
    if not isinstance(body, dict):
        return JSONResponse({"ok": False, "error": "body must be a JSON object"}, status_code=400)
    password = body.get("password")
    if not isinstance(password, str):
        return JSONResponse({"ok": False, "error": "password is required"}, status_code=400)
    try:
        archive, summary = await asyncio.to_thread(
            create_backup,
            password,
            root=pathlib.Path.cwd(),
            store=_store(),
            mirror_state_dir=getattr(config, "OPEN_MIRROR_STATE_DIR", "./.open_mirror_state"),
        )
    except (BackupError, OSError, RuntimeError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
    headers = {
        "Content-Disposition": f'attachment; filename="fsp-backup-{stamp}.fspbackup"',
        "X-FSP-Backup-Summary": ",".join(f"{key}={value}" for key, value in summary.items()),
        "Cache-Control": "no-store",
    }
    return Response(archive, media_type="application/vnd.fsp.backup", headers=headers)


@router.post("/api/restore")
async def upload_restore(
    password: str = Form(...),
    backup: UploadFile = File(...),
) -> JSONResponse:
    """Restore an uploaded backup; the Manager must restart to load it."""
    archive = await backup.read(512 * 1024 * 1024 + 1)
    try:
        summary = await asyncio.to_thread(
            restore_backup,
            archive,
            password,
            root=pathlib.Path.cwd(),
            store=_store(),
            mirror_state_dir=getattr(config, "OPEN_MIRROR_STATE_DIR", "./.open_mirror_state"),
        )
    except (BackupError, OSError, RuntimeError, ValueError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    from security.access_keys import invalidate_cache
    invalidate_cache()
    return JSONResponse({"ok": True, **summary})


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
        "manager_restart_required": True,
        "note": ("Saved and restarted Agents. Restart the Manager to apply the "
                 "credential to Manager-hosted services such as Open Mirroring."
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


def _serialize_object_store_columns(cols) -> list[dict]:
    """Serialize parsed ColumnDefs back to the persisted config.mounts.json shape."""
    out: list[dict] = []
    for c in cols:
        item: dict = {"field_id": c.field_id, "name": c.name,
                      "type": c.iceberg_type, "nullable": c.nullable}
        if c.source and c.source != c.name:
            item["source"] = c.source
        if c.transform:
            t: dict = {"kind": c.transform.kind}
            if c.transform.key_ref:
                t["key_ref"] = c.transform.key_ref
            if c.transform.domain is not None:
                t["domain"] = c.transform.domain
            if c.transform.normalization and c.transform.normalization != "none":
                t["normalization"] = c.transform.normalization
            item["transform"] = t
        out.append(item)
    return out


def _object_store_capabilities() -> dict:
    """Format capability matrix + reader backend support + extra availability."""
    import importlib.util
    from storage.objectstore_capabilities import capabilities_summary
    from storage.objectstore_reader import reader_backend_support
    return {
        "formats": capabilities_summary(),
        "reader_backends": reader_backend_support(),
        "output_formats": ["auto", "delta", "iceberg"],
        "reader_available": {
            "delta": importlib.util.find_spec("deltalake") is not None,
            "iceberg": importlib.util.find_spec("pyiceberg") is not None,
        },
    }


def _arrow_to_iceberg_type(pa_type) -> str:
    """Best-effort pyarrow -> Iceberg type label for the policy editor."""
    s = str(pa_type).lower()
    if s.startswith("bool"):
        return "boolean"
    if s.startswith(("int8", "int16", "int32", "uint8", "uint16")):
        return "int"
    if s.startswith(("int64", "uint32", "uint64")):
        return "long"
    if s.startswith("double") or "float64" in s:
        return "double"
    if s.startswith(("float", "halffloat")):
        return "float"
    if s.startswith("decimal"):
        return "decimal"
    if s.startswith("date"):
        return "date"
    if s.startswith("timestamp"):
        return "timestamp"
    if s.startswith("time"):
        return "time"
    if s.startswith(("binary", "large_binary")):
        return "binary"
    return "string"


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
        fmt = str(e.get("format") or "").strip().lower()
        if fmt:
            from storage.objectstore_capabilities import (
                SUPPORTED_FORMATS, validate_object_store_policy,
            )
            if fmt not in SUPPORTED_FORMATS:
                errors.append(f"mounts[{i}]: unsupported table format {fmt!r} "
                              f"(use one of {list(SUPPORTED_FORMATS)})")
                continue
            key_column = str(e.get("key_column") or "").strip()
            raw_columns = e.get("columns") or []
            if not raw_columns:
                errors.append(f"mounts[{i}]: tokenizing mount {bucket!r} needs a 'columns' policy")
                continue
            try:
                parsed = sm._parse_columns(raw_columns)
                validate_object_store_policy(format=fmt, key_column=key_column,
                                             columns=list(parsed))
            except ValueError as exc:
                errors.append(f"mounts[{i}]: {exc}")
                continue
            out_fmt = str(e.get("output_format") or "").strip().lower()
            if out_fmt and out_fmt not in ("auto", "delta", "iceberg"):
                errors.append(f"mounts[{i}]: output_format {out_fmt!r} must be "
                              f"'auto', 'delta', or 'iceberg'")
                continue
            entry["format"] = fmt
            entry["key_column"] = key_column
            entry["columns"] = _serialize_object_store_columns(parsed)
            if out_fmt:
                entry["output_format"] = out_fmt
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
        if getattr(m, "format", ""):
            entry["format"] = m.format
            entry["key_column"] = m.key_column
            entry["columns"] = _serialize_object_store_columns(m.columns)
            if getattr(m, "output_format", ""):
                entry["output_format"] = m.output_format
        mounts.append(entry)
    return JSONResponse({
        "ok": True,
        "enabled": bool(config.ENABLE_STORAGE_PROXY),
        "supported_backends": list(sm._SUPPORTED_BACKENDS),
        "reserved_bucket": config.BUCKET_NAME,
        "object_store": _object_store_capabilities(),
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


@router.post("/api/mounts/inspect")
async def inspect_mount(request: Request) -> JSONResponse:
    """Reflect a tokenizing mount's source columns through the object-store reader.

    Builds a transient mount from the posted form and calls the Delta/Iceberg
    reader's ``schema()`` so the Config Builder can populate the column policy
    editor. Requires the ``objectstore`` extra and a reachable source (and, for
    s3/azure, the credential already saved) — same trust as the mount test.
    """
    import storage.mounts as sm
    body = await request.json()
    fmt = str(body.get("format") or "").strip().lower()
    if not fmt:
        return JSONResponse({"ok": False, "error": "pick a table format (delta or iceberg) first"})
    try:
        mount = sm._mount_from_json({**body, "columns": []})
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": _clean_error(exc)})
    try:
        from storage.objectstore_reader import reader_for_mount
        schema = reader_for_mount(mount).schema()
    except Exception as exc:  # noqa: BLE001 - surface a clean, secret-free message
        return JSONResponse({"ok": False, "error": _clean_error(exc)})
    columns = [{"name": f.name, "type": _arrow_to_iceberg_type(f.type),
                "nullable": bool(f.nullable)} for f in schema]
    return JSONResponse({"ok": True, "columns": columns, "count": len(columns)})


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
