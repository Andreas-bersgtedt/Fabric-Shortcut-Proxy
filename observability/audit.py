"""
Audit logging for storage-proxy access (devplan/StorageProxy.md, Phase 4-C).

Emits one structured event per mounted-object access — identity, bucket, key,
backend, method, status, bytes — through a dedicated ``audit`` structlog channel
(and optionally an append-only file). Secrets are never included; free-text values
pass through the existing scrubber. A small in-memory ring keeps the most recent
events for the admin/monitor to surface.

Gated by ``config.ENABLE_AUDIT_LOG`` and only called for mounted buckets, so the
DB→Iceberg serving path is unaffected.
"""
from __future__ import annotations

import hashlib
import threading
import time
from collections import deque

from observability.logging import get_logger

try:
    from security.credentials import scrub_secrets as _scrub
except Exception:  # noqa: BLE001 - scrubber is best-effort
    def _scrub(s: str) -> str:  # type: ignore
        return s

_log = get_logger("audit")

_lock = threading.Lock()
_buf: deque[dict] = deque(maxlen=512)
_fh_lock = threading.Lock()
_fh = None
_fh_path = ""


class AuditUnavailable(RuntimeError):
    """Raised when a required audit event cannot reach the configured file sink."""


def _file_handle():
    """Lazily open (and reopen on path change) the optional audit file."""
    global _fh, _fh_path
    import config
    path = (getattr(config, "AUDIT_LOG_FILE", "") or "").strip()
    if not path:
        if _fh is not None:
            try:
                _fh.close()
            finally:
                _fh, _fh_path = None, ""
        return None
    if _fh is None or path != _fh_path:
        try:
            if _fh is not None:
                _fh.close()
            _fh = open(path, "a", encoding="utf-8")
            _fh_path = path
        except OSError as exc:  # noqa: BLE001
            _log.warning("audit_file_open_failed", path=path, error=str(exc))
            _fh, _fh_path = None, ""
    return _fh


def record(*, identity: str, bucket: str, key: str, backend: str, method: str,
           status: int, action: str = "access", bytes_: int = 0, client: str = "",
           reason: str = "") -> None:
    """Record one audit event (no-op when auditing is disabled)."""
    import config
    if not getattr(config, "ENABLE_AUDIT_LOG", True):
        return
    event = {
        "ts": time.time(),
        "identity": identity or "-",
        "client": client or "-",
        "method": method,
        "action": action,
        "bucket": bucket,
        "key": _scrub(key or ""),
        "backend": backend,
        "status": status,
        "bytes": int(bytes_ or 0),
    }
    if reason:
        event["reason"] = _scrub(reason)
    with _lock:
        _buf.append(event)
    _log.info("audit", **event)
    with _fh_lock:
        fh = _file_handle()
        if fh is not None:
            try:
                import json
                fh.write(json.dumps(event, separators=(",", ":")) + "\n")
                fh.flush()
            except OSError as exc:  # noqa: BLE001
                _log.warning("audit_file_write_failed", error=str(exc))
def record_mount_operation(
    *, request_id: str, identity: str, operation: str, provider_id: str,
    destination: str, status: int, outcome: str, reason: str = "",
) -> None:
    """Record a mount test/inspection outcome without request bodies or provider output."""
    import config
    if not getattr(config, "ENABLE_AUDIT_LOG", True):
        return
    event = {
        "ts": time.time(),
        "request_id": request_id,
        "identity": identity or "-",
        "action": "mount_operation",
        "operation": operation,
        "provider_id": _scrub(provider_id or "-"),
        "destination": _scrub(destination or "-"),
        "status": int(status),
        "outcome": outcome,
    }
    if reason:
        event["reason"] = _scrub(reason)
    with _lock:
        _buf.append(event)
    _log.info("audit", **event)
    with _fh_lock:
        fh = _file_handle()
        if fh is not None:
            try:
                import json
                fh.write(json.dumps(event, separators=(",", ":")) + "\n")
                fh.flush()
            except OSError as exc:  # noqa: BLE001
                _log.warning("audit_file_write_failed", error=str(exc))


def record_operator_auth(
    *, request_id: str, identity: str, path: str, method: str,
    status: int, outcome: str, reason: str = "",
) -> None:
    """Record an operator authentication decision without credential material."""
    import config
    if not getattr(config, "ENABLE_AUDIT_LOG", True):
        return
    event = {
        "ts": time.time(),
        "request_id": request_id,
        "identity": identity or "-",
        "method": method,
        "action": "operator_auth",
        "path": path,
        "status": int(status),
        "outcome": outcome,
    }
    if reason:
        event["reason"] = _scrub(reason)
    with _lock:
        _buf.append(event)
    _log.info("audit", **event)
    with _fh_lock:
        fh = _file_handle()
        if fh is not None:
            try:
                import json
                fh.write(json.dumps(event, separators=(",", ":")) + "\n")
                fh.flush()
            except OSError as exc:  # noqa: BLE001
                _log.warning("audit_file_write_failed", error=str(exc))


def record_directory_search(
    *, request_id: str, identity: str, object_type: str, query: str,
    result_count: int, status: int, outcome: str,
) -> None:
    """Record a bounded Entra directory search without storing the query text."""
    import config

    if not getattr(config, "ENABLE_AUDIT_LOG", True):
        return
    fingerprint = hashlib.sha256(query.encode(), usedforsecurity=False).hexdigest()[:16]
    event = {
        "ts": time.time(),
        "request_id": request_id,
        "identity": identity or "-",
        "action": "entra_directory_search",
        "object_type": object_type,
        "query_length": len(query),
        "query_fingerprint": fingerprint,
        "result_count": int(result_count),
        "status": int(status),
        "outcome": outcome,
    }
    with _lock:
        _buf.append(event)
    _log.info("audit", **event)
    with _fh_lock:
        fh = _file_handle()
        if fh is not None:
            try:
                import json
                fh.write(json.dumps(event, separators=(",", ":")) + "\n")
                fh.flush()
            except OSError as exc:  # noqa: BLE001
                _log.warning("audit_file_write_failed", error=str(exc))


def record_reidentification(
    *,
    request_id: str,
    identity: str,
    method: str,
    status: int,
    outcome: str,
    reason: str,
    policy_id: str = "",
    table_id: str = "",
    column_id: str = "",
    token_fingerprint: str = "",
    case_reference: str = "",
    match_count: int | None = None,
    latency_ms: int | None = None,
) -> None:
    """Persist a required redacted re-identification audit event.

    This deliberately accepts no token, clear-text result, SQL, or credentials.
    ``AUDIT_LOG_FILE`` is required because the in-memory audit ring is not durable.
    """
    import config

    if not getattr(config, "ENABLE_AUDIT_LOG", True):
        raise AuditUnavailable("re-identification requires ENABLE_AUDIT_LOG=1")
    if not (getattr(config, "AUDIT_LOG_FILE", "") or "").strip():
        raise AuditUnavailable("re-identification requires AUDIT_LOG_FILE")
    event = {
        "ts": time.time(),
        "request_id": request_id,
        "identity": identity or "-",
        "method": method,
        "action": "reidentification_request",
        "status": status,
        "outcome": _scrub(outcome),
        "reason": _scrub(reason),
    }
    for name, value in {
        "policy_id": policy_id,
        "table_id": table_id,
        "column_id": column_id,
        "token_fingerprint": token_fingerprint,
        "case_reference": case_reference,
    }.items():
        if value:
            event[name] = _scrub(value)
    if match_count is not None:
        event["match_count"] = int(match_count)
    if latency_ms is not None:
        event["latency_ms"] = int(latency_ms)
    with _fh_lock:
        fh = _file_handle()
        if fh is None:
            raise AuditUnavailable("re-identification audit file is unavailable")
        try:
            import json
            fh.write(json.dumps(event, separators=(",", ":")) + "\n")
            fh.flush()
        except OSError as exc:
            raise AuditUnavailable("re-identification audit write failed") from exc
    with _lock:
        _buf.append(event)
    _log.info("audit", **event)


def recent(limit: int = 100) -> list[dict]:
    """Return the most recent audit events (newest last)."""
    with _lock:
        items = list(_buf)
    return items[-limit:]
