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
    fh = None
    with _fh_lock:
        fh = _file_handle()
        if fh is not None:
            try:
                import json
                fh.write(json.dumps(event, separators=(",", ":")) + "\n")
                fh.flush()
            except OSError as exc:  # noqa: BLE001
                _log.warning("audit_file_write_failed", error=str(exc))


def recent(limit: int = 100) -> list[dict]:
    """Return the most recent audit events (newest last)."""
    with _lock:
        items = list(_buf)
    return items[-limit:]
