"""Opt-in function authorization for operator HTTP surfaces."""
from __future__ import annotations

from collections import defaultdict, deque
import threading
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from security.authorization import (
    AuthorizationError,
    authenticate_request,
    bearer_token,
    require,
)

_EXEMPT_PREFIXES = ("/healthz", "/readyz", "/favicon.ico")
_MOUNT_OPERATIONS = {
    "/_config/api/mounts/test": "test",
    "/_config/api/mounts/inspect": "inspect",
}


def _audit_reidentification(request: Request, status: int, reason: str, identity: str = "") -> bool:
    """Record the route outcome without reading a sensitive request body."""
    if not request.url.path.startswith("/_reidentify/"):
        return True
    from observability import audit
    import uuid

    try:
        audit.record_reidentification(
        request_id=request.headers.get("x-request-id", "").strip()[:128] or str(uuid.uuid4()),
        identity=identity,
        method=request.method,
        status=status,
        outcome="denied" if status in {401, 403} else "failed",
        reason=reason,
        )
    except audit.AuditUnavailable:
        return False
    return True


def authorization_enforced(path: str = "/_config") -> bool:
    """Require RBAC explicitly, or for operator routes when operator auth is active."""
    import os

    if path.startswith("/_reidentify/"):
        return True
    if os.environ.get("FSP_AUTHZ_ENFORCE", "0").strip() == "1":
        return True
    import config
    from security.operator_auth import is_operator_route
    return bool(
        is_operator_route(path)
        and config.MANAGER_AUTH_ENABLED
        and config.MANAGER_AUTH_PASSWORD
    )


def _permission(path: str, method: str) -> str | None:
    if path.startswith("/_reidentify/api/v1/lookup"):
        return "tokenization.reidentify"
    if path.startswith("/_admin"):
        return "monitor.read" if method in {"GET", "HEAD"} else "system.admin"
    if path == "/agents" or path.startswith("/agents/"):
        return "monitor.read" if method in {"GET", "HEAD"} else "system.admin"
    if path == "/control" or path.startswith("/control/"):
        return "system.admin"
    if not (path.startswith("/_config") or path.startswith("/_manager") or path.startswith("/_monitor")):
        return None
    if path in {"/_config/api/authorization/login", "/_config/api/authorization/status", "/_config/", "/_config"}:
        return None
    if path.startswith("/_config/api/authorization/logout") or path.startswith("/_config/api/authorization/me"):
        return "monitor.read"
    if path.startswith("/_config/api/authorization/users"):
        return "users.admin"
    if path.startswith("/_config/api/tokenization/policies"):
        return "tokenization.policy.read" if method in {"GET", "HEAD"} else "tokenization.policy.admin"
    if path.startswith("/_config/api/tokenization-keys"):
        return "security.metadata.read" if method in {"GET", "HEAD"} else "security.credentials.admin"
    if path.startswith("/_config/api/reidentification/mappings"):
        return "system.admin"
    if any(path.startswith(f"/_config/api/{prefix}") for prefix in (
        "credentials", "s3-credentials", "azure-credentials", "access-keys",
        "backup", "restore",
    )):
        return "security.metadata.read" if method in {"GET", "HEAD"} else "security.credentials.admin"
    if path.startswith("/_config/api/keyvault"):
        return "security.metadata.read"
    if path in {"/_config/api/mounts/test", "/_config/api/mounts/inspect"}:
        return "storage.mount.inspect"
    if path.startswith("/_manager/api/health") or path.startswith("/_monitor"):
        return "monitor.read"
    if path.startswith("/_manager/api/"):
        return "system.admin" if method not in {"GET", "HEAD"} else "monitor.read"
    if path.startswith("/_config/api/"):
        return "config.write" if method not in {"GET", "HEAD"} else "config.read"
    return None


def _context(request: Request) -> dict[str, str]:
    """Build bounded context from route-owned identifiers only.

    Caller-supplied context headers and query parameters are intentionally not
    authorization inputs; routes must not let a client claim a broader scope.
    """
    context: dict[str, str] = {}
    path = request.url.path
    if path.startswith("/_config/api/tokenization/policies/"):
        policy_id = path.rsplit("/", 1)[-1]
        if policy_id and len(policy_id) <= 200:
            context["policy_namespace"] = policy_id
    if path.startswith("/_config/api/authorization/users/"):
        user_id = path.rsplit("/", 1)[-1]
        if user_id and len(user_id) <= 200:
            context["user"] = user_id
    return context


class AuthorizationMiddleware(BaseHTTPMiddleware):
    """Enforce named permissions on operator APIs when explicitly enabled."""

    def __init__(self, app):
        super().__init__(app)
        import config

        self._mount_rate = max(1, int(config.MOUNT_TEST_REQUESTS_PER_MINUTE))
        self._mount_max_active = max(1, int(config.MOUNT_TEST_MAX_CONCURRENCY))
        self._mount_hits: dict[str, deque[float]] = defaultdict(deque)
        self._mount_active = 0
        self._mount_lock = threading.Lock()

    def _mount_rate_allowed(self, identity: str) -> bool:
        now = time.monotonic()
        with self._mount_lock:
            hits = self._mount_hits[identity]
            while hits and hits[0] <= now - 60:
                hits.popleft()
            if len(hits) >= self._mount_rate:
                return False
            hits.append(now)
            return True

    def _acquire_mount_slot(self) -> bool:
        with self._mount_lock:
            if self._mount_active >= self._mount_max_active:
                return False
            self._mount_active += 1
            return True

    def _release_mount_slot(self) -> None:
        with self._mount_lock:
            self._mount_active -= 1

    @staticmethod
    def _audit_mount(request: Request, status: int, outcome: str, reason: str, identity: str = "") -> None:
        from observability import audit

        audit.record_mount_operation(
            request_id=request.headers.get("x-request-id", "").strip()[:128] or str(uuid.uuid4()),
            identity=identity,
            operation=_MOUNT_OPERATIONS[request.url.path],
            provider_id=getattr(request.state, "mount_provider_id", ""),
            destination=getattr(request.state, "mount_destination", ""),
            status=status,
            outcome=outcome,
            reason=reason,
        )

    async def dispatch(self, request: Request, call_next):
        mount_operation = request.url.path in _MOUNT_OPERATIONS
        if request.headers.get("x-fsp-authz-bypass") == "1":
            if mount_operation:
                self._audit_mount(request, 401, "denied", "invalid authorization request")
            return JSONResponse({"ok": False, "error": "invalid authorization request"}, status_code=401)
        if request.url.path.startswith(_EXEMPT_PREFIXES):
            return await call_next(request)
        permission = _permission(request.url.path, request.method.upper())
        if permission is None:
            return await call_next(request)
        user = getattr(request.state, "user", None)
        enforced = authorization_enforced(request.url.path)
        if enforced:
            if user is None:
                user = authenticate_request(
                    request.headers.get("x-admin-token", ""),
                    request.cookies.get("fsp_session", ""),
                    bearer_token(request.headers.get("authorization", "")),
                )
            if user is None:
                if mount_operation:
                    self._audit_mount(request, 401, "denied", "authentication required")
                if not _audit_reidentification(request, 401, "authentication required"):
                    return JSONResponse({"ok": False, "error": "audit service unavailable"}, status_code=503)
                return JSONResponse({"ok": False, "error": "authentication required"}, status_code=401)
            try:
                decision = require(user, permission, _context(request))
            except AuthorizationError:
                if mount_operation:
                    self._audit_mount(request, 403, "denied", "permission denied", user.user_id)
                if not _audit_reidentification(request, 403, "permission denied", user.user_id):
                    return JSONResponse({"ok": False, "error": "audit service unavailable"}, status_code=503)
                return JSONResponse({"ok": False, "error": "permission denied"}, status_code=403)
            request.state.authorization = decision
            request.state.user = user

        if not mount_operation:
            return await call_next(request)
        identity = user.user_id if user is not None else "unauthenticated"
        if not self._mount_rate_allowed(identity):
            self._audit_mount(request, 429, "denied", "request limit exceeded", identity)
            return JSONResponse({"ok": False, "error": "request limit exceeded"}, status_code=429)
        if not self._acquire_mount_slot():
            self._audit_mount(request, 429, "denied", "concurrency limit exceeded", identity)
            return JSONResponse({"ok": False, "error": "concurrency limit exceeded"}, status_code=429)
        try:
            response = await call_next(request)
        except Exception:
            self._audit_mount(request, 500, "failed", "operation failed", identity)
            raise
        finally:
            self._release_mount_slot()
        outcome = "allowed" if response.status_code < 400 else "failed"
        self._audit_mount(request, response.status_code, outcome, "operation completed", identity)
        return response
