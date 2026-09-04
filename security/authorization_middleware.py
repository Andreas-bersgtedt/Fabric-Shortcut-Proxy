"""Opt-in function authorization for operator HTTP surfaces."""
from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from security.authorization import AuthorizationError, authenticate_request, require
from security.identity import identity_provider

_EXEMPT_PREFIXES = ("/healthz", "/readyz", "/favicon.ico")


def _permission(path: str, method: str) -> str | None:
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
    if any(path.startswith(f"/_config/api/{prefix}") for prefix in (
        "credentials", "s3-credentials", "azure-credentials", "access-keys",
        "backup", "restore",
    )):
        return "security.metadata.read" if method in {"GET", "HEAD"} else "security.credentials.admin"
    if path.startswith("/_config/api/keyvault"):
        return "security.metadata.read"
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

    async def dispatch(self, request: Request, call_next):
        if request.headers.get("x-fsp-authz-bypass") == "1":
            return JSONResponse({"ok": False, "error": "invalid authorization request"}, status_code=401)
        if request.url.path.startswith(_EXEMPT_PREFIXES):
            return await call_next(request)
        permission = _permission(request.url.path, request.method.upper())
        if permission is None:
            return await call_next(request)
        import os
        if os.environ.get("FSP_AUTHZ_ENFORCE", "0").strip() != "1":
            return await call_next(request)
        user = authenticate_request(
            request.headers.get("x-admin-token", ""),
            request.cookies.get("fsp_session", ""),
        )
        if user is None:
            return JSONResponse({"ok": False, "error": "authentication required"}, status_code=401)
        try:
            decision = require(user, permission, _context(request))
        except AuthorizationError:
            return JSONResponse({"ok": False, "error": "permission denied"}, status_code=403)
        request.state.authorization = decision
        request.state.user = user
        return await call_next(request)
