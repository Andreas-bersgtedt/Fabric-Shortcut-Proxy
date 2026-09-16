"""Shared Basic, local-session, and OIDC authentication for operator routes."""
from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

import config

_EXEMPT_PREFIXES = ("/healthz", "/readyz", "/favicon.ico")
_DEFAULT_OPERATOR_PREFIXES = (
    "/_admin", "/_config", "/_manager", "/_monitor", "/agents", "/control",
)
_REALM = "Fabric Shortcut Proxy Manager"
_INTERNAL_MONITOR_HEADER = "x-fsp-internal-monitor"


def internal_monitor_ok(request: Request) -> bool:
    """True if the Manager's fleet-scrape presented a valid shared token.

    The Manager's /_monitor scrape (control/monitor_proxy.py) authenticates
    with FSP_INTERNAL_MONITOR_TOKEN instead of an operator credential — this
    lets operator-facing auth (Basic/session/RBAC) stay enforced for humans
    while still allowing the internal Manager->Agent control-plane call.
    """
    import os

    if not request.url.path.startswith("/_monitor/api/"):
        return False
    expected = os.environ.get("FSP_INTERNAL_MONITOR_TOKEN", "")
    presented = request.headers.get(_INTERNAL_MONITOR_HEADER, "")
    return bool(expected) and bool(presented) and hmac.compare_digest(presented, expected)
_IDENTITY_BOOTSTRAP_PATHS = {
    "/_config",
    "/_config/",
    "/_config/api/authorization/login",
    "/_config/api/authorization/status",
    "/_config/api/authorization/msal-config",
    "/_manager",
    "/_manager/",
    "/_manager/api/authorization/msal-config",
}


def is_operator_route(path: str, prefixes: tuple[str, ...] = _DEFAULT_OPERATOR_PREFIXES) -> bool:
    return any(path == prefix or path.startswith(f"{prefix}/") for prefix in prefixes)


def manager_auth_active() -> bool:
    return bool(config.MANAGER_AUTH_ENABLED)


def _unauthorized() -> Response:
    if config.ENTRA_ENABLED:
        return JSONResponse({"detail": "authentication required"}, status_code=401)
    return JSONResponse(
        {"detail": "authentication required"},
        status_code=401,
        headers={"WWW-Authenticate": f'Basic realm="{_REALM}"'},
    )


def _config_unauthorized() -> Response:
    return JSONResponse({"detail": "authentication required"}, status_code=401)


def _misconfigured() -> Response:
    return JSONResponse({"detail": "manager authentication is not configured"}, status_code=503)


def _credentials_ok(header: str) -> bool:
    scheme, _, encoded = header.partition(" ")
    if scheme.lower() != "basic" or not encoded:
        return False
    try:
        raw = base64.b64decode(encoded, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        return False
    user, sep, password = raw.partition(":")
    if not sep:
        return False
    return hmac.compare_digest(user, config.MANAGER_AUTH_USERNAME) and hmac.compare_digest(
        password, config.MANAGER_AUTH_PASSWORD
    )


def _session_ok(request: Request) -> bool:
    token = request.cookies.get("fsp_session", "")
    if not token:
        return False
    try:
        from security.identity import identity_provider
        user = identity_provider().resolve_session(token)
    except (OSError, ValueError):
        return False
    if user is None:
        return False
    request.state.user = user
    return True


def _bearer_ok(request: Request) -> bool:
    from security.authorization import bearer_token

    if getattr(request.state, "user", None) is not None:
        return True
    token = bearer_token(request.headers.get("authorization", ""))
    if not token:
        return False
    try:
        from security.identity import authenticate_entra_token, authenticate_oidc_token
        user = authenticate_entra_token(token)
        if user is None:
            user = authenticate_oidc_token(token)
    except (OSError, RuntimeError, ValueError):
        return False
    if user is None:
        return False
    request.state.user = user
    return True


def _audit_operator_auth(
    request: Request, status: int, outcome: str, reason: str, identity: str = ""
) -> None:
    from observability import audit

    audit.record_operator_auth(
        request_id=request.headers.get("x-request-id", "").strip()[:128] or str(uuid.uuid4()),
        identity=identity,
        path=request.url.path[:256],
        method=request.method,
        status=status,
        outcome=outcome,
        reason=reason,
    )


class ManagerAuthMiddleware(BaseHTTPMiddleware):
    """Require credentials on the configured operator route prefixes."""

    def __init__(
        self,
        app,
        *,
        operator_only: bool = False,
        operator_prefixes: tuple[str, ...] = _DEFAULT_OPERATOR_PREFIXES,
    ):
        super().__init__(app)
        self._operator_only = operator_only
        self._operator_prefixes = operator_prefixes

    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith(_EXEMPT_PREFIXES):
            return await call_next(request)
        if internal_monitor_ok(request):
            return await call_next(request)
        if self._operator_only and not is_operator_route(request.url.path, self._operator_prefixes):
            return await call_next(request)
        if not manager_auth_active():
            _audit_operator_auth(request, 503, "failed", "operator authentication disabled")
            return _misconfigured()
        if not config.MANAGER_AUTH_PASSWORD:
            _audit_operator_auth(request, 503, "failed", "operator authentication incomplete")
            return _misconfigured()
        if request.url.path in _IDENTITY_BOOTSTRAP_PATHS:
            response = await call_next(request)
            _audit_operator_auth(request, response.status_code, "bootstrap", "identity bootstrap")
            return response
        basic_ok = _credentials_ok(request.headers.get("authorization", ""))
        if basic_ok:
            from security.authorization import User
            request.state.user = User("manager-basic", roles=("system_administrator",))
        session_ok = False if basic_ok else _session_ok(request)
        bearer_ok = (
            False if basic_ok or session_ok
            else await asyncio.to_thread(_bearer_ok, request)
        )
        if not (basic_ok or session_ok or bearer_ok):
            _audit_operator_auth(request, 401, "denied", "authentication required")
            if request.url.path.startswith("/_config"):
                return _config_unauthorized()
            return _unauthorized()
        response = await call_next(request)
        user = getattr(request.state, "user", None)
        identity = user.user_id if user is not None else "authenticated"
        _audit_operator_auth(request, response.status_code, "authenticated", "operator request", identity)
        return response