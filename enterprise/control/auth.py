"""
Standalone HTTP Basic auth for the Manager control-plane surface.

Gates the operator surface on the Manager port — the ``/_manager`` console,
``/_config`` builder, ``/_monitor`` fleet view, ``/agents``, and the root — behind
a single username/password. Machine-to-machine and liveness endpoints stay open
so the cluster keeps working:

  - ``/control/*`` — Agents register + heartbeat here (they don't carry the
    console password; this channel is meant to be scoped by the network).
  - ``/healthz`` / ``/readyz`` — load-balancer probes.

The Manager operator and control-plane surfaces are protected by default. A missing
password produces a fail-closed 503 response rather than exposing the service.
"""
from __future__ import annotations

import base64
import binascii
import hmac

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

import config

# Endpoints that must stay reachable without the console password.
_EXEMPT_PREFIXES = ("/healthz", "/readyz", "/favicon.ico")

_REALM = "Fabric Shortcut Proxy Manager"


def manager_auth_active() -> bool:
    """True when Manager authentication is enabled."""
    return bool(config.MANAGER_AUTH_ENABLED)


def _unauthorized() -> Response:
    return JSONResponse(
        {"detail": "authentication required"},
        status_code=401,
        headers={"WWW-Authenticate": f'Basic realm="{_REALM}"'},
    )


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
    user, sep, pw = raw.partition(":")
    if not sep:
        return False
    # Constant-time compare on both fields (compare_digest is length-safe).
    user_ok = hmac.compare_digest(user, config.MANAGER_AUTH_USERNAME)
    pw_ok = hmac.compare_digest(pw, config.MANAGER_AUTH_PASSWORD)
    return user_ok and pw_ok


def _session_ok(request: Request) -> bool:
    """Accept a valid local identity session alongside legacy Basic auth."""
    token = request.cookies.get("fsp_session", "")
    if not token:
        return False
    try:
        from security.identity import identity_provider
        return identity_provider().resolve_session(token) is not None
    except (OSError, ValueError):
        return False


class ManagerAuthMiddleware(BaseHTTPMiddleware):
    """Require HTTP Basic credentials for the Manager's operator surface."""

    async def dispatch(self, request: Request, call_next):
        if not manager_auth_active():
            return _misconfigured()
        if request.url.path.startswith(_EXEMPT_PREFIXES):
            return await call_next(request)
        if not config.MANAGER_AUTH_PASSWORD:
            return _misconfigured()
        if not _credentials_ok(request.headers.get("authorization", "")) and not _session_ok(request):
            return _unauthorized()
        return await call_next(request)
