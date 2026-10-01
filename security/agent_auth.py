"""Shared Agent authentication transport contract."""
from __future__ import annotations

import hmac
import os
import time
import uuid
from dataclasses import dataclass

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

AGENT_TOKEN_HEADER = "X-FSP-Agent-Token"
AGENT_ID_HEADER = "X-FSP-Agent-ID"
AGENT_AUTH_CHALLENGE = "FSP-Agent"
_MAX_AGENT_ID_LENGTH = 128
_DUMMY_TOKEN = "0" * 64

_AGENT_EXACT_ROUTES = frozenset({
    ("POST", "/control/register"),
    ("POST", "/control/heartbeat"),
    ("POST", "/control/task-result"),
    ("POST", "/control/materialize"),
})
_AGENT_PREFIX_ROUTES = (
    ("GET", "/control/assignment/"),
    ("GET", "/control/snapshot/"),
)
_OPERATOR_CONTROL_EXACT_ROUTES = frozenset({
    ("GET", "/control/work-queue"),
})
_OPERATOR_CONTROL_PREFIX_ROUTES = (
    ("POST", "/control/work-queue/requests/", "cancel"),
    ("POST", "/control/work-queue/tasks/", "retry"),
)


def _matches_single_segment(method: str, path: str, route_method: str, prefix: str) -> bool:
    suffix = path[len(prefix):] if path.startswith(prefix) else ""
    return method == route_method and bool(suffix) and "/" not in suffix


def _matches_operator_action(
    method: str, path: str, route_method: str, prefix: str, action: str
) -> bool:
    suffix = path[len(prefix):] if path.startswith(prefix) else ""
    identifier, separator, candidate_action = suffix.partition("/")
    return (
        method == route_method
        and bool(identifier)
        and separator == "/"
        and candidate_action == action
    )


def is_agent_route(method: str, path: str) -> bool:
    """Return whether the method/path pair is an Agent control request."""
    normalized_method = method.upper()
    if (normalized_method, path) in _AGENT_EXACT_ROUTES:
        return True
    return any(
        _matches_single_segment(normalized_method, path, route_method, prefix)
        for route_method, prefix in _AGENT_PREFIX_ROUTES
    )


def is_operator_control_route(method: str, path: str) -> bool:
    """Return whether the method/path pair is an operator queue request."""
    normalized_method = method.upper()
    if (normalized_method, path) in _OPERATOR_CONTROL_EXACT_ROUTES:
        return True
    return any(
        _matches_operator_action(normalized_method, path, route_method, prefix, action)
        for route_method, prefix, action in _OPERATOR_CONTROL_PREFIX_ROUTES
    )


def agent_authentication_required() -> Response:
    """Return the generic Agent authentication denial response."""
    return JSONResponse(
        {"detail": "agent authentication required"},
        status_code=401,
        headers={"WWW-Authenticate": AGENT_AUTH_CHALLENGE},
    )


def agent_authentication_unavailable() -> Response:
    """Return the generic Agent authentication configuration response."""
    return JSONResponse(
        {"detail": "agent authentication unavailable"},
        status_code=503,
    )


@dataclass(frozen=True)
class AgentTokenResult:
    accepted: bool
    reason: str


class AgentTokenProvider:
    """Verify fleet tokens from the live environment without exposing values."""

    @staticmethod
    def verify(presented: str, *, now: float | None = None) -> AgentTokenResult:
        import config

        active = os.environ.get("AGENT_TOKEN", "")
        previous = os.environ.get("AGENT_TOKEN_PREVIOUS", "")
        deadline_raw = os.environ.get(
            "AGENT_TOKEN_PREVIOUS_VALID_UNTIL",
            str(getattr(config, "AGENT_TOKEN_PREVIOUS_VALID_UNTIL", 0)),
        )
        mode = os.environ.get(
            "AGENT_AUTH_MODE", getattr(config, "AGENT_AUTH_MODE", "compatibility")
        ).strip().lower()

        if mode == "required" and (
            not active
            or (previous and hmac.compare_digest(active, previous))
        ):
            return AgentTokenResult(False, "misconfigured")
        if not presented:
            return AgentTokenResult(False, "missing")
        try:
            encoded = presented.encode("ascii")
        except UnicodeEncodeError:
            return AgentTokenResult(False, "malformed")
        if len(encoded) < 32 or any(byte < 0x21 or byte > 0x7E for byte in encoded):
            return AgentTokenResult(False, "malformed")

        try:
            deadline = int(deadline_raw)
        except ValueError:
            deadline = 0
        current_time = time.time() if now is None else now
        active_candidate = active or _DUMMY_TOKEN
        previous_candidate = previous or _DUMMY_TOKEN
        active_match = hmac.compare_digest(presented, active_candidate)
        previous_match = hmac.compare_digest(presented, previous_candidate)

        if active and active_match:
            return AgentTokenResult(True, "active")
        if previous and previous_match:
            if deadline > current_time:
                return AgentTokenResult(True, "previous")
            return AgentTokenResult(False, "expired")
        return AgentTokenResult(False, "invalid")


def _bounded_agent_id(value: str) -> str:
    identity = (value or "").strip()
    if not identity or len(identity) > _MAX_AGENT_ID_LENGTH:
        return ""
    try:
        identity.encode("ascii")
    except UnicodeEncodeError:
        return ""
    if any(ord(char) < 0x21 or ord(char) > 0x7E for char in identity):
        return ""
    return identity


def _audit_agent_auth(
    request: Request, *, status: int, outcome: str, reason: str, identity: str
) -> None:
    from observability import audit

    audit.record_agent_auth(
        request_id=request.headers.get("x-request-id", "").strip()[:128]
        or str(uuid.uuid4()),
        identity=identity or "unknown",
        path=request.url.path[:256],
        method=request.method,
        status=status,
        outcome=outcome,
        reason=reason,
    )


class AgentAuthMiddleware(BaseHTTPMiddleware):
    """Authenticate only the exact Agent control route set."""

    def __init__(self, app, *, provider: AgentTokenProvider | None = None):
        super().__init__(app)
        self._provider = provider or AgentTokenProvider()

    async def dispatch(self, request: Request, call_next):
        if not is_agent_route(request.method, request.url.path):
            return await call_next(request)

        identity = _bounded_agent_id(request.headers.get(AGENT_ID_HEADER, ""))
        result = self._provider.verify(request.headers.get(AGENT_TOKEN_HEADER, ""))
        import config

        mode = os.environ.get(
            "AGENT_AUTH_MODE", getattr(config, "AGENT_AUTH_MODE", "compatibility")
        ).strip().lower()
        compatibility_basic = False
        if mode == "compatibility" and not result.accepted:
            from security.operator_auth import manager_basic_credentials_ok

            compatibility_basic = manager_basic_credentials_ok(
                request.headers.get("authorization", "")
            )

        if result.reason == "misconfigured" and mode == "required":
            _audit_agent_auth(
                request,
                status=503,
                outcome="failed",
                reason="misconfigured",
                identity=identity,
            )
            return agent_authentication_unavailable()
        if not identity and not compatibility_basic:
            _audit_agent_auth(
                request,
                status=401,
                outcome="denied",
                reason="invalid_identity",
                identity="unknown",
            )
            return agent_authentication_required()
        if not result.accepted and not compatibility_basic:
            _audit_agent_auth(
                request,
                status=401,
                outcome="denied",
                reason=result.reason,
                identity=identity,
            )
            return agent_authentication_required()

        if identity:
            request.state.agent_id = identity
        request.state.agent_auth = result.reason if result.accepted else "compatibility"
        response = await call_next(request)
        reason = getattr(request.state, "agent_auth_failure_reason", request.state.agent_auth)
        _audit_agent_auth(
            request,
            status=response.status_code,
            outcome="denied" if response.status_code == 401 else "authenticated",
            reason=reason,
            identity=identity or "manager-basic",
        )
        return response
