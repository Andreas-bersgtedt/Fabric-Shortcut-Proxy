"""Problem Details and correlation IDs for HTTP operator APIs."""
from __future__ import annotations

import json
import re
import uuid
from http import HTTPStatus
from typing import Any

from fastapi import FastAPI
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from fabric_shortcut_proxy.observability.logging import get_logger
from fabric_shortcut_proxy.security.credentials import (
    _is_sensitive_key,
    scrub_secrets,
)

log = get_logger(__name__)

STANDALONE_OPERATOR_API_PREFIXES = (
    "/_admin",
    "/_config/api",
    "/_monitor/api",
)
OPERATOR_API_PREFIXES = (
    *STANDALONE_OPERATOR_API_PREFIXES,
    "/_manager/api",
    "/agents",
    "/control",
)
CATALOG_API_PREFIXES = ("/catalog",)
_CORRELATION_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s\"']+")
_MAX_ERROR_BODY = 64 * 1024
_CODES = {
    400: "bad_request",
    401: "authentication_required",
    403: "permission_denied",
    404: "not_found",
    405: "method_not_allowed",
    406: "not_acceptable",
    409: "conflict",
    422: "validation_error",
    428: "precondition_required",
    429: "rate_limited",
    500: "internal_error",
    503: "service_unavailable",
    504: "gateway_timeout",
}
_SPECIFIC_CODES = {
    "configuration_not_representable": 422,
    "configuration_persist_failed": 500,
    "residency_policy_violation": 422,
    "stale_revision": 409,
}
_DOCUMENTED_STATUSES = (400, 401, 403, 404, 409, 422, 429, 500, 503)


def correlation_id(value: str) -> str:
    """Keep a bounded, log-safe caller ID or create a new one."""
    return value if _CORRELATION_ID.fullmatch(value) else str(uuid.uuid4())


def _operator_api(method: str, path: str, prefixes: tuple[str, ...]) -> bool:
    if not any(path == prefix or path.startswith(prefix + "/") for prefix in prefixes):
        return False
    if path == "/control" or path.startswith("/control/"):
        from fabric_shortcut_proxy.security.agent_auth import is_agent_route

        return not is_agent_route(method, path)
    return True


def _safe_legacy_value(value: Any) -> Any:
    if isinstance(value, str):
        return scrub_secrets(_URL.sub("[REDACTED URL]", value))
    if isinstance(value, list):
        return [_safe_legacy_value(item) for item in value]
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if _is_sensitive_key(key) else _safe_legacy_value(item)
            for key, item in value.items()
        }
    return value


def _problem(status: int, request_id: str, body: bytes) -> dict[str, Any]:
    try:
        original = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        original = {}
    if not isinstance(original, dict):
        original = {}

    specific = original.get("code")
    code = specific if isinstance(specific, str) and _SPECIFIC_CODES.get(specific) == status else (
        _CODES.get(status, f"http_{status}")
    )
    try:
        title = HTTPStatus(status).phrase
    except ValueError:
        title = "HTTP error"
    if code == "stale_revision":
        title = "Configuration revision is stale"

    if status >= 500:
        problem: dict[str, Any] = {
            "ok": False,
            "error": (
                "Could not persist typed configuration"
                if code == "configuration_persist_failed" else title
            ),
        }
    else:
        problem = _safe_legacy_value(original)
        if status == 422 and isinstance(problem.get("detail"), list):
            problem["detail"] = "Request validation failed"
        problem.setdefault("ok", False)
        problem.setdefault("error", problem.get("detail") or title)

    problem.update({
        "type": "about:blank",
        "title": title,
        "status": status,
        "code": code,
        "correlationId": request_id,
    })
    return problem


class OperatorProblemMiddleware:
    """Normalize operator errors without changing the Agent or S3 protocols."""

    def __init__(self, app: ASGIApp, *, prefixes: tuple[str, ...]):
        self.app = app
        self.prefixes = prefixes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not _operator_api(
            scope["method"], scope["path"], self.prefixes,
        ):
            await self.app(scope, receive, send)
            return

        request_id = correlation_id(Headers(scope=scope).get("x-correlation-id", ""))
        scope.setdefault("state", {})["correlation_id"] = request_id
        start: Message | None = None
        buffered = bytearray()
        sent = False
        replaced = False

        async def send_problem(status: int, headers: list[tuple[bytes, bytes]], body: bytes,
                               *, exception_type: str = "") -> None:
            nonlocal sent, replaced
            problem = _problem(status, request_id, body)
            payload = json.dumps(problem, ensure_ascii=True, separators=(",", ":")).encode()
            preserved = [
                (key, value) for key, value in headers
                if key.lower() not in {
                    b"content-type", b"content-length", b"content-encoding",
                    b"transfer-encoding", b"x-correlation-id", b"cache-control",
                }
            ]
            preserved.extend([
                (b"content-type", b"application/problem+json"),
                (b"content-length", str(len(payload)).encode("ascii")),
                (b"x-correlation-id", request_id.encode("ascii")),
                (b"cache-control", b"no-store"),
            ])
            fields = {
                "status": status,
                "code": problem["code"],
                "correlation_id": request_id,
            }
            if exception_type:
                fields["exception_type"] = exception_type
            if status >= 500:
                log.error("operator_api_error", **fields)
            else:
                log.warning("operator_api_error", **fields)
            sent = True
            replaced = True
            await send({"type": "http.response.start", "status": status, "headers": preserved})
            await send({"type": "http.response.body", "body": payload})

        async def send_wrapped(message: Message) -> None:
            nonlocal start, sent
            if replaced:
                return
            if message["type"] == "http.response.start":
                if message["status"] >= 400:
                    start = message
                    return
                sent = True
            elif message["type"] == "http.response.body" and start is not None:
                chunk = message.get("body", b"")
                remaining = _MAX_ERROR_BODY - len(buffered)
                buffered.extend(chunk[:remaining + 1])
                if len(buffered) > _MAX_ERROR_BODY or not message.get("more_body", False):
                    body = bytes(buffered) if len(buffered) <= _MAX_ERROR_BODY else b""
                    await send_problem(start["status"], start.get("headers", []), body)
                return
            await send(message)

        try:
            await self.app(scope, receive, send_wrapped)
        except Exception as exc:
            if sent:
                raise
            await send_problem(500, [], b"", exception_type=type(exc).__name__)


def install_operator_errors(app: FastAPI, *, prefixes: tuple[str, ...]) -> None:
    """Install the runtime boundary and publish the same error schema in OpenAPI."""
    app.add_middleware(OperatorProblemMiddleware, prefixes=prefixes)
    original_openapi = app.openapi
    augmented_schema: dict[str, Any] | None = None

    def openapi() -> dict[str, Any]:
        nonlocal augmented_schema
        schema = original_openapi()
        if schema is augmented_schema:
            return schema
        components = schema.setdefault("components", {}).setdefault("schemas", {})
        components["OperatorProblem"] = {
            "type": "object",
            "required": ["type", "title", "status", "code", "correlationId"],
            "properties": {
                "type": {"type": "string"},
                "title": {"type": "string"},
                "status": {"type": "integer"},
                "code": {"type": "string"},
                "correlationId": {"type": "string"},
            },
        }
        for path, path_item in schema.get("paths", {}).items():
            for method, operation in path_item.items():
                if method not in {"get", "post", "put", "patch", "delete", "head", "options"}:
                    continue
                if not _operator_api(method.upper(), path, prefixes):
                    continue
                parameters = operation.setdefault("parameters", [])
                if not any(p.get("name", "").lower() == "x-correlation-id" for p in parameters):
                    parameters.append({
                        "name": "X-Correlation-ID",
                        "in": "header",
                        "required": False,
                        "description": "Optional request correlation ID; invalid values are replaced.",
                        "schema": {"type": "string", "maxLength": 128},
                    })
                responses = operation.setdefault("responses", {})
                statuses = {str(status) for status in _DOCUMENTED_STATUSES}
                statuses.update(
                    status for status in responses
                    if status.isdigit() and int(status) >= 400
                )
                for status in statuses:
                    existing = responses.get(status, {})
                    headers = dict(existing.get("headers", {}))
                    headers["X-Correlation-ID"] = {"schema": {"type": "string"}}
                    content = existing.get("content", {}).get("application/problem+json", {})
                    specific_schema = content.get("schema")
                    shared_schema: dict[str, Any] = {
                        "$ref": "#/components/schemas/OperatorProblem",
                    }
                    if specific_schema:
                        shared_schema = {"allOf": [shared_schema, specific_schema]}
                    responses[status] = {
                        "description": existing.get("description", "Operator API error"),
                        "headers": headers,
                        "content": {
                            "application/problem+json": {"schema": shared_schema},
                        },
                    }
        augmented_schema = schema
        return schema

    app.openapi = openapi
