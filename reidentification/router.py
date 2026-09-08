"""Auditor-only bounded re-identification route."""
from __future__ import annotations

import hashlib
import re
import time
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from observability import audit
import config
from reidentification.limits import RequestLimiter
from reidentification.mappings import ReidentificationMappingError, load_default_mappings
from reidentification.source_lookup import lookup_rows

router = APIRouter(prefix="/_reidentify/api/v1")

# Mounting an enabled module validates the administrative mapping contract before
# any Auditor can reach its route.
_mappings = load_default_mappings()
_mappings.validate()
_limiter = RequestLimiter()
_REASON_CODES = frozenset({"audit", "investigation", "legal_hold"})
_CASE_REFERENCE = re.compile(r"^[ -~]{1,128}$")


def _token_fingerprint(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


async def _audit_response(
    *, request_id: str, identity: str, method: str, status: int, outcome: str,
    reason: str, policy_id: str, table_id: str, column_id: str,
    token: str = "", case_reference: str = "", match_count: int | None = None, started: float,
    payload: dict | None = None,
) -> JSONResponse:
    try:
        audit.record_reidentification(
            request_id=request_id, identity=identity, method=method, status=status,
            outcome=outcome, reason=reason, policy_id=policy_id, table_id=table_id,
            column_id=column_id, token_fingerprint=_token_fingerprint(token) if token else "",
            case_reference=case_reference,
            match_count=match_count, latency_ms=round((time.perf_counter() - started) * 1000),
        )
    except audit.AuditUnavailable:
        return JSONResponse({"ok": False, "error": "audit service unavailable"}, status_code=503)
    return JSONResponse(payload or {"ok": False, "error": "not_found_or_ambiguous", "request_id": request_id}, status_code=status)


@router.post("/lookup/{policy_id}/{table_id}/{column_id}")
async def lookup(policy_id: str, table_id: str, column_id: str, request: Request) -> JSONResponse:
    """Return one approved source value only after durable redacted auditing."""
    user = request.state.user
    request_id = str(uuid.uuid4())
    started = time.perf_counter()
    token = ""
    case_reference = ""
    decision = _limiter.allow(
        user.user_id,
        per_minute=config.REIDENTIFICATION_REQUESTS_PER_MINUTE,
        per_day=config.REIDENTIFICATION_REQUESTS_PER_DAY,
    )
    if not decision.allowed:
        return await _audit_response(
            request_id=request_id, identity=user.user_id, method=request.method,
            status=429, outcome="rate_limited", reason=decision.reason,
            policy_id=policy_id, table_id=table_id, column_id=column_id, started=started,
            payload={"ok": False, "error": "request limit exceeded", "request_id": request_id},
        )
    try:
        body = await request.json()
        if not isinstance(body, dict) or set(body) != {"token", "reason_code", "case_reference"}:
            raise ReidentificationMappingError("invalid re-identification request")
        token = str(body["token"])
        reason_code = str(body["reason_code"])
        case_reference = str(body["case_reference"])
        if reason_code not in _REASON_CODES or not _CASE_REFERENCE.fullmatch(case_reference):
            raise ReidentificationMappingError("invalid re-identification request")
        mapping = _mappings.get(policy_id, table_id, column_id)
        rows = await lookup_rows(mapping, token)
    except (ReidentificationMappingError, ValueError, TypeError):
        return await _audit_response(
            request_id=request_id, identity=user.user_id, method=request.method,
            status=400, outcome="invalid_request", reason="invalid request or mapping",
            policy_id=policy_id, table_id=table_id, column_id=column_id, token=token,
            case_reference=case_reference, started=started,
        )
    except Exception:  # source failures are deliberately indistinguishable to callers
        return await _audit_response(
            request_id=request_id, identity=user.user_id, method=request.method,
            status=503, outcome="source_failure", reason="source lookup unavailable",
            policy_id=policy_id, table_id=table_id, column_id=column_id, token=token,
            case_reference=case_reference, started=started,
        )
    if len(rows) != 1:
        return await _audit_response(
            request_id=request_id, identity=user.user_id, method=request.method,
            status=404, outcome="not_found_or_ambiguous", reason="lookup did not produce one result",
            policy_id=policy_id, table_id=table_id, column_id=column_id, token=token,
            case_reference=case_reference, match_count=len(rows), started=started,
        )
    result = rows[0]
    return await _audit_response(
        request_id=request_id, identity=user.user_id, method=request.method,
        status=200, outcome="success", reason=reason_code, policy_id=policy_id,
        table_id=table_id, column_id=column_id, token=token, match_count=1, started=started,
        case_reference=case_reference,
        payload={
            "ok": True,
            "request_id": request_id,
            "primary_key": result["__reidentify_primary_key"],
            "value": result["__reidentify_value"],
        },
    )