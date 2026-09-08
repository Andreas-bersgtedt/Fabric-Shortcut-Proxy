"""Auditor-only re-identification route boundary.

Source lookup and clear-text responses are intentionally deferred to Epic #44
stories #47 through #49. This endpoint proves the optional mounting and
authorization contract without accepting a token or reading source data.
"""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from observability import audit
from reidentification.mappings import load_default_mappings

router = APIRouter(prefix="/_reidentify/api/v1")

# Mounting an enabled module validates the administrative mapping contract before
# any Auditor can reach its route.
load_default_mappings().validate()


@router.post("/lookup")
async def lookup(request: Request) -> JSONResponse:
    """Record an authorized placeholder request without accepting sensitive input."""
    user = request.state.user
    request_id = str(uuid.uuid4())
    try:
        audit.record_reidentification(
        request_id=request_id,
        identity=user.user_id,
        method=request.method,
        status=501,
        outcome="not_implemented",
        reason="module enabled; source lookup is not implemented",
        )
    except audit.AuditUnavailable:
        return JSONResponse({"ok": False, "error": "audit service unavailable"}, status_code=503)
    return JSONResponse({
        "ok": False,
        "error": "re-identification lookup is not implemented",
        "request_id": request_id,
    }, status_code=501)