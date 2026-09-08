"""Auditor-only re-identification route boundary.

Source lookup and clear-text responses are intentionally deferred to Epic #44
stories #47 through #49. This endpoint proves the optional mounting and
authorization contract without accepting a token or reading source data.
"""
from __future__ import annotations

import time
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from observability import audit

router = APIRouter(prefix="/_reidentify/api/v1")


@router.post("/lookup")
async def lookup(request: Request) -> JSONResponse:
    """Record an authorized placeholder request without accepting sensitive input."""
    user = request.state.user
    request_id = str(uuid.uuid4())
    audit.record(
        identity=user.user_id,
        bucket="reidentification",
        key=request_id,
        backend="source_lookup",
        method=request.method,
        status=501,
        action="reidentification_placeholder",
        reason="module enabled; source lookup is not implemented",
    )
    return JSONResponse({
        "ok": False,
        "error": "re-identification lookup is not implemented",
        "request_id": request_id,
        "timestamp": time.time(),
    }, status_code=501)