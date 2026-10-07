"""Shared Python-side SigV4 conformance cases also used by the C++ tests."""
from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest

from fabric_shortcut_proxy.s3.auth import SigV4Error, _canonical_uri, verify_signature
from sigv4_conformance_vectors import (
    ACCESS_KEY,
    INVALID_REQUESTS,
    REGION,
    SECRET_KEY,
    VALID_REQUESTS,
    mutate_request,
)

try:
    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials
except ImportError as exc:
    if os.environ.get("FSP_REQUIRE_CPP_TESTS") == "1":
        raise RuntimeError(
            "FSP_REQUIRE_CPP_TESTS=1 requires botocore for SigV4 conformance tests."
        ) from exc
    pytest.skip("botocore is required for SigV4 conformance tests", allow_module_level=True)


def _sign(vector):
    headers = {"host": "s3.local", **vector["headers"]}
    request = AWSRequest(
        method=vector["method"],
        url=f"http://s3.local{vector['path']}?{vector['query']}"
        if vector["query"]
        else f"http://s3.local{vector['path']}",
        headers=headers,
    )
    S3SigV4Auth(Credentials(ACCESS_KEY, SECRET_KEY), "s3", REGION).add_auth(request)
    return dict(request.headers)


def _request_time(headers):
    date = next(value for key, value in headers.items() if key.lower() == "x-amz-date")
    return datetime.strptime(date, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)


@pytest.mark.parametrize("vector", VALID_REQUESTS, ids=lambda vector: vector["name"])
def test_python_verifier_accepts_shared_vectors(vector):
    headers = _sign(vector)
    verify_signature(
        vector["method"],
        vector["path"],
        vector["query"],
        headers,
        access_key_id=ACCESS_KEY,
        secret_access_key=SECRET_KEY,
        now=_request_time(headers),
    )


@pytest.mark.parametrize("vector", INVALID_REQUESTS, ids=lambda vector: vector["name"])
def test_python_verifier_rejects_shared_mutations(vector):
    base = next(case for case in VALID_REQUESTS if case["name"] == vector["base"])
    headers, path, query = mutate_request(
        vector["mutation"], _sign(base), base["path"], base["query"]
    )
    verification_time = (
        datetime.now(UTC)
        if vector["mutation"] == "stale_date"
        else _request_time(_sign(base))
    )

    with pytest.raises(SigV4Error) as exc:
        verify_signature(
            base["method"],
            path,
            query,
            headers,
            access_key_id=ACCESS_KEY,
            secret_access_key=SECRET_KEY,
            now=verification_time,
        )
    assert exc.value.code == vector["error"]


def test_python_verifier_canonicalizes_invalid_percent_as_literal():
    assert _canonical_uri("/sigv4-test/a%2g") == "/sigv4-test/a%252g"
