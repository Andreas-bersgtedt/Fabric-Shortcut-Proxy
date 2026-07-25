"""
SigV4 authentication tests (Plan item H3).

Uses botocore (the same signer real S3 clients use) to produce valid signatures,
then asserts the proxy's verifier accepts good signatures and rejects tampered /
wrong-key / unsigned requests. Includes an ASGI integration test with
REQUIRE_SIGV4 enabled.
"""
from __future__ import annotations

import os
import pathlib

_DB = pathlib.Path(__file__).parent / "test_auth.db"
os.environ["DB_URL"] = f"sqlite+aiosqlite:///{_DB.as_posix()}"
os.environ["NUM_SPLITS"] = "4"
os.environ["S3_BUCKET"] = "auth-bucket"

import pytest

import config
config.DB_URL = f"sqlite+aiosqlite:///{_DB.as_posix()}"
config.NUM_SPLITS = 4
config.BUCKET_NAME = "auth-bucket"

from s3.auth import verify_signature, SigV4Error

botocore = pytest.importorskip("botocore")
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

_HOST = "s3.local"
_REGION = "us-east-1"


def _sign(method, path, query="", *, key=None, secret=None):
    key = key or config.ACCESS_KEY_ID
    secret = secret or config.SECRET_ACCESS_KEY
    url = f"http://{_HOST}{path}"
    # Pass the query via ``params=`` (not in the URL) so botocore signs the
    # RFC 3986 percent-encoded canonical form -- the same path real S3 clients
    # and Microsoft Fabric's signer use. Signing via the URL would instead use
    # botocore's raw URL path and not exercise the encoding the server must do.
    params = None
    if query:
        from urllib.parse import parse_qsl
        params = dict(parse_qsl(query, keep_blank_values=True))
    req = AWSRequest(method=method, url=url, headers={"host": _HOST}, params=params)
    S3SigV4Auth(Credentials(key, secret), "s3", _REGION).add_auth(req)
    return dict(req.headers)


# ---------------------------------------------------------------------------
# Unit: verifier behavior
# ---------------------------------------------------------------------------

def test_valid_signature_accepted():
    path = "/auth-bucket/warehouse/db/sales/metadata/v1.metadata.json"
    headers = _sign("GET", path)
    # Should not raise.
    verify_signature(
        "GET", path, "", headers,
        access_key_id=config.ACCESS_KEY_ID,
        secret_access_key=config.SECRET_ACCESS_KEY,
    )


def test_valid_signature_with_query_accepted():
    path = "/auth-bucket/"
    query = "list-type=2&prefix=warehouse/db/sales/metadata/"
    headers = _sign("GET", path, query)
    verify_signature(
        "GET", path, query, headers,
        access_key_id=config.ACCESS_KEY_ID,
        secret_access_key=config.SECRET_ACCESS_KEY,
    )


def test_valid_signature_encoded_slash_query_accepted():
    # Mirrors Fabric's list request that previously failed with
    # SignatureDoesNotMatch: the "/" delimiter must canonicalize to "%2F".
    path = "/auth-bucket/"
    query = "list-type=2&max-keys=1000&delimiter=/"
    headers = _sign("GET", path, query)
    verify_signature(
        "GET", path, query, headers,
        access_key_id=config.ACCESS_KEY_ID,
        secret_access_key=config.SECRET_ACCESS_KEY,
    )


def test_tampered_signature_rejected():
    path = "/auth-bucket/warehouse/db/sales/metadata/v1.metadata.json"
    headers = _sign("GET", path)
    headers["Authorization"] = headers["Authorization"][:-4] + "dead"
    with pytest.raises(SigV4Error) as exc:
        verify_signature(
            "GET", path, "", headers,
            access_key_id=config.ACCESS_KEY_ID,
            secret_access_key=config.SECRET_ACCESS_KEY,
        )
    assert exc.value.code == "SignatureDoesNotMatch"


def test_wrong_secret_rejected():
    path = "/auth-bucket/x"
    headers = _sign("GET", path, secret="the-wrong-secret")
    with pytest.raises(SigV4Error) as exc:
        verify_signature(
            "GET", path, "", headers,
            access_key_id=config.ACCESS_KEY_ID,
            secret_access_key=config.SECRET_ACCESS_KEY,
        )
    assert exc.value.code == "SignatureDoesNotMatch"


def test_wrong_access_key_rejected():
    path = "/auth-bucket/x"
    headers = _sign("GET", path, key="AKIAWRONGKEYEXAMPLE")
    with pytest.raises(SigV4Error) as exc:
        verify_signature(
            "GET", path, "", headers,
            access_key_id=config.ACCESS_KEY_ID,
            secret_access_key=config.SECRET_ACCESS_KEY,
        )
    assert exc.value.code == "InvalidAccessKeyId"


def test_missing_authorization_rejected():
    with pytest.raises(SigV4Error) as exc:
        verify_signature(
            "GET", "/auth-bucket/x", "", {"host": _HOST},
            access_key_id=config.ACCESS_KEY_ID,
            secret_access_key=config.SECRET_ACCESS_KEY,
        )
    assert exc.value.code == "AccessDenied"


# ---------------------------------------------------------------------------
# Integration: middleware with REQUIRE_SIGV4 enabled
# ---------------------------------------------------------------------------

@pytest.fixture
async def auth_client(monkeypatch):
    import httpx
    from demo.seed_db import seed_demo_database
    import db.executor as _executor
    from iceberg.state_store import build_snapshot
    from main import app

    monkeypatch.setattr(config, "REQUIRE_SIGV4", True)
    # Other test modules mutate these globals at import time; pin them for this
    # module's requests so the router resolves the bucket/snapshot correctly.
    monkeypatch.setattr(config, "BUCKET_NAME", "auth-bucket")
    monkeypatch.setattr(config, "NUM_SPLITS", 4)

    _executor._engine = None
    await seed_demo_database()
    build_snapshot(
        table_name=config.TABLE_NAME,
        num_splits=config.NUM_SPLITS,
        bucket=config.BUCKET_NAME,
        warehouse_prefix=config.WAREHOUSE_PREFIX,
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=f"http://{_HOST}"
    ) as c:
        yield c

    if _executor._engine is not None:
        await _executor._engine.dispose()
        _executor._engine = None


async def test_middleware_blocks_unsigned(auth_client):
    r = await auth_client.get("/auth-bucket/warehouse/db/sales/metadata/v1.metadata.json")
    assert r.status_code == 403
    assert "AccessDenied" in r.text


async def test_middleware_exempts_health(auth_client):
    assert (await auth_client.get("/healthz")).status_code == 200
    assert (await auth_client.get("/readyz")).status_code == 200


async def test_middleware_accepts_signed(auth_client):
    path = "/auth-bucket/warehouse/db/sales/metadata/v1.metadata.json"
    headers = _sign("GET", path)
    r = await auth_client.get(path, headers=headers)
    assert r.status_code == 200
    assert r.json()["format-version"] == 2
