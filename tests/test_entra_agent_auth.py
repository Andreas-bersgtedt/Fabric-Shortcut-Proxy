from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import quote

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi import FastAPI, Request

from fabric_shortcut_proxy.security import identity
from fabric_shortcut_proxy.security.agent_auth import AgentAuthMiddleware, AgentMtlsProvider
from fabric_shortcut_proxy.security.entra_agent_auth import EntraAgentTokenProvider

jwt = pytest.importorskip("jwt")

TENANT = "11111111-1111-4111-8111-111111111111"
AUDIENCE = "22222222-2222-4222-8222-222222222222"
CLIENT = "33333333-3333-4333-8333-333333333333"
PRINCIPAL = "44444444-4444-4444-8444-444444444444"
AGENT = "site-a-worker-0:9000"


@pytest.fixture
def signed_token(monkeypatch):
    monkeypatch.setenv("AGENT_AUTH_MODE", "entra")
    monkeypatch.setenv("AGENT_ENTRA_TENANT_ID", TENANT)
    monkeypatch.setenv("AGENT_ENTRA_AUDIENCE", AUDIENCE)
    monkeypatch.setenv(
        "AGENT_ENTRA_IDENTITIES",
        json.dumps({AGENT: {"client_id": CLIENT, "principal_id": PRINCIPAL}}),
    )
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    monkeypatch.setattr(
        identity,
        "_oidc_jwk_client",
        lambda url: SimpleNamespace(
            get_signing_key_from_jwt=lambda token: SimpleNamespace(key=key.public_key())
        ),
    )
    claims = {
        "iss": f"https://login.microsoftonline.com/{TENANT}/v2.0",
        "aud": AUDIENCE,
        "sub": PRINCIPAL,
        "tid": TENANT,
        "oid": PRINCIPAL,
        "azp": CLIENT,
        "roles": ["FSP.Agent"],
        "nbf": int(time.time()) - 60,
        "iat": int(time.time()) - 60,
        "exp": int(time.time()) + 3600,
    }

    def sign(**overrides):
        return jwt.encode({**claims, **overrides}, key, algorithm="RS256", headers={"kid": "test"})

    return sign


async def test_entra_agent_accepts_only_bound_workload(signed_token):
    provider = EntraAgentTokenProvider()
    result = await provider.verify(signed_token(), identity=AGENT)
    assert result.accepted and result.reason == "entra"
    assert not (await provider.verify(signed_token(), identity="another-pool-worker")).accepted


@pytest.mark.parametrize(
    ("claim", "value"),
    [
        ("iss", "https://attacker.invalid"),
        ("aud", CLIENT),
        ("tid", CLIENT),
        ("azp", AUDIENCE),
        ("oid", AUDIENCE),
        ("roles", ["Reader"]),
        ("roles", "FSP.Agent"),
        ("exp", 1),
        ("nbf", 4102444800),
    ],
)
async def test_entra_agent_rejects_invalid_claims(signed_token, claim, value):
    assert not (
        await EntraAgentTokenProvider().verify(signed_token(**{claim: value}), identity=AGENT)
    ).accepted


async def test_entra_agent_rejects_bad_signature_and_oversize(signed_token):
    token = signed_token()
    parts = token.split(".")
    parts[2] = "A" * len(parts[2])
    provider = EntraAgentTokenProvider()
    assert not (await provider.verify(".".join(parts), identity=AGENT)).accepted
    assert not (await provider.verify("A" * 16385, identity=AGENT)).accepted


async def test_entra_agent_jwks_unavailable_fails_closed(signed_token, monkeypatch):
    provider = EntraAgentTokenProvider()

    def unavailable(token):
        raise jwt.PyJWKClientConnectionError("network unavailable")

    monkeypatch.setattr(provider, "_decode", unavailable)
    result = await provider.verify(signed_token(), identity=AGENT)
    assert not result.accepted and result.reason == "unavailable"


async def test_entra_middleware_rejects_static_and_basic_credentials(signed_token, monkeypatch):
    monkeypatch.setenv("AGENT_TOKEN", "x" * 48)
    app = FastAPI()
    app.add_middleware(AgentAuthMiddleware)

    @app.post("/control/materialize")
    async def materialize(request: Request):
        return {"identity": request.state.agent_id, "auth": request.state.agent_auth}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        headers = {"X-FSP-Agent-ID": AGENT, "Authorization": f"Bearer {signed_token()}"}
        response = await client.post("/control/materialize", headers=headers)
        assert response.status_code == 200
        assert response.json() == {"identity": AGENT, "auth": "entra"}
        response = await client.post(
            "/control/materialize",
            headers={"X-FSP-Agent-ID": AGENT, "X-FSP-Agent-Token": "x" * 48},
            auth=("admin", "password"),
        )
        assert response.status_code == 401


def _mtls_certificate(tmp_path, monkeypatch):
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "FSP test CA")])
    now = datetime.now(timezone.utc)
    ca = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    client_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client_name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "site-a-worker")]
    )
    client = (
        x509.CertificateBuilder()
        .subject_name(client_name)
        .issuer_name(ca.subject)
        .public_key(client_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .sign(ca_key, hashes.SHA256())
    )
    ca_file = tmp_path / "ca.crt"
    ca_file.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    fingerprint = client.fingerprint(hashes.SHA256()).hex()
    monkeypatch.setenv("AGENT_MTLS_CA_FILE", str(ca_file))
    monkeypatch.setenv(
        "AGENT_MTLS_IDENTITIES", json.dumps({fingerprint: AGENT})
    )
    return quote(
        client.public_bytes(serialization.Encoding.PEM).decode("ascii"),
        safe="",
    )


def test_mtls_provider_verifies_ca_fingerprint_and_agent_binding(
    tmp_path, monkeypatch
):
    certificate = _mtls_certificate(tmp_path, monkeypatch)
    provider = AgentMtlsProvider()
    accepted = provider.verify(certificate, "SUCCESS", identity=AGENT)
    assert accepted.accepted and accepted.reason == "mtls"
    assert not provider.verify(certificate, "SUCCESS", identity="other-agent").accepted
    assert not provider.verify(certificate, "FAILED", identity=AGENT).accepted


async def test_entra_middleware_uses_verified_mtls_fallback(
    signed_token, tmp_path, monkeypatch
):
    certificate = _mtls_certificate(tmp_path, monkeypatch)
    app = FastAPI()
    app.add_middleware(AgentAuthMiddleware)

    @app.post("/control/materialize")
    async def materialize(request: Request):
        return {"identity": request.state.agent_id, "auth": request.state.agent_auth}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/control/materialize",
            headers={
                "X-FSP-Agent-ID": AGENT,
                "ssl-client-verify": "SUCCESS",
                "ssl-client-cert": certificate,
            },
        )
        assert response.status_code == 200
        assert response.json() == {"identity": AGENT, "auth": "mtls"}


async def test_entra_control_client_uses_refreshed_tokens(monkeypatch):
    from enterprise.control.transport import RestControlClient
    from fabric_shortcut_proxy.security import entra_agent_auth

    class Credential:
        def __init__(self):
            self.version = 0
            self.closed = False

        async def token(self):
            self.version += 1
            return f"access-token-{self.version}"

        async def close(self):
            self.closed = True

    monkeypatch.setenv("AGENT_AUTH_MODE", "entra")
    credential = Credential()
    monkeypatch.setattr(entra_agent_auth, "AgentEntraCredential", lambda: credential)
    client = RestControlClient("https://manager.example", agent_id=AGENT)
    assert (await client._auth_headers())["Authorization"] == "Bearer access-token-1"
    assert (await client._auth_headers())["Authorization"] == "Bearer access-token-2"
    assert "X-FSP-Agent-Token" not in await client._auth_headers()
    await client.aclose()
    assert credential.closed


async def test_azure_sdk_renews_expired_token_and_reads_rotated_federation(monkeypatch, tmp_path):
    pytest.importorskip("azure.identity")
    from azure.core.credentials import AccessTokenInfo

    from fabric_shortcut_proxy.security.entra_agent_auth import AgentEntraCredential

    clock = [int(time.time())]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    token_file = tmp_path / "federation-token"
    token_file.write_text("first-federation", encoding="utf-8")
    monkeypatch.setenv("AZURE_TENANT_ID", TENANT)
    monkeypatch.setenv("AZURE_CLIENT_ID", CLIENT)
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("AGENT_ENTRA_SCOPE", f"api://{AUDIENCE}/.default")
    credential = AgentEntraCredential()
    assertions = []
    cached = [None]

    def obtain_token(scopes, assertion, **kwargs):
        assert scopes == (f"api://{AUDIENCE}/.default",)
        assertions.append(assertion)
        cached[0] = AccessTokenInfo(f"access-token-{len(assertions)}", clock[0] + 60)
        return cached[0]

    monkeypatch.setattr(
        credential.credential._client,
        "get_cached_access_token",
        lambda scopes, **kwargs: cached[0],
    )
    monkeypatch.setattr(
        credential.credential._client, "obtain_token_by_jwt_assertion", obtain_token
    )
    try:
        assert await credential.token() == "access-token-1"
        token_file.write_text("rotated-federation", encoding="utf-8")
        clock[0] += 601
        assert await credential.token() == "access-token-2"
        assert assertions == ["first-federation", "rotated-federation"]
    finally:
        await credential.close()


@pytest.mark.parametrize(
    "binding",
    [
        {},
        {AGENT: {"client_id": None, "principal_id": PRINCIPAL}},
        {AGENT: {"client_id": CLIENT, "principal_id": []}},
        {AGENT: {"client_id": "invalid", "principal_id": PRINCIPAL}},
        {AGENT: {"client_id": CLIENT}},
    ],
)
def test_entra_rejects_malformed_configuration(signed_token, monkeypatch, binding):
    monkeypatch.setenv("AGENT_ENTRA_IDENTITIES", json.dumps(binding))
    with pytest.raises(ValueError):
        EntraAgentTokenProvider()
