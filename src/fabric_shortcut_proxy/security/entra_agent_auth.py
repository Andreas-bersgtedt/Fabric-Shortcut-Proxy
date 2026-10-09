"""Short-lived, identity-bound Entra credentials for the Agent control plane."""
from __future__ import annotations

import asyncio
import json
import os
import uuid

from fabric_shortcut_proxy.security.agent_auth import AgentTokenResult


def entra_identities() -> dict[str, dict[str, str]]:
    raw = json.loads(os.environ.get("AGENT_ENTRA_IDENTITIES", "{}"))
    if not isinstance(raw, dict) or not raw:
        raise ValueError("AGENT_ENTRA_IDENTITIES must contain Agent identity bindings")
    result = {}
    for agent_id, binding in raw.items():
        if not isinstance(agent_id, str) or not agent_id or not isinstance(binding, dict):
            raise ValueError("Invalid Entra Agent identity binding")
        if set(binding) != {"client_id", "principal_id"}:
            raise ValueError("Entra identity bindings require client_id and principal_id")
        if any(not isinstance(value, str) for value in binding.values()):
            raise ValueError("Entra identity bindings must contain UUID strings")
        result[agent_id] = {
            key: str(uuid.UUID(value)) for key, value in binding.items()
        }
    return result


class EntraAgentTokenProvider:
    def __init__(self) -> None:
        try:
            from jwt.algorithms import get_default_algorithms
        except ImportError as exc:
            raise ValueError("Entra Agent authentication requires the agent-entra extra") from exc
        if "RS256" not in get_default_algorithms():
            raise ValueError("Entra Agent authentication requires the agent-entra crypto extra")
        self.tenant_id = str(uuid.UUID(os.environ.get("AGENT_ENTRA_TENANT_ID", "")))
        self.audience = str(uuid.UUID(os.environ.get("AGENT_ENTRA_AUDIENCE", "")))
        self.identities = entra_identities()
        self.issuer = f"https://login.microsoftonline.com/{self.tenant_id}/v2.0"
        self.jwks_url = f"https://login.microsoftonline.com/{self.tenant_id}/discovery/v2.0/keys"

    def _decode(self, token: str) -> dict:
        try:
            import jwt
        except ImportError as exc:
            raise RuntimeError("Entra Agent authentication requires the agent-entra extra") from exc
        from fabric_shortcut_proxy.security.identity import _oidc_jwk_client

        key = _oidc_jwk_client(self.jwks_url).get_signing_key_from_jwt(token)
        return jwt.decode(
            token,
            key.key,
            algorithms=["RS256"],
            issuer=self.issuer,
            audience=self.audience,
            options={
                "strict_aud": True,
                "require": ["exp", "iat", "nbf", "iss", "aud", "sub", "tid", "oid", "azp", "roles"],
            },
        )

    async def verify(self, token: str, *, identity: str) -> AgentTokenResult:
        if identity not in self.identities:
            return AgentTokenResult(False, "invalid_identity")
        if not token:
            return AgentTokenResult(False, "missing")
        if len(token) > 16384:
            return AgentTokenResult(False, "malformed")
        try:
            import jwt
        except ImportError:
            return AgentTokenResult(False, "misconfigured")
        try:
            claims = await asyncio.to_thread(self._decode, token)
        except jwt.PyJWKClientConnectionError:
            return AgentTokenResult(False, "unavailable")
        except (jwt.PyJWTError, TypeError, ValueError):
            return AgentTokenResult(False, "invalid")
        binding = self.identities[identity]
        if (
            claims.get("tid") != self.tenant_id
            or claims.get("azp") != binding["client_id"]
            or claims.get("oid") != binding["principal_id"]
            or not isinstance(claims.get("roles"), list)
            or "FSP.Agent" not in claims["roles"]
        ):
            return AgentTokenResult(False, "identity_mismatch")
        return AgentTokenResult(True, "entra")


class AgentEntraCredential:
    def __init__(self) -> None:
        from fabric_shortcut_proxy.security.azure_credential import get_credential

        self.scope = os.environ.get("AGENT_ENTRA_SCOPE", "").strip()
        if not self.scope.startswith("api://") or not self.scope.endswith("/.default"):
            raise ValueError("AGENT_ENTRA_SCOPE must be api://<application-client-id>/.default")
        uuid.UUID(self.scope[len("api://"):-len("/.default")])
        client_id = str(uuid.UUID(os.environ.get("AZURE_CLIENT_ID", "")))
        if os.environ.get("AZURE_FEDERATED_TOKEN_FILE"):
            self.credential = get_credential(
                "workload_identity",
                tenant_id=os.environ.get("AZURE_TENANT_ID", ""),
                client_id=client_id,
                token_file=os.environ["AZURE_FEDERATED_TOKEN_FILE"],
            )
        else:
            self.credential = get_credential("managed_identity", client_id=client_id)

    async def token(self) -> str:
        access_token = await asyncio.to_thread(self.credential.get_token, self.scope)
        return access_token.token

    async def close(self) -> None:
        await asyncio.to_thread(self.credential.close)
