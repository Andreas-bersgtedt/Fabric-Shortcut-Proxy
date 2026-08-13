"""Fabric REST API helpers for the Open Mirror config UI.

Lists Fabric workspaces and their mirrored databases using the proxy's OWN Entra
identity — the same service principal / managed identity / default credential
already configured for Key Vault and OneLake (issue #16) via
:func:`security.azure_credential.proxy_credential`. Lets the config builder offer
workspace/mirrored-database pickers so operators never paste a OneLake URL.

``azure-identity`` (the ``onelake``/``azureblob`` extra) and ``httpx`` are imported
lazily so the core install needs neither.
"""
from __future__ import annotations

import config
from security.azure_credential import proxy_credential

_FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
_ONELAKE_HOST = "https://onelake.dfs.fabric.microsoft.com"


class FabricApiError(RuntimeError):
    """A Fabric REST API call failed (auth, permission, or transport)."""


def landing_zone_url(workspace_id: str, mirrored_db_id: str) -> str:
    """Compose the OneLake landing-zone root for a mirrored database."""
    return f"{_ONELAKE_HOST}/{workspace_id.strip()}/{mirrored_db_id.strip()}/Files/LandingZone"


def _token(credential=None) -> str:
    cred = credential or proxy_credential(config)
    return cred.get_token(_FABRIC_SCOPE).token


def _get_all(path: str, token: str, params: dict | None = None) -> list[dict]:
    """GET a Fabric collection, following continuation tokens; return ``value`` items."""
    import httpx

    url = f"{_FABRIC_BASE}/{path.lstrip('/')}"
    headers = {"Authorization": f"Bearer {token}"}
    out: list[dict] = []
    with httpx.Client(timeout=30.0) as client:
        while True:
            resp = client.get(url, headers=headers, params=params)
            if resp.status_code == 401:
                raise FabricApiError(
                    "Fabric API returned 401 Unauthorized. The proxy's Entra identity needs "
                    "access to Fabric — add the service principal to the workspace (Viewer+) "
                    "and enable 'Service principals can use Fabric APIs' in the tenant settings."
                )
            if resp.status_code == 403:
                raise FabricApiError(
                    "Fabric API returned 403 Forbidden. The proxy identity lacks permission "
                    "for this resource; grant it access to the workspace."
                )
            if resp.status_code >= 400:
                raise FabricApiError(f"Fabric API {resp.status_code}: {resp.text[:200]}")
            data = resp.json()
            out.extend(data.get("value", []) or [])
            cont_uri = data.get("continuationUri")
            cont_tok = data.get("continuationToken")
            if cont_uri:
                url, params = cont_uri, None
            elif cont_tok:
                params = dict(params or {})
                params["continuationToken"] = cont_tok
            else:
                break
    return out


def list_workspaces(credential=None) -> list[dict]:
    """Return ``[{id, name}]`` for every Fabric workspace the identity can see."""
    items = _get_all("workspaces", _token(credential))
    return [
        {"id": w.get("id"), "name": w.get("displayName") or w.get("name") or w.get("id")}
        for w in items if w.get("id")
    ]


def list_mirrored_databases(workspace_id: str, credential=None) -> list[dict]:
    """Return ``[{id, name, workspace_id, landing_zone_root}]`` for a workspace."""
    token = _token(credential)
    try:
        items = _get_all(f"workspaces/{workspace_id}/mirroredDatabases", token)
    except FabricApiError:
        # Fall back to the generic items endpoint filtered by type.
        items = _get_all(f"workspaces/{workspace_id}/items", token, params={"type": "MirroredDatabase"})
    return [
        {
            "id": i.get("id"),
            "name": i.get("displayName") or i.get("name") or i.get("id"),
            "workspace_id": workspace_id,
            "landing_zone_root": landing_zone_url(workspace_id, i["id"]),
        }
        for i in items if i.get("id")
    ]
