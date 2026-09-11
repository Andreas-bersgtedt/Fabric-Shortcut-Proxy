"""Microsoft Graph directory lookup for the operator role picker."""
from __future__ import annotations

import json
import os
from urllib.parse import quote
from urllib.request import Request, urlopen


class EntraDirectoryError(RuntimeError):
    """Raised when the configured Graph directory cannot be queried."""


def _escape_filter(value: str) -> str:
    return value.replace("'", "''")[:80]


class EntraDirectoryClient:
    def __init__(self) -> None:
        import config

        self.tenant_id = str(config.ENTRA_TENANT_ID or "").strip()
        self.client_id = str(
            os.environ.get("FSP_ENTRA_GRAPH_CLIENT_ID", config.ENTRA_GRAPH_CLIENT_ID)
        ).strip()
        self.client_secret = os.environ.get("FSP_ENTRA_GRAPH_CLIENT_SECRET", "").strip()
        self.authority = f"https://login.microsoftonline.com/{self.tenant_id}"

    def _access_token(self) -> str:
        if not self.tenant_id or not self.client_id or not self.client_secret:
            raise EntraDirectoryError("Entra Graph app credentials are not configured")
        try:
            import msal
        except ImportError as exc:
            raise EntraDirectoryError("install the entra extra to enable Graph lookup") from exc
        app = msal.ConfidentialClientApplication(
            self.client_id, authority=self.authority, client_credential=self.client_secret,
        )
        result = app.acquire_token_for_client(["https://graph.microsoft.com/.default"])
        token = result.get("access_token") if isinstance(result, dict) else None
        if not token:
            raise EntraDirectoryError("Microsoft Graph token acquisition failed")
        return token

    def _get(self, path: str) -> list[dict]:
        request = Request(
            "https://graph.microsoft.com/v1.0/" + path,
            headers={
                "Authorization": f"Bearer {self._access_token()}",
                "Accept": "application/json",
                "ConsistencyLevel": "eventual",
            },
        )
        try:
            with urlopen(request, timeout=10) as response:
                payload = json.load(response)
        except Exception as exc:  # noqa: BLE001 - normalize Graph/network errors
            raise EntraDirectoryError("Microsoft Graph directory lookup failed") from exc
        values = payload.get("value") if isinstance(payload, dict) else None
        return [item for item in values if isinstance(item, dict)] if isinstance(values, list) else []

    def search_users(self, query: str) -> list[dict]:
        value = _escape_filter(query.strip())
        if not value:
            return []
        select = quote("id,displayName,userPrincipalName,mail", safe="")
        filter_value = quote(
            f"startswith(displayName,'{value}') or startswith(userPrincipalName,'{value}')",
            safe="",
        )
        return [
            {
                "id": item.get("id", ""),
                "tenant_id": self.tenant_id,
                "kind": "user",
                "display_name": item.get("displayName", ""),
                "user_principal_name": item.get("userPrincipalName", ""),
                "mail": item.get("mail", ""),
            }
            for item in self._get(f"users?$select={select}&$filter={filter_value}&$top=25")
            if item.get("id") and item.get("displayName")
        ]

    def search_security_groups(self, query: str) -> list[dict]:
        value = _escape_filter(query.strip())
        if not value:
            return []
        select = quote("id,displayName,description,securityEnabled,mailEnabled", safe="")
        filter_value = quote(
            f"securityEnabled eq true and startswith(displayName,'{value}')", safe="",
        )
        return [
            {
                "id": item.get("id", ""),
                "tenant_id": self.tenant_id,
                "kind": "group",
                "display_name": item.get("displayName", ""),
                "description": item.get("description", ""),
            }
            for item in self._get(f"groups?$select={select}&$filter={filter_value}&$top=25")
            if item.get("id") and item.get("displayName") and item.get("securityEnabled") is True
        ]