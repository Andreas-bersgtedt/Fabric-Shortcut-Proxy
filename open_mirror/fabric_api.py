"""Fabric REST helpers using the Manager's existing Entra credential."""
from __future__ import annotations

import base64
import copy
import email.utils
import json
import time

import config
from security.azure_credential import proxy_credential

_FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
_ONELAKE_HOST = "https://onelake.dfs.fabric.microsoft.com"


class FabricApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retriable: bool = False,
        retry_after: float | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retriable = retriable
        self.retry_after = retry_after
        self.request_id = request_id


def landing_zone_url(workspace_id: str, mirrored_db_id: str) -> str:
    return f"{_ONELAKE_HOST}/{workspace_id.strip()}/{mirrored_db_id.strip()}/Files/LandingZone"


def _token(credential=None) -> str:
    cred = credential or proxy_credential(config)
    return cred.get_token(_FABRIC_SCOPE).token


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(value)
            return max(0.0, parsed.timestamp() - time.time())
        except (TypeError, ValueError):
            return None


def _fabric_error(resp) -> FabricApiError:
    request_id = (
        resp.headers.get("request-id")
        or resp.headers.get("x-ms-request-id")
        or resp.headers.get("x-ms-correlation-id")
    )
    try:
        body = resp.json()
    except ValueError:
        body = {}
    detail = body.get("error", body) if isinstance(body, dict) else {}
    message = detail.get("message") if isinstance(detail, dict) else None
    is_retriable = bool(
        detail.get("isRetriable", body.get("isRetriable", False))
        if isinstance(detail, dict) and isinstance(body, dict) else False
    )
    suffix = f" (request ID {request_id})" if request_id else ""
    if resp.status_code in {401, 403}:
        return FabricApiError(
            "Fabric API authorization failed. The Manager identity requires Read and "
            "Write permission on the mirrored database, and the Fabric tenant setting "
            "that permits service principals to use Fabric APIs must be enabled."
            + suffix,
            status_code=resp.status_code,
            request_id=request_id,
        )
    return FabricApiError(
        f"Fabric API {resp.status_code}: {(message or resp.text[:200])}{suffix}",
        status_code=resp.status_code,
        retriable=(
            resp.status_code == 429 or resp.status_code >= 500 or is_retriable
        ),
        retry_after=_parse_retry_after(resp.headers.get("Retry-After")),
        request_id=request_id,
    )


def _request(
    method: str,
    path: str,
    token: str,
    *,
    timeout: float = 30.0,
    json_body: dict | None = None,
) -> dict:
    import httpx

    url = f"{_FABRIC_BASE}/{path.lstrip('/')}"
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.request(
                method,
                url,
                headers={"Authorization": f"Bearer {token}"},
                **({"json": json_body} if json_body is not None else {}),
            )
    except httpx.TransportError as exc:
        raise FabricApiError(
            f"Fabric API transport failure: {exc}", retriable=True
        ) from exc
    if resp.status_code >= 400:
        raise _fabric_error(resp)
    if not resp.content:
        return {}
    try:
        return resp.json()
    except ValueError as exc:
        raise FabricApiError(
            "Fabric API returned invalid JSON",
            request_id=resp.headers.get("request-id"),
        ) from exc


def _get_all(path: str, token: str, params: dict | None = None) -> list[dict]:
    import httpx

    url = f"{_FABRIC_BASE}/{path.lstrip('/')}"
    headers = {"Authorization": f"Bearer {token}"}
    out: list[dict] = []
    with httpx.Client(timeout=30.0) as client:
        while True:
            try:
                resp = client.get(url, headers=headers, params=params)
            except httpx.TransportError as exc:
                raise FabricApiError(
                    f"Fabric API transport failure: {exc}", retriable=True
                ) from exc
            if resp.status_code >= 400:
                raise _fabric_error(resp)
            data = resp.json()
            out.extend(data.get("value", []) or [])
            continuation_uri = data.get("continuationUri")
            continuation_token = data.get("continuationToken")
            if continuation_uri:
                url, params = continuation_uri, None
            elif continuation_token:
                params = dict(params or {})
                params["continuationToken"] = continuation_token
            else:
                break
    return out


def list_workspaces(credential=None) -> list[dict]:
    items = _get_all("workspaces", _token(credential))
    return [
        {"id": item.get("id"), "name": item.get("displayName") or item.get("name") or item.get("id")}
        for item in items if item.get("id")
    ]


def list_mirrored_databases(workspace_id: str, credential=None) -> list[dict]:
    token = _token(credential)
    try:
        items = _get_all(f"workspaces/{workspace_id}/mirroredDatabases", token)
    except FabricApiError:
        items = _get_all(
            f"workspaces/{workspace_id}/items",
            token,
            params={"type": "MirroredDatabase"},
        )
    return [
        {
            "id": item.get("id"),
            "name": item.get("displayName") or item.get("name") or item.get("id"),
            "workspace_id": workspace_id,
            "landing_zone_root": landing_zone_url(workspace_id, item["id"]),
        }
        for item in items if item.get("id")
    ]


def get_mirroring_status(
    workspace_id: str, mirrored_database_id: str, credential=None
) -> dict:
    path = (
        f"workspaces/{workspace_id}/mirroredDatabases/{mirrored_database_id}"
        "/getMirroringStatus"
    )
    return _request("POST", path, _token(credential))


def start_mirroring(
    workspace_id: str, mirrored_database_id: str, credential=None
) -> dict:
    path = (
        f"workspaces/{workspace_id}/mirroredDatabases/{mirrored_database_id}"
        "/startMirroring"
    )
    return _request("POST", path, _token(credential))


def get_mirrored_database_definition(
    workspace_id: str, mirrored_database_id: str, credential=None
) -> dict:
    path = (
        f"workspaces/{workspace_id}/mirroredDatabases/{mirrored_database_id}"
        "/getDefinition"
    )
    return _request("POST", path, _token(credential))


def update_mirrored_database_retention(
    workspace_id: str,
    mirrored_database_id: str,
    retention_days: int,
    credential=None,
) -> bool:
    """Set retentionInDays while preserving every other definition part."""
    if (
        isinstance(retention_days, bool)
        or not isinstance(retention_days, int)
        or not 1 <= retention_days <= 30
    ):
        raise ValueError("retention_days must be an integer from 1 through 30")
    current = get_mirrored_database_definition(
        workspace_id, mirrored_database_id, credential
    )
    definition = copy.deepcopy(current.get("definition"))
    if not isinstance(definition, dict) or not isinstance(definition.get("parts"), list):
        raise FabricApiError("Fabric mirrored database definition has no parts")
    for part in definition["parts"]:
        if not isinstance(part, dict) or part.get("path") != "mirroring.json":
            continue
        if part.get("payloadType") != "InlineBase64" or not isinstance(part.get("payload"), str):
            raise FabricApiError("Fabric mirroring.json definition is not InlineBase64")
        try:
            payload = json.loads(base64.b64decode(part["payload"]).decode("utf-8"))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FabricApiError("Fabric mirroring.json definition is invalid") from exc
        target = payload.setdefault("properties", {}).setdefault("target", {})
        type_properties = target.setdefault("typeProperties", {})
        if type_properties.get("retentionInDays") == retention_days:
            return False
        type_properties["retentionInDays"] = retention_days
        part["payload"] = base64.b64encode(
            json.dumps(payload, separators=(",", ":")).encode("utf-8")
        ).decode("ascii")
        path = (
            f"workspaces/{workspace_id}/mirroredDatabases/{mirrored_database_id}"
            "/updateDefinition"
        )
        _request("POST", path, _token(credential), json_body={"definition": definition})
        return True
    raise FabricApiError("Fabric mirrored database definition has no mirroring.json part")
