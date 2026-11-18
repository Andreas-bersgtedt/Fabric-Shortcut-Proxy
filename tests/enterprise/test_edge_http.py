import json

from fastapi import FastAPI
from fastapi.testclient import TestClient
import httpx
import pytest

from enterprise.control.edge_client import EdgeReplayClient
from enterprise.control.edge_http import create_edge_router
from enterprise.control.edge_journal import EdgeJournal
from enterprise.control.edge_receiver import (
    EdgeBinding, EdgeReplayReceiver, MAX_EDGE_REQUEST_BYTES, decode_edge_request,
)
from enterprise.control.federation import FederatedPublicationCoordinator, GlobalCatalog, RegionalReplicaVerifier
from enterprise.control.placement import PlacementConfig
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.config import TableDef
from fabric_shortcut_proxy.runtime.artifact_store import LocalDirStore
from fabric_shortcut_proxy.security.agent_auth import AgentAuthMiddleware
from fabric_shortcut_proxy.security.operator_auth import ManagerAuthMiddleware
from tests.enterprise.test_edge_receiver import _generation
from tests.enterprise.test_federated_publication_runtime import _runtime
from tests.enterprise.test_residency_policy import _config


def _client(monkeypatch, *, scheme="https"):
    monkeypatch.setenv("AGENT_AUTH_MODE", "required")
    monkeypatch.setenv("AGENT_TOKEN", "shared-token-" + "x" * 32)
    monkeypatch.setenv("AGENT_IDENTITY_TOKENS", json.dumps({
        "site-1": "site-one-token-" + "x" * 32,
        "site-2": "site-two-token-" + "x" * 32,
    }))
    catalog, _, _, primary, _ = _runtime()
    receiver = EdgeReplayReceiver(
        primary, {"site-1": EdgeBinding("source::sales", "sales", "warehouse/sales")}
    )
    app = FastAPI()
    app.add_middleware(ManagerAuthMiddleware)
    app.add_middleware(AgentAuthMiddleware)
    app.include_router(create_edge_router(receiver))
    return TestClient(app, base_url=f"{scheme}://testserver"), catalog


def _headers(identity="site-1", token="site-one-token-" + "x" * 32):
    return {"X-FSP-Agent-ID": identity, "X-FSP-Agent-Token": token}


def test_http_requires_identity_and_acknowledges_after_publication(tmp_path, monkeypatch):
    client, catalog = _client(monkeypatch)
    payload = _generation(tmp_path, 1)
    assert client.post("/control/edge/replay", json=payload).status_code == 401
    assert client.post(
        "/control/edge/replay", headers=_headers(token="shared-token-" + "x" * 32),
        json=payload,
    ).status_code == 401
    assert client.post(
        "/control/edge/replay",
        headers=_headers("site-3", "shared-token-" + "x" * 32), json=payload,
    ).status_code == 403
    assert client.post(
        "/control/edge/replay",
        headers=_headers("site-2", "site-two-token-" + "x" * 32), json=payload,
    ).status_code == 403
    assert catalog.get("source::sales")["replicas"] == {}
    response = client.post("/control/edge/replay", headers=_headers(), json=payload)
    assert response.status_code == 200
    assert response.json()["sequence"] == 1
    assert catalog.get("source::sales")["edge_replay"]["sequence"] == 1
    assert client.post("/control/edge/replay", headers=_headers(), json=payload).json() == response.json()


def test_http_requires_tls_and_limits_request_size(tmp_path, monkeypatch):
    client, catalog = _client(monkeypatch, scheme="http")
    assert client.post("/control/edge/replay", headers=_headers(), json=_generation(tmp_path, 1)).status_code == 403
    client, catalog = _client(monkeypatch)
    response = client.post(
        "/control/edge/replay",
        headers={**_headers(), "content-length": str(MAX_EDGE_REQUEST_BYTES + 1)},
        content=b"{}",
    )
    assert response.status_code == 413
    assert catalog.get("source::sales")["replicas"] == {}


async def test_client_retries_when_acknowledgement_is_lost(tmp_path, monkeypatch):
    client, catalog = _client(monkeypatch)
    monkeypatch.setenv("AGENT_TOKEN", "site-one-token-" + "x" * 32)
    store = LocalDirStore(str(tmp_path / "source"))
    journal = EdgeJournal(tmp_path / "client.db", dataset_id="source::sales")
    entry, objects = decode_edge_request(_generation(tmp_path, 1))
    for key, data in objects.items():
        store.put(key, data)
    journal.append(entry.manifest, metadata=entry.metadata)

    class LostAcknowledgement(httpx.AsyncBaseTransport):
        def __init__(self):
            self.transport = httpx.ASGITransport(app=client.app)
            self.first = True

        async def handle_async_request(self, request):
            response = await self.transport.handle_async_request(request)
            if self.first:
                self.first = False
                await response.aclose()
                raise httpx.ReadError("acknowledgement lost", request=request)
            return response

        async def aclose(self):
            await self.transport.aclose()

    replay = EdgeReplayClient("https://testserver", "site-1", transport=LostAcknowledgement())
    try:
        with pytest.raises(httpx.ReadError):
            await replay.replay_once(journal, store)
        assert journal.next_pending() is not None
        record = catalog.get("source::sales")
        assert record["edge_replay"]["sequence"] == 1
        assert await replay.replay_once(journal, store)
        assert journal.next_pending() is None
        assert catalog.get("source::sales") == record
    finally:
        await replay.aclose()


def test_manager_installs_edge_route_and_rejects_ha(tmp_path, monkeypatch):
    from enterprise.control import materialize_service
    from enterprise.control.manager_app import create_manager_app
    from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore

    value = _config()
    value["connections"]["source"] = value["connections"].pop("erp")
    value["connections"]["source"]["staging_storage_profile"] = "published"
    for pool in value["pools"]:
        pool["allowed_connection_ids"] = ["source"]
    value["pools"][0]["identities"].append("site-1")
    placement = PlacementConfig.from_dict(value)
    store = MemoryStore()
    catalog = GlobalCatalog(store, placement)
    catalog.create(
        "source::sales", owner_id="manager-swc", owner_location="azure:swedencentral",
        lease_ms=100_000, freshness_target_ms=5_000,
    )
    coordinator = FederatedPublicationCoordinator(
        catalog, RegionalReplicaVerifier(placement, {"published": store}), store,
        owner_id="manager-swc", owner_location="azure:swedencentral",
        storage_profile="published", lease_ms=100_000,
    )
    token = "site-one-token-" + "x" * 32
    monkeypatch.setenv("AGENT_AUTH_MODE", "required")
    monkeypatch.setenv("AGENT_TOKEN", "shared-token-" + "x" * 32)
    monkeypatch.setenv("AGENT_IDENTITY_TOKENS", json.dumps({
        "site-1": token, "eu-agent": "eu-" + "x" * 32, "backup-agent": "backup-" + "x" * 32,
    }))
    monkeypatch.setenv("FSP_EDGE_BINDINGS", json.dumps({
        "site-1": {"dataset_id": "source::sales", "table": "sales", "key_prefix": "warehouse/sales"},
    }))
    monkeypatch.setenv("FSP_FEDERATION_ENABLED", "1")
    monkeypatch.setenv("FSP_ARTIFACT_STORE_PROFILE", "published")
    monkeypatch.setattr(PlacementConfig, "from_environment", lambda: placement)
    monkeypatch.setattr(FederatedPublicationCoordinator, "from_environment", lambda *_args: coordinator)
    monkeypatch.setattr("fabric_shortcut_proxy.runtime.artifact_store.get_default_store", lambda: store)
    monkeypatch.setattr(config, "AGENT_AUTH_MODE", "required")
    monkeypatch.setattr(config, "MANAGER_HA", False)
    monkeypatch.setattr(config, "ARTIFACT_STORE_BACKEND", "azure")
    monkeypatch.setattr(config, "MATERIALIZATION_WORK_QUEUE", True)
    monkeypatch.setattr(config, "MANAGER_SUPERVISION_MODE", "external")
    monkeypatch.setattr(config, "ENABLE_GATEWAY", False)
    monkeypatch.setattr(config, "TABLES", [
        TableDef(name="sales", source_table="sales", connection_id="source"),
    ])
    try:
        app = create_manager_app()
        response = TestClient(app, base_url="https://testserver").post(
            "/control/edge/replay", headers=_headers(token=token), json=_generation(tmp_path, 1),
        )
        assert response.status_code == 200
        assert catalog.get("source::sales")["edge_replay"]["sequence"] == 1
        monkeypatch.setenv("AGENT_IDENTITY_TOKENS", json.dumps({
            "site-1": token, "eu-agent": token, "backup-agent": "backup-" + "x" * 32,
        }))
        with pytest.raises(ValueError, match="tokens must be unique"):
            create_manager_app()
        monkeypatch.setenv("AGENT_IDENTITY_TOKENS", json.dumps({
            "site-1": token, "eu-agent": "eu-" + "x" * 32, "backup-agent": "backup-" + "x" * 32,
        }))
        monkeypatch.setattr(config, "MANAGER_HA", True)
        with pytest.raises(ValueError, match="non-HA"):
            create_manager_app()
    finally:
        materialize_service.reset()
