from __future__ import annotations

import base64
import hashlib
import time

import pytest
from fastapi.testclient import TestClient

from enterprise.control.catalog_app import create_catalog_app
from enterprise.control.contract import SnapshotManifest, SplitRef
from enterprise.control.federation import (
    CatalogConflict,
    FederatedPublicationCoordinator,
    GlobalCatalog,
    RegionalReplicaVerifier,
)
from enterprise.control.placement import PlacementConfig
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore
from tests.enterprise.test_residency_policy import _config


DATASET = "erp::sales"


def _auth() -> dict[str, str]:
    value = base64.b64encode(b"catalog-admin:catalog-password").decode()
    return {"Authorization": f"Basic {value}"}


def _configure_auth(monkeypatch):
    monkeypatch.setattr(config, "MANAGER_AUTH_ENABLED", True)
    monkeypatch.setattr(config, "MANAGER_AUTH_USERNAME", "catalog-admin")
    monkeypatch.setattr(config, "MANAGER_AUTH_PASSWORD", "catalog-password")
    monkeypatch.setattr(config, "ENTRA_ENABLED", False)


def test_catalog_service_requires_auth_and_survives_restart(monkeypatch):
    _configure_auth(monkeypatch)
    store = MemoryStore()
    placement = PlacementConfig.from_dict(_config())
    app = create_catalog_app(store=store, placement=placement)

    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/catalog/datasets").status_code == 401
        created = client.post(
            "/catalog/datasets",
            headers=_auth(),
            json={
                "dataset_id": DATASET,
                "owner_id": "manager-swc",
                "owner_location": "azure:swedencentral",
                "lease_ms": 60_000,
                "freshness_target_ms": 5_000,
            },
        )
        assert created.status_code == 201
        assert created.json()["fence"] == 1

    restarted = create_catalog_app(store=store, placement=placement)
    with TestClient(restarted) as client:
        record = client.get(
            f"/catalog/datasets/{DATASET}", headers=_auth()
        )
        assert record.status_code == 200
        assert record.json()["owner_id"] == "manager-swc"
        moved = client.post(
            f"/catalog/datasets/{DATASET}/handover",
            headers=_auth(),
            json={
                "owner_id": "manager-swc",
                "fence": 1,
                "new_owner_id": "manager-neu",
                "new_location": "azure:northeurope",
                "lease_ms": 60_000,
            },
        )
        assert moved.status_code == 200
        assert moved.json()["fence"] == 2


def _snapshot(store: MemoryStore, generation: int) -> SnapshotManifest:
    data = f"generation-{generation}".encode()
    metadata = f'{{"generation":{generation}}}'.encode()
    data_key = f"published/{generation}/part.parquet"
    metadata_key = f"published/{generation}/metadata.json"
    store.put(data_key, data)
    store.put(metadata_key, metadata)
    return SnapshotManifest(
        table="sales",
        epoch=generation,
        table_format="iceberg",
        splits=[
            SplitRef(
                object_key=data_key,
                size_bytes=len(data),
                record_count=1,
                content_hash=hashlib.sha256(data).hexdigest(),
            )
        ],
        metadata_keys=[metadata_key],
        generation_id=f"generation-{generation}",
        generation_fence=generation,
        plan_sha256=hashlib.sha256(str(generation).encode()).hexdigest(),
        published_at_ms=10_000 + generation,
        request_id=f"request-{generation}",
        dataset_id=DATASET,
        produced_at_ms=9_000 + generation,
    )


def test_manager_publication_is_fenced_by_catalog_handover():
    placement = PlacementConfig.from_dict(_config())
    store = MemoryStore()
    catalog = GlobalCatalog(store, placement)
    now_ms = time.time_ns() // 1_000_000
    catalog.create(
        DATASET,
        owner_id="manager-swc",
        owner_location="azure:swedencentral",
        lease_ms=100_000,
        freshness_target_ms=5_000,
        now_ms=now_ms,
    )
    verifier = RegionalReplicaVerifier(placement, {"published": store})
    swc = FederatedPublicationCoordinator(
        catalog,
        verifier,
        store,
        owner_id="manager-swc",
        owner_location="azure:swedencentral",
        storage_profile="published",
        lease_ms=100_000,
    )
    neu = FederatedPublicationCoordinator(
        catalog,
        verifier,
        store,
        owner_id="manager-neu",
        owner_location="azure:northeurope",
        storage_profile="published",
        lease_ms=100_000,
    )

    swc.publish(_snapshot(store, 1))
    moved = catalog.handover(
        DATASET,
        owner_id="manager-swc",
        fence=1,
        new_owner_id="manager-neu",
        new_location="azure:northeurope",
        lease_ms=100_000,
        now_ms=now_ms + 1,
    )
    assert moved["fence"] == 2
    with pytest.raises(CatalogConflict, match="another regional Manager"):
        swc.publish(_snapshot(store, 2))

    neu.publish(_snapshot(store, 2))
    resolved = catalog.resolve(DATASET, "primary", now_ms=11_000)
    assert resolved["generation"]["generation_id"] == "generation-2"

    returned = catalog.handover(
        DATASET,
        owner_id="manager-neu",
        fence=2,
        new_owner_id="manager-swc",
        new_location="azure:swedencentral",
        lease_ms=100_000,
    )
    assert returned["fence"] == 3
    with pytest.raises(CatalogConflict, match="another regional Manager"):
        neu.publish(_snapshot(store, 3))
    swc.publish(_snapshot(store, 3))
    assert catalog.resolve(DATASET, "primary", now_ms=11_000)[
        "generation"
    ]["generation_id"] == "generation-3"


def test_expired_owner_cannot_write_federation_manifest():
    placement = PlacementConfig.from_dict(_config())
    store = MemoryStore()
    catalog = GlobalCatalog(store, placement)
    catalog.create(
        DATASET,
        owner_id="manager-swc",
        owner_location="azure:swedencentral",
        lease_ms=1,
        freshness_target_ms=5_000,
        now_ms=1,
    )
    coordinator = FederatedPublicationCoordinator(
        catalog,
        RegionalReplicaVerifier(placement, {"published": store}),
        store,
        owner_id="manager-swc",
        owner_location="azure:swedencentral",
        storage_profile="published",
        lease_ms=60_000,
    )
    with pytest.raises(CatalogConflict, match="stale or expired"):
        coordinator.publish(_snapshot(store, 1))
    assert list(store.list("_control/federation/manifests/")) == []
    assert catalog.get(DATASET)["generations"] == {}


@pytest.mark.parametrize("interval", [
    "invalid", "0", "-1", "nan", "inf", "-inf", "60", "61",
])
def test_manager_rejects_invalid_renewal_interval_before_startup(
    monkeypatch, interval,
):
    from enterprise.control.manager_app import create_manager_app
    from fabric_shortcut_proxy.runtime.artifact_store import (
        reset_default_store,
        set_default_store,
    )

    store = MemoryStore()
    placement = PlacementConfig.from_dict(_config())
    coordinator = FederatedPublicationCoordinator(
        GlobalCatalog(store, placement),
        RegionalReplicaVerifier(placement, {"published": store}),
        store,
        owner_id="manager-swc",
        owner_location="azure:swedencentral",
        storage_profile="published",
        lease_ms=60_000,
    )
    set_default_store(store)
    monkeypatch.setattr(config, "MANAGER_HA", False)
    monkeypatch.setattr(config, "MATERIALIZATION_WORK_QUEUE", True)
    monkeypatch.setattr(config, "MANAGER_SUPERVISION_MODE", "external")
    monkeypatch.setattr(config, "ENABLE_GATEWAY", False)
    monkeypatch.setenv("FSP_FEDERATION_ENABLED", "1")
    monkeypatch.setenv("FSP_CATALOG_RENEW_INTERVAL_SECONDS", interval)
    monkeypatch.setattr(
        FederatedPublicationCoordinator,
        "from_environment",
        lambda *_args: coordinator,
    )
    try:
        with pytest.raises(ValueError, match="FSP_CATALOG_RENEW_INTERVAL_SECONDS"):
            create_manager_app()
    finally:
        reset_default_store()


def test_manager_uses_separate_catalog_store_when_configured(monkeypatch):
    placement = PlacementConfig.from_dict(_config())
    artifact_store = MemoryStore()
    catalog_store = MemoryStore()
    monkeypatch.setenv("FSP_REGIONAL_MANAGER_ID", "manager-swc")
    monkeypatch.setenv("FSP_REGIONAL_MANAGER_LOCATION", "azure:swedencentral")
    monkeypatch.setenv("FSP_ARTIFACT_STORE_PROFILE", "published")
    monkeypatch.setenv(
        "FSP_CATALOG_STORE_ACCOUNT_URL",
        "https://catalog.blob.core.windows.net",
    )
    monkeypatch.setenv("FSP_CATALOG_STORE_CONTAINER", "catalog")
    from fabric_shortcut_proxy.runtime import azure_artifact_store

    monkeypatch.setattr(
        azure_artifact_store,
        "build_azure_store",
        lambda **_settings: catalog_store,
    )

    coordinator = FederatedPublicationCoordinator.from_environment(
        artifact_store, placement
    )
    assert coordinator.store is artifact_store
    assert coordinator.catalog.store is catalog_store


def test_manager_lifespan_retries_replication_and_stops_it(monkeypatch):
    import threading
    from enterprise.control.manager_app import create_manager_app
    from enterprise.control import materialize_service
    from fabric_shortcut_proxy.runtime import artifact_store

    store = MemoryStore()
    placement = PlacementConfig.from_dict(_config())
    coordinator = FederatedPublicationCoordinator(
        GlobalCatalog(store, placement), RegionalReplicaVerifier(placement, {"published": store}),
        store, owner_id="manager-swc", owner_location="azure:swedencentral",
        storage_profile="published", lease_ms=60_000,
    )
    calls = []
    recovered = threading.Event()

    def reconcile():
        calls.append(1)
        if len(calls) == 1:
            raise OSError("transient replication outage")
        recovered.set()
        return 0

    monkeypatch.setattr(coordinator, "replicate_owned", reconcile)
    monkeypatch.setattr(coordinator, "renew_owned", lambda: [])
    monkeypatch.setattr(artifact_store, "get_default_store", lambda: store)
    monkeypatch.setattr(FederatedPublicationCoordinator, "from_environment", lambda *_args: coordinator)
    monkeypatch.setattr(config, "MANAGER_HA", False)
    monkeypatch.setattr(config, "MATERIALIZATION_WORK_QUEUE", True)
    monkeypatch.setattr(config, "MANAGER_SUPERVISION_MODE", "external")
    monkeypatch.setattr(config, "ENABLE_GATEWAY", False)
    monkeypatch.setattr(config, "OPEN_MIRROR_PUBLISH", False)
    monkeypatch.setenv("FSP_FEDERATION_ENABLED", "1")
    monkeypatch.setenv("FSP_CATALOG_RENEW_INTERVAL_SECONDS", "0.02")
    monkeypatch.setenv("FSP_FEDERATION_REPLICA_STORES", '{"replica":{}}')
    try:
        with TestClient(create_manager_app()):
            assert recovered.wait(5)
        stopped_count = len(calls)
        time.sleep(0.08)
        assert len(calls) == stopped_count and stopped_count >= 2
    finally:
        materialize_service.reset()
