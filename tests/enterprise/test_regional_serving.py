from dataclasses import replace
import json

from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from fastapi.testclient import TestClient
import pytest

from enterprise.control.regional_serving_app import create_regional_serving_app
from enterprise.control.federation import FederatedPublicationCoordinator
from enterprise.control.placement import PlacementConfig
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore
from fabric_shortcut_proxy.security import access_keys
from tests.enterprise.test_catalog_service import DATASET, _snapshot
from tests.enterprise.test_federated_publication_runtime import _runtime
from tests.enterprise.test_residency_policy import _config
from tests.test_cloud_artifact_conformance import FakeGCSBucket, FakeS3


def _sign(method, path, *, headers=None):
    request = AWSRequest(
        method=method, url=f"http://testserver{path}", headers=headers or {},
    )
    S3SigV4Auth(Credentials("regional-reader", "regional-test-secret"), "s3", "us-east-1").add_auth(request)
    return dict(request.headers)


def test_assigned_regional_s3_reads_stay_on_complete_replica(monkeypatch):
    catalog, _, stores, primary, secondary = _runtime()
    primary.publish(replace(_snapshot(stores["published"], 1), dataset_id="source::sales"))
    primary.replicate_owned()
    primary.publish(replace(_snapshot(stores["published"], 2), dataset_id="source::sales"))
    monkeypatch.setattr(access_keys, "resolve_secret", lambda key: "regional-test-secret" if key == "regional-reader" else None)
    monkeypatch.setattr(access_keys, "authorize", lambda *_args: None)
    app = create_regional_serving_app(
        secondary, endpoint_id="secondary", buckets={"sales": "source::sales"}
    )
    with TestClient(app) as client:
        assert client.get("/readyz").status_code == 200
        assert client.get("/sales/published/1/part.parquet").status_code == 403
        root = client.get("/", headers=_sign("GET", "/"))
        assert root.status_code == 200
        assert b"ListAllMyBucketsResult" in root.content
        assert b"<Name>sales</Name>" in root.content
        assert client.head("/", headers=_sign("HEAD", "/")).status_code == 200
        path = "/sales/published/1/part.parquet"
        assert client.get(path, headers=_sign("GET", path)).content == b"generation-1"
        response = client.get(
            path, headers=_sign("GET", path, headers={"range": "bytes=0-3"})
        )
        assert response.status_code == 206 and response.content == b"gene"
        assert client.head(path, headers=_sign("HEAD", path)).headers["content-length"] == "12"
        newer = "/sales/published/2/part.parquet"
        assert client.get(newer, headers=_sign("GET", newer)).status_code == 404
        listing = client.get("/sales?list-type=2", headers=_sign("GET", "/sales?list-type=2"))
        assert listing.status_code == 200
        assert b"published/1/part.parquet" in listing.content
        assert b"_control/federation" not in listing.content
        assert b"published/2" not in listing.content
        assert client.put(path, headers=_sign("PUT", path)).status_code == 405
        assert catalog.resolve("source::sales", "secondary", now_ms=14_002)["stale"]
        assert client.get(path, headers=_sign("GET", path)).content == b"generation-1"
        assert primary.replicate_owned() == 1
        assert client.get(newer, headers=_sign("GET", newer)).content == b"generation-2"


def test_regional_endpoint_rejects_cross_store_binding():
    _, _, _, primary, _ = _runtime()
    with pytest.raises(ValueError, match="own configured store"):
        create_regional_serving_app(
            primary, endpoint_id="secondary", buckets={"sales": "source::sales"}
        )


def test_regional_reads_enforce_scoped_key_authorization(monkeypatch):
    _, _, _, _, secondary = _runtime()
    monkeypatch.setattr(access_keys, "resolve_secret", lambda _key: "regional-test-secret")
    monkeypatch.setattr(access_keys, "authorize", lambda *_args: "reader is outside this bucket")
    app = create_regional_serving_app(
        secondary, endpoint_id="secondary", buckets={"sales": "source::sales"}
    )
    with TestClient(app) as client:
        response = client.get("/sales", headers=_sign("GET", "/sales"))
        assert response.status_code == 403 and b"AccessDenied" in response.content


def test_corrupted_committed_replica_fails_closed(monkeypatch):
    _, _, stores, primary, secondary = _runtime()
    primary.publish(replace(_snapshot(stores["published"], 1), dataset_id="source::sales"))
    primary.replicate_owned()
    stores["replica"].put("published/1/part.parquet", b"corruption-1")
    monkeypatch.setattr(access_keys, "resolve_secret", lambda _key: "regional-test-secret")
    monkeypatch.setattr(access_keys, "authorize", lambda *_args: None)
    app = create_regional_serving_app(secondary, endpoint_id="secondary", buckets={"sales": "source::sales"})
    with TestClient(app) as client:
        path = "/sales/published/1/part.parquet"
        response = client.get(path, headers=_sign("GET", path))
        assert response.status_code == 503 and b"corruption-1" not in response.content


def test_serving_factory_binds_replica_without_enabling_dispatch(monkeypatch):
    from enterprise.control.regional_serving_app import app_from_environment
    from fabric_shortcut_proxy import config
    from fabric_shortcut_proxy.runtime import artifact_store, azure_artifact_store
    from enterprise.control.placement import ResidencyPolicyViolation

    _, _, stores, primary, secondary = _runtime()
    placement = secondary.catalog.placement
    monkeypatch.setattr(type(placement), "from_environment", lambda: placement)
    monkeypatch.setattr(config, "ARTIFACT_STORE_BACKEND", "azure")
    monkeypatch.setenv("FSP_ARTIFACT_STORE_PROFILE", "replica")
    monkeypatch.setenv("FSP_REGIONAL_SERVING_ENDPOINT_ID", "secondary")
    monkeypatch.setenv("FSP_REGIONAL_SERVING_BUCKETS", '{"sales":"source::sales"}')
    monkeypatch.setenv("FSP_CATALOG_STORE_ACCOUNT_URL", "https://catalog.blob.core.windows.net")
    monkeypatch.setenv("FSP_CATALOG_STORE_CONTAINER", "catalog")
    monkeypatch.setenv("FSP_FEDERATION_REPLICA_STORES", "{}")
    monkeypatch.setattr(artifact_store, "get_default_store", lambda: stores["replica"])
    monkeypatch.setattr(azure_artifact_store, "build_azure_store", lambda **_kwargs: primary.catalog.store)
    app = app_from_environment()
    assert app.title.endswith("regional serving")
    with pytest.raises(ResidencyPolicyViolation, match="cross-store dispatch"):
        placement.validate_dispatch(placement.policy_for("source", "sales"))


@pytest.mark.parametrize(
    ("provider", "location"),
    [("s3", "aws:us-east-1"), ("gcs", "gcp:europe-west1")],
)
def test_cloud_replica_environment_replication_and_signed_regional_reads(
    monkeypatch, provider, location,
):
    from enterprise.control import regional_serving_app
    from fabric_shortcut_proxy.runtime import artifact_store, azure_artifact_store, cloud_artifact_store

    if provider == "s3":
        boto3 = pytest.importorskip("boto3")
        sdk_client = FakeS3()

        def client(service, *, region_name):
            assert service == "s3" and region_name == "us-east-1"
            return sdk_client

        monkeypatch.setattr(boto3, "client", client)
    else:
        storage = pytest.importorskip("google.cloud.storage")
        sdk_bucket = FakeGCSBucket()

        class StorageClient:
            def bucket(self, name):
                assert name == "artifacts"
                return sdk_bucket

        monkeypatch.setattr(storage, "Client", StorageClient)

    value = _config()
    value["stores"]["replica"].update(provider=provider, location=location)
    value["serving_endpoints"]["secondary"]["location"] = location
    value["connections"]["erp"]["residency_locations"].append(location)
    placement = PlacementConfig.from_dict(value)
    catalog_store, primary_store = MemoryStore(), MemoryStore()
    settings = {"bucket": "artifacts", "prefix": "replica"}
    if provider == "s3":
        settings["region"] = "us-east-1"
    calls = []
    build_cloud_data_store = cloud_artifact_store.build_cloud_data_store

    def cloud_store(selected_provider, **kwargs):
        store = build_cloud_data_store(selected_provider, **kwargs)
        calls.append((selected_provider, kwargs, store))
        return store

    monkeypatch.setattr(cloud_artifact_store, "build_cloud_data_store", cloud_store)
    monkeypatch.setattr(azure_artifact_store, "build_azure_store", lambda **kwargs: catalog_store)
    monkeypatch.setenv("FSP_CATALOG_STORE_ACCOUNT_URL", "https://catalog.example.test")
    monkeypatch.setenv("FSP_CATALOG_STORE_CONTAINER", "catalog")
    monkeypatch.setenv("FSP_REGIONAL_MANAGER_ID", "manager-swc")
    monkeypatch.setenv("FSP_REGIONAL_MANAGER_LOCATION", "azure:swedencentral")
    monkeypatch.setenv("FSP_ARTIFACT_STORE_PROFILE", "published")
    monkeypatch.setenv("FSP_FEDERATION_REPLICA_STORES", json.dumps({"replica": settings}))
    primary = FederatedPublicationCoordinator.from_environment(primary_store, placement)
    replica_store = calls[0][2]
    primary.catalog.create(
        DATASET, owner_id="manager-swc", owner_location="azure:swedencentral",
        lease_ms=100_000, freshness_target_ms=5_000,
    )
    primary.publish(_snapshot(primary_store, 1))
    assert primary.replicate_owned() == 1

    monkeypatch.setenv("FSP_ARTIFACT_STORE_PROFILE", "replica")
    monkeypatch.setenv("FSP_REGIONAL_SERVING_ENDPOINT_ID", "secondary")
    monkeypatch.setenv("FSP_REGIONAL_SERVING_BUCKETS", json.dumps({"sales": DATASET}))
    monkeypatch.setenv("FSP_REGIONAL_SERVING_CLOUD_STORE", json.dumps(settings))
    monkeypatch.setenv("FSP_FEDERATION_REPLICA_STORES", "{}")
    monkeypatch.setattr(PlacementConfig, "from_environment", lambda: placement)
    monkeypatch.setattr(
        artifact_store, "get_default_store",
        lambda: pytest.fail("cloud regional reader must not use the Manager store"),
    )
    monkeypatch.setattr(access_keys, "resolve_secret", lambda _key: "regional-test-secret")
    monkeypatch.setattr(access_keys, "authorize", lambda *_args: None)
    app = regional_serving_app.app_from_environment()
    expected_settings = {"region": "", **settings}
    assert [(name, kwargs) for name, kwargs, _store in calls] == [
        (provider, expected_settings), (provider, expected_settings),
    ]
    reader_store = calls[1][2]

    with TestClient(app) as client:
        old_path = "/sales/published/1/part.parquet"
        new_path = "/sales/published/2/part.parquet"
        assert client.get(old_path, headers=_sign("GET", old_path)).content == b"generation-1"
        primary.publish(_snapshot(primary_store, 2))
        original_put = replica_store.put_stream_if_absent

        def interrupted(*args, **kwargs):
            raise OSError("replication interrupted")

        monkeypatch.setattr(replica_store, "put_stream_if_absent", interrupted)
        with pytest.raises(OSError, match="replication interrupted"):
            primary.replicate_owned()
        assert client.get(old_path, headers=_sign("GET", old_path)).status_code == 200
        assert client.get(new_path, headers=_sign("GET", new_path)).status_code == 404
        monkeypatch.setattr(replica_store, "put_stream_if_absent", original_put)
        assert primary.replicate_owned() == 1
        assert client.get(new_path, headers=_sign("GET", new_path)).content == b"generation-2"

        if provider == "s3":
            from botocore.exceptions import ClientError, NoCredentialsError

            errors = [
                ClientError({"Error": {"Code": "AccessDenied", "Message": "private"}}, "HeadObject"),
                NoCredentialsError(),
            ]
        else:
            from google.api_core.exceptions import Forbidden
            from google.auth.exceptions import RefreshError

            errors = [Forbidden("private"), RefreshError("private")]

        for error in errors:
            def unavailable(*args, **kwargs):
                raise error

            monkeypatch.setattr(reader_store, "head", unavailable)
            response = client.get(new_path, headers=_sign("GET", new_path))
            assert response.status_code == 503
            assert b"ServiceUnavailable" in response.content and b"private" not in response.content


@pytest.mark.parametrize(
    "settings",
    [
        {"bucket": ""},
        {"bucket": "artifacts", "prefix": "../escape"},
        {"bucket": "artifacts", "access_key": "must-not-be-embedded"},
        {"bucket": "s3://user:password@artifacts"},
        {"bucket": "artifacts", "region": 123},
    ],
)
def test_cloud_replica_profile_rejects_invalid_or_secret_settings(monkeypatch, settings):
    value = _config()
    value["stores"]["replica"]["provider"] = "s3"
    placement = PlacementConfig.from_dict(value)
    monkeypatch.setenv("FSP_REGIONAL_MANAGER_ID", "manager-swc")
    monkeypatch.setenv("FSP_REGIONAL_MANAGER_LOCATION", "azure:swedencentral")
    monkeypatch.setenv("FSP_ARTIFACT_STORE_PROFILE", "published")
    monkeypatch.setenv("FSP_CATALOG_STORE_ACCOUNT_URL", "https://catalog.example.test")
    monkeypatch.setenv("FSP_CATALOG_STORE_CONTAINER", "catalog")
    monkeypatch.setenv("FSP_FEDERATION_REPLICA_STORES", json.dumps({"replica": settings}))
    from fabric_shortcut_proxy.runtime import azure_artifact_store, cloud_artifact_store
    monkeypatch.setattr(azure_artifact_store, "build_azure_store", lambda **kwargs: MemoryStore())
    monkeypatch.setattr(
        cloud_artifact_store, "build_cloud_data_store",
        lambda *args, **kwargs: pytest.fail("invalid settings must not create a cloud client"),
    )
    with pytest.raises(ValueError):
        FederatedPublicationCoordinator.from_environment(MemoryStore(), placement)


def test_azure_replica_profile_defaults_to_managed_identity(monkeypatch):
    placement = PlacementConfig.from_dict(_config())
    settings = {"account_url": "https://replica.example.test", "container_name": "replica"}
    calls = []
    monkeypatch.setenv("FSP_REGIONAL_MANAGER_ID", "manager-swc")
    monkeypatch.setenv("FSP_REGIONAL_MANAGER_LOCATION", "azure:swedencentral")
    monkeypatch.setenv("FSP_ARTIFACT_STORE_PROFILE", "published")
    monkeypatch.delenv("FSP_CATALOG_STORE_ACCOUNT_URL", raising=False)
    monkeypatch.delenv("FSP_CATALOG_STORE_CONTAINER", raising=False)
    monkeypatch.setenv("FSP_FEDERATION_REPLICA_STORES", json.dumps({"replica": settings}))
    from fabric_shortcut_proxy.runtime import azure_artifact_store

    def build(**kwargs):
        calls.append(kwargs)
        return MemoryStore()

    monkeypatch.setattr(azure_artifact_store, "build_azure_store", build)
    FederatedPublicationCoordinator.from_environment(MemoryStore(), placement)
    assert calls == [{"auth_mode": "managed_identity", **settings}]


def test_cloud_regional_reader_requires_explicit_settings_before_default_store(monkeypatch):
    from enterprise.control.regional_serving_app import app_from_environment
    from fabric_shortcut_proxy.runtime import artifact_store

    value = _config()
    value["stores"]["replica"]["provider"] = "s3"
    placement = PlacementConfig.from_dict(value)
    monkeypatch.setattr(PlacementConfig, "from_environment", lambda: placement)
    monkeypatch.setenv("FSP_ARTIFACT_STORE_PROFILE", "replica")
    monkeypatch.setenv("FSP_REGIONAL_SERVING_ENDPOINT_ID", "secondary")
    monkeypatch.delenv("FSP_REGIONAL_SERVING_CLOUD_STORE", raising=False)
    monkeypatch.setattr(
        artifact_store, "get_default_store",
        lambda: pytest.fail("missing regional settings must not select the Manager store"),
    )
    with pytest.raises(ValueError, match="requires FSP_REGIONAL_SERVING_CLOUD_STORE"):
        app_from_environment()


def test_gcs_regional_reader_can_use_gcs_catalog_store(monkeypatch):
    from enterprise.control.regional_serving_app import app_from_environment
    from fabric_shortcut_proxy.runtime import cloud_artifact_store
    from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore

    value = _config()
    value["stores"]["replica"]["provider"] = "gcs"
    value["stores"]["replica"]["location"] = "gcp:europe-west1"
    value["serving_endpoints"]["secondary"]["location"] = "gcp:europe-west1"
    value["connections"]["erp"]["residency_locations"].append("gcp:europe-west1")
    placement = PlacementConfig.from_dict(value)
    monkeypatch.setattr(PlacementConfig, "from_environment", lambda: placement)
    monkeypatch.setenv("FSP_ARTIFACT_STORE_PROFILE", "replica")
    monkeypatch.setenv("FSP_REGIONAL_SERVING_ENDPOINT_ID", "secondary")
    monkeypatch.setenv("FSP_REGIONAL_SERVING_CLOUD_STORE", '{"bucket":"artifacts"}')
    monkeypatch.setenv("FSP_REGIONAL_SERVING_BUCKETS", '{"sales":"erp::sales"}')
    monkeypatch.delenv("FSP_CATALOG_STORE_ACCOUNT_URL", raising=False)
    monkeypatch.delenv("FSP_CATALOG_STORE_CONTAINER", raising=False)
    monkeypatch.setattr(
        cloud_artifact_store, "build_cloud_data_store",
        lambda *args, **kwargs: MemoryStore(),
    )
    app = app_from_environment()
    assert app.title.endswith("regional serving")


def test_cloud_regional_reader_rejects_cross_profile_endpoint(monkeypatch):
    from enterprise.control.placement import ResidencyPolicyViolation
    from enterprise.control.regional_serving_app import app_from_environment
    from fabric_shortcut_proxy.runtime import cloud_artifact_store

    value = _config()
    value["stores"]["replica"]["provider"] = "s3"
    placement = PlacementConfig.from_dict(value)
    monkeypatch.setattr(PlacementConfig, "from_environment", lambda: placement)
    monkeypatch.setenv("FSP_ARTIFACT_STORE_PROFILE", "replica")
    monkeypatch.setenv("FSP_REGIONAL_SERVING_ENDPOINT_ID", "primary")
    monkeypatch.setenv("FSP_REGIONAL_SERVING_CLOUD_STORE", '{"bucket":"artifacts"}')
    monkeypatch.setenv("FSP_CATALOG_STORE_ACCOUNT_URL", "https://catalog.example.test")
    monkeypatch.setenv("FSP_CATALOG_STORE_CONTAINER", "catalog")
    monkeypatch.setattr(
        cloud_artifact_store, "build_cloud_data_store",
        lambda *args, **kwargs: pytest.fail("cross-profile binding must not create cloud clients"),
    )
    with pytest.raises(ResidencyPolicyViolation, match="serving store must match"):
        app_from_environment()
