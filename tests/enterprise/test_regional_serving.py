from dataclasses import replace

from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from fastapi.testclient import TestClient
import pytest

from enterprise.control.regional_serving_app import create_regional_serving_app
from fabric_shortcut_proxy.security import access_keys
from tests.enterprise.test_catalog_service import _snapshot
from tests.enterprise.test_federated_publication_runtime import _runtime


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
