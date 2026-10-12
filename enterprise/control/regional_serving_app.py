"""Read-only S3 endpoint pinned to the assigned catalog replica for each request."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import os

from fastapi import FastAPI, Request
from fastapi.responses import Response
from azure.core.exceptions import AzureError

from enterprise.control.federation import (
    CatalogConflict, FederatedPublicationCoordinator, GenerationDescriptor,
)
from fabric_shortcut_proxy.runtime.artifact_store import (
    ArtifactStore, ObjectNotFound, ObjectStat, _normalize_key,
)
from fabric_shortcut_proxy.observability.logging import get_logger
from fabric_shortcut_proxy.s3.auth import SigV4Error, verify_signature
from fabric_shortcut_proxy.s3.xml_responses import error_response
from fabric_shortcut_proxy.security import access_keys
from fabric_shortcut_proxy.storage.mounts import Mount
from fabric_shortcut_proxy.storage import passthrough

log = get_logger(__name__)


class RegionalServingStore(ArtifactStore):
    """Expose only immutable objects in one catalog-committed generation."""

    def __init__(self, store: ArtifactStore, descriptor: GenerationDescriptor):
        self.store = store
        self.objects = {
            ref.key: ref for ref in descriptor.objects
            if ref.key != descriptor.snapshot_key
        }

    def put(self, key: str, data: bytes) -> ObjectStat:
        raise PermissionError("regional serving is read-only")

    def delete(self, key: str) -> bool:
        raise PermissionError("regional serving is read-only")

    def exists(self, key: str) -> bool:
        return self.head(key) is not None

    def head(self, key: str) -> ObjectStat | None:
        key = _normalize_key(key)
        ref = self.objects.get(key)
        if ref is None:
            return None
        stat = self.store.head(key)
        if stat is None or not self.store.verify(key, size=ref.size_bytes, content_hash=ref.sha256):
            raise CatalogConflict("committed regional object is missing or changed")
        return stat

    def list(self, prefix: str = "") -> list[ObjectStat]:
        return [stat for key in sorted(self.objects) if key.startswith(prefix)
                and (stat := self.head(key)) is not None]

    def get(self, key: str, *, offset: int = 0, length: int | None = None) -> bytes:
        if self.head(key) is None:
            raise ObjectNotFound(key)
        return self.store.get(key, offset=offset, length=length)

    def get_stream(self, key: str, *, offset: int = 0, length: int | None = None,
                   chunk_size: int = 1 << 20):
        if self.head(key) is None:
            raise ObjectNotFound(key)
        return self.store.get_stream(key, offset=offset, length=length, chunk_size=chunk_size)


def create_regional_serving_app(
    coordinator: FederatedPublicationCoordinator,
    *,
    endpoint_id: str,
    buckets: dict[str, str],
) -> FastAPI:
    placement = coordinator.catalog.placement
    endpoint = placement.serving_endpoints.get(endpoint_id)
    if endpoint is None or endpoint.storage_profile != coordinator.storage_profile:
        raise ValueError("regional endpoint must bind its own configured store")
    if not buckets or any(not bucket or "/" in bucket for bucket in buckets):
        raise ValueError("regional serving requires bucket-to-dataset assignments")
    for dataset_id in buckets.values():
        policy = coordinator.catalog._policy(dataset_id)
        if endpoint_id not in policy.serving_endpoints:
            raise ValueError("regional endpoint is not assigned to the dataset")

    @asynccontextmanager
    async def lifespan(app):
        access_keys.list_access_keys()
        yield

    app = FastAPI(title="Fabric Shortcut Proxy regional serving", lifespan=lifespan)

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        if request.url.path in {"/healthz", "/readyz"}:
            return await call_next(request)
        raw_path = request.scope.get("raw_path", b"")
        canonical_path = "".join(
            chr(byte) if byte < 0x80 else f"%{byte:02X}" for byte in raw_path
        ) if isinstance(raw_path, bytes) and raw_path else request.url.path
        try:
            identity = verify_signature(
                request.method, canonical_path, request.url.query, request.headers,
                secret_resolver=access_keys.resolve_secret,
            )
        except SigV4Error as exc:
            log.warning("regional_sigv4_rejected", code=exc.code)
            return Response(error_response(exc.code, exc.message, request.url.path),
                            status_code=403, media_type="application/xml")
        bucket, _, key = request.url.path.lstrip("/").partition("/")
        denial = access_keys.authorize(identity, bucket, key, request.method)
        if denial:
            log.warning("regional_authz_denied", identity=identity, bucket=bucket)
            return Response(error_response("AccessDenied", denial, request.url.path),
                            status_code=403, media_type="application/xml")
        request.state.identity = identity
        return await call_next(request)

    @app.exception_handler(CatalogConflict)
    @app.exception_handler(OSError)
    @app.exception_handler(AzureError)
    async def unavailable(request, exc):
        log.error("regional_catalog_unavailable", detail=str(exc))
        return Response(error_response("ServiceUnavailable", "Regional publication is unavailable", request.url.path),
                        status_code=503, media_type="application/xml")

    @app.get("/healthz")
    def health():
        return {"status": "ok", "endpoint_id": endpoint_id}

    @app.get("/readyz")
    def ready():
        for dataset_id in buckets.values():
            coordinator.catalog.resolve(dataset_id, endpoint_id)
        return {"status": "ready"}

    def assigned_store(bucket: str) -> RegionalServingStore | None:
        dataset_id = buckets.get(bucket)
        if dataset_id is None:
            return None
        value = coordinator.catalog.resolve(dataset_id, endpoint_id)
        return RegionalServingStore(
            coordinator.store, GenerationDescriptor.from_dict(value["generation"])
        )

    @app.api_route("/{bucket}", methods=["GET", "HEAD"])
    async def bucket_request(bucket: str, request: Request):
        store = await asyncio.to_thread(assigned_store, bucket)
        if store is None:
            return Response(status_code=404)
        mount = Mount(bucket=bucket, backend="azure")
        if request.method == "HEAD":
            return passthrough.head_bucket(mount)
        return await asyncio.to_thread(passthrough.list_objects, mount, request, store)

    @app.api_route("/{bucket}/{key:path}", methods=["GET", "HEAD"])
    async def object_request(bucket: str, key: str, request: Request):
        store = await asyncio.to_thread(assigned_store, bucket)
        if store is None:
            return Response(status_code=404)
        mount = Mount(bucket=bucket, backend="azure")
        handler = passthrough.head_object if request.method == "HEAD" else passthrough.get_object
        return await asyncio.to_thread(handler, mount, key, request, store)

    return app


def app_from_environment() -> FastAPI:
    import json

    from enterprise.control.placement import PlacementConfig
    from fabric_shortcut_proxy.runtime.artifact_store import get_default_store
    from fabric_shortcut_proxy import config

    placement = PlacementConfig.from_environment()
    placement.bind_serving_store(
        os.environ.get("FSP_ARTIFACT_STORE_PROFILE", ""), config.ARTIFACT_STORE_BACKEND,
        os.environ.get("FSP_REGIONAL_SERVING_ENDPOINT_ID", ""),
    )
    coordinator = FederatedPublicationCoordinator.from_environment(
        get_default_store(), placement,
        serving_endpoint_id=os.environ.get("FSP_REGIONAL_SERVING_ENDPOINT_ID", ""),
    )
    buckets = json.loads(os.environ.get("FSP_REGIONAL_SERVING_BUCKETS", "{}"))
    if not isinstance(buckets, dict) or any(not isinstance(value, str) for value in buckets.values()):
        raise ValueError("FSP_REGIONAL_SERVING_BUCKETS must map buckets to dataset IDs")
    return create_regional_serving_app(
        coordinator, endpoint_id=os.environ.get("FSP_REGIONAL_SERVING_ENDPOINT_ID", ""),
        buckets=buckets,
    )
