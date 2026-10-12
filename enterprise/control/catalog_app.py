"""Authenticated HTTP service for durable federated dataset catalog operations."""
from __future__ import annotations

import contextlib

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from enterprise.control.federation import CatalogConflict, GlobalCatalog
from enterprise.control.placement import PlacementConfig, ResidencyPolicyViolation
from fabric_shortcut_proxy.observability.logging import configure_logging
from fabric_shortcut_proxy.security.operator_auth import ManagerAuthMiddleware


class CreateDatasetRequest(BaseModel):
    dataset_id: str
    owner_id: str
    owner_location: str
    lease_ms: int = Field(gt=0)
    freshness_target_ms: int = Field(gt=0)


class HandoverRequest(BaseModel):
    owner_id: str
    fence: int = Field(gt=0)
    new_owner_id: str
    new_location: str
    lease_ms: int = Field(gt=0)


class TakeoverRequest(BaseModel):
    new_owner_id: str
    new_location: str
    lease_ms: int = Field(gt=0)


class RenewRequest(BaseModel):
    owner_id: str
    fence: int = Field(gt=0)
    lease_ms: int = Field(gt=0)


def create_catalog_app(
    *,
    store=None,
    placement: PlacementConfig | None = None,
) -> FastAPI:
    if store is None:
        from fabric_shortcut_proxy.runtime.artifact_store import get_default_store

        store = get_default_store()
    catalog = GlobalCatalog(store, placement or PlacementConfig.from_environment())

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI):
        configure_logging()
        catalog.list_records()
        yield

    app = FastAPI(
        title="Fabric Shortcut Proxy: Global Catalog",
        version="3.1.0",
        lifespan=lifespan,
    )
    app.state.catalog = catalog
    app.add_middleware(
        ManagerAuthMiddleware,
        operator_only=True,
        operator_prefixes=("/catalog",),
    )

    @app.exception_handler(CatalogConflict)
    async def catalog_conflict(_request, exc: CatalogConflict):
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.exception_handler(ResidencyPolicyViolation)
    async def residency_violation(_request, exc: ResidencyPolicyViolation):
        return JSONResponse(
            {"detail": str(exc), "code": "residency_policy_violation"},
            status_code=422,
        )

    @app.exception_handler(ValueError)
    async def invalid_request(_request, exc: ValueError):
        return JSONResponse({"detail": str(exc)}, status_code=422)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok", "role": "global-catalog"}

    @app.get("/readyz")
    async def readyz():
        records = catalog.list_records()
        return {"status": "ready", "datasets": len(records)}

    @app.get("/catalog/datasets")
    async def list_datasets():
        return {"datasets": catalog.list_records()}

    @app.post("/catalog/datasets", status_code=201)
    async def create_dataset(request: CreateDatasetRequest):
        return catalog.create(**request.model_dump())

    @app.get("/catalog/datasets/{dataset_id}")
    async def get_dataset(dataset_id: str):
        return catalog.get(dataset_id)

    @app.post("/catalog/datasets/{dataset_id}/handover")
    async def handover(dataset_id: str, request: HandoverRequest):
        return catalog.handover(dataset_id, **request.model_dump())

    @app.post("/catalog/datasets/{dataset_id}/takeover")
    async def takeover(dataset_id: str, request: TakeoverRequest):
        return catalog.takeover(dataset_id, **request.model_dump())

    @app.post("/catalog/datasets/{dataset_id}/renew")
    async def renew(dataset_id: str, request: RenewRequest):
        return catalog.renew(dataset_id, **request.model_dump())

    @app.get("/catalog/datasets/{dataset_id}/resolve")
    async def resolve(dataset_id: str, endpoint_id: str = Query(min_length=1)):
        return catalog.resolve(dataset_id, endpoint_id)

    return app


app = create_catalog_app()
