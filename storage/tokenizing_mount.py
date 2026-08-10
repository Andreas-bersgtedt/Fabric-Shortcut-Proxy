"""
Transforming (tokenizing) mount serve path (issue #12).

A mount tagged with a table ``format`` is served as a virtual, read-only,
tokenized copy of its source Delta/Iceberg table. This module ensures the
tokenized copy is materialized (``storage/tokenizing_store.py``) and then serves
List/HEAD/GET straight through the existing passthrough handlers over the cache
directory — so auth, prefix confinement, HTTP Range, audit, and metrics are all
inherited unchanged. It lives entirely in the storage subsystem; the SQL engine is
never involved.
"""
from __future__ import annotations

from dataclasses import replace

from fastapi import Request
from fastapi.responses import Response

from s3.xml_responses import error_response
from storage import passthrough, tokenizing_store
from storage.mounts import Mount
from storage.objectstore_reader import ObjectStoreReaderUnavailable
from observability.logging import get_logger

log = get_logger(__name__)


def _serve_target(mount: Mount) -> tuple[Mount, object]:
    """Materialize if needed and return a plain local serve-mount + its store."""
    store = tokenizing_store.store_for(mount)
    serve_mount = replace(
        mount, backend="local", root=store.root, prefix="",
        format="", key_column="", columns=(),
    )
    return serve_mount, store


def _error_response(mount: Mount, key: str, exc: Exception) -> Response:
    if isinstance(exc, ObjectStoreReaderUnavailable):
        code, status = "NotImplemented", 501
    else:
        code, status = "ServiceUnavailable", 503
    log.warning("tokenizing_mount_error", bucket=mount.bucket, key=key,
                error=str(exc), status=status)
    return Response(
        content=error_response(code, str(exc), f"/{mount.bucket}/{key}"),
        status_code=status, media_type="application/xml",
    )


def head_bucket(mount: Mount) -> Response:
    return Response(status_code=200)


def list_objects(mount: Mount, request: Request) -> Response:
    try:
        serve_mount, store = _serve_target(mount)
    except Exception as exc:  # noqa: BLE001 - surface a clean S3 error, never a trace
        return _error_response(mount, "", exc)
    return passthrough.list_objects(serve_mount, request, store=store)


def head_object(mount: Mount, key: str, request: Request) -> Response:
    try:
        serve_mount, store = _serve_target(mount)
    except Exception as exc:  # noqa: BLE001
        return _error_response(mount, key, exc)
    return passthrough.head_object(serve_mount, key, request, store=store)


def get_object(mount: Mount, key: str, request: Request) -> Response:
    try:
        serve_mount, store = _serve_target(mount)
    except Exception as exc:  # noqa: BLE001
        return _error_response(mount, key, exc)
    return passthrough.get_object(serve_mount, key, request, store=store)
