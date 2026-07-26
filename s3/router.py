"""
S3-compatible API router.

Implements the minimal subset of the S3 REST API that Fabric needs to read
an Iceberg table through a shortcut:

  GET  /{bucket}?list-type=2&prefix=...  → ListObjectsV2
  GET  /{bucket}/{key+}                  → GetObject  (with Range support)
  HEAD /{bucket}/{key+}                  → HeadObject

Authentication: The proxy accepts any request regardless of the SigV4
credentials sent; signature verification is out of scope for the POC.

Object resolution logic
-----------------------
  metadata.json             → served from iceberg.metadata
  manifest list  (*.avro)   → served from iceberg.manifest
  manifest file  (*.avro)   → served from iceberg.manifest
  data/*.parquet            → SQL pushdown → Parquet generation
"""
from __future__ import annotations

import asyncio
import hashlib
import time

from fastapi import APIRouter, Request
from fastapi.responses import Response as FastAPIResponse

import config
import cache.lru_cache as cache
from iceberg.manifest import build_manifest_file, build_manifest_list
from iceberg.metadata import build_metadata_json
from iceberg.state_store import (
    active_to_legacy_key,
    alias_to_active_key,
    get_all_snapshots,
    get_split_by_key,
)
from db.executor import execute_split_query, SourceUnavailable
from parquet.generator import rows_to_parquet
from planner.split_planner import build_split_query
from s3.xml_responses import error_response, list_buckets_response, list_objects_v2_response
from observability.logging import get_logger
from observability import metrics
from observability import querystats

log = get_logger(__name__)

router = APIRouter()

# H4 — bound simultaneous on-demand Parquet generations to protect CPU/memory.
_generation_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_GENERATIONS)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _object_etag(data: bytes) -> str:
    return f'"{hashlib.md5(data, usedforsecurity=False).hexdigest()}"'


def _warehouse_alias_enabled() -> bool:
    return config.ENABLE_LEGACY_PATH_ALIASES and not config.WAREHOUSE_PREFIX.startswith("warehouse/")


def _normalize_incoming_key(key: str) -> str:
    """Map legacy aliases to active object keys."""
    k = alias_to_active_key(key)
    if _warehouse_alias_enabled() and k.startswith("warehouse/"):
        k = k[len("warehouse/"):]
    return k


def _normalize_incoming_prefix(prefix: str) -> tuple[str, bool]:
    """Map legacy list prefixes to active prefixes.

    Returns (normalized_prefix, requested_warehouse_alias).
    """
    requested_warehouse_alias = _warehouse_alias_enabled() and prefix.startswith("warehouse/")
    p = prefix[len("warehouse/"):] if requested_warehouse_alias else prefix
    p = alias_to_active_key(p)
    return p, requested_warehouse_alias


def _display_key_for_prefix(key: str, *, warehouse_alias: bool) -> str:
    if warehouse_alias and not key.startswith("warehouse/"):
        return f"warehouse/{key}"
    return key


def _apply_range(data: bytes, range_header: str | None) -> tuple[bytes, int, int, bool]:
    """
    Parse an HTTP Range header and return (slice, start, end, is_partial).

    Supports the three RFC 7233 single-range forms:
      - ``bytes=start-end``  explicit range
      - ``bytes=start-``     from start to end of object
      - ``bytes=-suffix``    the LAST ``suffix`` bytes (used by Parquet readers
                             to fetch the footer; must NOT be treated as 0-suffix)
    Returns the full data when range_header is None or unparseable.
    """
    total = len(data)
    if not range_header or not range_header.startswith("bytes="):
        return data, 0, total - 1, False
    try:
        spec = range_header[len("bytes="):].strip()
        # Only the first range of a multi-range request is honoured.
        if "," in spec:
            spec = spec.split(",", 1)[0].strip()
        start_s, end_s = spec.split("-", 1)
        if start_s == "":
            # Suffix range: bytes=-N  ->  last N bytes of the object.
            n = int(end_s)
            if n <= 0:
                return data, 0, total - 1, False
            n = min(n, total)
            start = total - n
            end = total - 1
        else:
            start = int(start_s)
            end = int(end_s) if end_s else total - 1
            end = min(end, total - 1)
        if start < 0 or start > end:
            return data, 0, total - 1, False
        return data[start : end + 1], start, end, True
    except (ValueError, IndexError):
        return data, 0, total - 1, False


def _range_is_unsatisfiable(range_header: str | None, total: int) -> bool:
    """True if an explicit byte range starts at/after the end of the object.

    Matches AWS S3, which returns ``416 Range Not Satisfiable`` for such ranges.
    Suffix ranges (``bytes=-N``) and open/absent ranges are always satisfiable
    here (the suffix form is clamped in ``_apply_range``).
    """
    if not range_header or not range_header.startswith("bytes="):
        return False
    spec = range_header[len("bytes="):].strip()
    if "," in spec:
        spec = spec.split(",", 1)[0].strip()
    if "-" not in spec:
        return False
    start_s, _, _end_s = spec.partition("-")
    if start_s == "":
        return False
    try:
        start = int(start_s)
    except ValueError:
        return False
    return start >= total


def _make_object_response(
    data: bytes,
    content_type: str,
    range_header: str | None,
    extra_headers: dict | None = None,
) -> FastAPIResponse:
    if _range_is_unsatisfiable(range_header, len(data)):
        return FastAPIResponse(
            status_code=416,
            headers={
                "Content-Range": f"bytes */{len(data)}",
                "Accept-Ranges": "bytes",
            },
        )
    sliced, start, end, is_partial = _apply_range(data, range_header)
    status = 206 if is_partial else 200
    metrics.record_bytes_served(len(sliced))
    headers = {
        "Content-Type": content_type,
        "Content-Length": str(len(sliced)),
        "ETag": _object_etag(data),
        "Accept-Ranges": "bytes",
        **(extra_headers or {}),
    }
    if is_partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{len(data)}"
    return FastAPIResponse(content=sliced, status_code=status, headers=headers)


# ---------------------------------------------------------------------------
# Snapshot object registry – returns known objects keyed by their S3 key.
# ---------------------------------------------------------------------------

def _objects_for_snapshot(snap) -> dict[str, dict]:
    """Return the object map for a single snapshot's virtual objects."""
    # Sizes are approximations for non-generated objects; accurate sizes
    # for metadata are filled in once bytes are built.
    meta_bytes   = snap.metadata_bytes or build_metadata_json(snap)
    mlist_bytes  = snap.manifest_list_bytes or build_manifest_list(snap)
    mfile_bytes  = snap.manifest_file_bytes or build_manifest_file(snap)

    objects: dict[str, dict] = {
        snap.metadata_key: {
            "size": len(meta_bytes),
            "last_modified_ms": snap.watermark_ms,
            "data": meta_bytes,
            "content_type": "application/json",
        },
        snap.version_hint_key: {
            "size": len(_version_hint_bytes_for(snap.version)),
            "last_modified_ms": snap.watermark_ms,
            "data": _version_hint_bytes_for(snap.version),
            "content_type": "text/plain",
        },
        snap.manifest_list_key: {
            "size": len(mlist_bytes),
            "last_modified_ms": snap.watermark_ms,
            "data": mlist_bytes,
            "content_type": "application/octet-stream",
        },
        snap.manifest_file_key: {
            "size": len(mfile_bytes),
            "last_modified_ms": snap.watermark_ms,
            "data": mfile_bytes,
            "content_type": "application/octet-stream",
        },
    }

    # Register data split entries (size is approximate until generated)
    for split in snap.splits:
        cached = cache.peek_parquet(split.object_key)
        if cached is not None:
            size = len(cached)
        elif split.file_size_in_bytes is not None:
            size = split.file_size_in_bytes
        else:
            size = 10 * 1024 * 1024  # 10 MB placeholder (only before materialization)
        objects[split.object_key] = {
            "size": size,
            "last_modified_ms": snap.watermark_ms,
            "data": None,  # generated on demand
            "content_type": "application/octet-stream",
        }

    if config.OBJECT_PATH_LAYOUT == "canonical" and config.ENABLE_LEGACY_PATH_ALIASES:
        aliases: dict[str, dict] = {}
        for k, v in objects.items():
            ak = active_to_legacy_key(snap, k)
            if ak != k:
                aliases[ak] = dict(v)
        objects.update(aliases)

    return objects


def _snapshot_objects() -> dict[str, dict]:
    """
    Return a mapping of object_key → {size, last_modified_ms} for every
    registered table's virtual objects (F1 — multi-table).
    This drives both ListObjectsV2 and HEAD responses.
    """
    if config.TABLE_FORMAT == "delta":
        from delta import log as delta_log
        return delta_log.delta_log_objects()
    objects: dict[str, dict] = {}
    # Iterate oldest -> newest so the shared version-hint.text object ends up
    # carrying the CURRENT (highest) version number.
    for snap in sorted(get_all_snapshots(), key=lambda s: s.version):
        objects.update(_objects_for_snapshot(snap))
    return objects


def _version_hint_bytes_for(version: int) -> bytes:
    return str(version).encode()


def _version_hint_bytes(key: str) -> bytes:
    """Return version-hint.text content = the current (max) version for a table."""
    versions = [s.version for s in get_all_snapshots() if s.version_hint_key == key]
    return _version_hint_bytes_for(max(versions)) if versions else b"1"


def _resolve_snapshot_for_key(key: str):
    """Return the snapshot owning a metadata/manifest key, or None."""
    key = alias_to_active_key(key)
    for snap in get_all_snapshots():
        if key in (snap.metadata_key, snap.version_hint_key,
                   snap.manifest_list_key, snap.manifest_file_key):
            return snap
    return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/")
async def list_buckets() -> FastAPIResponse:
    """Handle S3 ListBuckets — Fabric calls this first when browsing."""
    snapshots = get_all_snapshots()
    created_ms = snapshots[0].watermark_ms if snapshots else 0
    body = list_buckets_response(config.BUCKET_NAME, created_ms=created_ms)
    log.info("list_buckets", bucket=config.BUCKET_NAME)
    return FastAPIResponse(content=body, media_type="application/xml")


@router.head("/")
async def head_service() -> FastAPIResponse:
    """S3 service-level HEAD — return 200 to confirm endpoint is alive."""
    return FastAPIResponse(status_code=200)


@router.head("/{bucket}")
async def head_bucket(bucket: str) -> FastAPIResponse:
    """S3 HeadBucket — confirm the bucket exists."""
    if bucket != config.BUCKET_NAME:
        return FastAPIResponse(status_code=404)
    log.info("head_bucket", bucket=bucket)
    return FastAPIResponse(status_code=200)


@router.get("/{bucket}")
async def list_objects_v2(
    bucket: str,
    request: Request,
) -> FastAPIResponse:
    """Handle ListObjectsV2 (list-type=2) and legacy ListObjects."""
    if bucket != config.BUCKET_NAME:
        return FastAPIResponse(
            content=error_response("NoSuchBucket", f"Bucket {bucket!r} does not exist."),
            status_code=404,
            media_type="application/xml",
        )

    metrics.record_s3_request("list")
    list_type = request.query_params.get("list-type")
    prefix_in = request.query_params.get("prefix", "")
    delimiter = request.query_params.get("delimiter", "")
    prefix, warehouse_alias = _normalize_incoming_prefix(prefix_in)

    all_objects = _snapshot_objects()

    # Standard S3 delimiter semantics — behave EXACTLY like real AWS S3 (which
    # Fabric's browser is built against). Use the prefix as sent; a query for
    # `prefix=warehouse` returns CommonPrefix `warehouse/` (the folder itself),
    # while `prefix=warehouse/` returns `warehouse/db/`. This one-level-at-a-time
    # descent is exactly what the folder browser expects.
    matched_keys = [k for k in all_objects if k.startswith(prefix)]

    if delimiter:
        flat_objects: list[dict] = []
        common_prefix_set: set[str] = set()

        for k in matched_keys:
            remainder = k[len(prefix):]
            delim_pos = remainder.find(delimiter)
            if delim_pos == -1:
                flat_objects.append({
                    "key": _display_key_for_prefix(k, warehouse_alias=warehouse_alias),
                    "size": all_objects[k]["size"],
                    "last_modified_ms": all_objects[k]["last_modified_ms"],
                })
            else:
                cp = prefix + remainder[: delim_pos + len(delimiter)]
                common_prefix_set.add(_display_key_for_prefix(cp, warehouse_alias=warehouse_alias))

        common_prefixes = sorted(common_prefix_set)
        log.info("list_objects", bucket=bucket, list_type=list_type, prefix=prefix_in, delimiter=delimiter,
                 matched=len(flat_objects), common_prefixes=len(common_prefixes))
        body = list_objects_v2_response(bucket, prefix_in, flat_objects,
                                        delimiter=delimiter, common_prefixes=common_prefixes)
    else:
        flat_objects = [
            {"key": _display_key_for_prefix(k, warehouse_alias=warehouse_alias), "size": all_objects[k]["size"],
             "last_modified_ms": all_objects[k]["last_modified_ms"]}
            for k in matched_keys
        ]
        log.info("list_objects", bucket=bucket, prefix=prefix_in, matched=len(flat_objects))
        body = list_objects_v2_response(bucket, prefix_in, flat_objects)
    return FastAPIResponse(content=body, media_type="application/xml")


@router.head("/{bucket}/{key:path}")
async def head_object(
    bucket: str,
    key: str,
    request: Request,
) -> FastAPIResponse:
    if bucket != config.BUCKET_NAME:
        return FastAPIResponse(status_code=404)

    metrics.record_s3_request("head", metrics.classify_key(key))
    key = _normalize_incoming_key(key)
    all_objects = _snapshot_objects()
    obj = all_objects.get(key)
    if obj is not None:
        log.info("head_object", key=key, size=obj["size"])
        return FastAPIResponse(
            status_code=200,
            headers={
                "Content-Length": str(obj["size"]),
                "Content-Type": obj["content_type"],
                "Accept-Ranges": "bytes",
            },
        )

    # No literal object at this key. Real S3 returns 404 for a HEAD on a
    # folder-like prefix (there is no object stored at `warehouse` — only under
    # `warehouse/...`). Fabric relies on this 404 to distinguish a folder prefix
    # from a file; returning 200 here makes it treat the prefix as a file and
    # breaks folder navigation. So we must 404 for anything that isn't a real key.
    # This is an expected probe, not an error — log at debug to avoid noise.
    log.debug("head_object_not_found", key=key)
    return FastAPIResponse(status_code=404)


@router.get("/{bucket}/{key:path}")
async def get_object(
    bucket: str,
    key: str,
    request: Request,
) -> FastAPIResponse:
    """
    Serve a virtual S3 object.

    Metadata / manifest objects: served from pre-built bytes.
    Data objects (*.parquet): trigger SQL pushdown → Parquet generation.

    An empty key (trailing slash on bucket, e.g. /bucket/) is treated as
    a ListObjectsV2 request so Fabric's folder browser works correctly.
    """
    if bucket != config.BUCKET_NAME:
        return FastAPIResponse(
            content=error_response("NoSuchBucket", f"Bucket {bucket!r} does not exist."),
            status_code=404,
            media_type="application/xml",
        )

    # Trailing-slash bucket URL (e.g. /bucket/?list-type=2&delimiter=/)
    # FastAPI captures these as key="" — treat as a list request.
    if key == "":
        return await list_objects_v2(bucket, request)

    metrics.record_s3_request("get", metrics.classify_key(key))
    key = _normalize_incoming_key(key)
    range_header = request.headers.get("range")
    log.info("get_object", bucket=bucket, key=key, range=range_header)

    # ---- Delta transaction log (TABLE_FORMAT=delta) ------------------------
    if config.TABLE_FORMAT == "delta" and "/_delta_log/" in key:
        from delta import log as delta_log
        commit = delta_log.get_commit_bytes(key)
        if commit is not None:
            return _make_object_response(commit, "application/json", range_header)
        # A _last_checkpoint probe or unknown log file -> 404 (expected).
        log.debug("delta_log_not_found", key=key)
        return FastAPIResponse(
            content=error_response("NoSuchKey", f"Key {key!r} does not exist.", f"/{bucket}/{key}"),
            status_code=404,
            media_type="application/xml",
        )

    # ---- Metadata objects (pre-built bytes; Iceberg only) ------------------
    snap = None if config.TABLE_FORMAT == "delta" else _resolve_snapshot_for_key(key)
    if snap is not None:
        if key == snap.metadata_key:
            cached = cache.get_metadata(key)
            if cached is None:
                cached = build_metadata_json(snap)
                cache.put_metadata(key, cached)
            return _make_object_response(cached, "application/json", range_header)

        if key == snap.version_hint_key:
            return _make_object_response(_version_hint_bytes(key), "text/plain", range_header)

        if key == snap.manifest_list_key:
            cached = cache.get_metadata(key)
            if cached is None:
                cached = build_manifest_list(snap)
                cache.put_metadata(key, cached)
            return _make_object_response(cached, "application/octet-stream", range_header)

        if key == snap.manifest_file_key:
            cached = cache.get_metadata(key)
            if cached is None:
                cached = build_manifest_file(snap)
                cache.put_metadata(key, cached)
            return _make_object_response(cached, "application/octet-stream", range_header)

    # ---- Data objects (Parquet on demand) ----------------------------------
    split = get_split_by_key(key)
    if split is None:
        # A key ending in "/" is a folder-style probe (expected); anything else
        # is an unexpected miss worth surfacing.
        if key.endswith("/"):
            log.debug("get_object_not_found", key=key)
        else:
            log.warning("get_object_not_found", key=key)
        return FastAPIResponse(
            content=error_response("NoSuchKey", f"Key {key!r} does not exist.", f"/{bucket}/{key}"),
            status_code=404,
            media_type="application/xml",
        )

    _t_data0 = time.perf_counter()
    # Check Parquet cache
    cached_parquet = cache.get_parquet(key)
    if cached_parquet is not None:
        log.info("parquet_cache_hit", key=key)
        querystats.record_query(
            table=split.table.name, split_index=split.split_index,
            sql_ms=0.0, gen_ms=0.0, total_ms=(time.perf_counter() - _t_data0) * 1000.0,
            rows=split.record_count, resp_bytes=len(cached_parquet), cache_hit=True,
        )
        return _make_object_response(cached_parquet, "application/octet-stream", range_header)

    # SQL pushdown → Parquet (bounded concurrency to protect CPU/memory).
    try:
        async with _generation_semaphore:
            sql, params = build_split_query(split)
            _t_sql0 = time.perf_counter()
            rows = await execute_split_query(sql, params, split_index=split.split_index)
            _sql_ms = (time.perf_counter() - _t_sql0) * 1000.0
            _t_gen0 = time.perf_counter()
            parquet_bytes = rows_to_parquet(
                rows, split_index=split.split_index, columns=split.table.schema
            )
            _gen_ms = (time.perf_counter() - _t_gen0) * 1000.0
    except SourceUnavailable as exc:
        log.error("source_unavailable", key=key, error=str(exc))
        metrics.inc_counter("source_unavailable_total")
        return FastAPIResponse(
            content=error_response(
                "ServiceUnavailable",
                "The source database is temporarily unavailable; please retry.",
                f"/{bucket}/{key}",
            ),
            status_code=503,
            media_type="application/xml",
            headers={"Retry-After": "5"},
        )

    metrics.inc_counter("parquet_generations_total")
    cache.put_parquet(key, parquet_bytes)
    querystats.record_query(
        table=split.table.name, split_index=split.split_index,
        sql_ms=_sql_ms, gen_ms=_gen_ms, total_ms=(time.perf_counter() - _t_data0) * 1000.0,
        rows=len(rows), resp_bytes=len(parquet_bytes), cache_hit=False,
    )
    return _make_object_response(parquet_bytes, "application/octet-stream", range_header)
