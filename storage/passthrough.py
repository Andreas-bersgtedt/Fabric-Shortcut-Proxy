"""
Passthrough S3 serving for mounted buckets (devplan/StorageProxy.md, Phase 1).

Given a :class:`~storage.mounts.Mount`, serve List / HEAD / GET straight from the
backend as byte passthrough — no SQL, Parquet, or Iceberg. GET streams the object
in chunks (``get_stream``) and honours HTTP Range, so multi-GB files are never
fully buffered. Read-only: keys are confined to the mount's ``prefix`` subtree and
the backend rejects ``..`` traversal.
"""
from __future__ import annotations

import email.utils
import mimetypes
from urllib.parse import unquote

from fastapi import Request
from fastapi.responses import Response, StreamingResponse

from runtime.artifact_store import ObjectNotFound
from s3.xml_responses import error_response, list_objects_v2_response
from storage.mounts import Mount, backend_for
from observability import metrics
from observability import audit
from observability.logging import get_logger

log = get_logger(__name__)

_STREAM_CHUNK = 1 << 20


def _identity(request: Request) -> tuple[str, str]:
    """Authenticated access-key id + client ip from the request (set by the auth middleware)."""
    ident = getattr(getattr(request, "state", None), "identity", "") or "-"
    client = request.client.host if getattr(request, "client", None) else ""
    return ident, client


def _content_type(key: str) -> str:
    ctype, _ = mimetypes.guess_type(key)
    return ctype or "application/octet-stream"


def _http_date(mtime_ms: int | None) -> str | None:
    if not mtime_ms:
        return None
    return email.utils.formatdate(mtime_ms / 1000.0, usegmt=True)


def _backend_key(mount: Mount, key: str) -> str:
    """Map a bucket-relative S3 key to the backend key (prefix-confined)."""
    return mount.prefix + unquote(key).replace("\\", "/").lstrip("/")


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def head_bucket(mount: Mount) -> Response:
    if getattr(mount, "format", ""):
        from storage import tokenizing_mount
        return tokenizing_mount.head_bucket(mount)
    return Response(status_code=200)


def list_objects(mount: Mount, request: Request, store=None) -> Response:
    if getattr(mount, "format", ""):
        from storage import tokenizing_mount
        return tokenizing_mount.list_objects(mount, request)
    metrics.record_s3_request("list")
    store = store or backend_for(mount)
    s3_prefix = request.query_params.get("prefix", "")
    delimiter = request.query_params.get("delimiter", "")
    plen = len(mount.prefix)

    flat: list[dict] = []
    common: set[str] = set()

    if delimiter == "/":
        # Folder browse: enumerate ONE directory level (os.scandir), never a
        # recursive walk of the whole share — that would hang on a large mount.
        full = mount.prefix + s3_prefix.replace("\\", "/").lstrip("/")
        if full == "" or full.endswith("/"):
            dirpath, leaf = full, ""
        else:
            cut = full.rfind("/")
            dirpath = full[:cut + 1] if cut >= 0 else ""
            leaf = full[cut + 1:] if cut >= 0 else full
        try:
            entries = store.list_dir(dirpath)
        except ValueError:
            entries = []
        for name, is_dir, size, mtime in entries:
            if leaf and not name.startswith(leaf):
                continue
            full_key = dirpath + name
            bkey = full_key[plen:] if full_key.startswith(mount.prefix) else full_key
            if is_dir:
                common.add(bkey + "/")
            else:
                flat.append({"key": bkey, "size": size, "last_modified_ms": mtime or 0})
    else:
        # Flat listing (no delimiter): recursive — rare for Fabric's browser.
        backend_prefix = mount.prefix + s3_prefix.replace("\\", "/").lstrip("/")
        for st in store.list(backend_prefix):
            bkey = st.key[plen:] if st.key.startswith(mount.prefix) else st.key
            flat.append({"key": bkey, "size": st.size, "last_modified_ms": st.mtime_ms or 0})

    log.info("passthrough_list", bucket=mount.bucket, prefix=s3_prefix,
             delimiter=delimiter, matched=len(flat), common_prefixes=len(common))
    ident, client = _identity(request)
    audit.record(identity=ident, client=client, bucket=mount.bucket, key=s3_prefix,
                 backend=mount.backend, method="GET", action="list", status=200)
    body = list_objects_v2_response(mount.bucket, s3_prefix, flat,
                                    delimiter=delimiter, common_prefixes=sorted(common))
    return Response(content=body, media_type="application/xml")


def head_object(mount: Mount, key: str, request: Request, store=None) -> Response:
    if getattr(mount, "format", ""):
        from storage import tokenizing_mount
        return tokenizing_mount.head_object(mount, key, request)
    metrics.record_s3_request("head", metrics.classify_key(key))
    store = store or backend_for(mount)
    ident, client = _identity(request)
    try:
        stat = store.head(_backend_key(mount, key))
    except ValueError:
        log.warning("passthrough_confinement_block", bucket=mount.bucket, key=key)
        audit.record(identity=ident, client=client, bucket=mount.bucket, key=key,
                     backend=mount.backend, method="HEAD", action="head", status=403,
                     reason="confinement")
        return Response(status_code=403)
    if stat is None:
        audit.record(identity=ident, client=client, bucket=mount.bucket, key=key,
                     backend=mount.backend, method="HEAD", action="head", status=404)
        return Response(status_code=404)
    headers = {
        "Content-Length": str(stat.size),
        "Content-Type": _content_type(key),
        "Accept-Ranges": "bytes",
    }
    lm = _http_date(stat.mtime_ms)
    if lm:
        headers["Last-Modified"] = lm
    audit.record(identity=ident, client=client, bucket=mount.bucket, key=key,
                 backend=mount.backend, method="HEAD", action="head", status=200,
                 bytes_=stat.size)
    return Response(status_code=200, headers=headers)


def _parse_range(range_header: str | None, total: int):
    """Return ``(offset, length, start, end, partial)`` for a single HTTP range.

    Supports ``bytes=start-end`` / ``bytes=start-`` / ``bytes=-suffix``. Returns
    ``None`` when the range is present but unsatisfiable (=> 416).
    """
    if not range_header or not range_header.startswith("bytes="):
        return 0, None, 0, total - 1, False
    spec = range_header[len("bytes="):].strip()
    if "," in spec:
        spec = spec.split(",", 1)[0].strip()
    try:
        start_s, end_s = spec.split("-", 1)
        if start_s == "":
            n = int(end_s)
            if n <= 0:
                return 0, None, 0, total - 1, False
            n = min(n, total)
            start, end = total - n, total - 1
        else:
            start = int(start_s)
            end = int(end_s) if end_s else total - 1
            end = min(end, total - 1)
        if start >= total:
            return None                     # unsatisfiable -> 416
        if start < 0 or start > end:
            return 0, None, 0, total - 1, False
        return start, end - start + 1, start, end, True
    except (ValueError, IndexError):
        return 0, None, 0, total - 1, False


def get_object(mount: Mount, key: str, request: Request, store=None) -> Response:
    if getattr(mount, "format", ""):
        from storage import tokenizing_mount
        return tokenizing_mount.get_object(mount, key, request)
    if key == "":
        return list_objects(mount, request, store=store)
    metrics.record_s3_request("get", metrics.classify_key(key))
    store = store or backend_for(mount)
    ident, client = _identity(request)
    bkey = _backend_key(mount, key)
    try:
        stat = store.head(bkey)
    except ValueError:
        log.warning("passthrough_confinement_block", bucket=mount.bucket, key=key)
        audit.record(identity=ident, client=client, bucket=mount.bucket, key=key,
                     backend=mount.backend, method="GET", action="get", status=403,
                     reason="confinement")
        return Response(
            content=error_response("AccessDenied", "Key escapes the mount root.", f"/{mount.bucket}/{key}"),
            status_code=403, media_type="application/xml")
    if stat is None:
        log.debug("passthrough_not_found", bucket=mount.bucket, key=key)
        audit.record(identity=ident, client=client, bucket=mount.bucket, key=key,
                     backend=mount.backend, method="GET", action="get", status=404)
        return Response(
            content=error_response("NoSuchKey", f"Key {key!r} does not exist.", f"/{mount.bucket}/{key}"),
            status_code=404, media_type="application/xml")

    total = stat.size
    parsed = _parse_range(request.headers.get("range"), total)
    if parsed is None:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{total}",
                                                  "Accept-Ranges": "bytes"})
    offset, length, start, end, partial = parsed
    sent = length if length is not None else total - offset

    headers = {
        "Content-Type": _content_type(key),
        "Content-Length": str(sent),
        "Accept-Ranges": "bytes",
    }
    lm = _http_date(stat.mtime_ms)
    if lm:
        headers["Last-Modified"] = lm
    if partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{total}"
    metrics.record_bytes_served(sent)

    def _body():
        try:
            yield from store.get_stream(bkey, offset=offset, length=length, chunk_size=_STREAM_CHUNK)
        except ObjectNotFound:
            return                          # raced deletion; connection ends

    log.info("passthrough_get", bucket=mount.bucket, key=key, bytes=sent, partial=partial)
    audit.record(identity=ident, client=client, bucket=mount.bucket, key=key,
                 backend=mount.backend, method="GET", action="get",
                 status=206 if partial else 200, bytes_=sent)
    return StreamingResponse(_body(), status_code=206 if partial else 200, headers=headers)
