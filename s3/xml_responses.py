"""
S3-compatible XML response builders.

Returns well-formed XML bodies matching the AWS S3 REST API wire format
so Fabric's S3 client interprets them correctly.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _modified(ts_ms: int | None = None) -> str:
    if ts_ms:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return _iso_now()


def list_buckets_response(bucket_name: str, created_ms: int | None = None,
                          *, extra_buckets: list[str] | None = None) -> bytes:
    root = ET.Element("ListAllMyBucketsResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    owner = ET.SubElement(root, "Owner")
    ET.SubElement(owner, "ID").text = "poc-owner"
    ET.SubElement(owner, "DisplayName").text = "poc-owner"
    buckets_el = ET.SubElement(root, "Buckets")
    for name in [bucket_name, *(extra_buckets or [])]:
        bucket_el = ET.SubElement(buckets_el, "Bucket")
        ET.SubElement(bucket_el, "Name").text = name
        ET.SubElement(bucket_el, "CreationDate").text = _modified(created_ms)
    return b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode").encode()


def list_objects_v2_response(
    bucket: str,
    prefix: str,
    objects: list[dict],   # [{"key": str, "size": int, "last_modified_ms": int}]
    *,
    delimiter: str = "",
    common_prefixes: list[str] | None = None,
    max_keys: int = 1000,
    is_truncated: bool = False,
    next_continuation_token: str | None = None,
) -> bytes:
    """
    Build a ListObjectsV2 XML response body.

    `objects` is the flat list of matched object descriptors.
    `common_prefixes` is the list of virtual directory prefixes (if delimiter used).
    `max_keys` / `is_truncated` / `next_continuation_token` carry S3 pagination;
    the defaults reproduce a single, complete (non-truncated) page.
    """
    root = ET.Element("ListBucketResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")

    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix
    # KeyCount matches AWS S3: number of keys returned = Contents + CommonPrefixes.
    ET.SubElement(root, "KeyCount").text = str(len(objects) + len(common_prefixes or []))
    ET.SubElement(root, "MaxKeys").text = str(max_keys)
    ET.SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"
    if next_continuation_token:
        ET.SubElement(root, "NextContinuationToken").text = next_continuation_token
    if delimiter:
        ET.SubElement(root, "Delimiter").text = delimiter

    for obj in objects:
        contents = ET.SubElement(root, "Contents")
        ET.SubElement(contents, "Key").text = obj["key"]
        ET.SubElement(contents, "LastModified").text = _modified(obj.get("last_modified_ms"))
        ET.SubElement(contents, "ETag").text = f'"{_etag(obj["key"])}"'
        ET.SubElement(contents, "Size").text = str(obj["size"])
        ET.SubElement(contents, "StorageClass").text = "STANDARD"

    for cp in (common_prefixes or []):
        cp_el = ET.SubElement(root, "CommonPrefixes")
        ET.SubElement(cp_el, "Prefix").text = cp

    return b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode").encode()


def error_response(code: str, message: str, resource: str = "/") -> bytes:
    """Build an S3-style ``<Error>`` body with a unique RequestId and HostId."""
    import uuid

    root = ET.Element("Error")
    ET.SubElement(root, "Code").text = code
    ET.SubElement(root, "Message").text = message
    ET.SubElement(root, "Resource").text = resource
    ET.SubElement(root, "RequestId").text = uuid.uuid4().hex
    ET.SubElement(root, "HostId").text = uuid.uuid4().hex
    return b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode").encode()


def _etag(key: str) -> str:
    """Stable pseudo-ETag derived from the key (not cryptographically meaningful)."""
    import hashlib
    return hashlib.md5(key.encode(), usedforsecurity=False).hexdigest()
