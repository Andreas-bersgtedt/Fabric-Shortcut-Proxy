"""
Tokenized-copy cache store for object-store mounts (issue #12).

Materializes a *virtual, tokenized* copy of a mount's source Delta/Iceberg table
into a local cache directory (a real Delta table written by delta-rs), which the
transforming mount then serves through ordinary passthrough. The cache exists so
the read contract holds: HEAD/List/manifest must declare the exact byte size a GET
returns, and tokenized Parquet has a different, non-derivable size than the source
(see devplan/Issue_12_Object_Store_Tokenizer_Plan.md, PureVirtualization.md).

Cache key = source bucket + a policy hash over the column policy AND a one-way
fingerprint of each deterministic key, so rotating a key or changing the policy
invalidates the cache cleanly. The key value itself is never stored or logged.
This module lives in the storage subsystem and never touches the SQL engine.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
from typing import TYPE_CHECKING

import system_config
from runtime.artifact_store import LocalDirStore
from observability.logging import get_logger

if TYPE_CHECKING:
    from storage.mounts import Mount

log = get_logger(__name__)

_BATCH_ROWS = 65536


def _cache_root() -> str:
    override = os.environ.get("FSP_TOKENIZING_CACHE_DIR")
    if override:
        return override
    return os.path.join(getattr(system_config, "ARTIFACT_STORE_DIR", "./.artifacts"), "tokenized")


def _policy_document(mount: "Mount") -> dict:
    """Canonical, secret-free description of the mount's tokenization policy."""
    columns = []
    for column in mount.columns:
        transform = column.transform
        columns.append({
            "name": column.name,
            "source": column.source_name,
            "type": column.iceberg_type,
            "nullable": column.nullable,
            "transform": None if transform is None else {
                "kind": transform.kind,
                "key_ref": transform.key_ref,
                "domain": transform.domain,
                "normalization": transform.normalization,
            },
        })
    return {"format": mount.format, "key_column": mount.key_column, "columns": columns}


def _key_fingerprints(mount: "Mount") -> dict:
    """One-way fingerprints of the resolved deterministic keys (never the key)."""
    import config

    out: dict[str, str] = {}
    for column in mount.columns:
        transform = column.transform
        if transform and transform.kind == "deterministic_hash" and transform.key_ref:
            if transform.key_ref not in out:
                key = config.resolve_tokenization_key(transform.key_ref)
                out[transform.key_ref] = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return out


def _policy_hash(mount: "Mount") -> str:
    doc = {"policy": _policy_document(mount), "keys": _key_fingerprints(mount)}
    blob = json.dumps(doc, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:24]


def _paths(mount: "Mount") -> tuple[str, str]:
    """Return ``(table_dir, marker_file)``; the marker is a sibling so it never
    pollutes the served Delta table directory."""
    base = os.path.join(_cache_root(), mount.bucket)
    digest = _policy_hash(mount)
    return os.path.join(base, digest), os.path.join(base, f"{digest}.ok")


_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(path: str) -> threading.Lock:
    with _locks_guard:
        lock = _locks.get(path)
        if lock is None:
            lock = threading.Lock()
            _locks[path] = lock
        return lock


def cache_dir_for(mount: "Mount") -> str:
    return _paths(mount)[0]


def ensure_materialized(mount: "Mount") -> str:
    """Return the cache dir, materializing the tokenized table once if needed."""
    table_dir, marker = _paths(mount)
    if os.path.exists(marker):
        return table_dir
    with _lock_for(table_dir):
        if os.path.exists(marker):
            return table_dir
        _materialize(mount, table_dir)
        with open(marker, "w", encoding="utf-8") as fh:
            fh.write("ok")
    return table_dir


def store_for(mount: "Mount") -> LocalDirStore:
    return LocalDirStore(ensure_materialized(mount))


def _materialize(mount: "Mount", table_dir: str) -> None:
    import pyarrow as pa
    from storage.objectstore_reader import reader_for_mount
    from storage.tokenizer import output_arrow_schema, tokenize_batch

    reader = reader_for_mount(mount)
    columns = list(mount.columns)
    out_schema = output_arrow_schema(reader.schema(), columns)

    batches: list[pa.RecordBatch] = []
    total = 0
    for batch in reader.read_batches(batch_rows=_BATCH_ROWS):
        tokenized = tokenize_batch(batch, columns)
        batches.append(tokenized)
        total += tokenized.num_rows

    table = pa.Table.from_batches(batches).cast(out_schema) if batches else out_schema.empty_table()

    os.makedirs(table_dir, exist_ok=True)
    from deltalake import write_deltalake

    write_deltalake(table_dir, table, mode="overwrite")
    log.info("tokenized_materialized", bucket=mount.bucket, format=mount.format,
             rows=total, columns=len(columns), cache_dir=table_dir)
