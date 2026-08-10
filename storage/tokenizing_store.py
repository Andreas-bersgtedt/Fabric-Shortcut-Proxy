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


def _resolve_output_format(mount: "Mount") -> str:
    """Serve format for the tokenized copy: unset/"delta" -> Delta (safe default),
    "iceberg" -> Iceberg, "auto" -> mirror the source format."""
    of = (getattr(mount, "output_format", "") or "").strip().lower()
    if of == "auto":
        return (mount.format or "delta").strip().lower()
    if of in ("delta", "iceberg"):
        return of
    return "delta"


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
    return {"format": mount.format, "key_column": mount.key_column,
            "output_format": _resolve_output_format(mount), "columns": columns}


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


def _paths(mount: "Mount") -> tuple[str, str, str]:
    """Return ``(table_dir, marker_file, served_root_ptr)``. The marker and the
    served-root pointer are siblings so they never pollute the served table dir.
    The served root differs from ``table_dir`` for Iceberg output (nested warehouse)."""
    base = os.path.join(_cache_root(), mount.bucket)
    digest = _policy_hash(mount)
    return (os.path.join(base, digest),
            os.path.join(base, f"{digest}.ok"),
            os.path.join(base, f"{digest}.root"))


def _read_served_root(root_ptr: str, default: str) -> str:
    try:
        with open(root_ptr, "r", encoding="utf-8") as fh:
            value = fh.read().strip()
        return value or default
    except OSError:
        return default


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
    """Return the served table root, materializing the tokenized copy once if needed.

    For Delta output the served root is the cache dir; for Iceberg output it is the
    nested warehouse table location, recorded in a sibling ``.root`` pointer file.
    """
    table_dir, marker, root_ptr = _paths(mount)
    if os.path.exists(marker):
        return _read_served_root(root_ptr, table_dir)
    with _lock_for(table_dir):
        if os.path.exists(marker):
            return _read_served_root(root_ptr, table_dir)
        served_root = _materialize(mount, table_dir)
        with open(root_ptr, "w", encoding="utf-8") as fh:
            fh.write(served_root)
        with open(marker, "w", encoding="utf-8") as fh:
            fh.write("ok")
    return served_root


def store_for(mount: "Mount") -> LocalDirStore:
    return LocalDirStore(ensure_materialized(mount))


def _materialize(mount: "Mount", table_dir: str) -> str:
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
    output_format = _resolve_output_format(mount)
    if output_format == "iceberg":
        served = _write_iceberg(table_dir, table)
        log.info("tokenized_materialized", bucket=mount.bucket, format=mount.format,
                 output_format="iceberg", rows=total, columns=len(columns), cache_dir=served)
        return served

    from deltalake import write_deltalake
    write_deltalake(table_dir, table, mode="overwrite")
    log.info("tokenized_materialized", bucket=mount.bucket, format=mount.format,
             output_format="delta", rows=total, columns=len(columns), cache_dir=table_dir)
    return table_dir


def _write_iceberg(table_dir: str, table) -> str:
    """Write the tokenized Arrow table as an Iceberg table and return its root path."""
    import pathlib
    from pyiceberg.catalog.sql import SqlCatalog

    # fsspec FileIO handles Windows file:// paths that pyarrow FileIO mishandles.
    impl = {"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    catalog = SqlCatalog(
        "fsp",
        uri=f"sqlite:///{pathlib.Path(os.path.join(table_dir, '_fsp_catalog.db')).as_posix()}",
        warehouse=pathlib.Path(table_dir).as_uri(),
        **impl,
    )
    catalog.create_namespace("fsp")
    tbl = catalog.create_table("fsp.tokenized", schema=table.schema)
    if table.num_rows:
        tbl.append(table)
    location = tbl.location()
    served = location[len("file://"):] if location.startswith("file://") else location
    return served.lstrip("/") if os.name == "nt" else served
