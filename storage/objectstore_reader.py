"""
Object-store table readers for tokenizing mounts (issue #12).

Reads an existing Delta (Phase 1) or Iceberg (Phase 2) table from a mount and
yields Arrow ``RecordBatch`` chunks, which the transforming mount feeds through
``storage/tokenizer.py`` before re-encoding to Parquet. This is the ingestion half
of the object-store tokenizer; it lives entirely in the storage subsystem and does
not touch the relational SQL->Iceberg/Delta engine.

``deltalake`` (delta-rs) and ``pyiceberg`` are imported lazily, so importing this
module is safe without the ``objectstore`` extra installed; a clear
``ObjectStoreReaderUnavailable`` is raised only when a reader is actually used.
"""
from __future__ import annotations

import os
from typing import TYPE_CHECKING, Iterator, Protocol

if TYPE_CHECKING:
    import pyarrow as pa
    from storage.mounts import Mount


class ObjectStoreReaderUnavailable(RuntimeError):
    """A required reader dependency is missing or the backend isn't supported yet."""


class ObjectStoreTableReader(Protocol):
    def schema(self) -> "pa.Schema": ...
    def read_batches(self, *, batch_rows: int) -> "Iterator[pa.RecordBatch]": ...


class DeltaTableReader:
    """Read a Delta Lake table as Arrow batches via delta-rs."""

    def __init__(self, uri: str, *, storage_options: dict | None = None) -> None:
        self._uri = uri
        self._storage_options = storage_options or None
        self._table = None

    def _open(self):
        if self._table is None:
            try:
                from deltalake import DeltaTable
            except ImportError as exc:  # pragma: no cover - exercised only without the extra
                raise ObjectStoreReaderUnavailable(
                    "reading Delta tables needs the 'objectstore' extra "
                    "(pip install 'fabric-shortcut-proxy[objectstore]')"
                ) from exc
            self._table = DeltaTable(self._uri, storage_options=self._storage_options)
        return self._table

    def schema(self) -> "pa.Schema":
        return self._open().schema().to_pyarrow()

    def read_batches(self, *, batch_rows: int) -> "Iterator[pa.RecordBatch]":
        dataset = self._open().to_pyarrow_dataset()
        yield from dataset.to_batches(batch_size=batch_rows)


def _local_table_path(mount: "Mount", subpath: str) -> str:
    """Filesystem path of the table root inside a local mount (prefix-confined)."""
    prefix = (mount.prefix or "").replace("\\", "/").strip("/")
    sub = (subpath or "").replace("\\", "/").strip("/")
    if ".." in prefix.split("/") or ".." in sub.split("/"):
        raise ValueError("mount path must not contain '..'")
    return os.path.join(mount.root, *[p for p in (prefix, sub) if p])


def reader_for_mount(mount: "Mount", *, subpath: str = "") -> ObjectStoreTableReader:
    """Build the reader for a transforming mount's declared table ``format``."""
    fmt = (getattr(mount, "format", "") or "").strip().lower()
    if fmt == "delta":
        if mount.backend == "local":
            return DeltaTableReader(_local_table_path(mount, subpath))
        # delta-rs storage_options for s3/azure reuse the mount's credential store
        # wiring; that mapping lands in the next increment.
        raise ObjectStoreReaderUnavailable(
            f"Delta reading for backend {mount.backend!r} is not wired yet; "
            f"local mounts are supported in Phase 1"
        )
    if fmt == "iceberg":
        raise ObjectStoreReaderUnavailable("the Iceberg reader lands in Phase 2")
    raise ValueError(f"mount {mount.bucket!r} has no tokenizing table format")
