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

import glob
import os
import pathlib
import re
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
        self._dataset = None

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

    def _pyarrow_dataset(self):
        # Dataset.schema is a pyarrow.Schema across delta-rs versions, unlike the
        # internal Schema object whose conversion method has been renamed.
        if self._dataset is None:
            self._dataset = self._open().to_pyarrow_dataset()
        return self._dataset

    def schema(self) -> "pa.Schema":
        return self._pyarrow_dataset().schema

    def read_batches(self, *, batch_rows: int) -> "Iterator[pa.RecordBatch]":
        yield from self._pyarrow_dataset().to_batches(batch_size=batch_rows)


def _discover_iceberg_metadata(table_root: str) -> str:
    """Locate the current Iceberg metadata JSON under a local table root.

    Honors ``metadata/version-hint.text`` when present, else picks the highest
    numeric-prefixed ``*.metadata.json`` (delta-rs-style zero-padded counters and
    ``vN`` both parse), breaking ties by mtime.
    """
    meta_dir = os.path.join(table_root, "metadata")
    hint = os.path.join(meta_dir, "version-hint.text")
    if os.path.isfile(hint):
        with open(hint, "r", encoding="utf-8") as fh:
            version = fh.read().strip()
        for name in (f"v{version}.metadata.json", f"{version}.metadata.json"):
            candidate = os.path.join(meta_dir, name)
            if os.path.isfile(candidate):
                return candidate
    candidates = glob.glob(os.path.join(meta_dir, "*.metadata.json"))
    if not candidates:
        raise ObjectStoreReaderUnavailable(
            f"no Iceberg metadata found under {meta_dir!r}"
        )

    def _version(path: str) -> int:
        match = re.match(r"[vV]?(\d+)", os.path.basename(path))
        return int(match.group(1)) if match else -1

    return max(candidates, key=lambda p: (_version(p), os.path.getmtime(p)))


class IcebergTableReader:
    """Read an Apache Iceberg table as Arrow batches via pyiceberg (metadata-file)."""

    def __init__(self, metadata_location: str) -> None:
        self._metadata_location = metadata_location
        self._table = None

    def _open(self):
        if self._table is None:
            try:
                from pyiceberg.table import StaticTable
            except ImportError as exc:  # pragma: no cover - exercised only without the extra
                raise ObjectStoreReaderUnavailable(
                    "reading Iceberg tables needs the 'objectstore' extra "
                    "(pip install 'fabric-shortcut-proxy[objectstore]')"
                ) from exc
            location = self._metadata_location
            properties: dict = {}
            if os.path.exists(location):
                # Local table: use a file URI + fsspec FileIO so Windows drive
                # letters and file:// paths resolve (pyarrow FileIO mishandles both).
                location = pathlib.Path(location).resolve().as_uri()
                properties = {"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
            self._table = StaticTable.from_metadata(location, properties=properties)
        return self._table

    def schema(self) -> "pa.Schema":
        return self._open().scan().to_arrow_batch_reader().schema

    def read_batches(self, *, batch_rows: int) -> "Iterator[pa.RecordBatch]":
        yield from self._open().scan().to_arrow_batch_reader()


def _local_table_path(mount: "Mount", subpath: str) -> str:
    """Filesystem path of the table root inside a local mount (prefix-confined)."""
    prefix = (mount.prefix or "").replace("\\", "/").strip("/")
    sub = (subpath or "").replace("\\", "/").strip("/")
    if ".." in prefix.split("/") or ".." in sub.split("/"):
        raise ValueError("mount path must not contain '..'")
    return os.path.join(mount.root, *[p for p in (prefix, sub) if p])


def _s3_table_uri(mount: "Mount", subpath: str) -> str:
    """``s3://<upstream-bucket>/<prefix><subpath>`` table root (no trailing slash)."""
    prefix = (mount.prefix or "").replace("\\", "/").strip("/")
    sub = (subpath or "").replace("\\", "/").strip("/")
    key = "/".join(p for p in (prefix, sub) if p)
    return f"s3://{mount.root}/{key}".rstrip("/")


def _s3_storage_options(mount: "Mount", *, store=None) -> dict:
    """Map a mount's S3 connection + credential into delta-rs storage options.

    Covers the common private-deployment modes (static/session keys, anonymous,
    instance/default chain). Modes that delta-rs/object_store cannot consume from
    boto3-resolved material (assume_role, web_identity, sso, profile, process) fail
    closed with a clear message. Secrets are read from the credential store and
    never logged.
    """
    from storage.s3_auth import options_from_mount, resolve_s3_auth

    auth = resolve_s3_auth(mount, store=store)
    opts = options_from_mount(mount)
    options: dict[str, str] = {"AWS_REGION": opts.region or "us-east-1"}
    if opts.endpoint:
        options["AWS_ENDPOINT_URL"] = opts.endpoint
        if opts.endpoint.lower().startswith("http://"):
            options["AWS_ALLOW_HTTP"] = "true"
    addressing = (opts.addressing_style or "").lower()
    if addressing == "path" or (opts.endpoint and addressing != "virtual"):
        options["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "false"

    if auth.mode in ("static", "session"):
        options["AWS_ACCESS_KEY_ID"] = auth.access_key
        options["AWS_SECRET_ACCESS_KEY"] = auth.secret_key
        if auth.session_token:
            options["AWS_SESSION_TOKEN"] = auth.session_token
    elif auth.mode == "anonymous":
        options["AWS_SKIP_SIGNATURE"] = "true"
    elif auth.mode == "instance":
        pass  # object_store falls back to the instance/default credential chain
    else:
        raise ObjectStoreReaderUnavailable(
            f"object-store Delta on s3 with {auth.mode!r} auth is not supported yet; "
            f"use static/session keys, 'anonymous', or 'instance'"
        )
    if opts.verify is False:
        raise ObjectStoreReaderUnavailable(
            "the object-store Delta reader cannot skip TLS verification; "
            "use a trusted certificate or an http endpoint"
        )
    return options


def reader_for_mount(mount: "Mount", *, subpath: str = "") -> ObjectStoreTableReader:
    """Build the reader for a transforming mount's declared table ``format``."""
    fmt = (getattr(mount, "format", "") or "").strip().lower()
    if fmt == "delta":
        if mount.backend == "local":
            return DeltaTableReader(_local_table_path(mount, subpath))
        if mount.backend == "s3":
            return DeltaTableReader(_s3_table_uri(mount, subpath),
                                    storage_options=_s3_storage_options(mount))
        raise ObjectStoreReaderUnavailable(
            f"Delta reading for backend {mount.backend!r} is not wired yet; "
            f"local and s3 mounts are supported"
        )
    if fmt == "iceberg":
        if mount.backend == "local":
            return IcebergTableReader(_discover_iceberg_metadata(_local_table_path(mount, subpath)))
        raise ObjectStoreReaderUnavailable(
            f"Iceberg reading for backend {mount.backend!r} is not wired yet; "
            f"local mounts are supported in Phase 2"
        )
    raise ValueError(f"mount {mount.bucket!r} has no tokenizing table format")
