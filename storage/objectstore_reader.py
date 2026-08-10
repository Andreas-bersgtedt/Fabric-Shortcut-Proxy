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


def _azure_table_uri(mount: "Mount", subpath: str) -> str:
    """``az://<container>/<prefix><subpath>`` table root (no trailing slash)."""
    prefix = (mount.prefix or "").replace("\\", "/").strip("/")
    sub = (subpath or "").replace("\\", "/").strip("/")
    key = "/".join(p for p in (prefix, sub) if p)
    return f"az://{mount.root}/{key}".rstrip("/")


def _parse_azure_connection_string(cs: str) -> dict:
    out: dict[str, str] = {}
    for part in cs.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _azure_storage_options(mount: "Mount", *, store=None) -> dict:
    """Map an ADLS Gen2 / Blob mount's credential into delta-rs storage options.

    Covers the secret-based modes (account key, SAS, connection string, service
    principal). ``managed_identity`` / ``default`` / ``anonymous`` fail closed with
    a clear message — object_store's ambient-credential handling isn't wired here
    yet. Secrets are read from the credential store and never logged.
    """
    from storage.azure_auth import options_from_mount, resolve_azure_auth

    auth = resolve_azure_auth(mount, store=store)
    opts = options_from_mount(mount)
    options: dict[str, str] = {}
    account = opts.account

    if auth.mode == "connection_string":
        parsed = _parse_azure_connection_string(auth.connection_string)
        account = account or parsed.get("AccountName", "")
        if not account:
            raise ObjectStoreReaderUnavailable("azure connection_string is missing AccountName")
        options["AZURE_STORAGE_ACCOUNT_NAME"] = account
        if parsed.get("AccountKey"):
            options["AZURE_STORAGE_ACCOUNT_KEY"] = parsed["AccountKey"]
        elif parsed.get("SharedAccessSignature"):
            options["AZURE_STORAGE_SAS_KEY"] = parsed["SharedAccessSignature"].lstrip("?")
        else:
            raise ObjectStoreReaderUnavailable(
                "azure connection_string must carry an AccountKey or SharedAccessSignature"
            )
    else:
        if not account:
            raise ObjectStoreReaderUnavailable(
                "azure Delta reader needs the storage account name (mount 'account')"
            )
        options["AZURE_STORAGE_ACCOUNT_NAME"] = account
        if auth.mode == "account_key":
            options["AZURE_STORAGE_ACCOUNT_KEY"] = auth.account_key
        elif auth.mode == "sas":
            options["AZURE_STORAGE_SAS_KEY"] = auth.sas_token
        elif auth.mode == "aad_client_secret":
            options["AZURE_STORAGE_CLIENT_ID"] = auth.client_id
            options["AZURE_STORAGE_CLIENT_SECRET"] = auth.client_secret
            options["AZURE_STORAGE_TENANT_ID"] = auth.tenant_id
        else:
            raise ObjectStoreReaderUnavailable(
                f"azure Delta reader does not support {auth.mode!r} auth yet; use "
                f"account_key, sas, connection_string, or aad_client_secret"
            )
    # Azurite / sovereign clouds: an explicit account URL overrides the default host.
    if opts.account_url:
        options["AZURE_STORAGE_ENDPOINT"] = opts.account_url.rstrip("/")
    return options


def reader_for_mount(mount: "Mount", *, subpath: str = "") -> ObjectStoreTableReader:
    """Build the reader for a transforming mount's declared table ``format``."""
    fmt = (getattr(mount, "format", "") or "").strip().lower()
    if fmt not in READER_BACKENDS:
        raise ValueError(f"mount {mount.bucket!r} has no tokenizing table format")
    if mount.backend not in READER_BACKENDS[fmt]:
        raise ObjectStoreReaderUnavailable(
            f"{fmt} reading for backend {mount.backend!r} is not wired yet; "
            f"supported backends: {list(READER_BACKENDS[fmt])}"
        )
    if fmt == "delta":
        if mount.backend == "local":
            return DeltaTableReader(_local_table_path(mount, subpath))
        if mount.backend == "s3":
            return DeltaTableReader(_s3_table_uri(mount, subpath),
                                    storage_options=_s3_storage_options(mount))
        return DeltaTableReader(_azure_table_uri(mount, subpath),
                                storage_options=_azure_storage_options(mount))
    # iceberg (local only for now)
    return IcebergTableReader(_discover_iceberg_metadata(_local_table_path(mount, subpath)))


# Backends the reader can serve per format today; drives the Config Builder's
# "not wired yet" gating and the capability surfaced on /api/mounts.
READER_BACKENDS: dict[str, tuple[str, ...]] = {
    "delta": ("local", "s3", "azure"),
    "iceberg": ("local",),
}


def reader_backend_support() -> dict[str, list[str]]:
    return {fmt: list(backends) for fmt, backends in READER_BACKENDS.items()}
