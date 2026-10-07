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
from typing import TYPE_CHECKING, Protocol
from urllib.parse import unquote, urlsplit

if TYPE_CHECKING:
    from typing import Iterator

    import pyarrow as pa

    from fabric_shortcut_proxy.storage.mounts import Mount


class ObjectStoreReaderUnavailable(RuntimeError):
    """A required reader dependency is missing or the backend isn't supported yet."""


class ObjectStoreTableReader(Protocol):
    def schema(self) -> pa.Schema: ...
    def read_batches(self, *, batch_rows: int) -> Iterator[pa.RecordBatch]: ...


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

    def schema(self) -> pa.Schema:
        return self._pyarrow_dataset().schema

    def read_batches(self, *, batch_rows: int) -> Iterator[pa.RecordBatch]:
        yield from self._pyarrow_dataset().to_batches(batch_size=batch_rows)


def _discover_iceberg_metadata(
    table_root: str, *, scope_root: str | None = None,
) -> str:
    """Locate the current Iceberg metadata JSON under a local table root.

    Honors ``metadata/version-hint.text`` when present, else picks the highest
    numeric-prefixed ``*.metadata.json`` (delta-rs-style zero-padded counters and
    ``vN`` both parse), breaking ties by mtime.
    """
    allowed_root = os.path.realpath(scope_root or table_root)

    def _confined(path: str) -> bool:
        resolved = os.path.realpath(path)
        try:
            return os.path.commonpath((allowed_root, resolved)) == allowed_root
        except ValueError:
            return False

    meta_dir = os.path.join(table_root, "metadata")
    hint = os.path.join(meta_dir, "version-hint.text")
    if _confined(hint) and os.path.isfile(hint):
        with open(hint, "r", encoding="utf-8") as fh:
            version = fh.read().strip()
        for name in (f"v{version}.metadata.json", f"{version}.metadata.json"):
            candidate = os.path.join(meta_dir, name)
            if _confined(candidate) and os.path.isfile(candidate):
                return candidate
    candidates = [
        path for path in glob.glob(os.path.join(meta_dir, "*.metadata.json"))
        if _confined(path) and os.path.isfile(path)
    ]
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

    def __init__(
        self, metadata_location: str, *, properties: dict | None = None,
        snapshot_id: int | None = None,
    ) -> None:
        self._metadata_location = metadata_location
        self._properties = properties or {}
        self._snapshot_id = snapshot_id
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
            properties = dict(self._properties)
            if os.path.exists(location):
                # Local table: use a file URI + fsspec FileIO so Windows drive
                # letters and file:// paths resolve (pyarrow FileIO mishandles both).
                location = pathlib.Path(location).resolve().as_uri()
                if not properties:
                    properties = {"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
            self._table = StaticTable.from_metadata(location, properties=properties)
        return self._table

    def _scan(self):
        if self._snapshot_id is None:
            return self._open().scan()
        return self._open().scan(snapshot_id=self._snapshot_id)

    def schema(self) -> pa.Schema:
        return self._scan().to_arrow_batch_reader().schema

    def read_batches(self, *, batch_rows: int) -> Iterator[pa.RecordBatch]:
        yield from self._scan().to_arrow_batch_reader()


def _safe_relative_path(value: str, *, description: str) -> str:
    """Normalize a mount-relative path and reject traversal and URI inputs."""
    path = (value or "").replace("\\", "/")
    parsed = urlsplit(path)
    decoded = path
    for _ in range(3):
        decoded = unquote(decoded)
    if (
        parsed.scheme or parsed.netloc or parsed.query or parsed.fragment
        or path.startswith("/") or decoded.startswith("/")
        or "\\" in decoded or "\x00" in decoded
        or any(part in (".", "..") for part in decoded.split("/"))
    ):
        raise ValueError(f"{description} must be a confined relative path")
    return "/".join(part for part in path.split("/") if part)


def _remote_table_root(mount: Mount, subpath: str) -> str:
    prefix = _safe_relative_path(mount.prefix, description="mount prefix")
    sub = _safe_relative_path(subpath, description="table path")
    key = "/".join(part for part in (prefix, sub) if part)
    if mount.backend == "s3":
        return f"s3://{mount.root}/{key}".rstrip("/")
    return f"az://{mount.root}/{key}".rstrip("/")


def _iceberg_fileio_properties(
    mount: Mount, *, table_root: str, client=None,
) -> dict:
    return {
        "py-io-impl": (
            "fabric_shortcut_proxy.storage.iceberg_fileio.ScopedIcebergFileIO"
        ),
        "_fsp_backend": mount.backend,
        "_fsp_mount": mount,
        "_fsp_scope_root": (
            os.path.join(mount.root, *mount.prefix.strip("/").replace("\\", "/").split("/"))
            if mount.backend == "local"
            else _remote_table_root(mount, "")
        ),
        "_fsp_table_root": table_root,
        "_fsp_client": client,
    }


def _remote_client(mount: Mount, *, store=None):
    if mount.backend == "s3":
        try:
            from fabric_shortcut_proxy.storage.s3_auth import (
                build_s3_client,
                options_from_mount,
                resolve_s3_auth,
            )
            return build_s3_client(resolve_s3_auth(mount, store=store), options_from_mount(mount))
        except RuntimeError as exc:
            raise ObjectStoreReaderUnavailable(
                "remote Iceberg on S3 requires boto3; install "
                "'fabric-shortcut-proxy[s3proxy]'"
            ) from exc
    if mount.backend == "azure":
        try:
            from fabric_shortcut_proxy.storage.azure_auth import (
                build_container_client,
                options_from_mount,
                resolve_azure_auth,
            )
            auth = resolve_azure_auth(mount, store=store)
            return build_container_client(auth, options_from_mount(mount), mount.root)
        except RuntimeError as exc:
            raise ObjectStoreReaderUnavailable(
                "remote Iceberg on Azure requires azure-storage-blob; install "
                "'fabric-shortcut-proxy[azureblob]'"
            ) from exc
    raise ValueError(f"remote Iceberg client is not available for {mount.backend!r}")


def _remote_object_key(mount: Mount, location: str, *, table_root: str) -> str:
    """Resolve an Iceberg URI to a key in this mount; reject cross-scope paths."""
    parsed = urlsplit(location)
    if (
        parsed.query or parsed.fragment
        or (
            parsed.scheme.lower() not in ("abfs", "abfss")
            and (parsed.username or parsed.password)
        )
    ):
        raise ValueError("Iceberg file URI must not contain credentials, query, or fragment")

    backend = mount.backend
    if not parsed.scheme:
        if not location or location.startswith(("/", "\\")):
            raise ValueError("Iceberg relative file path is not confined")
        parent = urlsplit(table_root)
        candidate_path = f"{parent.path.rstrip('/')}/{location}"
        scheme = parent.scheme
        authority = parent.netloc
    else:
        scheme = parsed.scheme.lower()
        authority = parsed.netloc
        candidate_path = parsed.path

    if backend == "s3":
        if scheme not in ("s3", "s3a", "s3n") or authority.lower() != mount.root.lower():
            raise ValueError("Iceberg file URI is outside the configured S3 bucket")
    elif backend == "azure":
        if scheme == "az":
            if authority.lower() != mount.root.lower():
                raise ValueError("Iceberg file URI is outside the configured Azure container")
        elif scheme in ("abfs", "abfss"):
            container, separator, host = authority.partition("@")
            if authority.count("@") > 1 or ":" in container:
                raise ValueError("Iceberg abfs URI has an invalid authority")
            if not separator:
                container, host = authority, ""
            if container.lower() != mount.root.lower():
                raise ValueError("Iceberg file URI is outside the configured Azure container")
            expected_account = (mount.account or "").lower()
            actual_account = host.partition(".")[0].lower()
            if host and expected_account and actual_account != expected_account:
                raise ValueError("Iceberg file URI is outside the configured Azure account")
            if host and not expected_account:
                raise ValueError("Iceberg abfs URI requires the mount's storage account")
        else:
            raise ValueError("Iceberg file URI must use az, abfs, or abfss for Azure")
    else:
        raise ValueError(f"unsupported remote Iceberg backend {backend!r}")

    decoded_once = unquote(candidate_path)
    decoded = decoded_once
    for _ in range(3):
        decoded = unquote(decoded)
    if (
        "\\" in decoded or "\x00" in decoded
        or any(part in (".", "..") for part in decoded.split("/"))
    ):
        raise ValueError("Iceberg file URI contains path traversal or invalid characters")
    parts = [part for part in decoded_once.lstrip("/").split("/") if part]
    if any(part in (".", "..") for part in parts):
        raise ValueError("Iceberg file URI contains path traversal")

    prefix = _safe_relative_path(mount.prefix, description="mount prefix")
    if prefix and (not parts or (parts[0] != prefix.split("/", 1)[0])):
        raise ValueError("Iceberg file URI is outside the configured mount prefix")
    key = "/".join(parts)
    if prefix and not (key == prefix or key.startswith(prefix + "/")):
        raise ValueError("Iceberg file URI is outside the configured mount prefix")
    return key


def _read_remote_object(client, backend: str, *, bucket: str, key: str) -> bytes | None:
    try:
        if backend == "s3":
            response = client.get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            try:
                return body.read()
            finally:
                body.close()
        return client.get_blob_client(key).download_blob().readall()
    except Exception as exc:
        status = getattr(exc, "status_code", None)
        response = getattr(exc, "response", None)
        code = ""
        if isinstance(response, dict):
            code = str((response.get("Error") or {}).get("Code") or "")
        if status == 404 or code in ("404", "NoSuchKey", "NotFound", "BlobNotFound"):
            return None
        raise


def _discover_remote_iceberg_metadata(
    mount: Mount, subpath: str, client,
) -> tuple[str, str]:
    table_root = _remote_table_root(mount, subpath)
    table_key = _remote_object_key(mount, table_root, table_root=table_root)
    metadata_prefix = "/".join(
        part for part in (table_key.rstrip("/"), "metadata") if part
    )
    hint_key = f"{metadata_prefix}/version-hint.text"
    hint = _read_remote_object(client, mount.backend, bucket=mount.root, key=hint_key)
    if hint is not None:
        version = hint.decode("utf-8").strip()
        if version.isdigit():
            for name in (f"v{version}.metadata.json", f"{version}.metadata.json"):
                key = f"{metadata_prefix}/{name}"
                if _read_remote_object(client, mount.backend, bucket=mount.root, key=key) is not None:
                    return f"{'s3' if mount.backend == 's3' else 'az'}://{mount.root}/{key}", table_root

    candidates: list[tuple[int, float, str]] = []
    if mount.backend == "s3":
        request: dict = {"Bucket": mount.root, "Prefix": metadata_prefix + "/"}
        while True:
            page = client.list_objects_v2(**request)
            for item in page.get("Contents") or []:
                key = str(item.get("Key") or "")
                if not key.startswith(metadata_prefix + "/"):
                    continue
                suffix = key[len(metadata_prefix) + 1:]
                if "/" in suffix or not suffix.endswith(".metadata.json"):
                    continue
                match = re.match(r"[vV]?(\d+)", os.path.basename(key))
                version = int(match.group(1)) if match else -1
                modified = item.get("LastModified")
                timestamp = modified.timestamp() if modified is not None else 0.0
                candidates.append((version, timestamp, key))
            token = page.get("NextContinuationToken")
            if not page.get("IsTruncated") or not token:
                break
            request["ContinuationToken"] = token
    else:
        for item in client.list_blobs(name_starts_with=metadata_prefix + "/"):
            key = str(item.name)
            if not key.startswith(metadata_prefix + "/"):
                continue
            suffix = key[len(metadata_prefix) + 1:]
            if "/" in suffix or not suffix.endswith(".metadata.json"):
                continue
            match = re.match(r"[vV]?(\d+)", os.path.basename(key))
            version = int(match.group(1)) if match else -1
            modified = getattr(item, "last_modified", None)
            timestamp = modified.timestamp() if modified is not None else 0.0
            candidates.append((version, timestamp, key))
    if not candidates:
        raise ObjectStoreReaderUnavailable(
            f"no Iceberg metadata found under the configured {mount.backend} mount"
        )
    key = max(candidates)[2]
    scheme = "s3" if mount.backend == "s3" else "az"
    return f"{scheme}://{mount.root}/{key}", table_root


def _local_table_path(mount: Mount, subpath: str) -> str:
    """Filesystem path of the table root inside a local mount (prefix-confined)."""
    prefix = (mount.prefix or "").replace("\\", "/").strip("/")
    sub = (subpath or "").replace("\\", "/").strip("/")
    if ".." in prefix.split("/") or ".." in sub.split("/"):
        raise ValueError("mount path must not contain '..'")
    return os.path.join(mount.root, *[p for p in (prefix, sub) if p])


def _s3_table_uri(mount: Mount, subpath: str) -> str:
    """``s3://<upstream-bucket>/<prefix><subpath>`` table root (no trailing slash)."""
    prefix = (mount.prefix or "").replace("\\", "/").strip("/")
    sub = (subpath or "").replace("\\", "/").strip("/")
    key = "/".join(p for p in (prefix, sub) if p)
    return f"s3://{mount.root}/{key}".rstrip("/")


def _s3_storage_options(mount: Mount, *, store=None) -> dict:
    """Map a mount's S3 connection + credential into delta-rs storage options.

    Covers the common private-deployment modes (static/session keys, anonymous,
    instance/default chain). Modes that delta-rs/object_store cannot consume from
    boto3-resolved material (assume_role, web_identity, sso, profile, process) fail
    closed with a clear message. Secrets are read from the credential store and
    never logged.
    """
    from fabric_shortcut_proxy.storage.s3_auth import options_from_mount, resolve_s3_auth

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


def _azure_table_uri(mount: Mount, subpath: str) -> str:
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


def _azure_storage_options(mount: Mount, *, store=None) -> dict:
    """Map an ADLS Gen2 / Blob mount's credential into delta-rs storage options.

    Covers the secret-based modes (account key, SAS, connection string, service
    principal). ``managed_identity`` / ``default`` / ``anonymous`` fail closed with
    a clear message — object_store's ambient-credential handling isn't wired here
    yet. Secrets are read from the credential store and never logged.
    """
    from fabric_shortcut_proxy.storage.azure_auth import options_from_mount, resolve_azure_auth

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


def _local_iceberg_roots(mount: Mount, subpath: str) -> tuple[str, str]:
    prefix = _safe_relative_path(mount.prefix, description="mount prefix")
    sub = _safe_relative_path(subpath, description="table path")
    mount_root = os.path.realpath(mount.root)
    scope_root = os.path.realpath(os.path.join(mount_root, *prefix.split("/"))) if prefix else mount_root
    table_root = os.path.realpath(os.path.join(scope_root, *sub.split("/"))) if sub else scope_root
    if os.path.commonpath((mount_root, scope_root)) != mount_root:
        raise ValueError("local Iceberg mount prefix escapes its configured root")
    if os.path.commonpath((scope_root, table_root)) != scope_root:
        raise ValueError("local Iceberg table path escapes its configured prefix")
    return scope_root, table_root


def _selected_snapshot_id(mount: Mount, snapshot_id: int | str | None) -> int | None:
    selected = snapshot_id if snapshot_id is not None else getattr(mount, "snapshot_id", None)
    from fabric_shortcut_proxy.storage.mounts import _parse_snapshot_id

    try:
        return _parse_snapshot_id(selected)
    except ValueError as exc:
        raise ValueError("Iceberg snapshot_id must be a positive integer") from exc


def reader_for_mount(
    mount: Mount, *, subpath: str = "", snapshot_id: int | str | None = None,
    store=None,
) -> ObjectStoreTableReader:
    """Build the reader for a transforming mount's declared table ``format``."""
    fmt = (getattr(mount, "format", "") or "").strip().lower()
    if fmt not in READER_BACKENDS:
        raise ValueError(f"mount {mount.bucket!r} has no tokenizing table format")
    if mount.backend not in READER_BACKENDS[fmt]:
        raise ObjectStoreReaderUnavailable(
            f"{fmt} reading for backend {mount.backend!r} is not wired yet; "
            f"supported backends: {list(READER_BACKENDS[fmt])}"
        )
    if fmt != "iceberg" and (
        snapshot_id is not None or getattr(mount, "snapshot_id", None) is not None
    ):
        raise ValueError("snapshot_id is only valid for Iceberg mounts")
    if fmt == "delta":
        if mount.backend == "local":
            return DeltaTableReader(_local_table_path(mount, subpath))
        if mount.backend == "s3":
            return DeltaTableReader(_s3_table_uri(mount, subpath),
                                    storage_options=_s3_storage_options(mount))
        return DeltaTableReader(_azure_table_uri(mount, subpath),
                                storage_options=_azure_storage_options(mount))
    selected_snapshot = _selected_snapshot_id(mount, snapshot_id)
    if mount.backend == "local":
        scope_root, table_root = _local_iceberg_roots(mount, subpath)
        metadata = _discover_iceberg_metadata(table_root, scope_root=scope_root)
        properties = _iceberg_fileio_properties(mount, table_root=pathlib.Path(table_root).as_uri())
        properties["_fsp_scope_root"] = scope_root
        return IcebergTableReader(
            metadata, properties=properties, snapshot_id=selected_snapshot,
        )

    client = _remote_client(mount, store=store)
    metadata, table_root = _discover_remote_iceberg_metadata(mount, subpath, client)
    properties = _iceberg_fileio_properties(
        mount, table_root=table_root, client=client,
    )
    return IcebergTableReader(
        metadata, properties=properties, snapshot_id=selected_snapshot,
    )


# Backends the reader can serve per format; drives Config Builder and API support.
READER_BACKENDS: dict[str, tuple[str, ...]] = {
    "delta": ("local", "s3", "azure"),
    "iceberg": ("local", "s3", "azure"),
}

READER_AUTH_SUPPORT = {
    "s3": {
        "supported": [
            "static", "session", "assume_role", "web_identity", "profile", "sso",
            "instance", "anonymous",
        ],
        "refreshing": ["assume_role", "web_identity", "sso"],
        "provider_dependent_refresh": ["profile", "instance"],
        "non_refreshing": ["static", "session"],
    },
    "azure": {
        "supported": [
            "connection_string", "account_key", "sas", "aad_client_secret",
            "managed_identity", "workload_identity", "default", "anonymous",
        ],
        "refreshing": [
            "aad_client_secret", "managed_identity", "workload_identity", "default",
        ],
        "non_refreshing": ["connection_string", "account_key", "sas"],
    },
}


def reader_backend_support() -> dict[str, list[str]]:
    return {fmt: list(backends) for fmt, backends in READER_BACKENDS.items()}


def reader_auth_support() -> dict:
    return {
        backend: {key: list(values) for key, values in capabilities.items()}
        for backend, capabilities in READER_AUTH_SUPPORT.items()
    }


def reader_backend_available() -> dict[str, bool]:
    import importlib.util

    def available(module: str) -> bool:
        try:
            return importlib.util.find_spec(module) is not None
        except ModuleNotFoundError:
            return False

    return {
        "local": True,
        "s3": available("boto3"),
        "azure": available("azure.storage.blob"),
    }
