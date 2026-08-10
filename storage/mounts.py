"""
Mount registry — the storage-proxy mount table (devplan/StorageProxy.md, Phase 1).

A **mount** maps an S3 *bucket* to a storage backend + optional prefix so the
proxy can serve existing files as byte passthrough. Loaded from the gitignored
``config.mounts.json`` (top-level ``mounts`` array); empty by default, so the
feature is inert unless configured AND ``ENABLE_STORAGE_PROXY`` is set.

Phase 1 supports the ``local`` backend only — a filesystem path, which covers an
NFS or SMB share mounted by the OS (UNC path / mount point). Native S3/SMB/Azure
backends are later phases; unknown backends are rejected at load with a clear
message.

Security: a mount bucket must differ from the DB warehouse bucket, keys are
confined to the mount's ``prefix`` subtree (the backend also rejects ``..``), and
mounts are read-only in Phase 1.
"""
from __future__ import annotations

import json
import os
import re
import sys
import threading
from dataclasses import dataclass, replace

import system_config
from runtime.artifact_store import ArtifactStore, LocalDirStore
from observability.logging import get_logger

log = get_logger(__name__)

_CONFIG_FILE = os.environ.get("MOUNTS_CONFIG_FILE", "config.mounts.json")
_SUPPORTED_BACKENDS = ("local", "s3", "azure")   # local (P1), s3/MinIO (P2), Azure Blob/ADLS (P3)


def _enabled() -> bool:
    """Whether the storage proxy is turned on (env or config.system.json)."""
    v = os.environ.get("ENABLE_STORAGE_PROXY")
    if v is not None:
        return v.strip().lower() in ("1", "true", "yes", "on")
    return bool(getattr(system_config, "ENABLE_STORAGE_PROXY", False))


@dataclass(frozen=True)
class Mount:
    """One bucket served as passthrough from a storage backend."""
    bucket: str
    backend: str                 # "local" | "s3" | "azure"
    root: str = ""               # local: filesystem path; s3: upstream bucket; azure: container
    prefix: str = ""             # confine serving to this subtree of the backend
    read_only: bool = True
    credential: str = ""         # credential-store id for upstream creds
    # s3 backend (non-secret connection knobs; secrets live in the credential store)
    endpoint: str = ""           # custom endpoint URL (MinIO/Ceph/R2/...); azure: account URL override
    region: str = ""
    addressing_style: str = ""   # auto | path | virtual
    signature_version: str = ""  # s3v4 | s3 ("" = botocore default)
    verify_tls: str = ""         # ""=default | "false"=skip | else CA bundle path
    use_fips: bool = False
    use_dualstack: bool = False
    auth: str = ""               # credential-less mode: anonymous | instance | default | managed_identity
    # azure backend (non-secret connection knobs)
    account: str = ""            # storage account name
    endpoint_suffix: str = ""    # blob endpoint suffix (sovereign clouds); "" = blob.core.windows.net
    # object-store tokenizer (issue #12): a mount tagged with a table format is
    # served as a virtual, tokenized copy of the source Delta/Iceberg table.
    format: str = ""             # "" = plain passthrough | "delta" | "iceberg"
    key_column: str = ""         # ordering/key column; may not carry a transform
    columns: tuple = ()          # tuple[config.ColumnDef]; output schema + column policy


def _norm_prefix(p: str) -> str:
    p = (p or "").replace("\\", "/").strip("/")
    return f"{p}/" if p else ""


def _parse_columns(raw) -> tuple:
    """Parse a mount's column policy into ``config.ColumnDef`` objects.

    Reuses the same allowlisted transform model as the SQL pushdown path; the
    ColumnDef/ColumnTransform constructors validate kind, key_ref, normalization,
    and the string-output rule, raising ``ValueError`` on a bad entry.
    """
    from config import ColumnDef, ColumnTransform  # lazy: avoid an import cycle

    out = []
    for c in raw or []:
        if not isinstance(c, dict):
            continue
        tr = c.get("transform")
        transform = None
        if isinstance(tr, dict):
            transform = ColumnTransform(
                kind=str(tr.get("kind") or ""),
                key_ref=(tr.get("key_ref") or None),
                domain=(tr.get("domain") or None),
                normalization=str(tr.get("normalization") or "none"),
            )
        out.append(ColumnDef(
            field_id=int(c.get("field_id") or 0),
            name=str(c.get("name") or ""),
            iceberg_type=str(c.get("type") or "string"),
            nullable=bool(c.get("nullable", True)),
            source=(c.get("source") or None),
            transform=transform,
        ))
    return tuple(out)


def _mount_from_json(d: dict) -> Mount:
    return Mount(
        bucket=str(d.get("bucket") or "").strip(),
        backend=str(d.get("backend") or "local").strip().lower(),
        root=str(d.get("root") or "").strip(),
        prefix=_norm_prefix(d.get("prefix") or ""),
        read_only=bool(d.get("read_only", True)),
        credential=str(d.get("credential") or "").strip(),
        endpoint=str(d.get("endpoint") or "").strip(),
        region=str(d.get("region") or "").strip(),
        addressing_style=str(d.get("addressing_style") or "").strip().lower(),
        signature_version=str(d.get("signature_version") or "").strip().lower(),
        verify_tls=str(d.get("verify_tls") or "").strip(),
        use_fips=bool(d.get("use_fips", False)),
        use_dualstack=bool(d.get("use_dualstack", False)),
        auth=str(d.get("auth") or "").strip().lower(),
        account=str(d.get("account") or "").strip(),
        endpoint_suffix=str(d.get("endpoint_suffix") or "").strip(),
        format=str(d.get("format") or "").strip().lower(),
        key_column=str(d.get("key_column") or "").strip(),
        columns=_parse_columns(d.get("columns")),
    )


def _load_file() -> list:
    try:
        with open(_CONFIG_FILE, "r", encoding="utf-8-sig") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return []
    except (OSError, ValueError) as exc:
        print(f"[mounts] could not read {_CONFIG_FILE!r}: {exc}", file=sys.stderr)
        return []
    raw = data.get("mounts") if isinstance(data, dict) else data
    return raw if isinstance(raw, list) else []


_VALID_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,62}$")


def _build_mounts() -> dict[str, Mount]:
    reserved = getattr(system_config, "BUCKET_NAME", "")
    out: dict[str, Mount] = {}
    for entry in _load_file():
        if not isinstance(entry, dict):
            continue
        try:
            m = _mount_from_json(entry)
        except ValueError as exc:  # bad column policy (kind/key_ref/type) — never a secret
            print(f"[mounts] entry {entry.get('bucket')!r}: {exc}; skipped.", file=sys.stderr)
            continue
        if not m.bucket:
            print("[mounts] entry missing 'bucket'; skipped.", file=sys.stderr)
            continue
        if m.bucket == reserved:
            print(f"[mounts] bucket {m.bucket!r} is reserved for the DB warehouse; skipped.", file=sys.stderr)
            continue
        if not _VALID_BUCKET.match(m.bucket):
            print(f"[mounts] bucket {m.bucket!r} is not a valid S3 bucket name; skipped.", file=sys.stderr)
            continue
        if m.backend not in _SUPPORTED_BACKENDS:
            print(f"[mounts] backend {m.backend!r} not supported yet (Phase 1 = {_SUPPORTED_BACKENDS}); "
                  f"bucket {m.bucket!r} skipped.", file=sys.stderr)
            continue
        if m.backend == "local" and not m.root:
            print(f"[mounts] local mount {m.bucket!r} missing 'root'; skipped.", file=sys.stderr)
            continue
        if m.backend == "s3" and not m.root:
            print(f"[mounts] s3 mount {m.bucket!r} missing 'root' (upstream bucket); skipped.", file=sys.stderr)
            continue
        if m.backend == "azure" and not m.root:
            print(f"[mounts] azure mount {m.bucket!r} missing 'root' (container); skipped.", file=sys.stderr)
            continue
        if not m.read_only:
            print(f"[mounts] mount {m.bucket!r}: read-write not supported yet; serving read-only.", file=sys.stderr)
            m = replace(m, read_only=True)
        if m.format:
            try:
                from storage.objectstore_capabilities import validate_object_store_policy
                validate_object_store_policy(format=m.format, key_column=m.key_column,
                                             columns=list(m.columns))
            except ValueError as exc:
                print(f"[mounts] mount {m.bucket!r}: {exc}; skipped.", file=sys.stderr)
                continue
            if not m.columns:
                print(f"[mounts] tokenizing mount {m.bucket!r} needs a 'columns' policy; skipped.",
                      file=sys.stderr)
                continue
        if m.bucket in out:
            print(f"[mounts] duplicate mount bucket {m.bucket!r}; last wins.", file=sys.stderr)
        out[m.bucket] = m
    return out


MOUNTS: dict[str, Mount] = _build_mounts()

_backends: dict[str, ArtifactStore] = {}
_backends_lock = threading.Lock()


def enabled() -> bool:
    return _enabled()


def get_mount(bucket: str) -> Mount | None:
    """Return the Mount for a bucket when the proxy is enabled, else ``None``."""
    if not _enabled():
        return None
    return MOUNTS.get(bucket)


def mount_ids() -> list[str]:
    return sorted(MOUNTS.keys())


def backend_for(mount: Mount) -> ArtifactStore:
    """Return (lazily building + caching) the backend store for a mount."""
    with _backends_lock:
        store = _backends.get(mount.bucket)
        if store is None:
            store = _build_backend(mount)
            _backends[mount.bucket] = store
        return store


def _build_backend(mount: Mount) -> ArtifactStore:
    if mount.backend == "local":
        return LocalDirStore(mount.root)
    if mount.backend == "s3":
        from storage.s3_store import build_s3_store
        return build_s3_store(mount)
    if mount.backend == "azure":
        from storage.azure_store import build_azure_store
        return build_azure_store(mount)
    raise ValueError(f"unsupported mount backend: {mount.backend!r}")


def validate_mounts() -> list[str]:
    """Return a list of config problems (empty = OK). Used by config.validate_config."""
    problems: list[str] = []
    if not _enabled():
        return problems
    reserved = getattr(system_config, "BUCKET_NAME", "")
    for bucket, m in MOUNTS.items():
        if bucket == reserved:
            problems.append(f"mount bucket {bucket!r} collides with the DB warehouse bucket")
        if m.backend == "local":
            if not m.root:
                problems.append(f"mount {bucket!r}: local backend needs 'root'")
            elif not os.path.isdir(m.root):
                problems.append(f"mount {bucket!r}: root {m.root!r} is not a directory (mount it first)")
        elif m.backend == "s3":
            if not m.root:
                problems.append(f"mount {bucket!r}: s3 backend needs 'root' (the upstream bucket)")
            try:
                from storage.s3_auth import resolve_s3_auth, validate_s3_auth
                auth = resolve_s3_auth(m)
                problems.extend(f"mount {bucket!r}: {p}" for p in validate_s3_auth(auth))
            except Exception as exc:  # noqa: BLE001 - surface a clean message, never a secret
                problems.append(f"mount {bucket!r}: {exc}")
        elif m.backend == "azure":
            if not m.root:
                problems.append(f"mount {bucket!r}: azure backend needs 'root' (the container)")
            try:
                from storage.azure_auth import resolve_azure_auth, validate_azure_auth
                auth = resolve_azure_auth(m)
                problems.extend(f"mount {bucket!r}: {p}" for p in validate_azure_auth(auth))
            except Exception as exc:  # noqa: BLE001 - surface a clean message, never a secret
                problems.append(f"mount {bucket!r}: {exc}")
    return problems
