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
from dataclasses import dataclass

import system_config
from runtime.artifact_store import ArtifactStore, LocalDirStore
from observability.logging import get_logger

log = get_logger(__name__)

_CONFIG_FILE = os.environ.get("MOUNTS_CONFIG_FILE", "config.mounts.json")
_SUPPORTED_BACKENDS = ("local",)          # Phase 1; s3/smb/azure land later


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
    backend: str                 # "local" (Phase 1)
    root: str = ""               # local backend: filesystem path (NFS/SMB mount)
    prefix: str = ""             # confine serving to this subtree of the backend
    read_only: bool = True
    credential: str = ""         # credential-store id for upstream creds (future)


def _norm_prefix(p: str) -> str:
    p = (p or "").replace("\\", "/").strip("/")
    return f"{p}/" if p else ""


def _mount_from_json(d: dict) -> Mount:
    return Mount(
        bucket=str(d.get("bucket") or "").strip(),
        backend=str(d.get("backend") or "local").strip().lower(),
        root=str(d.get("root") or "").strip(),
        prefix=_norm_prefix(d.get("prefix") or ""),
        read_only=bool(d.get("read_only", True)),
        credential=str(d.get("credential") or "").strip(),
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
        m = _mount_from_json(entry)
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
        if not m.read_only:
            print(f"[mounts] mount {m.bucket!r}: read-write not supported yet; serving read-only.", file=sys.stderr)
            m = Mount(m.bucket, m.backend, m.root, m.prefix, True, m.credential)
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
    return problems
