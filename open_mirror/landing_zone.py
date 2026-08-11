"""Landing-zone path model + write backends for Open Mirroring.

A landing zone is rooted at either a OneLake DFS URI
(``https://onelake.dfs.fabric.microsoft.com/<ws>/<db>/Files/LandingZone``) or a
local/UNC filesystem path used for staging and tests. A table lands under a
per-table folder, optionally nested inside a ``<schema>.schema`` folder.

Phase 2 ships the :class:`LocalLandingZone` filesystem backend (fully testable
and usable for on-prem staging). The OneLake DFS backend is a separate seam
(``open_landing_zone`` raises a clear error for ``https://onelake`` roots) so the
authenticated ADLS writer can be added later without touching the writer/metadata
logic.
"""
from __future__ import annotations

import os
from typing import Protocol, runtime_checkable

_ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"

# Fabric folder-name conventions.
SCHEMA_FOLDER_SUFFIX = ".schema"


def is_onelake_uri(root: str) -> bool:
    """True when ``root`` is a OneLake DFS URI rather than a local path."""
    r = (root or "").strip().lower()
    return r.startswith("https://") and _ONELAKE_HOST in r


def table_relative_path(target_table: str, schema: str | None = None) -> str:
    """Landing-zone-relative folder for a mirrored table.

    ``<schema>.schema/<table>`` when a schema is given (Fabric schema layout),
    else just ``<table>``. Always uses ``/`` separators (backend-neutral).
    """
    table = (target_table or "").strip().strip("/")
    if not table:
        raise ValueError("target_table must be non-empty")
    schema = (schema or "").strip().strip("/")
    if schema:
        return f"{schema}{SCHEMA_FOLDER_SUFFIX}/{table}"
    return table


@runtime_checkable
class LandingZoneBackend(Protocol):
    """Minimal filesystem-like surface the publisher needs.

    Paths are landing-zone-relative and always use ``/`` separators.
    """

    def exists(self, rel_path: str) -> bool: ...

    def list_dir(self, rel_path: str) -> list[str]: ...

    def read_text(self, rel_path: str) -> str: ...

    def write_bytes(self, rel_path: str, data: bytes) -> None: ...

    def write_text(self, rel_path: str, text: str) -> None: ...


class LocalLandingZone:
    """Filesystem-backed landing zone (local directory or UNC/SMB share)."""

    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)

    def _abs(self, rel_path: str) -> str:
        # Normalize separators and confine every path under the root (no traversal).
        parts = [p for p in (rel_path or "").replace("\\", "/").split("/") if p not in ("", ".")]
        if any(p == ".." for p in parts):
            raise ValueError(f"path escapes the landing zone: {rel_path!r}")
        return os.path.join(self.root, *parts)

    def exists(self, rel_path: str) -> bool:
        return os.path.exists(self._abs(rel_path))

    def list_dir(self, rel_path: str) -> list[str]:
        target = self._abs(rel_path)
        if not os.path.isdir(target):
            return []
        return sorted(os.listdir(target))

    def read_text(self, rel_path: str) -> str:
        with open(self._abs(rel_path), "r", encoding="utf-8") as fh:
            return fh.read()

    def write_bytes(self, rel_path: str, data: bytes) -> None:
        target = self._abs(rel_path)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        tmp = f"{target}.tmp"
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)

    def write_text(self, rel_path: str, text: str) -> None:
        self.write_bytes(rel_path, text.encode("utf-8"))


def open_landing_zone(root: str, *, credential=None) -> LandingZoneBackend:
    """Resolve a landing-zone root to a write backend.

    Local/UNC paths return a :class:`LocalLandingZone`. OneLake DFS URIs return an
    :class:`~open_mirror.onelake.OneLakeLandingZone` authenticated with the proxy's
    OWN Entra identity (the same credential configured for Key Vault, issue #16);
    pass ``credential`` only to override that reuse.
    """
    if is_onelake_uri(root):
        from open_mirror.onelake import OneLakeLandingZone
        return OneLakeLandingZone(root, credential=credential)
    return LocalLandingZone(root)
