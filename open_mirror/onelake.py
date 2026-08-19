"""OneLake (ADLS Gen2) landing-zone backend for Open Mirroring.

Writes the Fabric landing-zone files to a mirrored database's OneLake location
over the DFS endpoint, authenticated with the proxy's OWN Entra identity — the
same service principal / managed identity / default credential already configured
for Key Vault (issue #16). No separate credential is introduced here.

``azure-storage-file-datalake`` and ``azure-identity`` are imported lazily (the
optional ``onelake`` extra), so the core install needs no Azure SDK. Tests inject
a fake service client to exercise the backend without the SDK or a live account.
"""
from __future__ import annotations

from urllib.parse import urlsplit

import config
from security.azure_credential import proxy_credential

_INSTALL_HINT = (
    "OneLake landing zones need azure-storage-file-datalake; install it with "
    "pip install 'fabric-shortcut-proxy[onelake]'"
)


def _is_directory(value) -> bool:
    """Normalize Azure SDK and test-double directory flags."""
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def _require_sdk() -> None:
    try:
        import azure.storage.filedatalake  # noqa: F401
    except ImportError as exc:  # pragma: no cover - only without the extra
        raise RuntimeError(_INSTALL_HINT) from exc


def _parse_onelake_url(url: str) -> tuple[str, str, str]:
    """Split a OneLake DFS URL into ``(account_url, filesystem, base_path)``.

    ``https://onelake.dfs.fabric.microsoft.com/<ws>/<db>/Files/LandingZone`` ->
    account ``https://onelake.dfs.fabric.microsoft.com``, filesystem ``<ws>``
    (the workspace), base path ``<db>/Files/LandingZone``.
    """
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        raise ValueError(f"not a OneLake URL: {url!r}")
    account_url = f"{parts.scheme}://{parts.netloc}"
    segments = [s for s in parts.path.split("/") if s]
    if not segments:
        raise ValueError(f"OneLake URL is missing the workspace (filesystem): {url!r}")
    filesystem = segments[0]
    base_path = "/".join(segments[1:])
    return account_url, filesystem, base_path


class OneLakeLandingZone:
    """ADLS Gen2 (OneLake DFS) implementation of the landing-zone backend.

    The service client is built lazily from the proxy's Entra credential; a test
    may inject ``service_client`` to avoid the Azure SDK and a live account.
    """

    def __init__(self, url: str, *, credential=None, service_client=None) -> None:
        self.account_url, self.filesystem, self.base_path = _parse_onelake_url(url)
        self._credential = credential
        self._service = service_client

    # -- client plumbing --------------------------------------------------

    def _svc(self):
        if self._service is None:
            _require_sdk()
            from azure.storage.filedatalake import DataLakeServiceClient
            credential = self._credential or proxy_credential(config)
            self._service = DataLakeServiceClient(account_url=self.account_url, credential=credential)
        return self._service

    def _fs(self):
        return self._svc().get_file_system_client(self.filesystem)

    def _abs(self, rel_path: str) -> str:
        parts = [p for p in (rel_path or "").replace("\\", "/").split("/") if p not in ("", ".")]
        if any(p == ".." for p in parts):
            raise ValueError(f"path escapes the landing zone: {rel_path!r}")
        base = [self.base_path] if self.base_path else []
        return "/".join(base + parts)

    # -- LandingZoneBackend protocol --------------------------------------

    def exists(self, rel_path: str) -> bool:
        return self._fs().get_file_client(self._abs(rel_path)).exists()

    def list_dir(self, rel_path: str) -> list[str]:
        try:
            from azure.core.exceptions import ResourceNotFoundError
        except ImportError:
            ResourceNotFoundError = ()  # SDK absent (injected client): nothing to catch
        path = self._abs(rel_path)
        try:
            paths = self._fs().get_paths(path=path or None, recursive=False)
            return sorted(p.name.split("/")[-1] for p in paths)
        except ResourceNotFoundError:
            return []

    def list_entries(self, rel_path: str) -> list[dict]:
        try:
            from azure.core.exceptions import ResourceNotFoundError
        except ImportError:
            ResourceNotFoundError = ()
        try:
            paths = self._fs().get_paths(path=self._abs(rel_path), recursive=False)
            return [{
                "name": p.name.split("/")[-1],
                "is_directory": _is_directory(getattr(p, "is_directory", False)),
                "last_modified": getattr(p, "last_modified", None),
                "content_length": getattr(p, "content_length", 0) or 0,
            } for p in paths]
        except ResourceNotFoundError:
            return []

    def read_text(self, rel_path: str) -> str:
        data = self._fs().get_file_client(self._abs(rel_path)).download_file().readall()
        return data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else str(data)

    def read_bytes(self, rel_path: str) -> bytes:
        data = self._fs().get_file_client(self._abs(rel_path)).download_file().readall()
        return bytes(data)

    def write_bytes(self, rel_path: str, data: bytes) -> None:
        self._fs().get_file_client(self._abs(rel_path)).upload_data(data, overwrite=True)

    def write_text(self, rel_path: str, text: str) -> None:
        self.write_bytes(rel_path, text.encode("utf-8"))

    def delete(self, rel_path: str) -> None:
        try:
            from azure.core.exceptions import ResourceNotFoundError
        except ImportError:
            ResourceNotFoundError = ()  # SDK absent (injected client): nothing to catch
        try:
            self._fs().get_file_client(self._abs(rel_path)).delete_file()
        except ResourceNotFoundError:
            pass

    def remove_tree(self, rel_path: str) -> None:
        try:
            from azure.core.exceptions import ResourceNotFoundError
        except ImportError:
            ResourceNotFoundError = ()  # SDK absent (injected client): nothing to catch
        try:
            self._fs().get_directory_client(self._abs(rel_path)).delete_directory()
        except ResourceNotFoundError:
            pass
