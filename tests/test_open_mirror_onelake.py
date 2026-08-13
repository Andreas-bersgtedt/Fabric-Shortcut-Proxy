"""Phase 5 — OneLake landing-zone backend + proxy-identity auth reuse.

Verifies OneLake DFS URL parsing, that ``open_landing_zone`` returns the OneLake
backend for a OneLake URI, that the backend reuses the proxy's Entra credential
(the Key Vault identity, issue #16), and an end-to-end publish against an injected
fake ADLS service client (no Azure SDK or live account required).
"""
from __future__ import annotations

import io
import json
import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import pyarrow.parquet as pq
import pytest

import config
import security.azure_credential as azcred
from config import ColumnDef
from open_mirror import open_landing_zone, target_from_dict
from open_mirror.landing_zone import is_onelake_uri
from open_mirror.onelake import OneLakeLandingZone, _parse_onelake_url
from open_mirror.publisher import LandingZonePublisher

_ONELAKE = "https://onelake.dfs.fabric.microsoft.com/ws-guid/db-guid/Files/LandingZone"

_COLUMNS = [
    ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
    ColumnDef(field_id=2, name="name", iceberg_type="string", nullable=True),
]


# --- fake ADLS service client (in-memory filesystem) -----------------------

class _FakeDownload:
    def __init__(self, data: bytes):
        self._data = data

    def readall(self) -> bytes:
        return self._data


class _FakeFileClient:
    def __init__(self, store: dict, path: str):
        self._store = store
        self._path = path

    def exists(self) -> bool:
        return self._path in self._store

    def upload_data(self, data, overwrite: bool = True) -> None:
        self._store[self._path] = bytes(data)

    def download_file(self) -> _FakeDownload:
        return _FakeDownload(self._store[self._path])


class _FakePath:
    def __init__(self, name: str):
        self.name = name


class _FakeFileSystemClient:
    def __init__(self, store: dict):
        self._store = store

    def get_file_client(self, path: str) -> _FakeFileClient:
        return _FakeFileClient(self._store, path)

    def get_paths(self, path=None, recursive: bool = False):
        prefix = (path.rstrip("/") + "/") if path else ""
        children = set()
        for key in self._store:
            if key.startswith(prefix):
                rest = key[len(prefix):]
                if rest:
                    children.add(prefix + rest.split("/")[0])
        return [_FakePath(n) for n in sorted(children)]


class _FakeServiceClient:
    def __init__(self, store: dict):
        self._store = store

    def get_file_system_client(self, filesystem: str) -> _FakeFileSystemClient:
        return _FakeFileSystemClient(self._store)


# --- URL parsing -----------------------------------------------------------

def test_parse_onelake_url():
    account, filesystem, base = _parse_onelake_url(_ONELAKE)
    assert account == "https://onelake.dfs.fabric.microsoft.com"
    assert filesystem == "ws-guid"
    assert base == "db-guid/Files/LandingZone"


def test_open_landing_zone_returns_onelake_backend_for_onelake_uri():
    assert is_onelake_uri(_ONELAKE)
    backend = open_landing_zone(_ONELAKE)
    assert isinstance(backend, OneLakeLandingZone)
    assert backend.filesystem == "ws-guid"


# --- proxy-identity reuse (the steer) --------------------------------------

def test_onelake_reuses_proxy_entra_identity(monkeypatch):
    captured = {}

    def fake_get_credential(mode, *, tenant_id="", client_id="", client_secret=""):
        captured.update(mode=mode, tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        return object()

    monkeypatch.setattr(azcred, "get_credential", fake_get_credential)
    monkeypatch.setattr(config, "AUTH_MODE", "service_principal", raising=False)
    monkeypatch.setattr(config, "AZURE_TENANT_ID", "tenant-1", raising=False)
    monkeypatch.setattr(config, "AZURE_CLIENT_ID", "client-1", raising=False)
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret-1")

    cred = azcred.proxy_credential(config)
    assert cred is not None
    assert captured == {
        "mode": "service_principal",
        "tenant_id": "tenant-1",
        "client_id": "client-1",
        "client_secret": "secret-1",
    }


# --- end-to-end publish against the fake OneLake backend -------------------

def _target():
    return target_from_dict({
        "id": "fabric-sales",
        "connection": "default",
        "landing_zone_root": _ONELAKE,
        "source_type": "SQL",
        "tables": [{
            "name": "sales", "source_table": "dbo.sales", "target_table": "sales",
            "key_column": "id", "schema": "dbo",
        }],
    })


def test_publish_initial_load_to_onelake_backend():
    store: dict = {}
    target = _target()
    backend = OneLakeLandingZone(_ONELAKE, service_client=_FakeServiceClient(store))
    pub = LandingZonePublisher(backend, target)

    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    rel = pub.publish_initial_load(target.tables[0], rows, _COLUMNS)
    assert rel == "dbo.schema/sales/00000000000000000001.parquet"

    base = "db-guid/Files/LandingZone"
    meta_key = f"{base}/dbo.schema/sales/_metadata.json"
    data_key = f"{base}/dbo.schema/sales/00000000000000000001.parquet"
    assert json.loads(store[meta_key].decode("utf-8"))["keyColumns"] == ["id"]
    assert pq.read_table(io.BytesIO(store[data_key])).num_rows == 2


def test_onelake_backend_read_exists_and_list(monkeypatch):
    store = {"db-guid/Files/LandingZone/dbo.schema/sales/00000000000000000001.parquet": b"x",
             "db-guid/Files/LandingZone/dbo.schema/sales/_metadata.json": b"{}"}
    backend = OneLakeLandingZone(_ONELAKE, service_client=_FakeServiceClient(store))
    assert backend.exists("dbo.schema/sales/_metadata.json")
    assert not backend.exists("dbo.schema/sales/nope.parquet")
    listed = backend.list_dir("dbo.schema/sales")
    assert "_metadata.json" in listed
    assert "00000000000000000001.parquet" in listed


def test_onelake_backend_blocks_traversal():
    backend = OneLakeLandingZone(_ONELAKE, service_client=_FakeServiceClient({}))
    with pytest.raises(ValueError):
        backend.write_bytes("../escape.txt", b"x")
