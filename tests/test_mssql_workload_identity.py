from __future__ import annotations

import struct
from unittest.mock import Mock

import pytest
from sqlalchemy.engine import make_url

import db.reflect
from db.executor import (
    _extract_mssql_workload_identity,
    _odbc_access_token,
    _strip_odbc_trusted_connection,
)


def test_extracts_mssql_workload_identity_authentication() -> None:
    url, enabled = _extract_mssql_workload_identity(
        "mssql+aioodbc://sql.example/database?"
        "driver=ODBC+Driver+18+for+SQL+Server&"
        "Authentication=ActiveDirectoryWorkloadIdentity&Encrypt=yes"
    )

    assert enabled is True
    assert "Authentication" not in url.query
    assert url.username is None
    assert url.password is None
    assert url.query["Encrypt"] == "yes"
    assert url.query["driver"] == "ODBC Driver 18 for SQL Server"


def test_extracts_mssql_managed_identity_authentication() -> None:
    url, enabled = _extract_mssql_workload_identity(
        "mssql+aioodbc://client-id@sql.example/database?"
        "driver=ODBC+Driver+18+for+SQL+Server&"
        "Authentication=ActiveDirectoryManagedIdentity&Encrypt=yes"
    )

    assert enabled is True
    assert "Authentication" not in url.query
    assert url.username is None


def test_leaves_other_authentication_modes_unchanged() -> None:
    original = (
        "mssql+aioodbc://sql.example/database?"
        "Authentication=ActiveDirectoryServicePrincipal"
    )

    url, enabled = _extract_mssql_workload_identity(original)

    assert enabled is False
    assert url == original


def test_odbc_access_token_uses_length_prefixed_utf16() -> None:
    packed = _odbc_access_token("token")
    length = struct.unpack("<I", packed[:4])[0]

    assert length == len("token".encode("utf-16-le"))
    assert packed[4:].decode("utf-16-le") == "token"


def test_strips_trusted_connection_from_aioodbc_dsn() -> None:
    connect_params = {"dsn": "Driver=X;Server=Y;Trusted_Connection=Yes"}

    _strip_odbc_trusted_connection([], connect_params)

    assert connect_params["dsn"] == "Driver=X;Server=Y"


def test_strips_trusted_connection_from_positional_connection_string() -> None:
    connect_args = ["Driver=X;Server=Y;Trusted_Connection=Yes"]

    _strip_odbc_trusted_connection(connect_args, {})

    assert connect_args == ["Driver=X;Server=Y"]


@pytest.mark.asyncio
async def test_schema_reflector_uses_workload_identity_engine_factory(monkeypatch) -> None:
    engine = Mock()
    engine.dispose.return_value = None
    factory = Mock(return_value=engine)
    monkeypatch.setattr(db.reflect, "_make_async_engine", factory)
    url = make_url(
        "mssql+aioodbc://sql.example/database?"
        "driver=ODBC+Driver+18+for+SQL+Server&"
        "Authentication=ActiveDirectoryWorkloadIdentity&Encrypt=yes"
    )

    reflector = db.reflect.SchemaReflector(url)
    await reflector.__aenter__()

    factory.assert_called_once_with(url)