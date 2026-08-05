from __future__ import annotations

from db.capabilities import (
    capabilities_for_db_url,
    capabilities_for_dialect,
    capability_matrix,
    flavor_from_db_url,
    missing_required_fields,
)


def test_flavor_from_db_url():
    assert flavor_from_db_url("postgresql+asyncpg://h/db") == "postgresql"
    assert flavor_from_db_url("mssql+aioodbc://h/db") == "mssql"
    assert flavor_from_db_url("oracle+oracledb://h/db") == "oracle"
    assert flavor_from_db_url("databricks://token:pat@dbc") == "databricks"
    assert flavor_from_db_url("sqlite+aiosqlite:///x.db") == "sqlite"


def test_capability_matrix_has_oracle_and_databricks():
    m = capability_matrix()
    assert "oracle" in m
    assert "databricks" in m
    assert m["oracle"]["execution_mode"] == "sync-threadpool-fallback"
    assert m["databricks"]["supports_primary_key_reflection"] is False

def test_tokenization_capabilities_by_dialect():
    matrix = capability_matrix()
    for flavor in ("mssql", "postgresql", "oracle", "databricks"):
        assert matrix[flavor]["supports_deterministic_tokenization"] is True
        assert matrix[flavor]["supports_random_tokenization"] is True
    assert matrix["generic"]["supports_deterministic_tokenization"] is False
    assert matrix["sqlite"]["supports_random_tokenization"] is False


def test_databricks_requires_http_path():
    assert missing_required_fields("databricks", {}) == ["http_path"]
    assert missing_required_fields("databricks", {"http_path": "/sql/1.0/warehouses/x"}) == []


def test_async_driver_detection():
    assert capabilities_for_dialect("postgresql").async_driver is True
    assert capabilities_for_dialect("mssql").async_driver is True
    assert capabilities_for_dialect("oracle").async_driver is False
    assert capabilities_for_db_url("databricks://token:pat@dbc").async_driver is False
