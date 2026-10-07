from __future__ import annotations

from fabric_shortcut_proxy.db.capabilities import (
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


def test_tokenization_backend_topology_prefers_native_then_arrow_then_none():
    mssql = capabilities_for_dialect("mssql")
    impala = capabilities_for_dialect("impala")
    assert mssql.tokenization_backend("deterministic_hash") == "native"
    assert impala.tokenization_backend("deterministic_hash") == "none"
    assert impala.tokenization_backend("deterministic_hash", "arrow") == "arrow"
    assert impala.tokenization_warning("deterministic_hash") is None
    assert "plaintext" in impala.tokenization_warning("deterministic_hash", "arrow")


def test_flavor_warnings_include_arrow_operational_impact(monkeypatch):
    from fabric_shortcut_proxy import config

    monkeypatch.setattr(config, "TOKENIZATION_FALLBACK", "arrow", raising=False)
    warnings = __import__("fabric_shortcut_proxy.db.capabilities", fromlist=["flavor_warnings"]).flavor_warnings("impala")
    assert any("plaintext source values" in warning for warning in warnings)


def test_databricks_requires_http_path():
    assert missing_required_fields("databricks", {}) == ["http_path"]
    assert missing_required_fields("databricks", {"http_path": "/sql/1.0/warehouses/x"}) == []


def test_async_driver_detection():
    assert capabilities_for_dialect("postgresql").async_driver is True
    assert capabilities_for_dialect("mssql").async_driver is True
    assert capabilities_for_dialect("oracle").async_driver is False
    assert capabilities_for_db_url("databricks://token:pat@dbc").async_driver is False


def test_flavor_from_db_url_expanded_sources():
    assert flavor_from_db_url("redshift+redshift_connector://h:5439/db") == "redshift"
    assert flavor_from_db_url("teradatasql://h/?database=dbc") == "teradata"
    assert flavor_from_db_url("impala://h:21050/db") == "impala"
    # Redshift must not be misread as PostgreSQL.
    assert flavor_from_db_url("postgresql+asyncpg://h/db") == "postgresql"


def test_expanded_sources_capabilities_are_conservative():
    matrix = capability_matrix()
    for flavor in ("redshift", "teradata", "impala"):
        assert flavor in matrix
        assert matrix[flavor]["execution_mode"] == "sync-threadpool-fallback"
        assert matrix[flavor]["supports_deterministic_tokenization"] is False
        assert matrix[flavor]["supports_random_tokenization"] is False
        assert matrix[flavor]["supports_stats_histogram"] is False
        assert matrix[flavor]["supports_fast_row_estimate"] is False
        assert matrix[flavor]["required_connection_fields"] == []


def test_source_capability_matrix_exposes_evidence_and_gap_contract():
    matrix = capability_matrix()
    for flavor in ("oracle", "databricks", "redshift", "teradata", "impala"):
        source = matrix[flavor]
        assert source["support_status"] in {"supported", "beta", "preview"}
        assert source["supports_streaming_query"] is False
        assert "streaming_query" in source["capability_gaps"]
        for gap in source["capability_gaps"].values():
            assert gap["reason"]
            assert gap["fallback"]
            assert gap["cost"]
    for flavor in ("databricks", "impala"):
        assert matrix[flavor]["requires_explicit_split_key"] is True
        assert "explicit key_column" in matrix[flavor]["capability_gaps"][
            "primary_key_reflection"
        ]["fallback"]


def test_issue_94_beta_release_gate_is_conservative():
    matrix = capability_matrix()
    assert {
        flavor: matrix[flavor]["support_status"]
        for flavor in ("oracle", "databricks", "redshift", "teradata", "impala")
    } == {
        "oracle": "supported",
        "databricks": "beta",
        "redshift": "beta",
        "teradata": "beta",
        "impala": "supported",
    }
    for flavor in ("databricks", "redshift", "teradata"):
        source = matrix[flavor]
        assert source["supports_streaming_query"] is False
        assert source["supports_freshness_probe"] is False
        assert source["supports_stats_histogram"] is False
        assert source["source_snapshot_provider"] == "none"
        assert {"streaming_query", "freshness_probe", "stats_histogram"} <= set(
            source["capability_gaps"]
        )
    for flavor in ("redshift", "teradata"):
        source = matrix[flavor]
        assert source["supports_deterministic_tokenization"] is False
        assert source["supports_random_tokenization"] is False
        assert "native_tokenization" in source["capability_gaps"]


def test_capability_documentation_matches_source_matrix():
    from pathlib import Path

    matrix = capability_matrix()
    document = (
        Path(__file__).parents[1] / "docs" / "SOURCE_CAPABILITIES.md"
    ).read_text(encoding="utf-8")
    for flavor in ("oracle", "databricks", "redshift", "teradata", "impala"):
        caps = matrix[flavor]
        row = next(
            line for line in document.splitlines()
            if line.startswith(f"| {flavor} |")
        )
        expected = [
            caps["support_status"],
            "yes" if caps["supports_view_listing"] else "no",
            "yes" if caps["supports_primary_key_reflection"] else "no",
            "yes" if caps["requires_explicit_split_key"] else "no",
            "yes" if caps["supports_range_key_bounds"] else "no",
            "yes" if caps["supports_modulo_split"] else "no",
            "yes" if caps["supports_ntile"] else "no",
            "yes" if caps["supports_stats_histogram"] else "no",
            "yes" if caps["supports_fast_row_estimate"] else "no",
            "yes" if caps["supports_freshness_probe"] else "no",
            "yes" if caps["supports_deterministic_tokenization"] else "no",
            "yes" if caps["supports_random_tokenization"] else "no",
            "yes" if caps["supports_streaming_query"] else "no",
        ]
        cells = [cell.strip().lower() for cell in row.strip().strip("|").split("|")]
        assert cells[1:] == expected


def test_snapshot_capabilities_advertise_only_implemented_providers():
    matrix = capability_matrix()
    assert matrix["postgresql"]["source_snapshot_provider"] == "postgresql_exported"
    assert matrix["postgresql"]["supports_distributed_snapshot"] is True
    assert matrix["postgresql"]["source_snapshot_reopenable"] is False
    assert matrix["mssql"]["source_snapshot_provider"] == "mssql_transaction"
    assert matrix["mssql"]["supports_distributed_snapshot"] is False
    assert matrix["mssql"]["source_snapshot_reopenable"] is False
    for flavor, capabilities in matrix.items():
        if flavor in {"postgresql", "mssql"}:
            continue
        assert capabilities["source_snapshot_provider"] == "none"
        assert capabilities["supports_distributed_snapshot"] is False
        assert capabilities["source_snapshot_reopenable"] is False
