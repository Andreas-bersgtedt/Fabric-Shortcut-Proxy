from __future__ import annotations

import pytest

from benchmarks.artifact_throughput import deterministic_chunks
from benchmarks.topology_benchmark import (
    StageSample,
    percentile,
    resolve_benchmark_table,
    summarize,
)


def _sample(index: int, duration: float) -> StageSample:
    return StageSample(
        scenario="test",
        location="site-a",
        split_index=index,
        rows=100,
        estimated_source_bytes=1000,
        parquet_bytes=250,
        upload_bytes=250,
        source_wait_seconds=duration / 4,
        encode_wall_seconds=duration / 4,
        encode_cpu_seconds=duration / 8,
        upload_seconds=duration / 4,
        total_seconds=duration,
        failed_upload_bytes=index,
        retried_upload_bytes=0,
        failed_bytes_measured_remotely=False,
        sha256="a" * 64,
    )


def test_percentile_and_summary_are_deterministic():
    samples = [
        _sample(0, 1.0),
        _sample(1, 2.0),
        _sample(2, 3.0),
    ]

    assert percentile([1.0, 2.0, 3.0], 0.95) == 3.0
    result = summarize(samples, wall_seconds=6.0)
    assert result["samples"] == 3
    assert result["rows"] == 300
    assert result["estimated_source_bytes"] == 3000
    assert result["parquet_bytes"] == 750
    assert result["compression_ratio"] == 4.0
    assert result["failed_upload_bytes"] == 3
    assert result["rows_per_second"] == 50.0
    assert result["total_seconds"] == {
        "min": 1.0,
        "median": 2.0,
        "p95": 3.0,
        "max": 3.0,
    }


def test_sample_rates_use_measured_totals():
    sample = _sample(0, 2.0)

    assert sample.rows_per_second == 50.0
    assert sample.upload_bytes_per_second == 500.0
    assert sample.to_dict()["compression_ratio"] == 4.0


def test_summary_uses_scenario_wall_time_for_concurrency_waves():
    samples = [_sample(index, 2.0) for index in range(8)]

    result = summarize(samples, wall_seconds=4.0)

    assert result["rows"] == 800
    assert result["rows_per_second"] == 200.0


def test_throughput_chunks_are_bounded_and_exact():
    chunks = list(deterministic_chunks((1 << 20) + 17))

    assert [len(chunk) for chunk in chunks] == [1 << 20, 17]
    assert len(b"".join(chunks)) == (1 << 20) + 17


@pytest.mark.asyncio
async def test_reflected_benchmark_table_uses_integer_primary_key(
    monkeypatch,
):
    from fabric_shortcut_proxy.config import ColumnDef

    async def schema(_source, _connection):
        return [
            ColumnDef(
                field_id=1,
                name="AuditLogId",
                iceberg_type="long",
                nullable=False,
            )
        ]

    async def primary_key(_source, _connection):
        return ["AuditLogId"]

    monkeypatch.setattr(
        "benchmarks.topology_benchmark.derive_table_schema",
        schema,
    )
    monkeypatch.setattr(
        "benchmarks.topology_benchmark.reflect_primary_key",
        primary_key,
    )

    table = await resolve_benchmark_table(
        source_table="dbo.PBICleanedAuditlogs",
        connection_id="Sales_DW",
        key_column="",
        split_count=4,
        split_strategy="range",
    )

    assert table.source_table == "dbo.PBICleanedAuditlogs"
    assert table.connection_id == "Sales_DW"
    assert table.key_column == "AuditLogId"
    assert table.num_splits == 4
    assert table.split_strategy == "range"
