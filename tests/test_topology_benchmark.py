from __future__ import annotations

from benchmarks.topology_benchmark import StageSample, percentile, summarize


def _sample(index: int, duration: float) -> StageSample:
    return StageSample(
        scenario="test",
        location="site-a",
        split_index=index,
        rows=100,
        estimated_source_bytes=1000,
        parquet_bytes=250,
        source_wait_seconds=duration / 4,
        encode_wall_seconds=duration / 4,
        encode_cpu_seconds=duration / 8,
        upload_seconds=duration / 4,
        total_seconds=duration,
        failed_upload_bytes=index,
        sha256="a" * 64,
    )


def test_percentile_and_summary_are_deterministic():
    samples = [
        _sample(0, 1.0),
        _sample(1, 2.0),
        _sample(2, 3.0),
    ]

    assert percentile([1.0, 2.0, 3.0], 0.95) == 3.0
    result = summarize(samples)
    assert result["samples"] == 3
    assert result["rows"] == 300
    assert result["estimated_source_bytes"] == 3000
    assert result["parquet_bytes"] == 750
    assert result["compression_ratio"] == 4.0
    assert result["failed_upload_bytes"] == 3
    assert result["rows_per_second"] == 100.0
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
