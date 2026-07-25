"""Tests for the request-trace / Fabric-timeline observability."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import pytest

import config
from observability import trace


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(config, "WAREHOUSE_PREFIX", "warehouse/db", raising=False)
    monkeypatch.setattr(config, "REQUEST_TRACE", True, raising=False)
    trace.reset()
    yield
    trace.reset()


def test_table_from_key():
    assert trace.table_from_key("warehouse/db/Product/data/split-0-x.parquet") == "Product"
    assert trace.table_from_key("warehouse/db/Product/metadata/v1.metadata.json") == "Product"
    assert trace.table_from_key("") == "-"
    assert trace.table_from_key("some/other/thing") == "-"


def test_classify():
    assert trace.classify("warehouse/db/t/metadata/v1.metadata.json") == "metadata"
    assert trace.classify("warehouse/db/t/metadata/version-hint.text") == "version_hint"
    assert trace.classify("warehouse/db/t/metadata/snap-1.avro") == "manifest"
    assert trace.classify("warehouse/db/t/data/split-0-x.parquet") == "data"
    # Native Delta commit files are their own kind; the _last_checkpoint probe
    # stays a benign "other".
    assert trace.classify("warehouse/db/t/_delta_log/00000000000000000000.json") == "delta_log"
    assert trace.classify("warehouse/db/t/_delta_log/_last_checkpoint") == "other"
    assert trace.classify("") == "list"


def test_record_and_recent_filters():
    trace.record(method="GET", key="warehouse/db/Product/metadata/v1.metadata.json",
                 status=200, duration_ms=3.0)
    trace.record(method="GET", key="warehouse/db/Product/data/split-0-x.parquet",
                 status=200, duration_ms=50.0, resp_bytes=1000)
    trace.record(method="GET", key="warehouse/db/Customer/data/split-9-y.parquet",
                 status=404, duration_ms=1.0)

    all_recs = trace.recent(limit=10)
    assert len(all_recs) == 3
    # newest first
    assert all_recs[0]["table"] == "Customer"

    assert len(trace.recent(table="Product")) == 2
    assert len(trace.recent(kind="data")) == 2
    assert len(trace.recent(status=404)) == 1


def test_timeline_aggregation():
    for i in range(3):
        trace.record(method="GET", key=f"warehouse/db/Product/data/split-{i}-x.parquet",
                     status=200, duration_ms=10.0, resp_bytes=100)
    trace.record(method="GET", key="warehouse/db/Product/data/split-9-z.parquet",
                 status=404, duration_ms=2.0)

    tl = trace.timeline("Product")["tables"]["Product"]
    assert tl["requests"] == 4
    assert tl["errors"] == 1
    assert tl["by_kind"]["data"]["count"] == 4
    assert tl["by_kind"]["data"]["errors"] == 1
    assert tl["proxy_ms_total"] == pytest.approx(32.0, abs=0.1)
    # first record has no gap; later ones do (may round to 0 when very fast)
    assert tl["fabric_gap_ms_total"] >= 0


def test_trace_disabled_records_nothing(monkeypatch):
    monkeypatch.setattr(config, "REQUEST_TRACE", False, raising=False)
    trace.record(method="GET", key="warehouse/db/Product/data/split-0-x.parquet",
                 status=200, duration_ms=5.0)
    assert trace.recent(limit=10) == []


def test_timeline_distinguishes_probe_404s_from_errors():
    # Benign S3 probes: HEAD on folders / _delta_log / schema.json.gz -> 404.
    trace.record(method="HEAD", key="warehouse/db/Address/", status=404, duration_ms=1)
    trace.record(method="GET", key="warehouse/db/Address/_delta_log", status=404, duration_ms=1)
    trace.record(method="GET", key="warehouse/db/Address/metadata/version-hint.text/",
                 status=404, duration_ms=1)
    # A real error: 404 on an actual data object.
    trace.record(method="GET", key="warehouse/db/Address/data/split-9-z.parquet",
                 status=404, duration_ms=1)
    tl = trace.timeline("Address")["tables"]["Address"]
    assert tl["errors"] == 1          # only the data 404 counts
    assert tl["probe_404s"] == 3      # folder + _delta_log + trailing-slash probes
    assert tl["error_samples"][0]["key"].endswith(".parquet")
