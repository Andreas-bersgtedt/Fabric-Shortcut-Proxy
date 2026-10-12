from __future__ import annotations

import pytest

from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.observability import metrics


@pytest.fixture(autouse=True)
def reset_metrics():
    metrics.reset()
    yield
    metrics.reset()


def test_source_latency_is_exported_by_location(monkeypatch) -> None:
    monkeypatch.setattr(config, "AGENT_LOCATION", "site-a", raising=False)

    metrics.record_sql(0.02)
    metrics.record_sql(0.3, error=True)

    snapshot = metrics.snapshot()
    source = snapshot["source_sql_latency"]["site-a"]
    assert source["count"] == 2
    assert source["errors"] == 1
    assert source["buckets_le"]["0.025"] == 1
    assert source["buckets_le"]["0.5"] == 2

    rendered = metrics.render_prometheus()
    assert (
        'fsp_source_query_duration_seconds_count{location="site-a"} 2'
        in rendered
    )
    assert (
        'fsp_source_query_duration_seconds_bucket'
        '{location="site-a",le="+Inf"} 2'
        in rendered
    )


def test_phase3_operational_metrics_export_by_location(monkeypatch) -> None:
    monkeypatch.setattr(config, "AGENT_LOCATION", "site-b", raising=False)

    metrics.set_location_gauges(
        "fsp_materialization_queue_depth",
        {"site-b": 3.0, "unassigned": 1.0},
    )
    metrics.record_assignment_rejection("site-b", "no_eligible_materializer")
    metrics.record_artifact_upload(4096, 0.5)
    metrics.record_agent_heartbeat_ages(
        {("site-b", "site-b-agent-0"): 7.0},
        {("site-b", "site-b-agent-0"): 50.0},
    )

    snapshot = metrics.snapshot()
    counters = snapshot["counters"]
    assert counters["fsp_assignment_rejections_total"][0]["labels"] == {
        "location": "site-b",
        "reason": "no_eligible_materializer",
    }
    assert counters["fsp_artifact_upload_bytes_total"][0]["value"] == 4096
    assert counters["fsp_artifact_upload_duration_seconds_total"][0]["value"] == 0.5
    assert snapshot["gauges"]["fsp_materialization_queue_depth"]
    heartbeat = snapshot["gauges"]["fsp_agent_heartbeat_age_seconds"][0]
    assert heartbeat["labels"] == {
        "agent_id": "site-b-agent-0",
        "location": "site-b",
    }
    assert heartbeat["value"] == 7.0
    timeout = snapshot["gauges"]["fsp_agent_heartbeat_timeout_seconds"][0]
    assert timeout["labels"] == heartbeat["labels"]
    assert timeout["value"] == 50.0
    metrics.record_agent_heartbeat_ages({}, {})
    assert metrics.snapshot()["gauges"]["fsp_agent_heartbeat_timeout_seconds"] == []

    metrics.set_location_gauges("fsp_materialization_queue_depth", {})
    assert metrics.snapshot()["gauges"]["fsp_materialization_queue_depth"] == []
