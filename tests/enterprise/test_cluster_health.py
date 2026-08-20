from types import SimpleNamespace

from enterprise.control.cluster_health import aggregate_health, clear_history, health_history


class _Registry:
    def __init__(self, records):
        self.records = records

    def list_public(self):
        return self.records


def _supervisor(name="agent-0", *, alive=True, crash_looped=False):
    return SimpleNamespace(
        name=name, is_alive=alive, crash_looped=crash_looped, restart_count=0,
    )


def _record(agent_id="agent-0", age=0.1, cpu=10.0):
    return {
        "agent_id": agent_id,
        "seconds_since_heartbeat": age,
        "health": {"cpu_pct": cpu, "mem_bytes": 1024, "cache_bytes": 0, "inflight": 0},
        "serving_tables": ["sales"],
    }


def test_healthy_snapshot_and_resource_aggregation():
    clear_history()
    snapshot = aggregate_health(
        _Registry([_record()]), [_supervisor()], now=10.0,
    )
    assert snapshot["status"] == "Healthy"
    assert snapshot["resources"]["cpu_pct"] == 10.0
    assert snapshot["resources"]["memory_bytes"] == 1024
    assert set(snapshot["host"]) == {
        "cpu_pct",
        "memory_used_bytes",
        "memory_total_bytes",
        "memory_pct",
        "disk_used_bytes",
        "disk_total_bytes",
        "disk_pct",
    }
    assert snapshot["alerts"] == []


def test_stale_heartbeat_is_critical_with_remediation():
    clear_history()
    snapshot = aggregate_health(
        _Registry([_record(age=7.0)]), [_supervisor()], now=10.0,
    )
    assert snapshot["status"] == "Critical"
    assert snapshot["alerts"][0]["severity"] == "Critical"
    assert snapshot["alerts"][0]["remediation"]


def test_history_is_newest_first_and_bounded_by_requested_limit():
    clear_history()
    aggregate_health(_Registry([_record()]), [_supervisor()])
    aggregate_health(_Registry([_record(cpu=91.0)]), [_supervisor()])
    history = health_history(1)
    assert len(history) == 1
    assert history[0]["status"] == "Warning"
