"""Deterministic Manager-side cluster health aggregation."""
from __future__ import annotations

import time
from collections import deque
from threading import Lock
from typing import Any

try:
    import psutil
except ImportError:  # pragma: no cover - enterprise dependencies include psutil
    psutil = None

_history: deque[dict[str, Any]] = deque(maxlen=17280)
_history_lock = Lock()
_network_sample: tuple[float, int, int] | None = None


def _alert(alert_id: str, severity: str, message: str, remediation: str) -> dict[str, str]:
    return {
        "id": alert_id,
        "severity": severity,
        "message": message,
        "remediation": remediation,
    }


def _host_resources() -> dict[str, float | int | None]:
    global _network_sample
    if psutil is None:
        return {
            "cpu_pct": None,
            "memory_used_bytes": None,
            "memory_total_bytes": None,
            "memory_pct": None,
            "disk_used_bytes": None,
            "disk_total_bytes": None,
            "disk_pct": None,
            "network_receive_bytes_per_sec": None,
            "network_transmit_bytes_per_sec": None,
        }
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage(".")
    now = time.time()
    counters = psutil.net_io_counters()
    receive_rate = transmit_rate = 0.0
    if _network_sample is not None:
        previous_time, previous_receive, previous_transmit = _network_sample
        elapsed = now - previous_time
        if elapsed > 0:
            receive_rate = max(0, counters.bytes_recv - previous_receive) / elapsed
            transmit_rate = max(0, counters.bytes_sent - previous_transmit) / elapsed
    _network_sample = (now, counters.bytes_recv, counters.bytes_sent)
    return {
        "cpu_pct": float(psutil.cpu_percent(interval=None)),
        "memory_used_bytes": int(memory.used),
        "memory_total_bytes": int(memory.total),
        "memory_pct": float(memory.percent),
        "disk_used_bytes": int(disk.used),
        "disk_total_bytes": int(disk.total),
        "disk_pct": float(disk.percent),
        "network_receive_bytes_per_sec": receive_rate,
        "network_transmit_bytes_per_sec": transmit_rate,
    }


def aggregate_health(registry, supervisors, *, now: float | None = None) -> dict[str, Any]:
    """Build a fleet health snapshot from registered Agents and supervisors."""
    now = time.monotonic() if now is None else now
    records = registry.list_public() if registry is not None else []
    by_id = {record["agent_id"]: record for record in records}
    agents = []
    alerts = []
    cpu_values = []
    memory_values = []
    supervised_ids = set()

    for supervisor in supervisors:
        supervised_ids.add(supervisor.name)
        record = by_id.get(supervisor.name)
        if record is None:
            agent = {
                "agent_id": supervisor.name,
                "status": "Critical" if not supervisor.is_alive else "Warning",
                "heartbeat": None,
                "health": {},
                "serving_tables": [],
                "restart_count": supervisor.restart_count,
                "crash_looped": supervisor.crash_looped,
                "alive": supervisor.is_alive,
            }
        else:
            age = record["seconds_since_heartbeat"]
            health = record.get("health", {})
            cpu = float(health.get("cpu_pct", 0.0) or 0.0)
            memory = int(health.get("mem_bytes", 0) or 0)
            cpu_values.append(cpu)
            memory_values.append(memory)
            status = "Healthy"
            if age > 6:
                status = "Critical"
                alerts.append(_alert(
                    f"agent-heartbeat-{record['agent_id']}", "Critical",
                    f"Agent {record['agent_id']} heartbeat is stale.",
                    "Check the Agent process and control-plane connectivity.",
                ))
            elif age > 2:
                status = "Warning"
                alerts.append(_alert(
                    f"agent-heartbeat-{record['agent_id']}", "Warning",
                    f"Agent {record['agent_id']} heartbeat is delayed.",
                    "Inspect Agent logs and network latency.",
                ))
            if cpu >= 98:
                status = "Critical"
                alerts.append(_alert(
                    f"agent-cpu-{record['agent_id']}", "Critical",
                    f"Agent {record['agent_id']} CPU usage is {cpu:.1f}%.",
                    "Reduce load or add Agent capacity.",
                ))
            elif cpu >= 90:
                status = "Warning"
                alerts.append(_alert(
                    f"agent-cpu-{record['agent_id']}", "Warning",
                    f"Agent {record['agent_id']} CPU usage is {cpu:.1f}%.",
                    "Review workload and capacity.",
                ))
            agent = {
                "agent_id": record["agent_id"],
                "status": status,
                "heartbeat": age,
                "health": health,
                "serving_tables": record.get("serving_tables", []),
                "restart_count": supervisor.restart_count,
                "crash_looped": supervisor.crash_looped,
                "alive": supervisor.is_alive,
            }
        if supervisor.crash_looped:
            agent["status"] = "Critical"
            alerts.append(_alert(
                f"agent-crash-loop-{supervisor.name}", "Critical",
                f"Agent {supervisor.name} is in a crash loop.",
                "Inspect the Agent logs before restarting it.",
            ))
        agents.append(agent)

    for record in records:
        agent_id = record["agent_id"]
        if agent_id in supervised_ids:
            continue
        age = record["seconds_since_heartbeat"]
        health = record.get("health", {})
        cpu = float(health.get("cpu_pct", 0.0) or 0.0)
        memory = int(health.get("mem_bytes", 0) or 0)
        cpu_values.append(cpu)
        memory_values.append(memory)
        alive = registry.is_alive(agent_id) if registry is not None else False
        status = "Healthy" if alive else "Critical"
        if not alive:
            alerts.append(_alert(
                f"agent-heartbeat-{agent_id}", "Critical",
                f"Agent {agent_id} heartbeat is stale.",
                "Check the Agent process and control-plane connectivity.",
            ))
        elif age > 2:
            status = "Warning"
            alerts.append(_alert(
                f"agent-heartbeat-{agent_id}", "Warning",
                f"Agent {agent_id} heartbeat is delayed.",
                "Inspect Agent logs and network latency.",
            ))
        if cpu >= 98:
            status = "Critical"
            alerts.append(_alert(
                f"agent-cpu-{agent_id}", "Critical",
                f"Agent {agent_id} CPU usage is {cpu:.1f}%.",
                "Reduce load or add Agent capacity.",
            ))
        elif cpu >= 90 and status != "Critical":
            status = "Warning"
            alerts.append(_alert(
                f"agent-cpu-{agent_id}", "Warning",
                f"Agent {agent_id} CPU usage is {cpu:.1f}%.",
                "Review workload and capacity.",
            ))
        agents.append({
            "agent_id": agent_id,
            "status": status,
            "heartbeat": age,
            "health": health,
            "serving_tables": record.get("serving_tables", []),
            "restart_count": None,
            "crash_looped": False,
            "alive": alive,
        })

    if not agents:
        status = "Critical"
        alerts.append(_alert(
            "cluster-no-agents", "Critical", "No Agents are configured.",
            "Start at least one Agent or configure the fleet.",
        ))
    elif any(a["status"] == "Critical" for a in agents):
        status = "Critical"
    elif any(a["status"] == "Warning" for a in agents):
        status = "Warning"
    else:
        status = "Healthy"

    generated_at = time.time()
    snapshot = {
        "status": status,
        "generated_at": generated_at,
        "agents": agents,
        "resources": {
            "cpu_pct": sum(cpu_values) / len(cpu_values) if cpu_values else None,
            "memory_bytes": sum(memory_values) if memory_values else None,
            "storage": None,
            "network": None,
        },
        "host": _host_resources(),
        "alerts": alerts,
        "events": [],
    }
    with _history_lock:
        _history.append(snapshot)
    return snapshot


def health_history(limit: int = 60, *, hours: float | None = None) -> list[dict[str, Any]]:
    """Return newest health snapshots first, optionally bounded by age."""
    with _history_lock:
        samples = list(_history)
    if hours is not None:
        cutoff = time.time() - max(0.0, float(hours)) * 3600
        samples = [sample for sample in samples if sample["generated_at"] >= cutoff]
    return list(reversed(samples[-max(1, int(limit)):]))


def clear_history() -> None:
    with _history_lock:
        _history.clear()
