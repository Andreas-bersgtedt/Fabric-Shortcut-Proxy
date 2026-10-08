"""Phase 0 routed PoC acceptance tests (#119).

Covers the control-plane flow a remote materializer follows through the
agent-control ingress: register, receive work, lose heartbeat, have the claim
requeued to a replacement, stale result rejection and snapshot publish.
The Helm tests check the chart profiles added for #114-#117 and are skipped
when the helm binary is not installed.
"""

from __future__ import annotations

import hashlib
import io
import shutil
import subprocess
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from enterprise.control.contract import (
    HeartbeatRequest,
    MaterializeTask,
    RegisterRequest,
    SnapshotManifest,
    SplitRef,
    TaskResult,
)
from enterprise.control.registry import Registry
from enterprise.control.task_scheduler import TaskScheduler
from enterprise.control.work_queue import DurableWorkQueue
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore

REPO_ROOT = Path(__file__).resolve().parents[2]
CHART_DIR = REPO_ROOT / "deploy" / "helm" / "fabric-shortcut-proxy"
REMOTE_VALUES = CHART_DIR / "values-remote-materializer.yaml"
METADATA_KEY = "warehouse/sales/metadata.json"
EXCLUDED_ROUTES = ("/control/work-queue", "/_config", "/_manager", "/_monitor")


def _register(registry: Registry, agent_id: str):
    return registry.register(
        RegisterRequest(
            agent_id=agent_id,
            host="10.20.30.40",
            port=9000 + len(registry.list_public()),
            os="linux",
            version="phase0",
            capabilities=["materializer"],
            shard_index=-1,
            capacity_hint=0,
        )
    )


def _parquet_bytes() -> bytes:
    sink = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2]}), sink)
    return sink.getvalue()


def _result(claimed: MaterializeTask, agent_id: str, data: bytes) -> TaskResult:
    return TaskResult(
        agent_id=agent_id,
        table=claimed.table,
        epoch=claimed.epoch,
        split_index=claimed.split_index,
        ok=True,
        size_bytes=len(data),
        record_count=2,
        content_hash=hashlib.sha256(data).hexdigest(),
        task_id=claimed.task_id,
        request_id=claimed.request_id,
        claim_token=claimed.claim_token,
        attempt=claimed.attempt,
        generation_id=claimed.generation_id,
        generation_fence=claimed.generation_fence,
        plan_sha256=claimed.plan_sha256,
        membership_version=claimed.membership_version,
        worker_fence=claimed.worker_fence,
    )


def _queue(task_count: int) -> tuple[DurableWorkQueue, dict]:
    queue = DurableWorkQueue(MemoryStore())
    tasks = [
        MaterializeTask(
            table="sales",
            epoch=1,
            split_index=index,
            source_table="sales",
            output_key=f"warehouse/sales/{index}.parquet",
        )
        for index in range(task_count)
    ]
    request = queue.create_request(
        requested_key=METADATA_KEY,
        table="sales",
        epoch=1,
        table_format="iceberg",
        generation_id="generation-1",
        generation_fence=1,
        plan_sha256="a" * 64,
        tasks=tasks,
        deadline_ms=int(time.time() * 1000) + 300_000,
    )
    return queue, request


def _scheduler(queue: DurableWorkQueue, registry: Registry) -> TaskScheduler:
    return TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
        max_inflight_per_agent=1,
        membership_policy="elastic",
    )


def _claim(registry: Registry, agent_id: str, lease_id: str) -> MaterializeTask:
    commands = registry.heartbeat(
        HeartbeatRequest(agent_id=agent_id, lease_id=lease_id)
    )
    assert len(commands) == 1
    return commands[0].materialize


def test_remote_agent_registers_and_receives_assignment():
    queue, request = _queue(task_count=1)
    registry = Registry()
    lease = _register(registry, "site-b-materializer")
    assert lease.lease_id
    assert "site-b-materializer" in {
        agent["agent_id"] if isinstance(agent, dict) else agent.agent_id
        for agent in registry.list_public()
    }

    assert _scheduler(queue, registry).dispatch_once() == 1
    claim = _claim(registry, "site-b-materializer", lease.lease_id)
    assert claim.request_id == request["request_id"]
    assert claim.task_id in request["task_ids"]
    assert claim.claim_token


def test_heartbeat_loss_requeues_rejects_stale_result_and_publishes_once():
    queue, request = _queue(task_count=2)
    registry = Registry(heartbeat_ms=1000, miss_limit=1)
    site_a = _register(registry, "site-a-materializer")
    site_b = _register(registry, "site-b-materializer")
    scheduler = _scheduler(queue, registry)
    assert scheduler.dispatch_once() == 2

    data = _parquet_bytes()
    claim_a = _claim(registry, "site-a-materializer", site_a.lease_id)
    stale_claim = _claim(registry, "site-b-materializer", site_b.lease_id)
    queue.store.put(claim_a.output_key, data)
    assert queue.accept_result(
        _result(claim_a, "site-a-materializer", data),
        agent_lease_id=site_a.lease_id,
    ).ok

    # Site B stops heartbeating across the WAN; its claim must expire and be
    # handed to a replacement agent with a new claim token.
    registry.get("site-b-materializer").last_seen -= 10
    site_c = _register(registry, "site-c-materializer")
    assert scheduler.dispatch_once() == 1
    replacement = _claim(registry, "site-c-materializer", site_c.lease_id)
    assert replacement.task_id == stale_claim.task_id
    assert replacement.claim_token != stale_claim.claim_token

    assert not queue.accept_result(
        _result(stale_claim, "site-b-materializer", data),
        agent_lease_id=site_b.lease_id,
    ).ok
    queue.store.put(replacement.output_key, data)
    assert queue.accept_result(
        _result(replacement, "site-c-materializer", data),
        agent_lease_id=site_c.lease_id,
    ).ok

    assert queue.get_request(request["request_id"])["state"] == "SUCCEEDED"
    assert queue.status()["membership"]["reassignment_count"] == 1

    queue.store.put(METADATA_KEY, b"{}")
    content_hash = hashlib.sha256(data).hexdigest()
    manifest = SnapshotManifest(
        table="sales",
        epoch=1,
        table_format="iceberg",
        splits=[
            SplitRef(
                object_key=claim.output_key,
                size_bytes=len(data),
                record_count=2,
                content_hash=content_hash,
            )
            for claim in sorted(
                (claim_a, replacement), key=lambda item: item.split_index
            )
        ],
        metadata_keys=[METADATA_KEY],
        generation_id="generation-1",
        generation_fence=1,
        plan_sha256="a" * 64,
        request_id=request["request_id"],
    )
    queue.publish_snapshot(request["request_id"], manifest)
    assert len(queue.store.list("_control/work-queue/v1/snapshots/")) == 1


helm = pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")


def _helm_template(*args: str) -> str:
    completed = subprocess.run(
        ["helm", "template", "fsp", str(CHART_DIR), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout


def _documents(rendered: str) -> list[str]:
    return [doc for doc in rendered.split("\n---") if doc.strip()]


def _kind_name(doc: str) -> tuple[str, str]:
    kind = name = ""
    in_metadata = False
    for line in doc.splitlines():
        if line.startswith("kind:"):
            kind = line.split(":", 1)[1].strip()
        elif line.startswith("metadata:"):
            in_metadata = True
        elif in_metadata and line.startswith("  name:") and not name:
            name = line.split(":", 1)[1].strip()
        elif line and not line.startswith(" "):
            in_metadata = False
    return kind, name


@helm
def test_default_render_points_agents_at_in_cluster_manager():
    rendered = _helm_template()
    assert "MANAGER_URL: http://fsp-manager:9200" in rendered
    resources = {_kind_name(doc) for doc in _documents(rendered)}
    assert ("Deployment", "fsp-manager") in resources
    assert ("Ingress", "fsp-agent-control") not in resources


@helm
def test_remote_materializer_profile_renders_only_materializer_side():
    rendered = _helm_template("-f", str(REMOTE_VALUES))
    resources = {_kind_name(doc) for doc in _documents(rendered)}
    names = {name for _, name in resources}
    assert ("Deployment", "fsp-manager") not in resources
    assert not any("nginx" in name for name in names)
    assert not any("cpp-agent" in name for name in names)
    assert ("Ingress", "fsp-agent-control") not in resources
    assert ("NetworkPolicy", "fsp-materializer-egress") in resources
    assert "MANAGER_URL: https://fsp-control.example.com" in rendered
    egress = next(
        doc
        for doc in _documents(rendered)
        if _kind_name(doc) == ("NetworkPolicy", "fsp-materializer-egress")
    )
    assert "203.0.113.10/32" in egress


@helm
def test_agent_control_ingress_exposes_only_agent_routes():
    rendered = _helm_template(
        "--set",
        "agentControlIngress.enabled=true,agentControlIngress.host=fsp-control.test",
    )
    ingress = next(
        doc
        for doc in _documents(rendered)
        if _kind_name(doc) == ("Ingress", "fsp-agent-control")
    )
    for route in (
        "/control/register",
        "/control/heartbeat",
        "/control/task-result",
        "/control/materialize",
        "/control/assignment/",
        "/control/snapshot/",
    ):
        assert f"path: {route}" in ingress
    for route in EXCLUDED_ROUTES:
        assert route not in ingress
    assert "fsp-control.test" in ingress


@helm
def test_agent_control_ingress_requires_host_and_manager():
    for extra in (
        "agentControlIngress.enabled=true",
        "agentControlIngress.enabled=true,agentControlIngress.host=x,manager.enabled=false",
    ):
        completed = subprocess.run(
            ["helm", "template", "fsp", str(CHART_DIR), "--set", extra],
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode != 0
