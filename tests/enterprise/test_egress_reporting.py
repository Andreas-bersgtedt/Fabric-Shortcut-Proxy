from __future__ import annotations

from enterprise.control.contract import (
    Ack,
    MaterializeTask,
    RegisterRequest,
    TaskResult,
)
from enterprise.control.egress import (
    EgressEvent,
    EgressLedger,
    parse_pricing,
)
from enterprise.control.placement import PlacementConfig
from enterprise.control.registry import Registry
from enterprise.control.server import ControlService
from enterprise.control.work_queue import DurableWorkQueue
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore


_JANUARY_MS = 1_800_000_000_000


def test_monthly_report_uses_measured_bytes_and_operator_pricing():
    ledger = EgressLedger(MemoryStore())
    event = EgressEvent(
        event_id="task-a:1",
        occurred_at_ms=_JANUARY_MS,
        tenant_id="tenant-a",
        dataset_id="source-a::sales",
        pool_id="pool-a",
        source_region="site-a",
        destination_region="azure:swedencentral",
        direction="materialization_upload",
        bytes=1024**3,
    )
    ledger.record(event)
    ledger.record(event)
    pricing = parse_pricing(
        '{"currency":"USD","rates":'
        '{"site-a->azure:swedencentral":0.025}}'
    )

    report = ledger.report(event.month, pricing=pricing)

    assert report["total_bytes"] == 1024**3
    assert report["priced_bytes"] == 1024**3
    assert report["unpriced_bytes"] == 0
    assert report["estimated_cost"] == "0.025000"
    assert report["rows"] == [
        {
            "tenant_id": "tenant-a",
            "dataset_id": "source-a::sales",
            "pool_id": "pool-a",
            "source_region": "site-a",
            "destination_region": "azure:swedencentral",
            "direction": "materialization_upload",
            "bytes": 1024**3,
            "transfers": 1,
            "price_per_gib": "0.025",
            "estimated_cost": "0.025000",
        }
    ]


def test_report_keeps_unpriced_bytes_visible():
    event = EgressEvent(
        event_id="replica-a",
        occurred_at_ms=_JANUARY_MS,
        tenant_id="tenant-a",
        dataset_id="source-a::sales",
        pool_id="replication",
        source_region="azure:swedencentral",
        destination_region="azure:northeurope",
        direction="replication",
        bytes=1234,
    )
    ledger = EgressLedger(MemoryStore())
    ledger.record(event)

    report = ledger.report(event.month, pricing=parse_pricing("{}"))

    assert report["total_bytes"] == 1234
    assert report["priced_bytes"] == 0
    assert report["unpriced_bytes"] == 1234
    assert report["estimated_cost"] is None
    assert report["rows"][0]["estimated_cost"] is None


def test_task_result_records_only_cross_boundary_bytes(monkeypatch):
    placement = PlacementConfig.from_dict(
        {
            "tenants": ["tenant-a"],
            "pools": [
                {
                    "pool_id": "pool-a",
                    "tenant_id": "tenant-a",
                    "identities": ["agent-a"],
                    "location": "site-a",
                    "storage_profile": "published",
                    "allowed_connection_ids": ["source-a"],
                }
            ],
            "stores": {
                "published": {
                    "tenant_id": "tenant-a",
                    "location": "azure:swedencentral",
                    "provider": "azure",
                }
            },
            "connections": {
                "source-a": {
                    "tenant_id": "tenant-a",
                    "required_pool": "pool-a",
                    "required_storage_profile": "published",
                }
            },
        }
    )
    registry = Registry(placement=placement)
    lease = registry.register(
        RegisterRequest(
            agent_id="agent-a",
            host="127.0.0.1",
            port=9000,
            os="linux",
            version="test",
        )
    )
    queue = DurableWorkQueue(MemoryStore())
    task = MaterializeTask(
        table="sales",
        epoch=1,
        split_index=0,
        source_table="sales",
        output_key="warehouse/0.parquet",
        task_id="task-a",
        request_id="request-a",
        connection_id="source-a",
        tenant_id="tenant-a",
    )
    record = {
        "task": task.to_dict(),
        "attempt": 1,
        "updated_at_ms": _JANUARY_MS,
        "result": {
            "completed_at_ms": _JANUARY_MS,
            "size_bytes": 4096,
        },
    }
    monkeypatch.setattr(
        queue,
        "accept_result",
        lambda _result, *, agent_lease_id: Ack(
            ok=agent_lease_id == lease.lease_id,
            state="SUCCEEDED",
            terminal=True,
        ),
    )
    monkeypatch.setattr(queue, "get_task", lambda _task_id: record)
    service = ControlService(
        registry,
        work_queue=queue,
    )

    ack = service.report_task_result(
        TaskResult(
            agent_id="agent-a",
            table="sales",
            epoch=1,
            split_index=0,
            ok=True,
            task_id="task-a",
            attempt=1,
            size_bytes=4096,
        )
    )

    assert ack.ok
    report = service.egress_report(
        EgressEvent(
            event_id="date",
            occurred_at_ms=_JANUARY_MS,
            tenant_id="",
            dataset_id="date",
            pool_id="date",
            source_region="a",
            destination_region="b",
            direction="date",
            bytes=0,
        ).month
    )
    assert report["total_bytes"] == 4096
    assert report["rows"][0]["tenant_id"] == "tenant-a"
    assert report["rows"][0]["pool_id"] == "pool-a"
