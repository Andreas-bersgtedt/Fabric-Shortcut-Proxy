"""Manager implementation of the Agent control-plane contract.

Registration, heartbeat, assignment, durable snapshot lookup, materialization
claim renewal, and task-result acceptance are active behavior. Assignment
remains table-wide rather than per-worker because durable split ownership lives
in the #90 work queue and #96 generation-membership record.
"""
from __future__ import annotations

from typing import Callable

from enterprise.control.contract import (
    RegisterRequest, RegisterResponse, HeartbeatRequest, ControlCommand,
    Assignment, SnapshotManifest, MaterializeTask, TaskResult, Ack,
)
from enterprise.control.egress import EgressEvent, EgressLedger
from enterprise.control.registry import Registry
from enterprise.control.work_queue import DurableWorkQueue
from fabric_shortcut_proxy.observability.logging import get_logger

log = get_logger(__name__)

# Returns the durable published manifest for (table, epoch), or None.
# ``epoch == 0`` means "current".
SnapshotProvider = Callable[[str, int], "SnapshotManifest | None"]


class ControlService:
    """Implements the ControlServer interface over a :class:`Registry`."""

    def __init__(
        self,
        registry: Registry,
        *,
        tables: list[str] | None = None,
        snapshot_provider: SnapshotProvider | None = None,
        work_queue: DurableWorkQueue | None = None,
        leadership_check: Callable[[], None] | None = None,
    ) -> None:
        self._registry = registry
        self._tables = list(tables or [])
        self._snapshot_provider = snapshot_provider
        self._work_queue = work_queue
        self._leadership_check = leadership_check
        self._egress_ledger = (
            EgressLedger(work_queue.store)
            if work_queue is not None
            else None
        )

    def _require_leader(self) -> None:
        if self._leadership_check is not None:
            self._leadership_check()

    # -- Agent lifecycle -----------------------------------------------------

    def register(self, req: RegisterRequest) -> RegisterResponse:
        self._require_leader()
        resp = self._registry.register(req)
        log.info("agent_registered", agent_id=req.agent_id, host=req.host,
                 port=req.port, os=req.os, version=req.version)
        return resp

    def heartbeat(self, req: HeartbeatRequest) -> list[ControlCommand]:
        self._require_leader()
        # May raise LeaseError -> the transport maps it to HTTP 409.
        commands = self._registry.heartbeat(req)
        if self._work_queue is not None:
            self._work_queue.renew_agent_claims(
                req.agent_id,
                req.lease_id,
                task_ids=req.active_task_ids,
            )
        return commands

    def get_assignment(self, agent_id: str) -> Assignment:
        # Serving Agents remain interchangeable; materialization is assigned per task.
        return Assignment(agent_id=agent_id, tables=list(self._tables))

    def get_snapshot(self, table: str, epoch: int) -> SnapshotManifest | None:
        if self._snapshot_provider is None:
            return None
        return self._snapshot_provider(table, epoch)

    def report_task_result(self, res: TaskResult) -> Ack:
        self._require_leader()
        if self._work_queue is not None:
            agent = self._registry.get(res.agent_id)
            if agent is None:
                return Ack(
                    ok=False,
                    terminal=True,
                    reason_code="wrong_owner",
                )
            result = self._work_queue.accept_result(
                res, agent_lease_id=agent.lease_id
            )
            if (
                result.ok
                and result.state == "SUCCEEDED"
                and self._egress_ledger is not None
            ):
                task = self._work_queue.get_task(res.task_id)
                if task is not None:
                    payload = MaterializeTask.from_dict(task["task"])
                    pool = self._registry.placement.pool_for_identity(
                        res.agent_id
                    )
                    profile = (
                        self._registry.placement.stores.get(
                            pool.storage_profile
                        )
                        if pool is not None and pool.storage_profile
                        else None
                    )
                    if (
                        pool is not None
                        and profile is not None
                        and pool.location
                        and profile.location
                        and pool.location != profile.location
                    ):
                        accepted = task.get("result") or {}
                        self._egress_ledger.record(
                            EgressEvent(
                                event_id=(
                                    f"materialize:{res.task_id}:"
                                    f"{int(task['attempt'])}"
                                ),
                                occurred_at_ms=int(
                                    accepted.get(
                                        "completed_at_ms",
                                        task["updated_at_ms"],
                                    )
                                ),
                                tenant_id=payload.tenant_id,
                                dataset_id=(
                                    f"{payload.connection_id}::"
                                    f"{payload.source_table}"
                                ),
                                pool_id=pool.pool_id,
                                source_region=pool.location,
                                destination_region=profile.location,
                                direction="materialization_upload",
                                bytes=int(
                                    accepted.get("size_bytes", 0)
                                ),
                            )
                        )
            return result
        log.info("task_result", agent_id=res.agent_id, table=res.table,
                 epoch=res.epoch, split_index=res.split_index, ok=res.ok,
                 error=res.error or None)
        return Ack(ok=True)

    def egress_report(
        self,
        month: str,
        *,
        pricing: dict | None = None,
    ) -> dict:
        if self._egress_ledger is None:
            return {
                "month": month,
                "currency": None,
                "total_bytes": 0,
                "priced_bytes": 0,
                "unpriced_bytes": 0,
                "estimated_cost": None,
                "rows": [],
            }
        return self._egress_ledger.report(month, pricing=pricing)
