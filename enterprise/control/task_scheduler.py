"""Primary-Manager dispatch of durable materialization tasks."""
from __future__ import annotations

import asyncio
import hashlib
import time

from enterprise.control.contract import ControlCommand
from enterprise.control.contract import MaterializeTask
from enterprise.control.contract import TASK_TERMINAL_STATES
from enterprise.control.registry import Registry
from enterprise.control.work_queue import DurableWorkQueue, WorkQueueConflict
from fabric_shortcut_proxy.observability.logging import get_logger

log = get_logger(__name__)


class TaskScheduler:
    def __init__(
        self,
        queue: DurableWorkQueue,
        registry: Registry,
        *,
        manager_owner: str,
        manager_fence: int,
        max_inflight_per_agent: int = 2,
        scan_interval_seconds: float = 1.0,
        membership_policy: str = "fixed",
    ) -> None:
        self.queue = queue
        self.registry = registry
        self.manager_owner = manager_owner
        self.manager_fence = int(manager_fence)
        self.max_inflight_per_agent = max(1, int(max_inflight_per_agent))
        self.scan_interval_seconds = max(0.05, float(scan_interval_seconds))
        self.membership_policy = str(membership_policy).strip().lower()
        if self.membership_policy not in {"fixed", "elastic"}:
            raise ValueError("membership_policy must be 'fixed' or 'elastic'")
        self._running = False
        self._task: asyncio.Task | None = None

    def _eligible_agents(
        self,
        *,
        active_claims: dict[str, int] | None = None,
        required_owner_shard: int | None = None,
        allowed_workers: set[str] | None = None,
    ):
        result = []
        claims = active_claims or {}
        inflight_limit = (
            1
            if required_owner_shard is not None
            else self.max_inflight_per_agent
        )
        for public in self.registry.list_public():
            agent_id = str(public["agent_id"])
            if not self.registry.is_alive(agent_id):
                continue
            if allowed_workers is not None and agent_id not in allowed_workers:
                continue
            if "materializer" not in set(public.get("capabilities", [])):
                continue
            if public.get("draining"):
                continue
            if (
                required_owner_shard is not None
                and int(public.get("shard_index", -1)) != required_owner_shard
            ):
                continue
            capacity = max(1, int(public.get("capacity_hint", 0) or 1))
            inflight = int((public.get("health") or {}).get("inflight", 0))
            agent_claims = claims.get(agent_id, 0)
            effective_limit = (
                inflight_limit
                if required_owner_shard is not None
                else inflight_limit * capacity
            )
            if agent_claims >= effective_limit:
                continue
            result.append(((inflight + agent_claims) / capacity, agent_id))
        return result

    @staticmethod
    def _tie_break(task_id: str, agent_id: str) -> str:
        return hashlib.sha256(f"{task_id}|{agent_id}".encode()).hexdigest()

    def dispatch_once(self) -> int:
        self.queue.expire_claims()
        dispatched = 0
        all_tasks = self.queue.list_tasks()
        workers = []
        for public in self.registry.list_public():
            agent_id = str(public["agent_id"])
            record = self.registry.get(agent_id)
            if (
                record is None
                or not self.registry.is_alive(agent_id)
                or "materializer" not in set(public.get("capabilities", []))
                or public.get("draining")
            ):
                continue
            workers.append({
                "agent_id": agent_id,
                "lease_id": record.lease_id,
                "capacity": max(
                    1,
                    int(public.get("capacity_hint", 0) or 1),
                ),
            })
        memberships = {}
        for task in all_tasks:
            if task.get("state") in TASK_TERMINAL_STATES:
                continue
            payload = MaterializeTask.from_dict(task["task"])
            if not payload.generation_id or payload.generation_id in memberships:
                continue
            membership = self.queue.reconcile_membership(
                payload.generation_id,
                payload.generation_fence,
                workers,
                policy=self.membership_policy,
            )
            self.queue.reassign_fenced_claims(
                payload.generation_id,
                membership,
            )
            memberships[payload.generation_id] = self.queue.get_membership(
                payload.generation_id
            ) or membership
        all_tasks = self.queue.list_tasks()
        active_claims: dict[str, int] = {}
        for current in all_tasks:
            if current["state"] != "CLAIMED":
                continue
            agent_id = str((current.get("claim") or {}).get("agent_id", ""))
            if agent_id:
                active_claims[agent_id] = active_claims.get(agent_id, 0) + 1
        for task in self.queue.runnable_tasks(
            tasks=all_tasks, expire=False
        ):
            from fabric_shortcut_proxy.runtime.generation import current_generation

            generation = current_generation(self.queue.store)
            task_payload = MaterializeTask.from_dict(task["task"])
            if generation is not None and (
                task_payload.generation_id != generation.generation_id
                or task_payload.generation_fence != generation.fence
                or task_payload.plan_sha256 != generation.plan_sha256
            ):
                self.queue.cancel_request(
                    task_payload.request_id, "generation_fenced"
                )
                continue
            required_owner_shard = None
            membership = memberships.get(task_payload.generation_id)
            active_members = None
            if membership is not None:
                active_members = {
                    agent_id
                    for agent_id, worker in (
                        membership.get("workers") or {}
                    ).items()
                    if worker.get("state") == "active"
                }
            if generation is not None:
                table_id = (
                    f"{task_payload.connection_id}::{task_payload.source_table}"
                )
                plan = next(
                    (
                        item
                        for item in generation.table_plans
                        if item.table_id == table_id
                    ),
                    None,
                )
                if (
                    plan is not None
                    and plan.descriptor.provider == "mssql_transaction"
                ):
                    required_owner_shard = plan.descriptor.owner_shard
            agents = self._eligible_agents(
                active_claims=active_claims,
                required_owner_shard=required_owner_shard,
                allowed_workers=active_members,
            )
            if not agents:
                break
            _, agent_id = min(
                agents,
                key=lambda item: (
                    item[0],
                    self._tie_break(task["task_id"], item[1]),
                ),
            )
            record = self.registry.get(agent_id)
            if record is None:
                continue
            try:
                membership_worker = (
                    (membership.get("workers") or {}).get(agent_id, {})
                    if membership is not None else {}
                )
                command_task = self.queue.claim_task(
                    task["task_id"],
                    agent_id=agent_id,
                    agent_lease_id=record.lease_id,
                    manager_owner=self.manager_owner,
                    manager_fence=self.manager_fence,
                    membership_version=int(
                        (membership or {}).get("membership_version", 0)
                    ),
                    worker_fence=int(
                        membership_worker.get("worker_fence", 0)
                    ),
                )
            except WorkQueueConflict:
                continue
            command = ControlCommand(
                kind="materialize", materialize=command_task
            )
            if not self.registry.queue_command(agent_id, command):
                self.queue.release_claim(
                    command_task.task_id,
                    command_task.claim_token,
                    reason="agent_unregistered_before_delivery",
                )
                continue
            dispatched += 1
            active_claims[agent_id] = active_claims.get(agent_id, 0) + 1
            log.info(
                "materialize_task_claimed",
                task_id=command_task.task_id,
                request_id=command_task.request_id,
                agent_id=agent_id,
                attempt=command_task.attempt,
                membership_version=command_task.membership_version,
                worker_fence=command_task.worker_fence,
            )
        return dispatched

    async def _loop(self) -> None:
        try:
            while self._running:
                try:
                    await asyncio.to_thread(self.dispatch_once)
                except Exception:
                    log.exception("materialize_scheduler_error")
                await asyncio.sleep(self.scan_interval_seconds)
        except asyncio.CancelledError:
            raise

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(
            self._loop(), name="materialize-task-scheduler"
        )

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def status(self) -> dict:
        return {
            **self.queue.status(),
            "scheduler_running": self._running,
            "manager_owner": self.manager_owner,
            "manager_fence": self.manager_fence,
            "membership_policy": self.membership_policy,
            "checked_at_ms": int(time.time() * 1000),
        }
