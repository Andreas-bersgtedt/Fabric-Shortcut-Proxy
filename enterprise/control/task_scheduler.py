"""Primary-Manager dispatch of durable materialization tasks."""
from __future__ import annotations

import asyncio
import hashlib
import time

from enterprise.control.contract import ControlCommand
from enterprise.control.contract import MaterializeTask
from enterprise.control.registry import Registry
from enterprise.control.work_queue import DurableWorkQueue, WorkQueueConflict
from observability.logging import get_logger

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
    ) -> None:
        self.queue = queue
        self.registry = registry
        self.manager_owner = manager_owner
        self.manager_fence = int(manager_fence)
        self.max_inflight_per_agent = max(1, int(max_inflight_per_agent))
        self.scan_interval_seconds = max(0.05, float(scan_interval_seconds))
        self._running = False
        self._task: asyncio.Task | None = None

    def _eligible_agents(
        self,
        *,
        active_claims: dict[str, int] | None = None,
        required_owner_shard: int | None = None,
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
            if "materializer" not in set(public.get("capabilities", [])):
                continue
            if public.get("draining"):
                continue
            if (
                required_owner_shard is not None
                and int(public.get("shard_index", -1)) != required_owner_shard
            ):
                continue
            inflight = int((public.get("health") or {}).get("inflight", 0))
            agent_claims = claims.get(agent_id, 0)
            if agent_claims >= inflight_limit:
                continue
            result.append((inflight + agent_claims, agent_id))
        return result

    @staticmethod
    def _tie_break(task_id: str, agent_id: str) -> str:
        return hashlib.sha256(f"{task_id}|{agent_id}".encode()).hexdigest()

    def dispatch_once(self) -> int:
        self.queue.expire_claims()
        dispatched = 0
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
            from runtime.generation import current_generation

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
                required_owner_shard=required_owner_shard
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
                command_task = self.queue.claim_task(
                    task["task_id"],
                    agent_id=agent_id,
                    agent_lease_id=record.lease_id,
                    manager_owner=self.manager_owner,
                    manager_fence=self.manager_fence,
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
            "checked_at_ms": int(time.time() * 1000),
        }
