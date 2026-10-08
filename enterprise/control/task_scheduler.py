"""Primary-Manager dispatch of durable materialization tasks."""
from __future__ import annotations

import asyncio
import hashlib
import time

from enterprise.control.contract import ControlCommand
from enterprise.control.contract import MaterializeTask
from enterprise.control.contract import TASK_TERMINAL_STATES
from enterprise.control.registry import Registry
from enterprise.control.placement import MaterializerPolicy
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

    @staticmethod
    def _contract_version(agent: dict) -> tuple[int, int]:
        try:
            major, minor = str(agent.get("contract_version", "1.0")).split(".", 1)
            return int(major), int(minor)
        except (TypeError, ValueError):
            return 0, 0

    def _dataset_scope(
        self, connection_id: str, source_table: str
    ) -> tuple[str, str]:
        if (connection_id, source_table) in self.registry.placement.tables:
            return connection_id, source_table
        return connection_id, ""

    def _placement_eligible(
        self,
        candidates: list[tuple[float, str]],
        *,
        connection_id: str,
        source_table: str,
        policy: MaterializerPolicy,
        active_dataset_claims: dict[tuple[str, str], int],
        active_pool_claims: dict[str, int],
    ) -> list[tuple[float, str]]:
        placement = self.registry.placement
        if not placement.enabled:
            return candidates
        scope = self._dataset_scope(connection_id, source_table)
        if (
            policy.max_concurrency is not None
            and active_dataset_claims.get(scope, 0) >= policy.max_concurrency
        ):
            return []
        pinning = bool(
            policy.required_pool
            or policy.required_location
            or policy.required_storage_profile
            or policy.fallback_pools
        )
        targets = (
            ([policy.required_pool] if policy.required_pool else [])
            + list(policy.fallback_pools)
        )
        public_agents = {
            str(item["agent_id"]): item
            for item in self.registry.list_public()
        }

        def matching(candidate: tuple[float, str], target: str = "") -> bool:
            _score, agent_id = candidate
            agent = public_agents.get(agent_id, {})
            if pinning and self._contract_version(agent) < (1, 1):
                return False
            pool_id = str(agent.get("pool_id", ""))
            pool = placement.pools.get(pool_id)
            if pool is None or (target and pool_id != target):
                return False
            if not pool.allows(connection_id, source_table):
                return False
            if (
                policy.required_location
                and pool.location != policy.required_location
            ):
                return False
            if (
                policy.required_storage_profile
                and pool.storage_profile != policy.required_storage_profile
            ):
                return False
            if (
                pool.max_concurrency is not None
                and active_pool_claims.get(pool_id, 0) >= pool.max_concurrency
            ):
                return False
            return True

        if targets:
            for target in targets:
                eligible = [candidate for candidate in candidates if matching(candidate, target)]
                if eligible:
                    return eligible
            return []
        eligible = [candidate for candidate in candidates if matching(candidate)]
        return eligible

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
            generation_payloads = [
                MaterializeTask.from_dict(current["task"])
                for current in all_tasks
                if (
                    current.get("state") not in TASK_TERMINAL_STATES
                    and str(
                        (current.get("task") or {}).get("generation_id", "")
                    )
                    == payload.generation_id
                )
            ]
            eligible_workers = []
            for worker in workers:
                agent_id = str(worker["agent_id"])
                if any(
                    self._placement_eligible(
                        [(0.0, agent_id)],
                        connection_id=generation_payload.connection_id,
                        source_table=generation_payload.source_table,
                        policy=self.registry.placement.policy_for(
                            generation_payload.connection_id,
                            generation_payload.source_table,
                        ),
                        active_dataset_claims={},
                        active_pool_claims={},
                    )
                    for generation_payload in generation_payloads
                ):
                    eligible_workers.append(worker)
            membership = self.queue.reconcile_membership(
                payload.generation_id,
                payload.generation_fence,
                eligible_workers,
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
        active_dataset_claims: dict[tuple[str, str], int] = {}
        active_pool_claims: dict[str, int] = {}
        for current in all_tasks:
            if current["state"] != "CLAIMED":
                continue
            agent_id = str((current.get("claim") or {}).get("agent_id", ""))
            if agent_id:
                active_claims[agent_id] = active_claims.get(agent_id, 0) + 1
                payload = MaterializeTask.from_dict(current["task"])
                scope = self._dataset_scope(
                    payload.connection_id, payload.source_table
                )
                active_dataset_claims[scope] = (
                    active_dataset_claims.get(scope, 0) + 1
                )
                worker = self.registry.get(agent_id)
                if worker is not None and worker.pool_id:
                    active_pool_claims[worker.pool_id] = (
                        active_pool_claims.get(worker.pool_id, 0) + 1
                    )
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
            policy = self.registry.placement.policy_for(
                task_payload.connection_id,
                task_payload.source_table,
            )
            agents = self._placement_eligible(
                agents,
                connection_id=task_payload.connection_id,
                source_table=task_payload.source_table,
                policy=policy,
                active_dataset_claims=active_dataset_claims,
                active_pool_claims=active_pool_claims,
            )
            if not agents:
                if self.queue.set_dispatch_status(
                    task["task_id"], "no_eligible_materializer"
                ):
                    from fabric_shortcut_proxy.observability import metrics
                    from fabric_shortcut_proxy.observability.audit import (
                        record_placement_decision,
                    )

                    metrics.inc_counter(
                        "materialization_placement_decisions_total",
                        outcome="no_eligible_materializer",
                    )
                    record_placement_decision(
                        task_id=str(task["task_id"]),
                        request_id=task_payload.request_id,
                        dataset=(
                            f"{task_payload.connection_id}::"
                            f"{task_payload.source_table}"
                        ),
                        pool_id=(
                            policy.required_pool
                            or ",".join(policy.fallback_pools)
                            or "any-authorized-pool"
                        ),
                        location=(
                            policy.required_location
                            or "any-authorized-location"
                        ),
                        outcome="queued",
                        reason="no_eligible_materializer",
                        fallback=False,
                    )
                continue
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
            scope = self._dataset_scope(
                task_payload.connection_id, task_payload.source_table
            )
            active_dataset_claims[scope] = active_dataset_claims.get(scope, 0) + 1
            worker = self.registry.get(agent_id)
            actual_pool_id = worker.pool_id if worker is not None else ""
            if actual_pool_id:
                active_pool_claims[actual_pool_id] = (
                    active_pool_claims.get(actual_pool_id, 0) + 1
                )
            from fabric_shortcut_proxy.observability import metrics
            from fabric_shortcut_proxy.observability.audit import (
                record_placement_decision,
            )

            metrics.inc_counter(
                "materialization_placement_decisions_total",
                outcome="assigned",
                pool=actual_pool_id or "unmapped",
            )
            record_placement_decision(
                task_id=command_task.task_id,
                request_id=command_task.request_id,
                dataset=(
                    f"{command_task.connection_id}::"
                    f"{command_task.source_table}"
                ),
                pool_id=actual_pool_id,
                location=worker.location if worker is not None else "",
                outcome="assigned",
                fallback=bool(
                    policy.required_pool
                    and actual_pool_id != policy.required_pool
                ),
            )
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
            "placement_enabled": self.registry.placement.enabled,
            "placement_pool_count": len(self.registry.placement.pools),
            "checked_at_ms": int(time.time() * 1000),
        }
