"""Durable Manager materialization requests, tasks, claims, and snapshots."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import secrets
import threading
import time
from dataclasses import replace
from typing import Callable

import pyarrow.parquet as pq

import config
from enterprise.control.contract import (
    Ack,
    MaterializeTask,
    SnapshotManifest,
    TaskResult,
    TASK_CANCELLED,
    TASK_CLAIMED,
    TASK_FAILED,
    TASK_EXPIRED,
    TASK_QUEUED,
    TASK_RETRY_WAIT,
    TASK_SUCCEEDED,
    TASK_TERMINAL_STATES,
    RESULT_ACCEPTED,
    RESULT_CONFLICT,
    RESULT_DUPLICATE,
    RESULT_INVALID_OUTPUT,
    RESULT_REJECTED,
    RESULT_STALE_CLAIM,
    RESULT_STALE_GENERATION,
    RESULT_WRONG_OWNER,
)
from enterprise.control.lease import LEASE_KEY, StaleLeaderError
from runtime.artifact_store import ArtifactStore, ObjectNotFound
from observability import metrics

PREFIX = "_control/work-queue/v1"
REQUESTS_PREFIX = f"{PREFIX}/requests"
TASKS_PREFIX = f"{PREFIX}/tasks"
RESULTS_PREFIX = f"{PREFIX}/results"
SNAPSHOTS_PREFIX = f"{PREFIX}/snapshots"
IDEMPOTENCY_PREFIX = f"{PREFIX}/idempotency"
MEMBERSHIP_PREFIX = f"{PREFIX}/memberships"


class WorkQueueError(RuntimeError):
    pass


class WorkQueueConflict(WorkQueueError):
    pass


def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_bytes(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _safe_error(value: str) -> str:
    return config.redact_db_url(str(value or ""))[:500]


def _request_key(request_id: str) -> str:
    return f"{REQUESTS_PREFIX}/{request_id}.json"


def _task_key(task_id: str) -> str:
    return f"{TASKS_PREFIX}/{task_id}.json"


def _result_key(task_id: str, attempt: int) -> str:
    return f"{RESULTS_PREFIX}/{task_id}/{attempt}.json"


def _snapshot_key(table: str, epoch: int) -> str:
    return f"{SNAPSHOTS_PREFIX}/{_hash(table)}/{int(epoch):020d}.json"


def _idempotency_key(value: str) -> str:
    return f"{IDEMPOTENCY_PREFIX}/{_hash(value)}.json"


def _membership_key(generation_id: str) -> str:
    return f"{MEMBERSHIP_PREFIX}/{_hash(generation_id)}.json"


class DurableWorkQueue:
    """Single-leader durable queue over the shared artifact store."""

    def __init__(
        self,
        store: ArtifactStore,
        *,
        task_lease_seconds: int = 120,
        max_attempts: int = 3,
        retry_backoff_seconds: int = 5,
        retry_max_seconds: int = 60,
        leadership_check: Callable[[], dict] | None = None,
    ) -> None:
        self.store = store
        self.task_lease_seconds = max(1, int(task_lease_seconds))
        self.max_attempts = max(1, int(max_attempts))
        self.retry_backoff_seconds = max(0, int(retry_backoff_seconds))
        self.retry_max_seconds = max(
            self.retry_backoff_seconds, int(retry_max_seconds)
        )
        self._lock = threading.RLock()
        self._leadership_check = leadership_check

    def _read(self, key: str) -> dict | None:
        try:
            raw = self.store.get(key)
        except ObjectNotFound:
            return None
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WorkQueueError(f"invalid queue record {key}: {exc}") from exc
        if not isinstance(value, dict):
            raise WorkQueueError(f"invalid queue record {key}: expected object")
        return value

    def _write(self, key: str, value: dict) -> None:
        if self._leadership_check is not None:
            term = self._leadership_check()
            value = dict(value)
            value["manager_owner"] = str(term["owner_id"])
            value["manager_fence"] = int(term["fence"])
            committed = self.store.fenced_put(
                LEASE_KEY,
                value["manager_owner"],
                value["manager_fence"],
                key,
                _json_bytes(value),
            )
            if not committed:
                raise StaleLeaderError("Manager leadership changed before queue write")
        else:
            self.store.put(key, _json_bytes(value))
        if self._read(key) != value:
            raise WorkQueueError(f"queue record verification failed: {key}")

    def _delete(self, key: str) -> bool:
        if self._leadership_check is None:
            return self.store.delete(key)
        term = self._leadership_check()
        deleted = self.store.fenced_delete(
            LEASE_KEY,
            str(term["owner_id"]),
            int(term["fence"]),
            key,
        )
        if deleted is None:
            raise StaleLeaderError("Manager leadership changed before queue delete")
        return deleted

    def _guarded_write_batch(
        self,
        guard_key: str,
        expected_guard: bytes,
        values: dict[str, dict],
    ) -> None:
        prepared = {key: dict(value) for key, value in values.items()}
        if self._leadership_check is not None:
            term = self._leadership_check()
            for value in prepared.values():
                value["manager_owner"] = str(term["owner_id"])
                value["manager_fence"] = int(term["fence"])
            committed = self.store.fenced_guarded_put_batch(
                LEASE_KEY,
                str(term["owner_id"]),
                int(term["fence"]),
                guard_key,
                expected_guard,
                {
                    key: _json_bytes(value)
                    for key, value in prepared.items()
                },
            )
        else:
            committed = self.store.guarded_put_batch(
                guard_key,
                expected_guard,
                {
                    key: _json_bytes(value)
                    for key, value in prepared.items()
                },
            )
        if not committed:
            raise WorkQueueConflict(
                "guard changed before durable publication commit"
            )
        for key, value in prepared.items():
            if self._read(key) != value:
                raise WorkQueueError(
                    f"queue batch verification failed: {key}"
                )

    def get_request(self, request_id: str) -> dict | None:
        return self._read(_request_key(request_id))

    def get_task(self, task_id: str) -> dict | None:
        return self._read(_task_key(task_id))

    def list_tasks(self) -> list[dict]:
        records = []
        for item in self.store.list(f"{TASKS_PREFIX}/"):
            value = self._read(item.key)
            if value is not None:
                records.append(value)
        return sorted(records, key=lambda value: value["task_id"])

    def get_membership(self, generation_id: str) -> dict | None:
        return self._read(_membership_key(generation_id))

    def list_memberships(self) -> list[dict]:
        records = []
        for item in self.store.list(f"{MEMBERSHIP_PREFIX}/"):
            value = self._read(item.key)
            if value is not None:
                records.append(value)
        return sorted(
            records,
            key=lambda value: (
                int(value.get("updated_at_ms", 0)),
                str(value.get("generation_id", "")),
            ),
        )

    def reconcile_membership(
        self,
        generation_id: str,
        generation_fence: int,
        workers: list[dict],
        *,
        policy: str,
        now_ms: int | None = None,
    ) -> dict:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        policy = str(policy).strip().lower()
        if policy not in {"fixed", "elastic"}:
            raise ValueError("membership policy must be 'fixed' or 'elastic'")
        current_workers = {
            str(worker["agent_id"]): {
                "agent_id": str(worker["agent_id"]),
                "lease_hash": _hash(str(worker["lease_id"])),
                "capacity": max(1, int(worker.get("capacity", 1) or 1)),
            }
            for worker in workers
        }
        key = _membership_key(generation_id)
        with self._lock:
            membership = self._read(key)
            if membership is None:
                if not current_workers:
                    return {
                        "version": 1,
                        "generation_id": generation_id,
                        "generation_fence": int(generation_fence),
                        "membership_version": 0,
                        "policy": policy,
                        "workers": {},
                        "reassignment_count": 0,
                        "last_change_reason": "waiting_for_workers",
                        "created_at_ms": now_ms,
                        "updated_at_ms": now_ms,
                    }
                membership = {
                    "version": 1,
                    "generation_id": generation_id,
                    "generation_fence": int(generation_fence),
                    "membership_version": 1,
                    "policy": policy,
                    "workers": {
                        agent_id: {
                            **worker,
                            "state": "active",
                            "worker_fence": 1,
                            "joined_at_ms": now_ms,
                            "last_seen_ms": now_ms,
                        }
                        for agent_id, worker in current_workers.items()
                    },
                    "reassignment_count": 0,
                    "last_change_reason": "initialized",
                    "created_at_ms": now_ms,
                    "updated_at_ms": now_ms,
                }
                self._write(key, membership)
                return membership
            if (
                str(membership.get("generation_id")) != generation_id
                or int(membership.get("generation_fence", -1))
                != int(generation_fence)
            ):
                raise WorkQueueConflict("membership belongs to another generation")
            if str(membership.get("policy")) != policy:
                raise WorkQueueConflict(
                    "membership policy cannot change during an active generation"
                )
            existing = {
                str(agent_id): dict(worker)
                for agent_id, worker in (membership.get("workers") or {}).items()
            }
            changed = False
            reasons = []
            allowed = (
                set(existing) | set(current_workers)
                if policy == "elastic"
                else set(existing)
            )
            for agent_id in sorted(allowed):
                observed = current_workers.get(agent_id)
                prior = existing.get(agent_id)
                if prior is None and observed is not None:
                    existing[agent_id] = {
                        **observed,
                        "state": "active",
                        "worker_fence": 1,
                        "joined_at_ms": now_ms,
                        "last_seen_ms": now_ms,
                    }
                    changed = True
                    reasons.append("worker_joined")
                    continue
                if prior is None:
                    continue
                if observed is None:
                    if prior.get("state") == "active":
                        prior["state"] = "departed"
                        prior["worker_fence"] = int(
                            prior.get("worker_fence", 0)
                        ) + 1
                        prior["last_seen_ms"] = now_ms
                        changed = True
                        reasons.append("worker_departed")
                    continue
                lease_changed = not hmac.compare_digest(
                    str(prior.get("lease_hash", "")),
                    observed["lease_hash"],
                )
                if lease_changed or prior.get("state") != "active":
                    prior["worker_fence"] = int(
                        prior.get("worker_fence", 0)
                    ) + 1
                    prior["joined_at_ms"] = now_ms
                    prior["state"] = "active"
                    changed = True
                    reasons.append("worker_rejoined")
                if int(prior.get("capacity", 1)) != observed["capacity"]:
                    prior["capacity"] = observed["capacity"]
                    changed = True
                    reasons.append("capacity_changed")
                prior["lease_hash"] = observed["lease_hash"]
                prior["last_seen_ms"] = now_ms
            membership["workers"] = existing
            membership["updated_at_ms"] = now_ms
            if changed:
                membership["membership_version"] = int(
                    membership.get("membership_version", 0)
                ) + 1
                membership["last_change_reason"] = ",".join(sorted(set(reasons)))
            self._write(key, membership)
            if changed:
                metrics.inc_counter(
                    "generation_membership_changes_total",
                    policy=policy,
                    reason=membership["last_change_reason"],
                )
            return membership

    def reassign_fenced_claims(
        self,
        generation_id: str,
        membership: dict,
        *,
        now_ms: int | None = None,
    ) -> int:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        workers = membership.get("workers") or {}
        reassigned = 0
        with self._lock:
            for task in self.list_tasks():
                payload = task.get("task") or {}
                if (
                    task.get("state") != TASK_CLAIMED
                    or str(payload.get("generation_id", "")) != generation_id
                ):
                    continue
                claim = task.get("claim") or {}
                worker = workers.get(str(claim.get("agent_id", "")))
                valid = bool(
                    worker
                    and worker.get("state") == "active"
                    and hmac.compare_digest(
                        str(worker.get("lease_hash", "")),
                        str(claim.get("agent_lease_hash", "")),
                    )
                    and int(worker.get("worker_fence", -1))
                    == int(claim.get("worker_fence", -2))
                )
                if valid:
                    continue
                task["state"] = TASK_RETRY_WAIT
                task["claim"] = None
                task["available_at_ms"] = now_ms
                task["last_error"] = "worker_membership_fenced"
                task["updated_at_ms"] = now_ms
                task["revision"] = int(task.get("revision", 0)) + 1
                task["reassignment_count"] = int(
                    task.get("reassignment_count", 0)
                ) + 1
                self._write(_task_key(task["task_id"]), task)
                reassigned += 1
            if reassigned:
                membership = dict(membership)
                membership["reassignment_count"] = int(
                    membership.get("reassignment_count", 0)
                ) + reassigned
                membership["updated_at_ms"] = now_ms
                membership["last_change_reason"] = "unfinished_work_reassigned"
                self._write(_membership_key(generation_id), membership)
                metrics.inc_counter(
                    "materialization_queue_reassignments_total",
                    reassigned,
                )
        return reassigned

    def create_request(
        self,
        *,
        requested_key: str,
        table: str,
        epoch: int,
        table_format: str,
        generation_id: str,
        generation_fence: int,
        plan_sha256: str,
        tasks: list[MaterializeTask],
        deadline_ms: int,
    ) -> dict:
        identity = "|".join((
            table,
            str(epoch),
            generation_id,
            str(generation_fence),
            plan_sha256,
        ))
        request_id = _hash(identity)
        id_key = _idempotency_key(identity)
        with self._lock:
            existing_id = self._read(id_key)
            if existing_id is not None:
                request = self.get_request(str(existing_id["request_id"]))
                if request is None:
                    raise WorkQueueError("idempotency record points to a missing request")
                return request
            created_at = _now_ms()
            task_ids = []
            task_records = []
            for task in tasks:
                task_id = _hash(f"{request_id}|{task.split_index}|{task.output_key}")
                task_ids.append(task_id)
                materialize = replace(
                    task,
                    task_id=task_id,
                    request_id=request_id,
                    generation_id=generation_id,
                    generation_fence=generation_fence,
                    plan_sha256=plan_sha256,
                    deadline_ms=deadline_ms,
                )
                record = {
                    "version": 1,
                    "task_id": task_id,
                    "request_id": request_id,
                    "state": TASK_QUEUED,
                    "attempt": 0,
                    "max_attempts": self.max_attempts,
                    "available_at_ms": created_at,
                    "claim": None,
                    "task": materialize.to_dict(),
                    "result": None,
                    "last_error": "",
                    "created_at_ms": created_at,
                    "updated_at_ms": created_at,
                    "revision": 1,
                }
                task_records.append(record)
            request = {
                "version": 1,
                "request_id": request_id,
                "idempotency_key": _hash(identity),
                "requested_key": requested_key,
                "table": table,
                "epoch": int(epoch),
                "table_format": table_format,
                "generation_id": generation_id,
                "generation_fence": int(generation_fence),
                "plan_sha256": plan_sha256,
                "deadline_ms": int(deadline_ms),
                "task_ids": task_ids,
                "state": TASK_QUEUED,
                "published": False,
                "snapshot_key": "",
                "error": "",
                "created_at_ms": created_at,
                "updated_at_ms": created_at,
                "ready": False,
            }
            self._write(_request_key(request_id), request)
            for record in task_records:
                self._write(_task_key(record["task_id"]), record)
            request["ready"] = True
            self._write(_request_key(request_id), request)
            self._write(id_key, {"version": 1, "request_id": request_id})
            metrics.inc_counter("materialization_queue_requests_total")
            return request

    def expire_claims(self, *, now_ms: int | None = None) -> int:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        expired = 0
        with self._lock:
            for task in self.list_tasks():
                claim = task.get("claim") or {}
                deadline_ms = int(
                    (task.get("task") or {}).get("deadline_ms", 0)
                )
                if (
                    task["state"] in {TASK_QUEUED, TASK_RETRY_WAIT}
                    and deadline_ms
                    and deadline_ms <= now_ms
                ):
                    task["state"] = TASK_FAILED
                    task["last_error"] = "deadline_expired"
                    task["updated_at_ms"] = now_ms
                    task["revision"] = int(task["revision"]) + 1
                    self._write(_task_key(task["task_id"]), task)
                    metrics.inc_counter(
                        "materialization_queue_events_total",
                        event=task["last_error"],
                    )
                    expired += 1
                    continue
                if (
                    task["state"] == TASK_CLAIMED
                    and (
                        int(claim.get("expires_at_ms", 0)) <= now_ms
                        or (deadline_ms and deadline_ms <= now_ms)
                    )
                ):
                    target_state = (
                        TASK_FAILED
                        if (
                            int(task["attempt"]) >= int(task["max_attempts"])
                            or (deadline_ms and deadline_ms <= now_ms)
                        )
                        else TASK_QUEUED
                    )
                    task["available_at_ms"] = now_ms
                    task["claim"] = None
                    task["last_error"] = (
                        "deadline_expired"
                        if deadline_ms and deadline_ms <= now_ms
                        else "claim_expired"
                    )
                    task["updated_at_ms"] = now_ms
                    task["revision"] = int(task["revision"]) + 1
                    task["state"] = TASK_EXPIRED
                    self._write(_task_key(task["task_id"]), task)
                    task["state"] = target_state
                    task["revision"] = int(task["revision"]) + 1
                    self._write(_task_key(task["task_id"]), task)
                    expired += 1
            self._refresh_requests(now_ms)
        return expired

    def runnable_tasks(
        self,
        *,
        now_ms: int | None = None,
        tasks: list[dict] | None = None,
        expire: bool = True,
    ) -> list[dict]:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        if expire:
            self.expire_claims(now_ms=now_ms)
        runnable = []
        for task in self.list_tasks() if tasks is None else tasks:
            if (
                task["state"] not in {TASK_QUEUED, TASK_RETRY_WAIT}
                or int(task.get("available_at_ms", 0)) > now_ms
            ):
                continue
            request = self.get_request(str(task.get("request_id", "")))
            if request is not None and request.get("ready", True):
                runnable.append(task)
        return runnable

    def recover(self) -> dict[str, int]:
        """Remove incomplete creates and requeue expired durable claims."""
        removed_requests = 0
        removed_tasks = 0
        with self._lock:
            requests = {
                str(record["request_id"]): record
                for item in self.store.list(f"{REQUESTS_PREFIX}/")
                if (record := self._read(item.key)) is not None
            }
            for request_id, request in list(requests.items()):
                if request.get("ready", True):
                    missing = [
                        task_id
                        for task_id in request.get("task_ids", [])
                        if self.get_task(task_id) is None
                    ]
                    if missing:
                        request["state"] = TASK_FAILED
                        request["error"] = "missing_task_record"
                        request["updated_at_ms"] = _now_ms()
                        self._write(_request_key(request_id), request)
                    self._write(
                        f"{IDEMPOTENCY_PREFIX}/{request['idempotency_key']}.json",
                        {"version": 1, "request_id": request_id},
                    )
                    continue
                for task_id in request.get("task_ids", []):
                    if self._delete(_task_key(task_id)):
                        removed_tasks += 1
                self._delete(_request_key(request_id))
                self._delete(
                    f"{IDEMPOTENCY_PREFIX}/{request['idempotency_key']}.json"
                )
                requests.pop(request_id, None)
                removed_requests += 1
            for task in self.list_tasks():
                if str(task.get("request_id", "")) not in requests:
                    for result in self.store.list(
                        f"{RESULTS_PREFIX}/{task['task_id']}/"
                    ):
                        self._delete(result.key)
                    if self._delete(_task_key(task["task_id"])):
                        removed_tasks += 1
                elif task["state"] == TASK_EXPIRED:
                    deadline_ms = int(
                        (task.get("task") or {}).get("deadline_ms", 0)
                    )
                    task["state"] = (
                        TASK_FAILED
                        if (
                            int(task["attempt"]) >= int(task["max_attempts"])
                            or (deadline_ms and deadline_ms <= _now_ms())
                        )
                        else TASK_QUEUED
                    )
                    task["available_at_ms"] = _now_ms()
                    task["updated_at_ms"] = _now_ms()
                    task["revision"] = int(task["revision"]) + 1
                    self._write(_task_key(task["task_id"]), task)
        expired_claims = self.expire_claims()
        return {
            "removed_requests": removed_requests,
            "removed_tasks": removed_tasks,
            "expired_claims": expired_claims,
        }

    def claim_task(
        self,
        task_id: str,
        *,
        agent_id: str,
        agent_lease_id: str,
        manager_owner: str,
        manager_fence: int,
        membership_version: int = 0,
        worker_fence: int = 0,
        now_ms: int | None = None,
    ) -> MaterializeTask:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        with self._lock:
            task = self.get_task(task_id)
            if task is None:
                raise WorkQueueError(f"unknown task {task_id!r}")
            if task["state"] not in {TASK_QUEUED, TASK_RETRY_WAIT}:
                raise WorkQueueConflict(
                    f"task {task_id!r} cannot be claimed from {task['state']}"
                )
            if int(task.get("available_at_ms", 0)) > now_ms:
                raise WorkQueueConflict(f"task {task_id!r} is not available yet")
            if task["state"] == TASK_RETRY_WAIT:
                task["state"] = TASK_QUEUED
                task["updated_at_ms"] = now_ms
                task["revision"] = int(task["revision"]) + 1
                self._write(_task_key(task_id), task)
            attempt = int(task["attempt"]) + 1
            claim_token = secrets.token_urlsafe(32)
            deadline_ms = int((task.get("task") or {}).get("deadline_ms", 0))
            expires_at = min(
                now_ms + self.task_lease_seconds * 1000,
                deadline_ms or now_ms + self.task_lease_seconds * 1000,
            )
            task["state"] = TASK_CLAIMED
            task["attempt"] = attempt
            task["claim"] = {
                "agent_id": agent_id,
                "agent_lease_hash": _hash(agent_lease_id),
                "claim_token": claim_token,
                "claimed_at_ms": now_ms,
                "expires_at_ms": expires_at,
                "manager_owner": manager_owner,
                "manager_fence": int(manager_fence),
                "membership_version": int(membership_version),
                "worker_fence": int(worker_fence),
            }
            task["updated_at_ms"] = now_ms
            task["revision"] = int(task["revision"]) + 1
            self._write(_task_key(task_id), task)
            metrics.inc_counter(
                "materialization_queue_events_total", event="claimed"
            )
            return replace(
                MaterializeTask.from_dict(task["task"]),
                claim_token=claim_token,
                attempt=attempt,
                claim_expires_at_ms=expires_at,
                membership_version=int(membership_version),
                worker_fence=int(worker_fence),
            )

    def renew_agent_claims(
        self,
        agent_id: str,
        agent_lease_id: str,
        *,
        task_ids: list[str] | None = None,
        now_ms: int | None = None,
    ) -> int:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        renewed = 0
        lease_hash = _hash(agent_lease_id)
        active = set(task_ids or ())
        if not active:
            return 0
        with self._lock:
            for task_id in active:
                task = self.get_task(task_id)
                if task is None:
                    continue
                claim = task.get("claim") or {}
                if (
                    task["state"] == TASK_CLAIMED
                    and claim.get("agent_id") == agent_id
                    and hmac.compare_digest(
                        str(claim.get("agent_lease_hash", "")), lease_hash
                    )
                ):
                    claim["expires_at_ms"] = (
                        min(
                            now_ms + self.task_lease_seconds * 1000,
                            int((task.get("task") or {}).get("deadline_ms", 0))
                            or now_ms + self.task_lease_seconds * 1000,
                        )
                    )
                    task["updated_at_ms"] = now_ms
                    task["revision"] = int(task["revision"]) + 1
                    self._write(_task_key(task_id), task)
                    renewed += 1
        return renewed

    def release_claim(
        self, task_id: str, claim_token: str, *, reason: str
    ) -> bool:
        """Return one undelivered or abandoned claim to the runnable queue."""
        now_ms = _now_ms()
        with self._lock:
            task = self.get_task(task_id)
            if task is None or task["state"] != TASK_CLAIMED:
                return False
            claim = task.get("claim") or {}
            if not hmac.compare_digest(
                str(claim.get("claim_token", "")), str(claim_token)
            ):
                return False
            task["state"] = TASK_RETRY_WAIT
            task["claim"] = None
            task["available_at_ms"] = now_ms
            task["last_error"] = _safe_error(reason)
            task["updated_at_ms"] = now_ms
            task["revision"] = int(task["revision"]) + 1
            self._write(_task_key(task_id), task)
            self._refresh_requests(now_ms)
            metrics.inc_counter(
                "materialization_queue_events_total", event="claim_released"
            )
            return True

    def accept_result(
        self,
        result: TaskResult,
        *,
        agent_lease_id: str,
        now_ms: int | None = None,
    ) -> Ack:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        with self._lock:
            prior = self._read(_result_key(result.task_id, result.attempt))
            if prior is not None:
                prior_lease_hash = str(prior.get("agent_lease_hash", ""))
                if prior_lease_hash and not hmac.compare_digest(
                    prior_lease_hash, _hash(agent_lease_id)
                ):
                    return Ack(
                        ok=False,
                        state=str(prior.get("state", "")),
                        terminal=str(prior.get("state", ""))
                        in TASK_TERMINAL_STATES,
                        reason_code=RESULT_STALE_CLAIM,
                    )
                saved = prior.get("result") or {}
                same = (
                    saved.get("agent_id") == result.agent_id
                    and int(saved.get("attempt", -1)) == result.attempt
                    and int(saved.get("size_bytes", 0)) == result.size_bytes
                    and int(saved.get("record_count", 0)) == result.record_count
                    and str(saved.get("content_hash", "")) == result.content_hash
                    and str(saved.get("error_code", "")) == result.error_code
                    and int(saved.get("membership_version", 0))
                    == result.membership_version
                    and int(saved.get("worker_fence", 0))
                    == result.worker_fence
                )
                return Ack(
                    ok=same,
                    state=str(prior.get("state", "")),
                    duplicate=same,
                    terminal=str(prior.get("state", "")) in TASK_TERMINAL_STATES,
                    reason_code=RESULT_DUPLICATE if same else RESULT_CONFLICT,
                )
            self.expire_claims(now_ms=now_ms)
            task = self.get_task(result.task_id)
            if task is None:
                return Ack(ok=False, state="", terminal=True, reason_code=RESULT_REJECTED)
            accepted = task.get("result") or {}
            if task["state"] == TASK_SUCCEEDED:
                same = (
                    accepted.get("content_hash") == result.content_hash
                    and int(accepted.get("size_bytes", -1)) == result.size_bytes
                    and int(accepted.get("record_count", -1)) == result.record_count
                )
                return Ack(
                    ok=same,
                    state=TASK_SUCCEEDED,
                    duplicate=same,
                    terminal=True,
                    reason_code=RESULT_DUPLICATE if same else RESULT_CONFLICT,
                )
            claim = task.get("claim") or {}
            if task["state"] != TASK_CLAIMED or not hmac.compare_digest(
                str(claim.get("claim_token", "")), result.claim_token
            ):
                return Ack(
                    ok=False,
                    state=str(task["state"]),
                    terminal=task["state"] in TASK_TERMINAL_STATES,
                    reason_code=RESULT_STALE_CLAIM,
                )
            if claim.get("agent_id") != result.agent_id:
                return Ack(
                    ok=False,
                    state=TASK_CLAIMED,
                    reason_code=RESULT_WRONG_OWNER,
                )
            if result.attempt != int(task["attempt"]):
                return Ack(
                    ok=False,
                    state=TASK_CLAIMED,
                    reason_code=RESULT_STALE_CLAIM,
                )
            if not hmac.compare_digest(
                str(claim.get("agent_lease_hash", "")), _hash(agent_lease_id)
            ):
                return Ack(
                    ok=False,
                    state=TASK_CLAIMED,
                    reason_code=RESULT_STALE_CLAIM,
                )
            if (
                int(claim.get("membership_version", 0))
                != int(result.membership_version)
                or int(claim.get("worker_fence", 0))
                != int(result.worker_fence)
            ):
                return Ack(
                    ok=False,
                    state=TASK_CLAIMED,
                    reason_code=RESULT_STALE_CLAIM,
                )
            membership = (
                self.get_membership(result.generation_id)
                if result.generation_id else None
            )
            if membership is not None:
                worker = (membership.get("workers") or {}).get(result.agent_id)
                if not (
                    worker
                    and worker.get("state") == "active"
                    and hmac.compare_digest(
                        str(worker.get("lease_hash", "")),
                        _hash(agent_lease_id),
                    )
                    and int(worker.get("worker_fence", -1))
                    == int(result.worker_fence)
                ):
                    return Ack(
                        ok=False,
                        state=TASK_CLAIMED,
                        reason_code=RESULT_STALE_CLAIM,
                    )
            task_payload = MaterializeTask.from_dict(task["task"])
            if (
                result.generation_id != task_payload.generation_id
                or result.generation_fence != task_payload.generation_fence
                or result.plan_sha256 != task_payload.plan_sha256
            ):
                return Ack(
                    ok=False,
                    state=TASK_CLAIMED,
                    reason_code=RESULT_STALE_GENERATION,
                )
            from runtime.generation import current_generation

            generation = current_generation(self.store)
            if generation is not None and (
                result.generation_id != generation.generation_id
                or result.generation_fence != generation.fence
                or result.plan_sha256 != generation.plan_sha256
            ):
                self.cancel_request(task["request_id"], "generation_fenced")
                return Ack(
                    ok=False,
                    state=TASK_CANCELLED,
                    terminal=True,
                    reason_code=RESULT_STALE_GENERATION,
                )
            if result.ok:
                reason = self._verify_output(task_payload, result)
                if reason:
                    return Ack(
                        ok=False,
                        state=TASK_CLAIMED,
                        reason_code=RESULT_INVALID_OUTPUT,
                    )
                task["state"] = TASK_SUCCEEDED
                task["result"] = {
                    "agent_id": result.agent_id,
                    "attempt": result.attempt,
                    "size_bytes": result.size_bytes,
                    "record_count": result.record_count,
                    "content_hash": result.content_hash,
                    "completed_at_ms": result.completed_at_ms or now_ms,
                    "error_code": "",
                }
                task["last_error"] = ""
                reason_code = RESULT_ACCEPTED
            else:
                task["last_error"] = _safe_error(result.error or result.error_code)
                if result.retryable and int(task["attempt"]) < int(task["max_attempts"]):
                    task["state"] = TASK_RETRY_WAIT
                    delay = min(
                        self.retry_max_seconds,
                        self.retry_backoff_seconds * (2 ** max(0, int(task["attempt"]) - 1)),
                    )
                    task["available_at_ms"] = now_ms + delay * 1000
                else:
                    task["state"] = TASK_FAILED
                task["result"] = {
                    "agent_id": result.agent_id,
                    "attempt": result.attempt,
                    "completed_at_ms": result.completed_at_ms or now_ms,
                    "error_code": result.error_code or "agent_error",
                }
                reason_code = RESULT_ACCEPTED
            task["claim"] = None
            task["updated_at_ms"] = now_ms
            task["revision"] = int(task["revision"]) + 1
            self._write(_task_key(result.task_id), task)
            self._write(
                _result_key(result.task_id, int(task["attempt"])),
                {
                    "version": 1,
                    "task_id": result.task_id,
                    "attempt": int(task["attempt"]),
                    "state": task["state"],
                    "result": task["result"],
                    "agent_lease_hash": str(
                        claim.get("agent_lease_hash", "")
                    ),
                },
            )
            self._refresh_requests(now_ms)
            metrics.inc_counter(
                "materialization_queue_results_total",
                outcome=str(task["state"]).lower(),
            )
            return Ack(
                ok=True,
                state=str(task["state"]),
                terminal=task["state"] in TASK_TERMINAL_STATES,
                reason_code=reason_code,
            )

    def _verify_output(
        self, task: MaterializeTask, result: TaskResult
    ) -> str | None:
        stat = self.store.head(task.output_key)
        if stat is None or stat.size != result.size_bytes:
            return "size_mismatch"
        data = self.store.get(task.output_key)
        if hashlib.sha256(data).hexdigest() != result.content_hash:
            return "hash_mismatch"
        try:
            metadata = pq.read_metadata(__import__("io").BytesIO(data))
        except Exception:
            return "invalid_parquet"
        if metadata.num_rows != result.record_count:
            return "row_count_mismatch"
        return None

    def cancel_request(self, request_id: str, reason: str) -> bool:
        now_ms = _now_ms()
        with self._lock:
            request = self.get_request(request_id)
            if request is None:
                return False
            for task_id in request["task_ids"]:
                task = self.get_task(task_id)
                if task and task["state"] not in TASK_TERMINAL_STATES:
                    task["state"] = TASK_CANCELLED
                    task["claim"] = None
                    task["last_error"] = _safe_error(reason)
                    task["updated_at_ms"] = now_ms
                    task["revision"] = int(task["revision"]) + 1
                    self._write(_task_key(task_id), task)
            request["state"] = TASK_CANCELLED
            request["error"] = _safe_error(reason)
            request["updated_at_ms"] = now_ms
            self._write(_request_key(request_id), request)
            metrics.inc_counter(
                "materialization_queue_events_total", event="cancelled"
            )
            return True

    def retry_task(self, task_id: str) -> bool:
        """Reset one failed task and its request for an operator-requested retry."""
        now_ms = _now_ms()
        with self._lock:
            task = self.get_task(task_id)
            if task is None or task["state"] != TASK_FAILED:
                return False
            request = self.get_request(str(task["request_id"]))
            if request is None or request.get("published"):
                return False
            original_window_ms = max(
                1_000,
                int(request.get("deadline_ms", 0))
                - int(request.get("created_at_ms", 0)),
            )
            deadline_ms = now_ms + original_window_ms
            for item in self.store.list(f"{RESULTS_PREFIX}/{task_id}/"):
                self._delete(item.key)
            task["state"] = TASK_QUEUED
            task["attempt"] = 0
            task["available_at_ms"] = now_ms
            task["claim"] = None
            task["result"] = None
            task["last_error"] = ""
            task["task"]["deadline_ms"] = deadline_ms
            task["updated_at_ms"] = now_ms
            task["revision"] = int(task["revision"]) + 1
            self._write(_task_key(task_id), task)
            request["state"] = TASK_QUEUED
            request["error"] = ""
            request["deadline_ms"] = deadline_ms
            request["updated_at_ms"] = now_ms
            self._write(_request_key(request["request_id"]), request)
            metrics.inc_counter(
                "materialization_queue_events_total", event="manual_retry"
            )
            return True

    def fence_claims(self, manager_owner: str, manager_fence: int) -> int:
        """Release claims created by a different Manager leadership term."""
        now_ms = _now_ms()
        fenced = 0
        with self._lock:
            for task in self.list_tasks():
                claim = task.get("claim") or {}
                if task["state"] != TASK_CLAIMED:
                    continue
                if (
                    claim.get("manager_owner") == manager_owner
                    and int(claim.get("manager_fence", -1)) == manager_fence
                ):
                    continue
                task["state"] = TASK_RETRY_WAIT
                task["claim"] = None
                task["available_at_ms"] = now_ms
                task["last_error"] = "manager_fenced"
                task["updated_at_ms"] = now_ms
                task["revision"] = int(task["revision"]) + 1
                self._write(_task_key(task["task_id"]), task)
                fenced += 1
            if fenced:
                self._refresh_requests(now_ms)
                metrics.inc_counter(
                    "materialization_queue_events_total",
                    value=float(fenced),
                    event="manager_fenced",
                )
        return fenced

    def prune_terminal(
        self, *, retention_seconds: int, now_ms: int | None = None
    ) -> int:
        """Delete old terminal queue records while retaining published snapshots."""
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        cutoff = now_ms - max(0, int(retention_seconds)) * 1000
        pruned = 0
        with self._lock:
            for item in self.store.list(f"{REQUESTS_PREFIX}/"):
                request = self._read(item.key)
                if (
                    request is None
                    or request.get("state") not in TASK_TERMINAL_STATES
                    or int(request.get("updated_at_ms", 0)) > cutoff
                ):
                    continue
                for task_id in request.get("task_ids", []):
                    for result in self.store.list(f"{RESULTS_PREFIX}/{task_id}/"):
                        self._delete(result.key)
                    self._delete(_task_key(task_id))
                self._delete(
                    f"{IDEMPOTENCY_PREFIX}/{request['idempotency_key']}.json"
                )
                self._delete(item.key)
                pruned += 1
            if pruned:
                metrics.inc_counter(
                    "materialization_queue_events_total",
                    value=float(pruned),
                    event="retention_pruned",
                )
        return pruned

    def _refresh_requests(self, now_ms: int) -> None:
        for item in self.store.list(f"{REQUESTS_PREFIX}/"):
            request = self._read(item.key)
            if (
                request is None
                or request.get("published")
                or request.get("state") in TASK_TERMINAL_STATES
            ):
                continue
            tasks = [
                self.get_task(task_id) for task_id in request.get("task_ids", [])
            ]
            tasks = [task for task in tasks if task is not None]
            states = {task["state"] for task in tasks}
            if tasks and states == {TASK_SUCCEEDED}:
                request["state"] = TASK_SUCCEEDED
            elif TASK_FAILED in states:
                request["state"] = TASK_FAILED
                failed = next(task for task in tasks if task["state"] == TASK_FAILED)
                request["error"] = failed.get("last_error", "")
                for task in tasks:
                    if task["state"] not in TASK_TERMINAL_STATES:
                        task["state"] = TASK_CANCELLED
                        task["claim"] = None
                        task["last_error"] = "request_failed"
                        task["updated_at_ms"] = now_ms
                        task["revision"] = int(task["revision"]) + 1
                        self._write(_task_key(task["task_id"]), task)
            elif TASK_CLAIMED in states:
                request["state"] = TASK_CLAIMED
            elif TASK_RETRY_WAIT in states:
                request["state"] = TASK_RETRY_WAIT
            else:
                request["state"] = TASK_QUEUED
            request["updated_at_ms"] = now_ms
            self._write(item.key, request)

    async def wait_request(
        self,
        request_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float = 0.1,
    ) -> dict:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            await asyncio.to_thread(self.expire_claims)
            request = await asyncio.to_thread(self.get_request, request_id)
            if request is None:
                raise WorkQueueError(f"unknown request {request_id!r}")
            if request["state"] in TASK_TERMINAL_STATES:
                return request
            await asyncio.sleep(poll_seconds)
        raise TimeoutError(f"materialization request timed out: {request_id}")

    def publish_snapshot(
        self, request_id: str, manifest: SnapshotManifest
    ) -> dict:
        now_ms = _now_ms()
        with self._lock:
            request = self.get_request(request_id)
            if request is None:
                raise WorkQueueError(f"unknown request {request_id!r}")
            if request["state"] != TASK_SUCCEEDED:
                raise WorkQueueConflict(
                    f"request {request_id!r} is not ready for publication"
                )
            if (
                manifest.request_id != request_id
                or manifest.table != request["table"]
                or manifest.epoch != int(request["epoch"])
                or manifest.generation_id != request["generation_id"]
                or manifest.generation_fence != int(request["generation_fence"])
                or manifest.plan_sha256 != request["plan_sha256"]
            ):
                raise WorkQueueConflict("snapshot identity does not match request")
            from runtime.generation import (
                COORDINATOR_KEY,
                current_generation_record,
            )

            generation_raw, generation = current_generation_record(self.store)
            if generation is not None and (
                generation.generation_id != manifest.generation_id
                or generation.fence != manifest.generation_fence
                or generation.plan_sha256 != manifest.plan_sha256
                or generation.expires_at_ms <= now_ms
            ):
                raise WorkQueueConflict(
                    "snapshot generation is no longer active"
                )
            for split in manifest.splits:
                data = self.store.get(split.object_key)
                if (
                    len(data) != split.size_bytes
                    or hashlib.sha256(data).hexdigest() != split.content_hash
                ):
                    raise WorkQueueConflict(
                        f"snapshot output verification failed: {split.object_key}"
                    )
            for metadata_key in manifest.metadata_keys:
                if self.store.head(metadata_key) is None:
                    raise WorkQueueConflict(
                        f"snapshot metadata is missing: {metadata_key}"
                    )
            key = _snapshot_key(manifest.table, manifest.epoch)
            payload = manifest.to_dict()
            request["published"] = True
            request["snapshot_key"] = key
            request["updated_at_ms"] = now_ms
            if generation is not None and generation_raw is not None:
                self._guarded_write_batch(
                    COORDINATOR_KEY,
                    generation_raw,
                    {
                        key: {"version": 1, "manifest": payload},
                        _request_key(request_id): request,
                    },
                )
            else:
                self._write(key, {"version": 1, "manifest": payload})
                self._write(_request_key(request_id), request)
            metrics.inc_counter(
                "materialization_queue_events_total", event="published"
            )
            return request

    def get_snapshot(self, table: str, epoch: int = 0) -> SnapshotManifest | None:
        prefix = f"{SNAPSHOTS_PREFIX}/{_hash(table)}/"
        records = self.store.list(prefix)
        if not records:
            return None
        selected = records[-1] if epoch == 0 else next(
            (
                item for item in records
                if item.key == _snapshot_key(table, epoch)
            ),
            None,
        )
        if selected is None:
            return None
        record = self._read(selected.key)
        if record is None:
            return None
        manifest = SnapshotManifest.from_dict(record["manifest"])
        for split in manifest.splits:
            try:
                data = self.store.get(split.object_key)
            except ObjectNotFound as exc:
                raise WorkQueueError(
                    f"published snapshot output is missing: {split.object_key}"
                ) from exc
            if (
                len(data) != split.size_bytes
                or hashlib.sha256(data).hexdigest() != split.content_hash
            ):
                raise WorkQueueError(
                    f"published snapshot output is invalid: {split.object_key}"
                )
        for metadata_key in manifest.metadata_keys:
            if self.store.head(metadata_key) is None:
                raise WorkQueueError(
                    f"published snapshot metadata is missing: {metadata_key}"
                )
        return manifest

    def status(self) -> dict:
        tasks = self.list_tasks()
        counts = {}
        for task in tasks:
            counts[task["state"]] = counts.get(task["state"], 0) + 1
        queued = [
            int(task["created_at_ms"])
            for task in tasks
            if task["state"] in {TASK_QUEUED, TASK_RETRY_WAIT}
        ]
        now = _now_ms()
        memberships = self.list_memberships()
        active_generation_ids = {
            str((task.get("task") or {}).get("generation_id", ""))
            for task in tasks
            if (
                task.get("state") not in TASK_TERMINAL_STATES
                and (task.get("task") or {}).get("generation_id")
            )
        }
        active_memberships = [
            membership
            for membership in memberships
            if str(membership.get("generation_id", ""))
            in active_generation_ids
        ]
        latest_membership = (
            active_memberships[-1]
            if active_memberships
            else memberships[-1] if memberships else None
        )
        membership_status = None
        if latest_membership is not None:
            generation_id = str(latest_membership.get("generation_id", ""))
            generation_tasks = [
                task
                for task in tasks
                if str((task.get("task") or {}).get("generation_id", ""))
                == generation_id
            ]
            claim_counts: dict[str, int] = {}
            for task in generation_tasks:
                agent_id = str((task.get("claim") or {}).get("agent_id", ""))
                if agent_id:
                    claim_counts[agent_id] = claim_counts.get(agent_id, 0) + 1
            workers = []
            for agent_id, worker in sorted(
                (latest_membership.get("workers") or {}).items()
            ):
                workers.append({
                    "agent_id": agent_id,
                    "state": str(worker.get("state", "")),
                    "capacity": int(worker.get("capacity", 1)),
                    "worker_fence": int(worker.get("worker_fence", 0)),
                    "joined_at_ms": int(worker.get("joined_at_ms", 0)),
                    "last_seen_ms": int(worker.get("last_seen_ms", 0)),
                    "active_claims": claim_counts.get(agent_id, 0),
                })
            total = len(generation_tasks)
            completed = sum(
                task.get("state") == TASK_SUCCEEDED
                for task in generation_tasks
            )
            membership_status = {
                "generation_id": generation_id,
                "generation_fence": int(
                    latest_membership.get("generation_fence", 0)
                ),
                "membership_version": int(
                    latest_membership.get("membership_version", 0)
                ),
                "policy": str(latest_membership.get("policy", "fixed")),
                "workers": workers,
                "unassigned_work": sum(
                    task.get("state") in {TASK_QUEUED, TASK_RETRY_WAIT}
                    for task in generation_tasks
                ),
                "reassignment_count": int(
                    latest_membership.get("reassignment_count", 0)
                ),
                "completed_tasks": completed,
                "total_tasks": total,
                "progress": completed / total if total else 1.0,
                "last_change_reason": str(
                    latest_membership.get("last_change_reason", "")
                ),
            }
        return {
            "requests": len(self.store.list(f"{REQUESTS_PREFIX}/")),
            "tasks": len(tasks),
            "published_snapshots": len(
                self.store.list(f"{SNAPSHOTS_PREFIX}/")
            ),
            "states": counts,
            "queue_depth": sum(
                counts.get(state, 0) for state in (TASK_QUEUED, TASK_RETRY_WAIT)
            ),
            "active_claims": counts.get(TASK_CLAIMED, 0),
            "oldest_queued_age_ms": max(0, now - min(queued)) if queued else 0,
            "membership": membership_status,
        }
