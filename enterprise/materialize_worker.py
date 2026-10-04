"""Python Agent executor for Manager-claimed materialization tasks."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import time

import config
from config import ColumnDef, ColumnTransform, TableDef
from db.executor import SourceUnavailable
from enterprise.control.contract import MaterializeTask, TaskResult
from iceberg.state_store import SplitDescriptor
from runtime.artifact_store import get_default_store
from runtime.materializer import materialize_queued_split
from runtime.generation import GenerationError


def _columns(task: MaterializeTask) -> list[ColumnDef]:
    columns = []
    for column in task.schema:
        transform = None
        if column.transform:
            transform = ColumnTransform(
                kind=str(column.transform.get("kind") or ""),
                key_ref=(
                    str(column.transform["key_ref"])
                    if column.transform.get("key_ref")
                    else None
                ),
                domain=(
                    str(column.transform["domain"])
                    if column.transform.get("domain") is not None
                    else None
                ),
                normalization=str(
                    column.transform.get("normalization") or "none"
                ),
            )
        columns.append(
            ColumnDef(
                field_id=column.field_id,
                name=column.name,
                iceberg_type=column.iceberg_type,
                nullable=column.nullable,
                source=column.source or None,
                transform=transform,
                policy_id=column.policy_id or None,
            )
        )
    return columns


def _split(task: MaterializeTask) -> SplitDescriptor:
    if task.connection_fingerprint:
        local_fingerprint = hashlib.sha256(
            config.redact_db_url(
                config.effective_db_url(task.connection_id)
            ).encode("utf-8")
        ).hexdigest()
        if not hmac.compare_digest(task.connection_fingerprint, local_fingerprint):
            raise ValueError("task connection identity does not match Agent configuration")
    table = TableDef(
        name=task.table,
        source_table=task.source_table,
        schema=_columns(task),
        num_splits=max(1, task.num_splits),
        key_column=task.key_column or None,
        connection_id=task.connection_id or "default",
        split_strategy=task.split_strategy or None,
    )
    split = SplitDescriptor(
        split_index=task.split_index,
        num_splits=max(1, task.num_splits),
        object_key=task.output_key,
        watermark_ms=0,
        table=table,
        split_key_column=task.key_column or None,
        key_lo=task.range.lo if task.range is not None else None,
        key_hi=task.range.hi if task.range is not None else None,
    )
    split.generation_id = task.generation_id
    split.generation_fence = task.generation_fence
    split.generation_plan_sha256 = task.plan_sha256
    from runtime.generation import current_generation

    generation = current_generation(get_default_store())
    if generation is not None and (
        generation.generation_id != task.generation_id
        or generation.fence != task.generation_fence
        or generation.plan_sha256 != task.plan_sha256
    ):
        raise GenerationError("task generation was fenced before execution")
    split.generation_token = generation.lease_token if generation is not None else ""
    return split


async def execute_task(task: MaterializeTask, agent_id: str) -> TaskResult:
    """Execute one claimed task and return a claim-bound result."""
    now_ms = int(time.time() * 1000)
    if task.deadline_ms and now_ms >= task.deadline_ms:
        return TaskResult(
            agent_id=agent_id,
            table=task.table,
            epoch=task.epoch,
            split_index=task.split_index,
            ok=False,
            error="task deadline expired",
            task_id=task.task_id,
            request_id=task.request_id,
            claim_token=task.claim_token,
            attempt=task.attempt,
            generation_id=task.generation_id,
            generation_fence=task.generation_fence,
            membership_version=task.membership_version,
            worker_fence=task.worker_fence,
            plan_sha256=task.plan_sha256,
            retryable=False,
            completed_at_ms=now_ms,
            error_code="deadline_expired",
        )
    try:
        split = await asyncio.to_thread(_split, task)
        rows = await materialize_queued_split(split)
        data = await asyncio.to_thread(
            get_default_store().get, task.output_key
        )
        return TaskResult(
            agent_id=agent_id,
            table=task.table,
            epoch=task.epoch,
            split_index=task.split_index,
            ok=True,
            size_bytes=len(data),
            record_count=rows,
            content_hash=hashlib.sha256(data).hexdigest(),
            task_id=task.task_id,
            request_id=task.request_id,
            claim_token=task.claim_token,
            attempt=task.attempt,
            generation_id=task.generation_id,
            generation_fence=task.generation_fence,
            membership_version=task.membership_version,
            worker_fence=task.worker_fence,
            plan_sha256=task.plan_sha256,
            completed_at_ms=int(time.time() * 1000),
        )
    except Exception as exc:
        retryable = isinstance(
            exc, (SourceUnavailable, TimeoutError, OSError)
        )
        error_code = (
            "generation_fenced"
            if isinstance(exc, GenerationError)
            else "source_unavailable"
            if retryable
            else "task_failed"
        )
        return TaskResult(
            agent_id=agent_id,
            table=task.table,
            epoch=task.epoch,
            split_index=task.split_index,
            ok=False,
            error=config.redact_db_url(str(exc))[:500],
            task_id=task.task_id,
            request_id=task.request_id,
            claim_token=task.claim_token,
            attempt=task.attempt,
            generation_id=task.generation_id,
            generation_fence=task.generation_fence,
            membership_version=task.membership_version,
            worker_fence=task.worker_fence,
            plan_sha256=task.plan_sha256,
            retryable=retryable,
            completed_at_ms=int(time.time() * 1000),
            error_code=error_code,
        )
