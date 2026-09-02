"""Durable owner-completion records for distributed split materialization."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from iceberg.stats import ColumnStats
from runtime.artifact_store import ObjectNotFound, get_default_store
from runtime.generation import assert_generation_identity

_PREFIX = ".fsp/generations"


@dataclass(frozen=True)
class SplitCompletion:
    generation_id: str
    fence: int
    object_key: str
    file_size_in_bytes: int
    record_count: int
    sha256: str
    stats: dict[int, ColumnStats]


def completion_key(object_key: str, generation_id: str = "legacy") -> str:
    digest = hashlib.sha256(object_key.encode("utf-8")).hexdigest()
    return f"{_PREFIX}/{generation_id}/split-completions/{digest}.json"


def _encode_stats(stats: dict[int, ColumnStats]) -> dict[str, dict]:
    return {
        str(field_id): {
            "field_id": stat.field_id,
            "column_size": stat.column_size,
            "value_count": stat.value_count,
            "null_count": stat.null_count,
            "lower": stat.lower.hex() if stat.lower is not None else None,
            "upper": stat.upper.hex() if stat.upper is not None else None,
        }
        for field_id, stat in stats.items()
    }


def _decode_stats(raw: dict) -> dict[int, ColumnStats]:
    result: dict[int, ColumnStats] = {}
    for field_id_text, value in raw.items():
        field_id = int(field_id_text)
        if int(value["field_id"]) != field_id:
            raise ValueError("completion statistic field ID mismatch")
        result[field_id] = ColumnStats(
            field_id=field_id,
            column_size=int(value["column_size"]),
            value_count=int(value["value_count"]),
            null_count=int(value["null_count"]),
            lower=bytes.fromhex(value["lower"]) if value.get("lower") is not None else None,
            upper=bytes.fromhex(value["upper"]) if value.get("upper") is not None else None,
        )
    return result


def publish_split_completion(split, parquet_bytes: bytes) -> SplitCompletion:
    """Publish data first and its completion record last, failing on store errors."""
    if split.file_size_in_bytes is None or split.record_count is None:
        raise ValueError("split metadata must be populated before completion")
    store = get_default_store()
    generation_id = str(getattr(split, "generation_id", "legacy"))
    fence = int(getattr(split, "generation_fence", 0))
    lease_token = str(getattr(split, "generation_token", ""))
    if generation_id != "legacy":
        assert_generation_identity(store, generation_id, fence, lease_token)
    store.put(split.object_key, parquet_bytes)
    completion = SplitCompletion(
        generation_id=generation_id,
        fence=fence,
        object_key=split.object_key,
        file_size_in_bytes=int(split.file_size_in_bytes),
        record_count=int(split.record_count),
        sha256=hashlib.sha256(parquet_bytes).hexdigest(),
        stats=dict(split.stats or {}),
    )
    payload = {
        "version": 1,
        "generation_id": completion.generation_id,
        "fence": completion.fence,
        "object_key": completion.object_key,
        "file_size_in_bytes": completion.file_size_in_bytes,
        "record_count": completion.record_count,
        "sha256": completion.sha256,
        "stats": _encode_stats(completion.stats),
    }
    store.put(
        completion_key(split.object_key, generation_id),
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )
    return completion


def read_split_completion(split) -> SplitCompletion | None:
    """Read and validate a completion without loading the Parquet object."""
    store = get_default_store()
    generation_id = str(getattr(split, "generation_id", "legacy"))
    fence = int(getattr(split, "generation_fence", 0))
    lease_token = str(getattr(split, "generation_token", ""))
    if generation_id != "legacy":
        assert_generation_identity(store, generation_id, fence, lease_token)
    try:
        raw = store.get(completion_key(split.object_key, generation_id))
    except ObjectNotFound:
        return None
    try:
        payload = json.loads(raw)
        if (
            payload.get("version") != 1
            or payload.get("generation_id") != generation_id
            or int(payload.get("fence", -1)) != fence
            or payload.get("object_key") != split.object_key
        ):
            raise ValueError("completion identity mismatch")
        completion = SplitCompletion(
            generation_id=payload["generation_id"],
            fence=int(payload["fence"]),
            object_key=payload["object_key"],
            file_size_in_bytes=int(payload["file_size_in_bytes"]),
            record_count=int(payload["record_count"]),
            sha256=str(payload["sha256"]),
            stats=_decode_stats(payload.get("stats", {})),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid split completion for {split.object_key}: {exc}") from exc
    stat = store.head(split.object_key)
    if stat is None or stat.size != completion.file_size_in_bytes:
        raise RuntimeError(f"split completion data mismatch for {split.object_key}")
    return completion


def apply_split_completion(split, completion: SplitCompletion) -> int:
    split.file_size_in_bytes = completion.file_size_in_bytes
    split.record_count = completion.record_count
    split.stats = completion.stats
    return completion.record_count