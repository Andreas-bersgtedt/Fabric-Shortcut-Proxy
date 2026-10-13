"""Live per-stage central/distributed materialization benchmark."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import statistics
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from typing import AsyncIterator

from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.config import TableDef
from fabric_shortcut_proxy.db.executor import (
    derive_table_schema,
    dispose_engines,
    fetch_key_bounds,
    reflect_primary_key,
    stream_split_query,
)
from fabric_shortcut_proxy.iceberg.state_store import SplitDescriptor
from fabric_shortcut_proxy.parquet.generator import (
    stream_rows_to_parquet_sink,
)
from fabric_shortcut_proxy.planner.split_planner import (
    build_split_query,
    compute_key_ranges,
)
from fabric_shortcut_proxy.runtime.artifact_store import get_default_store


@dataclass(frozen=True)
class StageSample:
    scenario: str
    location: str
    split_index: int
    rows: int
    estimated_source_bytes: int
    parquet_bytes: int
    upload_bytes: int
    source_wait_seconds: float
    encode_wall_seconds: float
    encode_cpu_seconds: float
    upload_seconds: float
    total_seconds: float
    failed_upload_bytes: int
    retried_upload_bytes: int
    failed_bytes_measured_remotely: bool
    sha256: str

    @property
    def rows_per_second(self) -> float:
        return self.rows / self.total_seconds if self.total_seconds else 0.0

    @property
    def upload_bytes_per_second(self) -> float:
        return (
            self.upload_bytes / self.upload_seconds
            if self.upload_seconds
            else 0.0
        )

    def to_dict(self) -> dict:
        return {
            **asdict(self),
            "rows_per_second": self.rows_per_second,
            "upload_bytes_per_second": self.upload_bytes_per_second,
            "compression_ratio": (
                self.estimated_source_bytes / self.parquet_bytes
                if self.parquet_bytes
                else None
            ),
        }


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(
        0,
        min(
            len(ordered) - 1,
            int((len(ordered) - 1) * quantile + 0.999999),
        ),
    )
    return ordered[index]


def summarize(
    samples: list[StageSample],
    *,
    wall_seconds: float | None = None,
) -> dict:
    totals = [sample.total_seconds for sample in samples]
    rows = sum(sample.rows for sample in samples)
    parquet_bytes = sum(sample.parquet_bytes for sample in samples)
    source_bytes = sum(sample.estimated_source_bytes for sample in samples)
    elapsed = (
        wall_seconds
        if wall_seconds is not None
        else sum(totals)
    )
    return {
        "samples": len(samples),
        "rows": rows,
        "estimated_source_bytes": source_bytes,
        "parquet_bytes": parquet_bytes,
        "upload_bytes": sum(sample.upload_bytes for sample in samples),
        "compression_ratio": (
            source_bytes / parquet_bytes if parquet_bytes else None
        ),
        "failed_upload_bytes": sum(
            sample.failed_upload_bytes for sample in samples
        ),
        "retried_upload_bytes": sum(
            sample.retried_upload_bytes for sample in samples
        ),
        "rows_per_second": rows / elapsed if elapsed else 0.0,
        "total_seconds": {
            "min": min(totals, default=0.0),
            "median": statistics.median(totals) if totals else 0.0,
            "p95": percentile(totals, 0.95),
            "max": max(totals, default=0.0),
        },
        "source_wait_seconds": sum(
            sample.source_wait_seconds for sample in samples
        ),
        "encode_wall_seconds": sum(
            sample.encode_wall_seconds for sample in samples
        ),
        "encode_cpu_seconds": sum(
            sample.encode_cpu_seconds for sample in samples
        ),
        "upload_seconds": sum(
            sample.upload_seconds for sample in samples
        ),
    }


def _estimated_batch_bytes(batch: list[dict]) -> int:
    return sum(
        len(
            json.dumps(
                row,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode()
        )
        for row in batch
    )


async def _materialize_one(
    *,
    scenario: str,
    table,
    split_index: int,
    split_count: int,
    failure_fraction: float | None,
    rows_per_split: int,
    key_range: tuple[int, int] | None,
) -> StageSample:
    store = get_default_store()
    key = (
        f"_benchmarks/{scenario}/{uuid.uuid4().hex}/"
        f"split-{split_index}.parquet"
    )
    split = SplitDescriptor(
        split_index=split_index,
        num_splits=split_count,
        object_key=key,
        watermark_ms=int(time.time() * 1000),
        table=table,
        split_key_column=table.key_column,
        key_lo=key_range[0] if key_range is not None else None,
        key_hi=key_range[1] if key_range is not None else None,
    )
    sql, params = build_split_query(split)
    if rows_per_split > 0:
        params["max_rows"] = rows_per_split
    source_wait = 0.0
    source_bytes = 0
    query = stream_split_query(
        sql,
        params,
        split_index=split_index,
        batch_rows=config.STREAM_BATCH_ROWS,
        connection=table.connection_id,
    )

    async def measured_batches() -> AsyncIterator[list[dict]]:
        nonlocal source_wait, source_bytes
        iterator = aiter(query)
        while True:
            started = time.perf_counter()
            try:
                batch = await anext(iterator)
            except StopAsyncIteration:
                return
            source_wait += time.perf_counter() - started
            source_bytes += _estimated_batch_bytes(batch)
            yield batch

    total_started = time.perf_counter()
    cpu_started = time.process_time()
    with tempfile.TemporaryFile(mode="w+b") as output:
        encode_started = time.perf_counter()
        rows = await stream_rows_to_parquet_sink(
            measured_batches(),
            output,
            split_index=split_index,
            columns=table.schema,
        )
        encode_wall = time.perf_counter() - encode_started
        encode_cpu = time.process_time() - cpu_started
        output.flush()
        parquet_size = output.tell()
        upload_size = parquet_size
        if failure_fraction is not None:
            upload_size = max(parquet_size, 16 * 1024 * 1024)
            padding = hashlib.sha256(b"fsp-benchmark-padding").digest()
            remaining = upload_size - parquet_size
            while remaining:
                chunk = (padding * ((min(remaining, 1 << 20) // len(padding)) + 1))[
                    : min(remaining, 1 << 20)
                ]
                output.write(chunk)
                remaining -= len(chunk)
            output.flush()
        output.seek(0)
        digest = hashlib.sha256()
        uploaded = 0

        def chunks(*, inject_failure: bool):
            nonlocal uploaded
            threshold = (
                max(1, int(upload_size * failure_fraction))
                if inject_failure and failure_fraction is not None
                else None
            )
            while True:
                if threshold is not None and uploaded >= threshold:
                    raise OSError("injected benchmark upload failure")
                read_size = 1 << 20
                if threshold is not None:
                    read_size = min(read_size, threshold - uploaded)
                chunk = output.read(read_size)
                if not chunk:
                    return
                uploaded += len(chunk)
                digest.update(chunk)
                yield chunk

        failed_bytes = 0
        retried_bytes = 0
        failed_bytes_measured_remotely = False
        upload_started = time.perf_counter()
        if failure_fraction is not None and failure_fraction < 1:
            try:
                store.put_stream(
                    key,
                    chunks(inject_failure=True),
                    length=upload_size,
                )
            except OSError:
                failed_bytes = uploaded
                blob_factory = getattr(store, "_blob", None)
                if callable(blob_factory):
                    try:
                        block_list = blob_factory(key).get_block_list(
                            block_list_type="uncommitted"
                        )
                        blocks = getattr(
                            block_list,
                            "uncommitted_blocks",
                            None,
                        )
                        if blocks is None and isinstance(
                            block_list,
                            tuple,
                        ):
                            blocks = block_list[1]
                        remote_bytes = sum(
                            int(block.size) for block in (blocks or [])
                        )
                        if remote_bytes:
                            failed_bytes = remote_bytes
                            failed_bytes_measured_remotely = True
                    except Exception:
                        pass
            else:
                raise AssertionError("injected upload failure did not fire")
            output.seek(0)
            digest = hashlib.sha256()
            uploaded = 0
            retried_bytes = upload_size
            store.put_stream_if_absent(
                key,
                chunks(inject_failure=False),
                length=upload_size,
            )
        elif failure_fraction == 1:
            store.put_stream_if_absent(
                key,
                chunks(inject_failure=False),
                length=upload_size,
            )
            output.seek(0)
            digest = hashlib.sha256()
            uploaded = 0
            retried_bytes = upload_size
            store.put_stream_if_absent(
                key,
                chunks(inject_failure=False),
                length=upload_size,
            )
        else:
            store.put_stream(
                key,
                chunks(inject_failure=False),
                length=upload_size,
            )
        upload_seconds = time.perf_counter() - upload_started
        try:
            return StageSample(
                scenario=scenario,
                location=str(config.AGENT_LOCATION or "unknown"),
                split_index=split_index,
                rows=rows,
                estimated_source_bytes=source_bytes,
                parquet_bytes=parquet_size,
                upload_bytes=upload_size,
                source_wait_seconds=source_wait,
                encode_wall_seconds=encode_wall,
                encode_cpu_seconds=encode_cpu,
                upload_seconds=upload_seconds,
                total_seconds=time.perf_counter() - total_started,
                failed_upload_bytes=failed_bytes,
                retried_upload_bytes=retried_bytes,
                failed_bytes_measured_remotely=(
                    failed_bytes_measured_remotely
                ),
                sha256=digest.hexdigest(),
            )
        finally:
            store.delete(key)


async def run_scenario(
    *,
    scenario: str,
    split_count: int,
    concurrency: int,
    failure_fraction: float | None = None,
    source_table: str = "",
    connection_id: str = "default",
    key_column: str = "",
    rows_per_split: int = 0,
    split_strategy: str = "modulo",
) -> dict:
    if rows_per_split < 0:
        raise ValueError("rows_per_split must be non-negative")
    table = await resolve_benchmark_table(
        source_table=source_table,
        connection_id=connection_id,
        key_column=key_column,
        split_count=split_count,
        split_strategy=split_strategy,
    )
    ranges = None
    if split_strategy == "range":
        bounds = await fetch_key_bounds(
            table.source_table,
            str(table.key_column),
            connection=table.connection_id,
        )
        if bounds is None:
            raise RuntimeError(
                f"no key bounds for {table.source_table!r}"
            )
        ranges = compute_key_ranges(
            int(bounds[0]),
            int(bounds[1]),
            split_count,
        )
    gate = asyncio.Semaphore(max(1, concurrency))

    async def run(index: int) -> StageSample:
        async with gate:
            return await _materialize_one(
                scenario=scenario,
                table=table,
                split_index=index,
                split_count=split_count,
                failure_fraction=failure_fraction,
                rows_per_split=rows_per_split,
                key_range=(ranges[index] if ranges is not None else None),
            )

    started = time.perf_counter()
    samples = await asyncio.gather(
        *(run(index) for index in range(split_count))
    )
    wall = time.perf_counter() - started
    return {
        "scenario": scenario,
        "location": str(config.AGENT_LOCATION or "unknown"),
        "split_count": split_count,
        "concurrency": concurrency,
        "failure_fraction": failure_fraction,
        "source_table": table.source_table,
        "connection_id": table.connection_id,
        "key_column": table.key_column,
        "rows_per_split": rows_per_split,
        "split_strategy": split_strategy,
        "wall_seconds": wall,
        "summary": summarize(samples, wall_seconds=wall),
        "samples": [sample.to_dict() for sample in samples],
    }


async def resolve_benchmark_table(
    *,
    source_table: str,
    connection_id: str,
    key_column: str,
    split_count: int,
    split_strategy: str,
) -> TableDef:
    if not source_table:
        if not config.TABLES:
            raise RuntimeError(
                "benchmark requires one configured source table"
            )
        table = config.TABLES[0]
        table.num_splits = split_count
        table.split_strategy = split_strategy
        return table
    schema = await derive_table_schema(source_table, connection_id)
    reflected_key = key_column.strip()
    if not reflected_key:
        primary_key = await reflect_primary_key(
            source_table,
            connection_id,
        )
        reflected_key = primary_key[0] if primary_key else ""
    if not reflected_key:
        reflected_key = next(
            (
                column.name
                for column in schema
                if (
                    not column.nullable
                    and column.iceberg_type in {"int", "long"}
                )
            ),
            "",
        )
    if not reflected_key:
        raise RuntimeError(
            f"benchmark source {source_table!r} has no integer split key"
        )
    return TableDef(
        name=source_table.rsplit(".", 1)[-1],
        source_table=source_table,
        schema=schema,
        num_splits=split_count,
        key_column=reflected_key,
        connection_id=connection_id,
        split_strategy=split_strategy,
    )


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--splits", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument(
        "--failure-fraction",
        type=float,
        choices=(0.25, 0.75, 1.0),
    )
    parser.add_argument("--source-table", default="")
    parser.add_argument("--connection", default="default")
    parser.add_argument("--key-column", default="")
    parser.add_argument("--rows-per-split", type=int, default=0)
    parser.add_argument(
        "--split-strategy",
        choices=("modulo", "range"),
        default="modulo",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _arguments()
    if (
        args.splits < 1
        or args.concurrency < 1
        or args.rows_per_split < 0
    ):
        raise SystemExit(
            "splits/concurrency must be positive and rows-per-split non-negative"
        )
    async def main() -> dict:
        try:
            return await run_scenario(
                scenario=args.scenario,
                split_count=args.splits,
                concurrency=args.concurrency,
                failure_fraction=args.failure_fraction,
                source_table=args.source_table,
                connection_id=args.connection,
                key_column=args.key_column,
                rows_per_split=args.rows_per_split,
                split_strategy=args.split_strategy,
            )
        finally:
            await dispose_engines()

    print(json.dumps(asyncio.run(main()), sort_keys=True))
