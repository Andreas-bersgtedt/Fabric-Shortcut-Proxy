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
from fabric_shortcut_proxy.db.executor import (
    dispose_engines,
    stream_split_query,
)
from fabric_shortcut_proxy.iceberg.state_store import SplitDescriptor
from fabric_shortcut_proxy.parquet.generator import (
    stream_rows_to_parquet_sink,
)
from fabric_shortcut_proxy.planner.split_planner import build_split_query
from fabric_shortcut_proxy.runtime.artifact_store import get_default_store


@dataclass(frozen=True)
class StageSample:
    scenario: str
    location: str
    split_index: int
    rows: int
    estimated_source_bytes: int
    parquet_bytes: int
    source_wait_seconds: float
    encode_wall_seconds: float
    encode_cpu_seconds: float
    upload_seconds: float
    total_seconds: float
    failed_upload_bytes: int
    sha256: str

    @property
    def rows_per_second(self) -> float:
        return self.rows / self.total_seconds if self.total_seconds else 0.0

    @property
    def upload_bytes_per_second(self) -> float:
        return (
            self.parquet_bytes / self.upload_seconds
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


def summarize(samples: list[StageSample]) -> dict:
    totals = [sample.total_seconds for sample in samples]
    rows = sum(sample.rows for sample in samples)
    parquet_bytes = sum(sample.parquet_bytes for sample in samples)
    source_bytes = sum(sample.estimated_source_bytes for sample in samples)
    elapsed = max(totals, default=0.0)
    return {
        "samples": len(samples),
        "rows": rows,
        "estimated_source_bytes": source_bytes,
        "parquet_bytes": parquet_bytes,
        "compression_ratio": (
            source_bytes / parquet_bytes if parquet_bytes else None
        ),
        "failed_upload_bytes": sum(
            sample.failed_upload_bytes for sample in samples
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
    )
    sql, params = build_split_query(split)
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
        size = output.tell()
        output.seek(0)
        digest = hashlib.sha256()
        uploaded = 0

        def chunks(*, inject_failure: bool):
            nonlocal uploaded
            threshold = (
                max(1, int(size * failure_fraction))
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
        upload_started = time.perf_counter()
        if failure_fraction is not None and failure_fraction < 1:
            try:
                store.put_stream(
                    key,
                    chunks(inject_failure=True),
                    length=size,
                )
            except OSError:
                failed_bytes = uploaded
            else:
                raise AssertionError("injected upload failure did not fire")
            output.seek(0)
            digest = hashlib.sha256()
            uploaded = 0
        store.put_stream(
            key,
            chunks(inject_failure=False),
            length=size,
        )
        upload_seconds = time.perf_counter() - upload_started
        if failure_fraction == 1:
            failed_bytes = size
            output.seek(0)
            store.put_stream(key, output, length=size)
        try:
            return StageSample(
                scenario=scenario,
                location=str(config.AGENT_LOCATION or "unknown"),
                split_index=split_index,
                rows=rows,
                estimated_source_bytes=source_bytes,
                parquet_bytes=size,
                source_wait_seconds=source_wait,
                encode_wall_seconds=encode_wall,
                encode_cpu_seconds=encode_cpu,
                upload_seconds=upload_seconds,
                total_seconds=time.perf_counter() - total_started,
                failed_upload_bytes=failed_bytes,
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
) -> dict:
    if not config.TABLES:
        raise RuntimeError("benchmark requires one configured source table")
    table = config.TABLES[0]
    gate = asyncio.Semaphore(max(1, concurrency))

    async def run(index: int) -> StageSample:
        async with gate:
            return await _materialize_one(
                scenario=scenario,
                table=table,
                split_index=index,
                split_count=split_count,
                failure_fraction=failure_fraction,
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
        "wall_seconds": wall,
        "summary": summarize(samples),
        "samples": [sample.to_dict() for sample in samples],
    }


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
    return parser.parse_args()


if __name__ == "__main__":
    args = _arguments()
    if args.splits < 1 or args.concurrency < 1:
        raise SystemExit("splits and concurrency must be positive")
    async def main() -> dict:
        try:
            return await run_scenario(
                scenario=args.scenario,
                split_count=args.splits,
                concurrency=args.concurrency,
                failure_fraction=args.failure_fraction,
            )
        finally:
            await dispose_engines()

    print(json.dumps(asyncio.run(main()), sort_keys=True))
