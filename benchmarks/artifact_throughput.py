"""Measure artifact-store upload/download throughput from one runtime site."""
from __future__ import annotations

import argparse
import hashlib
import json
import time
import uuid

from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.runtime.artifact_store import get_default_store

from benchmarks.topology_benchmark import percentile


_CHUNK_SIZE = 1 << 20


def deterministic_chunks(total_bytes: int):
    remaining = total_bytes
    chunk = bytes((index % 251 for index in range(_CHUNK_SIZE)))
    while remaining:
        size = min(remaining, len(chunk))
        yield chunk[:size]
        remaining -= size


def run(*, total_bytes: int, iterations: int) -> dict:
    if total_bytes < 1 or iterations < 1:
        raise ValueError("bytes and iterations must be positive")
    store = get_default_store()
    expected = hashlib.sha256()
    for chunk in deterministic_chunks(total_bytes):
        expected.update(chunk)
    digest = expected.hexdigest()
    uploads = []
    downloads = []
    failures = []
    for iteration in range(iterations):
        key = (
            f"_benchmarks/throughput/{uuid.uuid4().hex}/"
            f"{total_bytes}.bin"
        )
        try:
            started = time.perf_counter()
            store.put_stream(
                key,
                deterministic_chunks(total_bytes),
                length=total_bytes,
            )
            upload_seconds = time.perf_counter() - started
            if not store.verify(
                key,
                size=total_bytes,
                content_hash=digest,
            ):
                raise RuntimeError("uploaded throughput object failed verification")
            started = time.perf_counter()
            downloaded = sum(len(chunk) for chunk in store.get_stream(key))
            download_seconds = time.perf_counter() - started
            if downloaded != total_bytes:
                raise RuntimeError(
                    "throughput download length differs from upload"
                )
            uploads.append(total_bytes / upload_seconds)
            downloads.append(total_bytes / download_seconds)
        except Exception as exc:
            failures.append(
                {
                    "iteration": iteration + 1,
                    "type": type(exc).__name__,
                    "detail": str(exc)[:200],
                }
            )
        finally:
            store.delete(key)
    return {
        "location": str(config.AGENT_LOCATION or "unknown"),
        "bytes_per_iteration": total_bytes,
        "iterations": iterations,
        "successful_iterations": len(uploads),
        "failures": failures,
        "upload_bytes_per_second": {
            "p50": percentile(uploads, 0.50),
            "p95": percentile(uploads, 0.95),
            "min": min(uploads, default=0.0),
            "max": max(uploads, default=0.0),
        },
        "download_bytes_per_second": {
            "p50": percentile(downloads, 0.50),
            "p95": percentile(downloads, 0.95),
            "min": min(downloads, default=0.0),
            "max": max(downloads, default=0.0),
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--iterations", type=int, default=3)
    args = parser.parse_args()
    print(
        json.dumps(
            run(total_bytes=args.bytes, iterations=args.iterations),
            sort_keys=True,
        )
    )
