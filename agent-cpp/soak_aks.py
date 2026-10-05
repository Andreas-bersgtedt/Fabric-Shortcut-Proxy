#!/usr/bin/env python3
"""Sustained SigV4 GET validation for an isolated C++ Agent in AKS."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import http.client
import json
import math
import os
import random
import threading
import time
from typing import Any


REGION = "us-east-1"
SERVICE = "s3"
SAMPLE_LIMIT = 100_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=80)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--object-key", required=True)
    parser.add_argument("--duration", type=int, required=True)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--object-size", type=int, required=True)
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser.parse_args()


def sign_request(host: str, path: str, access_key: str, secret_key: str) -> dict[str, str]:
    now = dt.datetime.now(dt.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    payload_hash = hashlib.sha256(b"").hexdigest()
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_headers = (
        f"host:{host}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    canonical_request = (
        f"GET\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    )
    scope = f"{date}/{REGION}/{SERVICE}/aws4_request"
    string_to_sign = (
        "AWS4-HMAC-SHA256\n"
        f"{amz_date}\n{scope}\n"
        f"{hashlib.sha256(canonical_request.encode()).hexdigest()}"
    )
    key = hmac.new(("AWS4" + secret_key).encode(), date.encode(), hashlib.sha256).digest()
    key = hmac.new(key, REGION.encode(), hashlib.sha256).digest()
    key = hmac.new(key, SERVICE.encode(), hashlib.sha256).digest()
    key = hmac.new(key, b"aws4_request", hashlib.sha256).digest()
    signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    return {
        "Host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
        "Authorization": (
            "AWS4-HMAC-SHA256 "
            f"Credential={access_key}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
    }


def read_ready(host: str, port: int, timeout: float) -> dict[str, Any]:
    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        connection.request("GET", "/readyz", headers={"Connection": "close"})
        response = connection.getresponse()
        body = response.read()
    finally:
        connection.close()
    if response.status != 200:
        raise RuntimeError(f"readiness returned HTTP {response.status}: {body[:512]!r}")
    result = json.loads(body)
    if result.get("status") != "ready" or result.get("impl") != "cpp":
        raise RuntimeError(f"unexpected readiness response: {result!r}")
    return result


def wait_ready(host: str, port: int, timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error = "not attempted"
    while time.monotonic() < deadline:
        try:
            return read_ready(host, port, min(5.0, timeout))
        except (OSError, http.client.HTTPException, json.JSONDecodeError, RuntimeError) as error:
            last_error = str(error)
            time.sleep(1)
    raise RuntimeError(f"agent did not become ready within {timeout:g}s: {last_error}")


def verify_object(
    host: str,
    port: int,
    path: str,
    access_key: str,
    secret_key: str,
    timeout: float,
    expected_size: int,
    expected_digest: str,
) -> tuple[int, float]:
    authority = f"{host}:{port}" if port != 80 else host
    headers = sign_request(authority, path, access_key, secret_key)
    headers["Connection"] = "close"
    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    started = time.perf_counter()
    try:
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        if response.status != 200:
            body = response.read(4096)
            raise RuntimeError(f"object GET returned HTTP {response.status}: {body[:512]!r}")
        digest = hashlib.sha256()
        received = 0
        while chunk := response.read(64 * 1024):
            digest.update(chunk)
            received += len(chunk)
    finally:
        connection.close()
    elapsed = time.perf_counter() - started
    if received != expected_size:
        raise RuntimeError(f"object GET delivered {received} bytes, expected {expected_size}")
    actual_digest = digest.hexdigest()
    if actual_digest != expected_digest:
        raise RuntimeError(
            f"object GET SHA-256 was {actual_digest}, expected {expected_digest}"
        )
    return received, elapsed


def percentile(values: list[float], percent: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percent * len(ordered)) - 1)]


def main() -> int:
    args = parse_args()
    access_key = os.environ.get("S3_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("S3_SECRET_ACCESS_KEY", "")
    if not access_key or not secret_key:
        raise SystemExit("S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required")
    if args.duration <= 0 or args.concurrency < 1 or args.concurrency > 32:
        raise SystemExit("duration must be positive and concurrency must be between 1 and 32")

    started_at = dt.datetime.now(dt.timezone.utc)
    readiness = wait_ready(args.host, args.port, 300)
    expected_digest = hashlib.sha256(b"\0" * args.object_size).hexdigest()
    path = f"/{args.bucket}/{args.object_key}"
    deadline = time.monotonic() + args.duration
    stop_event = threading.Event()
    lock = threading.Lock()
    sample_random = random.Random(20261005)
    latency_sample: list[float] = []
    successful_requests = 0
    bytes_verified = 0
    errors: list[str] = []

    def worker() -> None:
        nonlocal successful_requests, bytes_verified
        while time.monotonic() < deadline and not stop_event.is_set():
            try:
                received, elapsed = verify_object(
                    args.host,
                    args.port,
                    path,
                    access_key,
                    secret_key,
                    args.timeout,
                    args.object_size,
                    expected_digest,
                )
            except (OSError, http.client.HTTPException, RuntimeError, ValueError) as error:
                with lock:
                    errors.append(f"{type(error).__name__}: {error}")
                stop_event.set()
                return
            with lock:
                successful_requests += 1
                bytes_verified += received
                if len(latency_sample) < SAMPLE_LIMIT:
                    latency_sample.append(elapsed)
                else:
                    index = sample_random.randrange(successful_requests)
                    if index < SAMPLE_LIMIT:
                        latency_sample[index] = elapsed

    workers = [
        threading.Thread(target=worker, name=f"soak-worker-{index}")
        for index in range(args.concurrency)
    ]
    started = time.perf_counter()
    for thread in workers:
        thread.start()

    while any(thread.is_alive() for thread in workers):
        for thread in workers:
            thread.join(timeout=60)
        with lock:
            progress = {
                "event": "progress",
                "elapsed_seconds": round(time.perf_counter() - started, 3),
                "successful_requests": successful_requests,
                "request_errors": len(errors),
            }
        print(json.dumps(progress), flush=True)

    elapsed = time.perf_counter() - started
    completed_at = dt.datetime.now(dt.timezone.utc)
    with lock:
        result = {
            "passed": not errors and elapsed >= args.duration,
            "agent_version": readiness.get("version"),
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "configured_duration_seconds": args.duration,
            "measured_elapsed_seconds": elapsed,
            "concurrency": args.concurrency,
            "object_size_bytes": args.object_size,
            "successful_requests": successful_requests,
            "request_errors": len(errors),
            "errors": errors[:10],
            "bytes_received_and_verified": bytes_verified,
            "response_sha256": expected_digest,
            "throughput_requests_per_second": successful_requests / elapsed if elapsed else 0,
            "latency_sample_size": len(latency_sample),
            "p50_latency_ms": percentile(latency_sample, 0.50) * 1000 if latency_sample else None,
            "p95_latency_ms": percentile(latency_sample, 0.95) * 1000 if latency_sample else None,
        }
    print(f"FINAL_RESULT={json.dumps(result, separators=(',', ':'))}", flush=True)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
