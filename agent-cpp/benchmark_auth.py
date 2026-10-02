#!/usr/bin/env python3
"""Local, end-to-end GET benchmark for the C++ serving Agent."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import http.client
import json
import math
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any


DEFAULT_BINARY = Path(__file__).with_name("agent.exe" if os.name == "nt" else "agent")
BUCKET = "fsp-benchmark-bucket"
OBJECT_KEY = "benchmark/payload.bin"
ACCESS_KEY = "FSPBENCHMARK"
SECRET_KEY = "local-only-not-a-real-credential"
REGION = "us-east-1"
PAYLOAD_BLOCK = hashlib.sha256(b"Fabric Shortcut Proxy local benchmark payload").digest()


class BenchmarkError(RuntimeError):
    """An explicit benchmark setup, request, or validation failure."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare local C++ Agent GET performance with SigV4 on and off."
    )
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY,
                        help=f"agent executable (default: {DEFAULT_BINARY})")
    parser.add_argument("--duration", type=float, default=15.0,
                        help="measurement duration per mode in seconds (default: 15)")
    parser.add_argument("--concurrency", type=int, default=4,
                        help="simultaneous request workers (default: 4)")
    parser.add_argument("--object-size", type=int, default=64 * 1024,
                        help="object size in bytes (default: 65536)")
    parser.add_argument("--timeout", type=float, default=30.0,
                        help="per-request and readiness timeout in seconds (default: 30)")
    parser.add_argument("--output", type=Path,
                        help="optional path for machine-readable results")
    return parser.parse_args()


def percentile(values: list[float], percent: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percent * len(ordered)) - 1)]


def compiler_version() -> str:
    compiler = shutil.which("cl.exe") or shutil.which("cl")
    command: list[str] | None = [compiler, "/Bv"] if compiler else None

    if command is None and os.name == "nt":
        program_files_x86 = os.environ.get("ProgramFiles(x86)")
        if program_files_x86:
            vswhere = Path(program_files_x86) / "Microsoft Visual Studio" / "Installer" / "vswhere.exe"
            if vswhere.is_file():
                located = subprocess.run(
                    [str(vswhere), "-latest", "-products", "*", "-requires",
                     "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
                     "-property", "installationPath"],
                    capture_output=True, text=True, check=False,
                )
                if located.returncode == 0 and located.stdout.strip():
                    toolchain = Path(located.stdout.strip()) / "VC" / "Tools" / "MSVC"
                    candidates = list(toolchain.glob("*/bin/HostX64/x64/cl.exe"))
                    if candidates:
                        latest = max(
                            candidates,
                            key=lambda path: tuple(int(part) for part in path.parents[3].name.split(".")),
                        )
                        command = [str(latest), "/Bv"]

    if command is None:
        return "not detected (prebuilt binary; compiler may not be on this host)"
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        return "not detected (prebuilt binary; compiler query failed)"
    output = result.stdout + result.stderr
    match = re.search(r"compiler version\s+([\d.]+)", output, re.IGNORECASE)
    if match:
        return f"MSVC {match.group(1)}"
    return "not detected (prebuilt binary; compiler version unavailable)"


def make_payload(store: Path, size: int) -> str:
    target = store.joinpath(*OBJECT_KEY.split("/"))
    target.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    remaining = size
    with target.open("wb") as output:
        while remaining:
            count = min(remaining, 64 * 1024)
            chunk = (PAYLOAD_BLOCK * ((count + len(PAYLOAD_BLOCK) - 1) // len(PAYLOAD_BLOCK)))[:count]
            output.write(chunk)
            digest.update(chunk)
            remaining -= count
    return digest.hexdigest()


def available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def sign_request(host: str, path: str) -> dict[str, str]:
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
    scope = f"{date}/{REGION}/s3/aws4_request"
    string_to_sign = (
        "AWS4-HMAC-SHA256\n"
        f"{amz_date}\n{scope}\n"
        f"{hashlib.sha256(canonical_request.encode()).hexdigest()}"
    )
    key = hmac.new(("AWS4" + SECRET_KEY).encode(), date.encode(), hashlib.sha256).digest()
    key = hmac.new(key, REGION.encode(), hashlib.sha256).digest()
    key = hmac.new(key, b"s3", hashlib.sha256).digest()
    key = hmac.new(key, b"aws4_request", hashlib.sha256).digest()
    signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    authorization = (
        "AWS4-HMAC-SHA256 "
        f"Credential={ACCESS_KEY}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return {
        "Host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
        "Authorization": authorization,
    }


def read_http(host: str, port: int, path: str, timeout: float,
              expected_version: str | None = None) -> tuple[int, bytes]:
    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        connection.request("GET", path, headers={"Connection": "close"})
        response = connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise BenchmarkError(f"GET {path} returned HTTP {response.status}: {body[:512]!r}")
        if expected_version is not None:
            try:
                status = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise BenchmarkError(f"GET {path} returned invalid readiness JSON") from error
            if status.get("impl") != "cpp" or status.get("version") != expected_version:
                raise BenchmarkError(f"GET {path} reported unexpected agent identity: {status!r}")
        return response.status, body
    finally:
        connection.close()


def wait_ready(host: str, port: int, process: subprocess.Popen[bytes],
               timeout: float, log_path: Path) -> dict[str, str]:
    deadline = time.monotonic() + timeout
    last_error = "no readiness response"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise BenchmarkError(
                f"agent exited with code {process.returncode} before readiness\n{tail_log(log_path)}"
            )
        try:
            _, health = read_http(host, port, "/healthz", min(1.0, timeout))
            _, ready = read_http(host, port, "/readyz", min(1.0, timeout))
            health_json = json.loads(health)
            ready_json = json.loads(ready)
            if health_json.get("impl") != "cpp" or health_json.get("version") != ready_json.get("version"):
                raise BenchmarkError(f"unexpected health/readiness payload: {health_json!r}, {ready_json!r}")
            if ready_json.get("status") != "ready":
                raise BenchmarkError(f"/readyz returned an unexpected status: {ready_json!r}")
            return health_json
        except (OSError, http.client.HTTPException, json.JSONDecodeError, BenchmarkError) as error:
            last_error = str(error)
            if process.poll() is not None:
                raise BenchmarkError(
                    f"agent exited with code {process.returncode} before readiness: {last_error}\n"
                    f"{tail_log(log_path)}"
                ) from error
            time.sleep(0.1)
    raise BenchmarkError(f"agent did not become ready within {timeout:g}s: {last_error}\n{tail_log(log_path)}")


def tail_log(path: Path) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return "(agent log unavailable)"
    return "\n".join(lines[-20:]) or "(agent produced no log output)"


def stop_agent(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def launch_agent(binary: Path, store: Path, mode: str, port: int, concurrency: int,
                 stdout_path: Path, stderr_path: Path) -> subprocess.Popen[bytes]:
    environment = os.environ.copy()
    environment.update({
        "HOST": "127.0.0.1",
        "PORT": str(port),
        "STORE_DIR": str(store),
        "S3_BUCKET": BUCKET,
        "S3_AUTH_MODE": mode,
        "S3_ACCESS_KEY_ID": ACCESS_KEY,
        "S3_SECRET_ACCESS_KEY": SECRET_KEY,
        "S3_ALLOWED_PREFIXES": "",
        "MANAGER_URL": "",
        "MATERIALIZE_MODE": "eager",
        "MAX_INFLIGHT": str(max(8, concurrency + 2)),
        "SOCKET_TIMEOUT_MS": "30000",
        "INDEX_REFRESH_SECONDS": "0",
        "AGENT_DRAIN_GRACE_SECONDS": "0",
        "AGENT_ID": f"local-benchmark-{mode}",
    })
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        return subprocess.Popen(
            [str(binary)], cwd=str(binary.parent), env=environment,
            stdout=stdout, stderr=stderr,
        )


def verify_object(host: str, port: int, mode: str, timeout: float,
                  expected_size: int, expected_digest: str) -> tuple[int, float]:
    path = f"/{BUCKET}/{OBJECT_KEY}"
    headers = sign_request(f"{host}:{port}", path) if mode == "sigv4" else {"Connection": "close"}
    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    started = time.perf_counter()
    try:
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        if response.status != 200:
            error_body = response.read(4096)
            raise BenchmarkError(f"object GET returned HTTP {response.status}: {error_body[:512]!r}")
        content_length = response.getheader("Content-Length")
        if content_length is None or int(content_length) != expected_size:
            raise BenchmarkError(
                f"object GET Content-Length was {content_length!r}, expected {expected_size}"
            )
        digest = hashlib.sha256()
        received = 0
        while True:
            chunk = response.read(64 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            received += len(chunk)
        if received != expected_size:
            raise BenchmarkError(f"object GET delivered {received} bytes, expected {expected_size}")
        actual_digest = digest.hexdigest()
        if actual_digest != expected_digest:
            raise BenchmarkError(
                f"object GET content digest mismatch: got {actual_digest}, expected {expected_digest}"
            )
        return received, time.perf_counter() - started
    finally:
        connection.close()


def run_mode(binary: Path, mode: str, duration: float, concurrency: int,
             object_size: int, timeout: float, temp_root: Path) -> dict[str, Any]:
    mode_root = temp_root / mode
    store = mode_root / "store"
    store.mkdir(parents=True)
    expected_digest = make_payload(store, object_size)
    port = available_port()
    host = "127.0.0.1"
    stdout_path, stderr_path = mode_root / "agent.stdout.log", mode_root / "agent.stderr.log"
    stop_event = threading.Event()
    workers: list[threading.Thread] = []
    process = launch_agent(binary, store, mode, port, concurrency, stdout_path, stderr_path)
    try:
        identity = wait_ready(host, port, process, timeout, stderr_path)
        version = identity["version"]
        stop_agent(process)
        process = launch_agent(binary, store, mode, port, concurrency, stdout_path, stderr_path)
        wait_ready(host, port, process, timeout, stderr_path)

        verify_object(host, port, mode, timeout, object_size, expected_digest)
        errors: list[str] = []
        latencies: list[float] = []
        bytes_served = 0
        stats_lock = threading.Lock()
        deadline = time.monotonic() + duration

        def worker() -> None:
            nonlocal bytes_served
            while time.monotonic() < deadline and not stop_event.is_set():
                try:
                    count, elapsed = verify_object(
                        host, port, mode, timeout, object_size, expected_digest
                    )
                except (OSError, http.client.HTTPException, BenchmarkError, ValueError) as error:
                    with stats_lock:
                        errors.append(f"{type(error).__name__}: {error}")
                    stop_event.set()
                    return
                with stats_lock:
                    latencies.append(elapsed)
                    bytes_served += count

        workers = [
            threading.Thread(target=worker, name=f"{mode}-worker-{i}")
            for i in range(concurrency)
        ]
        started = time.perf_counter()
        for worker_thread in workers:
            worker_thread.start()
        for worker_thread in workers:
            worker_thread.join(timeout=duration + timeout + 5)
        still_running = [worker_thread.name for worker_thread in workers if worker_thread.is_alive()]
        if still_running:
            stop_event.set()
            raise BenchmarkError(f"load workers did not stop: {', '.join(still_running)}")
        elapsed = time.perf_counter() - started
        if errors:
            details = "; ".join(errors[:5])
            extra = f" ({len(errors) - 5} more errors)" if len(errors) > 5 else ""
            raise BenchmarkError(
                f"{mode} load failed: {details}{extra}\n"
                f"agent stderr:\n{tail_log(stderr_path)}\n"
                f"agent stdout:\n{tail_log(stdout_path)}"
            )
        if not latencies:
            raise BenchmarkError(f"{mode} load completed no successful requests")

        return {
            "mode": mode,
            "binary_version": version,
            "restart_readiness_check": "passed",
            "configured_duration_seconds": duration,
            "measured_elapsed_seconds": elapsed,
            "concurrency": concurrency,
            "object_size_bytes": object_size,
            "successful_requests": len(latencies),
            "throughput_requests_per_second": len(latencies) / elapsed,
            "p50_latency_ms": percentile(latencies, 0.50) * 1000,
            "p95_latency_ms": percentile(latencies, 0.95) * 1000,
            "bytes_received_and_verified": bytes_served,
            "response_sha256": expected_digest,
            "request_errors": 0,
        }
    finally:
        stop_event.set()
        stop_agent(process)
        for worker_thread in workers:
            worker_thread.join(timeout=timeout + 5)
        still_running = [worker_thread.name for worker_thread in workers if worker_thread.is_alive()]
        if still_running:
            raise BenchmarkError(
                f"load workers did not stop during cleanup: {', '.join(still_running)}"
            )


def main() -> int:
    args = parse_args()
    binary = args.binary.expanduser().resolve()
    if not binary.is_file():
        print(f"ERROR: agent binary does not exist: {binary}", file=sys.stderr)
        return 2
    if args.duration <= 0 or not math.isfinite(args.duration):
        print("ERROR: --duration must be a finite positive number", file=sys.stderr)
        return 2
    if args.concurrency < 1 or args.concurrency > 32:
        print("ERROR: --concurrency must be between 1 and 32 (agent worker limit)", file=sys.stderr)
        return 2
    if args.object_size < 1 or args.object_size > 1024 * 1024 * 1024:
        print("ERROR: --object-size must be between 1 byte and 1 GiB", file=sys.stderr)
        return 2
    if args.timeout <= 0 or not math.isfinite(args.timeout):
        print("ERROR: --timeout must be a finite positive number", file=sys.stderr)
        return 2

    try:
        version_result = subprocess.run(
            [str(binary), "--version"], capture_output=True, text=True,
            check=False, timeout=args.timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"ERROR: could not query agent version: {error}", file=sys.stderr)
        return 2
    if version_result.returncode != 0:
        print(
            f"ERROR: agent --version exited {version_result.returncode}: "
            f"{version_result.stderr.strip()}",
            file=sys.stderr,
        )
        return 2

    metadata = {
        "agent_binary": str(binary),
        "agent_binary_version": version_result.stdout.strip(),
        "python_version": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "compiler": compiler_version(),
        "benchmark": {
            "duration_seconds_per_mode": args.duration,
            "concurrency": args.concurrency,
            "object_size_bytes": args.object_size,
            "timeout_seconds": args.timeout,
        },
    }
    print(
        f"Binary {metadata['agent_binary_version']} | {metadata['platform']} "
        f"| Python {platform.python_version()} | {metadata['compiler']}"
    )
    try:
        with tempfile.TemporaryDirectory(prefix="fsp-cpp-benchmark-") as temporary:
            temp_root = Path(temporary)
            results = [
                run_mode(binary, mode, args.duration, args.concurrency,
                         args.object_size, args.timeout, temp_root)
                for mode in ("trusted-upstream", "sigv4")
            ]
    except (BenchmarkError, OSError, subprocess.SubprocessError, KeyboardInterrupt) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    output = {"environment": metadata, "results": results}
    print("\nResults")
    print("mode               req/s       p50 ms       p95 ms      requests")
    for result in results:
        print(
            f"{result['mode']:<18} "
            f"{result['throughput_requests_per_second']:>8.2f} "
            f"{result['p50_latency_ms']:>12.3f} "
            f"{result['p95_latency_ms']:>12.3f} "
            f"{result['successful_requests']:>13}"
        )
    print("Readiness + restart check: passed in both modes; every measured response status and SHA-256 verified.")
    if args.output:
        output_path = args.output.expanduser().resolve()
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
        except OSError as error:
            print(f"ERROR: could not write JSON output {output_path}: {error}", file=sys.stderr)
            return 1
        print(f"JSON results: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
