"""Benchmark C++ agent ListObjectsV2 latency and memory behavior.

Build first with: bash agent-cpp/build.sh
Example: python tests/benchmark_cpp_agent_list.py --counts 10000,100000 --repeats 5
"""

import argparse
import http.client
import os
import pathlib
import shutil
import statistics
import subprocess
import tempfile
import time
import urllib.parse
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor


ROOT = pathlib.Path(__file__).resolve().parents[1]
AGENT = ROOT / "agent-cpp" / "agent"
BUCKET = "benchmark-bucket"


def hwm_kib(pid):
    for line in pathlib.Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1])
    return 0


def request(port, path):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
    started = time.perf_counter()
    connection.request("GET", path)
    response = connection.getresponse()
    body = response.read()
    elapsed_ms = (time.perf_counter() - started) * 1000
    connection.close()
    if response.status != 200:
        raise RuntimeError(f"GET {path} returned HTTP {response.status}")
    return elapsed_ms, body


def list_page(port, prefix, token="", max_keys=1000):
    query = {"list-type": "2", "prefix": prefix, "max-keys": str(max_keys)}
    if token:
        query["continuation-token"] = token
    path = f"/{BUCKET}/?{urllib.parse.urlencode(query)}"
    elapsed_ms, body = request(port, path)
    root = ET.fromstring(body)
    is_truncated = root.findtext("{*}IsTruncated") == "true"
    next_token = root.findtext("{*}NextContinuationToken") or ""
    key_count = int(root.findtext("{*}KeyCount") or "0")
    return elapsed_ms, key_count, is_truncated, next_token


def create_store(count):
    store = pathlib.Path(tempfile.mkdtemp(prefix=f"cpp-list-{count}-"))
    prefix = store / "objects"
    prefix.mkdir()
    for index in range(count):
        (prefix / f"item-{index:07d}.bin").write_bytes(b"x")
    return store


def start_agent(store, port):
    env = os.environ.copy()
    env.update({"PORT": str(port), "STORE_DIR": str(store), "S3_BUCKET": BUCKET})
    process = subprocess.Popen(
        [str(AGENT)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env
    )
    for _ in range(100):
        try:
            elapsed, body = request(port, "/readyz")
            if b'"status":"ready"' in body:
                return process
        except (OSError, RuntimeError):
            time.sleep(0.1)
    process.terminate()
    process.wait(timeout=5)
    raise RuntimeError("agent did not become ready")


def percentile(values, fraction):
    values = sorted(values)
    index = min(len(values) - 1, int(round((len(values) - 1) * fraction)))
    return values[index]


def benchmark_count(count, repeats, concurrency, port):
    store = create_store(count)
    process = start_agent(store, port)
    try:
        prefix = "objects/"
        first_samples = []
        continuation_samples = []
        first_token = ""
        before_hwm = hwm_kib(process.pid)
        for _ in range(repeats):
            elapsed, key_count, truncated, token = list_page(port, prefix)
            if key_count != min(count, 1000) or (count > 1000 and not truncated):
                raise RuntimeError("unexpected first-page result")
            first_samples.append(elapsed)
            first_token = token

        if count > 1000:
            for _ in range(repeats):
                elapsed, _, _, _ = list_page(port, prefix, first_token)
                continuation_samples.append(elapsed)

        concurrent_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(list_page, port, prefix) for _ in range(concurrency)]
            [future.result() for future in futures]
        concurrent_elapsed = (time.perf_counter() - concurrent_started) * 1000
        after_hwm = hwm_kib(process.pid)

        def summary(values):
            if not values:
                return "n/a"
            return f"p50={percentile(values, 0.50):.1f}ms p95={percentile(values, 0.95):.1f}ms"

        print(
            f"objects={count} first_page={summary(first_samples)} "
            f"continuation_page={summary(continuation_samples)} "
            f"concurrent_{concurrency}={concurrent_elapsed:.1f}ms "
            f"hwm_delta={after_hwm - before_hwm}KiB"
        )
    finally:
        process.terminate()
        process.wait(timeout=5)
        shutil.rmtree(store, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--counts", default="10000,100000", help="comma-separated object counts")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--port", type=int, default=19500 + (os.getpid() % 400))
    args = parser.parse_args()
    if os.name == "nt":
        raise SystemExit("This benchmark requires Linux /proc and the C++ agent binary.")
    if not AGENT.exists():
        raise SystemExit(f"Missing {AGENT}; build it first with bash agent-cpp/build.sh")
    counts = [int(value) for value in args.counts.split(",") if value]
    for offset, count in enumerate(counts):
        benchmark_count(count, args.repeats, args.concurrency, args.port + offset)


if __name__ == "__main__":
    main()
