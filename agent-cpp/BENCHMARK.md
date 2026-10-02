# Local C++ serving-agent benchmark

`benchmark_auth.py` measures local object GET throughput and latency for the C++
serving agent with request authentication set to `trusted-upstream` and `sigv4`.
Each mode gets its own temporary object store and agent process. It uses only Python's
standard library, including a small SigV4 signer, so it does not require the
repository's optional `botocore` dependency.

## Run

From the repository root, with the existing C++ binary present:

```powershell
python agent-cpp/benchmark_auth.py --duration 15 --concurrency 4 --output cpp-auth-benchmark.json
```

`--output` writes complete machine-readable results, including environment metadata,
to the specified JSON path. Parameters are per mode: `--duration` is seconds,
`--concurrency` is simultaneous request workers, `--object-size` is bytes, and
`--timeout` is the per-request/readiness timeout in seconds. `--binary` can select a
different executable. The default binary is `agent-cpp\agent.exe` on Windows or
`agent-cpp\agent` elsewhere. The agent currently limits its worker pool to 32 threads;
the script enforces the same concurrency ceiling.

For each mode the script creates a distinct temporary store containing deterministic
test bytes, launches the agent on loopback with isolated test credentials, checks
`/healthz` and `/readyz`, stops and restarts it on the same port, and checks readiness
again. It makes a verified warm-up GET, then sends concurrent GETs until the
configured measurement window ends. Every response must have HTTP 200, the expected
length, and the expected SHA-256 digest. Any startup, HTTP, response-validation,
worker, or output-file error exits nonzero with a diagnostic. The agent process and
temporary stores/logs are cleaned up on completion or failure.

The reported throughput is successful responses divided by actual elapsed benchmark
wall time; p50 and p95 are nearest-rank percentiles of per-request client-observed
latency, including signing (in SigV4 mode), connection setup, response transfer, and
content verification. The script reports Python/platform/compiler metadata and the
agent version reported by the executable. Compiler discovery describes the local
toolchain, not necessarily the provenance of an already-built binary.

## Recorded local run

This Windows run is supplemental to the parent workflow's Linux Docker benchmark. It
uses the workspace `agent.exe` as-is; its embedded version is the version reported at
runtime, which may differ from the `APP_VERSION` in a newer source edit until the
binary is rebuilt. This is a reproducibility smoke measurement, not a performance
target or a production-capacity claim.

| Setting | Value |
| --- | --- |
| Benchmark command | `python agent-cpp/benchmark_auth.py --duration 15 --concurrency 4 --output cpp-auth-benchmark.json` |
| Binary version reported by `agent.exe --version` | `cpp-1.0.0-rc.1` |
| OS / architecture | Windows 11 (10.0.26200-SP0), AMD64 |
| Python | 3.12.10 |
| Compiler toolchain | MSVC 19.44.35228 for x64 (Visual Studio 2022) |
| Concurrency / object size | 4 / 65,536 bytes (default object size) |
| Measurement duration | 15 seconds per auth mode |

| Authentication mode | Throughput (req/s) | p50 (ms) | p95 (ms) | Requests |
| --- | ---: | ---: | ---: | ---: |
| trusted-upstream | 726.66 | 3.036 | 21.762 | 10,909 |
| sigv4 | 661.51 | 3.058 | 24.028 | 9,935 |

Both modes passed the restart/readiness check; every reported object response was
HTTP 200 with the expected length and SHA-256, with zero request errors. The parent
saved the JSON result in session files as `cpp-auth-benchmark-windows.json`.
Measurements are local loopback and include Python client work; they do not model
network, storage, TLS, proxy, deployment, or production load. Results vary by host,
thermal state, process scheduling, and other work. The benchmark intentionally makes
no target threshold or production-capacity claim.

## Linux Docker run

The parent workflow ran the same 15-second, four-worker workload in Docker on WSL2.
The two modes ran sequentially, with `trusted-upstream` first. The Docker benchmark
driver and host were shared and potentially noisy; request rates and latencies can
reflect scheduling, resource contention, and run order. In particular, this single
measurement does not establish that enabling signature verification improves
performance. Do not compare these Linux figures directly with the Windows run above.

| Setting | Value |
| --- | --- |
| Benchmark command | `python agent-cpp/benchmark_auth.py --duration 15 --concurrency 4 --output /evidence/cpp-auth-benchmark-linux.json` |
| Binary version reported | `cpp-1.0.0-rc.1` |
| OS / architecture | Linux 5.15.167.4-microsoft-standard-WSL2, x86_64, glibc 2.41 |
| Python | 3.12.14 |
| Compiler command | `g++ -O2 -std=c++17 -Wall -Wextra -pthread` |
| Compiler version | Not recorded; parent may append the exact version |
| Concurrency / object size | 4 / 65,536 bytes (default object size) |
| Measurement duration | 15 seconds per auth mode |

| Authentication mode | Throughput (req/s) | p50 (ms) | p95 (ms) | Requests |
| --- | ---: | ---: | ---: | ---: |
| trusted-upstream | 1,900.81 | 1.937 | 3.367 | 28,514 |
| sigv4 | 2,017.95 | 1.862 | 2.864 | 30,272 |

Both modes passed restart/readiness and response status and SHA-256 verification.
The parent recorded the result at `/evidence/cpp-auth-benchmark-linux.json`. These
figures are one shared-host run, not a controlled comparison, production-capacity
estimate, or evidence that SigV4 verification increases throughput.
