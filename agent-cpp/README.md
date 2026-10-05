# C++ serving Agent

Version: `cpp-1.0.0`. This is the stable 1.0 serving release.
Its version is independent of the Python application's version.

## Release scope

The 1.0 serving Agent reads published local/shared-store objects through a
read-only S3-compatible endpoint: GET, HEAD, byte ranges, and object listings.
It registers and heartbeats with Manager using the shared internal Agent-token
contract. Manager and Python materializers still own generation and source
access; experimental native materialization tools are not part of this release.

Two explicit authentication modes are supported:

- `sigv4`: verify authorization-header SigV4 using one environment-backed
  `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` pair and `S3_BUCKET`.
  `S3_ALLOWED_PREFIXES` optionally restricts the key to semicolon-separated
  object prefixes. Credential rotation requires a process/pod restart.
- `trusted-upstream`: a gateway verifies authentication. Keep the Agent private
  and restrict its network ingress to that gateway. This mode does not verify
  signatures locally.

Health endpoints are unsigned. TLS must terminate externally in either mode.
Valid read retries are accepted within the 15-minute clock-skew window.
Presigned query authentication, temporary-session credentials, S3 writes, and
the Python encrypted multi-key credential store are outside the 1.0 scope.
Use the Python gateway for credential-store policies not supported by the
single-key C++ configuration.

See [configuration](../docs/CONFIGURATION.md#83-c-agent-s3-authentication)
and [Kubernetes guidance](../deploy/kubernetes/README.md) for deployment wiring.
Never expose `trusted-upstream` directly to untrusted clients.

## Build and verify

Linux:

```sh
bash agent-cpp/build.sh
bash agent-cpp/build_tier1.sh
FSP_REQUIRE_CPP_TESTS=1 python -m pytest \
  tests/test_cpp_agent_sigv4.py tests/test_sigv4_conformance.py tests/test_auth.py \
  tests/test_cpp_agent_hardening.py -v
python agent-cpp/benchmark_auth.py --duration 15 --concurrency 4 \
  --output cpp-auth-benchmark.json
```

Install the repository's `.[dev]` extras before running Python tests. The
required-test flag fails rather than skips when the binary or signing
dependency is unavailable. CI builds the Linux binary before these tests.

Windows development builds use `agent-cpp\build.ps1` and
`agent-cpp\build_tier1.ps1` with Visual Studio C++ Build Tools. Linux-only socket
hardening tests run in Linux CI, not Windows.

Benchmark output records the actual binary version, concurrency, object size,
throughput, latency, failures and restart checks. Measurements are diagnostic,
not a universal production latency or throughput guarantee.

For the live AKS release gate, run the isolated SigV4 soak through the peered
Linux jump box:

```powershell
.\agent-cpp\Run-AksSoakTest.ps1 -Action Start
.\agent-cpp\Run-AksSoakTest.ps1 -Action Status
.\agent-cpp\Run-AksSoakTest.ps1 -Action Collect
.\agent-cpp\Run-AksSoakTest.ps1 -Action Cleanup
```

The default four-hour run verifies every response body and SHA-256, uses bounded
memory for latency sampling, and fails on any request error. `Start` first
requires a digest-pinned candidate, checks its embedded version, and measures
termination/restart recovery against a five-minute limit. The soak Deployment,
Service, Secret, NetworkPolicy, ConfigMaps, and Job are isolated from the
gateway-backed service and carry a dedicated cleanup label. Collected
environment evidence is local and ignored by Git.

## Stable-release checklist

- [x] Merge #92's implementation and release-gate changes after CI passes.
- [x] Review shared auth conformance and single-key scope against #92.
- [x] Record Linux benchmark evidence with shared-host and client-cost caveats.
- [x] Review workload-specific performance results and document that they are
      diagnostic rather than a universal production-capacity target.
- [x] Test the candidate image in an isolated AKS pod with `sigv4`, without
      changing the gateway-backed production service.
- [x] Run sustained load and termination/restart checks against the candidate
      image on the target Linux platform.
- [x] Publish immutable candidate binaries/image and record checksums/digest.
- [x] Approve the tested candidate implementation for stable `cpp-1.0.0`.

An existing gateway-backed deployment is not proof of direct C++ SigV4.
Environment-specific deployment evidence belongs in ignored local deployment
records, not this document.

## Release artifacts

The main CI workflow uploads a checksummed Linux x64 serving binary and the
benchmark JSON for each verified commit. Release-candidate tags use the
`cpp-v<version>-rc.<number>` form and publish a GitHub prerelease. Stable tags
use the `cpp-v<version>` form and are accepted only when this checklist is
complete. The C++ release workflow rebuilds and tests the tagged source, checks
that the binary version matches the tag, and publishes:

- Linux x64 binary archive;
- Docker-loadable Linux x64 container archive;
- container image ID, benchmark JSON, and `SHA256SUMS`.

Verify archive checksums before extracting or running `docker load`. The image
ID identifies the local image configuration, not a registry manifest digest;
record the immutable registry digest when pushing it for AKS deployment.
Release tests and benchmarks run against the binary extracted from the
candidate container; the binary archive and container archive therefore
contain the same verified serving executable.
