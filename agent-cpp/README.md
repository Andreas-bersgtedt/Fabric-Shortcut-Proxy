# C++ serving Agent

Version: `cpp-1.0.0-rc.1`. This is a release candidate, not a stable release.
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

## Stable-release checklist

- [ ] Merge #92's implementation and release-gate changes after CI passes.
- [ ] Review shared auth conformance and single-key scope against #92.
- [ ] Review Linux benchmark evidence and workload-specific performance limits.
- [ ] Test the candidate image in an isolated AKS pod with `sigv4`, without
      changing the gateway-backed production service.
- [ ] Run sustained load and termination/restart checks against the candidate
      image on the target Linux platform.
- [ ] Publish immutable candidate binaries/image and record checksums/digest.
- [ ] Promote only the tested candidate to stable `cpp-1.0.0`.

An existing gateway-backed deployment is not proof of direct C++ SigV4.
Environment-specific deployment evidence belongs in ignored local deployment
records, not this document.

## Candidate artifacts

The main CI workflow uploads a checksummed Linux x64 serving binary and the
benchmark JSON for each verified commit. After reviewing the candidate gates,
tag that exact commit `cpp-v1.0.0-rc.1`. A candidate can be published from the
issue branch while its integration PR is open; merge approval remains a
separate stable-release gate. The C++ release workflow rebuilds and
tests the tagged source, checks that the binary version matches the tag, and
publishes a GitHub prerelease with:

- Linux x64 binary archive;
- Docker-loadable Linux x64 container archive;
- container image ID, benchmark JSON, and `SHA256SUMS`.

Verify archive checksums before extracting or running `docker load`. The image
ID identifies the local image configuration, not a registry manifest digest;
record the immutable registry digest when pushing it for AKS deployment.
The workflow refuses stable publication until the live checklist has been
reviewed and an explicit stable-promotion workflow is implemented.
