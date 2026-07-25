# Initial Feature Review: C++ Agent (Phase 6)

Date: 2026-07-25  
Project: Fabric Shortcut Proxy  
Primary reference: [SCALE_ARCHITECTURE_PLAN.md](SCALE_ARCHITECTURE_PLAN.md)

## 1) Scope

This review evaluates [agent-cpp/agent.cpp](agent-cpp/agent.cpp) against the Phase 6 commitments in [SCALE_ARCHITECTURE_PLAN.md](SCALE_ARCHITECTURE_PLAN.md#L642), with emphasis on:

- Contract and architecture alignment
- Deviations and operational risk
- Performance and memory handling opportunities
- Linux deployment alignment (requested point)

## 2) Phase 6 Plan Commitments (Baseline)

From [SCALE_ARCHITECTURE_PLAN.md](SCALE_ARCHITECTURE_PLAN.md#L654):

- C++ serving Agent is Win32 + winsock only and no third-party deps
- Serves S3 data plane from shared store: GET/HEAD with Range, ListObjectsV2, health
- Optional Manager control link: register + heartbeat
- Backlog includes Linux build support (socket layer as the platform-specific part)

## 3) Alignment Summary

Status: Mostly aligned for current Windows serving scope, with portability and hardening gaps.

Aligned:

- Win32 + winsock implementation and no external OSS libs in [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L23)
- S3 serving primitives implemented (GET/HEAD, Range, ListObjectsV2) in [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L263)
- Health endpoints implemented in [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L346)
- Control register/heartbeat path implemented in [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L446)

Needs hardening / partial deviation:

- Path safety checks are not root-canonicalized before open in [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L130)
- Unhandled numeric parse exceptions can terminate the process in [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L117)
- Thread-per-connection model is unbounded in [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L515)

## 4) Findings (Ordered by Severity)

### High

1. Path root escape risk
- Location: [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L130), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L137)
- Issue: Key validation only blocks empty and ".."; absolute/drive-qualified key forms can still bypass intended store-root guarantees.
- Risk: Read outside configured artifact store root.

2. Crash-on-parse risk
- Location: [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L117), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L122), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L287), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L292), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L388), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L424)
- Issue: stoi/stoll use on untrusted values without guard paths.
- Risk: malformed input can terminate process.

### Medium

3. Unbounded detached thread model
- Location: [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L513), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L515)
- Issue: each accepted socket creates a detached thread.
- Risk: memory and scheduler pressure under load spikes.

4. Memory-heavy object handling
- Location: [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L140), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L183)
- Issue: full object read into memory + extra substring allocation for partial responses.
- Risk: elevated peak RSS and allocation churn for large splits.

5. List path cost scales with store size
- Location: [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L226), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L242)
- Issue: full recursive walk + sort per request.
- Risk: O(n log n) CPU/alloc overhead at larger object counts.

### Low

6. Bucket segment ignored
- Location: [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L364), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L366)
- Issue: request bucket parsed but not enforced.
- Risk: semantic drift if strict bucket isolation is required later.

## 5) Performance and Memory Opportunities

1. Stream GET/Range from file in chunks
- Avoid full object buffering and substring copies.

2. HEAD fast-path via file metadata only
- Do not read object bytes for HEAD responses.

3. Replace detached thread-per-connection with bounded worker model
- Fixed worker pool or async socket model to bound memory and improve tail latency.

4. Add socket timeouts and request limits
- Set recv/send timeouts and cap max header bytes.

5. Add safe non-throwing parse helpers
- Parse env/range/status defensively and return explicit protocol errors.

6. Optional listing index/cache for large stores
- Reduce repeated full tree scans for ListObjectsV2.

## 6) Linux Deployment Alignment (Requested Point)

Question: part of the scale architecture plan includes Linux deployment support. How does current implementation align?

Short answer: partially aligned in direction, not yet aligned in deliverable.

Evidence and interpretation:

- Plan intent: Linux is explicitly listed in backlog for Phase 6 in [SCALE_ARCHITECTURE_PLAN.md](SCALE_ARCHITECTURE_PLAN.md#L662).
- Current code: hard-coupled to Windows networking and headers in [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L23), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L24), [agent-cpp/agent.cpp](agent-cpp/agent.cpp#L25).
- Build path: Windows-first via [agent-cpp/build.ps1](agent-cpp/build.ps1).

Conclusion:

- Architecture direction is consistent with plan (Linux acknowledged as next step).
- Current implementation is Windows-only in practical deployment terms.
- Full Linux alignment requires a portability shim around sockets/process bits plus Linux build/test pipeline.

## 7) Recommended Next Slice (to close Linux + hardening gap)

1. Introduce transport abstraction (platform socket shim)
2. Add POSIX backend (Linux) behind same interface
3. Implement canonical path-under-root check
4. Add safe parsing and protocol-level error handling
5. Move GET/Range to chunked streaming + HEAD metadata fast-path
6. Add Linux CI job with conformance smoke parity

## 8) Dependency and Licensing Note

For [agent-cpp/agent.cpp](agent-cpp/agent.cpp), no third-party open-source dependency is present; it uses standard C++ and Windows SDK/Winsock only.

Operational note: runtime redistribution still needs normal Microsoft runtime compliance when distributing Windows binaries.
