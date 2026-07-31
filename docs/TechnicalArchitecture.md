# Technical Architecture

Detailed architecture for the Fabric Shortcut Proxy. The first diagram is the
high-level view (as in [README.md](../README.md)); the sections that follow drill
into each process with its granular components and control/data flow. Every
diagram is grounded in the actual modules referenced beneath it.

> Companion docs: [README.md](../README.md) (setup), [CONFIGURATION.md](CONFIGURATION.md)
> (settings), [SECURITY.md](SECURITY.md) (auth/TLS/audit),
> [SCALE_ARCHITECTURE_PLAN.md](SCALE_ARCHITECTURE_PLAN.md) (fleet),
> [DELTA_FORMAT.md](DELTA_FORMAT.md) (Delta output).

---

## 1. High-level architecture

```mermaid
flowchart LR
  Fabric[Microsoft Fabric / S3 clients]

  subgraph Proxy["Fabric Shortcut Proxy (FastAPI)"]
    AUTH["Auth middleware<br/>SigV4 (multi-key) + per-key ACL<br/>+ forced mount auth"]
    RT["s3/router.py<br/>GET / HEAD / ListObjectsV2 (+ range)"]
    subgraph WH["Warehouse bucket — DB to table"]
      RES["Iceberg / Delta resolver"]
      GEN["SQL pushdown to Parquet<br/>planner + db + parquet"]
    end
    subgraph MNT["Mounted buckets — storage proxy"]
      PT["passthrough<br/>byte streaming + range"]
      LOC["local — NFS / SMB"]
      S3B["s3 / MinIO"]
      AZ["azure Blob / ADLS"]
    end
    AUD["audit log"]
  end

  SRC[(Source RDBMS)]
  UP[(Upstream S3 / Azure / file share)]

  Fabric -->|S3 + SigV4| AUTH --> RT
  RT -->|warehouse bucket| RES --> GEN -->|SQL| SRC
  RT -->|mounted bucket| PT --> LOC & S3B & AZ --> UP
  AUTH -. mount denials .-> AUD
  PT -. access .-> AUD
```

**Two serving modes, one S3 front door.** A bucket with **no mount** resolves
through the DB→Iceberg/Delta path; a bucket **with a mount** streams bytes from a
storage backend. Both share the same SigV4 auth + audit seam.

---

## 2. Package / module map

```mermaid
flowchart TB
  subgraph Entry["Entrypoints"]
    MAIN["main.py<br/>Agent app + auth/trace middleware + TLS"]
    MGR["manager.py<br/>Manager control-plane app"]
  end

  subgraph Front["S3 front door — s3/"]
    ROUTER["router.py"]
    AUTHV["auth.py — SigV4 verify"]
    XML["xml_responses.py"]
  end

  subgraph Sec["security/"]
    AK["access_keys.py — keys + ACL"]
    CS["credential_store.py — DPAPI/Fernet"]
    SCRUB["credentials.py — scrubbing"]
  end

  subgraph Meta["Table metadata"]
    ICE["iceberg/ — schema, metadata,<br/>manifest, stats, state_store, freshness"]
    DELTA["delta/log.py"]
  end

  subgraph Gen["Generation"]
    PLAN["planner/ — split_planner,<br/>dialects, shard_weight"]
    DB["db/ — executor, reflect, capabilities"]
    PARQ["parquet/generator.py"]
  end

  subgraph Store["storage/ + runtime/ + cache/"]
    MOUNTS["storage/mounts.py"]
    PASS["storage/passthrough.py"]
    S3S["storage/s3_store.py + s3_auth.py"]
    AZS["storage/azure_store.py + azure_auth.py"]
    ART["runtime/artifact_store.py"]
    LRU["cache/lru_cache.py"]
  end

  subgraph Ctl["control/ — Manager/Agent plane"]
    REG["registry.py"]
    SUP["supervisor.py"]
    GW["gateway.py"]
    LEASE["lease.py (HA)"]
    ADMIN["admin.py (/_manager)"]
    CONTRACT["contract.py + transport.py"]
  end

  subgraph Obs["observability/"]
    LOG["logging.py"]
    MET["metrics.py"]
    TRACE["trace.py"]
    QS["querystats.py"]
    AUDIT["audit.py"]
    EP["endpoints.py"]
  end

  CFG["config.py + system_config.py + connection_config.py"]

  MAIN --> ROUTER --> AUTHV & XML
  ROUTER --> ICE & DELTA & PASS
  ICE --> PLAN --> DB
  ICE --> PARQ
  PASS --> MOUNTS --> S3S & AZS & ART
  ROUTER --> LRU
  MAIN --> AK --> CS
  MGR --> REG & SUP & GW & LEASE & ADMIN
  MAIN -.-> CFG
  MAIN --> Obs
```

Referenced: [s3/router.py](../s3/router.py), [security/access_keys.py](../security/access_keys.py),
[iceberg/state_store.py](../iceberg/state_store.py), [storage/mounts.py](../storage/mounts.py),
[control/registry.py](../control/registry.py), [config.py](../config.py).

---

## 3. Request lifecycle — auth middleware

The outermost hop for every S3 request. Grounded in
[main.py](../main.py) (`sigv4_auth_middleware`) and [s3/auth.py](../s3/auth.py).

```mermaid
sequenceDiagram
  autonumber
  participant C as Client (Fabric)
  participant MW as sigv4_auth_middleware (main.py)
  participant AK as security.access_keys
  participant V as s3.auth.verify_signature
  participant RT as s3.router
  participant AU as observability.audit

  C->>MW: HTTP request /{bucket}/{key}
  alt method OPTIONS or exempt prefix (/healthz, /_config, ...)
    MW->>RT: pass through (no auth)
  else data path
    MW->>MW: parse bucket/key, mounted = get_mount(bucket) is not None
    MW->>MW: require = REQUIRE_SIGV4 or (mounted and ENFORCE_MOUNT_AUTH)
    alt require auth
      MW->>V: verify_signature(secret_resolver=AK.resolve_secret)
      V->>AK: resolve_secret(access_key_id)
      AK-->>V: signing secret (ACL key, else legacy wildcard)
      alt bad signature / unknown key
        V-->>MW: SigV4Error
        MW->>AU: record(denial) if mounted
        MW-->>C: 403 AccessDenied (S3 XML)
      else verified, sets identity
        MW->>AK: authorize(identity, bucket, key, method)
        alt out of scope / write / disabled
          MW->>AU: record(denial) if mounted
          MW-->>C: 403 AccessDenied
        else allowed
          MW->>MW: request.state.identity = identity
          MW->>RT: dispatch
        end
      end
    else no auth required
      MW->>RT: dispatch
    end
  end
```

---

## 4. Access keys + per-key authorization (ACL)

Grounded in [security/access_keys.py](../security/access_keys.py) and
[security/credential_store.py](../security/credential_store.py).

```mermaid
flowchart TB
  REQ["presented access_key_id<br/>(from SigV4 Credential scope)"]

  subgraph AK["security.access_keys"]
    CACHE["_all_keys()<br/>5s TTL snapshot cache"]
    RESOLVE["resolve_secret(id)"]
    AUTHZ["authorize(id, bucket, key, method)"]
  end

  subgraph STORE["security.credential_store (encrypted)"]
    DPAPI["DPAPI (Windows)"]
    FERNET["Fernet (other OS)"]
    SECT["access_keys section<br/>{id: enc(record)}"]
  end

  LEGACY["config.ACCESS_KEY_ID / SECRET<br/>(implicit wildcard)"]

  REQ --> RESOLVE --> CACHE --> SECT
  SECT --- DPAPI & FERNET
  RESOLVE -->|no ACL key| LEGACY
  REQ --> AUTHZ --> CACHE
  AUTHZ --> D1{"method in GET/HEAD?"}
  D1 -->|no| DENY["read-only: deny writes"]
  D1 -->|yes| D2{"bucket in allowed_buckets or '*'?"}
  D2 -->|no| DENY2["not authorized for bucket"]
  D2 -->|yes| D3{"key under allowed_prefixes[bucket]?"}
  D3 -->|no| DENY3["not authorized for prefix"]
  D3 -->|yes| ALLOW["None (allowed)"]
```

Record shape (encrypted at rest): `{access_key_id, secret_key, label,
allowed_buckets, allowed_prefixes, permissions, enabled}`.

---

## 5. Warehouse read path — metadata & data

The DB→table serving path in [s3/router.py](../s3/router.py) `get_object`.

```mermaid
flowchart TB
  GET["GET /{bucket}/{key}"] --> N["_normalize_incoming_key<br/>(legacy alias -> active)"]
  N --> D{"TABLE_FORMAT?"}

  D -->|delta and /_delta_log/| DL["delta.log.get_commit_bytes"]
  DL --> RESP

  D -->|iceberg| SNAP["_resolve_snapshot_for_key(key)"]
  SNAP --> K{"which object kind?"}
  K -->|metadata.json| M1["cache.get_metadata<br/>else build_metadata_json -> put"]
  K -->|version-hint.text| M2["version hint bytes"]
  K -->|manifest list .avro| M3["build_manifest_list (cache)"]
  K -->|manifest file .avro| M4["build_manifest_file (cache)"]
  M1 & M2 & M3 & M4 --> RESP

  K -->|*.parquet data| SPLIT["get_split_by_key(key)"]
  SPLIT --> CH{"cache.get_parquet hit?"}
  CH -->|yes| RESP2["pinned/cached bytes"]
  CH -->|no| SEM["_generation_semaphore<br/>(bounded concurrency)"]
  SEM --> Q["build_split_query (planner.dialects)"]
  Q --> EX["db.executor.execute_split_query -> rows"]
  EX --> PG["parquet.generator.rows_to_parquet"]
  PG --> PUT["cache.put_parquet (+ pin)"]
  PUT --> RESP2

  RESP["_make_object_response (+ HTTP range)"]
  RESP2 --> RESP
```

Data-generation timing (`querystats.record_query`) captures SQL vs generation vs
total ms per split. Referenced: [planner/split_planner.py](../planner/split_planner.py),
[db/executor.py](../db/executor.py), [parquet/generator.py](../parquet/generator.py),
[cache/lru_cache.py](../cache/lru_cache.py).

---

## 6. Snapshot state & data freshness

Deterministic, content-addressed snapshots. Grounded in
[iceberg/state_store.py](../iceberg/state_store.py) and [iceberg/freshness.py](../iceberg/freshness.py).

```mermaid
flowchart TB
  subgraph BUILD["Snapshot build (startup / refresh)"]
    T["TableDef (config.TABLES)"] --> PATH["active_table_path<br/>legacy db/&lt;table&gt; OR canonical db/&lt;srv&gt;/&lt;db&gt;/&lt;schema&gt;/&lt;obj&gt;"]
    PATH --> PLANS["split planning<br/>planner.split_planner + shard_weight"]
    PLANS --> SNAPO["SnapshotState<br/>metadata_key, manifest keys, splits[]"]
    SNAPO --> REG["state registry (versioned)"]
  end

  subgraph FRESH["Auto-refresh poller (AUTO_REFRESH)"]
    POLL["freshness poller<br/>every REFRESH_POLL_SECONDS"] --> STRAT{"REFRESH_STRATEGY"}
    STRAT -->|dialect_probe| PB["cheap change probe"]
    STRAT -->|content_hash| RH["re-read + hash rows"]
    STRAT -->|ttl / manual| TT["window / on-demand"]
    PB & RH & TT --> MAT["materialize_table<br/>content-addressed split-&lt;hash&gt;.parquet"]
    MAT --> CHG{"content changed?"}
    CHG -->|no| NOOP["no-op (stable ids)"]
    CHG -->|yes| PUB["publish new snapshot version<br/>+ retain history (SNAPSHOT_HISTORY_LIMIT)"]
  end

  REG --> SERVE["served by s3.router / _snapshot_objects"]
  PUB --> REG
```

Each split file is named by the hash of **its rows**, so identical data is
restart-stable and any change yields a new path + `current-snapshot-id`.

---

## 7. Storage proxy — passthrough serving

Read-only byte passthrough for mounted buckets. Grounded in
[storage/passthrough.py](../storage/passthrough.py) and [storage/mounts.py](../storage/mounts.py).

```mermaid
flowchart TB
  REQ["s3.router: bucket in MOUNTS"] --> H{"operation"}
  H -->|List| LIST["passthrough.list_objects"]
  H -->|HEAD| HEAD["passthrough.head_object"]
  H -->|GET| GETO["passthrough.get_object"]

  subgraph REGY["storage.mounts"]
    GM["get_mount(bucket)<br/>(gated by ENABLE_STORAGE_PROXY)"]
    BF["backend_for(mount)<br/>lazy build + cache"]
  end

  LIST & HEAD & GETO --> GM --> BF --> BK{"mount.backend"}
  BK -->|local| LDS["runtime.LocalDirStore<br/>os.scandir / seek"]
  BK -->|s3| S3ST["storage.s3_store.S3Store<br/>boto3: head/get_object(Range)/list_v2"]
  BK -->|azure| AZST["storage.azure_store.AzureBlobStore<br/>ContainerClient: get_blob/download(offset,length)"]

  GETO --> CONF["_backend_key: prefix-confine + reject '..'"]
  CONF --> RANGE["_parse_range -> offset/length (or 416)"]
  RANGE --> STREAM["store.get_stream(offset,length)<br/>chunked StreamingResponse (206/200)"]
  LDS & S3ST & AZST --> STREAM

  GETO -. audit .-> AU["observability.audit.record"]
  LIST -. audit .-> AU
  HEAD -. audit .-> AU
```

Backends implement the same [runtime/artifact_store.py](../runtime/artifact_store.py)
`ArtifactStore` interface (`head`, `get_stream`, `list`, `list_dir`), so serving
code is backend-agnostic.

---

## 8. Outbound credential mediation

Upstream secrets never reach the client. Grounded in
[storage/s3_auth.py](../storage/s3_auth.py), [storage/azure_auth.py](../storage/azure_auth.py),
and [security/credential_store.py](../security/credential_store.py).

```mermaid
flowchart TB
  MOUNT["Mount (config.mounts.json)<br/>backend, root, endpoint, credential id, auth"]

  subgraph RESOLVE["resolve_{s3,azure}_auth"]
    CID{"credential id set?"}
    CID -->|yes| GET["credential_store.get_secret(id)"]
    CID -->|no| MODE["explicit auth mode<br/>(anonymous / instance / default / managed_identity)"]
  end

  subgraph CSTORE["credential_store (encrypted)"]
    SECT["secrets section {id: enc(blob)}"]
  end

  MOUNT --> CID
  GET --> SECT

  subgraph S3["build_s3_client (boto3)"]
    S3M["static · session · assume_role · web_identity<br/>profile · sso · instance · process · anonymous"]
  end
  subgraph AZ["build_container_client (azure-storage-blob)"]
    AZM["connection_string · account_key · sas<br/>aad_client_secret · managed_identity · default · anonymous"]
  end

  GET --> S3M
  GET --> AZM
  MODE --> S3M
  MODE --> AZM
  S3M --> UPS[(Upstream S3 / MinIO)]
  AZM --> UPA[(Azure Blob / ADLS)]
```

Non-secret knobs (endpoint, region, addressing/signature style, TLS verify) live
on the mount; secret material lives only in the encrypted store.

---

## 9. Caching & artifact store

Grounded in [cache/lru_cache.py](../cache/lru_cache.py) and
[runtime/artifact_store.py](../runtime/artifact_store.py).

```mermaid
flowchart LR
  subgraph LRU["cache.lru_cache"]
    META["metadata cache<br/>(metadata.json / manifests)"]
    PARQ["parquet cache<br/>(LRU by bytes, TTL)"]
    PIN["pinned splits<br/>(byte-identical snapshot data)"]
    DISK["optional disk cache<br/>(PARQUET_DISK_CACHE)"]
  end

  subgraph AS["runtime.artifact_store (ArtifactStore)"]
    MEM["MemoryStore"]
    LOCAL["LocalDirStore<br/>(fs / NFS / SMB)"]
    IFACE["put/get/head/exists/list/delete<br/>+ get_stream + list_dir"]
  end

  ROUTER["s3.router / delta.log"] --> META & PARQ
  PARQ --> PIN
  PARQ <--> DISK
  PARQ -. write-through (fleet) .-> AS
  AS --- MEM & LOCAL
```

In fleet mode the parquet cache can write through to a shared `ArtifactStore` so
stateless Agents serve the Manager's published splits.

---

## 10. Configuration system

Layered, multi-file config with a settings registry. Grounded in
[config.py](../config.py), [system_config.py](../system_config.py),
[connection_config.py](../connection_config.py).

```mermaid
flowchart TB
  subgraph SRC["Sources (precedence: env > file > default)"]
    ENV["environment variables"]
    SYS["config.system.json"]
    PERF["config.performance.json"]
    FRESHF["config.freshness.json"]
    TAB["config.tables.json"]
    CONN["config.connection.json"]
    MNT["config.mounts.json"]
  end

  subgraph LOAD["config loading"]
    LCF["_load_config_file()<br/>reads section files"]
    SYSC["system_config.py"]
    CONNC["connection_config.py"]
  end

  subgraph API["config.py"]
    REGY["_SETTINGS_REGISTRY (_register)"]
    META["SETTINGS_META (cat/help/secret)"]
    MAP["_SETTINGS_TO_FILE_MAP"]
    VAL["validate_setting_updates"]
    WRITE["write_config_updates<br/>(atomic, per-section)"]
    CAT["settings_catalog / effective_settings"]
  end

  ENV & SYS & PERF & FRESHF --> LOAD
  TAB & CONN & MNT --> LOAD
  LOAD --> API
  VAL --> WRITE --> SYS & PERF & FRESHF & TAB & CONN
  CAT --> UI["/_config builder + monitor"]
```

Secret material (DB URLs, upstream creds, access keys) is **not** stored in these
JSON files — it lives in the encrypted credential store.

---

## 11. Manager / Agent control plane (fleet)

Stateless Agents behind a Manager. Grounded in [control/](../control) —
`registry.py`, `supervisor.py`, `gateway.py`, `lease.py`, `admin.py`,
`contract.py`, and [runtime/agent_link.py](../runtime/agent_link.py).

```mermaid
flowchart TB
  Fabric[Fabric / S3 clients] --> GW

  subgraph MGR["Manager (manager.py + control.manager_app)"]
    GW["gateway.Gateway<br/>round-robin pick() over ready Agents"]
    REG["registry.Registry<br/>register / heartbeat / dead detection"]
    SUP["supervisor.AgentSupervisor<br/>spawn / restart / crash-loop guard + RSS"]
    LEASE["lease.LeaderLease (HA primary)"]
    JOBS["materialization jobs<br/>split plan + publish snapshot"]
    ADMIN["admin.py — /_manager fleet console"]
  end

  subgraph FLEET["Agent fleet (stateless)"]
    A1["Agent 1 (main.py)<br/>S3 data plane + agent_link"]
    A2["Agent 2"]
    AN["Agent N"]
  end

  OBJ[(Shared artifact store)]
  SRC[(Source RDBMS)]

  GW --> A1 & A2 & AN
  A1 & A2 & AN -->|register / heartbeat / pull assignment| REG
  REG --- LEASE & JOBS
  SUP -->|spawn / restart| A1 & A2 & AN
  JOBS -->|assign split-gen| A1 & A2 & AN
  A1 & A2 & AN -->|SQL scans| SRC
  A1 & A2 & AN -->|read/write materialized splits| OBJ
  ADMIN --- REG & SUP
```

### 11a. Register / heartbeat contract

```mermaid
sequenceDiagram
  autonumber
  participant AG as Agent (runtime.agent_link)
  participant MG as Manager (control.server / transport)
  participant RG as control.registry.Registry

  AG->>MG: RegisterRequest(agent_id, host, port, os, version)
  MG->>RG: register()
  RG-->>MG: RegisterResponse(lease, contract_version)
  MG-->>AG: assignment + snapshot
  loop every HEARTBEAT_MS
    AG->>MG: HeartbeatRequest(agent_id, lease, serving stats)
    MG->>RG: heartbeat() returns pending ControlCommands
    RG-->>MG: Assignment / Drain / ReloadConfig / PublishSnapshot
    MG-->>AG: commands (Ack)
  end
  Note over RG: after miss_limit the agent is dead and excluded by gateway.pick()
```

Message shapes are transport-neutral dataclasses in
[control/contract.py](../control/contract.py) with a dict/JSON codec, mirrored by
`control/proto/control.proto`.

---

## 12. Observability & audit

Grounded in [observability/](../observability) — `logging.py`, `metrics.py`,
`trace.py`, `querystats.py`, `audit.py`, `endpoints.py`.

```mermaid
flowchart LR
  subgraph REQ["per request"]
    RT["s3.router / passthrough"]
  end

  subgraph OBS["observability"]
    LOG["logging.py (structlog, scrubbed)"]
    MET["metrics.py<br/>s3_requests, bytes_served, cache, SQL latency"]
    TRACE["trace.py<br/>Fabric read timeline (ring buffer)"]
    QS["querystats.py<br/>SQL vs Parquet lag per split"]
    AUD["audit.py<br/>mounted-object access + denials"]
  end

  subgraph EP["HTTP surfaces"]
    HZ["/healthz /readyz"]
    MTX["/metrics (Prometheus)"]
    ADM["/_admin/stats /_admin/timeline /_admin/trace"]
    CFGA["/_config/api/audit"]
    MON["/_monitor dashboard"]
  end

  RT --> LOG & MET & TRACE & QS & AUD
  MET --> MTX
  TRACE & QS --> ADM
  AUD --> CFGA
  QS & MET --> MON
  RT --> HZ
```

Audit events (`identity, client, bucket, key, backend, method, status, bytes`)
are emitted for every mounted-object access and for auth/authz denials, secrets
scrubbed, with an in-memory ring surfaced at `/_config/api/audit`.
