# Lite and Enterprise End-to-End Workflows

This document traces the runtime from installation through startup, Fabric reads,
operations, failure handling, and shutdown. It also marks each ownership handover.
The diagrams reflect the current Python implementation in `main.py`,
`enterprise/manager.py`, and `enterprise/control/`.

## 1. Edition boundary

```mermaid
flowchart LR
  subgraph Shared["Shared data plane"]
    CFG["JSON + environment configuration"]
    AG["Agent: main.py"]
    AUTH["SigV4 + ACL middleware"]
    S3["S3 router"]
    META["Iceberg / Delta metadata"]
    GEN["SQL planner and Parquet generator"]
    MOUNT["Mounted-storage passthrough"]
    SRC[("Source database")]
    UP[("S3 / Azure / file share")]
    CFG --> AG --> AUTH --> S3
    S3 --> META --> GEN --> SRC
    S3 --> MOUNT --> UP
  end

  subgraph Lite["Lite ownership"]
    OP1["Operator / service manager"]
    OP1 -->|"starts, monitors, restarts"| AG
  end

  subgraph Ent["Enterprise ownership"]
    OP2["Operator"]
    MGR["Manager control plane"]
    SUP["AgentSupervisor"]
    REG["Registry + heartbeat leases"]
    STORE[("Shared artifact store")]
    OP2 -->|"starts one Manager"| MGR
    MGR --> SUP -->|"spawns and restarts"| AG
    AG -.->|"register + heartbeat"| REG
    REG --> MGR
    AG <--> STORE
  end
```

| Concern | Lite | Enterprise |
| --- | --- | --- |
| Install | `pip install -e .` | `pip install -e . -e ./enterprise` |
| Entrypoint | `python main.py` | `python -m enterprise.manager` or `Manager.ps1` / `Manager.sh` |
| Process owner | Operator, OS service, or container runtime | Manager `AgentSupervisor` |
| Fabric endpoint | Agent port, default `9000` | Agent ports `9000+`, built-in gateway, or external load balancer |
| Admin surface | Optional `/_config` and `/_monitor` on the Agent | `/_manager`, `/_config`, and aggregated `/_monitor` on Manager port `9200` |
| Materialization | One Agent owns all splits | Splits are assigned by shard and published to the shared store |
| Failure recovery | External process owner restarts the Agent | Supervisor restarts children; heartbeat registry removes dead targets from the gateway |
| Scale | One process | `AGENT_COUNT` agents, live scaling, optional Manager HA |

## 2. Lite lifecycle and ownership handover

```mermaid
flowchart TB
  A["Operator prepares configuration"] --> B["Install core package"]
  B --> C["Start python main.py"]
  C --> D["Uvicorn creates FastAPI Agent"]
  D --> E["lifespan startup"]
  E --> F["Validate effective configuration"]
  F --> G["Seed demo DB when SQLite"]
  G --> H["Reflect table schemas and split keys"]
  H --> I{"AUTO_REFRESH?"}

  I -->|"yes"| J["Materialize content-addressed snapshots"]
  J --> K["Publish snapshots and start freshness poller"]

  I -->|"no"| L["Build snapshot descriptors"]
  L --> M["Choose split counts and key ranges"]
  M --> N{"MATERIALIZE_MODE"}
  N -->|"eager"| O["Generate and pin every Parquet split"]
  N -->|"lazy / virtual"| P["Defer generation until metadata or data read"]
  O --> Q["Build Delta log when Delta is selected"]
  P --> Q

  K --> R["Agent readiness becomes true"]
  Q --> R
  R --> S["Fabric reads S3 endpoint :9000"]
  S --> T["Agent serves until stopped or failed"]
  T --> U["Lifespan shutdown"]
  U --> V["Stop AgentLink if configured"]
  V --> W["Stop GC and freshness tasks"]
  W --> X["Dispose database engines"]

  H -.->|"invalid config or source unavailable"| FAIL["Exit 78: permanent configuration error"]
  T -.->|"uncaught failure"| EXIT["Process exits"]
  FAIL --> OWNER["Ownership returns to operator / OS service"]
  EXIT --> OWNER
  OWNER -->|"correct config, then restart"| C
```

### Lite handover points

1. **Deployment to process:** the operator hands configuration and credentials to
   `main.py` through environment variables and JSON files.
2. **Process to runtime:** Uvicorn calls the FastAPI lifespan. The Agent validates
   configuration before accepting traffic.
3. **Runtime to serving:** readiness follows schema resolution and the selected
   materialization path. Fabric then owns the request cadence.
4. **Request to source:** the router turns metadata and split requests into
   parameterized source queries, or passes mounted-bucket reads to a storage backend.
5. **Failure to operator:** Lite has no in-process supervisor. Exit code `78`
   identifies a permanent configuration or source-connectivity fault; an external
   service owner decides when to restart.

## 3. Shared S3 request workflow

The same Agent request path is used in both editions. Enterprise changes how the
request reaches an Agent and who controls that Agent.

```mermaid
sequenceDiagram
  autonumber
  actor F as Fabric / S3 client
  participant MW as Auth + trace middleware
  participant RT as s3.router
  participant SS as Snapshot state
  participant CA as Cache / artifact store
  participant PL as Split planner
  participant DB as Source database
  participant PQ as Parquet generator
  participant BK as Mount backend

  F->>MW: GET /{bucket}/{key} with optional Range
  MW->>MW: Verify SigV4 and key ACL when required
  alt Authentication or authorization fails
    MW-->>F: 403 S3 AccessDenied XML
  else Request allowed
    MW->>RT: Dispatch bucket and key
    alt Mounted bucket
      RT->>BK: HEAD, list, or ranged stream
      BK-->>RT: Object metadata or bytes
      RT-->>F: 200 / 206 / 404
    else Warehouse metadata object
      RT->>SS: Resolve snapshot and object type
      SS->>CA: Read or build metadata / manifest / Delta log
      CA-->>RT: Stable metadata bytes
      RT-->>F: 200 metadata object
    else Warehouse Parquet split
      RT->>CA: Lookup authoritative split bytes
      alt Cache or shared-store hit
        CA-->>RT: Parquet bytes
      else Miss
        RT->>PL: Build parameterized range / modulo query
        PL->>DB: Execute SQL with bound values
        DB-->>PQ: Rows
        PQ->>CA: Encode and store Parquet bytes
        CA-->>RT: Parquet bytes
      end
      RT-->>F: 200 or 206 with exact declared size
    end
  end
```

## 4. Lite operational workflow

```mermaid
flowchart LR
  subgraph Configure
    C1["Edit local JSON or set environment"] --> C2["Validate DB URL, tables, auth, TLS"]
  end
  subgraph Run
    C2 --> R1["Start main.py"] --> R2["Check /healthz and /readyz"]
    R2 --> R3["Point Fabric shortcut to Agent"]
  end
  subgraph Operate
    R3 --> O1["Observe /metrics, /_monitor, logs"]
    O1 --> O2{"Change required?"}
    O2 -->|"data refresh"| O3["POST /_admin/refresh"]
    O2 -->|"configuration"| O4["Update config"]
    O4 --> O5["Stop Agent gracefully"]
    O5 --> R1
    O3 --> O1
  end
  subgraph Recover
    O1 -->|"process unavailable"| X1["External monitor detects failure"]
    X1 --> X2["Inspect exit code and logs"]
    X2 -->|"exit 78"| X3["Correct configuration or connectivity"]
    X2 -->|"transient crash"| X4["Restart Agent"]
    X3 --> X4 --> R2
  end
```

## 5. Enterprise bootstrap and control handover

```mermaid
sequenceDiagram
  autonumber
  actor O as Operator
  participant L as Manager.ps1 / Manager.sh
  participant M as Manager FastAPI :9200
  participant HA as Leader lease
  participant S as AgentSupervisor
  participant A as Agent main.py :9000+
  participant R as Registry
  participant ST as Shared artifact store

  O->>L: Start with AgentCount, ports, gateway, HA options
  L->>L: Prepare venv and install core + enterprise packages
  L->>M: python -m enterprise.manager
  M->>M: Hydrate encrypted DB credentials before importing config
  M->>M: Validate configuration and create supervisors
  alt Manager HA enabled
    M->>HA: Acquire or renew lease
    alt This Manager is primary
      HA-->>M: leader=true
    else This Manager is standby
      HA-->>M: leader=false
      M->>M: Stay ready without supervising Agents
    end
  end
  M->>S: Start one supervisor per AGENT_COUNT
  S->>A: Spawn child with Manager URL, ID, port, shard, store, mode
  A->>A: Run shared Agent startup and materialization
  A->>R: POST register
  R-->>A: Lease ID and heartbeat interval
  loop Every HEARTBEAT_MS
    A->>R: Health, served tables, snapshot epochs
    R-->>A: Queued commands
  end
  A->>ST: Publish owned splits / read non-owned splits
  M-->>O: /readyz reports live, failed, and crash-looped Agents
```

### Control handover contract

The Manager passes these values when it starts each Agent:

| Value | Handover purpose |
| --- | --- |
| `MANAGER_URL` | Enables registration, heartbeat, and command delivery |
| `AGENT_ID` | Stable fleet identity such as `agent-1` |
| `PORT` | Assigns the data-plane listener (`base + shard index`) |
| `AGENT_SHARD_INDEX`, `AGENT_SHARD_COUNT` | Divides cold materialization work |
| `SHARD_STRATEGY` | Keeps split ownership consistent across the fleet |
| `MATERIALIZE_MODE` | Keeps eager, lazy, or virtual behavior fleet-wide |
| `ARTIFACT_STORE_SERVING`, `ARTIFACT_STORE_DIR` | Makes generated objects available to every Agent |
| `ENABLE_MONITOR` | Exposes per-Agent data for Manager aggregation |

The Agent remains a complete S3 server. A Manager outage does not terminate it;
the heartbeat loop catches transport failures and retries. The Manager owns child
process restart only while it is the active supervisor.

## 6. Enterprise request routing

```mermaid
flowchart TB
  F["Fabric / S3 client"] --> E{"Configured endpoint"}
  E -->|"built-in gateway :9200"| GW["Manager Gateway"]
  E -->|"external load balancer"| LB["External L7 load balancer"]
  E -->|"direct Agent"| A0["Agent :9000+"]

  GW --> REG["Registry live-target view"]
  REG --> PICK["Round-robin ready Agent"]
  PICK --> A1["Agent 1"]
  PICK --> A2["Agent 2"]
  PICK --> AN["Agent N"]
  LB --> A1 & A2 & AN

  A0 --> AUTH["Shared SigV4 + ACL path"]
  A1 --> AUTH
  A2 --> AUTH
  AN --> AUTH
  AUTH --> ROUTE{"Warehouse or mounted bucket?"}
  ROUTE -->|"warehouse"| STORE[("Shared artifact store")]
  STORE -->|"miss owned by this shard"| DB[("Source database")]
  STORE -->|"hit or split built by peer"| RESP["Ranged S3 response"]
  DB --> GEN["Generate and publish Parquet"] --> STORE
  ROUTE -->|"mount"| UP[("Upstream storage")]
  UP --> RESP
  RESP --> F

  REG -.->|"no ready Agent"| UNAV["503 no_ready_agent"]
```

## 7. Enterprise failure, drain, and restart handover

```mermaid
sequenceDiagram
  autonumber
  actor O as Operator / admin console
  participant M as Manager
  participant R as Registry
  participant A as Active Agent
  participant G as Gateway / load balancer
  participant S as Supervisor
  participant N as Replacement Agent

  O->>M: Drain or restart Agent
  M->>R: Queue drain command
  A->>R: Next heartbeat
  R-->>A: drain
  A->>A: Set draining and return 503 from /readyz
  G->>A: Read readiness / stop new routing
  A->>A: Wait AGENT_DRAIN_GRACE_SECONDS
  A->>A: Request graceful Uvicorn shutdown
  A->>M: AgentLink stops during lifespan shutdown
  A-->>S: Process exits
  S->>S: Record restart and apply backoff
  alt Below crash-loop limit
    S->>N: Spawn replacement with same fleet contract
    N->>R: Register and receive a new lease
    N->>N: Warm from shared artifact store
    G->>N: Resume routing when live
  else Rapid restart limit reached
    S->>M: Mark crash_looped
    M-->>O: /readyz 503 and console alert
  end
```

### Failure decisions

```mermaid
flowchart TB
  D["Agent process exits"] --> CODE{"Exit code 78?"}
  CODE -->|"yes"| PERM["Permanent config / source fault"]
  PERM --> HOLD["Supervisor does not restart"]
  HOLD --> FIX["Operator fixes connection or config and restarts Manager / Agent"]

  CODE -->|"no"| RAPID["Record restart in rapid window"]
  RAPID --> LIMIT{"Restart limit reached?"}
  LIMIT -->|"no"| BACK["Wait restart backoff"] --> SPAWN["Spawn replacement"]
  LIMIT -->|"yes"| LOOP["Mark crash_looped; readiness fails"]

  HB["Heartbeats stop while process still exists"] --> DEAD["Registry marks Agent dead after miss limit"]
  DEAD --> REMOVE["Gateway excludes Agent from target list"]
  DEAD --> OPS["Operator or supervisor action"]
```

## 8. Enterprise live scaling handover

```mermaid
flowchart LR
  O["Operator requests target count"] --> API["Manager scale action"]
  API --> LOCK["Acquire fleet scale lock"]
  LOCK --> D{"Target versus current"}
  D -->|"larger"| ADD["Create supervisors for new indexes"]
  ADD --> ENV["Assign ports and shard contract"]
  ENV --> START["Start Agents and register leases"]
  D -->|"smaller"| DROP["Remove selected Agents from registry"]
  DROP --> STOP["Stop supervisors and child processes"]
  START --> SAVE["Persist agent_count"]
  STOP --> SAVE
  SAVE --> VIEW["Console and gateway use mutated supervisor list"]

  STBY["Standby Manager receives scale request"] --> PERSIST["Persist target only"]
  PERSIST --> NOTE["Primary must apply active fleet change"]
```

Scaling changes the process count immediately, but existing Agents keep the shard
count passed at their original spawn. Plan a coordinated restart when changing the
fleet size for workloads where every Agent must recalculate split ownership.

## 9. Manager HA ownership transfer

```mermaid
stateDiagram-v2
  [*] --> Standby: Start with MANAGER_HA=1
  Standby --> Primary: Acquire shared-store leader lease
  Primary --> Primary: Renew lease
  Primary --> Standby: Lease lost or renewal fails
  Primary --> ShuttingDown: Manager stop requested
  Standby --> ShuttingDown: Manager stop requested
  ShuttingDown --> [*]

  state Primary {
    [*] --> Supervising
    Supervising --> AgentsStopped: Leadership lost
  }

  state Standby {
    [*] --> NoLocalAgents
    NoLocalAgents --> LeaseElection: Retry acquire / renew
    LeaseElection --> NoLocalAgents: Another owner holds lease
  }
```

Only the primary starts supervisors. A standby reports ready as a warm spare. On
leadership loss, the former primary stops its Agents before returning to standby;
the new primary starts its own supervised fleet after acquiring the lease.

## 10. Responsibility handover matrix

| Stage | Lite owner | Enterprise owner | Completion signal |
| --- | --- | --- | --- |
| Package and config preparation | Operator | Operator / launcher | Dependencies installed; config files and environment available |
| Credential hydration | Agent configuration load | Manager before config import | Effective DB URLs available without logging secrets |
| Process creation | Operator / OS service | Manager supervisor | Agent PID exists |
| Schema and source validation | Agent | Each Agent | Startup continues; exit `78` on permanent fault |
| Split planning | Single Agent | All Agents with common shard contract | Snapshot descriptors and ranges exist |
| Split generation | Single Agent | Owning shard; peers consume shared bytes | Exact record count and file size stored |
| Client routing | Direct to Agent | Gateway, external LB, or direct Agent | A live Agent receives the request |
| Request authorization | Agent | Selected Agent | Identity attached or S3 `403` returned |
| Warehouse read | Agent | Selected Agent plus shared store | Stable metadata or exact Parquet range returned |
| Mount read | Agent | Selected Agent | Backend stream returned and audit event recorded |
| Health observation | External monitor | Manager registry, supervisor, and monitor proxy | Health/readiness and heartbeat age available |
| Drain | External service removes traffic, then stops Agent | Manager command rides heartbeat; Agent flips readiness | No new traffic; grace period expires |
| Crash recovery | External process owner | Supervisor with backoff and crash-loop guard | Replacement registers a new lease |
| Permanent config recovery | Operator | Operator; Manager remains available | Config fixed, then Agent or Manager restarted |
| Final shutdown | Operator stops Agent | Manager stops all supervisors, then Uvicorn | Background tasks stop and DB engines dispose |

## 11. Source modules

- `main.py`: Agent startup, materialization, middleware, readiness, drain, and shutdown.
- `s3/router.py`: S3 list, head, metadata, split, range, and mount routing.
- `enterprise/manager.py`: Manager process entrypoint and credential hydration.
- `enterprise/control/manager_app.py`: supervisor construction, HA, gateway, scaling,
  admin surfaces, and Manager lifespan.
- `enterprise/control/supervisor.py`: child spawn, restart, memory checks, exit `78`
  handling, backoff, and crash-loop guard.
- `enterprise/agent_link.py`: registration, heartbeat, stale-lease recovery, and drain.
- `enterprise/control/registry.py`: leases, liveness, epochs, and command queues.
- `enterprise/control/gateway.py`: ready-target selection and streamed reverse proxy.
- `runtime/artifact_store.py`: shared object contract used by enterprise Agents.
