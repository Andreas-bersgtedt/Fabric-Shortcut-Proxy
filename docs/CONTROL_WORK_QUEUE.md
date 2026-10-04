# Manager materialization work-queue contract

Issue: [#90](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/90)

Status: Contract version 1.1 and durable queue execution are implemented.

## Purpose

The work queue coordinates source split materialization between the Manager and Python
materializer Agents.

It replaces two incomplete paths:

- process-local heartbeat commands that disappear after delivery;
- direct Manager-side source queries inside `POST /control/materialize`.

The queue persists requests, tasks, claims, results, and published snapshot
manifests in the shared artifact store.

## Compatibility

Current contract version: `1.1`

Minimum compatible version: `1.0`

Compatibility rules:

- a 1.1 Manager accepts 1.0 and 1.1 Agent registrations;
- missing additive fields receive 1.0 defaults;
- a different major version is rejected during registration;
- a future minor version above the Manager version is rejected until reviewed;
- existing register and heartbeat fields keep their tags and meanings.

Agent authentication is a REST transport concern and does not change the control
contract or protobuf messages. During the migration release,
`AGENT_AUTH_MODE=compatibility` is the runtime default so existing Agents can continue
using Manager Basic credentials while deployments are upgraded. The installer and
deployment migration phase will set `AGENT_AUTH_MODE=required` for new installations.

Agent requests use `X-FSP-Agent-Token` and `X-FSP-Agent-ID`. The active token comes from
`AGENT_TOKEN`. A different `AGENT_TOKEN_PREVIOUS` is accepted only until the positive
Unix UTC deadline in `AGENT_TOKEN_PREVIOUS_VALID_UNTIL`. Token values are secrets: they
must come from the environment or a supported secret store and are not fields in
`config.system.json`.

The Agent route set is:

- `POST /control/register`;
- `POST /control/heartbeat`;
- `GET /control/assignment/{agent_id}`;
- `GET /control/snapshot/{table}`;
- `POST /control/task-result`;
- `POST /control/materialize`.

The operator work-queue routes remain separate:

- `GET /control/work-queue`;
- `POST /control/work-queue/requests/{request_id}/cancel`;
- `POST /control/work-queue/tasks/{task_id}/retry`.

Authentication failures return only `agent authentication required` (401), or
`agent authentication unavailable` (503) when required authentication is not
configured. Responses never identify whether a token was missing, invalid, or expired.

The Manager binds `X-FSP-Agent-ID` to the registration, heartbeat, assignment, and
task-result identity before invoking the control service. Token-authenticated snapshot
and lazy materialization calls also require a non-empty identity. Legacy Basic clients
may omit the header only while `compatibility` mode is enabled. Failed
authentication therefore cannot register an Agent, renew a lease or claim, publish a
result, or start materialization.

`compatibility` is limited to the migration release. It accepts valid Manager Basic
credentials on Agent routes for old clients, but does not accept operator sessions or
Entra bearer credentials. `required` accepts only Agent tokens. Agent tokens never grant
access to work-queue or other operator routes.

At startup, Agent tokens resolve in this order:

1. an explicit environment variable;
2. Azure Key Vault (`agent-token` and `agent-token-previous`);
3. the encrypted last-known-good credential cache.

Python Agents read `AGENT_TOKEN` before each request, so an environment refresh applies
immediately. C++ Agents read it at process startup and must be rolled during the overlap.
To rotate, move the old active value to `AGENT_TOKEN_PREVIOUS`, set its absolute Unix UTC
deadline, install a new active value, roll Agents, verify the new value, wait for the
deadline, and then clear the previous value. The deadline is never extended implicitly.

Agent-authentication audit records contain only the bounded Agent identity, request ID,
method, route, status, outcome, and safe reason code. They never contain either token.

Python materializer Agents advertise:

```json
{"capabilities": ["materializer"], "shard_index": 0}
```

C++ serving Agents advertise:

```json
{"capabilities": ["serving"]}
```

The scheduler must not assign source-query work to an Agent without `materializer`.
For an owner-only SQL Server snapshot, it also restricts the task to the Agent whose
`shard_index` owns the transaction. PostgreSQL exported snapshots remain eligible on every
Agent that joined the generation.

## Task states

```text
QUEUED
CLAIMED
SUCCEEDED
RETRY_WAIT
FAILED
EXPIRED
CANCELLED
```

Allowed transitions:

```text
QUEUED -> CLAIMED
QUEUED -> FAILED
QUEUED -> CANCELLED

CLAIMED -> SUCCEEDED
CLAIMED -> RETRY_WAIT
CLAIMED -> FAILED
CLAIMED -> EXPIRED
CLAIMED -> CANCELLED

RETRY_WAIT -> QUEUED
RETRY_WAIT -> FAILED
RETRY_WAIT -> CANCELLED

EXPIRED -> QUEUED
EXPIRED -> FAILED
EXPIRED -> CANCELLED
```

Terminal states:

- `SUCCEEDED`;
- `FAILED`;
- `CANCELLED`.

Any transition not listed above is invalid.

## Ownership

A task claim binds work to:

- task ID;
- request ID;
- Agent ID;
- claim token;
- attempt number;
- claim expiry;
- generation ID;
- generation fence;
- membership version;
- worker fence;
- plan SHA-256.

Only the claiming Agent may report a result.

The claim token is carried in `MaterializeTask` and returned in `TaskResult`. It must not
appear in logs, monitor responses, audit details, or error messages.

## MaterializeTask 1.1 fields

Existing fields remain:

- table;
- epoch;
- split index;
- source table;
- output key;
- schema;
- key range.

Added fields:

- task ID;
- request ID;
- claim token;
- attempt;
- claim expiry;
- connection ID;
- connection fingerprint;
- generation ID;
- generation fence;
- plan SHA-256;
- table format;
- request deadline.
- split count;
- key column;
- split strategy.

Schema columns can carry:

- source column name;
- transform object;
- central policy ID.

## TaskResult 1.1 fields

Existing fields remain:

- Agent ID;
- table;
- epoch;
- split index;
- success flag;
- size;
- row count;
- content hash;
- error text.

Added fields:

- task ID;
- request ID;
- claim token;
- attempt;
- generation ID;
- generation fence;
- plan SHA-256;
- retryable flag;
- completion time;
- safe error code.

The Manager must verify stored output bytes before accepting a successful result.

## Result acknowledgement

`Ack` includes:

- accepted flag;
- resulting state;
- duplicate flag;
- terminal flag;
- safe reason code.

Expected reason codes include:

- `accepted`;
- `duplicate`;
- `stale_claim`;
- `stale_generation`;
- `wrong_owner`;
- `invalid_output`;
- `conflict`;
- `rejected`.

A duplicate result is accepted only when it matches the accepted task attempt and output
metadata.

## Snapshot manifest 1.1 fields

Existing fields remain:

- table;
- epoch;
- table format;
- split references;
- metadata keys.

Added fields:

- generation ID;
- generation fence;
- plan SHA-256;
- publication time;
- request ID.

`get_snapshot(table, epoch=0)` returns the latest verified publication. It does not
return queued, claimed, retrying, failed, or partially published work.

## Security rules

- validate output keys with artifact-store path rules;
- verify table, generation, fence, and plan digest before dispatch;
- bind results to Agent ID and claim token;
- compare claim tokens in constant time;
- verify output size, SHA-256, and Parquet row count;
- reject serving-only Agents for materialization work;
- cap and redact error text;
- keep source URLs and credentials out of queue records;
- keep claim tokens and Agent lease IDs out of observability payloads.

## Runtime behavior

The active Manager writes queue records under `_control/work-queue/v1` in the shared
artifact store. It claims work before adding a command to an Agent heartbeat. If command
delivery fails or the claim expires, the task returns to the runnable queue.
Version 1.1 heartbeats list active task IDs. The Manager renews only those claims, so a
command lost before Agent acknowledgement expires instead of remaining claimed.

`GENERATION_MEMBERSHIP_POLICY=fixed` preserves the first live materializer
identities recorded for a generation. `elastic` reconciles live,
non-draining materializers on every scheduler scan. A join increments the
membership version without revoking valid in-flight work. Heartbeat expiry,
drain, or re-registration increments the affected worker fence and returns only
its unfinished claims to the runnable queue. A late result must match the claim
token, Agent lease hash, membership version and worker fence.

Capacity hints set each worker's concurrent claim allowance. The scheduler
compares active claims divided by capacity, then uses the existing deterministic
task/worker tie-break.

Python Agents execute materialization commands. C++ Agents retain the 1.0 serving
contract and call `POST /control/materialize` on a store miss. That endpoint resolves the
table, creates or reuses one deterministic request, and waits for durable publication.
The Manager does not run the source query on the queued path.

Successful reports are accepted only after the Manager reads the stored Parquet object
and verifies its size, SHA-256 digest, and row count. Metadata is published after every
split succeeds. `get_snapshot` rechecks published objects before returning the manifest.

## Recovery and operations

`GET /control/work-queue` reports task counts, queue age, scheduler state, the
active Manager fence, membership policy and version, active worker load,
unassigned work, completed and total tasks, progress, and reassignment count.
It does not return claim tokens, Agent lease IDs, or lease hashes.

Operator endpoints:

- `POST /control/work-queue/requests/{request_id}/cancel`
- `POST /control/work-queue/tasks/{task_id}/retry`

The retry endpoint accepts failed tasks that do not belong to a published request. A
cancel or retry attempt writes a `work_queue_operation` audit event with the operator
identity, request ID, operation, target ID, status, and outcome. Task payloads, claim
tokens, and lease IDs are excluded.

A leadership takeover releases claims from the prior Manager term before dispatch starts.
Tasks for a replaced generation are cancelled. Startup also requeues expired claims and
prunes terminal queue records older than `WORK_QUEUE_RETENTION_SECONDS`. Published
snapshot manifests and artifact objects are retained.

`WORK_QUEUE_REQUEST_TIMEOUT_SECONDS` sets the Manager request deadline and defaults to 600
seconds. The Helm C++ Agent uses a matching `MATERIALIZE_TIMEOUT_MS=600000` default so an
owner-only SQL Server snapshot can process its splits sequentially.

The Helm chart sets `REQUIRE_GENERATION=0` on C++ Agents in lazy mode. Queue outputs are
published in the shared artifact root; eager serving images continue to use generation
activation with `REQUIRE_GENERATION=1`.

Queue counters are included in the Manager `/metrics` response under
`materialization_queue_requests_total`, `materialization_queue_events_total`, and
`materialization_queue_results_total`.

## Rollback setting

`MATERIALIZATION_WORK_QUEUE=1` is the default. Set it to `0` and restart the Manager to
stop the scheduler and restore the direct compatibility path for
`POST /control/materialize`. Existing queue records and published artifacts are left in
the shared store for diagnosis.
