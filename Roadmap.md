# Fabric Shortcut Proxy Roadmap

Date: 2026-07-26
Status: Active implementation

## Purpose
This roadmap defines the next major capability upgrades for the proxy:
1. Expand supported SQL server flavors (Oracle, Databricks SQL Warehouse, and future additions).
2. Improve object coverage and path clarity (tables + views, explicit schema-aware virtual folders).
3. Add production-grade transport security (HTTPS/TLS).
4. Redesign split planning for scale (target rows per split, date ranges, and non-PK strategies).

The plan is phased to minimize regression risk and preserve current known-good behavior.

## Design Principles
- Backward compatibility first: existing deployments keep working by default.
- Explicit feature flags for all major behavior changes.
- Deterministic outputs where possible (stable object keys, stable split predicates).
- Safe fallbacks instead of hard failures when optional capabilities are unavailable.
- Test coverage per phase: unit, integration, and compatibility checks.

## Current Baseline (Summary)
- SQL dialect support: SQLite, PostgreSQL, SQL Server.
- Views are discoverable in config builder, but split-key strategy for views needs stronger guidance/fallbacks.
- Virtual object keys are table-name centric and do not include server/database/schema hierarchy.
- Range splitting exists but is key-span based, not row-target based.
- Runtime is HTTP-only today.

## Roadmap Overview

## Phase 1: Canonical Source Path Model
Goal: represent source identity clearly and map virtual folders to source lineage.

Status: Completed (2026-07-26)

### Scope
- Introduce canonical source identity fields for each exposed object:
  - server_name
  - database_name
  - schema_name
  - object_name
  - object_kind (table/view)
- Add path template support for virtual object keys.
- Implement new default path layout:
  - /<Bucket>/<ServerName>/<Database>/<Schema>/<Object>
- Keep legacy path behavior behind a compatibility flag.

### Deliverables
- Config model updates for canonical source identity.
- Config Builder updates to capture/display source identity.
- Object key generation update in snapshot/state logic.
- Migration notes for existing shortcuts.

### Acceptance Criteria
- New shortcuts can be created with schema-visible paths.
- Existing legacy path deployments continue to function unchanged when compatibility mode is on.
- List/GET/HEAD behavior remains compatible for both layouts.

Completion Notes:
- Default path layout is canonical.
- Legacy aliases are disabled by default for immediate cutover.
- Delta `_delta_log` and data objects now align under canonical table paths.

### Risks
- Path migration can break existing shortcuts if forced by default.
- Case sensitivity differences across engines may create duplicate-looking paths.

### Mitigation
- Compatibility mode enabled by default for upgrades.
- Explicit path normalization policy documented and tested.

---

## Phase 2: Split Planner v2 (Target Rows + Multi-Strategy)
Goal: produce scalable split plans aligned with table shape and available columns.

### Scope
- Add row-target-based planning:
  - split_target_rows (default: 100000)
  - split_count = ceil(estimated_rows / split_target_rows), with min/max guardrails
- Add strategy cascade:
  1. Integer range split (preferred when usable)
  2. Date/timestamp range split
  3. Non-PK sortable column strategy (quantile/ntile-style boundaries)
  4. Modulo/hash fallback
- Add explicit support for non-PK split columns.
- Improve handling for views (which often have no PK).

### Deliverables
- Planner capability matrix by source flavor.
- Column profiling endpoint for key selection recommendations.
- Config options for preferred split column, strategy preference, and fallback policy.
- Extended tests for skewed distributions and sparse keyspaces.

### Acceptance Criteria
- Planner can generate range plans targeting approximately 100000 rows per split.
- Date-range strategy works for tables/views without numeric IDs.
- Non-PK split-column strategy works when PK is unsuitable or absent.
- Fallback path remains deterministic and safe.

### Risks
- Cardinality estimates can be inaccurate on some engines.
- Range boundaries can skew under highly non-uniform distributions.

### Mitigation
- Rebalance option for future iteration.
- Telemetry and explain output to inspect computed boundaries.

---

## Phase 3: SQL Flavor Expansion Framework
Goal: add Oracle and Databricks SQL Warehouse safely and make future flavor additions low-friction.

### Scope
- Introduce source-engine adapter interfaces for:
  - URL/connection construction
  - reflection/introspection
  - quoting and SQL syntax differences
  - limit/pagination behavior
  - optional capability detection
- Support both async-native and sync-in-threadpool execution paths.
- Add Oracle SQL support.
- Add Databricks SQL Warehouse support.

### Deliverables
- Adapter framework and capability registry.
- Oracle adapter + tests.
- Databricks adapter + tests.
- Config Builder dialect list update and per-dialect help text.

### Acceptance Criteria
- Users can connect, inspect, and query Oracle sources.
- Users can connect, inspect, and query Databricks SQL Warehouse sources.
- Split planner strategies execute with documented fallbacks per flavor.
- Existing SQLite/Postgres/SQL Server behavior remains stable.

### Risks
- Driver ecosystem differences (auth, ODBC/JDBC/HTTP transport, async support).
- Metadata permissions vary by enterprise setup.

### Mitigation
- Capability-based feature gating.
- Clear diagnostics for missing permissions/unsupported operations.

---

## Phase 4: HTTPS/TLS and Certificate Operations
Goal: eliminate plain HTTP exposure for production and standardize certificate lifecycle.

### Scope
- Add production deployment profile with TLS termination at reverse proxy.
- Document two certificate tracks:
  - Public DNS: Let's Encrypt automated issuance/renewal.
  - Private/internal: enterprise PKI or OpenSSL-based internal CA.
- Optional direct app TLS flags for local/dev use.

### Deliverables
- Reference deployment docs for HTTPS fronting.
- Certificate rotation and renewal runbook.
- Security hardening checklist for control plane and data plane.

### Acceptance Criteria
- Proxy is reachable via HTTPS with valid cert chain in production profile.
- Renewal process is automated (Let's Encrypt) or operationally documented (internal PKI).
- HTTP-to-HTTPS redirect policy defined for production.

### Risks
- Certificate misconfiguration can block Fabric connectivity.
- Internal CA trust distribution can be operationally complex.

### Mitigation
- Preflight TLS validation steps.
- Staging environment validation before production cutover.

---

## Cross-Cutting Work
- Observability:
  - Add planner decision telemetry (selected strategy, estimated rows, boundaries).
  - Add dialect capability telemetry for connection and reflection stages.
- Testing:
  - Keep compatibility tests for legacy behavior.
  - Add matrix tests by flavor and split strategy.
- Documentation:
  - Update configuration manual and examples per phase.
  - Provide migration guides for path layout and TLS adoption.

## Proposed Milestone Sequence
- Milestone A: Phase 1 complete (path model + compatibility mode) ✅
- Milestone B: Phase 2 complete (split planner v2 with row-target and date/non-PK support)
- Milestone C: Phase 3 complete (adapter framework + Oracle + Databricks)
- Milestone D: Phase 4 complete (HTTPS deployment profile + cert lifecycle docs)

## Definition of Done (Program Level)
- All phase acceptance criteria pass.
- Existing default deployment path remains non-breaking unless explicit migration is selected.
- Updated docs are sufficient for operator setup without code inspection.
- Test suite includes regression coverage for current supported flavors and new features.

## Out of Scope (This Roadmap Cycle)
- Full CDC-based incremental materialization redesign.
- Complete dynamic split rebalancing in live refresh loops.
- Multi-cloud abstraction beyond current S3-compatible contract.
- Control-plane mTLS enforcement between Manager and Agents (deferred to next cycle after edge TLS is stabilized).

## Open Decisions
- Exact canonicalization policy for server/database/schema casing.
- Default strategy order between date-range and non-PK quantile for specific flavors.
- Whether direct in-process TLS should be production-supported or dev-only.
- Initial min/max bounds for split_count guardrails.

## Locked Decisions (2026-07-26)
- Canonical path rollout defaults immediately for new implementations; migration safety is provided via legacy compatibility serving during transition.
- Numeric split-quality SLOs are deferred to per-phase design docs (not fixed in this roadmap).
- Control-plane mTLS is deferred for now; this cycle delivers HTTPS/TLS edge hardening first.

## Immediate Next Step
Execute Milestone B (Phase 2): implement split-planner strategy cascade (integer/date/non-PK), add strategy telemetry, and complete coverage for fallback determinism.