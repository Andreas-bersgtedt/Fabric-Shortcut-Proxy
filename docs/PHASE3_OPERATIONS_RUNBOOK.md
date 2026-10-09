# Phase 3 operations runbook

This runbook assumes the Helm release uses the Manager role, remote
materializers register with stable site-prefixed Agent IDs, and the Manager
admin API is protected by an admin token. Keep tokens in an approved secret
store. Do not paste them into shell history, issue comments, or logs.

Before changing placement or draining Agents, confirm that the Manager is the
active leader, the artifact store is reachable from the target pool, and the
target pool has valid source credentials and network access. Check the Manager
fleet view and queue state before and after each operation.
Set `manager.adminUiEnabled: "1"` to mount the admin API. Admin requests also
require the configured operator authentication. The public Agent ingress does
not expose admin, fleet, work-queue, or data routes; run these requests through
a private connection or from the Manager Pod.

## Drain a site

The Manager drain endpoint is per Agent. It queues a drain command, marks that
Agent ineligible for new assignments, and delivers the command on its next
heartbeat. The Agent marks itself unready, cancels active materialization
tasks, and exits after its configured drain grace period. Treat cancelled work
as unfinished until the queue shows it reassigned or completed.

1. Confirm that another authorized pool can serve each connection and table
   used by the site. Configure and verify a fallback pool first if required.
2. Get the Agent names from `GET /_manager/api/fleet`. For a remote release,
   use its configured `siteId` prefix to select every Agent at that site.
3. Drain each Agent using the Manager admin API:

   ```powershell
   $operator = [Convert]::ToBase64String(
     [Text.Encoding]::UTF8.GetBytes(
       "$($env:MANAGER_AUTH_USERNAME):$($env:MANAGER_AUTH_PASSWORD)"
     )
   )
   $headers = @{
     "X-Admin-Token" = $env:FSP_ADMIN_TOKEN
     "Authorization" = "Basic $operator"
   }
   Invoke-RestMethod `
     -Method Post `
     -Uri "$env:FSP_MANAGER_URL/_manager/api/agents/$AgentName/drain" `
     -Headers $headers
   ```

   The response only confirms that the command was queued. It does not confirm
   that the Agent received it.
   This example uses operator Basic authentication. Use the configured
   operator Bearer credential instead on an OIDC-protected Manager.
4. Confirm the Agent becomes unready and stops receiving new assignments.
   Watch the Manager fleet and work-queue views until each unfinished claim is
   reassigned or completed.
5. Scale down or remove the site's Kubernetes workload only after the fleet
   shows no live serving Agent for that site and all affected work is accounted
   for. If work remains queued with `no_eligible_materializer`, stop and restore
   a compatible pool before proceeding.

To return the site, restore its workload, confirm healthy heartbeats and
readiness, and verify the Manager registers the expected site-prefixed IDs
before removing any temporary fallback.

## Move a dataset to another pool

Placement is configured by `manager.agentPlacementConfig` in the Helm values.
Use a table-specific rule for one table; an exact table rule replaces the
connection-level rule for that table. Use a connection-level rule only when
every table on that connection should move.

1. Confirm the destination pool has the correct `allowed_connection_ids` and
   table patterns, source credentials, source network route, artifact-store
   route, and authenticated identity bindings. In Entra mode, confirm the
   managed identity has the API's `FSP.Agent` application role and the Manager
   binds its client and principal IDs to the stable Agent ID. Static modes
   require valid pool-specific tokens unique across pools.
2. In the release values, change the target table's policy, for example:

   ```yaml
   manager:
     agentPlacementConfig:
       tables:
         erp-a::sales.orders:
           required_pool: erp-site-b
           required_location: site-b
   ```

   Preserve the other existing `pools`, `connections`, and `tables` entries.
   Do not replace the complete placement object with this excerpt.
3. Validate and apply the normal Helm release update:

   ```powershell
   helm upgrade --install $env:FSP_RELEASE `
     deploy/helm/fabric-shortcut-proxy `
     --namespace $env:FSP_NAMESPACE `
     --values $env:FSP_VALUES_FILE `
     --dry-run=server

   helm upgrade --install $env:FSP_RELEASE `
     deploy/helm/fabric-shortcut-proxy `
     --namespace $env:FSP_NAMESPACE `
     --values $env:FSP_VALUES_FILE
   ```

4. Confirm the Manager becomes ready after rollout. Submit or observe a
   controlled request for the table and verify its placement audit decision
   names the destination pool and location. Confirm source reads succeed and
   the snapshot is published before declaring the move complete.
   Use fresh controlled work, not a previously cached snapshot. For an
   isolated acceptance fixture, initialize a generation using the generation
   coordinator API only after confirming the isolated queue has no unfinished
   tasks. Acquiring a generation fences its predecessor; never do this to a
   live demo or production generation just to run a test. Verify the durable
   manifest's request ID, generation ID, split owners, row counts, metadata,
   and store-backed split integrity, not just request status.
5. If placement is queued as `no_eligible_materializer`, source access fails,
   or the result is incorrect, restore the previous values and run the same
   validation and upgrade steps. Keep both pools authorized until active work
   from the prior placement is accounted for.

## Enable a fallback pool

Fallback pools are configured on a connection policy. A table-specific policy
overrides that connection policy, so check for an exact table entry before
changing the connection rule.

1. Confirm the fallback pool has authenticated identities, source access
   for the connection and tables, artifact-store access, and enough capacity.
2. Add its `pool_id` to the connection's `fallback_pools` list in
   `manager.agentPlacementConfig`. Keep `required_pool` set to the primary.
   Add the pool to the exact table policy instead if that table has its own
   policy.
3. Run the Helm server-side dry run and apply the release update as described
   in **Move a dataset to another pool**.
4. Verify a controlled test request uses the primary while it is eligible.
   Then, in the approved non-production window, make the primary unavailable
   and verify placement selects only the configured fallback. Confirm queue
   state, audit decision, source access, and successful publication.
5. Restore the primary and verify that new work returns to it. Remove the
   fallback only after the primary is healthy and no work remains assigned to
   the fallback.

## Validation record

Attach the release and chart revision, environment, change window, sanitized
values diff, fleet and queue observations, placement audit events, and
recovery result to the change record. Do not include credentials, access
tokens, or source data.

The local scenario matrix is in
[PHASE3_SCENARIO_MATRIX.md](./PHASE3_SCENARIO_MATRIX.md). Local tests do not
fulfill the required non-production runbook exercise.

## Non-production exercise record

The isolated `fsp-phase3` namespaces in `aks-fspdemo-xdev-swc` and
`aks-fspdemo-xdev-neu` passed these procedures on 2026-10-09. The original demo
release was not upgraded. Sweden Central release `fsp`, revision 4, used
candidate digest
`sha256:3c618c9066c1370664ed6ce9c798466329ff2421d04cb3d1bdcb8c8fc3c42eaf`.
These exercises used static pool credentials; Entra rollout and renewal are
separate acceptance checks, not implied by this record.

| Exercise | Verified result |
| --- | --- |
| Dataset move | Generation `00000000000000000002-f62b4be5887dd4d2` published eight successful Customer splits, all owned by pool `phase3-b` in North Europe. |
| Primary preference | Generation `00000000000000000007-cc777972ea673fb7` published one split on `phase3-a-fsp-materializer-0:9000`, with 847 records and one metadata object. |
| Drain | Authenticated request returned HTTP 200; heartbeat delivered drain, fleet marked the primary draining, readiness returned HTTP 503 `draining`, and zero unfinished claims remained before scale-down. |
| Fallback | With the primary Pod deleted, generation `00000000000000000008-16e1754fa38a8302` published one split on `phase3-b-fsp-materializer-0:9000`, with 847 records and one metadata object. |
| Recovery | After primary readiness returned, generation `00000000000000000009-6ca7d279ddaca439` published one split on the primary, with 847 records and one metadata object. |

The acceptance fixture used `Sales_DW` / `SalesLT.Customer` and one split to
make ownership measurable. Earlier eight-split runs with both pools eligible
had mixed ownership; this record does not establish strict primary preference
for every capacity or heartbeat condition.

Both acceptance default readiness connections were set to the allowed source,
not unrelated SQL blocked by policy. The fallback identity received SELECT
only on `SalesLT.Customer`. The colocated primary required
`materializer.enabled: true` in the Manager profile to retain its control-plane
NetworkPolicy allowance. Both sites mounted only the `phase3-20261009`
artifact/config subdirectories. No DNS change was needed.

### Entra deployment and repeated procedures

The same isolated estate was upgraded to Entra Agent authentication on
2026-10-09. Sweden Central Helm release `fsp` is revision 5; North Europe
release `fsp` is revision 1. Both use image digest
`sha256:b4e918d7a89c02dc4657daf6d602c15a29acd3f0fd28a8aa61650c4db4195a83`.
The dedicated single-tenant API has no application password credentials and
requires the `FSP.Agent` application role. Only the two acceptance workload
identities have that role. Both materializer Pods have no static Agent token
or identity-token environment variables.

| Check | Observed result |
| --- | --- |
| Bound workload identity, both sites | Snapshot request HTTP 200. |
| Wrong pool identity, both sites | HTTP 401. |
| Static token, missing Agent identity, and operator Basic credentials | Each request HTTP 401 at both sites. |
| Public unauthenticated Agent registration | HTTP 401. |
| Public fleet, admin, work-queue, and metrics paths | Each path HTTP 404. |
| Allowed egress, both sites | Source SQL, Key Vault, DNS, and isolated artifact read/write succeeded. |
| Denied egress, both sites | Arbitrary internet and unrelated SQL were blocked. |

The primary, drain, fallback, and recovery procedures were repeated using
Entra credentials. Each successful generation published one split containing
847 records and 92,314 bytes, with one metadata object. Verification checked
the durable manifest, request/generation IDs, split ownership, and store-backed
integrity without printing source rows.

| Stage | Generation | Split owner |
| --- | --- | --- |
| Healthy primary | `00000000000000000011-39a2ee99a910541d` | `phase3-a-fsp-materializer-0:9000` |
| Primary unavailable | `00000000000000000012-62c7b3f84e7acbd7` | `phase3-b-fsp-materializer-0:9000` |
| Primary restored | `00000000000000000013-ed7ed27ab8c2ab9b` | `phase3-a-fsp-materializer-0:9000` |

Drain returned HTTP 200, was delivered on a heartbeat, marked the primary
draining in the fleet, and changed readiness to HTTP 503 `draining`. Zero
unfinished primary claims remained before scale-down. The primary was restored
to one ready replica after fallback publication. This is a primary Pod-loss
exercise, not a whole-region outage or WAN partition.

When sharing an operator-authenticated HTTPX client for a test fixture, set
`auth=None` on Agent requests with an explicit Bearer header; client-level
Basic authentication otherwise replaces that header. North Europe's adoption
of manifest-managed resources retained a retired `AGENT_TOKEN` environment
entry. That entry was explicitly removed and the rollout verified. Unused
acceptance Secrets are retained for rollback; no running Entra workload
references them. The original demo release and its credentials are unchanged.

### Credential renewal observation

The North Europe Pod passed its normal projected workload credential rotation
without a Pod restart. Over 41 minutes 31 seconds, the monitor completed 156
authenticated control/readiness checks with zero failures. After rotation and
the SDK's file-cache interval, a fresh Azure Identity exchange using the
rotated federation file was accepted by Manager with HTTP 200.

| Sanitized monitor field | Value |
| --- | --- |
| State | `passed` |
| Observation start / completion, Unix seconds | `1791563215` / `1791565706` |
| Initial federation issued / expires, Unix seconds | `1791562761` / `1791566361` |
| Rotated federation issued / expires, Unix seconds | `1791565703` / `1791569303` |
| Successful / failed checks | `156` / `0` |
| Fresh exchange accepted | HTTP `200` |
| Natural access-token expiry observed | `false` |

Observed managed-identity access tokens have approximately 24-hour lifetimes;
the Kubernetes federation token has a one-hour lifetime. A claims-triggered
SDK exchange proves the rotated federation credential is usable, not natural
expiry of the existing access token. The SDK expiry/file-reload unit test
separately uses controlled time and a mocked AAD client boundary.

### Final committed candidate

After the rotation monitor completed, both sites were upgraded to the clean
committed runtime revision `8d0d911a18a5237d755c0073a9461d8ee0311e10`.
There is no runtime overlay in this build. Its image digest is
`sha256:39b2b01dbf11d99513ca1acffba3aa8696185302e5e7c1078755f12d0d91bc31`.
The only runtime difference from the rotation candidate is the explicit
PyJWT algorithm import used for type checking.

Both final pinned profiles passed Helm lint and server-side admission.
Sweden Central release `fsp` revision 6 and North Europe revision 2 rolled
out successfully. Both sites again passed the HTTP 200/401 authentication
matrix, absence of static Agent environment credentials, allowed source/vault
access, DNS and artifact read/write, and blocked arbitrary internet/unrelated
SQL checks.

Fresh generation `00000000000000000014-cf1dc4c275c946f2`, request
`8cd9f179078a47f4fad536d5105fb745c333b327e128eb60e988ede8dd38b851`,
published one successful primary-owned split with 847 records, 92,314 bytes,
and one metadata object. Store-backed integrity and durable request/generation
IDs were verified. The acceptance primary and fallback remain running for
review; no DNS or original demo workload change was made.

The focused local Phase 3 regression passed 149 tests. All 11 CI checks passed
at the committed runtime revision, including Lite and Enterprise on Python
3.11/3.12, chart validation, C++ parity/SigV4, and Kind integration. Windows
process-startup tests had intermittent 10-second timeouts in earlier runs;
they passed in the focused rerun and both Linux Enterprise CI jobs. Their
deadlines and unrelated HA implementation were not changed.
