# Fabric Shortcut Proxy Helm chart

This chart packages the FSP Manager, Python materializers, C++ serving Agents,
storage, network policy, nginx proxy, and optional cert-manager ingress as one
Helm release. Chart and application version `3.0.1` represent the stable
baseline used for the enterprise demo migration.

## Prerequisites

- Kubernetes 1.25 or later
- Helm 3.18.6 or later
- Existing `fsp-source` and optional `fsp-manager-auth` Secrets; static Agent
  modes also require `fsp-agent-auth`
- cert-manager and ingress-nginx when `tls.enabled` is true
- Azure Files CSI support when `storage.azureFiles.enabled` is true

The chart never creates credential-bearing Secrets. Create source, Agent-auth,
Manager operator, and configured image pull Secrets outside Helm before
installing the release. `fsp-agent-auth` must contain `AGENT_TOKEN`. During
rotation it may also contain `AGENT_TOKEN_PREVIOUS` and
`AGENT_TOKEN_PREVIOUS_VALID_UNTIL`; templates expose those two keys to Manager
only. Keep Basic or Entra credentials in `fsp-manager-auth`, which Agents do not
reference.
The Namespace, persistent volumes, and persistent claims carry Helm's `keep`
policy so uninstalling the release does not remove retained state.

## Windows PowerShell

Install the pinned Helm version:

```powershell
winget install --id Helm.Helm --exact --version 3.18.6
helm version --short
```

Copy the sanitized example to the ignored local values path and replace the
example identifiers:

```powershell
Copy-Item deploy/helm/fabric-shortcut-proxy/values-enterprise-demo.example.yaml `
  deploy/helm/fabric-shortcut-proxy/values-enterprise-demo.local.yaml
```

Validate without cluster access:

```powershell
helm lint deploy/helm/fabric-shortcut-proxy `
  -f deploy/helm/fabric-shortcut-proxy/values-enterprise-demo.local.yaml
helm template fsp deploy/helm/fabric-shortcut-proxy `
  -f deploy/helm/fabric-shortcut-proxy/values-enterprise-demo.local.yaml `
  --namespace fabric-shortcut-proxy
```

For the private enterprise demo AKS cluster, use
`infra/fsp-demo/Deploy-FspDemo.ps1`. It packages this chart and runs
`helm upgrade --install` through `az aks command invoke`, so no workstation
route to the private API server is required.

Do not run `helm install` directly against the enterprise demo from a workstation with stale
values. The deployment script validates Azure account, cluster state, DNS, network RBAC, chart
schema, remote Helm ownership support, and the existing source Secret before mutation.

## Values

- `images`: immutable application image digests and optional pull Secret names.
- `fsp`: shared runtime configuration. Set `fsp.s3Bucket` explicitly for each environment;
  Helm publishes it as `S3_BUCKET`, which overrides the Config UI's persisted `bucket` value.
  Keep both values identical. Credentials do not belong here. For the Azure artifact backend,
  set `fsp.artifactStoreBackend: azure`, `fsp.artifactStoreAccountUrl` to the Blob account
  endpoint, and `fsp.artifactStoreContainer` to a pre-created container. Set the auth mode to
  `managed_identity` (default) or `workload_identity`; workload identity also needs
  `fsp.artifactStoreClientId` and `fsp.artifactStoreTenantId`. Grant the Manager and every
  materializer identity **Storage Blob Data Contributor** on the container or its storage
  account. The chart does not configure storage keys or create the container.
- `workloadIdentity`: Azure workload identity service account configuration.
- `agentAuth`: Entra, required, or one-release compatibility mode plus Secret key
  references. The default is `required`; values never contain token material.
  `identityTokensSecretName` optionally references a Secret containing the
  JSON `AGENT_IDENTITY_TOKENS` map for authenticated pool identities.

## Entra Agent authentication

For Python control clients on AKS, install the image with the `agent-entra`
extra and set `agentAuth.mode: entra`. This mode rejects static Agent tokens
and operator Basic credentials on Agent routes. It does not change operator
authentication on the private admin API.

Register a single-tenant API application with identifier URI `api://<client-id>`,
`api.requestedAccessTokenVersion: 2`, and an enabled application-only app role
whose value is `FSP.Agent`. Require app-role assignments on its service
principal. Assign that role only to the authorized workload managed identities
using Microsoft Graph app-role assignments, not Azure RBAC assignments.
Do not create an application secret. See Microsoft's
[managed identity app-role instructions](https://learn.microsoft.com/entra/identity/managed-identities-azure-resources/assign-app-role-managed-identity-powershell).

```yaml
agentAuth:
  mode: entra
  entra:
    tenantId: "<tenant UUID>"
    audience: "<API application client UUID>"
    identities:
      "site-a-fsp-materializer-0:9000":
        client_id: "<workload managed-identity client UUID>"
        principal_id: "<workload managed-identity principal UUID>"
workloadIdentity:
  enabled: true
  clientId: "<workload managed-identity client UUID>"
cppAgent:
  enabled: false
```

The Manager needs bindings for every placement identity and every authorized
Python serving client. Client and principal IDs cannot be reused across
materializer pools. Remote materializer releases need the same tenant and
audience, their own workload identity, and their federation subject. The
identity map is Manager configuration, not a credential.

The chart omits Agent token Secret references in this mode. Azure Identity
obtains and renews access tokens from the projected federation credential;
Manager validates RS256 signatures, issuer, audience, lifetime, tenant,
application role, client ID, principal ID, and the claimed stable Agent ID.
JWT key-service failures return HTTP 503; invalid credentials return HTTP 401.
Permit the tenant's Entra endpoints in the Manager and materializer egress
rules. Restart workloads after changing identity bindings.

Native C++ control clients do not implement this mode yet. Helm rejects
`agentAuth.mode=entra` together with `cppAgent.enabled=true`; do not configure
a C++ serving release against an Entra-only Manager. The default static modes
remain available for existing deployments, but do not satisfy the no-static-
Agent-credentials production criterion.

For on-premises fallback, `agentAuth.mtls.enabled` keeps Entra as the preferred
Agent authentication path and accepts a verified client certificate only when
its SHA-256 fingerprint maps to the presented Agent ID. `caSecretName` must
contain `ca.crt`; ingress-nginx verifies the certificate and Manager verifies
it again against the mounted CA. Every mapped Agent ID must belong to exactly
one configured placement pool. Unknown certificates, certificate/Agent-ID
mismatches and unmapped identities fail closed.

## Placement and workload values

- `manager.agentPlacementConfig`: operator-owned pool membership, source access,
  and connection/table placement rules. The Manager ignores self-reported pool
  and location values for authorization.
- `storage`: dynamic storage defaults or static Azure Files NFS volumes.
- `manager`, `materializer`, `cppAgent`: workload sizing and feature settings.
- `federation`: opt-in Phase 4 catalog and regional ownership settings.
  Set `enabled`, a stable `managerId`, the cloud-qualified
  `managerLocation`, and `manager.artifactStoreProfile` on every regional
  Manager. Set `deployCatalog` in the primary release to add the authenticated
  `fsp-global-catalog` service. `ownerLeaseMs` must exceed the renewal interval
  by enough time to tolerate transient storage failures. Catalog egress is
  default-denied when network policies are enabled; add only the private Blob
  endpoint CIDR and port 443 to `catalog.egressAllowList`.
- `federation.replicaStores`: Manager-only map of declared replica profiles to
  Azure Blob `account_url`, `container_name`, `auth_mode`, and optional identity
  settings. Add private replica Blob and identity endpoint egress explicitly.
  The Manager reconciles catalog pointers on its renewal cadence; interrupted
  copies resume by immutable create-if-absent after restart.
- `federation.regionalServing`: optional database-free, SigV4-authenticated S3
  Deployment and ClusterIP Service. Configure `endpointId`, `storageProfile`,
  `buckets`, `artifactStoreAccountUrl`, and `artifactStoreContainer`.
  `serviceAccountName` and `clientId` create an annotated workload identity with
  **Storage Blob Data Reader** on both local publications and catalog records.
  Set `regionalServing.ingress.enabled`, `hostname`, and issuer settings to
  expose only the regional S3 reader through TLS ingress.
  Use a separate identity from the publishing Manager. `readerSecretName` holds
  only `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY`, not source credentials.
  The workload uses `CONTROL_PORT` (default chart port 9401), a read-only root
  filesystem, and no source Secret. Add private Blob and identity egress to
  `egressAllowList`; ingress permits the configured ingress-controller selectors.
  Publish TLS ingress separately at the exact HTTPS URL declared in placement.
  Each bucket selects one dataset's current complete regional descriptor.
  Object keys are immutable publication paths, not legacy mutable table aliases.
  Point a Fabric shortcut at the returned publication root, and validate both
  Iceberg metadata references and Delta absolute S3 paths with Fabric before exit.
- `materializer.autoscaling`: optional HPA for the Python materializer
  StatefulSet. It requires `fsp.materializeMode=lazy` and
  `fsp.generationMembershipPolicy=elastic`; Helm rejects unsafe combinations.
- `cppAgent.s3AuthMode`: `trusted-upstream` for the default gateway-terminated
  topology, or `sigv4` when the C++ Agent authenticates data-plane requests.
  In `sigv4` mode, create the Secret named by `cppAgent.s3AuthSecretName` with
  the keys named by `s3AccessKeyIdKey` and `s3SecretAccessKeyKey`. Values never
  contain credential material. Set `cppAgent.s3AllowedPrefixes` to a
  semicolon-separated list to confine reads and listings to object-key prefixes;
  an empty value allows the whole configured bucket.
- `nginx`: private and public application proxy configuration.
- `tls`: cert-manager issuer and ingress hostname configuration.
- `managerUrl`: URL that Agents and materializers use to reach Manager.
  Defaults to `http://fsp-manager:9200`, the in-cluster service.
- `manager.enabled`, `cppAgent.enabled`: turn off components a site does not
  run. A remote materializer site sets both to `false`.
- `siteId`: short DNS-label prefix for the materializer `AGENT_ID`
  (`<siteId>-<pod>:9000`). Empty keeps the default `<hostname>:9000`. Set a
  unique value on every site that registers with a shared Manager. Pod names
  repeat across clusters, and two agents with the same id fight over one lease
  and loop on HTTP 409 heartbeats.
- `agentPoolId` and `agentLocation`: optional values sent with Agent
  registration. They describe the Agent's declared placement; they do not grant
  pool membership. The Manager derives pool membership and location from
  `manager.agentPlacementConfig`.
- `agentControlIngress`: optional TLS ingress that exposes only the Agent
  control routes of Manager to remote sites. Requires `host` and Manager.
- `materializer.egress`: optional egress NetworkPolicy for materializer pods.
  Allows DNS, the local Manager (`allowLocalManager`), Manager CIDRs on
  `managerPort`, and extra `allowList` destinations such as the source
  endpoint.
- `networkPolicy`: opt-in namespace default-deny ingress and egress policies.
  The role profiles enable it. Configure the ingress-controller selectors,
  metrics-scraper selectors, and environment-specific CIDRs before install.

The default values render the cluster-private base deployment. The enterprise
example enables Azure Files, workload identity, nginx, Entra authentication,
and public TLS ingress.

The catalog reuses the Manager operator authentication Secret for its
administrative API and uses the configured Azure artifact backend for durable
CAS records. Production values should point it at the dedicated private
catalog container created by `infra/fsp-federation`, with shared-key access
disabled and workload identity enabled.

## Materializer placement policies

Placement is disabled when `manager.agentPlacementConfig` is empty. When
enabled, configure each materializer pool with its exact registered agent IDs,
location, optional storage profile, and an explicit `allowed_connection_ids`
list. An empty list grants no source access. The Manager rejects startup if a
pool identity has no entry in the identity-token Secret.

For example, configure the hub release with:

```yaml
manager:
  agentPlacementConfig:
    tenants:
      - erp
    pools:
      - pool_id: erp-site-a
        tenant_id: erp
        identities:
          - site-a-materializer-0:9000
          - site-a-materializer-1:9000
        location: site-a
        storage_profile: central-blob
        allowed_connection_ids:
          - erp-a
        allowed_table_patterns:
          - sales.*
        max_concurrency: 4
      - pool_id: erp-site-b
        tenant_id: erp
        identities:
          - site-b-materializer-0:9000
        location: site-b
        storage_profile: central-blob
        allowed_connection_ids:
          - erp-a
    connections:
      erp-a:
        tenant_id: erp
        required_pool: erp-site-a
        required_storage_profile: central-blob
        fallback_pools:
          - erp-site-b
        max_concurrency: 8
    tables:
      erp-a::sales.orders:
        required_pool: erp-site-b
        required_location: site-b
```

When `tenants` is present, every pool, connection, store profile, and serving
endpoint must declare exactly one listed `tenant_id`. Table policies inherit
their connection tenant and cannot change it. Pool source access, store and
serving references, queued tasks, catalog records, regional S3 access audits,
and scheduler assignment are checked against that tenant. A task with a
missing or different tenant remains queued with
`tenant_isolation_violation`; fallback pools do not bypass the check.

The config-builder audit endpoint accepts an exact tenant filter:

```text
GET /_config/api/audit?tenant_id=erp
```

The filtered response excludes legacy and other-tenant events rather than
returning unowned records. Without a top-level `tenants` list, existing
single-tenant placement behavior and wire payloads remain unchanged.

### Agent release rings

An optional `agent_versions` policy accepts only the current and previous
major/minor release lines, then narrows each operator-owned pool to its rollout
ring:

```yaml
manager:
  agentPlacementConfig:
    agent_versions:
      current: "3.1"
      previous: "3.0"
      rings:
        non-production: ["3.1"]
        production-canary: ["3.1", "3.0"]
        remote-sites: ["3.0"]
      rollout_order:
        - non-production
        - production-canary
        - remote-sites
    pools:
      - pool_id: dev-pool
        upgrade_ring: non-production
        identities: [dev-agent]
        allowed_connection_ids: [erp-a]
```

Agent registration derives the ring from the configured Agent identity; a
self-reported pool or ring grants nothing. A release outside N/N-1 or outside
the pool's enabled ring returns HTTP 409 with
`agent_version_unsupported`. Rejected Agents never enter the registry, so
their tasks remain queued for a compatible worker. Omit `agent_versions` and
`upgrade_ring` together to preserve the existing version behavior.

### Cross-boundary byte and cost reports

Manager records verified materializer upload bytes when the pool location
differs from its assigned store location. Federated replication records the
sum of every verified object and manifest copied to another region. Events are
idempotent per task attempt or replica generation and retain tenant, dataset,
pool, source region, destination region, direction, and byte count.

Pricing is optional and operator-supplied. Rates are decimal prices per GiB
for an exact `source->destination` boundary:

```yaml
manager:
  egressPricing:
    currency: USD
    rates:
      "site-a->azure:swedencentral": 0.025
      "azure:swedencentral->azure:northeurope": 0.02
```

Retrieve the current UTC month or a specified month:

```text
GET /control/egress-report
GET /control/egress-report?month=2026-10
```

Every row reports measured integer `bytes` even when no rate exists.
`unpriced_bytes` keeps missing operator rates visible; estimated cost covers
only `priced_bytes`.

### Performance guardrails

Queued distributed materialization writes Parquet to a site-local temporary
file, then streams the file once to the artifact store while computing size,
SHA-256, and S3-compatible MD5. The complete Parquet output is not held in
memory or inserted into the Agent cache. Cloud publication verification uses
object size and SHA-256 metadata; only the bounded generation manifest is read
for exact content validation.

Split planning can enforce row and compressed-byte targets together:

```yaml
fsp:
  splitTargetRows: "250000"
  splitTargetBytes: "134217728"
  splitEstimatedBytesPerRow: "96"
```

The planner chooses the larger split count required by either target, bounded
by `SPLIT_COUNT_MIN` and `SPLIT_COUNT_MAX`. Set the byte target and estimate to
`0` to preserve row-only behavior.

Placement policy supports `source_max_concurrency` on a connection and
`max_concurrency` on each pool. Source limits apply across all of that
connection's tables; pool limits apply across every source assigned to the
pool. An omitted value remains unlimited.

Set `agentAuth.identityTokensSecretName` to a Kubernetes Secret with the
`AGENT_IDENTITY_TOKENS` key. Its JSON object maps every configured identity to
a token of at least 32 bytes. A token can be shared by agents in the same pool,
but tokens must differ between pools and from the Manager's shared
`AGENT_TOKEN`. For example:

```json
{
  "site-a-materializer-0:9000": "site-a-pool-token-with-at-least-32-bytes",
  "site-a-materializer-1:9000": "site-a-pool-token-with-at-least-32-bytes",
  "site-b-materializer-0:9000": "site-b-pool-token-with-at-least-32-bytes"
}
```

The remote site's `fsp-agent-auth` Secret must use that site's pool token as
`AGENT_TOKEN`. Keep the shared Manager token separate. Pool credentials bind
the supplied agent ID to the configured pool; changing `AGENT_POOL_ID` or
`AGENT_LOCATION` cannot grant access to a different pool.

### Signed placement policy bundles

Set `manager.policyBundle.enabled=true` to load Manager placement policy from a
signed global bundle instead of `manager.agentPlacementConfig`. The two inputs
are mutually exclusive inside the Manager process. Helm renders
`AGENT_PLACEMENT_CONFIG={}` for Manager when the bundle is enabled; the chart
value remains available to catalog and regional-reader workloads that do not
fetch the global bundle. Keep that bootstrap copy equivalent to the bundle's
`placement` object. A bundle has this shape:

```json
{
  "schema_version": 1,
  "revision": 42,
  "issued_at_ms": 1800000000000,
  "key_id": "global-2026",
  "placement": {
    "pools": [],
    "connections": {},
    "tables": {},
    "stores": {},
    "serving_endpoints": {}
  },
  "signature": "base64-ed25519-signature"
}
```

The Ed25519 signature covers the UTF-8 JSON encoding of every field except
`signature`, serialized with sorted keys and separators `,` and `:`. Store the
PEM public key in a Kubernetes Secret and configure the Manager:

```yaml
manager:
  agentPlacementConfig: {}
  policyBundle:
    enabled: true
    url: https://policy.example.com/fsp/placement.json
    publicKeySecretName: fsp-policy-key
    publicKeySecretKey: public-key.pem
    cacheFile: /config/policy-bundle.json
    keyId: global-2026
    timeoutSeconds: 5
networkPolicy:
  managerEgress:
    allowList:
      - cidr: 203.0.113.10/32
        ports: [443]
```

For a private Azure global layer, replace `url` with an identity-authenticated
Blob source:

```yaml
manager:
  policyBundle:
    enabled: true
    azure:
      accountUrl: https://catalog.blob.core.windows.net
      container: catalog
      blobName: policies/phase5-placement.json
      authMode: workload_identity
      clientId: 00000000-0000-0000-0000-000000000000
      tenantId: 00000000-0000-0000-0000-000000000000
      tokenFile: /var/run/secrets/azure/tokens/azure-identity-token
    publicKeySecretName: fsp-policy-key
    cacheFile: /config/policy-bundle.json
    keyId: global-2026
```

Configure exactly one source: `url` or `azure.accountUrl`. The private Blob
path uses managed identity or workload identity and the same signature,
revision, validation, and last-known-good rules as HTTPS.

Manager validates the signature, key ID, monotonic revision, and complete
placement policy before replacing the cache. HTTP 5xx responses, connection
failures, and timeouts use the previously verified cached bundle. HTTP 4xx
responses, malformed or unsigned bundles, invalid placement, and revision
rollback fail startup without replacing the last-known-good cache. Use a
persistent `/config` volume so the cache survives a pod or node restart.
Set `manager.auditLogEnabled: "1"` to emit placement decisions to the audit
ring/file sink and structured Manager logs.

Connection rules are defaults. An exact `connection_id::source_table` entry in
`tables` replaces its connection policy. A fallback is considered only when no
eligible agent in the required pool can accept the task. Older contract 1.0
agents can receive only tasks without a pool, location, storage-profile, or
fallback pin. If no configured pool can accept a task, it remains queued with
`no_eligible_materializer`, visible in `/control/work-queue` and the placement
audit log. Placement audit events contain the task, request, dataset, selected
pool/location, decision, and fallback flag, but never credentials or claim
tokens.

For Phase 4 residency declarations, see
[Federated artifact primitives](../../../docs/FEDERATED_ARTIFACTS.md).
Set `manager.artifactStoreProfile` to bind the Manager's configured backend to
the named profile. A constrained policy requires this binding and currently
must use that same store for staging and publication.

Pools can also set `heartbeat_ms`, `heartbeat_miss_limit` and
`claim_lease_seconds` for disconnected/WAN sites. Defaults are unchanged when
these fields are absent. The heartbeat alert uses each pool's exported timeout.
For connections with `residency_locations`, exact table policies inherit the
connection rule and cannot widen or remove that boundary; unconstrained
connections keep the replacement behavior described above.

The C++ Agent defaults to `trusted-upstream` because the chart's supported
topology places it behind the authenticated gateway. This mode trusts that
gateway to authenticate requests. Do not expose the Agent service directly in
this mode. For direct exposure, set `cppAgent.s3AuthMode=sigv4` and create the
referenced credential Secret before deploying. The Agent refuses to start in
`sigv4` mode when either credential is missing. Restrict the key with
`cppAgent.s3AllowedPrefixes` when it should not read the whole bucket.

For an existing fleet, first upgrade Manager with
`agentAuth.mode=compatibility` and create the active token Secret. Roll Python
and C++ Agents, verify active-source Agent-auth audit events, then set the mode
to `required`. Roll back by restoring `compatibility` on Manager while leaving
the active token and operator credentials unchanged.

To rotate, update `fsp-agent-auth` with a new active token, the old token as
previous, and an absolute Unix UTC deadline. Restart Manager, roll Agents, and
verify the fleet. Remove the previous key and deadline after the deadline. A
Manager restart does not extend the overlap.

`nginx.enabled` and `tls.enabled` must be enabled or disabled together because nginx always
mounts the certificate Secret. The values schema also validates image digests, replica counts,
storage quantities, and required object structure before templates render.

## Phase 0 routed PoC

Phase 0 runs one Manager in a hub cluster and materializers in other
clusters. Remote materializers call Manager over HTTPS through the Agent
control ingress. This is a proof of concept for v3.1.0, not the federated
design.

### Hub cluster: Agent control ingress

Enable the ingress on the release that runs Manager:

```bash
helm upgrade --install fsp deploy/helm/fabric-shortcut-proxy \
  --namespace fabric-shortcut-proxy \
  --set agentControlIngress.enabled=true \
  --set agentControlIngress.host=fsp-control.example.com \
  --set agentControlIngress.clusterIssuer=letsencrypt-prod \
  --set 'agentControlIngress.sourceRanges={198.51.100.0/24}'
```

The ingress routes only these Agent paths to the `fsp-manager` control port:

- `/control/register`, `/control/heartbeat`, `/control/task-result`,
  `/control/materialize` (exact match)
- `/control/assignment/`, `/control/snapshot/` (prefix match)

It never exposes `/control/work-queue`, `/_config`, `/_manager`, or
`/_monitor`. Operator routes stay cluster-private. Set `sourceRanges` to the
egress addresses of the remote sites. Agent auth still applies. Keep
`agentAuth.mode=required` (the default) when the ingress is open.

For an isolated acceptance release sharing an existing control hostname,
set `agentControlIngress.pathPrefix=/phase3` and append `/phase3` to the
remote site's `managerUrl`. The ingress strips this prefix and still routes
only the six Agent paths above. A prefix requires ingress-nginx regex and
rewrite support; it is empty by default. Operator endpoints remain private.

When two releases use Azure Files in the same cluster, give
`storage.azureFiles.artifacts.pvName` and
`storage.azureFiles.managerConfig.pvName` distinct names. Their defaults
preserve existing deployments. To isolate acceptance data on existing
shares, set the corresponding `subPath` values to pre-created directories.
Copy only the required configuration into the isolated config directory;
never point an acceptance Manager at a live Manager's writable config or
artifact directory. Use unique CSI volume handles for the additional PVs.

When Manager and a placement-bound materializer share a release, set
`materializer.agentAuthSecretName` to the pool's token Secret. The default
uses `agentAuth.secretName` for backwards compatibility. Placement pool
tokens must differ from the Manager's shared active and previous tokens.

#### Setup checklist

1. **Prerequisites.** The hub cluster needs ingress-nginx and cert-manager
   with a ClusterIssuer. The chart can create one through
   `certManager.clusterIssuer`; otherwise set
   `agentControlIngress.clusterIssuer` to an existing issuer.
2. **DNS.** Find the ingress-nginx LoadBalancer address and create an A
   record for `agentControlIngress.host` that points to it:

   ```bash
   kubectl -n ingress-nginx get svc ingress-nginx-controller \
     -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
   ```

3. **Values.** Put the settings in the site values file instead of `--set`:

   ```yaml
   agentControlIngress:
     enabled: true
     host: fsp-control.example.com
     clusterIssuer: letsencrypt-prod
   ```

4. **Apply.** Use `helm upgrade` as shown above. If cluster policy blocks
   Helm, render the template locally and apply it with `kubectl`:

   ```bash
   helm template fsp deploy/helm/fabric-shortcut-proxy \
     --namespace fabric-shortcut-proxy -f values-site.yaml \
     --show-only templates/agent-control-ingress.yaml > agent-control-ingress.yaml
   kubectl apply -f agent-control-ingress.yaml
   ```

5. **Verify.** Check that the certificate is issued and that only Agent
   control routes answer:

   ```bash
   kubectl -n fabric-shortcut-proxy get secret fsp-agent-control-tls
   curl -s -o /dev/null -w '%{http_code}\n' https://fsp-control.example.com/control/heartbeat  # 401 without Agent credentials
   curl -s -o /dev/null -w '%{http_code}\n' https://fsp-control.example.com/_manager           # 404
   ```

   A 401 on `/control/heartbeat` shows the route reaches Manager and Agent
   auth is enforced. Any other path should return 404.

`sourceRanges` also applies to the cert-manager HTTP-01 solver. When it is
set, Let's Encrypt validation requests are rejected and certificate renewal
fails. Either allow the Let's Encrypt validation traffic or use a DNS-01
issuer when you restrict source ranges.

### Remote cluster: materializer-only profile

[values-remote-materializer.yaml](values-remote-materializer.yaml) installs the
materializer workload without Manager, the C++ Agent, nginx, TLS ingress, or
Azure Files. It points `managerUrl` at the hub ingress and enables materializer
egress. Edit `siteId`, `managerUrl`, `materializer.egress.managerCidrs`, and
`materializer.egress.allowList` for the site, then install. `siteId` must be
unique per site, or agent ids collide with another cluster's:

```bash
kubectl create namespace fabric-shortcut-proxy
kubectl -n fabric-shortcut-proxy create secret generic fsp-source --from-env-file=source.env
kubectl -n fabric-shortcut-proxy create secret generic fsp-agent-auth --from-env-file=agent-auth.env
helm upgrade --install fsp-remote deploy/helm/fabric-shortcut-proxy \
  --namespace fabric-shortcut-proxy \
  -f deploy/helm/fabric-shortcut-proxy/values-remote-materializer.yaml
```

Uninstall with:

```bash
helm uninstall fsp-remote --namespace fabric-shortcut-proxy
```

The `fsp-source` Secret is created in each site and is never copied to the
hub. Source credentials stay in the site that reads the source. The egress
policy limits materializer traffic to DNS, Manager, and the listed
destinations. It needs a CNI that enforces egress NetworkPolicy.

### Network policy requirements

The `networkPolicy.enabled` option installs namespace-wide default-deny
policies, then adds only the chart's declared DNS, Manager, materializer,
serving-agent, nginx, ingress-controller, and optional Prometheus flows.
Kubernetes must run a NetworkPolicy-enforcing CNI. A cluster API accepting
`NetworkPolicy` objects does not prove that its network provider enforces them.

Replace every reserved `203.0.113.0/24` example in the profile values before
installing. Remote materializer sites need the Manager control-ingress IP and
CIDRs for local source and private artifact endpoints. The Manager-only
profile needs egress CIDRs for its artifact store and any identity or secret
endpoints it contacts. NetworkPolicy `ipBlock` rules use IP ranges, not DNS
hostnames. For changing public service addresses, use a private endpoint with
a stable private address or an egress firewall that supports service tags.

Set `networkPolicy.ingressController` to the labels on the ingress controller
Pods that forward HTTPS to Manager. Set `networkPolicy.metricsScraper` to the
monitoring namespace and scraper Pod labels when scraping is enabled. The
Manager and materializer Pods carry `prometheus.io/scrape`, `prometheus.io/path`,
and `prometheus.io/port` annotations; the Prometheus or Azure Monitor scrape
configuration must honor those annotations. Scrapers need ingress to ports
9200 (Manager) and 9000 (materializer). The serving-agent profile also needs
the data-plane client namespace and Pod labels allowed by its ingress policy.

Run `helm lint` and `helm template` with each environment values file before
installing. Verify DNS, Manager heartbeat, artifact-store access, source
database access, and data-plane reads after applying the policies. The example
CIDRs are documentation-only and must not be used as live endpoint addresses.

### Phase 3 telemetry

The Manager exports registered Agent heartbeat age and scheduler queue depth,
assignment rejections, source query latency, and artifact upload bytes and
duration at `/metrics`. Heartbeat and scheduler metrics are Manager-process
metrics. Source-query and upload metrics are emitted by the materializer
process, so scrape both endpoints for a multi-site view. Labels include
`location`; heartbeat age also includes `agent_id`.

The Grafana dashboard and Prometheus Operator alert rules are in
[`deploy/observability/`](../../observability/README.md). Review the queue,
latency, and throughput thresholds against the site's normal workload before
enabling paging. The alert file uses the Prometheus Operator `PrometheusRule`
custom resource.

### Manager-only and serving-agent profiles

Use [values-manager.yaml](values-manager.yaml) for a central Manager release.
It disables materializer, C++ Agent, nginx, and TLS workloads. Use
[values-serving-agents.yaml](values-serving-agents.yaml) for the C++ serving
Agent without Manager or materializer pods:

```bash
helm upgrade --install fsp-manager deploy/helm/fabric-shortcut-proxy \
  --namespace fabric-shortcut-proxy \
  -f deploy/helm/fabric-shortcut-proxy/values-manager.yaml

helm upgrade --install fsp-serving deploy/helm/fabric-shortcut-proxy \
  --namespace fabric-shortcut-proxy \
  -f deploy/helm/fabric-shortcut-proxy/values-serving-agents.yaml
```

Install each profile in its own cluster or namespace. In one cluster, give
each release a matching Helm namespace and `namespace.name`; for example:

```bash
helm upgrade --install fsp-manager deploy/helm/fabric-shortcut-proxy \
  --namespace fabric-shortcut-proxy-manager \
  --set namespace.name=fabric-shortcut-proxy-manager \
  -f deploy/helm/fabric-shortcut-proxy/values-manager.yaml

helm upgrade --install fsp-serving deploy/helm/fabric-shortcut-proxy \
  --namespace fabric-shortcut-proxy-serving \
  --set namespace.name=fabric-shortcut-proxy-serving \
  -f deploy/helm/fabric-shortcut-proxy/values-serving-agents.yaml
```

Each release also creates support resources such as `fsp-common` and
`fsp-artifacts`. The serving profile exposes the `shortcut-proxy` ClusterIP
service; private load-balancer and ingress setup is covered by the Phase 3
networking work. The combined default values remain available for the existing
single-cluster deployment.

### Shared filesystem

The chart keeps the `fsp-artifacts` PVC for local storage and existing
deployments. For distributed deployments, configure
`fsp.artifactStoreBackend: azure` and a shared Blob container instead of
mounting the same RWX share across clusters. Azure Blob storage uses the
identity and container settings described under `fsp` in the Values section.

## Existing Kustomize deployment

The deployment script uses Helm's `--take-ownership` flag during migration.
This adds Helm ownership metadata to matching resources that were originally
created by the retired `enterprise-demo` Kustomize overlay. Review the first
rendered manifest before running the migration against another environment. The enterprise
render contains 27 resources. See the
[migration guide](../../../docs/HELM_MIGRATION_GUIDE.md) for ownership checks and rollback.

Adoption can retain legacy environment entries that are absent from the new
render. When moving to Entra Agent authentication, inspect the running
workload's environment variable names and Secret references after rollout.
If a retired `AGENT_TOKEN`, `AGENT_TOKEN_PREVIOUS`, or `AGENT_IDENTITY_TOKENS`
entry survives adoption, remove only that entry with a server-side dry run
followed by `kubectl set env`, then verify readiness and authenticated control
access. Do not print Secret values. Retain unused Secrets under the existing
rollback policy until rollback no longer requires them.

## Release lifecycle

The deployment script packages `fabric-shortcut-proxy-3.0.1.tgz` in a temporary directory and
runs `helm upgrade --install --atomic --take-ownership` through AKS Run Command. It also manages
pinned cert-manager and ingress-nginx releases.

The Namespace, PVs, and PVCs carry `helm.sh/resource-policy: keep`. Helm uninstall leaves those
objects behind. Roll back workload changes with `helm history` and `helm rollback`; manage Azure
resources and retained shares through the Bicep and retention process.
