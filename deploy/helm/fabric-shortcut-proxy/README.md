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

## Placement and workload values

- `manager.agentPlacementConfig`: operator-owned pool membership, source access,
  and connection/table placement rules. The Manager ignores self-reported pool
  and location values for authorization.
- `storage`: dynamic storage defaults or static Azure Files NFS volumes.
- `manager`, `materializer`, `cppAgent`: workload sizing and feature settings.
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
    pools:
      - pool_id: erp-site-a
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
        identities:
          - site-b-materializer-0:9000
        location: site-b
        storage_profile: central-blob
        allowed_connection_ids:
          - erp-a
    connections:
      erp-a:
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
