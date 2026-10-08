# Fabric Shortcut Proxy Helm chart

This chart packages the FSP Manager, Python materializers, C++ serving Agents,
storage, network policy, nginx proxy, and optional cert-manager ingress as one
Helm release. Chart and application version `3.0.1` represent the stable
baseline used for the enterprise demo migration.

## Prerequisites

- Kubernetes 1.25 or later
- Helm 3.18.6 or later
- Existing `fsp-source`, `fsp-agent-auth`, and optional `fsp-manager-auth` Secrets
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
  Keep both values identical. Credentials do not belong here.
- `workloadIdentity`: Azure workload identity service account configuration.
- `agentAuth`: required or one-release compatibility mode plus Secret key
  references. The default is `required`; values never contain token material.
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
- `agentControlIngress`: optional TLS ingress that exposes only the Agent
  control routes of Manager to remote sites. Requires `host` and Manager.
- `materializer.egress`: optional egress NetworkPolicy for materializer pods.
  Allows DNS, the local Manager (`allowLocalManager`), Manager CIDRs on
  `managerPort`, and extra `allowList` destinations such as the source
  endpoint.

The default values render the cluster-private base deployment. The enterprise
example enables Azure Files, workload identity, nginx, Entra authentication,
and public TLS ingress.

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

### Remote cluster: materializer-only profile

[values-remote-materializer.yaml](values-remote-materializer.yaml) disables
Manager, the C++ Agent, nginx, TLS ingress, and Azure Files. It points
`managerUrl` at the hub ingress and enables materializer egress. Edit
`siteId`, `managerUrl`, `materializer.egress.managerCidrs`, and
`materializer.egress.allowList` for the site, then install. `siteId` must be
unique per site (for example `neu`), or the site's agent ids collide with the
hub's:

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

### Shared filesystem

Materializers and Manager exchange data through the `fsp-artifacts` RWX volume.
Across clusters, Phase 0 mounts the same RWX share (for example one Azure Files
share) in every site. This is for the PoC only. It adds cross-site latency,
couples the sites to one storage account, and does not meet data residency
needs. Later phases replace it with per-site storage and manifest publication
through Manager.

## Existing Kustomize deployment

The deployment script uses Helm's `--take-ownership` flag during migration.
This adds Helm ownership metadata to matching resources that were originally
created by the retired `enterprise-demo` Kustomize overlay. Review the first
rendered manifest before running the migration against another environment. The enterprise
render contains 27 resources. See the
[migration guide](../../../docs/HELM_MIGRATION_GUIDE.md) for ownership checks and rollback.

## Release lifecycle

The deployment script packages `fabric-shortcut-proxy-3.0.1.tgz` in a temporary directory and
runs `helm upgrade --install --atomic --take-ownership` through AKS Run Command. It also manages
pinned cert-manager and ingress-nginx releases.

The Namespace, PVs, and PVCs carry `helm.sh/resource-policy: keep`. Helm uninstall leaves those
objects behind. Roll back workload changes with `helm history` and `helm rollback`; manage Azure
resources and retained shares through the Bicep and retention process.
