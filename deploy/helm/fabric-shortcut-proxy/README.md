# Fabric Shortcut Proxy Helm chart

This chart packages the FSP Manager, Python materializers, C++ serving Agents,
storage, network policy, nginx proxy, and optional cert-manager ingress as one
Helm release. Chart and application version `2.9.3` represent the stable
baseline used for the enterprise demo migration.

## Prerequisites

- Kubernetes 1.25 or later
- Helm 3.18.6 or later
- An existing `fsp-source` Secret containing the source and Manager credentials
- cert-manager and ingress-nginx when `tls.enabled` is true
- Azure Files CSI support when `storage.azureFiles.enabled` is true

The chart never creates credential-bearing Secrets. Create `fsp-source` and any
configured image pull Secret outside Helm before installing the release.
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
- `storage`: dynamic storage defaults or static Azure Files NFS volumes.
- `manager`, `materializer`, `cppAgent`: workload sizing and feature settings.
- `nginx`: private and public application proxy configuration.
- `tls`: cert-manager issuer and ingress hostname configuration.

The default values render the cluster-private base deployment. The enterprise
example enables Azure Files, workload identity, nginx, Entra authentication,
and public TLS ingress.

`nginx.enabled` and `tls.enabled` must be enabled or disabled together because nginx always
mounts the certificate Secret. The values schema also validates image digests, replica counts,
storage quantities, and required object structure before templates render.

## Existing Kustomize deployment

The deployment script uses Helm's `--take-ownership` flag during migration.
This adds Helm ownership metadata to matching resources that were originally
created by the retired `enterprise-demo` Kustomize overlay. Review the first
rendered manifest before running the migration against another environment. The enterprise
render contains 27 resources. See the
[migration guide](../../../docs/HELM_MIGRATION_GUIDE.md) for ownership checks and rollback.

## Release lifecycle

The deployment script packages `fabric-shortcut-proxy-2.9.3.tgz` in a temporary directory and
runs `helm upgrade --install --atomic --take-ownership` through AKS Run Command. It also manages
pinned cert-manager and ingress-nginx releases.

The Namespace, PVs, and PVCs carry `helm.sh/resource-policy: keep`. Helm uninstall leaves those
objects behind. Roll back workload changes with `helm history` and `helm rollback`; manage Azure
resources and retained shares through the Bicep and retention process.
