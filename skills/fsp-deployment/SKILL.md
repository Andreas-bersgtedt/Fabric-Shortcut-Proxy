---
name: fsp-deployment
description: "Deploy and install Fabric Shortcut Proxy on Windows, Linux, or private Azure Kubernetes Service. Use for Manager.ps1, Manager.sh, Bicep, Helm, Deploy-FspDemo.ps1, Docker, AKS, upgrades, rollback, and deployment verification."
argument-hint: "Describe the target platform, topology, source database, and deployment constraint."
---

# Fabric Shortcut Proxy Deployment

## Use When

- Installing the proxy on Windows or Linux.
- Deploying the Manager and Agents to AKS.
- Building or publishing container images.
- Adding a stable private data-plane endpoint.
- Upgrading or rolling back a deployment.

## Before Changing Anything

1. Identify the target platform and whether this is a single process, Manager plus local Agent, or multi-Agent AKS deployment.
2. Confirm the source database type, output mode (`iceberg` or `delta`), required ports, artifact-store location, and secret-management choice.
3. Keep populated configuration files, credentials, tenant IDs, hostnames, and private IPs outside source control.
4. Read the platform-specific installation guide: [Windows deployment](../../docs/installation/Windows_Deployment.md), [Linux deployment](../../docs/installation/Linux_Deployment.md), or the [enterprise AKS runbook](../../infra/fsp-demo/README.md).

## Local or VM Deployment

### Windows

```powershell
.\Manager.ps1 -Recreate -NoPull
.\Manager.ps1 -SkipInstall
```

Use `-DbUrl`, `-AgentPort`, `-ControlPort`, `-TableFormat`, `-AdminUi`, and `-ConfigUi` only when needed. For SQL Server, install ODBC Driver 18 separately. The Fabric/S3 endpoint is normally port `9000`; Manager administration is normally port `9200`.

### Linux or macOS

```bash
bash ./Manager.sh --recreate --no-pull
bash ./Manager.sh --skip-install
```

For systemd, run the service as a dedicated user with a protected environment file. Confirm the service uses the intended repository and virtual environment before troubleshooting dependency errors.

### Public operator console

For a public operator origin, place nginx or another TLS terminator in front of
the Manager and Config Builder. Use a trusted FQDN and certificate, normally on
443 or the configured operator TLS port such as 9443. Register the exact HTTPS
Config Builder and Manager redirect/logout URIs in the Entra SPA application.
Keep the direct Manager and Config Builder HTTP listeners private or loopback-only.

The browser uses MSAL and sends an FSP API bearer token. Do not expose a public
HTTP operator URL or use a self-signed certificate for the Entra browser flow.
A DNS-01 certificate is appropriate when port 80 is not reachable; manual DNS
certificates need a documented renewal procedure.

## AKS Deployment Sequence

1. Copy the sanitized Bicep, deployment, ingress, and Helm examples to their ignored local paths.
2. Run `az deployment sub what-if` and review the Azure resource changes. Provision with
	`main.bicep` only after approval.
3. Complete external RBAC, push immutable images to ACR, record digests in local Helm values,
	and create the existing `fsp-source` Secret without placing credentials in Helm values.
4. Run `Start-FspDemo.ps1 -SkipWorkloadValidation` to start SQL MI, OPDG VM, and AKS.
5. Run `Deploy-FspDemo.ps1 -WhatIf`, then `Deploy-FspDemo.ps1`. It installs pinned
	cert-manager, ingress-nginx, and FSP Helm releases through private AKS Run Command.
6. Publish private DNS for the fixed `fsp-nginx-private` frontend and use HTTPS on port 443.
	Do not use a pod IP or ClusterIP.
7. Verify `helm status`, pod readiness, certificate readiness, Manager fleet registration, and
	an authenticated S3 `HEAD`/`GET` before connecting Fabric.

There is no single all-phases PowerShell script. Keep infrastructure preview/create, runtime
startup, and Helm deployment as separate review and retry boundaries.

## Upgrade and Rollback

1. Record the current image digest or Git commit, effective configuration, and health responses.
2. Update immutable image digests or Helm values, render/lint, then run
	`Deploy-FspDemo.ps1 -WhatIf` and the atomic upgrade.
3. Check Manager health, Agent registration, readiness, logs, and a representative object read.
4. Use `helm history` and `helm rollback` through AKS Run Command when workload resources
	regress. Do not delete retained Namespace, PVs, PVCs, or Azure Files shares.

## AKS Endpoint Durability

An AKS stop/start causes downtime while nodes and Agent pods recover, but normally preserves the
Kubernetes Service and its Azure LoadBalancer frontend. Deleting and recreating the
`LoadBalancer` Service can allocate a new private IP. Use the Agent private DNS hostname for
clients, record `EXTERNAL-IP` after Service changes, and reserve or pin the frontend IP when a
fixed address is required. Never use a pod IP.

## References

- [Installation manual](../../docs/manual/04-installation.md)
- [Connectivity setup](../../docs/CONNECTIVITY_SETUP.md)
- [Enterprise AKS runbook](../../infra/fsp-demo/README.md)
- [Helm chart reference](../../deploy/helm/fabric-shortcut-proxy/README.md)
- [Helm migration guide](../../docs/HELM_MIGRATION_GUIDE.md)
- [Enterprise deployment design](../../docs/Enterprise_Deployment_guide.md)
- [Windows deployment](../../docs/installation/Windows_Deployment.md)
- [Linux deployment](../../docs/installation/Linux_Deployment.md)