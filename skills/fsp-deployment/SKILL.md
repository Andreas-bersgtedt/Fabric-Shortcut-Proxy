---
name: fsp-deployment
description: "Deploy and install Fabric Shortcut Proxy on Windows, Linux, or private Azure Kubernetes Service. Use for Manager.ps1, Manager.sh, Docker, AKS, internal load balancers, private networking, upgrades, and deployment verification."
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
4. Read the platform-specific installation guide: [Windows deployment](../../docs/installation/Windows_Deployment.md), [Linux deployment](../../docs/installation/Linux_Deployment.md), or the [enterprise AKS guide](../../docs/Enterprise_Deployment_guide.md).

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

## AKS Deployment Sequence

1. Build and push the image to a private ACR.
2. Provision the private AKS cluster, namespace, identity/RBAC, ConfigMap, Secrets, Manager config volume, and RWX artifact volume.
3. Deploy Manager on port `9200` and Agents on the S3 data-plane port, normally `9000`.
4. Expose the data plane through the AKS overlay's `fsp-materializer-internal` internal LoadBalancer Service on port `9000`, private ingress, or gateway. Do not use a pod IP as a production DNS target.
5. Create a private DNS A record for the Agent hostname pointing to the internal LoadBalancer frontend. Verify paths from the jump host, OPDG host, AKS pods, source SQL endpoint, Key Vault, ACR, and OneLake as applicable.
6. Use `/healthz` for process liveness and `/readyz` for readiness. In a Manager deployment, `/readyz` can represent fleet readiness and may remain non-200 until an Agent registers.
7. Verify an authenticated S3 `HEAD`/`GET` against a known object before connecting Fabric.

## Upgrade and Rollback

1. Record the current image digest or Git commit, effective configuration, and health responses.
2. Deploy the new version without replacing persistent config or artifact storage.
3. Check Manager health, Agent registration, readiness, logs, and a representative object read.
4. Roll back to the recorded image/commit if readiness or object reads regress. Do not delete the shared artifact store during a code rollback.

## References

- [Installation manual](../../docs/manual/04-installation.md)
- [Connectivity setup](../../docs/CONNECTIVITY_SETUP.md)
- [Enterprise deployment guide](../../docs/Enterprise_Deployment_guide.md)
- [Windows deployment](../../docs/installation/Windows_Deployment.md)
- [Linux deployment](../../docs/installation/Linux_Deployment.md)