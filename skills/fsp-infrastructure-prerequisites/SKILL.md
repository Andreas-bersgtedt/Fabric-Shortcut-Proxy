---
name: fsp-infrastructure-prerequisites
description: "Prepare infrastructure prerequisites for a private Fabric Shortcut Proxy deployment. Use for AKS, ACR, Key Vault, managed identities, private endpoints, private DNS, jump boxes, OPDG hosts, internal load balancers, and SSH or kubectl tunnels before deploying the application."
argument-hint: "Describe the Azure topology, private endpoints, administration path, and identity model."
---

# Fabric Shortcut Proxy Infrastructure Prerequisites

## Use When

- Preparing a private AKS deployment for the Manager and Agents.
- Creating ACR, Key Vault, managed identity, private endpoint, or private DNS prerequisites.
- Setting up a jump box for `az`, `kubectl`, Docker, and private administration.
- Connecting an On-Premises Data Gateway host to the data plane.
- Providing an operator browser path through SSH and `kubectl port-forward`.

## Target Topology

Establish these paths before deploying workloads:

```text
operator -> SSH -> jump box -> kubectl port-forward -> Manager :9200
OPDG/Fabric -> private DNS -> internal LB/private ingress -> Agent :9000
AKS pods -> private DNS/private endpoints -> source SQL, Key Vault, and ACR
Manager -> OneLake DFS endpoint (only when Open Mirroring is enabled)
```

Keep the Manager control plane private to administrators. Give Fabric or OPDG only the data-plane route. Do not use pod IPs as production endpoints.

## Prerequisite Checklist

1. Choose the Azure subscription, region, resource group, AKS VNet, application subnet, admin VNet, and private DNS ownership.
2. Create or identify a private ACR and grant the AKS kubelet identity pull permission.
3. Create or identify Key Vault and choose managed identity, service principal, or default Azure credential. Grant only the required secret read/write permissions.
4. Populate `main.local.bicepparam`, run Bicep what-if, and provision the private AKS cluster,
  node pools, workload identity, ACR, Key Vault, Azure Files shares, and network resources.
5. Keep Kubernetes Namespace, ConfigMap, Services, PVs, and PVCs under Helm ownership rather
  than creating parallel copies manually.
6. Create private endpoints and DNS links for ACR, Key Vault, source SQL, and other private services. Verify that AKS resolves private addresses, not public addresses.
7. Peer the admin and AKS VNets in both directions. Add NSG/firewall rules for only the required paths.
8. Prepare the operator host with Azure CLI, Helm 3.18.6, Git, and network test tools. Private
  cluster workload operations use `az aks command invoke`; direct kubectl access is optional.
9. Configure the Helm chart's `fsp-nginx-private` Service with a fixed IP in the application
  subnet, then create the private DNS A record.
10. Prepare the OPDG host, if used, and confirm it reaches the private HTTPS endpoint on port 443.

## Private Networking Checks

Run checks from the network location that will make the request, not only from the jump box:

```bash
az aks get-credentials -g <resource-group> -n <aks-cluster>
kubectl get nodes
kubectl run netcheck --rm -it --restart=Never --image=curlimages/curl -- \
  curl -sS -i http://<private-agent-fqdn>:9000/healthz
```

On the jump box or OPDG host, verify DNS and TCP separately:

```powershell
Resolve-DnsName <agent-private-fqdn>
Test-NetConnection <agent-private-fqdn> -Port 9000
Test-NetConnection <manager-private-fqdn> -Port 9200
```

Expected results are private DNS answers, reachable TCP ports, and no public route for administration or the data plane.

## Internal Load Balancer and Private Link

The production Helm chart creates `fsp-nginx-private`, an Azure internal LoadBalancer on port
443. It selects the in-cluster FSP nginx TLS proxy, which forwards data requests to the Python
materializers. The private IP and subnet are required Helm values.

```powershell
./infra/fsp-demo/Deploy-FspDemo.ps1 -WhatIf
az aks command invoke --resource-group <resource-group> --name <aks-cluster> `
  --command "kubectl -n fabric-shortcut-proxy get svc fsp-nginx-private -o wide"
```

Publish the configured private IP behind private DNS. Do not hardcode a pod IP or ClusterIP.
The `deploy/kubernetes/overlays/aks-validation` overlay is retained only for focused development
validation and is not the enterprise release definition.

An AKS stop/start causes a temporary data-plane outage while nodes and Agent pods recover, but it
normally preserves this Service frontend. A Service deletion and recreation can allocate a new
private IP. Use the DNS hostname for clients, and reserve or pin the frontend IP when a fixed
address is required.

For Fabric Spark Managed VNet access, place a Standard internal Load Balancer or the Service frontend behind a Private Link Service, then approve the Fabric managed private endpoint connection.

```bash
az network private-link-service show -g <resource-group> -n <pls-name> \
  --query 'privateEndpointConnections[].privateLinkServiceConnectionState.status' -o tsv
```

Do not approve a private endpoint until the target, subnet, DNS, and expected client path have been reviewed.

## Key Vault and Identity

1. Assign the proxy identity to the Manager and Agent workloads as needed.
2. Grant least-privilege Key Vault data-plane access to read the configured secret names; add write permission only when `KEYVAULT_WRITE_BACK=1` is intentional.
3. Configure the proxy identity mode and private DNS/private endpoint for Key Vault.
4. Test identity and vault access from an AKS pod without printing secret values.
5. Keep a local encrypted credential-store fallback available for controlled recovery; a Key Vault outage is designed to fail soft, but missing initial credentials still needs operator action.

## Jump Box and SSH Tunnel

Use a jump box instead of exposing Manager publicly. A stable port-forward can be run on the jump box:

```bash
kubectl -n fabric-shortcut-proxy port-forward svc/fsp-manager 9200:9200 --address 127.0.0.1
```

From the operator workstation, forward a local port through SSH to the jump box's loopback:

```bash
ssh -N -L 9200:127.0.0.1:9200 <admin-user>@<jump-host>
```

Browse `http://127.0.0.1:9200/` only after Manager authentication is configured. Bind port-forwards to loopback unless a controlled shared operator endpoint is explicitly required.

## Completion Gate

Infrastructure is ready when the AKS API is reachable from the jump box, AKS pods resolve and reach private dependencies, the OPDG/Fabric client reaches the intended data-plane frontend, Key Vault identity access works, ACR pulls succeed, and the Manager port-forward works. Then continue with [deployment](../fsp-deployment/SKILL.md).

## References

- [Enterprise AKS runbook](../../infra/fsp-demo/README.md)
- [Enterprise deployment guide](../../docs/Enterprise_Deployment_guide.md)
- [Helm chart reference](../../deploy/helm/fabric-shortcut-proxy/README.md)
- [Connectivity setup](../../docs/CONNECTIVITY_SETUP.md)
- [Installation manual](../../docs/manual/04-installation.md)
- [Security](../../docs/SECURITY.md)