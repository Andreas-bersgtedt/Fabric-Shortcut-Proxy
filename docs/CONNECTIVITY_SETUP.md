# Connectivity Setup Guide

The hands‑on companion to [UsecasesAndScenarios.md](UsecasesAndScenarios.md). That document
explains **why** each private‑connectivity pattern exists; this one is the **how** — concrete,
copy‑paste steps to wire Microsoft Fabric to the Fabric Shortcut Proxy for each pattern, plus a
recipe that maps every scenario in the matrix to its configuration.

**Companion docs:** [installation/Linux_Deployment.md](installation/Linux_Deployment.md) ·
[installation/Windows_Deployment.md](installation/Windows_Deployment.md) ·
[../SSL_Deployment.md](../SSL_Deployment.md) (public TLS) ·
[SECURITY.md](SECURITY.md) · [CONFIGURATION.md](CONFIGURATION.md) · [../FAQ.md](../FAQ.md).

---

## 0. Before you start (common baseline)

Get the proxy installed and a source table serving first — follow
[installation/Linux_Deployment.md](installation/Linux_Deployment.md) or
[installation/Windows_Deployment.md](installation/Windows_Deployment.md) through the
verification step. Every connectivity pattern below reuses the **same S3 endpoint**; only the
Fabric‑side networking and client differ.

Baseline proxy settings that all patterns share (`config.system.json`):

```json
{
  "system": {
    "bucket": "fabric-iceberg-poc",
    "require_sigv4": true,
    "table_format": "delta"
  }
}
```
- `require_sigv4: true` — Fabric/Spark present SigV4; the proxy verifies it.
- `table_format: delta` — Fabric reads `_delta_log` directly (recommended, esp. for Spark).
- SigV4 keys: `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` (or scoped access keys). These are the
  only credentials Fabric ever sees — the source DB/upstream secrets stay inside the proxy
  (credential mediation). See [SECURITY.md](SECURITY.md).

Two data paths from that one endpoint (choose per bucket):
- **Warehouse** (DB→table virtualization): the default path for the `fabric-iceberg-poc` bucket.
- **Passthrough** (files/objects): a *mounted* bucket — see [§4](#4-storage-proxy-mounts-passthrough-scenarios).

---

## 1. Pattern A — Shortcut via On‑Premises Data Gateway (private)

Use for **OneLake shortcuts** reaching a proxy with **no public exposure** (on‑prem, VPC‑private,
firewall‑restricted). The OPDG is the bridge; Fabric never touches the public internet.

**A1. Install + register the OPDG** on a Windows host that can reach the proxy on the same
LAN/VNet/VPC:
- Download *On‑premises data gateway* (standard mode), install, sign in with your Fabric account,
  register it to your tenant.
- Confirm it shows **online** in Fabric admin → *Connections and gateways*.

**A2. Open the proxy port to the gateway host only:**
```bash
# Linux host firewall example — allow only the OPDG's IP to reach the agent port
sudo ufw allow from <opdg-host-ip> to any port 9000 proto tcp
# keep the control plane (9200) closed to everyone but admins
```

**A3. Create the shortcut** in a Fabric Lakehouse → *New shortcut → Amazon S3 compatible*:

| Field | Value |
|---|---|
| URL | `http://<proxy-private-ip>:9000` |
| Access key / Secret | your `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` |
| **Data gateway** | select your **OPDG** |
| Path | browse the `fabric-iceberg-poc` bucket → pick the table folder(s) |

Only the queried rows traverse the gateway. Ref:
<https://learn.microsoft.com/fabric/onelake/create-on-premises-shortcut>.

> **Public‑internet variant (no OPDG):** if there is no private path, expose the proxy over
> HTTPS and point the shortcut **directly** at `https://<fqdn>` with *Data gateway* = *None*.
> Full TLS procedure: [../SSL_Deployment.md](../SSL_Deployment.md).

---

## 2. Pattern B — Spark via Managed Private Endpoint + Private Link Service (in‑VNet)

Use for **Fabric Spark** notebooks / Spark Job Definitions reading the proxy that runs in an
**Azure VNet**. Spark reaches it over the Microsoft backbone via an MPE→PLS, no public exposure.

```
Fabric Spark (Managed VNet) ─ MPE ─▶ Private Link Service ─▶ internal Std LB ─▶ proxy :9000
```

**B1. Proxy** — same baseline (§0); set `forwarded_allow_ips` to the LB subnet so audit logs the
real client IP, and bind so the LB can reach it:
```json
{ "system": { "host": "0.0.0.0", "port": 9000, "require_sigv4": true,
              "table_format": "delta", "forwarded_allow_ips": "10.0.0.0/8" } }
```

**B2. Internal Standard LB + Private Link Service** in the proxy's VNet (PLS requires a Standard
*internal* LB):
```bash
az network lb create -g rg-proxy -n fsp-ilb --sku Standard \
  --vnet-name proxy-vnet --subnet proxy-subnet \
  --frontend-ip-name feip --backend-pool-name bepool
az network lb probe create -g rg-proxy --lb-name fsp-ilb -n readyz \
  --protocol Http --port 9000 --path /readyz
az network lb rule create -g rg-proxy --lb-name fsp-ilb -n s3 \
  --protocol Tcp --frontend-port 9000 --backend-port 9000 \
  --frontend-ip-name feip --backend-pool-name bepool --probe-name readyz
# ... add the proxy VM NIC(s) to bepool ...

az network private-link-service create -g rg-proxy -n fsp-pls \
  --vnet-name proxy-vnet --subnet pls-subnet \
  --lb-name fsp-ilb --lb-frontend-ip-configs feip --connection-subnet-name pls-subnet
az network private-link-service show -g rg-proxy -n fsp-pls --query alias -o tsv
```
Ensure the PLS NAT subnet has `privateLinkServiceNetworkPolicies=Disabled`.

**B3. Fabric MPE** — the workspace must be on a **Managed VNet** (Workspace settings → Network).
Then *Managed private endpoints → New* → target the PLS **resource id / alias** from B2, and
**approve** the pending connection on the PLS:
```bash
az network private-link-service connection update -g rg-proxy \
  --service-name fsp-pls -n <pe-connection-name> --connection-status Approved
```

**B4. Read from a Spark notebook** (path‑style + SigV4; the proxy accepts any signed region):
```python
ep = "http://<pls-private-fqdn>:9000"     # https://... if you terminate TLS
spark.conf.set("spark.hadoop.fs.s3a.endpoint", ep)
spark.conf.set("spark.hadoop.fs.s3a.access.key", "<S3_ACCESS_KEY_ID>")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "<S3_SECRET_ACCESS_KEY>")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")      # bucket is in the path
spark.conf.set("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")   # any region
spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
               "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

df = spark.read.format("delta").load(
    "s3a://fabric-iceberg-poc/db/<server>/<database>/<schema>/<table>")
df.show()
```
If the Spark runtime lacks the `hadoop-aws` S3A connector, use **boto3** (always present) to
fetch objects instead. `path.style.access=true` is mandatory (the bucket rides in the path).

---

## 3. Pattern C — Spark for on‑prem / other‑cloud sources

When the proxy is **not** in Azure, front the PLS with a **forwarding VM** (iptables
DNAT/MASQUERADE) in an Azure VNet that reaches the proxy over ExpressRoute/VPN; the MPE targets
that PLS. Spark config is identical to §2 — only the private FQDN resolves through the forwarder.

```bash
# on the Azure forwarding VM (example): DNAT :9000 to the on-prem/other-cloud proxy
sudo sysctl -w net.ipv4.ip_forward=1
sudo iptables -t nat -A PREROUTING -p tcp --dport 9000 -j DNAT --to-destination <proxy-ip>:9000
sudo iptables -t nat -A POSTROUTING -p tcp -d <proxy-ip> --dport 9000 -j MASQUERADE
```
Put the internal LB + PLS in front of this VM's NIC, then follow §2 B2–B4.

---

## 4. Storage‑proxy mounts (passthrough scenarios)

For scenarios that serve **files/objects** (NAS, MinIO/S3, Azure Blob/ADLS) rather than a DB,
enable the storage proxy and add a mount. Turn it on (`ENABLE_STORAGE_PROXY=1`) and create
`config.mounts.json` (gitignored). Each mount **bucket must differ** from the DB warehouse
bucket, and upstream secrets live in the **credential store by id**, never inline:

```json
{
  "mounts": [
    { "bucket": "secure-nfs", "backend": "local", "root": "/mnt/finance", "read_only": true },
    { "bucket": "s3vault", "backend": "s3", "root": "reports-bucket", "prefix": "2026/",
      "endpoint": "https://minio.local:9000", "region": "us-east-1",
      "addressing_style": "path", "credential": "s3vault", "read_only": true },
    { "bucket": "blobvault", "backend": "azure", "root": "reports",
      "account": "mystorageacct", "credential": "blobvault", "read_only": true }
  ]
}
```
Store the upstream credential encrypted via the config builder **Storage** tab, or the API
(`POST /_config/api/s3-credentials` / `/_config/api/azure-credentials`). A credential‑less mount
must set `"auth"` (`anonymous`/`instance` for s3; `default`/`managed_identity`/`anonymous` for
azure). Scope Fabric's access with per‑key ACLs and keep `ENFORCE_MOUNT_AUTH=1` (default) so a
mount is never served anonymously. See [SECURITY.md](SECURITY.md) and
[../config.mounts.example.json](../config.mounts.example.json).

Then create the Fabric shortcut against the **mount bucket** (e.g. `secure-nfs`) exactly as in
§1 (OPDG) — the connectivity pattern is the same; only the bucket changes.

---

## 5. Scenario → setup recipe

Maps each row of the use‑case matrix in [UsecasesAndScenarios.md](UsecasesAndScenarios.md) to the
sections above.

| # | Scenario | Data path | Private‑link setup | Key proxy config |
|---|---|---|---|---|
| 1 | On‑prem OLTP as a live table [Shortcut] | DB→Delta | [§1](#1-pattern-a--shortcut-via-on-premises-data-gateway-private) OPDG on the LAN | `require_sigv4`, `table_format: delta`, `config.connection/tables.json` |
| 2 | On‑prem NAS files [Shortcut] | Passthrough (local NFS/SMB) | [§1](#1-pattern-a--shortcut-via-on-premises-data-gateway-private) OPDG | [§4](#4-storage-proxy-mounts-passthrough-scenarios) `backend: local` mount |
| 3 | AWS‑resident data [Shortcut] | Passthrough (MinIO/S3) or DB (RDS) | [§1](#1-pattern-a--shortcut-via-on-premises-data-gateway-private) OPDG **inside the VPC** | [§4](#4-storage-proxy-mounts-passthrough-scenarios) `backend: s3` + credential id |
| 4 | vNet Spark over private Azure DB [Spark] | DB→Delta | [§2](#2-pattern-b--spark-via-managed-private-endpoint--private-link-service-in-vnet) MPE→PLS | `require_sigv4`, `forwarded_allow_ips` |
| 5 | vNet Spark over private ADLS/MinIO [Spark] | Passthrough | [§2](#2-pattern-b--spark-via-managed-private-endpoint--private-link-service-in-vnet) MPE→PLS | [§4](#4-storage-proxy-mounts-passthrough-scenarios) `azure`/`s3` mount |
| 6 | Air‑gapped Spark over on‑prem Oracle [Spark] | DB→Delta | [§3](#3-pattern-c--spark-for-on-prem--other-cloud-sources) forwarding VM + PLS | Oracle URL + `[oracle]` extra |
| 7 | Multi‑cloud mesh, one endpoint [Shortcut + Spark] | Many mounts + DB | [§1](#1-pattern-a--shortcut-via-on-premises-data-gateway-private) OPDG per site + [§2](#2-pattern-b--spark-via-managed-private-endpoint--private-link-service-in-vnet) MPE | many mounts + **per‑key ACLs** per domain |
| 8 | Cross‑tenant / regulated [Shortcut] | DB or passthrough | [§1](#1-pattern-a--shortcut-via-on-premises-data-gateway-private) OPDG (+ OneLake Private Link) | TLS + audit on |

---

## 6. Verify

```bash
# Proxy is healthy and the source is reachable (run on the proxy host)
curl -s http://127.0.0.1:9000/healthz            # {"status":"ok"}
curl -s http://127.0.0.1:9000/readyz             # ready when snapshots built + DB reachable

# Shortcut path (Pattern A): from the OPDG host, the port is reachable
curl -s http://<proxy-private-ip>:9000/healthz

# Spark path (Pattern B/C): the MPE connection is Approved on the PLS, then run the B4 snippet
az network private-link-service show -g rg-proxy -n fsp-pls \
  --query 'privateEndpointConnections[].privateLinkServiceConnectionState.status' -o tsv
```
A resolving Fabric shortcut / a Spark `df.show()` returning rows confirms the path end‑to‑end.
For failures, see [installation/Linux_Deployment.md](installation/Linux_Deployment.md) §14 and
[LINUX_MANAGER_TROUBLESHOOTING.md](LINUX_MANAGER_TROUBLESHOOTING.md).

---

## 7. Where to go next

- **Concepts & scenario rationale:** [UsecasesAndScenarios.md](UsecasesAndScenarios.md)
- **Install baselines:** [installation/Linux_Deployment.md](installation/Linux_Deployment.md) · [installation/Windows_Deployment.md](installation/Windows_Deployment.md)
- **Public TLS (nginx):** [../SSL_Deployment.md](../SSL_Deployment.md)
- **Security (SigV4/ACL/credential store/TLS/audit):** [SECURITY.md](SECURITY.md)
- **All settings:** [CONFIGURATION.md](CONFIGURATION.md)
- **Everything in one place:** [../FAQ.md](../FAQ.md)
