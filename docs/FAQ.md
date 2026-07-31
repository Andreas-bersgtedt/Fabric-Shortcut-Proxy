# Frequently Asked Questions

## Is this a side project or a product that will become part of Microsoft Fabric?

This project was created to solve a specific customer problem and has since been released as an open-source project. It is not currently a committed Microsoft Fabric product or product-development effort.

Treat it as community-supported reference code and a proof of concept unless a separate support and ownership model is established for a deployment.

## What does the Fabric Shortcut Proxy do?

The proxy presents data from a relational database to Microsoft Fabric through an S3-compatible endpoint. Fabric sees a shortcut-readable Iceberg or Delta table, while the proxy executes bounded SQL queries against the source and generates the required Parquet data.

The source database remains the system of record. The proxy is a read-path virtualization layer; it is not a general query engine, database, or full S3 implementation.

## Can it also serve existing files or object storage, not just databases?

Yes. Alongside the database-to-table virtualization, the same S3 endpoint can act as a **secured storage proxy**: a **mounted** bucket streams existing files straight from a storage backend as **read-only byte passthrough**, while every other bucket (including the database warehouse) resolves through the Iceberg/Delta path unchanged.

This is **additive**: a single deployment can expose the relational warehouse *and* file shares/object stores at once, behind one authenticated front door. It is enabled per bucket through a mount table (`config.mounts.json`) or the config-builder **Storage** tab, and is off by default (`ENABLE_STORAGE_PROXY`).

## Which storage backends can the proxy front?

Three, all served read-only with ranged reads and one-level folder browsing:

| Backend | Serves | Notes |
| --- | --- | --- |
| `local` | a filesystem path | an OS-mounted **NFS/SMB** share (UNC path or mount point); no extra dependencies |
| `s3` | a native **S3 / MinIO / S3-compatible** bucket | ranged streaming + list pagination |
| `azure` | an **Azure Blob / ADLS Gen2** container | flat blob and hierarchical namespace |

Upstream credentials are **mediated**: clients never see them. They are held encrypted (DPAPI on Windows, Fernet elsewhere) and resolved by id. Outbound S3 supports static keys, session tokens, assume-role, web-identity (OIDC/IRSA), profiles, SSO, instance role, credential-process, and anonymous; Azure supports connection string, account key, SAS, service principal, managed identity, DefaultAzureCredential, and anonymous.

## How is access to mounted buckets secured?

The front door verifies AWS SigV4 against **scoped access keys**, not just one static pair. Each key is authorized to specific buckets/prefixes and is read-only. Mounted buckets are authenticated even when global signature enforcement is off (`ENFORCE_MOUNT_AUTH`, default on), so a secured mount is never served anonymously. Keys are managed in the config-builder **Storage → Access keys** panel; the legacy single key remains a wildcard until the first scoped key is created.

Transport can be secured with TLS at the proxy (`TLS_CERT_FILE` / `TLS_KEY_FILE`) or a fronting load balancer, and every mounted-object access (identity, bucket, key, bytes) is written to an audit log. Details are in [SECURITY.md](SECURITY.md) and [CONFIGURATION.md](CONFIGURATION.md) §14.

## How are source-data changes tracked?

The proxy publishes versioned Iceberg snapshots or Delta commits. Data is divided into deterministic splits, and each split is identified by a hash of its logical row content. When a split changes, it receives a new object path and the proxy publishes updated table metadata. Unchanged splits retain their existing paths.

Refresh can be manual or automatic. Automatic refresh can use database-specific change indicators where available; otherwise, detecting in-place updates requires rereading and hashing source data. This means freshness is bounded by the configured polling interval plus any Fabric shortcut or endpoint synchronization delay.

This is snapshot-level change detection, not a replacement for a source-native CDC stream. The current scale roadmap identifies watermark- or CDC-based incremental refresh as future work.

## How does this compare with Open Mirroring?

Open Mirroring and this proxy solve different problems:

| Area | Fabric Shortcut Proxy | Open Mirroring |
| --- | --- | --- |
| Primary pattern | Virtualized read access through a shortcut | Replication into OneLake |
| Data movement | Generates or serves Parquet when required, with optional materialization and caching | Continuously lands replicated changes as managed mirrored data |
| Source impact | Executes read queries against the source | Requires a producer or connector to publish changes |
| Freshness | Poll-, probe-, or manually triggered; not instant | Designed for incremental change delivery |
| Storage | Source remains authoritative; proxy artifacts may be cached or materialized | A durable copy is maintained in OneLake |
| Operations | Customer-managed open-source service | Fabric-managed capability, with source-side integration still required |

Use Open Mirroring when a supported, durable replication path and incremental freshness are the priority. The proxy is more relevant when the requirement is to expose an existing relational source through a Fabric shortcut without first building a conventional ingestion or replication pipeline.

## If the proxy uses SQL pushdown, how is it different from a Data Factory connector?

A Data Factory connector is used by a pipeline to copy or transform data into a destination. It is scheduled or event-driven ingestion: the pipeline owns movement, retries, checkpoints, and the persisted destination data.

The proxy participates in Fabric's table read path. A request for a virtual Parquet object is mapped to a bounded SQL query, converted to Parquet, and returned through an S3-compatible interface. It presents table metadata and files that Fabric can consume through a shortcut.

The key distinction is therefore not simply whether SQL is pushed to the source. Data Factory is an ingestion and orchestration model; this project is a shortcut and data-access virtualization model. For predictable production ingestion, complex transformations, or durable copies, Data Factory is generally the more conventional choice.

## Where should the agent run in a production-grade setup?

There is not yet a fully supported production deployment profile. A production design should run the service in customer-managed compute with network access to both Fabric and the source database.

The target architecture separates the Manager control plane from stateless Agents. Agents should run behind a load balancer or reverse proxy, use a shared artifact store, and scale horizontally. The Manager owns configuration, published table state, refresh orchestration, and agent supervision.

A production deployment would also need, at minimum:

- HTTPS/TLS termination and restricted network access. *(TLS termination at the proxy is available via `TLS_CERT_FILE`/`TLS_KEY_FILE`, or terminate at a load balancer.)*
- Strong S3 request authentication and read-only source credentials. *(Scoped, per-tenant SigV4 access keys with bucket/prefix ACLs are available; enable `REQUIRE_SIGV4` and/or `ENFORCE_MOUNT_AUTH`.)*
- Secrets stored outside configuration files. *(An encrypted credential store holds DB URLs, upstream S3/Azure credentials, and access keys.)*
- A durable shared artifact and state store.
- Monitoring, capacity limits, backups, upgrade procedures, and high-availability planning.

The repository contains pieces of this architecture, but its roadmap still lists production transport security, incremental refresh at scale, and additional control-plane hardening as ongoing or future work. Production use therefore requires an owning engineering team to validate, operate, secure, and support it.

## What is the current overall position?

The project demonstrates a technically viable pattern and includes substantial implementation for Iceberg and Delta metadata, SQL-to-Parquet generation, caching, freshness, monitoring, a Manager/Agent model, and a secured storage proxy (local/S3/Azure passthrough with per-key authorization, TLS, and audit). It should still be positioned as open-source reference code rather than a managed Fabric capability.

The clearest description is: **a customer-operated virtualization gateway that makes relational data appear as shortcut-readable table objects in Fabric without requiring a conventional copy pipeline first.**
