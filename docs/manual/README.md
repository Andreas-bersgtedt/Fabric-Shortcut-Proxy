# Fabric Shortcut Proxy — User Manual

Version 2.1.1

This manual is the end-to-end guide to installing, configuring, and operating the
Fabric Shortcut Proxy. It is organized into chapters (files) that each cover one
part of the system, and modules (sections) within a chapter. Read it in order for
a first deployment, or jump to a chapter for a specific task.

The manual stands on its own for day-to-day work. When a topic has a deeper design
or reference document elsewhere in `docs/`, the chapter links to it instead of
duplicating it.

## Who this is for

- Operators standing up the proxy next to a SQL Server, PostgreSQL, Oracle, or
  Databricks source, or in front of an existing file share or object store.
- Administrators wiring Microsoft Fabric shortcuts to on-premises or private-network
  data without copying it into OneLake.
- Engineers who need to understand the request path, the split model, and the
  snapshot lifecycle before tuning or extending a deployment.

## Chapters

| # | Chapter | What it covers |
|---|---|---|
| 1 | [Introduction](01-introduction.md) | What the proxy does, the two serving modes, editions, and where it fits |
| 2 | [Core concepts](02-concepts.md) | S3 front door, warehouse vs mount, Iceberg/Delta output, splits, snapshots, freshness, dialects |
| 3 | [Architecture](03-architecture.md) | Request lifecycle, module map, data plane vs control plane, Manager/Agent |
| 4 | [Installation](04-installation.md) | Prerequisites, getting the code, virtual environment, drivers, Lite vs cluster |
| 5 | [Configuration](05-configuration.md) | Settings model, the config files, the table registry, multi-connection sources |
| 6 | [Connecting Microsoft Fabric](06-connectivity.md) | Shortcut setup, OPDG and public patterns, storage-proxy mounts |
| 7 | [Security](07-security.md) | Credentials, SigV4 keys and ACL, credential mediation, TLS, audit, tokenization |
| 8 | [Operations](08-operations.md) | Running the service, endpoints, monitoring, freshness, scaling, troubleshooting |
| 9 | [Reference](09-reference.md) | Settings groups, dialect matrix, path formats, launcher flags, glossary |
| 10 | [Tutorials](10-tutorials.md) | End-to-end worked examples: demo, SQL Server shortcut, file-share mount, tokenized column |

## How to use this manual

- **First deployment:** read chapters 1 through 6 in order, then chapter 7 before
  exposing anything beyond a lab.
- **Prefer a worked example:** jump to [chapter 10](10-tutorials.md) and follow a tutorial
  end to end, referring back to the chapter each step cites.
- **Adding a source or table:** chapter 5, then chapter 6.
- **Tuning or scaling:** chapters 2 and 8.
- **Hardening:** chapter 7.

## Conventions

- Commands are shown for Windows PowerShell and Linux/macOS Bash where they differ.
  The Windows launcher is `Manager.ps1`; the Linux launcher is `Manager.sh`.
- Connection strings, keys, and hostnames in examples are placeholders. Replace
  them, and never commit real secrets.
- `db/<server>/<database>/<schema>/<object>` is the canonical object path. Substitute
  your own source identity where you see it.

## Related design and reference documents

The manual links these where relevant; they carry the full detail behind a topic.

- [CONFIGURATION.md](../CONFIGURATION.md) — complete PostgreSQL/SQL Server configuration reference
- [TechnicalArchitecture.md](../TechnicalArchitecture.md) — component-level flow diagrams
- [SECURITY.md](../SECURITY.md) — authentication, TLS, and audit policy
- [DELTA_FORMAT.md](../DELTA_FORMAT.md) — native Delta output mode
- [TOKENIZATION_PUSHDOWN.md](../TOKENIZATION_PUSHDOWN.md) — column tokenization design
- [CONNECTIVITY_SETUP.md](../CONNECTIVITY_SETUP.md) — network patterns (OPDG, Private Link)
- [UsecasesAndScenarios.md](../UsecasesAndScenarios.md) — connectivity scenarios
- [installation/Windows_Deployment.md](../installation/Windows_Deployment.md) and [installation/Linux_Deployment.md](../installation/Linux_Deployment.md) — host-specific baselines
- [FAQ.md](../FAQ.md) — frequently asked questions and quick answers
