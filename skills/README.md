# Fabric Shortcut Proxy Agent Skills

These project skills provide on-demand operational workflows for the Fabric Shortcut Proxy.
Load the skill that matches the task:

- [Rapid POC](./fsp-rapid-poc/SKILL.md): deploy a quick Lite or local Kind proof, or validate a Helm-based AKS environment.
- [Deployment](./fsp-deployment/SKILL.md): install locally or deploy the Bicep and Helm enterprise topology to AKS.
- [Infrastructure prerequisites](./fsp-infrastructure-prerequisites/SKILL.md): prepare parameterized Bicep inputs, AKS, Key Vault, workload identity, storage, and private networking.
- [Source connectivity](./fsp-source-connectivity/SKILL.md): install database drivers and validate source reachability and authentication.
- [Management](./fsp-management/SKILL.md): operate the Manager/Agent fleet, health endpoints, scaling, and rollout controls.
- [Configuration](./fsp-configuration/SKILL.md): configure connections, tables, mounts, security, and the Config Builder.
- [Tokenization](./fsp-tokenization/SKILL.md): manage central policies, keys, assignments, Arrow fallback, rotation, and UAT.
- [Troubleshooting](./fsp-troubleshooting/SKILL.md): diagnose startup, readiness, connectivity, authentication, and data-path failures.

The repository's `docs/` manuals remain authoritative for detailed reference material. For
enterprise AKS, the executable source of truth is
[infra/fsp-demo/README.md](../infra/fsp-demo/README.md), and chart behavior is documented in
[deploy/helm/fabric-shortcut-proxy/README.md](../deploy/helm/fabric-shortcut-proxy/README.md).
These skills are concise task procedures and should link to those sources instead of duplicating
every value.
