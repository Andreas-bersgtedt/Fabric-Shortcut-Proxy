---
name: fsp-tokenization
description: "Configure and operate Fabric Shortcut Proxy central tokenization policies. Use for durable or random tokens, tokenization keys, column policy assignments, Arrow fallback, key rotation, tokenization UAT, and tokenization authorization."
argument-hint: "Describe the source dialect, columns to protect, token behavior, policy/key identifiers, and deployment environment."
---

# Fabric Shortcut Proxy Tokenization

## Use When

- Creating or changing a durable or random tokenization policy.
- Storing a tokenization key or assigning a policy to a table, mount, or Open Mirror target.
- Enabling Arrow fallback for a source without native tokenization.
- Rotating a tokenization key or investigating a tokenization startup failure.
- Running SQL Server, PostgreSQL, Oracle, Databricks, or Open Mirror tokenization UAT.

## Policy Model

Central policies are stored in `config.tokenization.json` by default. Set
`TOKENIZATION_POLICY_FILE` to use another path. The catalog contains no key values.

A table-side selection contains one of these actions:

- `keep`: retain the source column.
- `remove`: omit the column from output.
- `durable_token`: select an enabled durable policy.
- `random_token`: select an enabled random policy.

A durable policy supplies `sha256`, a versioned key reference, a domain, and a
normalization rule. A random policy has no key reference. The policy kind must
match the selection action.

## Safe Workflow

1. Choose a policy id and versioned key reference, such as `customer-pii-v1`.
2. In Config Builder **Security**, save the key under **Tokenization keys**. The
   credential store encrypts it; the browser does not show the value afterward.
3. Create the durable or random policy under **Tokenization policies**.
4. In **Tables** or **Open Mirroring**, open the column editor and select the
   approved policy. Keep split keys. Remove source fields that consumers do not need.
5. Restart the Manager after a policy change, then check `/readyz` and one
   published object or mirror job.

Policy administration requires `tokenization.policy.admin`. Policy listing
requires `tokenization.policy.read`. Key administration requires
`security.credentials.admin`.

## Native and Arrow Execution

SQL Server, PostgreSQL, Oracle, and Databricks SQL run native token expressions.
PostgreSQL needs `pgcrypto`. Unsupported native paths fail closed by default.

Set `TOKENIZATION_FALLBACK=arrow` only when proxy-side processing is acceptable.
Arrow reads selected plaintext values into proxy memory before Parquet encoding.
Start with `STREAM_BATCH_ROWS=100000` or lower and one fallback materialization
per Agent. Monitor Agent RSS, source-query latency, and materialization duration.
Set `TOKENIZATION_FALLBACK=none` and restart when a memory threshold or source
query timeout is reached.

## Rotation and Removal

Never replace a durable key in place. Create a new key reference and policy, for
example `customer-pii-v2`, then publish old and new token columns when consumers
need historical joins. Disabling a policy preserves its historical identity but
prevents new assignments and generations.

Random tokens change on every read. Do not use them with `content_hash` refresh,
or with `auto` plus `REFRESH_ALLOW_FULL_PULL=1`; startup rejects that combination.

## Validation

- SQL Server: [TOKENIZATION_UAT.md](../../docs/TOKENIZATION_UAT.md)
- PostgreSQL, Oracle, Databricks: [TOKENIZATION_MULTI_DIALECT_UAT.md](../../docs/TOKENIZATION_MULTI_DIALECT_UAT.md)
- Open Mirror: [TOKENIZATION_OPEN_MIRROR_UAT.md](../../docs/TOKENIZATION_OPEN_MIRROR_UAT.md)

Verify that normalized equal input creates equal durable tokens, random tokens
change on repeat reads, null stays null, removed columns do not reach output, and
logs contain no key values.

## References

- [Tokenization policy manual](../../docs/manual/14-tokenization-policies.md)
- [Config Builder guide](../../docs/CONFIG_BUILDER_GUIDE.md)
- [Tokenization design and rollout limits](../../docs/TOKENIZATION_PUSHDOWN.md)
- [Security policy](../../docs/SECURITY.md)
