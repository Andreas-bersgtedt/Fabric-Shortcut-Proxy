# Chapter 14: Tokenization Policies

A central tokenization policy defines approved token behavior. Tables, mounts,
and Open Mirror targets select a policy by id; they do not carry an algorithm,
key value, domain, or normalization rule.

## 14.1 Policy types

A selection is one of four actions:

| Action | Result |
| --- | --- |
| Keep | Copy the source column to output |
| Remove | Omit the source column from output |
| Durable token | Replace non-null values with a stable token from a durable policy |
| Random token | Replace each non-null value with a fresh token from a random policy |

Durable policies use a key reference. Their tokens preserve equality within the
same key, domain, and normalization policy. Random policies preserve no equality
relationship and are incompatible with content-hash refresh.

## 14.2 Create a central policy

Use the Config Builder **Security** area. Creating or changing a policy requires
`tokenization.policy.admin`; listing policies requires `tokenization.policy.read`.
The builder writes the secret-free catalog to `config.tokenization.json` by default.
Set `TOKENIZATION_POLICY_FILE` to use another path.

A durable policy needs a stable id, `sha256`, a key reference, domain, and
normalization rule. A random policy has no key reference.

```json
{
  "policies": [
    {
      "policy_id": "customer-pii-v1",
      "kind": "durable_token",
      "algorithm": "sha256",
      "key_ref": "customer-pii-v1",
      "domain": "customer-email",
      "normalization": "trim_lower",
      "digest_size": 32,
      "framing_version": 1,
      "enabled": true
    },
    {
      "policy_id": "support-note-random-v1",
      "kind": "random_token",
      "algorithm": "sha256",
      "normalization": "none",
      "digest_size": 32,
      "framing_version": 1,
      "enabled": true
    }
  ]
}
```

Policy files reject secret fields such as `key`, `secret`, and `token_key`.
Store the durable key in **Security > Tokenization keys**, the encrypted
credential store, or `FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1`. The policy catalog
contains only the logical `key_ref`.

## 14.3 Assign a policy

In the Config Builder table or Open Mirror column editor, select **Durable token**
or **Random token**, then select an approved policy of the matching kind. The
stored selection contains only the action and policy id.

```json
{
  "name": "email_token",
  "source": "email",
  "type": "string",
  "tokenization": {
    "action": "durable_token",
    "policy_id": "customer-pii-v1"
  }
}
```

A policy must be enabled and its kind must match the selected action. The proxy
rejects a transform on a split key, a missing durable key, or a source dialect
without native support unless Arrow fallback is explicitly enabled.

## 14.4 Lifecycle and rotation

Disable a policy instead of deleting it when existing snapshots or configuration
need its historical identity. Disabled policies cannot be assigned or resolved
for a new generation.

Never rotate a durable key in place. Create a versioned key reference and policy,
for example `customer-pii-v2`, then publish an explicit migration with old and
new token columns if consumers need to join historical snapshots. Changing the
key changes every non-null durable token.

## 14.5 Native and Arrow execution

The proxy uses native SQL tokenization for SQL Server, PostgreSQL, Oracle, and
Databricks SQL. PostgreSQL requires `pgcrypto`. Unsupported native paths fail
closed unless `TOKENIZATION_FALLBACK=arrow` is set. Arrow fallback processes
plaintext values in proxy memory, so begin at `STREAM_BATCH_ROWS=100000` or lower
and one fallback materialization per Agent. See [Chapter 8](08-operations.md).

## 14.6 Verify a policy

For durable tokens, verify that equal normalized values produce the same
64-character uppercase SHA-256 token and that null input remains null. Re-read
with the same key to confirm stability. Change to a versioned test key to confirm
all non-null durable tokens change.

For random tokens, verify that each non-null output is replaced on a second read.
Confirm that source values and removed columns do not appear in Parquet, metadata,
or logs. Run [TOKENIZATION_UAT.md](../TOKENIZATION_UAT.md) for SQL Server and
[TOKENIZATION_MULTI_DIALECT_UAT.md](../TOKENIZATION_MULTI_DIALECT_UAT.md) for
PostgreSQL, Oracle, and Databricks.

## 14.7 Next

Return to [Chapter 8: Operations](08-operations.md) to monitor publication and
configure retention, or to the [manual index](README.md) for the full workflow.
