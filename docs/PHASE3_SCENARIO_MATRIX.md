# Phase 3 failure-scenario matrix

These tests exercise deterministic control-plane behavior. They do not replace
a failure exercise against a running non-production Kubernetes estate.

| Scenario | Test | Expected result |
| --- | --- | --- |
| Primary region unavailable | `tests/enterprise/test_materializer_placement.py::test_explicit_fallback_is_used_only_after_primary_is_unavailable` | Work is assigned to the configured fallback pool. An eligible but unauthorized pool is not selected. |
| Site disconnects during work | `tests/enterprise/test_phase0_routed_poc.py::test_heartbeat_loss_requeues_rejects_stale_result_and_publishes_once` | The disconnected Agent's claim is reassigned with a new claim token. Its stale result is rejected; the replacement completes and publishes the request once. |
| Credential rotation overlap | `tests/enterprise/test_agent_auth.py::test_active_and_unexpired_previous_tokens_are_accepted` | Manager accepts the active token and the previous token before its configured expiry. |
| Expired credential | `tests/enterprise/test_agent_auth.py::test_previous_token_is_rejected_at_deadline` | Manager rejects the previous token at its expiry deadline without registering the Agent. |
| Entra workload identity | `tests/test_entra_agent_auth.py` | Signed, identity-bound tokens are accepted; wrong issuer, audience, tenant, role, client, principal, lifetime, signature, and Agent identity are rejected. Static/Basic credentials cannot bypass Entra mode; a JWKS outage fails closed. |
| Rolling restart | `tests/enterprise/test_phase5_ha.py::test_rolling_restart_is_strictly_sequential`, `test_rolling_restart_deregisters_before_stop`, `test_rolling_restart_health_gate_times_out`, and `test_rolling_restart_reports_durable_progress_events` | Agents restart one at a time, leave rotation before stopping, and must pass the health gate before the next Agent restarts. A health timeout is reported as unsuccessful. |
| Protocol version skew | `tests/enterprise/test_control_plane.py::test_register_rejects_incompatible_contract_major` and `tests/enterprise/test_materializer_placement.py::test_legacy_agent_can_only_receive_unpinned_work` | Manager rejects an incompatible contract major. A legacy Agent does not receive work requiring newer placement contract fields. |

Run the matrix tests from the repository root:

```powershell
pytest `
  tests/enterprise/test_materializer_placement.py::test_explicit_fallback_is_used_only_after_primary_is_unavailable `
  tests/enterprise/test_phase0_routed_poc.py::test_heartbeat_loss_requeues_rejects_stale_result_and_publishes_once `
  tests/enterprise/test_agent_auth.py::test_active_and_unexpired_previous_tokens_are_accepted `
  tests/enterprise/test_agent_auth.py::test_previous_token_is_rejected_at_deadline `
  tests/test_entra_agent_auth.py `
  tests/enterprise/test_phase5_ha.py::test_rolling_restart_is_strictly_sequential `
  tests/enterprise/test_phase5_ha.py::test_rolling_restart_deregisters_before_stop `
  tests/enterprise/test_phase5_ha.py::test_rolling_restart_health_gate_times_out `
  tests/enterprise/test_phase5_ha.py::test_rolling_restart_reports_durable_progress_events `
  tests/enterprise/test_control_plane.py::test_register_rejects_incompatible_contract_major `
  tests/enterprise/test_materializer_placement.py::test_legacy_agent_can_only_receive_unpinned_work
```

## Non-production coverage and remaining checks

The tests above use in-process registries, queues, and fake Agent supervisors.
They do not verify WAN failure detection, Kubernetes/CNI behavior, source
connectivity, or a real credential rollout. Record the cluster, chart revision,
scenario, timestamps, observed queue and fleet state, and recovery result when
the scenarios are exercised in non-production.

The two-region acceptance estate has exercised dataset move, delivered drain,
primary Pod loss, fallback publication, and primary recovery. The sanitized
results are recorded in [PHASE3_OPERATIONS_RUNBOOK.md](./PHASE3_OPERATIONS_RUNBOOK.md).
Primary Pod loss is not a whole-region outage. Controlled local disconnect,
rotation, and version-skew tests are not proof of a live WAN partition or live
credential renewal. Keep those distinctions in the final acceptance record.

The Entra rollout repeated primary, delivered drain, fallback, and recovery
publication, and verified positive/negative authentication at both sites.
The live projected-credential rotation observation is pending. The actual
Azure Identity expiry/file-reload path is covered by
`test_azure_sdk_renews_expired_token_and_reads_rotated_federation`; its AAD
client is mocked, so this test is not evidence of a live token exchange.
