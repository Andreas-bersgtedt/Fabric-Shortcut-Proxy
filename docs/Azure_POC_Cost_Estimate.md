# Azure POC cost estimate

This note estimates the monthly cost of the FSP AKS proof-of-concept deployment from public Azure retail prices. It excludes the source SQL deployment and Fabric capacity because those are treated as existing infrastructure.

Assumptions:

- Currency: USD.
- Month length: 730 hours.
- Pay-as-you-go public retail rates.
- No reservations, savings plans, Azure Hybrid Benefit, taxes, support, or outbound bandwidth charges.
- Low data transfer and low operations volume for ACR, disks, private endpoints, and load balancers.
- The OPDG VM is shown separately because it can be considered shared gateway infrastructure rather than part of the FSP stack.

## Resource inventory used

| Area | Observed shape | Pricing basis |
| --- | --- | --- |
| AKS control plane | Free tier | No hourly control-plane charge. |
| AKS system nodes | 2 x `Standard_B2s` Linux | `$0.0432/hour` each. |
| AKS app nodes | 2 x `Standard_B2s` Linux | `$0.0432/hour` each. |
| Jump/admin VM | 1 x `Standard_D4s_v3` Linux | `$0.204/hour`. |
| OPDG VM | 1 x `Standard_D4s_v3` Windows | `$0.388/hour`; shown separately. |
| Azure NetApp Files | 4 TiB Standard pool | `$0.000202/GiB/hour`. |
| ACR | Premium registry | `$1.6666/day` registry unit, plus storage. |
| Disks | 128 GB Standard HDD OS disk, 8 GB Standard SSD PVC | S10 LRS and E3 LRS disk meters. |
| Private endpoints | AKS API and ACR for FSP stack | Estimated at `$0.01/hour` each. |
| Public IPs | Standard static IPs | `$0.005/hour` each. |
| Internal load balancer and private DNS | Low traffic | Small meter; rounded into network allowance. |

## Baseline estimate, one smoke agent

| Cost item | Quantity | Monthly estimate |
| --- | ---: | ---: |
| AKS B2s nodes | 4 | `$126` |
| Jump/admin VM | 1 | `$149` |
| Azure NetApp Files Standard | 4 TiB | `$604` |
| ACR Premium | 1 registry | `$51` |
| Managed disks | jump OS + Manager PVC | `$7` |
| Private endpoints | AKS API + ACR | `$15` |
| Public/static IPs | jump + AKS-related IPs | `$7` |
| Load balancer, private DNS, low operations | allowance | `$0-$10` |
| **FSP stack subtotal** | SQL and OPDG excluded | **about `$960/month`** |

If the OPDG VM is counted as part of the test, add about `$293/month`:

| OPDG item | Quantity | Monthly estimate |
| --- | ---: | ---: |
| Windows `Standard_D4s_v3` VM | 1 | `$283` |
| Standard HDD OS disk | 1 | `$6` |
| Standard static public IP | 1 | `$4` |
| **OPDG add-on** | optional | **about `$293/month`** |

Baseline with OPDG counted:

```text
FSP stack only:       about $960/month
FSP stack + OPDG VM:  about $1,250/month
```

## Scale scenarios

Agent count means registered FSP data-plane/materializer pods. Pod count by itself does not create Azure cost. Cost rises when node count, storage size, registry tier, or network resources change.

These scenarios keep the same ANF pool, ACR, Manager, jump host, private endpoints, and public IPs. Only the AKS app-node count changes.

| Scenario | AKS nodes | Agent assumption | FSP stack estimate | With OPDG counted |
| --- | ---: | --- | ---: | ---: |
| Current smoke | 2 system + 2 app = 4 B2s | 1 Python enterprise agent | about `$960/month` | about `$1,250/month` |
| 5-agent light scale | 2 system + 3 app = 5 B2s | About 1-2 agents per app node | about `$990/month` | about `$1,280/month` |
| 10-agent light scale | 2 system + 5 app = 7 B2s | About 2 agents per app node | about `$1,055/month` | about `$1,345/month` |

The B2s node increment is about `$31.54/month` per node:

```text
0.0432 USD/hour * 730 hours = 31.536 USD/month
```

### Higher-throughput scale option

If agents need stronger CPU or memory isolation, use larger app nodes instead of packing more agents onto B2s nodes. Example impact if only the app pool changes:

| App pool option | App nodes | App compute/month | Total AKS compute/month | FSP stack estimate |
| --- | ---: | ---: | ---: | ---: |
| Current app pool | 2 x B2s | `$63` | `$126` | about `$960/month` |
| 5-agent D4s app pool | 3 x D4s v3 Linux | `$447` | `$510` | about `$1,345/month` |
| 10-agent D4s app pool | 5 x D4s v3 Linux | `$745` | `$808` | about `$1,645/month` |

D4s v3 Linux calculation:

```text
0.204 USD/hour * 730 hours = 148.92 USD/month per node
```

Use this option only if the B-series burstable nodes throttle during materialization, Parquet generation, or S3 range-read tests.

## Cost drivers

The main monthly cost driver is Azure NetApp Files:

```text
4 TiB * 1024 GiB/TiB * 0.000202 USD/GiB/hour * 730 hours = about $604/month
```

Next largest items are the optional Windows OPDG VM, the jump/admin VM, AKS nodes, and ACR Premium.

| Driver | Why it matters | Reduction option |
| --- | --- | --- |
| Azure NetApp Files 4 TiB minimum | Dominates the test stack cost. | Use a smaller supported RWX option if tenant policy allows it, or delete ANF between test windows. |
| OPDG Windows VM | Windows VM meter is higher than Linux. | Stop/deallocate when not testing. |
| Jump/admin VM | Always-on D4s v3 Linux VM. | Stop/deallocate when not testing. |
| AKS node pools | Four always-on B2s nodes in the current test. | Scale user pool down, stop AKS when idle, or reduce system/user nodes for short tests. |
| ACR Premium | Required for private endpoint/private link registry pattern. | Downgrade only if private endpoint requirements are relaxed. |

## What to stop first when idle

1. Stop/deallocate the OPDG VM if it is only used for this test.
2. Stop/deallocate the jump/admin VM when no one needs tunnels, builds, or kubectl from that host.
3. Stop AKS or scale user node pools down when agents are not being tested.
4. Delete the temporary internal LoadBalancer service if it is still pending or unused.
5. Delete the ANF pool only after artifact persistence is no longer needed; this removes the largest monthly line item.

## Notes

- Source SQL cost is excluded.
- Fabric capacity and OneLake storage are excluded.
- Private endpoint estimates include the FSP stack endpoints, not all private endpoints that may exist in the shared admin VNet.
- The temporary smoke DNS record that points to a pod IP has no meaningful cost beyond private DNS zone charges, which are negligible for this test.
- Actual Azure invoices can differ because of regional meter changes, included allowances, data transfer, reservations, negotiated pricing, and taxes.
