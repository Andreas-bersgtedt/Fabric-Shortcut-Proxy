# FSP Prometheus and Grafana assets

`fsp-phase3-dashboard.json` is a Grafana dashboard for the FSP Phase 3
telemetry. Import it and select the Prometheus data source that scrapes the
Manager and materializer Pods.

`fsp-phase3-alerts.yaml` defines five Prometheus Operator alerts. Apply it only
when the `monitoring.coreos.com/v1` `PrometheusRule` CRD is installed and the
Prometheus resource selects this namespace and rule. Change the namespace or
required Prometheus labels to match the monitoring installation.

## Scrape targets

The chart annotates Manager Pods on port 9200 and materializer Pods on port
9000 with the path `/metrics`. Configure the scraper to discover those Pod
annotations. When chart NetworkPolicies are enabled, set
`networkPolicy.metricsScraper.namespaceSelector` and
`networkPolicy.metricsScraper.podSelector` to the scraper's namespace and Pod
labels. Manager metrics report queue depth, assignment rejections, and Agent
heartbeat age. Materializer metrics report source query latency and successful
artifact upload bytes and elapsed duration.

For a multi-site deployment, scrape materializer Pods in each site into a
Prometheus-compatible backend that retains the `location` label. Scraping only
the central Manager omits source-query and upload metrics from remote
materializers.

## Metric definitions

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `fsp_materialization_queue_depth` | Gauge | `location` | Runnable tasks observed during the latest Manager dispatch pass. |
| `fsp_assignment_rejections_total` | Counter | `location`, `reason` | Scheduler decisions that found no eligible Agent or lost a delivery target. |
| `fsp_source_query_duration_seconds` | Histogram | `location` | SQL source-query attempt latency, including failed attempts. |
| `fsp_artifact_upload_bytes_total` | Counter | `location` | Bytes from successful Azure Blob artifact writes. |
| `fsp_artifact_upload_duration_seconds_total` | Counter | `location` | Elapsed seconds spent in those successful writes. |
| `fsp_agent_heartbeat_age_seconds` | Gauge | `location`, `agent_id` | Age of each registered Agent's latest heartbeat, in seconds. |

Artifact throughput in the dashboard is calculated as the rate of successful
upload bytes divided by the rate of successful upload duration. It represents
application-observed Blob write throughput, not Azure Storage network
throughput. Immutable writes that find an identical pre-existing object do not
increment the success counters.

The alert thresholds are starting values. `FspAgentHeartbeatStale` uses the
existing Manager health threshold of six seconds. Queue depth ten and source
query p95 five seconds are defaults for review, not service-level objectives.
`FspArtifactUploadStalled` fires when a location has runnable work but no
successful upload bytes for ten minutes. Tune these rules against production
workload data before routing them to paging.
