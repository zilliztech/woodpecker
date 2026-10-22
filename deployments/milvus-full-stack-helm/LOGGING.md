# Optional Loki logging

Loki stores and queries logs; Alloy collects them; Grafana is an optional UI.
The core deployment contains none of these components unless selected.

```bash
./stack.sh images --local-arm64 --with loki,grafana
./stack.sh addons-deploy --with loki,grafana
./stack.sh forward --wpcli
# Another terminal:
./stack.sh logs
./stack.sh logs-smoke
```

`grafana/loki:3.5.7` runs in single-binary mode with a 4 GiB PVC. Its TSDB index,
compressed log chunks and ingestion WAL live under `/var/loki`. Retention is
24 hours by default and is enforced asynchronously by the compactor. A PVC is
not a disk-space quota or an exact deletion deadline. Adjust `loki.storage` and
`loki.retention` through `--support-values` for larger experiments. This local
filesystem configuration is not a production HA storage design.

`grafana/alloy:v1.10.2` runs once per node as a DaemonSet. It tails container logs
through the Kubernetes API, filtering discovery to its own node and the stack
namespace. Its namespace Role grants Pod/log read access; it needs neither a
privileged container nor a host log-directory mount. It collects the namespace's
application/dependency logs and excludes the Loki/Alloy containers themselves.
Already rotated logs and logs from removed containers cannot be recovered by
installing the collector later. Collection/retry state is ephemeral in this
example; this is not a no-loss log archival pipeline.

Labels include `cluster`, `namespace`, `pod`, `container` and, where present,
`component`. Trace IDs remain in log content rather than creating a high-cardinality
stream label. Queries first narrow the time range and labels, then filter content.

## Query in Grafana Explore

Open http://127.0.0.1:3000/explore and select **Loki**. Example queries:

```logql
{namespace="milvus-stack", component="streamingnode"}
  |= "append message into wal too slow"
```

```logql
{namespace="milvus-stack", component="woodpecker"}
  |~ "(?i)error|warn"
```

```logql
{namespace="milvus-stack"} |= "YOUR_TRACE_ID"
```

When Jaeger is selected too, the provisioned Loki datasource extracts a 32-digit
`[traceID=...]` from log lines and offers a link to that ID in Jaeger. A link does
not prove that the trace is sampled, still retained, or connected to the user
Insert; the existing write propagation gap is documented in TRACING.md.

The direct API is http://127.0.0.1:3100. Use `LOKI_PORT`/`LOKI_URL` overrides and
`stack.sh connections` when defaults differ. Grafana queries Loki's internal
service address and does not require the browser to connect directly to Loki.

## Verify actual collection

`logs-smoke` inserts ten vectors into a new collection, checks query/search, finds
that unique collection marker in the actual Proxy Pod logs via kubectl, then
polls Loki for the same marker and Pod identity. It does not inject a synthetic
log directly into Loki and call that collection success.

`.local/logging-smoke.json` saves the workload marker, source Pod/log line, exact
LogQL query and returned streams. `logs` performs a readiness and recent namespace
log check. `collect` includes logging verification only when Loki is selected.
Review captured logs before sharing.

If no logs arrive, check `full-stack-alloy` readiness/RBAC, its component graph
and export errors, the selected namespace/time range, and Loki readiness/storage.
When Prometheus is selected, PodMonitors also scrape Loki, Alloy and Jaeger
(where selected), making their ingestion/queue/resource metrics available for
inspection separately from Milvus/WP's eight application targets.

`stop/start` preserves the Loki PVC. `addons-clear --with loki` removes Loki and
Alloy and deletes that log PVC. `clear` removes all stack data. Removing Grafana
alone leaves the Loki backend and its logs intact.

## Local validation

The existing ARM64 minikube stack was migrated by adding only Loki/Alloy. All
8 Milvus/WP Pod UIDs remained unchanged, all 18 Pods were ready with zero restarts,
and Loki/Alloy/Jaeger scrape targets were UP. Grafana's Loki datasource health
check succeeded, and Explore displayed the actual unique Proxy log marker after
a 10-vector insert/query/search smoke. Its log trace ID resolved to 23 Jaeger spans.
The existing seven dashboards and all 861 PromQL expressions remained valid;
31 alert rules and five recording rules were healthy with local-only routing.

These checks establish live delivery and query visibility for this configuration.
They do not test loss during collector outages, log rotation, PVC recovery or
24-hour retention aging. Local reports/screenshots are kept under `.local/`.

Sources: [Loki deployment modes](https://grafana.com/docs/loki/latest/get-started/deployment-modes/),
[filesystem storage](https://grafana.com/docs/loki/latest/operations/storage/filesystem/),
[Alloy Kubernetes API collection](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.kubernetes/).
