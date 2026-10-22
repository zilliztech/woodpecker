# Woodpecker alerting

This example installs all 31 R/F/S/C alerts from the supplied [resource planning and alerting reference](http://remote-localhost:8000/woodpecker_resource_plan_and_scaling_and_altering/wp-scaling-alerting.html). Source provenance is recorded in `alerting-sources.json`; installation uses the bundled rules and does not depend on that local website.

The default receiver is `local-only`. The optional `--mail-capture` profile adds real SMTP notifications to a local Mailpit server, addressed to `zhenyuan.wei@zilliz.com`. No relay is configured, so captured messages stay local. An external SMTP sample is provided separately below.

## Access and verification

- Prometheus rules/alerts: http://127.0.0.1:9099/alerts
- Alertmanager: http://127.0.0.1:9093
- Grafana also provisions an Alertmanager datasource.

Install the optional backends first: `./stack.sh addons-deploy --with prometheus,alertmanager`
(and `--with grafana` for dashboards). Run `./stack.sh forward` to expose the selected services; set `ALERTMANAGER_PORT` or `ALERTMANAGER_URL` when overriding the local endpoint.
Alertmanager runs in its own StatefulSet Pod, separate from Prometheus and Grafana.
No SMTP service or Mailpit is needed for the default monitoring and alerting loop.

```bash
./stack.sh alerts        # Read-only rule health, coverage, receiver and discovery checks
./stack.sh alerts-test   # 16 offline behavioral tests, default namespace and capacity profile
./stack.sh alerts-smoke  # Temporary synthetic alert: firing -> AM receipt -> resolution
./stack.sh acceptance    # Full workload/monitoring/alerting acceptance; no email
```

The smoke command also injects isolated synthetic alerts into Alertmanager to verify inhibition and the absence of cross-node suppression. It cleans up its temporary PrometheusRule and synthetic events in a `finally` block. It neither stops services nor changes business data. It accepts the default receiver or bundled SMTP capture profile and refuses to run with external integrations. Its events do not carry the email-test label and stay on the local-only route. Results are saved in `.local/alerting*.json` and included by `collect`.

## Configuration and operational differences

- `charts/full-stack-support/rules/woodpecker.yaml` contains all expressions. The supporting chart scopes selectors and recording constants to this namespace/cluster/release. `full-stack=milvus` labels control rule discovery.
- `values/monitoring.yaml` enables Alertmanager 0.27.0, a 1 GiB PVC, grouping, and inhibition. Default Kubernetes alert bundles remain disabled.
- `alerts.limits` in the support chart holds the reference AWS medium-region constants: 3000 IOPS, 81 MB/s disk writes, 98 MB/s network, and replication factor 3. These are not measured limits of a minikube laptop. Calibrate them before interpreting R1-R3 as capacity recommendations; no rule performs scaling automatically.
- Three local WP pods share one physical node/device. Device counters can reflect shared disk activity; this setup does not validate production failure domains or independent volume capacity.
- R1/R2 include the disk metrics now exported by the supplied v0.1.46 server, even though the older reference marked them as pending.
- R2 prefers cAdvisor network counters per Pod and falls back to the existing `process_network_*` counters exported by each WP Pod when cAdvisor does not provide them. It takes the maximum across duplicate reports instead of summing them. This local kubelet lacks the cAdvisor network families; the coverage report records the fallback explicitly.
- F1 targets carry `node_id` from the pod name so up/process/gRPC alerts and server alerts share a reliable inhibition identity. Namespace/cluster/release labels prevent unrelated deployments inhibiting each other.
- F2 handles disappearance of every scrape target with an explicit zero fallback. F3 also uses Kubernetes restart counters because process series can change across restarts.
- F6 matches the actual `AddEntry` / `AddEntries` RPC names. WP histograms use milliseconds, gRPC uses seconds.
- R4 follows the exported disk soft watermark minus 0.05, falling back to the reference 0.75. It aggregates the highest usage and lowest watermark by node/data path before joining, so old/new scrape instances during Pod IP changes cannot create many-to-many errors.
- S2 requires client demand assigned to that node as well as active segments and zero server appends. Idle test environments do not generate a false write-stall P0. The placement join uses this chart's named service endpoints.
- C4/C6 use gauge deltas and require a stable segment ID. C9 deduplicates node endpoints across client reporters.
- Constants, placement recording, R2 and R3 are evaluated sequentially in one group; R3 observes continuous R2 activity, including pending time.

## Coverage gaps and warm-up

`./stack.sh alerts` reports missing metric names separately from rule evaluation errors. Lazy error counters may have no series until the corresponding operation/error occurs; missing series do not establish that a failure detector works. The supplied Milvus image currently has no `woodpecker_client_read_frontier_entry/segment` series, so C6 remains a known detection gap until instrumentation/export is available. R7/C8 may likewise be unexercised or unsupported in a selected image; inspect the report rather than treating an empty result as healthy.

C2 requires samples for the same log identity seven days ago. Retention is now 8 days, capped at 3 GB on the existing 4 GiB Prometheus PVC; high ingestion can shorten the retained history. The rule is in warm-up without that baseline. R6/S6 also need enough hourly history, and the normal 1-15 minute hold periods apply to production rules.

## Priorities and inhibition

| Priority | Severity label | Intended response |
| --- | --- | --- |
| P0 | critical | Data availability/safety at risk; immediate response |
| P1 | warning | Degradation/redundancy loss; investigate promptly |
| P2 | info | Capacity/fairness/long-term health |

Configured inhibition follows the reference: F1 suppresses same-node server consequences; F8 suppresses same-node S6 and cluster-level client C8; R5 suppresses same-node S3; C6 suppresses C5 for the same log. F5 additionally suppresses its lower-severity F4 counterpart. Inhibited alerts still exist in Prometheus and remain inspectable in Alertmanager. The selected notification profile determines delivery.

## Common diagnostics

Each rule carries its reference ID, source URL, local runbook path, dashboard URL, and diagnostic commands. For the `wp_command` annotation, first expose an independent server and install/build the repository CLI:

```bash
kubectl --context wp-ops-v0146 -n milvus-stack port-forward pod/milvus-woodpecker-0 19091:9091
# In another terminal:
wp --endpoint http://127.0.0.1:19091 node list
kubectl --context wp-ops-v0146 -n milvus-stack logs -l app.kubernetes.io/instance=milvus,component=woodpecker --tail=100 --all-containers=true
./stack.sh collect
```

Use the rule's node/log/reader labels to narrow the investigation; do not reset metadata or erase storage as a response to an alert.

## Rule catalogue

| ID | Alert | Hold | Priority | Meaning |
| --- | --- | --- | --- | --- |
| <a id="r1"></a>R1 | `WoodpeckerNodeResourcePressure` | 10m | P2 | Inspect placement and workload skew before changing capacity. IOPS limits are reference inputs, not measured local limits. |
| <a id="r2"></a>R2 | `WoodpeckerClusterNeedsScaleUp` | 10m | P1 | Advisory only: this example never changes node pools, replicas, or PVC sizes automatically. |
| <a id="r3"></a>R3 | `WoodpeckerScalePressurePersistent` | immediate | P1 | Check whether scaling actually relieved the constrained resource. Pending time is included, as in the reference. |
| <a id="r4"></a>R4 | `WoodpeckerDiskNearSoftWatermark` | 10m | P2 | Uses the exported soft watermark minus 5 percentage points, falling back to 0.75 when that metric is unavailable. |
| <a id="r5"></a>R5 | `WoodpeckerDiskHardWatermark` | 1m | P0 | All tenants on this node can be rejected. Restore object-store/compaction health before considering disk expansion. |
| <a id="r6"></a>R6 | `WoodpeckerDiskPredictedFull` | 15m | P1 | Inspect growth, object-store errors, and compaction before expanding storage. |
| <a id="r7"></a>R7 | `WoodpeckerCompactionAdmissionRejected` | immediate | P2 | Separate reservation pressure from memory_pressure using the reason label. |
| <a id="f1"></a>F1 | `WoodpeckerNodeUnreachable` | 1m | P1 | Check pod readiness, connectivity, and server logs. The scrape target supplies node_id even while down. |
| <a id="f2"></a>F2 | `WoodpeckerHealthyNodesInsufficient` | 1m | P0 | Includes the zero-discovered-target case. New segments may be unable to select a quorum. |
| <a id="f3"></a>F3 | `WoodpeckerNodeRestartingFrequently` | immediate | P1 | The Kubernetes restart counter covers process-series replacement after container restarts. Inspect OOM, panic, and probes. |
| <a id="f4"></a>F4 | `WoodpeckerFlushLatencyHigh` | 10m | P2 | Check local storage latency and contention. Woodpecker histogram units are milliseconds. |
| <a id="f5"></a>F5 | `WoodpeckerFlushLatencySevere` | 5m | P1 | Investigate severe local disk degradation; this inhibits the lower flush-latency warning. |
| <a id="f6"></a>F6 | `WoodpeckerGRPCAppendLatencyHigh` | 10m | P1 | Uses the live AddEntry/AddEntries method names and seconds, not milliseconds. |
| <a id="f7"></a>F7 | `WoodpeckerObjectStorageErrorRateHigh` | 5m | P1 | Inspect object-store connectivity, credentials, throttling, and the operation label. |
| <a id="f8"></a>F8 | `WoodpeckerObjectStorageUnavailable` | 2m | P0 | Compaction can stop and local WAL can grow even while short-term writes still succeed. |
| <a id="f9"></a>F9 | `WoodpeckerObjectStorageLatencyHigh` | 10m | P1 | The threshold is 5000 because the Woodpecker histogram uses milliseconds. |
| <a id="s1"></a>S1 | `WoodpeckerFlushQueueBacklog` | 15m | P2 | A busy pool alone is expected; investigate only sustained excessive queueing. |
| <a id="s2"></a>S2 | `WoodpeckerWritesStalled` | 5m | P0 | Requires current active-segment placement and positive client append traffic, so idle development clusters do not page. |
| <a id="s3"></a>S3 | `WoodpeckerAppendErrorRateHigh` | 5m | P1 | Hard disk-watermark alerts inhibit this expected consequence on the same node. |
| <a id="s4"></a>S4 | `WoodpeckerSlowReplica` | 10m | P1 | Inspect per-node flush latencies and the shared device before changing capacity. |
| <a id="s5"></a>S5 | `WoodpeckerSegmentPlacementSkew` | 15m | P2 | Active segments are a placement approximation; selection-mode counters cannot identify an individual skewed node. |
| <a id="s6"></a>S6 | `WoodpeckerLocalDataNotReclaimed` | 15m | P1 | Inspect compaction failures and object-store health before increasing storage. |
| <a id="c1"></a>C1 | `WoodpeckerClientAppendLatencyHigh` | 10m | P1 | Counts actual histogram buckets instead of using a threshold outside the histogram range. |
| <a id="c2"></a>C2 | `WoodpeckerClientAppendLatencyRegression` | 10m | P2 | Requires retained samples for the same log identity seven days ago; missing history is a coverage gap, not a healthy baseline. |
| <a id="c3"></a>C3 | `WoodpeckerClientPendingAppendsHigh` | 5m | P1 | Inspect client and server backpressure before increasing queue limits. |
| <a id="c4"></a>C4 | `WoodpeckerClientWriteFrontierStalled` | 5m | P0 | Checks the gauge with delta and requires no segment rollover, avoiding equal entry IDs across different segments. |
| <a id="c5"></a>C5 | `WoodpeckerClientReadLatencyHigh` | 10m | P1 | The maximum finite read bucket is 512 ms; a p99 > 1000 ms rule would never fire. |
| <a id="c6"></a>C6 | `WoodpeckerClientReadFrontierStalled` | 10m | P0 | Preserves reader_name and matches traffic by log identity. This rule cannot detect stalls if the image exports no read-frontier gauges. |
| <a id="c7"></a>C7 | `WoodpeckerClientDirectReadErrors` | 5m | P1 | Direct reads can fall back to quorum reads; investigate the latency impact and object-store errors. |
| <a id="c8"></a>C8 | `WoodpeckerClientCompactionFailed` | immediate | P1 | Use reason and the log identity to inspect why local WAL data is not being compacted. |
| <a id="c9"></a>C9 | `WoodpeckerActiveSegmentUnderReplicated` | 5m | P0 | Counts distinct node endpoints, not duplicate reports from multiple client processes. Same-node/AZ deployment is not HA. |

## Email notification profiles

### Local SMTP capture and repeatable acceptance

```bash
./stack.sh images --local-arm64 --mail-capture
./stack.sh deploy --local-arm64 --mail-capture
./stack.sh forward --mail-capture
# Another terminal:
./stack.sh email-smoke   # Email lifecycle only
./stack.sh acceptance --mail-capture # Also check SMTP and retain email evidence
```

Mailpit is pinned to `v1.31.6` and exposes SMTP port 1025 inside the cluster.
Its UI/API is available through the loopback forward at http://127.0.0.1:8025.
The receiver uses `woodpecker-alerts@example.test` as the local sender and
`zhenyuan.wei@zilliz.com` as the example recipient. Replace the recipient in
`values/mail-capture.yaml` and set `EMAIL_TO` for checks when reusing the example.
TLS is disabled only for this unauthenticated local capture service.

Real Woodpecker rules (`alert_set="woodpecker-full-stack"`) and dedicated email
probes (`notification_test="email"`) route to capture. Inhibition probes remain
local-only. The email test creates a uniquely labeled temporary PrometheusRule,
waits for a captured FIRING email, resolves the rule, then waits for a captured
RESOLVED email. It checks the exact recipient, test identity, subject status and
alert body, and cleans up the rule/events. Messages remain available for review;
Mailpit storage is ephemeral and limited to 1000 messages. Tests never clear
other runs' mail. Evidence is saved in `.local/email-smoke.json`.

### External SMTP sample

Copy `values/email-smtp.example.yaml` to `.local/email-smtp.yaml`. Replace the
SMTP host/port, verified sender and username. The sample already targets
`zhenyuan.wei@zilliz.com`, uses STARTTLS (usually port 587), and sends recovery
notifications. Use a relay/account authorized to send from the selected sender.
SMTP credentials cannot be inferred from the recipient address.

Prepare the password as a file outside version control without pasting it into
chat, command arguments, Helm values, or a committed manifest, then create its
Secret using the file path:

```bash
kubectl --context wp-ops-v0146 -n milvus-stack create secret generic alertmanager-smtp \
  --from-file=password=/absolute/path/to/smtp-password
./stack.sh render --local-arm64 --with prometheus,alertmanager --monitoring-values .local/email-smtp.yaml
./stack.sh deploy --local-arm64 --with prometheus,alertmanager --monitoring-values .local/email-smtp.yaml
```

The operator mounts the password file at
`/etc/alertmanager/secrets/alertmanager-smtp/password`. Omit the authentication
fields and Secret only if your relay explicitly supports unauthenticated use.
Do not combine the external SMTP override with `--mail-capture`: both replace
the receiver and route lists. The initial external route matches only dedicated
email probes; real rule alerts remain local-only until a route for
`alert_set="woodpecker-full-stack"` is added deliberately.

`email-smoke` verifies capture only and will not claim external delivery. A real
mailbox test needs the actual relay details and confirmation of inbox receipt;
successful SMTP acceptance alone cannot prove inbox delivery. `alerts` can still
inspect rule health with external receivers, while `alerts-smoke` refuses external
notification profiles to avoid sending its multiple inhibition events.

References: [Mailpit Docker installation](https://mailpit.axllent.org/docs/install/docker/),
[Mailpit API](https://mailpit.axllent.org/docs/api-v1/), and the
[pinned Alertmanager 0.27 configuration](https://github.com/prometheus/alertmanager/blob/v0.27.0/docs/configuration.md).

## Local validation, 2026-10-19

- Prometheus 3.0.1 accepted all 36 rules (31 alerts and 5 recording rules); all live evaluations were healthy.
- All 16 behavioral tests passed, including process-network fallback and cAdvisor precedence after the rate-window warm-up.
- Alertmanager 0.27.0 validated both the SMTP capture profile and external SMTP sample (two receivers and six inhibition rules). The deployed capture profile has no external SMTP relay.
- A temporary PrometheusRule exercised firing, delivery to Alertmanager, and resolution. Isolated AM events verified inhibition and that unrelated nodes remained visible; all synthetic resources/events were cleaned up.
- The coverage report used the WP process-network fallback. R7/C8 had no rejection/failure counter series, C2 had no seven-day baseline, and C6 had no observed read-frontier series. These gaps are recorded separately from healthy rule evaluation.
- The monitoring upgrade preserved the eight business scrape targets and all three dashboards; all 695 dashboard queries remained valid.
- The live email probe delivered both FIRING and RESOLVED messages through SMTP to Mailpit with `zhenyuan.wei@zilliz.com` as the recipient. Recipient, subject status, run identity, and body were checked. No external inbox delivery has been tested without relay credentials.

Reports are generated at `.local/alerting.json`, `.local/alerting-smoke.json`, `.local/email-smoke.json`, and under timestamped `.local/evidence/` directories. `acceptance` also runs the 16 offline behavioral tests before the live workload.
