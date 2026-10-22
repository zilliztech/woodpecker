# Milvus + Woodpecker full stack with Helm

A reproducible local Kubernetes environment for Milvus, **Woodpecker service mode**,
with optional Prometheus, Grafana, Alertmanager, Jaeger and Loki/Alloy. Milvus is installed using the unmodified official
[milvus-helm chart](https://github.com/zilliztech/milvus-helm). The example connects
deployment, a real Milvus workload, metrics collection, dashboards, and diagnostic
evidence so developers and local agents can repeat the same feedback loop.

The website's [Milvus–Woodpecker Stack Examples](https://zilliztech.github.io/woodpecker/docs.html#stack-examples)
explains the goals and connects deployment, monitoring/alerts, wpcli operations and
E2E experiments, including how to adapt these examples to your own instances.

**Current scope:** deployment, dashboards, and the reference document's 31 Woodpecker
alerts. Alertmanager defaults to a `local-only` receiver. An optional Mailpit
profile tests real SMTP delivery locally, with firing and recovery emails addressed
to `zhenyuan.wei@zilliz.com`; a separate sample supports a real SMTP relay.
See [alerting rules, runbooks, and verification](ALERTING.md).
A new deployment installs only Milvus + WP and their etcd/MinIO dependencies.
Choose optional components with `--with`, or add them later without rebuilding
the core configuration. See [component installation and lifecycle](COMPONENTS.md).
Tracing exports to Jaeger when selected. See [trace verification
and known E2E coverage gaps](TRACING.md); exporter availability alone does not
establish a complete user-to-storage trace.

```mermaid
flowchart LR
  workload[Local REST workload] --> proxy[Milvus proxy]
  proxy --> milvus[Milvus coordinators and workers]
  milvus --> wp[3 Woodpecker services]
  milvus --> etcd[Persistent etcd]
  milvus --> minio[Persistent MinIO]
  wp --> minio
  milvus -. metrics incl. Woodpecker client .-> prom[Prometheus]
  wp -. server metrics .-> prom
  milvus -. OTLP traces .-> jaeger[Jaeger: local trace history]
  wp -. OTLP traces .-> jaeger
  milvus -. container logs .-> alloy[Alloy]
  wp -. container logs .-> alloy
  alloy --> loki[Loki: persistent logs]
  loki --> grafana
  jaeger --> grafana
  k8s[Kubelet and kube-state-metrics] -. resource metrics .-> prom
  prom --> grafana[Grafana: Milvus + WP client + WP server]
  prom --> am[Alertmanager: grouping and inhibition]
  am --> mail[Optional Mailpit SMTP capture and email verification]
```

## Components and versions

| Component | Version / source |
| --- | --- |
| Milvus Helm chart | `5.0.31`, official `zilliztech/milvus-helm` release |
| Default Milvus image | `milvusdb/milvus:v3.0.3` |
| Local ARM64 Milvus override | Supplied `enhance-woodpecker-v0.1.46-3.0-20261012-27bd28d206-v2-arm64` image; see below |
| Woodpecker | `milvusdb/woodpecker:v0.1.46`, three independent service replicas |
| etcd | `quay.io/coreos/etcd:v3.5.25` |
| MinIO | `milvusdb/minio:RELEASE.2024-12-18T13-15-44Z` |
| kube-prometheus-stack | `67.4.0` |
| Grafana | `11.4.0` (retains the exported Milvus dashboard's Graph panels) |
| Prometheus / Prometheus operator | `3.0.1` / `0.79.2` |
| Alertmanager | `0.27.0`, persistent local-only receiver |
| Optional Jaeger | `jaegertracing/all-in-one:1.53.0`, bounded in-memory trace history |
| Optional Loki / Alloy | `grafana/loki:3.5.7` / `grafana/alloy:v1.10.2`, PVC-backed logs and namespaced collection |
| Optional Mailpit | `axllent/mailpit:v1.31.6`, local SMTP capture |
| Previously validated Kubernetes | `v1.33.1` on minikube, prepared externally |

The monitoring layout follows `tests/k8s/monitor/`. That test suite does not pin
the chart/Grafana version, so this example pins its own reproducible versions in
`versions.env` and `values/monitoring.yaml`.

## Prerequisites

Use an **existing Kubernetes cluster**, configured kubectl, Helm 3 and Python 3.9+.
The stack uses `kubectl config current-context` by default; select another with
`--context NAME` or `CONTEXT`. `NAMESPACE` / `--namespace` defaults to `milvus-stack`.
Configure credentials, connectivity, a suitable dynamic StorageClass and image
registry access before deployment. The stack never creates, starts, stops or
removes Kubernetes nodes, VMs, containers or profiles.

```bash
kubectl config current-context
kubectl version
python3 -m pip install -r scripts/requirements-connections.txt
```

Docker is optional for the offline Promtool suite. The notebook launcher needs
Python 3.11+ and installs a pinned isolated runtime on first launch. Private ARM64
Milvus images require registry access or node caches prepared externally.

## Preview, then execute

Commands that change resources, open forwards or run workloads **only print their
operations by default**. Add `-x` / `--execute` to execute them. Read-only status,
connection/metric inspection and local charts/render/images preparation run
without `-x`. Planning can read the selected cluster and write local plan files;
it does not change Kubernetes resources. Printed Helm/kubectl commands use explicit
context/namespace and can be copied directly. Local values/manifest files are
retained for inspection under `.local/plans/` and may contain credentials.

```bash
./stack.sh install --local-arm64  # Preview native commands
./stack.sh install --local-arm64 -x           # Install core on existing Kubernetes
./stack.sh addons-install --with all  # Preview optional observability
./stack.sh addons-install --with all -x
./stack.sh forward --wpcli -x                # Separate terminal
./stack.sh smoke -x
./stack.sh verify                           # Read-only checks
```

Omit `--local-arm64` for the public default Milvus image. Repeat core override
files on core upgrades. Addon operations preserve the live core overrides and
previously selected components. See [COMPONENTS.md](COMPONENTS.md) for independent
component installation and dependencies.

`render` generates local manifests; `images` lists images from that selected
profile. It does not pull images, load node caches or manage a container runtime.
Prepare image access using your cluster's normal tools.

## Command guide and lifecycle boundaries

| Command | Default behavior | Effect with `-x` |
| --- | --- | --- |
| `install` | Print native Helm/kubectl plan | Install/upgrade releases and run the selected stack; retain PVCs and existing addons |
| `stop` | Print pause plan | Save replica state in a namespace ConfigMap; pause this stack's workloads; retain releases/PVCs |
| `start` | Print resume plan | Restore the saved replica counts and scheduling; retain PVCs; does not install a missing stack |
| `uninstall` | Print uninstall/data-deletion plan | Uninstall all stack releases and delete their PVC data |
| `addons-install` / `addons-uninstall` | Print selected addon changes | Add components / remove selected components and their data |
| `forward` | Print native port-forward commands | Open/reuse only this stack's forwards |
| `smoke`, `perf`, `*-smoke`, `acceptance` | Print workload/probe entry points | Write test data and run the selected probes |
| `status`, `connections`, `addons-status`, `verify`, `logs`, `traces`, `alerts`, `collect` | Inspect/read and optionally save local evidence | Same read-only resource behavior |
| `charts`, `render`, `images`, `alerts-test`, `notebook` | Local preparation, offline checks or local notebook process | No Kubernetes lifecycle management |

`start/stop` apply to the **deployed stack**, not Kubernetes. Stop scales ordinary
Deployments/StatefulSets to zero, sets Prometheus/Alertmanager CR replicas to zero
while their Operator is available, and pauses Alloy's DaemonSet with a reversible
node selector. Start restores exact saved counts (including an original zero),
removes only the managed pause selector, and waits for rollouts. The saved state
is `milvus-full-stack-lifecycle` in this namespace; the printed operations are
standard kubectl commands, and recovery does not depend on a machine-local file.
Repeated stop preserves the original snapshot. Partial operations can be retried.

Pause/resume uses Helm manifest identities and live ownership checks. It refuses
workloads whose scale-down retention policy would delete PVCs, active HPAs,
paused Operator reconciliation or changed resource identities. Coordinate any
external GitOps reconciliation through its normal configuration. Helm changes
through this wrapper are refused while its saved stop state exists; resume first.

```bash
./stack.sh stop  # Inspect; does not stop anything
./stack.sh stop -x          # Pause only this stack's workloads
./stack.sh start -x         # Resume those workloads on the same running cluster
./stack.sh forward --wpcli -x
./stack.sh uninstall  # Inspect the destructive cleanup plan
./stack.sh uninstall -x         # Delete stack releases AND their PVC data
```

`stop` preserves **persistent volume data**, not in-memory/ephemeral state. Jaeger
memory history disappears when its Pod exits. Uninstall retains Kubernetes, the
namespace, shared CRDs and local evidence; it does not remove other clusters.

## Jupyter walkthrough

```bash
./stack.sh notebook
```

Open the authenticated loopback URL and select
`notebooks/full-stack-walkthrough.ipynb`. Review its context, images, ports and
load. `EXECUTE=False` previews operations by default. Set `EXECUTE=True` explicitly
for the full run, which selects `--with all`; stop/uninstall cells stay disabled by
default. The notebook reuses matching forwards and owns only those it starts.
Its source is unexecuted; local runs/evidence remain under `.local/`.

## Optional local Kubernetes preparation (outside stack.sh)

Users who choose minikube manage it directly, just as shared-cluster users manage
their existing infrastructure. For example:

```bash
minikube start -p wp-ops-v0146 --driver=docker --cpus=6 --memory=6656
kubectl config use-context wp-ops-v0146
# If a private development image is already in the host Docker daemon:
minikube -p wp-ops-v0146 image load YOUR_IMAGE --daemon
```

Stopping that entire local cluster is a separate explicit `minikube stop -p ...`
operation. It is not what `stack.sh stop` does. Resource sizes are examples; use
your own cluster capacity and availability requirements.

## Open Grafana, Prometheus, and Milvus

Keep this command running in a terminal:

```bash
./stack.sh forward -x
```

| Service | Local URL |
| --- | --- |
| Grafana | http://127.0.0.1:3000 — `admin` / `admin` |
| E2E Write / Append | http://127.0.0.1:3000/d/milvus-e2e-write |
| E2E WAL Read / Consume | http://127.0.0.1:3000/d/milvus-e2e-consume |
| E2E Flush / Checkpoint | http://127.0.0.1:3000/d/milvus-e2e-flush |
| E2E Search / Query | http://127.0.0.1:3000/d/milvus-e2e-query |
| Milvus dashboard | http://127.0.0.1:3000/d/milvus-full-stack |
| Woodpecker client dashboard | http://127.0.0.1:3000/d/woodpecker-client-k8s |
| Woodpecker server dashboard | http://127.0.0.1:3000/d/woodpecker-server-k8s |
| Prometheus | http://127.0.0.1:9099 |
| Alertmanager | http://127.0.0.1:9093 |
| Jaeger (when selected) | http://127.0.0.1:16686 |
| Loki API (when selected) | http://127.0.0.1:3100 |
| Milvus REST / SDK endpoint | http://127.0.0.1:19530 |

Only installed services are forwarded. All forwards bind to loopback; Ctrl-C
closes only forwards created by that invocation. Override local
ports using `GRAFANA_PORT`, `PROMETHEUS_PORT`, `MILVUS_PORT`, `ALERTMANAGER_PORT`, `JAEGER_PORT` and `LOKI_PORT`. Use the same
overrides for the verification commands, or set `GRAFANA_URL`, `PROMETHEUS_URL`,
and `MILVUS_URL` explicitly. Grafana uses login authentication; anonymous admin
access is disabled. Provisioned dashboards survive pod recreation; exploratory
UI edits should be exported and reconciled with the JSON files.

Start with the symptom-oriented [E2E dashboards](E2E-DASHBOARDS.md), whose numbered
groups follow each processing step with investigation guidance. Use the three
component reference dashboards for deeper inspection. When Grafana is selected, all seven are provisioned
by Helm; notebook and performance reports also link to them.

## Inspect connections and configure wpcli

```bash
./stack.sh connections              # Host ports, WP service context, separate Milvus metadata connection
./stack.sh connections --json       # Structured inventory for scripts and local agents
```

This reads the selected Kubernetes context/namespace, deployed Milvus ConfigMap,
Woodpecker Pod identities, and local `kubectl port-forward` processes. It reports
actual listening forwards separately from proposed ports and unrelated occupied
ports. Custom local ports already in use by a matching forward are discovered;
`get svc` ports alone are not treated as host endpoints. JSON includes Service
ports/type, NodePort/load-balancer information, per-Pod targets and forward commands.

Connection discovery and `forward --wpcli` require PyYAML, pinned separately from the standard-library
workload/monitoring scripts:

```bash
python3 -m pip install -r scripts/requirements-connections.txt
# Or reuse an existing Python environment with PyYAML:
# CONNECTIONS_PYTHON=.local/notebook-venv/bin/python ./stack.sh connections
```

For wpcli access, keep this command running in a terminal:

```bash
./stack.sh forward --wpcli -x
```

It opens the four normal endpoints plus **one Admin forward per Woodpecker Pod**
(`19091 + Pod ordinal`, normally `19091/19092/19093`) and tenant etcd (`32379`).
Override these with `WP_ADMIN_PORT_BASE` and `ETCD_PORT`. Matching existing forwards,
including those opened by the notebook, are reused without taking ownership;
Ctrl+C closes only the forwards started by this invocation. An unrelated port
conflict fails before starting any new forwards. Loopback binding remains in use.
Woodpecker RPC `18080` stays inside Kubernetes; wpcli uses the Admin HTTP endpoints.

In another terminal, export a ready context to a separate local configuration:

```bash
./stack.sh connections --wpcli-config > .local/wpcli.yaml
WOODPECKER_CLI_CONFIG=.local/wpcli.yaml wp ctx view
WOODPECKER_CLI_CONFIG=.local/wpcli.yaml wp shell --context wp-ops-v0146/milvus-stack
```

The generated name defaults to `<Kubernetes context>/<namespace>`; set
`WP_CONTEXT_NAME` to choose another name. This never updates your personal
`~/.woodpecker/cli.yaml`. The output has two clearly separated sections:

1. **WP service cluster context:** the YAML settings for `cli.yaml`, containing
   the Admin endpoint, per-node Admin URL mappings and Kubernetes context/namespace.
   This connects wpcli to the WP service cluster.
2. **Milvus instance WP client metadata connection:** comments describing etcd,
   the complete metadata prefix, storage paths and an explicit `connect` command.
   These locate metadata managed by the selected Milvus instance's WP client;
   they are instructions, not `cli.yaml` settings.

Export fails with no stdout if the WP Admin forwards are not ready. Metadata
readiness is reported independently and does not block service-context export.
Custom/external etcd endpoints remain unchanged and require separate host access
validation; they are not replaced by this example's etcd service.

The normal `connections` output includes a copyable shell attachment command.
With the default deployed configuration, inside `wp shell` run:

```text
connect milvus-stack --etcd 127.0.0.1:32379 --meta-prefix full-stack/woodpecker --bucket milvus-bucket --root-path milvus/wp
status --check
node list
meta log list
```

The generated context contains no etcd, metadata-prefix or storage settings.
Select the WP service cluster with `--context`, then attach the Milvus instance's
WP client metadata explicitly with `connect`. JSON keeps the service configuration
in `wpcli` and the metadata connection parameters, readiness and `connect_command`
in `tenant`. The report derives the complete metadata
prefix from deployed `etcd.rootPath` plus `woodpecker.meta.prefix`. It distinguishes
Milvus's `minio.rootPath` (`milvus`) from the Woodpecker WAL storage root (`milvus/wp`),
as implemented by the pinned [Milvus WAL adapter](https://github.com/milvus-io/milvus/blob/v3.0.3/pkg/streaming/walimpls/impls/wp/builder.go).
Credentials are not exported. The provenance is explicitly the deployed ConfigMap;
process environment/dynamic runtime overrides are not inspected. Re-run discovery
after deployment changes; `status --check` validates the selected live connections.
In the notebook, `lab.run("connections")` shows the same report.

## Run a workload and verify the monitoring chain

In another terminal, while the forwards are active:

```bash
./stack.sh smoke -x
# Allow at least two 15-second scrapes after the first workload.
./stack.sh verify
./stack.sh alerts
./stack.sh alerts-smoke  -x # Temporary synthetic events; no external notifications
./stack.sh collect
```

The acceptance loop needs no SMTP service or Mailpit and checks only selected components:

```bash
./stack.sh acceptance -x
```

It runs the offline rule tests, a Milvus workload, checks all seven dashboards and
scrape targets, and exercises alert firing/resolution and inhibition. View the
alerts in Prometheus and Alertmanager using the URLs above. Email testing is
skipped by default.

For optional local SMTP acceptance, use the same profile on installation,
port forwarding and acceptance:

```bash
./stack.sh images --local-arm64 --mail-capture
./stack.sh install --local-arm64 --mail-capture -x
./stack.sh forward --mail-capture  -x # Keep running in a separate terminal
# In another terminal:
./stack.sh acceptance --mail-capture -x
```

With `--mail-capture`, `acceptance` also verifies both FIRING and
RESOLVED emails through SMTP. It returns nonzero on a failed check and saves JSON
reports under `.local/`. Allow several minutes for Kubernetes configuration
projection, rule reloads and notification grouping. Captured emails are inspectable at
http://127.0.0.1:8025 (`MAILPIT_PORT` / `MAILPIT_URL` override the endpoint).
Mailpit has no relay configured, so these messages do **not** reach the external
mailbox. Capture storage is ephemeral; reports retain the message IDs and verified
headers/body checks. See [SMTP setup and real delivery](ALERTING.md#email-notification-profiles)
for the external SMTP template and Secret setup.

The smoke workload creates a uniquely named collection, inserts 1,200 vectors in
12 batches, and verifies primary-key queries and nearest-vector search after each
batch. Collections remain available for inspection. Each API call has a timeout,
and the workload has a finite batch count. Results are saved in `.local/workload.jsonl`.
To generate a longer observation window:

```bash
BATCHES=120 BATCH_SIZE=100 INTERVAL=2 ./stack.sh smoke -x
```

For a measured concurrent write experiment with automatic diagnostic collection:

```bash
./stack.sh perf -x
# Or configure a longer window and larger offered load:
PERF_DURATION=300 PERF_CONCURRENCY=4 PERF_BATCH_SIZE=200 PERF_RPS=10 ./stack.sh perf -x
```

The default is a bounded 60-second measurement with two workers, 100-row batches,
128-dimensional vectors and a total target of five insert requests/second. It
separates warm-up, reports throughput and p50/p95/p99 latency, checks strong-read
counts/sample IDs/search, and saves request records, metrics, AM snapshots and Pod
logs even when a test fails. Each run gets a unique collection and artifact
directory under `.local/perf/`. See [write performance and operational diagnosis](PERFORMANCE.md)
for gates, parameters, artifact formats and investigation steps. `perf` is separate
from the quick smoke and acceptance commands so its load and thresholds are explicit.

The client runs **inside Milvus**: the PodMonitor scrapes Milvus components for
both Milvus metrics and the embedded Woodpecker SDK's client metrics. A second,
disjoint PodMonitor scrapes the three independent Woodpecker servers. This avoids
counting the same endpoint twice or substituting a standalone WP load generator
for actual Milvus traffic.

`verify` checks the default topology's five Milvus and three Woodpecker targets,
positive workload/client/server metrics, Kubernetes metrics, server-side Grafana
dashboard provisioning, and every visible PromQL target in all seven dashboards.
It also checks required metric families in each E2E processing stage, so an UP
panel cannot hide a missing WAL-stage metric. It writes `.local/verification.json`
with per-query errors, stage coverage and optional missing families.
Queries without samples are recorded separately from rejected queries: a short
write/search workload does not exercise every error, compaction, import, sparse
index, or other optional feature. The default topology checks must be adjusted if
you deliberately change replica counts.

`collect` writes timestamped artifacts under `.local/evidence/`: resource status,
events, image versions, bounded container logs, Helm release versions, and live
dashboard query coverage. It does not export Secrets or Helm values. Review
application logs before sharing artifacts. The install and verify commands return
nonzero on failure; the evidence command attempts to retain useful diagnostics
even when observability verification fails.

## Dashboard sources and label contract

- **Milvus:** supplied production export `milvus_grafana_dashboard.json`; its source
  SHA256 and transformations are recorded in `dashboards-sources.json`.
- **Woodpecker:** `tests/k8s/monitor/grafana/templates/dashboard_client_k8s.json`
  and `dashboard_server_k8s.json`.
- **E2E:** four ordered dashboards authored in `scripts/e2e_dashboards.py`, with
  reference versions and stage coverage in `e2e-dashboard-catalog.json`. See
  [E2E-DASHBOARDS.md](E2E-DASHBOARDS.md) for metric semantics and interpretation.
- All seven JSON files are included in `charts/full-stack-support/dashboards/`;
  installation does not require files outside this directory.
- Dashboard ConfigMaps are imported by Grafana's dashboard sidecar, with a stable
  `prometheus` datasource UID and links between the seven dashboards.
- Application scrape labels include `cluster=local`, `namespace`,
  `app_kubernetes_io_name=milvus`, `app_kubernetes_io_instance=milvus`, `component`,
  `pod`, and `woodpecker_id=milvus`. Kube-state-metrics and kubelet/cAdvisor also
  receive `cluster=local`. The exported dashboard's Kubernetes queries depend on
  these infrastructure collectors, which are enabled here.
- Production datasource UIDs, selected namespaces/instances, and private Grafana
  navigation are removed. The production Loki events panel becomes an explanation
  of local Kubernetes event evidence collection. Optional [Loki/Alloy](LOGGING.md)
  collects container logs; it does not collect Kubernetes Events.
- Namespace/pod discovery is scoped to Milvus. The exported restart, slow-query,
  and disk-IOPS expressions are corrected for standard Prometheus. Service Quality
  and the Woodpecker server overview open by default.
- The WP server dashboard's IOPS/bandwidth/network limit inputs are reference
  values from the source template; adjust them to the actual storage/node limits
  before using its headroom ratios for capacity decisions.

To refresh from the original sources, from the repository root:

```bash
python3 deployments/milvus-full-stack-helm/scripts/sync-dashboards.py \
  --milvus-source ../milvus_grafana_dashboard.json
```

To regenerate only the E2E dashboards and shared navigation (no external source
export needed), run `python3 scripts/e2e_dashboards.py` from this example's directory.
Use `python3 tests/dashboards.test.py` to check scoping, units and coverage behavior.

Review changes, redeploy, and rerun the workload/verification. Dashboard sections
named “Alerts” in the WP templates are charts of threshold-related metrics.
The supporting chart separately installs the 31 reference alerts, and the monitoring
chart enables Alertmanager. `./stack.sh alerts-test` runs 16 offline behavioral
tests with the pinned Prometheus image; `alerts-smoke` checks firing, delivery,
resolution, inhibition, and synthetic-event cleanup against the local stack.

## Configuration and lifecycle

- `values/milvus.yaml`: upstream chart overrides, including three WP services,
  S3/etcd connectivity, persistence, resource sizing and shared OTLP tracing.
- `values/local-arm64.yaml`: supplied local Milvus image override.
- `values/monitoring.yaml`: metrics collection, Grafana provisioning, Alertmanager
  grouping/inhibition, local-only receiver, and 8-day / 3-GB-capped retention.
- `values/mail-capture.yaml`: optional Mailpit and SMTP receiver with the example recipient.
- `values/email-smtp.example.yaml`: external SMTP template with Secret-mounted credentials.
- `charts/full-stack-support/`: small supporting chart for persistent single-node
  etcd/MinIO and selected Jaeger/Loki/Alloy, PodMonitors, dashboard ConfigMaps, and PrometheusRule resources. The upstream etcd dependency
  expects a different image layout; the supplied official etcd image is deployed
  directly here instead. Milvus consumes these through external dependency settings.
- `stack.sh`: chart download, rendering, image inventory, installation, access and evidence.

Use `./stack.sh render --local-arm64` to inspect manifests before deployment.
Chart archives, rendered manifests, and results are ignored under `.local/`.
Additional Milvus overrides can be supplied with `-f path/to/values.yaml`; use the
same flags for rendering, image inventory, and deployment. Keep changes to S3 access
credentials consistent between the support chart and Milvus values.
Use `--monitoring-values file` and `--support-values file` for their corresponding
releases. Component selection is stored in the supporting Helm release. Repeating `install`
preserves already selected addons; `addons-install --with ...` adds components.
Use `addons-uninstall --with ...` to remove selected addons and their data. Core
override files still need to be repeated for a core `install`; addon changes
preserve the live core image, resources and user configuration. See COMPONENTS.md.

Re-running `install` performs a Helm upgrade. Pin and review chart/image changes,
then verify them with the same workload and dashboards. Monitor CRDs are installed
by kube-prometheus-stack; follow that chart's CRD upgrade instructions when changing
its version. The scripts do not delete shared CRDs.

```bash
# Remove the stack AND its persistent data; keep the cluster for a fresh install.
./stack.sh uninstall -x
# Or pause only this stack’s workloads while retaining releases/PVCs:
./stack.sh stop -x
```

`uninstall` resets etcd, MinIO and Woodpecker storage together, preventing log-ID reuse
from a partial metadata/WAL reset. It also deletes Prometheus history and
Alertmanager state. Local `.local/` reports and chart/image caches remain available.

The default minikube StorageClass uses the `Delete` reclaim policy, so removing
these PVCs also reclaims their backing volumes. On an external cluster, a `Retain`
policy can leave backing volumes/data for the storage administrator to remove.
External S3 buckets or pre-existing storage outside this stack's labeled PVCs are
not erased by `uninstall`.

## Troubleshooting

### Kubernetes configuration and connection diagnostics

Before a live operation, the stack reads the merged kubeconfig and validates the
selected API connection. Diagnostics distinguish missing files, an unreadable
configuration, no contexts, no current selection, an unknown explicit context,
and API/network/authentication/TLS failures. They display file paths and available
context/cluster names, never kubeconfig credentials or raw authentication output.
No context is automatically selected and no cluster is started.

```bash
kubectl config get-contexts
# Use a configured context for one invocation without changing your default:
./stack.sh status --context YOUR_CONTEXT
# Or explicitly choose your default outside this wrapper:
kubectl config use-context YOUR_CONTEXT
./stack.sh status
```

If no contexts exist, obtain/restore kubeconfig using your cluster provider's
normal process, or set `KUBECONFIG=/absolute/path/to/your/kubeconfig`. For local
Kubernetes, restore/start it with your own provider tools outside stack.sh.
An existing context does not imply that its cluster is running or credentials
are valid; connection failures print native kubectl diagnostic commands.


The local ARM64 image override was live-validated on 2026-10-18: all five Milvus
and three Woodpecker scrape targets were UP, a 1,200-vector workload passed insert,
primary-key query, and vector-search checks, and all 695 dashboard expressions
were accepted by Prometheus. Browser checks confirmed all three dashboards,
the default Milvus namespace, and populated overview panels. Detailed coverage,
workload output, diagnostic evidence, and screenshots are kept under `.local/`.
The public default Milvus image was not exercised in that validation run.

```bash
./stack.sh status
kubectl --context wp-ops-v0146 -n milvus-stack get events --sort-by=.lastTimestamp
kubectl --context wp-ops-v0146 -n milvus-stack logs install/milvus-streamingnode --tail=100
kubectl --context wp-ops-v0146 -n milvus-stack logs milvus-woodpecker-0 --tail=100
```

- `ImagePullBackOff`: use `images` to inspect required references, configure node registry
  credentials/network access externally, and check the node architecture.
- Pending PVCs: confirm the StorageClass and your cluster’s storage provisioner.
- OOM/restarts: inspect events and check node capacity and component memory limits.
- Empty charts: run the workload, select the local datasource/namespace/instance,
  allow scrapes to complete, then inspect `.local/verification.json`. Check whether
  the selected image exports that metric and whether the feature was exercised.
- Missing WP client traffic: inspect Milvus's generated config for
  `woodpecker.storage.type: service`, its seed list, and server operation counters.

The intended development loop is: install → run workload → verify/collect → inspect
gaps → update metrics/configuration/dashboards → rerun. Future work can extend this
with authorized email notifications, automated failure/performance scenarios,
agent orchestration, and a separate `milvus-full-stack-operator/` installation path.

## Kubernetes lifecycle validation

95 offline Python tests cover selection, native command previews/execution,
context/preflight failures, resource ownership, replica restoration, repeated
stop, Operator resources, DaemonSet scheduling and PVC-retention/HPA guards,
alongside the existing workload/observability checks. ShellCheck and local Helm
rendering pass. Website checks cover six pages and 66 internal links.

Pause/resume behavior in this revision was verified with simulated backends;
no live Kubernetes workload was stopped, resumed or cleared. An absent current
context was checked locally and produced configuration guidance without starting
a cluster. Earlier live metric/log/trace validation above describes the deployed
ARM64 configuration before this lifecycle change.
