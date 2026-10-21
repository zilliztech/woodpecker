# Milvus + Woodpecker full stack with Helm

A reproducible local Kubernetes environment for Milvus, **Woodpecker service mode**,
Prometheus, and Grafana. Milvus is installed using the unmodified official
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
| Optional Mailpit | `axllent/mailpit:v1.31.6`, local SMTP capture |
| Local Kubernetes | minikube `v1.33.1`, Docker driver |

The monitoring layout follows `tests/k8s/monitor/`. That test suite does not pin
the chart/Grafana version, so this example pins its own reproducible versions in
`versions.env` and `values/monitoring.yaml`.

## Prerequisites

- Docker running, with at least 8 GiB available to its Linux VM and 8 host CPUs.
- minikube, Helm 3, kubectl, curl, and Python 3.9+ (standard library only).
- Enough free disk space for images and approximately 20 GiB of provisioned storage.
- Registry access to download monitoring images. The ARM64 override requires the
  supplied Milvus image to exist in the host Docker daemon or registry access to it.

This is a small, single-node development cluster: one proxy, mix coordinator,
streaming node, query node, data node, etcd and MinIO instance, and three Woodpecker
replicas. It does not provide independent node/AZ failure domains or production HA.
Resources and short segment rolling intervals favor local development rather than
performance benchmarking. Increase resources and review workload settings before
using it to compare throughput.

## Quick start: supplied local ARM64 build

For an executable guide through deployment, dashboard access, smoke/performance
tests, alerting, evidence and lifecycle operations, use the
[Jupyter walkthrough](notebooks/full-stack-walkthrough.ipynb):

```bash
./stack.sh notebook
```

This starts JupyterLab on loopback port `8891` with token authentication. Open the
authenticated URL printed in the terminal and select `notebooks/full-stack-walkthrough.ipynb`
(the launcher's default page). Use Shift+Enter to run the cells in order.
The first launch creates `.local/notebook-venv/` and installs the pinned notebook
requirements; it requires Python 3.11+ and package-registry access. CLI deployment
and test scripts still use Python 3.9+. Set `JUPYTER_BOOTSTRAP_PYTHON=python3.11`
if needed, `NOTEBOOK_PORT` to choose another port, or `JUPYTER_PYTHON` to reuse an
existing Jupyter environment without installing packages into it.

The notebook defaults to the supplied ARM64 build and preloads the required images.
Review its configuration cell: set `RUN_IMAGES=False` only if all images are already
loaded in the current node, or `USE_LOCAL_ARM64=False` for the public default image. It reuses healthy existing
endpoints or starts its own background forwards. Stop/clear cells are explicitly
disabled by default, so Run All leaves the deployed stack and its data available.
Edited notebooks should have their outputs cleared before committing/sharing.

The walkthrough was executed locally against the real ARM64 stack, including
deployment, a 300-vector smoke workload, all three dashboards, 16 rule behavior
tests, live alert firing/recovery/inhibition, performance output with an inline
latency plot, and log/evidence inspection. Validation reused loaded node images
and shortened performance measurement to 10 seconds; source defaults remain a
60-second performance window. Stop/clear remained disabled and notebook-owned
test forwards were closed afterward. The unexecuted source stays clean; local
execution evidence is under `.local/notebook-validation/`.

For the equivalent CLI workflow, run from this directory:

```bash
./stack.sh start
./stack.sh images --local-arm64
./stack.sh deploy --local-arm64
./stack.sh status
```

Defaults are profile/context `wp-ops-v0146` and namespace `milvus-stack`. Every
kubectl/Helm operation explicitly names the context. To isolate another cluster:

```bash
PROFILE=milvus-full-stack ./stack.sh start
PROFILE=milvus-full-stack ./stack.sh images --local-arm64
PROFILE=milvus-full-stack ./stack.sh deploy --local-arm64
```

`images` renders all charts, pulls only missing host images, and loads them into
minikube with `--daemon`. It includes monitoring components and the dynamically
created Prometheus configuration reloader. Docker Desktop can fail to export some
distroless images; in that case the command attempts a direct minikube image pull.
Interrupted host downloads are retried up to three times, with a 300-second limit
per attempt; set `IMAGE_PULL_TIMEOUT` higher when downloading large images over a
slow connection.

The `--local-arm64` flag must be supplied to `render`, `images`, and `deploy` when
using the supplied build. Without the flag, the example uses the public default
Milvus image. The private image reference is kept in a separate override file.

For an existing Kubernetes cluster, set `CONTEXT` and `NAMESPACE`, skip `start`
and `images`, and run `deploy`. Configure a dynamic StorageClass and registry
credentials appropriate to that cluster. The defaults use local example
credentials and should be reviewed before exposing services externally.

## Command guide

Run `./stack.sh help`, `./stack.sh --help`, or `./stack.sh deploy --help` for the
grouped command list, current targets, options and examples.

| Category | Commands | Purpose |
| --- | --- | --- |
| Guided walkthrough | `notebook` | Open the executable Jupyter guide with an isolated local runtime |
| Cluster lifecycle | `start` / `stop` | Create or resume / stop the local minikube profile; preserve deployments and data |
| Stack lifecycle | `deploy` / `clear` | Install or upgrade / uninstall the three Helm releases and delete their PVC data |
| Preparation | `charts`, `render`, `images` | Download charts, inspect manifests, preload local images |
| Access and status | `connections`, `forward`, `status` | Show connection/configuration hints, open local ports and inspect deployed resources |
| Validation | `acceptance`, `smoke`, `verify`, `alerts`, `alerts-test`, `alerts-smoke` | Exercise workloads, monitoring, rules and alert delivery |
| Write performance | `perf` | Measure concurrent inserts and retain correctness, metrics, alert and log evidence |
| Diagnostics | `collect` | Save bounded logs, resource status and verification reports |
| Optional email | `email-smoke` | Check local SMTP capture when Mailpit is explicitly enabled |

`start` / `stop` operate on `PROFILE`. `deploy` / `clear` operate on `CONTEXT`
and `NAMESPACE`, which may also point to an existing external cluster. `start`
resumes existing workloads; it does not reinstall Helm releases. `deploy` installs
or upgrades workloads in a running cluster; it does not start minikube.

To pause the local environment and later resume it:

```bash
./stack.sh stop
./stack.sh start
./stack.sh forward  # Reopen forwards after the cluster has resumed
```

To remove the stack and its data, then deploy a fresh environment:

```bash
./stack.sh clear
./stack.sh deploy --local-arm64
```

`clear` uninstalls all three releases before deleting their PVCs: Milvus/Woodpecker,
etcd/MinIO, Prometheus/Alertmanager and optional Grafana persistence. It selects
PVCs by this stack's release/component labels within the selected namespace;
unrelated PVCs are left alone. Missing releases/PVCs are allowed, so cleanup can
be repeated after a partial deployment or cleanup. The namespace, shared CRDs,
local evidence and minikube profile are preserved. Use `stop` / `start` when you
want to retain deployments and data.

Lifecycle CLI tests run with simulated backends, so they never stop a real cluster
or uninstall a real release:

```bash
python3 tests/cli.test.py
```

## Open Grafana, Prometheus, and Milvus

Keep this command running in a terminal:

```bash
./stack.sh forward
```

| Service | Local URL |
| --- | --- |
| Grafana | http://127.0.0.1:3000 — `admin` / `admin` |
| Milvus dashboard | http://127.0.0.1:3000/d/milvus-full-stack |
| Woodpecker client dashboard | http://127.0.0.1:3000/d/woodpecker-client-k8s |
| Woodpecker server dashboard | http://127.0.0.1:3000/d/woodpecker-server-k8s |
| Prometheus | http://127.0.0.1:9099 |
| Alertmanager | http://127.0.0.1:9093 |
| Milvus REST / SDK endpoint | http://127.0.0.1:19530 |

All forwards bind to loopback; Ctrl-C closes the four forwards. Override local
ports using `GRAFANA_PORT`, `PROMETHEUS_PORT`, `MILVUS_PORT`, and `ALERTMANAGER_PORT`. Use the same
overrides for the verification commands, or set `GRAFANA_URL`, `PROMETHEUS_URL`,
and `MILVUS_URL` explicitly. Grafana uses login authentication; anonymous admin
access is disabled. Provisioned dashboards survive pod recreation; exploratory
UI edits should be exported and reconciled with the JSON files.

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
./stack.sh forward --wpcli
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
./stack.sh smoke
# Allow at least two 15-second scrapes after the first workload.
./stack.sh verify
./stack.sh alerts
./stack.sh alerts-smoke  # Temporary synthetic events; no external notifications
./stack.sh collect
```

The default acceptance loop needs no SMTP service or Mailpit:

```bash
./stack.sh acceptance
```

It runs the offline rule tests, a Milvus workload, checks all three dashboards and
scrape targets, and exercises alert firing/resolution and inhibition. View the
alerts in Prometheus and Alertmanager using the URLs above. Email testing is
skipped by default.

For optional local SMTP acceptance, use the same profile on installation,
port forwarding and acceptance:

```bash
./stack.sh images --local-arm64 --mail-capture
./stack.sh deploy --local-arm64 --mail-capture
./stack.sh forward --mail-capture  # Keep running in a separate terminal
# In another terminal:
./stack.sh acceptance --mail-capture
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
BATCHES=120 BATCH_SIZE=100 INTERVAL=2 ./stack.sh smoke
```

For a measured concurrent write experiment with automatic diagnostic collection:

```bash
./stack.sh perf
# Or configure a longer window and larger offered load:
PERF_DURATION=300 PERF_CONCURRENCY=4 PERF_BATCH_SIZE=200 PERF_RPS=10 ./stack.sh perf
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
dashboard provisioning, and every visible PromQL target in all three dashboards.
It writes `.local/verification.json` with per-query errors and data coverage.
Queries without samples are recorded separately from rejected queries: a short
write/search workload does not exercise every error, compaction, import, sparse
index, or other optional feature. The default topology checks must be adjusted if
you deliberately change replica counts.

`collect` writes timestamped artifacts under `.local/evidence/`: resource status,
events, image versions, bounded container logs, Helm release versions, and live
dashboard query coverage. It does not export Secrets or Helm values. Review
application logs before sharing artifacts. The deploy and verify commands return
nonzero on failure; the evidence command attempts to retain useful diagnostics
even when observability verification fails.

## Dashboard sources and label contract

- **Milvus:** supplied production export `milvus_grafana_dashboard.json`; its source
  SHA256 and transformations are recorded in `dashboards-sources.json`.
- **Woodpecker:** `tests/k8s/monitor/grafana/templates/dashboard_client_k8s.json`
  and `dashboard_server_k8s.json`.
- All three JSON files are included in `charts/full-stack-support/dashboards/`;
  installation does not require files outside this directory.
- Dashboard ConfigMaps are imported by Grafana's dashboard sidecar, with a stable
  `prometheus` datasource UID and links between the three dashboards.
- Application scrape labels include `cluster=local`, `namespace`,
  `app_kubernetes_io_name=milvus`, `app_kubernetes_io_instance=milvus`, `component`,
  `pod`, and `woodpecker_id=milvus`. Kube-state-metrics and kubelet/cAdvisor also
  receive `cluster=local`. The exported dashboard's Kubernetes queries depend on
  these infrastructure collectors, which are enabled here.
- Production datasource UIDs, selected namespaces/instances, and private Grafana
  navigation are removed. The production Loki events panel becomes an explanation
  of local event evidence collection; no logging stack is installed.
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

Review changes, redeploy, and rerun the workload/verification. Dashboard sections
named “Alerts” in the WP templates are charts of threshold-related metrics.
The supporting chart separately installs the 31 reference alerts, and the monitoring
chart enables Alertmanager. `./stack.sh alerts-test` runs 16 offline behavioral
tests with the pinned Prometheus image; `alerts-smoke` checks firing, delivery,
resolution, inhibition, and synthetic-event cleanup against the local stack.

## Configuration and lifecycle

- `values/milvus.yaml`: upstream chart overrides, including three WP services,
  S3/etcd connectivity, persistence, and resource sizing.
- `values/local-arm64.yaml`: supplied local Milvus image override.
- `values/monitoring.yaml`: metrics collection, Grafana provisioning, Alertmanager
  grouping/inhibition, local-only receiver, and 8-day / 3-GB-capped retention.
- `values/mail-capture.yaml`: optional Mailpit and SMTP receiver with the example recipient.
- `values/email-smtp.example.yaml`: external SMTP template with Secret-mounted credentials.
- `charts/full-stack-support/`: small supporting chart for persistent single-node
  etcd/MinIO, PodMonitors, dashboard ConfigMaps, and PrometheusRule resources. The upstream etcd dependency
  expects a different image layout; the supplied official etcd image is deployed
  directly here instead. Milvus consumes these through external dependency settings.
- `stack.sh`: chart download, rendering, preload, installation, access and evidence.

Use `./stack.sh render --local-arm64` to inspect manifests before deployment.
Chart archives, rendered manifests, and results are ignored under `.local/`.
Additional Milvus overrides can be supplied with `-f path/to/values.yaml`; use the
same flags for rendering, image loading, and deployment. Keep changes to S3 access
credentials consistent between the support chart and Milvus values.
Use `--monitoring-values file` and `--support-values file` for their corresponding
releases. Repeat the profile flags on subsequent deployments; Helm values are
reconstructed from the supplied files, so omitting `--mail-capture` disables it.

Re-running `deploy` performs a Helm upgrade. Pin and review chart/image changes,
then verify them with the same workload and dashboards. Monitor CRDs are installed
by kube-prometheus-stack; follow that chart's CRD upgrade instructions when changing
its version. The scripts do not delete shared CRDs.

```bash
# Remove the stack AND its persistent data; keep the cluster for a fresh deploy.
./stack.sh clear
# Or pause the whole local cluster while retaining the deployed stack:
./stack.sh stop
```

`clear` resets etcd, MinIO and Woodpecker storage together, preventing log-ID reuse
from a partial metadata/WAL reset. It also deletes Prometheus history and
Alertmanager state. Local `.local/` reports and chart/image caches remain available.

The default minikube StorageClass uses the `Delete` reclaim policy, so removing
these PVCs also reclaims their backing volumes. On an external cluster, a `Retain`
policy can leave backing volumes/data for the storage administrator to remove.
External S3 buckets or pre-existing storage outside this stack's labeled PVCs are
not erased by `clear`.

## Troubleshooting

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
kubectl --context wp-ops-v0146 -n milvus-stack logs deploy/milvus-streamingnode --tail=100
kubectl --context wp-ops-v0146 -n milvus-stack logs milvus-woodpecker-0 --tail=100
```

- `ImagePullBackOff`: run `images` with the same override used for `deploy`, check
  host registry credentials, and confirm the image supports the node architecture.
- Pending PVCs: confirm the default StorageClass and minikube storage provisioner.
- OOM/restarts: inspect events and raise Docker/minikube memory and component limits.
- Empty charts: run the workload, select the local datasource/namespace/instance,
  allow scrapes to complete, then inspect `.local/verification.json`. Check whether
  the selected image exports that metric and whether the feature was exercised.
- Missing WP client traffic: inspect Milvus's generated config for
  `woodpecker.storage.type: service`, its seed list, and server operation counters.

The intended development loop is: deploy → run workload → verify/collect → inspect
gaps → update metrics/configuration/dashboards → rerun. Future work can extend this
with authorized email notifications, automated failure/performance scenarios,
agent orchestration, and a separate `milvus-full-stack-operator/` installation path.
