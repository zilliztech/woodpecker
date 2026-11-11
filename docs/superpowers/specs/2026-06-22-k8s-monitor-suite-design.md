# Woodpecker — `log_ns` metric-label rename + K8s (minikube) Monitoring Suite & Dashboards

- **Issue:** [zilliztech/woodpecker#193](https://github.com/zilliztech/woodpecker/issues/193)
- **Date:** 2026-06-22
- **Status:** Approved design, ready for implementation plan

## Problem

The repo has a Docker-Compose monitoring suite (`tests/docker/monitor/`) that brings
up a 4-node Woodpecker cluster + Prometheus + Grafana, drives I/O, and verifies that
`woodpecker_*` metrics report correctly, plus Grafana dashboard templates.

We have no equivalent for a **Kubernetes** deployment. In production (Zilliz Cloud /
VDC) Woodpecker runs under the **Woodpecker operator** and is scraped by a **Prometheus
Operator** via a **`PodMonitor`** CR. That path is untested locally/in CI.

The deeper issue is a **label-naming collision**:

- Woodpecker emits its own metric label **`namespace`** =
  `BuildMetricsNamespace(bucket, rootPath)` (e.g. `woodpecker/files`) — an
  *application/logical* namespace, **not** the k8s namespace. But `namespace` is a
  reserved dimension in any k8s scrape (Prometheus attaches the pod's k8s namespace as
  a target label `namespace`). The two fight: on VDC the production `PodMonitor` uses
  `honorLabels: true`, so the **app** label wins and the **real k8s namespace is
  destroyed** — you can't filter metrics by k8s namespace / instance.

The fix is to stop using the k8s-reserved word: **rename the application metric label
`namespace` → `log_ns`** at the source. Then the k8s `namespace` target label is free
to mean the k8s namespace, `log_ns` carries the logical namespace, and there is no
collision regardless of `honorLabels`. Dashboards then filter by `cluster`, k8s
`namespace`, and `log_ns` independently.

## Goals

This is **three coordinated workstreams** (A is a prerequisite for the dashboards in B
and C to line up with the data):

- **A. Source rename.** Rename the metric label `namespace` → `log_ns` in
  `common/metrics/` (client + service metrics) so Woodpecker never emits the
  k8s-reserved `namespace` label.
- **B. Update the existing Docker monitor dashboards** (`tests/docker/monitor/`) to use
  `log_ns` instead of `namespace`, matching the renamed metric.
- **C. New `tests/k8s/monitor/` suite** — a **minikube** mirror of the Docker suite:
  one command does **build → deploy (operator + WoodpeckerCluster) → install monitoring
  → run workload → verify metrics**, leaving Grafana reachable to open the dashboard URL
  (`--no-cleanup`, same ergonomics as Docker). Monitoring is installed the **simplest**
  way — the `kube-prometheus-stack` Helm chart (Operator + Prometheus + Grafana),
  trimmed of extras — scraping Woodpecker via a `PodMonitor`. Ships **new k8s server +
  client dashboard templates** whose variables pre-filter by `cluster`, k8s `namespace`,
  and `log_ns`, so you can isolate one instance's server or client behavior.

Reuse the existing operator bring-up library (`deployments/operator/test/lib.sh`) and
the chaos suite's minikube helpers (`preload_image`, zone-label, `push_workload`) — this
is "add monitoring + dashboards," not "build a k8s e2e from scratch."

## Non-Goals

- Testing Prometheus/Grafana themselves, alerting rules, or long-term storage.
- Updating **production VDC** dashboards. After the renamed Woodpecker version ships,
  prod dashboards (incl. the ones fixed earlier this week) must swap `namespace` →
  `log_ns`; that rollout is a follow-up, tracked separately. The prod `PodMonitor`
  itself needs **no** change — once the app emits `log_ns`, its `honorLabels: true`
  already yields a clean k8s `namespace` + `log_ns`.
- Multi-node / multi-AZ minikube. Single node with a synthetic zone label suffices.

## Grounding (verified facts)

- **Rename is mechanical & contained.** The label key `"namespace"` appears only in the
  `[]string{…}` label-name slices — 21 in `client_metrics.go`, 24 in
  `service_metrics.go`. Values are passed **positionally** (`WithLabelValues(namespace,
  …)`), so renaming the key strings is sufficient; no call-site signature changes. No
  code outside `common/metrics/` references the label name. `BuildMetricsNamespace`
  builds the *value* (`bucket/rootPath`) and is unaffected (the value stays; only the
  label key changes).
- **Operator labels & port match the wiki `PodMonitor` as-is.** `labels.go` sets
  `app.kubernetes.io/{name=woodpecker, instance=<CR>, component=server}`;
  `reconcile_statefulset.go` exposes container port **`metrics`** (`spec.MetricsPort`,
  default 9091) serving `/metrics` and `/healthz`. No operator changes needed.
- **In k8s the scrape `job` is `woodpecker/woodpecker-server`** (`<ns>/<podmonitor>`),
  matching the wiki's `server_job` regex.
- **Bring-up is reusable.** `lib.sh` provides `wp_minikube_start`, `wp_deploy_operator`,
  `wp_build_wp_image`, `wp_deploy_deps` (etcd+MinIO), `wp_create_cr`, `wp_wait_healthy`,
  `wp_launch_client_pod`. The chaos `run_network_chaos.sh` / `smoke-test.sh` add
  `preload_image` (host-pull → `minikube image load`, dodging the host loopback-proxy
  that blocks the node from registries), single-node zone labeling, and `push_workload`
  (build a Go binary on the host, `kubectl cp` into a pod, exec it).

## Workstream A — rename `namespace` → `log_ns` in `common/metrics/`

- In `client_metrics.go` and `service_metrics.go`, change every label-name slice entry
  `"namespace"` → `"log_ns"` (e.g. `[]string{"node_id", "namespace", "log_id"}` →
  `[]string{"node_id", "log_ns", "log_id"}`). Positional `WithLabelValues(...)` calls
  are unchanged. Optionally rename local vars/params named `namespace` → `logNs` for
  readability (cosmetic).
- Keep `BuildMetricsNamespace` (returns the value); optionally add a doc note that its
  result populates the `log_ns` label.
- Update `common/metrics/*_test.go` that assert on label names; run the package tests.
- This is a **breaking change** to metric output — see Risks.

## Workstream B — update Docker monitor dashboards to `log_ns`

In `tests/docker/monitor/grafana/templates/`:

- `dashboard_server.json`: rename the `namespace` template variable → `log_ns`
  (query `query_result(max by (log_ns)(woodpecker_server_logstore_active_logs{…,
  log_ns=~".+"}))`, regex `/log_ns="([^"]+)"/`); the `log_id` variable's inner filter
  `namespace=~"$namespace"` → `log_ns=~"$log_ns"`; every panel selector
  `namespace=~"$namespace"` → `log_ns=~"$log_ns"`.
- `dashboard_client.json`: rename the `namespace` variable → `log_ns` (query
  `label_values(woodpecker_client_append_requests_total, log_ns)`); panels
  `namespace=~"$namespace"` → `log_ns=~"$log_ns"`.
- `README.md` metrics tables: the "Labels" column `namespace` → `log_ns`.
- `monitor_test.go`: update any PromQL that references the `namespace` label (verified
  during implementation).

> Note: the Docker dashboard files currently carry an **uncommitted** half-conversion
> to a `${datasource}` variable (no variable defined → broken). This work will land them
> in a coherent state — default plan: revert the stray `${datasource}` edit back to the
> committed `__DATASOURCE_UID__` base (which `grafana/setup.go` expects) and apply the
> `log_ns` rename on top. (If you'd rather also finish the datasource-variable
> conversion, say so and we'll fold it in.)

## Workstream C — k8s (minikube) monitoring suite

### Directory layout (mirrors `tests/docker/monitor/`)

```
tests/k8s/monitor/
├── README.md
├── run_monitor_tests.sh          # one-click bring-up + monitoring install + test
├── k8s_monitor_cluster.go        # K8sMonitorCluster: kubectl/port-forward + Prometheus query helpers
├── monitor_test.go               # workload + woodpecker_{server,client}_* metric verification
├── loadgen/
│   └── main.go                   # in-cluster client load generator: write/read loop + /metrics endpoint
├── manifests/
│   ├── podmonitor-server.yaml    # PodMonitor for server pods (port metrics) — matches prod wiki
│   ├── podmonitor-client.yaml    # PodMonitor for the client load-gen pod
│   ├── client-loadgen.yaml       # client pod: labels component=client + named port metrics
│   ├── kube-prometheus-values.yaml  # trimmed Helm values (externalLabels.cluster, grafana, extras off)
│   └── dashboard-configmaps.yaml # ConfigMap(s) labeled grafana_dashboard="1" wrapping the dashboard JSON
└── grafana/templates/
    ├── dashboard_server_k8s.json # server vars: prometheus/cluster/namespace/log_ns/log_id/server_job
    └── dashboard_client_k8s.json # client vars: prometheus/cluster/namespace/log_ns
```

The Prometheus-query helpers (`WaitForPrometheusReady`, `QueryMetric`,
`QueryMetricHasValue`, `QueryPromQL`) are lifted from the Docker suite's
`monitor_cluster.go`; they just hit an HTTP endpoint, pointed at a `kubectl
port-forward` local port instead of `localhost:9090`.

### Bring-up flow (`run_monitor_tests.sh`)

1. `wp_minikube_start`; label the node with synthetic `topology.kubernetes.io/zone` +
   `region` (single-node spread-constraint workaround, from chaos).
2. `preload_image` etcd + MinIO; `wp_deploy_operator`; `wp_build_wp_image`;
   `wp_deploy_deps`; `wp_create_cr` (namespace `woodpecker`); `wp_wait_healthy`.
3. **Install monitoring (Helm):** `helm repo add prometheus-community … && helm upgrade
   --install kps prometheus-community/kube-prometheus-stack -n monitoring
   --create-namespace -f manifests/kube-prometheus-values.yaml --wait`, preloading the
   chart's images with the chaos `helm template … | preload_image` trick.
4. `kubectl apply` the server + client `PodMonitor`s, the client load-gen pod, and the
   dashboard ConfigMaps.
5. Run workload + `go test`: the load-gen pod drives client writes/reads — populating
   both `woodpecker_server_*` (via the servers) and `woodpecker_client_*` (its own
   process); the test verifies via port-forwarded Prometheus (below).
6. `--no-cleanup`: print the Grafana URL (`minikube service -n monitoring kps-grafana
   --url`) + Prometheus URL, leave the cluster up. Otherwise `helm uninstall` + delete
   CR/deps (or `minikube delete`).

Knobs (`CLUSTER_NAME=wp-monitor`, `CR_NAME`, `NAMESPACE=woodpecker`, `REPLICAS=3`,
`WP_IMG`, `MK_*`) are exported before sourcing `lib.sh`, like `smoke-test.sh`.

### Monitoring stack — `kube-prometheus-values.yaml` (minimal)

```yaml
# Trim everything we don't need; the suite tests Woodpecker, not the monitoring stack.
nodeExporter:        { enabled: false }
kubeStateMetrics:    { enabled: false }
alertmanager:        { enabled: false }
defaultRules:        { create: false }
kubeApiServer:       { enabled: false }   # (other kube* scrape jobs off — only Woodpecker)
prometheus:
  prometheusSpec:
    externalLabels: { cluster: woodpecker-minikube }   # enables the `cluster` variable
    podMonitorSelectorNilUsesHelmValues: false         # select our PodMonitors w/o the release label
    serviceMonitorSelectorNilUsesHelmValues: false
    retention: 2h
grafana:
  adminPassword: admin
  grafana.ini:                                # frictionless viewing (like the Docker suite)
    auth.anonymous: { enabled: true, org_role: Admin }
    auth: { disable_login_form: true }
  sidecar:
    dashboards: { enabled: true, label: grafana_dashboard, labelValue: "1" }
  # Prometheus datasource auto-provisioned by the chart (name "Prometheus", default).
```

Grafana auto-provisions the datasource and the sidecar imports any ConfigMap labeled
`grafana_dashboard: "1"` — **no custom Grafana setup code** (unlike the Docker suite's
`grafana/setup.go`).

### `PodMonitor` (matches the prod wiki — no relabel needed)

Because Workstream A makes the app emit `log_ns` (not `namespace`), there is **no label
collision** and therefore **no `metricRelabelings`**. The `PodMonitor` is literally the
deploy wiki's:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: woodpecker-server
  namespace: woodpecker
  labels: { app.kubernetes.io/name: woodpecker, app.kubernetes.io/component: server }
spec:
  namespaceSelector: { matchNames: [woodpecker] }
  selector:
    matchLabels:
      app.kubernetes.io/name: woodpecker
      app.kubernetes.io/component: server
      app.kubernetes.io/instance: my-woodpecker   # = $CR_NAME
  podMetricsEndpoints:
    - port: metrics
      path: /metrics
      interval: 15s
      honorLabels: true     # matches the wiki; after the rename, true/false are equivalent (no collision)
```

Result on every server series: `cluster` (external label), `namespace` = **k8s
namespace** (`woodpecker`, the target label), `log_ns` = **app/logical namespace**
(`woodpecker/files`), plus `node_id`, `log_id`, `job=woodpecker/woodpecker-server`,
`pod`, etc.

A **second `PodMonitor` (`woodpecker-client`)** is identical except its `selector`
matches `app.kubernetes.io/component: client` (the load-gen pod), producing
`woodpecker_client_*` under the same `cluster` / `namespace` (k8s) / `log_ns` scheme.

### Workload + verification

- **Client load generator** (`loadgen/main.go`) runs as an **in-cluster pod** labeled
  `app.kubernetes.io/{name=woodpecker, component=client, instance=<CR>}` with a named
  container port `metrics`. Built on the host (`GOOS=linux`) and delivered via the chaos
  `push_workload` pattern (`kubectl cp` + exec), it creates a Woodpecker client against
  the in-cluster servers/etcd/MinIO, calls `RegisterClientMetricsWithRegisterer` and
  serves `promhttp` on the `metrics` port (the mechanism the Docker suite uses on
  `:29099`), and runs a continuous append/read loop. One workload populates **both**
  `woodpecker_server_*` and `woodpecker_client_*`.
- **Verification** (`monitor_test.go`, host-side): `kubectl port-forward -n monitoring
  svc/prometheus-operated 9090` (the operator always creates this stable headless
  service for the Prometheus CR, regardless of Helm release name), then assert:
  1. Both targets **Up** — `up{job="woodpecker/woodpecker-server"}==1` (count ==
     REPLICAS) and `up{job="woodpecker/woodpecker-client"}==1`.
  2. Representative `woodpecker_server_*` **and** `woodpecker_client_*` metrics exist
     with non-zero values after the workload.
  3. The label scheme is correct: series carry `cluster="woodpecker-minikube"`,
     `namespace="woodpecker"` (k8s), and `log_ns="woodpecker/files"` (app), and **no
     `namespace` with a slash / no `exported_namespace`** — the regression guard proving
     the source rename took effect and the k8s namespace is intact.

### New k8s dashboards

**Server (`dashboard_server_k8s.json`)** — derived from `dashboard_server.json`.
Template variables (`templating.list`, in order):

| var | type | query | regex |
|---|---|---|---|
| `prometheus` | datasource | (type `prometheus`, picks the provisioned datasource) | — |
| `cluster` | query | `label_values(go_info, cluster)` | — |
| `namespace` | query | `label_values(go_info{job=~"woodpecker-node[0-9]+\|woodpecker/woodpecker-server", cluster=~"$cluster"}, namespace)` | — |
| `log_ns` | query | `query_result(max by (log_ns)(woodpecker_server_logstore_active_logs{job=~"woodpecker-node[0-9]+\|woodpecker/woodpecker-server", namespace=~"$namespace", log_ns=~".+"}))` | `/log_ns="([^"]+)"/` |
| `log_id` | query | `query_result(max by (log_id)(woodpecker_server_logstore_active_segments{job=~"woodpecker-node[0-9]+\|woodpecker/woodpecker-server", namespace=~"$namespace", log_ns=~"$log_ns", log_id=~".+"}))` | `/log_id="([^"]+)"/` |
| `server_job` | query | `label_values(go_goroutines{job=~"woodpecker-node[0-9]+\|woodpecker/woodpecker-server"}, job)` | — |

All datasource refs use `${prometheus}`. Each of the 70 panel selectors is rewritten
`{namespace=~"$namespace", log_id=~"$log_id"}` →
`{cluster=~"$cluster", namespace=~"$namespace", log_ns=~"$log_ns", log_id=~"$log_id"}`
(old app `namespace` filter becomes `log_ns`; new `cluster` + k8s `namespace` added).

**Client (`dashboard_client_k8s.json`)** — derived from `dashboard_client.json` (single
`namespace` variable today). Prepend the three selectors and rename:

| var | type | query |
|---|---|---|
| `prometheus` | datasource | (type `prometheus`) |
| `cluster` | query | `label_values(go_info, cluster)` |
| `namespace` | query | `label_values(woodpecker_client_append_requests_total{cluster=~"$cluster"}, namespace)` |
| `log_ns` | query | `label_values(woodpecker_client_append_requests_total{cluster=~"$cluster", namespace=~"$namespace"}, log_ns)` |

The 14 client panel selectors: `{namespace=~"$namespace"}` →
`{cluster=~"$cluster", namespace=~"$namespace", log_ns=~"$log_ns"}` (panels still group
by `log_id`). Both k8s dashboards reference `${prometheus}` and ship as
`grafana_dashboard="1"` ConfigMaps.

## Testing

- `run_monitor_tests.sh` is the e2e entry point; `monitor_test.go` is the assertion
  layer. Failure collects `kubectl` describe/logs for the operator, server pods,
  Prometheus, and the PodMonitor target page — mirroring the Docker suite's
  `collect_logs_on_failure`.
- `go test ./common/metrics/...` proves the rename compiles and label-name assertions
  pass.
- A tiny unit check validates all four dashboard templates (2 Docker + 2 k8s) parse and
  that the k8s ones reference only `${prometheus}` (no stray `__DATASOURCE_UID__` /
  hard-coded UID), guarding the templates the way the earlier datasource bug would have
  been caught.

## Risks / open considerations

- **Breaking metric change.** Renaming `namespace` → `log_ns` changes Woodpecker's
  emitted labels. Any dashboard/alert/query filtering on the app `namespace` (the VDC
  dashboards, including the ones just fixed) breaks until updated to `log_ns`, and only
  after the renamed Woodpecker version is deployed. Workstreams B updates the in-repo
  Docker dashboards; prod dashboards are the tracked follow-up. Worth a CHANGELOG/PR
  note.
- **Docker dashboard WIP.** The uncommitted `${datasource}` half-edit must be reconciled
  first (default: revert to the committed `__DATASOURCE_UID__` base) — see Workstream B.
- **Image pulls on minikube behind a loopback proxy** — mitigated by the chaos
  host-pull-then-`minikube image load` pattern, applied to kube-prometheus-stack images.
- **Resource footprint** — trimming extras keeps it light; `MK_CPUS=4 / 4096MB` should
  suffice for 3 replicas + slim Prometheus + Grafana; README suggests 6/8192 if Pending.
