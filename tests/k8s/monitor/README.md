# Woodpecker K8s Monitor E2E Tests

End-to-end verification of the Woodpecker Prometheus monitoring stack on Kubernetes (minikube).

## Prerequisites

- [minikube](https://minikube.sigs.k8s.io/) >= 1.32
- [docker](https://docs.docker.com/get-docker/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [helm](https://helm.sh/docs/intro/install/) >= 3.12
- [go](https://go.dev/dl/) >= 1.21

## Running the Tests

### One-click run (creates and destroys a minikube cluster)

```bash
./run_monitor_tests.sh
```

### Keep the cluster after tests (for manual inspection)

```bash
./run_monitor_tests.sh --no-cleanup
```

After a `--no-cleanup` run, the script prints the Grafana URL and the
`kubectl port-forward` command for Prometheus.

### Tuning resource limits

If pods stay in `Pending` state (CPU/memory pressure), increase minikube
resources before running:

```bash
MK_CPUS=6 MK_MEMORY=8192 ./run_monitor_tests.sh
```

## Label Scheme: `namespace` / `log_ns`

Woodpecker metrics carry two key labels:

| Label | Value | Source |
|-------|-------|--------|
| `namespace` | Kubernetes namespace (e.g. `woodpecker`) | injected by kube-prometheus-stack from the PodMonitor namespace |
| `log_ns` | Woodpecker logical log namespace (e.g. `my-log`) | emitted by the server/client as a metric label |

Dashboards derive everything else from these: the `namespace` dropdown is
discovered via `go_info`, and the hidden `server_job` variable resolves the
PodMonitor-assigned `job` label within the selected namespace (also via
`go_info`), so no job name or cluster name is hardcoded.

### Breaking-change note for production dashboards

The Woodpecker metric label previously called `namespace` (which referred to
the **logical** log namespace) has been renamed to `log_ns`. Any production
Grafana dashboards that filter or group by `namespace` assuming it means the
Woodpecker log namespace **must be updated** to use `log_ns` instead.

After the rename the Kubernetes namespace is available as `namespace` and the
Woodpecker logical namespace is `log_ns` — there is no ambiguity.

**The prod `PodMonitor` manifests require no change** after this rename; the
label is emitted by the application itself and is already present in the
scraped metric.

## What the Tests Verify

`TestK8sMonitor_Metrics` (in `monitor_test.go`) runs only when
`WP_K8S_PROM_URL` is set (the runner sets it via `kubectl port-forward`).
Without a live cluster the test SKIPs automatically, so `go test ./...` is
always safe.

The test checks:

1. **ServerTargetUp** — at least one `woodpecker-server` scrape target is `up`.
2. **ClientTargetUp** — the `woodpecker-client` (load generator) scrape target is `up`.
3. **ServerMetrics** — core server metrics are present and non-zero.
4. **ClientMetrics** — core client metrics are present and non-zero.
5. **LabelScheme** — every `woodpecker_server_logstore_active_logs` series
   carries `namespace=woodpecker`, a non-empty `log_ns`, and no
   `exported_namespace` (which would indicate a relabeling conflict).
