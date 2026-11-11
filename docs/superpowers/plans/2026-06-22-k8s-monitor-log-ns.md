# `log_ns` Rename + K8s Monitoring Suite — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the Woodpecker app metric label `namespace`→`log_ns`, update the Docker monitor dashboards to match, and add a minikube monitoring E2E suite (`kube-prometheus-stack` + `PodMonitor`) with new k8s server/client dashboard templates.

**Architecture:** Three phases. **A** renames the label at the source (`common/metrics/`). **B** updates the in-repo Docker dashboards/tests/docs to `log_ns`. **C** adds `tests/k8s/monitor/` — a minikube mirror of `tests/docker/monitor/` that reuses `deployments/operator/test/lib.sh` for bring-up, installs `kube-prometheus-stack` via Helm, scrapes Woodpecker via a `PodMonitor` (identical to the prod wiki, no relabel needed after A), drives an in-cluster client load generator, and verifies `woodpecker_{server,client}_*` metrics over a port-forwarded Prometheus.

**Tech Stack:** Go 1.24, Prometheus `client_golang`, Prometheus Operator CRDs (`monitoring.coreos.com/v1`), `kube-prometheus-stack` Helm chart, minikube, kubectl, Grafana.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-22-k8s-monitor-suite-design.md`. Issue: #193. Branch: `feat/k8s-monitor-log-ns`.
- Go module: `github.com/zilliztech/woodpecker`. License header (Apache, the 16-line block at the top of existing `.go`/`.sh` files) is REQUIRED on every new `.go` and `.sh` file — copy it verbatim from `tests/docker/monitor/run_monitor_tests.sh`.
- The metric label key string is exactly `"log_ns"` (snake_case). The k8s namespace stays the target label `namespace`.
- k8s suite knobs (exported before sourcing `lib.sh`): `CLUSTER_NAME=wp-monitor`, `CR_NAME=my-woodpecker`, `NAMESPACE=woodpecker`, `REPLICAS=3`, monitoring Helm release `kps` in namespace `monitoring`, Prometheus external label `cluster=woodpecker-minikube`.
- Woodpecker server pods are labeled `app.kubernetes.io/{name=woodpecker, instance=$CR_NAME, component=server}` and expose container port `metrics` (9091). The client load-gen pod is labeled `…/component=client` with a named port `metrics` (29099).
- TDD throughout: failing test → run → minimal change → run → commit. Frequent commits, each referencing `#193`.

---

## Phase A — Rename `namespace` → `log_ns` in `common/metrics/`

### Task A1: Rename the metric label key

**Files:**
- Modify: `common/metrics/service_metrics.go` (24 label-name slice entries)
- Modify: `common/metrics/client_metrics.go` (21 label-name slice entries)
- Test: `common/metrics/metrics_registration_test.go` (add a label-name guard)

**Interfaces:**
- Consumes: existing `RegisterServerMetricsWithRegisterer(prometheus.Registerer)` and `RegisterClientMetricsWithRegisterer(prometheus.Registerer)`.
- Produces: all `woodpecker_server_*` / `woodpecker_client_*` metrics now carry label `log_ns` (was `namespace`). No function signatures change.

- [ ] **Step 1: Write the failing test**

Add to `common/metrics/metrics_registration_test.go`:

```go
// TestMetrics_UseLogNsLabel guards the rename of the application namespace
// label to log_ns (issue #193): the k8s scrape owns `namespace`, so the app
// must not emit it.
func TestMetrics_UseLogNsLabel(t *testing.T) {
	check := func(t *testing.T, reg *prometheus.Registry, metricName string) {
		mfs, err := reg.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() != metricName {
				continue
			}
			for _, m := range mf.GetMetric() {
				names := map[string]bool{}
				for _, lp := range m.GetLabel() {
					names[lp.GetName()] = true
				}
				require.True(t, names["log_ns"], "%s must carry log_ns", metricName)
				require.False(t, names["namespace"], "%s must NOT carry namespace", metricName)
				return
			}
		}
		t.Fatalf("metric %s not found in registry", metricName)
	}

	t.Run("server", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		RegisterServerMetricsWithRegisterer(reg)
		WpLogStoreActiveLogs.WithLabelValues("node-1", "bucket/root").Set(1)
		check(t, reg, "woodpecker_server_logstore_active_logs")
	})
	t.Run("client", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		RegisterClientMetricsWithRegisterer(reg)
		WpClientAppendRequestsTotal.WithLabelValues("bucket/root", "1").Inc()
		check(t, reg, "woodpecker_client_append_requests_total")
	})
}
```

Ensure the file imports `"github.com/prometheus/client_golang/prometheus"` and `"github.com/stretchr/testify/require"` (add if missing).

- [ ] **Step 2: Run the test, verify it FAILS**

Run: `go test ./common/metrics/ -run TestMetrics_UseLogNsLabel -v`
Expected: FAIL — `... must carry log_ns` (label is still `namespace`).

- [ ] **Step 3: Apply the rename**

The key string `"namespace"` appears in these two files **only** inside `[]string{…}` label-name slices (verified: no other quoted `"namespace"` occurrences; the metric-name prefix uses the identifier `woodpeckerNamespace`, untouched). Replace all:

```bash
cd /Users/zilliz/workspace_zilliz/e2e_src/woodpecker
sed -i '' 's/"namespace"/"log_ns"/g' common/metrics/service_metrics.go common/metrics/client_metrics.go
```

Verify nothing else changed and counts are right:

```bash
grep -c '"log_ns"' common/metrics/service_metrics.go   # expect 24
grep -c '"log_ns"' common/metrics/client_metrics.go     # expect 21
grep -rn '"namespace"' common/metrics/                  # expect: no matches
```

- [ ] **Step 4: Run the metrics package tests, verify PASS**

Run: `go test ./common/metrics/... -v`
Expected: PASS, including `TestMetrics_UseLogNsLabel` and the existing `TestBuildMetricsNamespace` (value builder is unchanged).

- [ ] **Step 5: Build the whole module to confirm no call-site breakage**

Run: `go build ./...`
Expected: success (call sites pass the value positionally; only the label key changed).

- [ ] **Step 6: Commit**

```bash
git add common/metrics/service_metrics.go common/metrics/client_metrics.go common/metrics/metrics_registration_test.go
git commit -m "refactor(metrics): rename app metric label namespace -> log_ns (#193)

The k8s scrape owns the namespace label; emitting our own collides with it.
Renaming to log_ns frees namespace for the real k8s namespace.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Phase B — Update Docker monitor dashboards/tests/docs to `log_ns`

### Task B1: Swap `namespace`→`log_ns` in the Docker monitor templates

**Files:**
- Modify: `tests/docker/monitor/grafana/templates/dashboard_server.json`
- Modify: `tests/docker/monitor/grafana/templates/dashboard_client.json`
- Modify: `tests/docker/monitor/README.md` (metrics-table "Labels" column)
- Modify: `tests/docker/monitor/monitor_test.go` (only if it references the `namespace` label in PromQL)

**Interfaces:**
- Consumes: the renamed metric label from Task A1.
- Produces: Docker dashboards that filter on `log_ns` (variable renamed; `__DATASOURCE_UID__` datasource refs preserved for `grafana/setup.go`).

- [ ] **Step 1: Confirm the templates are on the committed `__DATASOURCE_UID__` base**

Run: `git -C /Users/zilliz/workspace_zilliz/e2e_src/woodpecker status --short tests/docker/monitor/grafana/templates/`
Expected: no output (clean). If there are stray `${datasource}` edits, `git checkout -- tests/docker/monitor/grafana/templates/dashboard_*.json` first.

- [ ] **Step 2: Write the transform + verification script**

Create `tests/docker/monitor/grafana/templates/.rename_log_ns.py` (a throwaway transform; deleted in Step 5):

```python
import json, sys, re, pathlib

DIR = pathlib.Path(__file__).parent

def transform(path):
    raw = path.read_text()
    obj = json.loads(raw)

    # 1. rename template variables named "namespace" -> "log_ns"
    for v in obj.get("templating", {}).get("list", []):
        if v.get("name") == "namespace":
            v["name"] = "log_ns"

    # 2. in every string field, rewrite the label key and the variable ref.
    #    - PromQL label key:  namespace=~ / max by (namespace) / label_values(..., namespace)
    #    - Grafana var ref:   $namespace  and  ${namespace}
    #    - regex extractor:   /namespace="([^"]+)"/
    def fix(s):
        s = s.replace('namespace=~', 'log_ns=~')
        s = s.replace('max by (namespace)', 'max by (log_ns)')
        s = re.sub(r'label_values\(([^,]*),\s*namespace\)', r'label_values(\1, log_ns)', s)
        s = s.replace('$namespace', '$log_ns').replace('${namespace}', '${log_ns}')
        s = s.replace('namespace="([^"]+)"', 'log_ns="([^"]+)"')
        return s

    def walk(o):
        if isinstance(o, dict):
            return {k: walk(fix(val) if isinstance(val, str) else val) for k, val in o.items()}
        if isinstance(o, list):
            return [walk(x) for x in o]
        return o

    obj = walk(obj)
    path.write_text(json.dumps(obj, indent=2) + "\n")
    blob = json.dumps(obj)
    assert '$namespace' not in blob and '${namespace}' not in blob, f"{path}: stray $namespace"
    assert 'namespace=~' not in blob, f"{path}: stray namespace=~ filter"
    assert '__DATASOURCE_UID__' in blob, f"{path}: datasource placeholder lost"
    print(f"OK {path.name}: log_ns refs =", blob.count('log_ns'))

for name in ("dashboard_server.json", "dashboard_client.json"):
    transform(DIR / name)
```

- [ ] **Step 3: Run the transform, verify assertions pass**

Run: `python3 tests/docker/monitor/grafana/templates/.rename_log_ns.py`
Expected: two `OK …: log_ns refs = N` lines, no assertion errors. (Server has the `namespace`+`log_id`+`server_job` vars; client has the single var.)

- [ ] **Step 4: Update the README labels + check `monitor_test.go`**

In `tests/docker/monitor/README.md`, in the metrics-reference tables, change the `namespace` label entries in the "Labels" column to `log_ns` (the rows for `woodpecker_server_logstore_*`, `woodpecker_server_file_*`, `woodpecker_client_*`, etc.). Then check the test for PromQL referencing the label:

```bash
grep -n 'namespace' tests/docker/monitor/monitor_test.go
```
Expected: the test asserts only metric *names* (e.g. `woodpecker_server_logstore_active_logs`), not the `namespace` label — so no change needed. If any PromQL string contains `namespace=~`, change it to `log_ns=~`.

- [ ] **Step 5: Validate JSON + remove the throwaway script**

```bash
python3 -c "import json; [json.load(open('tests/docker/monitor/grafana/templates/'+f)) for f in ('dashboard_server.json','dashboard_client.json')]; print('valid')"
rm tests/docker/monitor/grafana/templates/.rename_log_ns.py
```
Expected: `valid`.

- [ ] **Step 6: Commit**

```bash
git add tests/docker/monitor/grafana/templates/dashboard_server.json tests/docker/monitor/grafana/templates/dashboard_client.json tests/docker/monitor/README.md tests/docker/monitor/monitor_test.go
git commit -m "test(monitor): update Docker dashboards to log_ns label (#193)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Phase C — `tests/k8s/monitor/` minikube monitoring suite

### Task C1: Package skeleton — `K8sMonitorCluster` (port-forward + Prometheus query helpers)

**Files:**
- Create: `tests/k8s/monitor/k8s_monitor_cluster.go`
- Test: `tests/k8s/monitor/k8s_monitor_cluster_test.go`

**Interfaces:**
- Produces:
  - `type K8sMonitorCluster struct { PromBaseURL string }`
  - `func NewK8sMonitorCluster(promBaseURL string) *K8sMonitorCluster`
  - `func (c *K8sMonitorCluster) WaitForPrometheusReady(t *testing.T, timeout time.Duration)`
  - `func (c *K8sMonitorCluster) QueryPromQL(t *testing.T, query string) (string, bool)`
  - `func (c *K8sMonitorCluster) QueryVector(t *testing.T, query string) []map[string]string` — returns each result's label set
  - `func (c *K8sMonitorCluster) QueryMetricHasValue(t *testing.T, metricName string) bool`

- [ ] **Step 1: Write the failing test**

Create `tests/k8s/monitor/k8s_monitor_cluster_test.go`:

```go
package monitor

import "testing"

func TestPromBaseURL_Default(t *testing.T) {
	c := NewK8sMonitorCluster("http://localhost:9090")
	if c.PromBaseURL != "http://localhost:9090" {
		t.Fatalf("got %q", c.PromBaseURL)
	}
}
```

- [ ] **Step 2: Run, verify FAIL (undefined)**

Run: `go test ./tests/k8s/monitor/ -run TestPromBaseURL_Default -v`
Expected: FAIL — `undefined: NewK8sMonitorCluster`.

- [ ] **Step 3: Implement `k8s_monitor_cluster.go`**

Create `tests/k8s/monitor/k8s_monitor_cluster.go` (Apache header + below). The query helpers mirror `tests/docker/monitor/monitor_cluster.go` but take a configurable base URL:

```go
package monitor

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// K8sMonitorCluster queries a Prometheus reachable at PromBaseURL (typically a
// kubectl port-forward to svc/prometheus-operated in the monitoring namespace).
type K8sMonitorCluster struct {
	PromBaseURL string
}

func NewK8sMonitorCluster(promBaseURL string) *K8sMonitorCluster {
	return &K8sMonitorCluster{PromBaseURL: promBaseURL}
}

type promResult struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func (c *K8sMonitorCluster) WaitForPrometheusReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 5 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(c.PromBaseURL + "/-/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				t.Log("Prometheus is ready")
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Prometheus at %s not ready within %v", c.PromBaseURL, timeout)
}

func (c *K8sMonitorCluster) query(t *testing.T, q string) *promResult {
	t.Helper()
	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s", c.PromBaseURL, url.QueryEscape(q))
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(reqURL)
	if err != nil {
		t.Logf("prometheus query failed: %v", err)
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var r promResult
	if err := json.Unmarshal(body, &r); err != nil {
		t.Logf("prometheus parse failed: %v", err)
		return nil
	}
	if r.Status != "success" {
		return nil
	}
	return &r
}

// QueryPromQL returns the first sample value as a string.
func (c *K8sMonitorCluster) QueryPromQL(t *testing.T, q string) (string, bool) {
	t.Helper()
	r := c.query(t, q)
	if r == nil || len(r.Data.Result) == 0 || len(r.Data.Result[0].Value) < 2 {
		return "", false
	}
	v, ok := r.Data.Result[0].Value[1].(string)
	return v, ok
}

// QueryVector returns the label set of every result series.
func (c *K8sMonitorCluster) QueryVector(t *testing.T, q string) []map[string]string {
	t.Helper()
	r := c.query(t, q)
	if r == nil {
		return nil
	}
	out := make([]map[string]string, 0, len(r.Data.Result))
	for _, s := range r.Data.Result {
		out = append(out, s.Metric)
	}
	return out
}

// QueryMetricHasValue reports whether the metric exists with a non-zero value.
func (c *K8sMonitorCluster) QueryMetricHasValue(t *testing.T, metricName string) bool {
	t.Helper()
	r := c.query(t, metricName)
	if r == nil {
		return false
	}
	for _, s := range r.Data.Result {
		if len(s.Value) >= 2 {
			if vs, ok := s.Value[1].(string); ok && vs != "0" && vs != "0.0" {
				return true
			}
		}
	}
	return false
}
```

- [ ] **Step 4: Run, verify PASS**

Run: `go test ./tests/k8s/monitor/ -run TestPromBaseURL_Default -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/k8s/monitor/k8s_monitor_cluster.go tests/k8s/monitor/k8s_monitor_cluster_test.go
git commit -m "test(k8s-monitor): add K8sMonitorCluster prometheus query helpers (#193)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

### Task C2: Client load generator (`loadgen/main.go`)

**Files:**
- Create: `tests/k8s/monitor/loadgen/main.go`

**Interfaces:**
- Consumes: `config.NewConfiguration`, `etcd.GetRemoteEtcdClient`, `woodpecker.NewClient`, `metrics.RegisterClientMetricsWithRegisterer`, the `log.*` write/read API (signatures from `tests/chaos_mesh/workload/main_test.go`).
- Produces: a `main` binary that serves `/metrics` on `:29099` and continuously appends/reads, populating `woodpecker_client_*` and (via the servers) `woodpecker_server_*`.

- [ ] **Step 1: Implement `loadgen/main.go`** (Apache header + below)

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func main() {
	configFile := flag.String("config-file", "/tmp/test-config.yaml", "woodpecker config path")
	metricsAddr := flag.String("metrics-addr", ":29099", "Prometheus /metrics listen address")
	logName := flag.String("log", "k8s-monitor-loadgen", "log name to write")
	interval := flag.Duration("interval", 50*time.Millisecond, "append interval")
	flag.Parse()

	// Expose client metrics for the woodpecker-client PodMonitor to scrape.
	metrics.RegisterClientMetricsWithRegisterer(prometheus.DefaultRegisterer)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
			panic(fmt.Sprintf("metrics server: %v", err))
		}
	}()

	ctx := context.Background()
	cfg, err := config.NewConfiguration(*configFile)
	if err != nil {
		panic(fmt.Sprintf("load config: %v", err))
	}
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	if err != nil {
		panic(fmt.Sprintf("etcd client: %v", err))
	}
	cli, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	if err != nil {
		panic(fmt.Sprintf("woodpecker client: %v", err))
	}
	defer cli.Close(ctx)

	_ = cli.CreateLog(ctx, *logName) // idempotent
	h, err := cli.OpenLog(ctx, *logName)
	if err != nil {
		panic(fmt.Sprintf("open log: %v", err))
	}
	w, err := h.OpenLogWriter(ctx)
	if err != nil {
		panic(fmt.Sprintf("open writer: %v", err))
	}
	defer w.Close(ctx)

	// Tail reader to exercise the read path (read-batch / reader metrics).
	go func() {
		earliest := log.EarliestLogMessageID()
		r, err := h.OpenLogReader(ctx, &earliest, "loadgen-tail")
		if err != nil {
			return
		}
		defer r.Close(ctx)
		for {
			rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			_, _ = r.ReadNext(rctx)
			cancel()
		}
	}()

	fmt.Printf("loadgen: writing to %q every %s, metrics on %s\n", *logName, *interval, *metricsAddr)
	seq := 0
	tick := time.NewTicker(*interval)
	defer tick.Stop()
	for range tick.C {
		seq++
		wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		res := <-w.WriteAsync(wctx, &log.WriteMessage{
			Payload:    []byte(fmt.Sprintf("loadgen-%d", seq)),
			Properties: map[string]string{"seq": fmt.Sprintf("%d", seq)},
		})
		cancel()
		if res.Err != nil {
			fmt.Printf("write %d err: %v\n", seq, res.Err)
		}
	}
}
```

- [ ] **Step 2: Verify it compiles for the minikube node arch (linux)**

Run: `cd /Users/zilliz/workspace_zilliz/e2e_src/woodpecker && GOOS=linux GOARCH=$(go env GOARCH) CGO_ENABLED=0 go build -o /tmp/wp-loadgen ./tests/k8s/monitor/loadgen`
Expected: builds, `/tmp/wp-loadgen` exists. (If any `log.*`/`woodpecker.*` symbol differs, fix the call to match `tests/chaos_mesh/workload/main_test.go`, which uses the same API.)

- [ ] **Step 3: Commit**

```bash
git add tests/k8s/monitor/loadgen/main.go
git commit -m "test(k8s-monitor): add in-cluster client load generator (#193)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

### Task C3: Monitoring + scrape manifests

**Files:**
- Create: `tests/k8s/monitor/manifests/kube-prometheus-values.yaml`
- Create: `tests/k8s/monitor/manifests/podmonitor-server.yaml`
- Create: `tests/k8s/monitor/manifests/podmonitor-client.yaml`
- Create: `tests/k8s/monitor/manifests/client-loadgen.yaml`

**Interfaces:**
- Produces: Helm values (extras off, `externalLabels.cluster`, Grafana anonymous + dashboard sidecar); two PodMonitors; the client load-gen pod (labels `component=client`, named port `metrics` 29099).

- [ ] **Step 1: Create `kube-prometheus-values.yaml`**

```yaml
nodeExporter: { enabled: false }
kubeStateMetrics: { enabled: false }
alertmanager: { enabled: false }
defaultRules: { create: false }
kubeApiServer: { enabled: false }
kubelet: { enabled: false }
kubeControllerManager: { enabled: false }
kubeScheduler: { enabled: false }
kubeProxy: { enabled: false }
kubeEtcd: { enabled: false }
coreDns: { enabled: false }
prometheus:
  prometheusSpec:
    externalLabels:
      cluster: woodpecker-minikube
    podMonitorSelectorNilUsesHelmValues: false
    serviceMonitorSelectorNilUsesHelmValues: false
    ruleSelectorNilUsesHelmValues: false
    retention: 2h
grafana:
  adminPassword: admin
  grafana.ini:
    auth.anonymous:
      enabled: true
      org_role: Admin
    auth:
      disable_login_form: true
  sidecar:
    dashboards:
      enabled: true
      label: grafana_dashboard
      labelValue: "1"
      searchNamespace: ALL
```

- [ ] **Step 2: Create `podmonitor-server.yaml`**

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: woodpecker-server
  namespace: woodpecker
  labels:
    app.kubernetes.io/name: woodpecker
    app.kubernetes.io/component: server
spec:
  namespaceSelector:
    matchNames: [woodpecker]
  selector:
    matchLabels:
      app.kubernetes.io/name: woodpecker
      app.kubernetes.io/component: server
      app.kubernetes.io/instance: my-woodpecker
  podMetricsEndpoints:
    - port: metrics
      path: /metrics
      interval: 15s
      honorLabels: true
```

- [ ] **Step 3: Create `podmonitor-client.yaml`**

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: woodpecker-client
  namespace: woodpecker
  labels:
    app.kubernetes.io/name: woodpecker
    app.kubernetes.io/component: client
spec:
  namespaceSelector:
    matchNames: [woodpecker]
  selector:
    matchLabels:
      app.kubernetes.io/name: woodpecker
      app.kubernetes.io/component: client
      app.kubernetes.io/instance: my-woodpecker
  podMetricsEndpoints:
    - port: metrics
      path: /metrics
      interval: 15s
      honorLabels: true
```

- [ ] **Step 4: Create `client-loadgen.yaml`** (sleeps; the runner cp's the binary in and execs it)

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: wp-loadgen
  namespace: woodpecker
  labels:
    app.kubernetes.io/name: woodpecker
    app.kubernetes.io/component: client
    app.kubernetes.io/instance: my-woodpecker
spec:
  containers:
    - name: loadgen
      image: golang:1.24
      imagePullPolicy: IfNotPresent
      command: ["/bin/bash", "-c", "sleep infinity"]
      ports:
        - name: metrics
          containerPort: 29099
      resources:
        requests: { cpu: "500m", memory: "512Mi" }
  restartPolicy: Never
```

- [ ] **Step 5: Validate YAML parses**

Run: `for f in tests/k8s/monitor/manifests/*.yaml; do python3 -c "import yaml,sys; list(yaml.safe_load_all(open('$f'))); print('ok','$f')"; done`
Expected: `ok` for each file. (If PyYAML is unavailable, `kubectl apply --dry-run=client -f <f>` once a cluster exists — defer to Task C6.)

- [ ] **Step 6: Commit**

```bash
git add tests/k8s/monitor/manifests/
git commit -m "test(k8s-monitor): add kube-prometheus values + PodMonitors + loadgen pod (#193)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

### Task C4: New k8s dashboards (server + client) as ConfigMaps

**Files:**
- Create: `tests/k8s/monitor/grafana/templates/dashboard_server_k8s.json`
- Create: `tests/k8s/monitor/grafana/templates/dashboard_client_k8s.json`
- Create: `tests/k8s/monitor/manifests/dashboard-configmaps.yaml`
- Test: `tests/k8s/monitor/dashboards_test.go`

**Interfaces:**
- Produces: two dashboards using a `${prometheus}` datasource variable + `cluster`/`namespace`(k8s)/`log_ns`(+`log_id`/`server_job` on server) variables; both wrapped as `grafana_dashboard="1"` ConfigMaps.

- [ ] **Step 1: Write the generator script**

Create `tests/k8s/monitor/grafana/templates/.gen_k8s_dashboards.py` (throwaway; deleted in Step 5). It reads the **already-log_ns** Docker templates (post Task B1) and adds the front selectors:

```python
import json, pathlib, copy

DOCKER = pathlib.Path("tests/docker/monitor/grafana/templates")
OUT = pathlib.Path("tests/k8s/monitor/grafana/templates")
OUT.mkdir(parents=True, exist_ok=True)
JOB = 'woodpecker-node[0-9]+|woodpecker/woodpecker-server'

def ds_var():
    return {"name": "prometheus", "label": "Datasource", "type": "datasource",
            "query": "prometheus", "current": {}, "options": [], "hide": 0,
            "refresh": 1, "regex": "", "includeAll": False, "multi": False}

def q_var(name, query, regex=None):
    v = {"name": name, "type": "query",
         "datasource": {"type": "prometheus", "uid": "${prometheus}"},
         "query": query, "refresh": 2, "includeAll": True, "multi": True,
         "current": {"text": "All", "value": "$__all"}}
    if regex:
        v["regex"] = regex
    return v

def retarget_datasource(o):
    # point every datasource ref at the ${prometheus} variable
    if isinstance(o, dict):
        if o.get("type") == "prometheus" and "uid" in o and o.get("uid") != "${prometheus}":
            o["uid"] = "${prometheus}"
        return {k: retarget_datasource(v) for k, v in o.items()}
    if isinstance(o, list):
        return [retarget_datasource(x) for x in o]
    return o

def add_filter(expr, extra):
    # insert extra label matchers right after the first "{" of each selector
    out, i = [], 0
    while True:
        b = expr.find("{", i)
        if b < 0:
            out.append(expr[i:]); break
        out.append(expr[i:b+1])
        # if selector is non-empty, add a comma
        nxt = expr[b+1:].lstrip()
        sep = "" if nxt.startswith("}") else ", "
        out.append(extra + sep)
        i = b + 1
    return "".join(out)

def build(src, dst, extra_vars, panel_extra, uid):
    obj = json.loads((DOCKER / src).read_text())
    obj = retarget_datasource(obj)
    obj["uid"] = uid
    obj["title"] = obj["title"] + " (k8s)"
    # rebuild templating list: prometheus, cluster, namespace, then the originals
    orig = obj.get("templating", {}).get("list", [])
    obj["templating"]["list"] = extra_vars + orig
    # add cluster + k8s namespace + (log_ns already present) to every panel target
    def walk(o):
        if isinstance(o, dict):
            if "expr" in o and isinstance(o["expr"], str):
                o["expr"] = add_filter(o["expr"], panel_extra)
            return {k: walk(v) for k, v in o.items()}
        if isinstance(o, list):
            return [walk(x) for x in o]
        return o
    obj = walk(obj)
    (OUT / dst).write_text(json.dumps(obj, indent=2) + "\n")
    blob = json.dumps(obj)
    assert '__DATASOURCE_UID__' not in blob, f"{dst}: stray placeholder"
    assert '${prometheus}' in blob, f"{dst}: missing datasource var"
    names = [v["name"] for v in obj["templating"]["list"]]
    print(f"OK {dst}: vars={names}")

# Server: prepend prometheus, cluster, namespace(k8s); log_ns/log_id/server_job already exist
server_vars = [
    ds_var(),
    q_var("cluster", "label_values(go_info, cluster)"),
    q_var("namespace", f'label_values(go_info{{job=~"{JOB}", cluster=~"$cluster"}}, namespace)'),
]
build("dashboard_server.json", "dashboard_server_k8s.json", server_vars,
      'cluster=~"$cluster", namespace=~"$namespace"', "woodpecker-server-k8s")

# Client: prepend prometheus, cluster, namespace(k8s); log_ns already exists
client_vars = [
    ds_var(),
    q_var("cluster", "label_values(go_info, cluster)"),
    q_var("namespace", 'label_values(woodpecker_client_append_requests_total{cluster=~"$cluster"}, namespace)'),
]
build("dashboard_client.json", "dashboard_client_k8s.json", client_vars,
      'cluster=~"$cluster", namespace=~"$namespace"', "woodpecker-client-k8s")
```

- [ ] **Step 2: Run the generator, verify assertions**

Run: `python3 tests/k8s/monitor/grafana/templates/.gen_k8s_dashboards.py`
Expected: two `OK …: vars=['prometheus','cluster','namespace',…]` lines, no assertion errors.

- [ ] **Step 3: Write the dashboard validation test**

Create `tests/k8s/monitor/dashboards_test.go`:

```go
package monitor

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestK8sDashboards_Valid(t *testing.T) {
	for _, f := range []string{
		"grafana/templates/dashboard_server_k8s.json",
		"grafana/templates/dashboard_client_k8s.json",
	} {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		var obj map[string]any
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		s := string(raw)
		if strings.Contains(s, "__DATASOURCE_UID__") {
			t.Errorf("%s: stray __DATASOURCE_UID__", f)
		}
		if !strings.Contains(s, "${prometheus}") {
			t.Errorf("%s: missing ${prometheus} datasource var", f)
		}
		names := map[string]bool{}
		for _, v := range obj["templating"].(map[string]any)["list"].([]any) {
			names[v.(map[string]any)["name"].(string)] = true
		}
		for _, want := range []string{"prometheus", "cluster", "namespace", "log_ns"} {
			if !names[want] {
				t.Errorf("%s: missing template var %q", f, want)
			}
		}
	}
}
```

- [ ] **Step 4: Run the test, verify PASS**

Run: `go test ./tests/k8s/monitor/ -run TestK8sDashboards_Valid -v`
Expected: PASS.

- [ ] **Step 5: Generate the ConfigMap manifest, remove the throwaway script**

```bash
cd /Users/zilliz/workspace_zilliz/e2e_src/woodpecker
{
  echo '# Generated from grafana/templates/*.json — Grafana sidecar imports these.'
  for n in server client; do
    echo '---'
    echo 'apiVersion: v1'
    echo 'kind: ConfigMap'
    echo 'metadata:'
    echo "  name: woodpecker-${n}-k8s-dashboard"
    echo '  namespace: monitoring'
    echo '  labels: { grafana_dashboard: "1" }'
    echo 'data:'
    echo "  dashboard_${n}_k8s.json: |"
    sed 's/^/    /' "tests/k8s/monitor/grafana/templates/dashboard_${n}_k8s.json"
  done
} > tests/k8s/monitor/manifests/dashboard-configmaps.yaml
python3 -c "import yaml; list(yaml.safe_load_all(open('tests/k8s/monitor/manifests/dashboard-configmaps.yaml'))); print('configmaps ok')"
rm tests/k8s/monitor/grafana/templates/.gen_k8s_dashboards.py
```
Expected: `configmaps ok`.

- [ ] **Step 6: Commit**

```bash
git add tests/k8s/monitor/grafana/templates/ tests/k8s/monitor/manifests/dashboard-configmaps.yaml tests/k8s/monitor/dashboards_test.go
git commit -m "test(k8s-monitor): add k8s server/client dashboards + configmaps (#193)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

### Task C5: E2E verification test + one-click runner + README

**Files:**
- Create: `tests/k8s/monitor/monitor_test.go`
- Create: `tests/k8s/monitor/run_monitor_tests.sh` (executable)
- Create: `tests/k8s/monitor/README.md`

**Interfaces:**
- Consumes: `K8sMonitorCluster` (C1), the load-gen pod + PodMonitors (C2/C3), the running stack (C5 runner).
- Produces: `TestK8sMonitor_Metrics` (gated on `WP_K8S_PROM_URL`), and the `run_monitor_tests.sh` entry point.

- [ ] **Step 1: Implement `monitor_test.go`** (Apache header + below)

```go
package monitor

import (
	"os"
	"testing"
	"time"
)

// TestK8sMonitor_Metrics verifies scrape targets and the cluster/namespace/log_ns
// label scheme against a Prometheus reachable at $WP_K8S_PROM_URL (a kubectl
// port-forward set up by run_monitor_tests.sh). Skipped when unset so the package
// stays `go test ./...`-safe without a live cluster.
func TestK8sMonitor_Metrics(t *testing.T) {
	base := os.Getenv("WP_K8S_PROM_URL")
	if base == "" {
		t.Skip("WP_K8S_PROM_URL unset; run via run_monitor_tests.sh")
	}
	c := NewK8sMonitorCluster(base)
	c.WaitForPrometheusReady(t, 60*time.Second)

	t.Run("ServerTargetUp", func(t *testing.T) {
		if v, ok := c.QueryPromQL(t, `count(up{job="woodpecker/woodpecker-server"}==1)`); !ok || v == "0" {
			t.Errorf("no server targets up (got %q)", v)
		}
	})
	t.Run("ClientTargetUp", func(t *testing.T) {
		if v, ok := c.QueryPromQL(t, `count(up{job="woodpecker/woodpecker-client"}==1)`); !ok || v == "0" {
			t.Errorf("no client target up (got %q)", v)
		}
	})
	t.Run("ServerMetrics", func(t *testing.T) {
		for _, m := range []string{
			"woodpecker_server_logstore_active_logs",
			"woodpecker_server_logstore_operations_total",
			"woodpecker_server_file_operations_total",
		} {
			if !c.QueryMetricHasValue(t, m) {
				t.Errorf("server metric %s missing/zero", m)
			}
		}
	})
	t.Run("ClientMetrics", func(t *testing.T) {
		for _, m := range []string{
			"woodpecker_client_append_requests_total",
			"woodpecker_client_log_handle_operations_total",
		} {
			if !c.QueryMetricHasValue(t, m) {
				t.Errorf("client metric %s missing/zero", m)
			}
		}
	})
	t.Run("LabelScheme", func(t *testing.T) {
		series := c.QueryVector(t, "woodpecker_server_logstore_active_logs")
		if len(series) == 0 {
			t.Fatal("no logstore_active_logs series")
		}
		for _, m := range series {
			if m["cluster"] != "woodpecker-minikube" {
				t.Errorf("cluster=%q, want woodpecker-minikube", m["cluster"])
			}
			if m["namespace"] != "woodpecker" {
				t.Errorf("namespace=%q, want k8s namespace woodpecker", m["namespace"])
			}
			if m["log_ns"] == "" {
				t.Errorf("missing log_ns label: %v", m)
			}
			if _, bad := m["exported_namespace"]; bad {
				t.Errorf("unexpected exported_namespace: %v", m)
			}
		}
	})
}
```

- [ ] **Step 2: Run the test with no cluster, verify it SKIPS**

Run: `go test ./tests/k8s/monitor/ -run TestK8sMonitor_Metrics -v`
Expected: `--- SKIP` (env var unset). Confirms the package builds and is `go test ./...`-safe.

- [ ] **Step 3: Implement `run_monitor_tests.sh`** (Apache header + below; `chmod +x`)

```bash
#!/bin/bash
# <Apache license header — copy from tests/docker/monitor/run_monitor_tests.sh>
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

export CLUSTER_NAME="${CLUSTER_NAME:-wp-monitor}"
export CR_NAME="${CR_NAME:-my-woodpecker}"
export NAMESPACE="${NAMESPACE:-woodpecker}"
export REPLICAS="${REPLICAS:-3}"
export WP_IMG="${WP_IMG:-zilliztech/woodpecker:v0.1.26}"
export MK_CPUS="${MK_CPUS:-4}" MK_MEMORY="${MK_MEMORY:-4096}"
MON_NS="monitoring"
HELM_RELEASE="kps"
NO_CLEANUP="${NO_CLEANUP:-false}"
[ "${1:-}" = "--no-cleanup" ] && NO_CLEANUP=true

source "$PROJECT_ROOT/deployments/operator/test/lib.sh"

preload_image() { log "preload: $1"; docker pull "$1" || fail "docker pull $1"; minikube -p "$CLUSTER_NAME" image load "$1" || fail "image load $1"; }

bringup() {
  wp_minikube_start
  kubectl label node "$CLUSTER_NAME" topology.kubernetes.io/zone=zone-a topology.kubernetes.io/region=region-local --overwrite
  preload_image quay.io/coreos/etcd:v3.5.18
  preload_image minio/minio:RELEASE.2024-06-13T22-53-53Z
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
  # lib.sh applies deps/CR namespace-lessly, so pin the active namespace to $NAMESPACE
  # (its seed DNS uses .$NAMESPACE.svc, so this keeps pods and DNS consistent).
  kubectl config set-context --current --namespace="$NAMESPACE"
  wp_deploy_operator; wp_build_wp_image
  wp_deploy_deps; wp_create_cr; wp_wait_healthy
}

install_monitoring() {
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
  helm repo update
  log "preloading kube-prometheus-stack images"
  helm template "$HELM_RELEASE" prometheus-community/kube-prometheus-stack -n "$MON_NS" \
    -f "$SCRIPT_DIR/manifests/kube-prometheus-values.yaml" 2>/dev/null \
    | grep -hoE 'image: *"?[^"[:space:]]+' | sed -E 's/image: *"?//' | sort -u \
    | while read -r img; do [ -n "$img" ] && preload_image "$img"; done
  helm upgrade --install "$HELM_RELEASE" prometheus-community/kube-prometheus-stack \
    -n "$MON_NS" --create-namespace -f "$SCRIPT_DIR/manifests/kube-prometheus-values.yaml" --wait --timeout 8m
  kubectl apply -f "$SCRIPT_DIR/manifests/podmonitor-server.yaml"
  kubectl apply -f "$SCRIPT_DIR/manifests/podmonitor-client.yaml"
  kubectl apply -f "$SCRIPT_DIR/manifests/dashboard-configmaps.yaml"
}

start_loadgen() {
  kubectl apply -f "$SCRIPT_DIR/manifests/client-loadgen.yaml"
  kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/wp-loadgen --timeout=120s
  log "building loadgen binary on host"
  ( cd "$PROJECT_ROOT" && GOOS=linux GOARCH="$(go env GOARCH)" CGO_ENABLED=0 \
      go build -o /tmp/wp-loadgen ./tests/k8s/monitor/loadgen ) || fail "loadgen build"
  kubectl -n "$NAMESPACE" cp /tmp/wp-loadgen wp-loadgen:/root/wp-loadgen
  # client config: reuse the same etcd/minio/seeds the CR uses (see README for the file)
  kubectl -n "$NAMESPACE" cp "$SCRIPT_DIR/manifests/loadgen-config.yaml" wp-loadgen:/tmp/test-config.yaml
  kubectl -n "$NAMESPACE" exec wp-loadgen -- bash -c 'chmod +x /root/wp-loadgen && nohup /root/wp-loadgen -config-file=/tmp/test-config.yaml >/tmp/loadgen.log 2>&1 &'
  log "loadgen started; sleeping 45s to accumulate metrics"; sleep 45
}

run_tests() {
  kubectl -n "$MON_NS" port-forward svc/prometheus-operated 9090:9090 >/tmp/pf-prom.log 2>&1 &
  PF_PID=$!; trap 'kill $PF_PID 2>/dev/null || true' RETURN
  sleep 5
  WP_K8S_PROM_URL="http://localhost:9090" go test "$PROJECT_ROOT/tests/k8s/monitor/" -run TestK8sMonitor_Metrics -v -count=1 -timeout 300s
}

cleanup() {
  helm uninstall "$HELM_RELEASE" -n "$MON_NS" 2>/dev/null || true
  minikube delete -p "$CLUSTER_NAME" 2>/dev/null || true
}

main() {
  bringup
  install_monitoring
  start_loadgen
  if run_tests; then RC=0; else RC=1; fi
  if [ "$NO_CLEANUP" = true ]; then
    log "kept up. Grafana: $(minikube -p "$CLUSTER_NAME" service -n "$MON_NS" "$HELM_RELEASE-grafana" --url 2>/dev/null | head -1)"
    log "Prometheus: kubectl -n $MON_NS port-forward svc/prometheus-operated 9090:9090"
  else
    cleanup
  fi
  exit $RC
}
main "$@"
```

Note: `loadgen-config.yaml` is the client config pointing at `etcd.$NAMESPACE.svc` / `minio.$NAMESPACE.svc` and the server seeds — author it in Step 4 (mirror the `client:` block from `lib.sh:wp_write_client_config`, with `NAMESPACE=woodpecker`).

- [ ] **Step 4: Create `tests/k8s/monitor/manifests/loadgen-config.yaml`**

Copy the client config block from `deployments/operator/test/lib.sh` (`wp_write_client_config`), substituting `${NAMESPACE}`→`woodpecker`, `${CR_NAME}`→`my-woodpecker`, `${REPLICAS}`→3 (3 seed entries `my-woodpecker-server-{0,1,2}.my-woodpecker-server-headless.woodpecker.svc:18080`). Validate:

Run: `python3 -c "import yaml; yaml.safe_load(open('tests/k8s/monitor/manifests/loadgen-config.yaml')); print('ok')"`
Expected: `ok`.

- [ ] **Step 5: Write `README.md`**

Create `tests/k8s/monitor/README.md` documenting: prerequisites (minikube, docker, kubectl, helm, go); `./run_monitor_tests.sh` / `--no-cleanup`; the `cluster`/`namespace`(k8s)/`log_ns` label scheme and the **breaking-change** note (prod dashboards must move `namespace`→`log_ns`); resource note (bump `MK_CPUS=6 MK_MEMORY=8192` if pods stay Pending); and that the prod `PodMonitor` needs no change after the `log_ns` rename.

- [ ] **Step 6: Verify the package builds + vets and shell parses**

Run:
```bash
go vet ./tests/k8s/monitor/...
bash -n tests/k8s/monitor/run_monitor_tests.sh
chmod +x tests/k8s/monitor/run_monitor_tests.sh
```
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add tests/k8s/monitor/monitor_test.go tests/k8s/monitor/run_monitor_tests.sh tests/k8s/monitor/manifests/loadgen-config.yaml tests/k8s/monitor/README.md
git commit -m "test(k8s-monitor): add e2e verification test, runner, README (#193)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

### Task C6: Live end-to-end dry run (manual gate)

**Files:** none (validation only).

- [ ] **Step 1: Run the suite end to end (requires Docker + minikube)**

Run: `cd /Users/zilliz/workspace_zilliz/e2e_src/woodpecker && ./tests/k8s/monitor/run_monitor_tests.sh --no-cleanup`
Expected: minikube up → operator + 3 server pods Ready → kube-prometheus-stack installed → both PodMonitors `Up` → `TestK8sMonitor_Metrics` PASS.

- [ ] **Step 2: Confirm the dashboards render with data**

Open the printed Grafana URL → "Woodpecker - Server (Namespace) (k8s)" and the client dashboard. Verify the `cluster` / `namespace` (= `woodpecker`) / `log_ns` dropdowns populate and panels show data. Spot-check in Prometheus that `woodpecker_server_logstore_active_logs` has `namespace="woodpecker"` and a non-empty `log_ns`, and **no** `exported_namespace`.

- [ ] **Step 3: Tear down**

Run: `minikube delete -p wp-monitor`
Expected: cluster removed.

- [ ] **Step 4: Final review commit (if any fixes were needed during C6)**

```bash
git add -A && git commit -m "test(k8s-monitor): fixes from end-to-end dry run (#193)"
```

---

## Self-Review notes (for the implementer)

- **Spec coverage:** A1↔Workstream A; B1↔Workstream B; C1–C6↔Workstream C (cluster helper, loadgen, manifests, dashboards, test+runner+README, dry run). The breaking-change + prod-follow-up are documented in the README (C5 Step 5).
- **Ordering:** A1 must land before B1 and C4 (dashboards assume the `log_ns` label). C1–C3 are order-independent; C4 depends on B1's log_ns templates; C5 depends on C1–C4.
- **Type consistency:** `K8sMonitorCluster` methods (`WaitForPrometheusReady`, `QueryPromQL`, `QueryVector`, `QueryMetricHasValue`) are defined in C1 and used unchanged in C5. The loadgen uses the exact `log.*`/`woodpecker.*`/`config.*`/`etcd.*` API from `tests/chaos_mesh/workload/main_test.go`.
- **Known integration risks to watch:** (1) `wp_create_cr`/`wp_deploy_deps` in `lib.sh` default to `NAMESPACE` — confirm they create resources in `woodpecker`, not `default` (the CR's headless service DNS in `loadgen-config.yaml` must match). (2) kube-prometheus-stack image preload list parsing — verify the `helm template | grep image:` extraction catches all images on the installed chart version.
