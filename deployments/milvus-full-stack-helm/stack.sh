#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$ROOT/versions.env"
PROFILE="${PROFILE:-wp-ops-v0146}"
CONTEXT="${CONTEXT:-$PROFILE}"
NAMESPACE="${NAMESPACE:-milvus-stack}"
STATE="${STATE:-$ROOT/.local}"
TIMEOUT="${TIMEOUT:-12m}"
MILVUS_CHART="$STATE/charts/milvus-$MILVUS_CHART_VERSION.tgz"
MONITORING_CHART="$STATE/charts/kube-prometheus-stack-$MONITORING_CHART_VERSION.tgz"
ACTION="${1:-help}"
if [ "$#" -gt 0 ]; then shift; fi
MV_VALUES=(-f "$ROOT/values/milvus.yaml")
MON_VALUES=(-f "$ROOT/values/monitoring.yaml")
# Keep this array nonempty for macOS Bash 3.2 with nounset enabled.
SUPPORT_VALUES=(-f "$ROOT/charts/full-stack-support/values.yaml")
MAIL_CAPTURE=false
WPCLI=false
CONNECTION_FORMAT=text
usage() {
  cat <<EOF
Usage: ./stack.sh <command> [options]

Guided walkthrough
  notebook        Open the executable Jupyter walkthrough (local port 8891)

Cluster lifecycle (local minikube profile)
  start           Create or resume the cluster; keep existing deployments/data
  stop            Stop the cluster; keep deployments/data for the next start

Stack lifecycle (Helm releases in the selected context/namespace)
  deploy          Install or upgrade Milvus, Woodpecker and monitoring
  clear           Uninstall the three releases AND delete their persistent data

Preparation
  charts          Download the pinned Helm charts
  render          Render manifests locally for inspection
  images          Pull required images and load them into local minikube

Access and status
  status          Show Helm releases, Pods, PVCs, monitors and alert resources
  connections     Show host ports, WP service context and separate Milvus metadata connection
  forward         Open local service ports; Ctrl-C closes these forwards
                  Add --wpcli for all Woodpecker Admin ports and tenant etcd

Validation (keep forward running for live checks)
  acceptance      Rule tests + workload + dashboards + alert lifecycle/inhibition
  smoke           Run a Milvus insert/query/search workload
  verify          Check scrape targets, workload metrics and dashboard queries
  alerts          Inspect rule health, coverage, receiver and AM discovery
  alerts-test     Run offline alert behavior tests using Docker; no forward needed
  alerts-smoke    Test firing, recovery and inhibition with temporary alerts

Write performance and diagnosis
  perf            Bounded concurrent writes, latency/throughput and read checks
                  Always save request records, metrics, alerts and Pod logs

Diagnostics
  collect         Save status, bounded logs and verification reports locally

Optional email testing (disabled by default)
  email-smoke     Verify FIRING/RESOLVED emails using the Mailpit capture profile
                  acceptance --mail-capture also includes this check

Options
  --local-arm64             Use the supplied ARM64 Milvus build
  -f, --values FILE          Additional Milvus Helm values (repeatable)
  --monitoring-values FILE  Additional monitoring Helm values (repeatable)
  --support-values FILE     Additional supporting-chart values (repeatable)
  --mail-capture            Enable the optional local SMTP capture profile
                            Repeat on render/images/deploy/forward/acceptance
  --wpcli                   forward: include per-Pod WP Admin and tenant etcd;
                            reuse matching existing forwards without stopping them
  --json                    connections: machine-readable connection inventory
  --wpcli-config            connections: emit a ready WP context YAML + separate metadata notes
  -h, --help                Show this help (also supported after a command)

Targets and environment
  PROFILE=$PROFILE
    start/stop/images target this local minikube profile
  CONTEXT=$CONTEXT  NAMESPACE=$NAMESPACE
    deploy/clear/status and live checks target this Kubernetes context/namespace
  STATE=$STATE
    Local chart cache, rendered manifests and reports; preserved by clear
  CPUS=${CPUS:-6}  MEMORY=${MEMORY:-6656}     Resources for start (memory in MiB)
  TIMEOUT=$TIMEOUT        Helm/rollout timeout
  Local ports: GRAFANA_PORT=3000 PROMETHEUS_PORT=9099 MILVUS_PORT=19530
               ALERTMANAGER_PORT=9093 MAILPIT_PORT=8025 (optional)
  wpcli ports: WP_ADMIN_PORT_BASE=19091 (plus Pod ordinal), ETCD_PORT=32379
  CONNECTIONS_PYTHON=python3  Python with PyYAML for connections/forward --wpcli
  Perf defaults: PERF_DURATION=60 PERF_CONCURRENCY=2 PERF_BATCH_SIZE=100
                 PERF_DIMENSION=128 PERF_RPS=5 (total batch requests/second)
  Perf gates: PERF_MIN_ROWS_PER_SECOND=0 PERF_MAX_P99_MS=0 (0 disables a gate)
  NOTEBOOK_PORT=8891     JupyterLab port; first launch installs an isolated runtime

Typical workflow
  ./stack.sh start
  ./stack.sh images --local-arm64
  ./stack.sh deploy --local-arm64
  ./stack.sh forward                 # Keep running in a separate terminal
  ./stack.sh connections             # See endpoints and wpcli setup instructions
  ./stack.sh acceptance
  ./stack.sh perf                    # Separate write-performance run

Pause/resume:  ./stack.sh stop   -> ./stack.sh start
Fresh deploy: ./stack.sh clear  -> ./stack.sh deploy --local-arm64

clear deletes this stack's PVCs: Milvus/Woodpecker, etcd/MinIO and monitoring.
It preserves the namespace, shared CRDs, minikube and local evidence.
Use stop/start when you want to keep the deployed stack and its data.
EOF
}
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h|--help) ACTION=help; shift ;;
    --local-arm64) MV_VALUES+=(-f "$ROOT/values/local-arm64.yaml"); shift ;;
    --wpcli) WPCLI=true; shift ;;
    --json|--wpcli-config)
      if [ "$CONNECTION_FORMAT" != text ]; then echo "Choose one connection output format" >&2; exit 2; fi
      if [ "$1" = --json ]; then CONNECTION_FORMAT=json; else CONNECTION_FORMAT=wpcli; fi
      shift ;;
    --mail-capture)
      MAIL_CAPTURE=true
      MON_VALUES+=(-f "$ROOT/values/mail-capture.yaml")
      SUPPORT_VALUES+=(-f "$ROOT/values/mail-capture.yaml"); shift ;;
    --monitoring-values|--support-values)
      if [ "$#" -lt 2 ]; then echo "Missing values file" >&2; exit 2; fi
      if [ "$1" = --monitoring-values ]; then MON_VALUES+=(-f "$2"); else SUPPORT_VALUES+=(-f "$2"); fi
      shift 2 ;;
    -f|--values)
      if [ "$#" -lt 2 ]; then echo "Missing values file" >&2; exit 2; fi
      MV_VALUES+=(-f "$2"); shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done
case "$ACTION" in
  help|-h|--help) usage; exit 0 ;;
  notebook|start|stop|deploy|clear|charts|render|images|status|connections|forward|smoke|perf|verify|alerts|alerts-smoke|email-smoke|acceptance|alerts-test|collect) ;;
  *) echo "Unknown command: $ACTION (see ./stack.sh help)" >&2; exit 2 ;;
esac
if "$WPCLI" && [ "$ACTION" != forward ]; then echo "--wpcli applies only to forward" >&2; exit 2; fi
if [ "$CONNECTION_FORMAT" != text ] && [ "$ACTION" != connections ]; then echo "Connection output options apply only to connections" >&2; exit 2; fi
mkdir -p "$STATE/charts" "$STATE/rendered"
# Keep Helm's caches/configuration local to this example.
export HELM_CACHE_HOME="$STATE/helm/cache"
export HELM_CONFIG_HOME="$STATE/helm/config"
export HELM_DATA_HOME="$STATE/helm/data"
k() { kubectl --context "$CONTEXT" -n "$NAMESPACE" "$@"; }
h() { helm --kube-context "$CONTEXT" -n "$NAMESPACE" "$@"; }
connection_info() { "${CONNECTIONS_PYTHON:-python3}" "$ROOT/scripts/connections.py" --context "$CONTEXT" --namespace "$NAMESPACE" "$@"; }
clear_stack() {
  # Uninstall consumers before their dependencies. Missing releases are fine,
  # allowing cleanup to be repeated after a partial installation or uninstall.
  h uninstall milvus --ignore-not-found --wait --timeout "$TIMEOUT"
  h uninstall full-stack --ignore-not-found --wait --timeout "$TIMEOUT"
  h uninstall monitoring --ignore-not-found --wait --timeout "$TIMEOUT"
  # StatefulSet/operator PVCs outlive Helm releases. Select only this stack's
  # release identities, including all replica ordinals and optional Grafana PVCs.
  k delete pvc --selector 'app.kubernetes.io/instance in (milvus,monitoring,monitoring-kube-prometheus-prometheus,monitoring-kube-prometheus-alertmanager)' \
    --ignore-not-found --wait --timeout="$TIMEOUT"
  k delete pvc --selector 'app in (full-stack-etcd,full-stack-minio)' \
    --ignore-not-found --wait --timeout="$TIMEOUT"
  echo "Stack releases and PVCs deleted from $CONTEXT/$NAMESPACE; cluster and local evidence retained."
}
charts() {
  if [ ! -s "$MILVUS_CHART" ]; then
    curl -fL --retry 2 --connect-timeout 15 --max-time 180 \
      "https://github.com/zilliztech/milvus-helm/releases/download/milvus-$MILVUS_CHART_VERSION/milvus-$MILVUS_CHART_VERSION.tgz" \
      -o "$MILVUS_CHART.part"
    mv "$MILVUS_CHART.part" "$MILVUS_CHART"
  fi
  if [ ! -s "$MONITORING_CHART" ]; then
    helm pull kube-prometheus-stack --repo https://prometheus-community.github.io/helm-charts \
      --version "$MONITORING_CHART_VERSION" --destination "$STATE/charts"
  fi
}
render() {
  charts
  helm template milvus "$MILVUS_CHART" -n "$NAMESPACE" "${MV_VALUES[@]}" > "$STATE/rendered/milvus.yaml"
  helm template monitoring "$MONITORING_CHART" -n "$NAMESPACE" \
    "${MON_VALUES[@]}" > "$STATE/rendered/monitoring.yaml"
  helm template full-stack "$ROOT/charts/full-stack-support" -n "$NAMESPACE" "${SUPPORT_VALUES[@]}" > "$STATE/rendered/support.yaml"
  echo "Rendered manifests: $STATE/rendered"
}
preload() {
  render
  # Includes regular containers, init containers, and Helm admission jobs.
  sed -nE 's/^[[:space:]]*image:[[:space:]]*"?([^"[:space:]]+).*/\1/p' \
    "$STATE"/rendered/*.yaml | sort -u > "$STATE/images.txt"
  # The operator creates this container dynamically; it appears as an argument,
  # rather than a container image field in the rendered resources.
  sed -nE 's/.*--prometheus-config-reloader=([^"[:space:]]+).*/\1/p' \
    "$STATE/rendered/monitoring.yaml" >> "$STATE/images.txt"
  while IFS= read -r image; do
    echo "Loading $image"
    if ! docker image inspect "$image" >/dev/null 2>&1; then
      python3 "$ROOT/scripts/pull-image.py" "$image"
    fi
    if ! minikube -p "$PROFILE" image load "$image" --daemon; then
      # Docker Desktop's containerd image store sometimes cannot export distroless images.
      minikube -p "$PROFILE" image pull "$image"
    fi
  done < "$STATE/images.txt"
}
deploy() {
  charts
  k get namespace "$NAMESPACE" >/dev/null 2>&1 || k create namespace "$NAMESPACE"
  k label namespace "$NAMESPACE" full-stack=milvus --overwrite
  h upgrade --install monitoring "$MONITORING_CHART" --create-namespace \
    "${MON_VALUES[@]}" --wait --timeout "$TIMEOUT"
  h upgrade --install full-stack "$ROOT/charts/full-stack-support" "${SUPPORT_VALUES[@]}" --wait --timeout "$TIMEOUT"
  h upgrade --install milvus "$MILVUS_CHART" "${MV_VALUES[@]}" --wait --timeout "$TIMEOUT"
  k rollout status statefulset/prometheus-monitoring-kube-prometheus-prometheus --timeout="$TIMEOUT"
  k rollout status statefulset/alertmanager-monitoring-kube-prometheus-alertmanager --timeout="$TIMEOUT"
  k get pods,pvc
}
forward() {
  if "$WPCLI"; then
    if "$MAIL_CAPTURE"; then
      connection_info --forward --mail-capture
    else
      connection_info --forward
    fi
    return
  fi
  k rollout status deployment/monitoring-grafana --timeout="$TIMEOUT"
  k rollout status statefulset/prometheus-monitoring-kube-prometheus-prometheus --timeout="$TIMEOUT"
  k rollout status deployment/milvus-proxy --timeout="$TIMEOUT"
  k rollout status statefulset/alertmanager-monitoring-kube-prometheus-alertmanager --timeout="$TIMEOUT"
  pids=()
  trap 'for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  # Start kubectl directly so the trap owns the actual processes, not wrapper shells.
  kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward --address 127.0.0.1 svc/monitoring-grafana "${GRAFANA_PORT:-3000}:80" & pids+=($!)
  kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward --address 127.0.0.1 svc/monitoring-kube-prometheus-prometheus "${PROMETHEUS_PORT:-9099}:9090" & pids+=($!)
  kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward --address 127.0.0.1 svc/milvus "${MILVUS_PORT:-19530}:19530" & pids+=($!)
  kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward --address 127.0.0.1 svc/monitoring-kube-prometheus-alertmanager "${ALERTMANAGER_PORT:-9093}:9093" & pids+=($!)
  if "$MAIL_CAPTURE"; then
    kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward --address 127.0.0.1 svc/full-stack-mailpit "${MAILPIT_PORT:-8025}:8025" & pids+=($!)
    echo "Captured email: http://127.0.0.1:${MAILPIT_PORT:-8025}"
  fi
  echo "Grafana: http://127.0.0.1:${GRAFANA_PORT:-3000} (admin/admin)"
  echo "Prometheus: http://127.0.0.1:${PROMETHEUS_PORT:-9099}"
  echo "Milvus: http://127.0.0.1:${MILVUS_PORT:-19530}"
  echo "Alertmanager: http://127.0.0.1:${ALERTMANAGER_PORT:-9093}"
  # Keep all forwards in this foreground process; Ctrl-C closes only these children.
  while true; do
    sleep 2
    for pid in "${pids[@]}"; do kill -0 "$pid" 2>/dev/null || return 1; done
  done
}
collect() {
  local out
  out="$STATE/evidence/$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$out"
  k get pods,svc,pvc,podmonitor,prometheusrule,alertmanager -o wide > "$out/resources.txt"
  k get events --sort-by=.lastTimestamp > "$out/events.txt"
  h list -o json > "$out/releases.json"
  k get pods -o 'custom-columns=NAME:.metadata.name,IMAGES:.spec.containers[*].image' > "$out/images.txt"
  if [ -f "$STATE/workload.jsonl" ]; then cp "$STATE/workload.jsonl" "$out/workload.jsonl"; fi
  if [ -f "$STATE/alerting-smoke.json" ]; then
    cp "$STATE/alerting-smoke.json" "$out/alerting-smoke.json"
  fi
  if "$MAIL_CAPTURE" && [ -f "$STATE/email-smoke.json" ]; then
    cp "$STATE/email-smoke.json" "$out/email-smoke.json"
  fi
  while IFS= read -r pod; do
    k logs "$pod" --all-containers --tail=1000 > "$out/${pod#pod/}.log" 2>&1 || true
  done < <(k get pods -o name)
  python3 "$ROOT/scripts/check.py" --output "$out/observability.json" || true
  python3 "$ROOT/scripts/check-alerts.py" --output "$out/alerting.json" || true
  echo "Evidence: $out (review application logs before sharing)"
}
alerts_test() {
  mkdir -p "$STATE/alert-tests"
  helm template full-stack "$ROOT/charts/full-stack-support" -n milvus-stack \
    --set alerts.renderRuleFile=true --show-only templates/alerts.yaml > "$STATE/alert-tests/rules.yaml"
  cp "$ROOT/tests/alerts.test.yaml" "$STATE/alert-tests/alerts.test.yaml"
  docker run --rm --network none --entrypoint /bin/promtool -v "$STATE/alert-tests:/work:ro" \
    quay.io/prometheus/prometheus:v3.0.1 test rules /work/alerts.test.yaml
}
case "$ACTION" in
  notebook) STATE="$STATE" bash "$ROOT/scripts/notebook.sh" ;;
  start)
    minikube start -p "$PROFILE" --driver=docker --cpus="${CPUS:-6}" \
      --memory="${MEMORY:-6656}" --kubernetes-version="$KUBERNETES_VERSION" ;;
  stop) minikube stop -p "$PROFILE" ;;
  clear) clear_stack ;;
  charts) charts ;;
  render) render ;;
  images) preload ;;
  deploy) deploy ;;
  status) h list; k get pods,pvc,podmonitor,prometheusrule,alertmanager ;;
  connections) connection_info --format "$CONNECTION_FORMAT" ;;
  forward) forward ;;
  smoke) python3 "$ROOT/scripts/workload.py" | tee "$STATE/workload.jsonl" ;;
  perf) python3 "$ROOT/scripts/perf.py" --context "$CONTEXT" --namespace "$NAMESPACE" --output "$STATE/perf" ;;
  verify) python3 "$ROOT/scripts/check.py" --output "$STATE/verification.json" ;;
  alerts) python3 "$ROOT/scripts/check-alerts.py" --output "$STATE/alerting.json" ;;
  alerts-smoke) python3 "$ROOT/scripts/check-alerts.py" --smoke --context "$CONTEXT" --output "$STATE/alerting-smoke.json" ;;
  email-smoke) python3 "$ROOT/scripts/check-email.py" --context "$CONTEXT" --output "$STATE/email-smoke.json" ;;
  acceptance)
    alerts_test
    python3 "$ROOT/scripts/workload.py" | tee "$STATE/workload.jsonl"
    # Wait for two scrapes so new workload metrics are visible to verification.
    sleep 35
    python3 "$ROOT/scripts/check.py" --output "$STATE/verification.json"
    python3 "$ROOT/scripts/check-alerts.py" --smoke --context "$CONTEXT" --output "$STATE/alerting-smoke.json"
    if "$MAIL_CAPTURE"; then
      python3 "$ROOT/scripts/check-email.py" --context "$CONTEXT" --output "$STATE/email-smoke.json"
    else
      echo "Email test skipped (enable explicitly with --mail-capture)."
    fi
    collect ;;
  alerts-test) alerts_test ;;
  collect) collect ;;
esac
