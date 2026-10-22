#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$ROOT/versions.env"
CONTEXT="${CONTEXT:-}"
NAMESPACE="${NAMESPACE:-milvus-stack}"
STATE="${STATE:-$ROOT/.local}"
TIMEOUT="${TIMEOUT:-12m}"
MILVUS_CHART="$STATE/charts/milvus-$MILVUS_CHART_VERSION.tgz"
MONITORING_CHART="$STATE/charts/kube-prometheus-stack-$MONITORING_CHART_VERSION.tgz"
EXECUTE=false
if [ "${1:-}" = -x ] || [ "${1:-}" = --execute ]; then EXECUTE=true; shift; fi
ACTION="${1:-help}"
if [ "$#" -gt 0 ]; then shift; fi
MV_VALUES=("$ROOT/values/milvus.yaml")
MON_VALUES=("$ROOT/values/monitoring.yaml")
# Keep this array nonempty for macOS Bash 3.2 with nounset enabled.
SUPPORT_VALUES=("$ROOT/charts/full-stack-support/values.yaml")
WITH=""
MAIL_CAPTURE=false
WPCLI=false
CONNECTION_FORMAT=text
usage() {
  cat <<EOF
Usage: ./stack.sh <command> [options]

Guided walkthrough
  notebook        Open the executable Jupyter walkthrough (local port 8891)

Stack lifecycle (existing Kubernetes context/namespace)
  start           Resume this stack using replica counts saved by stop
  stop            Pause this stack’s workloads; preserve Helm releases and PVCs
  install         Install/upgrade core Milvus + WP; preserve existing optional components
  uninstall       Uninstall all stack releases AND delete their persistent data

Optional component lifecycle
  addons-install   Add/upgrade selected components after core deployment (--with required)
  addons-uninstall Remove selected components AND their data (--with required)
  addons-status    Show the enabled optional components

Preparation
  charts          Download the pinned Helm charts
  render          Render manifests locally for inspection
  images          List images for the selected profile; registry/node setup is external

Access and status
  status          Show Helm releases, Pods, PVCs and selected optional components
  connections     Show host ports, WP service context and separate Milvus metadata connection
  forward         Open local service ports; Ctrl-C closes these forwards
                  Add --wpcli for all Woodpecker Admin ports and tenant etcd

Validation (keep forward running for live checks)
  acceptance      Workload + evidence; validate each installed observability backend
  smoke           Run a Milvus insert/query/search workload
  verify          Check scrape targets, workload metrics and dashboard queries
  logs            Check Loki readiness and collected namespace logs
  logs-smoke      Match a real Milvus/WP log line through Alloy into Loki
  traces          Check Jaeger services and trace evidence
  traces-smoke    Run a small read/write workload and check cross-service traces
  alerts          Inspect rule health, coverage, receiver and AM discovery
  alerts-test     Run offline alert behavior tests using Docker; no forward needed
  alerts-smoke    Test firing, recovery and inhibition with temporary alerts

Write performance and diagnosis
  perf            Bounded concurrent writes, latency/throughput and read checks
                  Save requests + Pod logs; metrics/alerts when selected

Diagnostics
  collect         Save status, bounded logs and verification reports locally

Optional email testing (disabled by default)
  email-smoke     Verify FIRING/RESOLVED emails using the Mailpit capture profile
                  acceptance --mail-capture also includes this check

Options
  -x, --execute            Execute the printed plan; default is preview only
  --context NAME           Use this kubeconfig context (default: kubectl current-context)
  --namespace NAME         Stack namespace (default: milvus-stack)
  --with LIST              grafana,prometheus,alertmanager,jaeger,loki or all
                           install: add these; addons-uninstall: remove only these
                           render/images/charts: choose an offline profile
  --local-arm64             Use the supplied ARM64 Milvus build
  -f, --values FILE          Additional Milvus Helm values (repeatable)
  --monitoring-values FILE  Additional monitoring Helm values (repeatable)
  --support-values FILE     Additional supporting-chart values (repeatable)
  --mail-capture            Enable the optional local SMTP capture profile
                            Repeat on render/images/install/forward/acceptance
  --wpcli                   forward: include per-Pod WP Admin and tenant etcd;
                            reuse matching existing forwards without stopping them
  --json                    connections: machine-readable connection inventory
  --wpcli-config            connections: emit a ready WP context YAML + separate metadata notes
  -h, --help                Show this help (also supported after a command)

Targets and environment
  CONTEXT=${CONTEXT:-<kubectl current-context>}  NAMESPACE=$NAMESPACE
    Uses existing Kubernetes; never creates, starts or stops the cluster
  STATE=$STATE
    Local chart cache, rendered manifests and reports; preserved by uninstall
  TIMEOUT=$TIMEOUT        Helm/rollout timeout
  Optional ports: GRAFANA_PORT=3000 PROMETHEUS_PORT=9099 MILVUS_PORT=19530
               ALERTMANAGER_PORT=9093 JAEGER_PORT=16686 LOKI_PORT=3100 MAILPIT_PORT=8025 (optional)
  wpcli ports: WP_ADMIN_PORT_BASE=19091 (plus Pod ordinal), ETCD_PORT=32379
  CONNECTIONS_PYTHON=python3  Python with PyYAML for connections/forward --wpcli
  Perf defaults: PERF_DURATION=60 PERF_CONCURRENCY=2 PERF_BATCH_SIZE=100
                 PERF_DIMENSION=128 PERF_RPS=5 (total batch requests/second)
  Perf gates: PERF_MIN_ROWS_PER_SECOND=0 PERF_MAX_P99_MS=0 (0 disables a gate)
  Trace sampling: PERF_TRACE_SAMPLE_FRACTION=1 (client); edit shared server values too
  Strict trace gate: TRACES_REQUIRE_E2E=1 (traces-smoke/acceptance)
  NOTEBOOK_PORT=8891     JupyterLab port; first launch installs an isolated runtime

Typical workflow (configure Kubernetes and node image access separately)
  kubectl config current-context
  ./stack.sh install --local-arm64       # Print native Helm/kubectl operations
  ./stack.sh install --local-arm64 -x    # Install/upgrade core on existing Kubernetes
  ./stack.sh addons-install --with all -x
  ./stack.sh forward --wpcli -x         # Keep running in a separate terminal
  ./stack.sh connections               # Read-only connection discovery
  ./stack.sh acceptance -x             # Runs writes and temporary alert probes
  ./stack.sh perf -x

Pause/resume workloads: ./stack.sh stop -x -> ./stack.sh start -x
Delete stack and PVCs: ./stack.sh uninstall -x

Without -x, resource changes, forwards and workloads are printed rather than run.
Read-only discovery/status and local charts/render/images/notebook remain available.
Planning may read cluster state and write local plan files; it does not change resources.
stop retains PVC data; in-memory/ephemeral history is not retained when Pods exit.
uninstall preserves Kubernetes, the namespace, shared CRDs and local evidence.
EOF
}
while [ "$#" -gt 0 ]; do
  case "$1" in
    -x|--execute) EXECUTE=true; shift ;;
    --context|--namespace)
      if [ "$#" -lt 2 ] || [ -z "$2" ]; then echo "$1 needs a value" >&2; exit 2; fi
      if [ "$1" = --context ]; then CONTEXT="$2"; else NAMESPACE="$2"; fi
      shift 2 ;;
    --with)
      if [ "$#" -lt 2 ] || [ -z "$2" ]; then echo "--with needs a component list" >&2; exit 2; fi
      WITH="${WITH:+$WITH,}$2"; shift 2 ;;
    -h|--help) ACTION=help; shift ;;
    --local-arm64) MV_VALUES+=("$ROOT/values/local-arm64.yaml"); shift ;;
    --wpcli) WPCLI=true; shift ;;
    --json|--wpcli-config)
      if [ "$CONNECTION_FORMAT" != text ]; then echo "Choose one connection output format" >&2; exit 2; fi
      if [ "$1" = --json ]; then CONNECTION_FORMAT=json; else CONNECTION_FORMAT=wpcli; fi
      shift ;;
    --mail-capture)
      MAIL_CAPTURE=true
      WITH="${WITH:+$WITH,}prometheus,alertmanager"
      MON_VALUES+=("$ROOT/values/mail-capture.yaml")
      SUPPORT_VALUES+=("$ROOT/values/mail-capture.yaml"); shift ;;
    --monitoring-values|--support-values)
      if [ "$#" -lt 2 ]; then echo "Missing values file" >&2; exit 2; fi
      if [ "$1" = --monitoring-values ]; then MON_VALUES+=("$2"); else SUPPORT_VALUES+=("$2"); fi
      shift 2 ;;
    -f|--values)
      if [ "$#" -lt 2 ]; then echo "Missing values file" >&2; exit 2; fi
      MV_VALUES+=("$2"); shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done
case "$ACTION" in
  help|-h|--help) usage; exit 0 ;;
  notebook|start|stop|install|addons-install|addons-uninstall|addons-status|uninstall|charts|render|images|status|connections|forward|smoke|perf|verify|logs|logs-smoke|traces|traces-smoke|alerts|alerts-smoke|email-smoke|acceptance|alerts-test|collect) ;;
  *) echo "Unknown command: $ACTION (see ./stack.sh help)" >&2; exit 2 ;;
esac
if "$WPCLI" && [ "$ACTION" != forward ]; then echo "--wpcli applies only to forward" >&2; exit 2; fi
if [ "$CONNECTION_FORMAT" != text ] && [ "$ACTION" != connections ]; then echo "Connection output options apply only to connections" >&2; exit 2; fi
python3 "$ROOT/scripts/components.py" validate --with "$WITH"
if [[ "$ACTION" == addons-* && "$ACTION" != addons-status && -z "$WITH" ]]; then echo "--with is required for addon changes" >&2; exit 2; fi
# Offline preparation needs no kubeconfig. Every live operation uses an existing context.
case "$ACTION" in
  charts|render|images|notebook|alerts-test) : ;;
  *)
    for tool in kubectl helm; do
      if ! command -v "$tool" >/dev/null 2>&1; then echo "Install $tool and configure your existing Kubernetes environment first." >&2; exit 2; fi
    done
    CONTEXT=$(python3 "$ROOT/scripts/kube-context.py" --context "$CONTEXT" --namespace "$NAMESPACE" --action "$ACTION") || exit 2 ;;
esac
export CONTEXT NAMESPACE
mkdir -p "$STATE/charts" "$STATE/rendered"
# Keep Helm's caches/configuration local to this example.
export HELM_CACHE_HOME="$STATE/helm/cache"
export HELM_CONFIG_HOME="$STATE/helm/config"
export HELM_DATA_HOME="$STATE/helm/data"
k() { kubectl --context "$CONTEXT" -n "$NAMESPACE" "$@"; }
h() { helm --kube-context "$CONTEXT" -n "$NAMESPACE" "$@"; }
run() {
  printf '%q ' "$@"; printf '\n'
  if "$EXECUTE"; then "$@"; fi
}
km() { run kubectl --context "$CONTEXT" -n "$NAMESPACE" "$@"; }
hm() { run helm --kube-context "$CONTEXT" -n "$NAMESPACE" "$@"; }
connection_info() { "${CONNECTIONS_PYTHON:-python3}" "$ROOT/scripts/connections.py" --context "$CONTEXT" --namespace "$NAMESPACE" "$@"; }
uninstall_stack() {
  # Uninstall consumers before their dependencies. Missing releases are fine,
  # allowing cleanup to be repeated after a partial installation or uninstall.
  hm uninstall milvus --ignore-not-found --wait --timeout "$TIMEOUT"
  hm uninstall full-stack --ignore-not-found --wait --timeout "$TIMEOUT"
  hm uninstall monitoring --ignore-not-found --wait --timeout "$TIMEOUT"
  # StatefulSet/operator PVCs outlive Helm releases. Select only this stack's
  # release identities, including all replica ordinals and optional Grafana PVCs.
  km delete pvc --selector 'app.kubernetes.io/instance in (milvus,monitoring,monitoring-kube-prometheus-prometheus,monitoring-kube-prometheus-alertmanager)' \
    --ignore-not-found --wait --timeout="$TIMEOUT"
  km delete pvc --selector 'app in (full-stack-etcd,full-stack-minio,full-stack-loki)' \
    --ignore-not-found --wait --timeout="$TIMEOUT"
  km delete configmap milvus-full-stack-lifecycle --ignore-not-found
  if "$EXECUTE"; then echo "Stack releases and PVCs deleted from $CONTEXT/$NAMESPACE; cluster and local evidence retained."; fi
}
prepare() {
  local mode="$1" file
  mkdir -p "$STATE/plans"
  PLAN=$(mktemp -d "$STATE/plans/plan-XXXXXXXX")
  plan_args=("$mode" --context "$CONTEXT" --namespace "$NAMESPACE" --with "$WITH" --output "$PLAN")
  for file in "${MV_VALUES[@]}"; do
    if [ "$file" != "$ROOT/values/milvus.yaml" ]; then plan_args+=(--milvus-values "$file"); fi
  done
  for file in "${MON_VALUES[@]}"; do
    if [ "$file" != "$ROOT/values/monitoring.yaml" ]; then plan_args+=(--monitoring-values "$file"); fi
  done
  for file in "${SUPPORT_VALUES[@]}"; do
    if [ "$file" != "$ROOT/charts/full-stack-support/values.yaml" ]; then plan_args+=(--support-values "$file"); fi
  done
  python3 "$ROOT/scripts/components.py" "${plan_args[@]}"
  # Generated file contains only fixed boolean flags, never credentials or YAML.
  # shellcheck source=/dev/null
  source "$PLAN/flags.env"
}
features() {
  local flags
  flags=$(python3 "$ROOT/scripts/components.py" flags --context "$CONTEXT" --namespace "$NAMESPACE")
  # Only the known boolean flags printed by components.py are evaluated.
  eval "$flags"
}
charts() {
  if "$NEED_MILVUS" && [ ! -s "$MILVUS_CHART" ]; then
    curl -fL --retry 2 --connect-timeout 15 --max-time 180 \
      "https://github.com/zilliztech/milvus-helm/releases/download/milvus-$MILVUS_CHART_VERSION/milvus-$MILVUS_CHART_VERSION.tgz" \
      -o "$MILVUS_CHART.part"
    mv "$MILVUS_CHART.part" "$MILVUS_CHART"
  fi
  if "$NEED_MONITORING" && [ ! -s "$MONITORING_CHART" ]; then
    helm pull kube-prometheus-stack --repo https://prometheus-community.github.io/helm-charts \
      --version "$MONITORING_CHART_VERSION" --destination "$STATE/charts"
  fi
}
render() {
  prepare render
  charts
  mkdir -p "$PLAN/rendered"
  helm template milvus "$MILVUS_CHART" -n "$NAMESPACE" -f "$PLAN/milvus.yaml" > "$PLAN/rendered/milvus.yaml"
  : > "$PLAN/rendered/monitoring.yaml"
  if "$NEED_MONITORING"; then
    helm template monitoring "$MONITORING_CHART" -n "$NAMESPACE" -f "$PLAN/monitoring.yaml" > "$PLAN/rendered/monitoring.yaml"
  fi
  helm template full-stack "$ROOT/charts/full-stack-support" -n "$NAMESPACE" -f "$PLAN/support.yaml" > "$PLAN/rendered/support.yaml"
  cp "$PLAN/rendered/"*.yaml "$STATE/rendered/"
  echo "Rendered manifests: $STATE/rendered (isolated plan: $PLAN)"
}
image_inventory() {
  render
  sed -nE 's/^[[:space:]]*image:[[:space:]]*"?([^"[:space:]]+).*/\1/p' \
    "$PLAN"/rendered/*.yaml | sort -u > "$PLAN/images.txt"
  if "$NEED_MONITORING"; then
    sed -nE 's/.*--prometheus-config-reloader=([^"[:space:]]+).*/\1/p' \
      "$PLAN/rendered/monitoring.yaml" >> "$PLAN/images.txt"
  fi
  cp "$PLAN/images.txt" "$STATE/images.txt"
  cat "$PLAN/images.txt"
}

install_stack() {
  if k get namespace "$NAMESPACE" >/dev/null 2>&1; then
    paused=$(k get configmap milvus-full-stack-lifecycle --ignore-not-found -o name)
    if [ -n "$paused" ]; then echo "Stack is paused. Resume with ./stack.sh start -x before changing Helm releases." >&2; return 2; fi
  else
    km create namespace "$NAMESPACE"
  fi
  km label namespace "$NAMESPACE" full-stack=milvus --overwrite
  prepare "$ACTION"
  charts
  # Stop exporting before removing Jaeger. Addon changes preserve all live core overrides.
  if [ "$ACTION" = addons-uninstall ] && "$TRACE_CHANGED"; then
    hm upgrade milvus "$MILVUS_CHART" -f "$PLAN/milvus.yaml" --wait --timeout "$TIMEOUT"
  fi
  if "$NEED_MONITORING"; then
    # Helm installs CRDs only on first install. Grafana may have been installed
    # alone first, so ensure the pinned CRDs before enabling operator-backed addons.
    if "$HAS_PROMETHEUS" || "$HAS_ALERTMANAGER"; then
      helm show crds "$MONITORING_CHART" > "$PLAN/crds.yaml"
      km apply --server-side --field-manager=milvus-full-stack -f "$PLAN/crds.yaml"
    fi
    hm upgrade --install monitoring "$MONITORING_CHART" --skip-crds -f "$PLAN/monitoring.yaml" --wait --timeout "$TIMEOUT"
  fi
  hm upgrade --install full-stack "$ROOT/charts/full-stack-support" -f "$PLAN/support.yaml" --wait --timeout "$TIMEOUT"
  if "$NEED_MILVUS" && [ "$ACTION" != addons-uninstall ]; then
    hm upgrade --install milvus "$MILVUS_CHART" -f "$PLAN/milvus.yaml" --wait --timeout "$TIMEOUT"
  fi
  if ! "$NEED_MONITORING" && "$HAD_MONITORING"; then hm uninstall monitoring --wait --timeout "$TIMEOUT"; fi
  if [ "$ACTION" = addons-uninstall ]; then
    if "$REMOVE_LOKI"; then km delete pvc --selector 'app in (full-stack-loki)' --ignore-not-found --wait; fi
    if "$REMOVE_PROMETHEUS"; then km delete pvc --selector 'app.kubernetes.io/instance in (monitoring-kube-prometheus-prometheus)' --ignore-not-found --wait; fi
    if "$REMOVE_ALERTMANAGER"; then km delete pvc --selector 'app.kubernetes.io/instance in (monitoring-kube-prometheus-alertmanager)' --ignore-not-found --wait; fi
    if "$REMOVE_GRAFANA"; then km delete pvc --selector 'app.kubernetes.io/name=grafana,app.kubernetes.io/instance=monitoring' --ignore-not-found --wait; fi
  fi
  if "$EXECUTE"; then k get pods,pvc; fi
}
forward() {
  forward_args=(--forward)
  if "$EXECUTE"; then forward_args+=(--execute); fi
  if ! "$WPCLI"; then forward_args+=(--services-only); fi
  if "$MAIL_CAPTURE"; then forward_args+=(--mail-capture); fi
  connection_info "${forward_args[@]}"
}
verify() {
  if ! "$HAS_PROMETHEUS"; then echo "Install Prometheus first: ./stack.sh addons-install --with prometheus -x" >&2; return 2; fi
  if "$HAS_GRAFANA"; then
    python3 "$ROOT/scripts/check.py" --output "$STATE/verification.json"
  else
    python3 "$ROOT/scripts/check.py" --skip-grafana --output "$STATE/verification.json"
  fi
}
perf() {
  perf_args=(--context "$CONTEXT" --namespace "$NAMESPACE" --output "$STATE/perf")
  if ! "$HAS_PROMETHEUS"; then perf_args+=(--skip-metrics); fi
  if ! "$HAS_ALERTMANAGER"; then perf_args+=(--skip-alerts); fi
  python3 "$ROOT/scripts/perf.py" "${perf_args[@]}"
}
collect() {
  local out
  out="$STATE/evidence/$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$out"
  k get pods,svc,pvc -o wide > "$out/resources.txt"
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
  if "$HAS_PROMETHEUS"; then python3 "$ROOT/scripts/check.py" --skip-grafana --output "$out/observability.json" || true; fi
  if "$HAS_PROMETHEUS" && "$HAS_ALERTMANAGER"; then python3 "$ROOT/scripts/check-alerts.py" --output "$out/alerting.json" || true; fi
  if "$HAS_JAEGER"; then python3 "$ROOT/scripts/check-traces.py" --output "$out/tracing.json" || true; fi
  if "$HAS_LOKI"; then python3 "$ROOT/scripts/check-logs.py" --namespace "$NAMESPACE" --output "$out/logging.json" || true; fi
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
if ! "$EXECUTE"; then
  case "$ACTION" in
    smoke|perf|logs-smoke|traces-smoke|alerts-smoke|email-smoke|acceptance)
      case "$ACTION" in
        smoke) run env "CONTEXT=$CONTEXT" "NAMESPACE=$NAMESPACE" python3 "$ROOT/scripts/workload.py" ;;
        perf) features; perf_args=(--context "$CONTEXT" --namespace "$NAMESPACE" --output "$STATE/perf")
              if ! "$HAS_PROMETHEUS"; then perf_args+=(--skip-metrics); fi
              if ! "$HAS_ALERTMANAGER"; then perf_args+=(--skip-alerts); fi
              run env "CONTEXT=$CONTEXT" "NAMESPACE=$NAMESPACE" python3 "$ROOT/scripts/perf.py" "${perf_args[@]}" ;;
        logs-smoke) run env "CONTEXT=$CONTEXT" "NAMESPACE=$NAMESPACE" python3 "$ROOT/scripts/check-logs.py" --smoke --context "$CONTEXT" --namespace "$NAMESPACE" --output "$STATE/logging-smoke.json" ;;
        traces-smoke) run env "CONTEXT=$CONTEXT" "NAMESPACE=$NAMESPACE" python3 "$ROOT/scripts/check-traces.py" --smoke --output "$STATE/tracing-smoke.json" ;;
        alerts-smoke) run env "CONTEXT=$CONTEXT" "NAMESPACE=$NAMESPACE" python3 "$ROOT/scripts/check-alerts.py" --smoke --context "$CONTEXT" --output "$STATE/alerting-smoke.json" ;;
        email-smoke) run env "CONTEXT=$CONTEXT" "NAMESPACE=$NAMESPACE" python3 "$ROOT/scripts/check-email.py" --context "$CONTEXT" --output "$STATE/email-smoke.json" ;;
        acceptance)
          echo "# Acceptance writes test data and temporary alert resources. Execute explicitly:"
          run "$ROOT/stack.sh" acceptance -x --context "$CONTEXT" --namespace "$NAMESPACE" ;;
      esac
      exit 0 ;;
  esac
fi
case "$ACTION" in
  notebook) STATE="$STATE" bash "$ROOT/scripts/notebook.sh" ;;
  start|stop)
    mkdir -p "$STATE/plans"
    PLAN=$(mktemp -d "$STATE/plans/lifecycle-XXXXXXXX")
    lifecycle_args=("$ACTION" --context "$CONTEXT" --namespace "$NAMESPACE" --output "$PLAN" --timeout "$TIMEOUT")
    if "$EXECUTE"; then lifecycle_args+=(--execute); fi
    python3 "$ROOT/scripts/lifecycle.py" "${lifecycle_args[@]}" ;;
  uninstall) uninstall_stack ;;
  charts) prepare render; charts ;;
  render) render ;;
  images) image_inventory ;;
  install|addons-install|addons-uninstall) install_stack ;;
  addons-status) python3 "$ROOT/scripts/components.py" status --context "$CONTEXT" --namespace "$NAMESPACE" ;;
  status) h list; k get pods,pvc; python3 "$ROOT/scripts/components.py" status --context "$CONTEXT" --namespace "$NAMESPACE" ;;
  connections) connection_info --format "$CONNECTION_FORMAT" ;;
  forward) forward ;;
  smoke) python3 "$ROOT/scripts/workload.py" | tee "$STATE/workload.jsonl" ;;
  perf) features; perf ;;
  verify) features; verify ;;
  logs) python3 "$ROOT/scripts/check-logs.py" --namespace "$NAMESPACE" --output "$STATE/logging.json" ;;
  logs-smoke) python3 "$ROOT/scripts/check-logs.py" --smoke --context "$CONTEXT" --namespace "$NAMESPACE" --output "$STATE/logging-smoke.json" ;;
  traces) python3 "$ROOT/scripts/check-traces.py" --output "$STATE/tracing.json" ;;
  traces-smoke) python3 "$ROOT/scripts/check-traces.py" --smoke --output "$STATE/tracing-smoke.json" ;;
  alerts) python3 "$ROOT/scripts/check-alerts.py" --output "$STATE/alerting.json" ;;
  alerts-smoke) python3 "$ROOT/scripts/check-alerts.py" --smoke --context "$CONTEXT" --output "$STATE/alerting-smoke.json" ;;
  email-smoke) python3 "$ROOT/scripts/check-email.py" --context "$CONTEXT" --output "$STATE/email-smoke.json" ;;
  acceptance)
    features
    if "$HAS_GRAFANA"; then curl -fsS "${GRAFANA_URL:-http://127.0.0.1:${GRAFANA_PORT:-3000}}/api/health" > "$STATE/grafana-health.json"; fi
    if "$HAS_ALERTMANAGER"; then curl -fsS "${ALERTMANAGER_URL:-http://127.0.0.1:${ALERTMANAGER_PORT:-9093}}/-/ready" > "$STATE/alertmanager-health.txt"; fi
    if "$HAS_PROMETHEUS"; then alerts_test; fi
    python3 "$ROOT/scripts/workload.py" | tee "$STATE/workload.jsonl"
    if "$HAS_JAEGER"; then python3 "$ROOT/scripts/check-traces.py" --smoke --output "$STATE/tracing-smoke.json"; fi
    if "$HAS_LOKI"; then python3 "$ROOT/scripts/check-logs.py" --smoke --context "$CONTEXT" --namespace "$NAMESPACE" --output "$STATE/logging-smoke.json"; fi
    # Wait for two scrapes so new workload metrics are visible to verification.
    if "$HAS_PROMETHEUS"; then sleep 35; verify; fi
    if "$HAS_PROMETHEUS" && "$HAS_ALERTMANAGER"; then python3 "$ROOT/scripts/check-alerts.py" --smoke --context "$CONTEXT" --output "$STATE/alerting-smoke.json"; fi
    if "$MAIL_CAPTURE"; then
      python3 "$ROOT/scripts/check-email.py" --context "$CONTEXT" --output "$STATE/email-smoke.json"
    else
      echo "Email test skipped (enable explicitly with --mail-capture)."
    fi
    collect ;;
  alerts-test) alerts_test ;;
  collect) features; collect ;;
esac
