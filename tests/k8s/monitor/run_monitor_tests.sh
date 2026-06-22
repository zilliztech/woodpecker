#!/bin/bash
# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
