#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Chaos knobs (override lib.sh defaults) — spec Layer 1 + Correction #8.
export CLUSTER_NAME="wp-chaos"
export CR_NAME="my-woodpecker"
export NAMESPACE="default"
export REPLICAS=3
# Default to minikube's docker runtime: it uses built-in bridge networking and needs no
# external CNI image. The containerd runtime forces a kindnet CNI whose image
# (kindest/kindnetd:<ver>) frequently cannot be pulled on constrained/offline hosts, leaving
# the cluster with no pod networking ("failed to find network info for sandbox"). Chaos Mesh
# supports the docker runtime, and the injection smoke-check guards correctness either way.
# Override with MK_RUNTIME=containerd only where the kindnet image is reachable.
export MK_CPUS="${MK_CPUS:-6}" MK_MEMORY="${MK_MEMORY:-8192}" MK_RUNTIME="${MK_RUNTIME:-}"
export WP_IMG="zilliztech/woodpecker:v0.1.26"
export CLIENT_POD="wp-client-test"

source "$PROJECT_ROOT/deployments/operator/test/lib.sh"

WORKLOAD_IN_POD=/root/woodpecker/tests/chaos_mesh/workload
RECORD_FILE=/tmp/wp-chaos-acked.jsonl

# External dependency images (keep etcd/minio in sync with deployments/operator/test/lib.sh).
ETCD_IMG="quay.io/coreos/etcd:v3.5.18"
MINIO_IMG="minio/minio:RELEASE.2024-06-13T22-53-53Z"

# The minikube node can't reach external registries when the host uses a loopback proxy
# (HTTP_PROXY=127.0.0.1:xxxx — minikube logs "Local proxy ignored"). The HOST can pull (its
# proxy works), so for images we don't build locally we pull on the host and load into the node.
preload_image() {  # $1 = image ref
  log "preload: $1"
  docker pull "$1" || fail "host 'docker pull $1' failed (proxy/network?)"
  minikube -p "$CLUSTER_NAME" image load "$1" || fail "'minikube image load $1' failed"
}

bringup() {
  wp_minikube_start
  # Single-node minikube has no zone label, but the operator's StatefulSet sets a topology
  # spread constraint (topologyKey=topology.kubernetes.io/zone, whenUnsatisfiable=DoNotSchedule),
  # so server pods stay Pending ("didn't match pod topology spread constraints"). Give the node a
  # synthetic zone/region so all replicas can schedule (we don't test physical AZ separation).
  kubectl label node "$CLUSTER_NAME" topology.kubernetes.io/zone=zone-a topology.kubernetes.io/region=region-local --overwrite
  preload_image "$ETCD_IMG"
  preload_image "$MINIO_IMG"
  wp_deploy_operator; wp_build_wp_image
  wp_deploy_deps; wp_create_cr; wp_wait_healthy
  wp_launch_client_pod; wp_write_client_config "$REPLICAS"
}

install_chaos_mesh() {
  # chaos-daemon must point at the SAME container runtime minikube uses, else injection is a
  # silent no-op (caught by injection_smoke_check). Empty MK_RUNTIME = minikube's docker runtime.
  local daemon_runtime daemon_socket
  if [ "${MK_RUNTIME:-}" = "containerd" ]; then
    daemon_runtime=containerd; daemon_socket=/run/containerd/containerd.sock
  else
    daemon_runtime=docker; daemon_socket=/var/run/docker.sock
  fi
  log "installing chaos-mesh (chaosDaemon.runtime=$daemon_runtime socket=$daemon_socket)"
  helm repo add chaos-mesh https://charts.chaos-mesh.org 2>/dev/null || true
  helm repo update
  # Preload chaos-mesh images: the node can't reach ghcr.io through the host's loopback proxy.
  # dashboard.create=false trims a pod we don't need (saves resources on the tight node).
  local cm_imgs img
  cm_imgs=$(helm template chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --set dashboard.create=false 2>/dev/null \
    | grep -hoE 'image: *"?[^"[:space:]]+' | sed -E 's/image: *"?//' | sort -u)
  while read -r img; do
    [ -n "$img" ] && preload_image "$img"
  done <<< "$cm_imgs"
  helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace \
    --set chaosDaemon.runtime="$daemon_runtime" \
    --set chaosDaemon.socketPath="$daemon_socket" \
    --set dashboard.create=false \
    --wait --timeout 5m
  kubectl -n chaos-mesh rollout status daemonset/chaos-daemon --timeout=180s
}

# Deliver CURRENT source into the pod (Correction #5 / refinement B), then it's ready for `go test`.
push_workload() {
  local tgz=/tmp/wp-src.tgz
  tar --exclude='./.git' --exclude='./tests/chaos_mesh/artifacts' -czf "$tgz" -C "$PROJECT_ROOT" .
  kubectl exec "$CLIENT_POD" -- mkdir -p /root/woodpecker
  kubectl cp "$tgz" "$CLIENT_POD:/tmp/wp-src.tgz"
  kubectl exec "$CLIENT_POD" -- bash -c "tar -xzf /tmp/wp-src.tgz -C /root/woodpecker"
  kubectl exec "$CLIENT_POD" -- bash -c "cd $WORKLOAD_IN_POD && go build ./..." \
    || fail "in-pod go build failed (module/source delivery problem)"
}

# Spec Layer 2 / Risk #1: prove injection is NOT a silent no-op before trusting any result.
injection_smoke_check() {
  local ip; ip=$(kubectl get pod ${CR_NAME}-server-0 -o jsonpath='{.status.podIP}')
  local probe="S=\$(date +%s%N); (exec 3<>/dev/tcp/$ip/18080) 2>/dev/null; echo \$(( (\$(date +%s%N)-S)/1000000 ))"
  local before; before=$(kubectl exec "$CLIENT_POD" -- bash -c "$probe")
  kubectl apply -f "$SCRIPT_DIR/manifests/client-server-delay.yaml"
  kubectl wait --for=condition=AllInjected networkchaos/client-server-delay -n "$NAMESPACE" --timeout=60s \
    || { kubectl describe networkchaos client-server-delay -n "$NAMESPACE"; fail "chaos never reached AllInjected"; }
  local after; after=$(kubectl exec "$CLIENT_POD" -- bash -c "$probe")
  kubectl delete -f "$SCRIPT_DIR/manifests/client-server-delay.yaml" --ignore-not-found
  log "smoke-check connect ms: before=$before after=$after"
  if [ "$after" -lt $(( before + 200 )) ]; then
    kubectl logs -n chaos-mesh -l app.kubernetes.io/component=chaos-daemon --tail=200 || true
    fail "INJECTION SMOKE-CHECK FAILED (before=${before}ms after=${after}ms) — likely runtime/socket mismatch; chaos is a silent no-op. Aborting before false-green."
  fi
  log "Injection smoke-check PASSED (${before}ms -> ${after}ms)"
}

run_phase() {  # $1 = phase ; $2 = optional extra go-test flags
  kubectl exec -i "$CLIENT_POD" -- /bin/bash -c "
    set -e; cd $WORKLOAD_IN_POD
    go test -v -count=1 -timeout 16m -run TestNetworkChaosWorkload \
      -config-file /tmp/test-config.yaml -phase $1 -record-file $RECORD_FILE ${2:-} ."
}

restart_sum() {  # I6: total server container restarts (host-side; client pod is itself under chaos)
  kubectl get pod -l app.kubernetes.io/instance=${CR_NAME},app.kubernetes.io/component=server \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"+"}{end}0' | bc
}

collect_artifacts() {  # mirror integration-test-chaos.yaml log collection
  local scen="$1" out="$SCRIPT_DIR/artifacts/$1"; mkdir -p "$out"
  for i in $(seq 0 $((REPLICAS-1))); do kubectl logs "${CR_NAME}-server-$i" >"$out/server-$i.log" 2>&1 || true; done
  kubectl get networkchaos -A -o yaml            >"$out/networkchaos.yaml" 2>&1 || true
  kubectl get events -A --sort-by=.lastTimestamp >"$out/events.txt" 2>&1 || true
  kubectl get pods -o wide                        >"$out/pods.txt" 2>&1 || true
  kubectl logs etcd  >"$out/etcd.log"  2>&1 || true
  kubectl logs minio >"$out/minio.log" 2>&1 || true
  kubectl cp "$CLIENT_POD:$RECORD_FILE" "$out/acked.jsonl" 2>/dev/null || true
  echo "restarts: $(restart_sum)" >"$out/restartcounts.txt"
}

run_steady() {  # $1 = manifest basename
  local m="$SCRIPT_DIR/manifests/$1.yaml" rc0; rc0=$(restart_sum)
  run_phase warmup || { collect_artifacts "$1"; return 1; }
  kubectl apply -f "$m" || { collect_artifacts "$1"; return 1; }
  kubectl wait --for=condition=AllInjected "networkchaos/$1" -n "$NAMESPACE" --timeout=60s \
      || { collect_artifacts "$1"; kubectl delete -f "$m" --ignore-not-found; return 1; }
  run_phase under-chaos || { collect_artifacts "$1"; kubectl delete -f "$m" --ignore-not-found; return 1; }
  kubectl delete -f "$m" --ignore-not-found
  run_phase recovery || { collect_artifacts "$1"; return 1; }
  run_phase verify   || { collect_artifacts "$1"; return 1; }
  [ "$(restart_sum)" -eq "$rc0" ] || { collect_artifacts "$1"; fail "I6 VIOLATION: server restarted during $1"; }
  log "SCENARIO $1 PASSED"
}

run_blip() {  # $1 = manifest basename (N3/N5)
  local m="$SCRIPT_DIR/manifests/$1.yaml" rc0; rc0=$(restart_sum)
  run_phase warmup || { collect_artifacts "$1"; return 1; }
  ( run_phase under-chaos "-window 60" ) & local wl=$!
  for _ in $(seq 1 6); do kubectl apply -f "$m" || warn "blip apply failed for $1"; sleep 5; kubectl delete -f "$m" --ignore-not-found; sleep 5; done
  wait "$wl" || { collect_artifacts "$1"; return 1; }
  run_phase recovery && run_phase verify || { collect_artifacts "$1"; return 1; }
  [ "$(restart_sum)" -eq "$rc0" ] || { collect_artifacts "$1"; fail "I6 VIOLATION: server restarted during $1"; }
  log "SCENARIO $1 PASSED"
}

main() {
  bringup; install_chaos_mesh; injection_smoke_check; push_workload
  local rc=0
  for s in server-minio-delay server-minio-loss server-etcd-delay client-server-delay client-server-loss; do
    run_steady "$s" || rc=1
  done
  for s in server-minio-blip server-etcd-blip; do run_blip "$s" || rc=1; done
  [ "$rc" -eq 0 ] && log "ALL SCENARIOS PASSED" || warn "SOME SCENARIOS FAILED (see artifacts/)"
  exit $rc
}

trap 'rc=$?; [ -n "${KEEP:-}" ] || { helm uninstall chaos-mesh -n chaos-mesh 2>/dev/null || true; minikube delete -p "$CLUSTER_NAME" 2>/dev/null || true; }; exit $rc' EXIT
main "$@"
