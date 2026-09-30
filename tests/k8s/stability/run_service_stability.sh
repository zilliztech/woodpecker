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

# Service stability, K8s tier (nightly). Brings up minikube with the operator
# and a 5-pod WoodpeckerCluster, then runs each TestServiceStabilityK8s_<Case>
# of workload/ in a client pod while this script faults a pod from outside:
#
#   RestartIdlePod, RestartQuorumPod   kubectl delete pod (graceful)
#   KillQuorumPod                      SIGKILL the server process; the container restarts in place
#   KillQuorumPod_NeverReturns         SIGKILL, then Chaos Mesh PodChaos pod-failure, lifted after the workload ends
#   KillQuorumPod_Rescheduled          SIGSTOP, then force delete: dies without a leave, comes back with a new IP (#395)
#   VanishQuorumPod, VanishReadPod_*   Chaos Mesh NetworkChaos partition for 10s
#   RollingRestartAllPods              graceful delete of every pod, highest ordinal first
#
# A deleted pod is replaced under its DNS name with a new IP, as in production.
# A killed one is not deleted: its container restarts in the same pod, as after
# an OOM kill or a crash.
#
# "Kill" means SIGKILL to the server process (pkill -KILL -x woodpecker): no
# gossip leave, no drain. Deleting the pod, even with --grace-period=0 --force,
# would not do: the kubelet still sends SIGTERM first, which the server handles
# with a graceful stop. PID 1 is tini, which the kernel does not let a SIGKILL
# from inside the pod's namespace reach, so the server itself is killed.
#
# The workload writes the address of the pod to fault to <case>.target and
# waits for <case>.done; everything in between is this script's.
#
# Env:
#   CASES                  space-separated case names (default: all)
#   WP_STABILITY_LATENCY   "report" (default here) reports the stall budget
#                          instead of failing on it; "enforce" fails on it.
#                          Completeness and order are always enforced.
#   KEEP=1                 keep the minikube cluster afterwards

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

export CLUSTER_NAME="wp-stability"
export CR_NAME="my-woodpecker"
export NAMESPACE="default"
# 5 pods for an ensemble of 3, so a pod that is down still leaves enough nodes
# for new segments: what is measured is the fault, not a capacity shortage.
export REPLICAS=5
export ENSEMBLE=3
export MK_CPUS="${MK_CPUS:-6}" MK_MEMORY="${MK_MEMORY:-8192}" MK_RUNTIME="${MK_RUNTIME:-}"
export WP_IMG="zilliztech/woodpecker:v0.1.26"
export CLIENT_POD="wp-client-test"

source "$PROJECT_ROOT/deployments/operator/test/lib.sh"
source "$PROJECT_ROOT/tests/chaos_mesh/chaos_mesh_lib.sh"

ETCD_IMG="quay.io/coreos/etcd:v3.5.18"
MINIO_IMG="milvusdb/minio:RELEASE.2024-12-18T13-15-44Z"
WORKLOAD_BIN_IN_POD=/root/service_stability.test
STATE_DIR=/tmp/wp-stability
ARTIFACTS="$SCRIPT_DIR/artifacts"
LATENCY_MODE="${WP_STABILITY_LATENCY:-report}"
ALL_CASES="Baseline RestartIdlePod RestartQuorumPod KillQuorumPod KillQuorumPod_NeverReturns KillQuorumPod_Rescheduled VanishQuorumPod VanishReadPod_SeparateReader RollingRestartAllPods"
CASES="${CASES:-$ALL_CASES}"
SERVER_SELECTOR="app.kubernetes.io/instance=${CR_NAME},app.kubernetes.io/component=server"

bringup() {
  wp_minikube_start
  # The operator's StatefulSet spreads pods across zones; give the single node one.
  kubectl label node "$CLUSTER_NAME" topology.kubernetes.io/zone=zone-a topology.kubernetes.io/region=region-local --overwrite
  preload_image "$ETCD_IMG"
  preload_image "$MINIO_IMG"
  wp_deploy_operator; wp_build_wp_image
  wp_deploy_deps; wp_create_cr; wp_wait_healthy
  launch_client_pod; write_client_config
}

# The workload is a static binary, so the client pod needs only a shell: it
# runs the woodpecker image, already loaded into the node, instead of the
# golang image wp_launch_client_pod pulls.
launch_client_pod() {
  kubectl get pod "$CLIENT_POD" &>/dev/null && return 0
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${CLIENT_POD}
  labels: { role: wp-client }
spec:
  containers:
    - name: test
      image: ${WP_IMG}
      imagePullPolicy: Never
      command: ["/bin/bash","-c","sleep infinity"]
      resources: { requests: { cpu: "500m", memory: "1Gi" } }
  restartPolicy: Never
EOF
  kubectl wait --for=condition=Ready pod/"$CLIENT_POD" --timeout=300s
}

# The seeds are the pods' full names, the form each pod advertises, so the
# workload can match a quorum's addresses against them.
write_client_config() {
  local seeds="" i
  for i in $(seq 0 $((REPLICAS-1))); do
    seeds="$seeds
            - ${CR_NAME}-server-${i}.${CR_NAME}-server-headless.${NAMESPACE}.svc.cluster.local:18080"
  done
  kubectl exec "$CLIENT_POD" -- bash -c "cat > /tmp/test-config.yaml <<'CFGEOF'
woodpecker:
  meta:
    type: etcd
  client:
    quorum:
      replicaCount: ${ENSEMBLE}
      quorumBufferPools:
        - name: default-region-pool
          seeds:${seeds}
  storage:
    type: service
    rootPath: /tmp/wp-test-data
log:
  level: info
  format: json
  stdout: true
etcd:
  endpoints:
    - etcd.${NAMESPACE}.svc:2379
  rootPath: by-dev
minio:
  address: minio.${NAMESPACE}.svc
  port: 9000
  accessKeyID: minioadmin
  secretAccessKey: minioadmin
  bucketName: woodpecker
  rootPath: files
  createBucket: true
CFGEOF"
}

# Built on the host (module cache, working proxy) and copied in; see tests/chaos_mesh.
push_workload() {
  local bin=/tmp/service_stability.test
  log "building workload test binary on host (GOOS=linux GOARCH=$(go env GOARCH) CGO_ENABLED=0)"
  ( cd "$PROJECT_ROOT" && GOOS=linux GOARCH="$(go env GOARCH)" CGO_ENABLED=0 \
      go test -c -o "$bin" ./tests/k8s/stability/workload ) || fail "host build of workload test binary failed"
  kubectl cp "$bin" "$CLIENT_POD:$WORKLOAD_BIN_IN_POD"
  kubectl exec "$CLIENT_POD" -- chmod +x "$WORKLOAD_BIN_IN_POD"
}

pod_of() { echo "${1%%.*}"; }  # my-woodpecker-server-2.<headless>...:18080 -> my-woodpecker-server-2

# Every server pod exists, is Ready, and none is being deleted.
wait_pods_ready() {
  local deadline=$((SECONDS + 300)) ready
  while [ $SECONDS -lt $deadline ]; do
    ready=$(kubectl get pods -l "$SERVER_SELECTOR" -o jsonpath='{range .items[*]}{.metadata.deletionTimestamp}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
      | grep -c '^|True$' || true)
    [ "$ready" -eq "$REPLICAS" ] && return 0
    sleep 2
  done
  kubectl get pods -o wide
  fail "server pods not all Ready within 300s"
}

# Every pod's memberlist lists every pod as alive.
wait_converged() {
  local deadline=$((SECONDS + 180)) i alive ok
  while [ $SECONDS -lt $deadline ]; do
    ok=true
    for i in $(seq 0 $((REPLICAS-1))); do
      alive=$(kubectl exec "${CR_NAME}-server-$i" -- curl -s -H 'Accept: application/json' http://localhost:9091/admin/memberlist 2>/dev/null \
        | grep -o '"state":0' | wc -l | tr -d ' ' || true)
      [ "${alive:-0}" -eq "$REPLICAS" ] || { ok=false; break; }
    done
    $ok && return 0
    sleep 2
  done
  fail "memberlist did not reconverge within 180s"
}

apply_chaos() {  # $1 = kind, $2 = name, $3 = spec body (indented 2)
  kubectl apply -f - <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: $1
metadata: { name: $2, namespace: ${NAMESPACE} }
spec:
$3
EOF
  kubectl wait --for=condition=AllInjected "$(echo "$1" | tr '[:upper:]' '[:lower:]')/$2" -n "$NAMESPACE" --timeout=60s \
    || { kubectl describe "$1" "$2" -n "$NAMESPACE"; fail "$1 $2 never reached AllInjected"; }
}

delete_chaos() { kubectl delete "$1" "$2" -n "$NAMESPACE" --ignore-not-found --wait=true; }

# The pod stops answering anything, in both directions, for 10s.
partition_pod() {  # $1 = pod
  apply_chaos NetworkChaos stability-partition "  action: partition
  mode: one
  selector: { pods: { ${NAMESPACE}: [$1] } }
  direction: both
  target:
    mode: all
    selector: { namespaces: [${NAMESPACE}] }"
  sleep 10
  delete_chaos networkchaos stability-partition
}

marker() { kubectl exec "$CLIENT_POD" -- "$@"; }

# Waits for the workload of case $1 to name its target; prints it (may be empty).
wait_target() {
  local c="$1" wl="$2" deadline=$((SECONDS + 300))
  while [ $SECONDS -lt $deadline ]; do
    if marker test -f "$STATE_DIR/$c.target" 2>/dev/null; then
      marker cat "$STATE_DIR/$c.target"
      return 0
    fi
    kill -0 "$wl" 2>/dev/null || return 1
    sleep 1
  done
  return 1
}

restart_count() { kubectl get pod "$1" -o jsonpath='{.status.containerStatuses[0].restartCount}'; }

# SIGKILLs the server in pod $1 and waits until its container has restarted
# (the restart count moved), so a later readiness wait does not see the old
# container still marked Ready.
kill_server() {
  local pod="$1" before deadline=$((SECONDS + 120))
  before=$(restart_count "$pod") || return 1
  kubectl exec "$pod" -- pkill -KILL -x woodpecker || return 1
  while [ $SECONDS -lt $deadline ]; do
    [ "$(restart_count "$pod")" -gt "$before" ] && return 0
    sleep 1
  done
  warn "container of $pod did not restart within 120s after SIGKILL"
  return 1
}

collect_artifacts() {  # $1 = case
  local out="$ARTIFACTS/$1" i; mkdir -p "$out"
  for i in $(seq 0 $((REPLICAS-1))); do kubectl logs "${CR_NAME}-server-$i" >"$out/server-$i.log" 2>&1 || true; done
  kubectl get podchaos,networkchaos -A -o yaml >"$out/chaos.yaml" 2>&1 || true
  kubectl get events -A --sort-by=.lastTimestamp >"$out/events.txt" 2>&1 || true
  kubectl get pods -o wide >"$out/pods.txt" 2>&1 || true
  [ -n "${2:-}" ] && kubectl logs "$2" --previous >"$out/$2-previous.log" 2>&1 || true
}

# A fault could not be injected: stop the workload waiting for it and fail the
# case, rather than let an uninjected case pass.
abort_case() {  # $1 = case, $2 = workload pid, $3 = what failed
  kill "$2" 2>/dev/null || true
  marker sh -c "pkill -f service_stability.test" 2>/dev/null || true
  wait "$2" 2>/dev/null || true
  collect_artifacts "$1"
  warn "CASE $1 FAILED: $3"
  return 1
}

run_case() {  # $1 = case name
  local c="$1" out="$ARTIFACTS/$1" target pod wl rc=0 i
  mkdir -p "$out"
  marker sh -c "mkdir -p $STATE_DIR && rm -f $STATE_DIR/$c.*"
  log "=== CASE $c ==="
  kubectl exec "$CLIENT_POD" -- env WP_STABILITY_LATENCY="$LATENCY_MODE" "$WORKLOAD_BIN_IN_POD" \
    -test.v -test.count=1 -test.timeout=20m -test.run "^TestServiceStabilityK8s_${c}\$" \
    -config-file /tmp/test-config.yaml -state-dir "$STATE_DIR" >"$out/workload.log" 2>&1 &
  wl=$!

  if ! target=$(wait_target "$c" "$wl"); then
    wait "$wl" || true
    collect_artifacts "$c"; warn "CASE $c: workload never named a target (see $out/workload.log)"; return 1
  fi
  pod=""; [ -n "$target" ] && pod=$(pod_of "$target")
  log "CASE $c: fault target '${pod:-none}'"

  local lift_after=""
  case "$c" in
    Baseline) sleep 10 ;;
    RestartIdlePod|RestartQuorumPod)
      kubectl delete pod "$pod" --wait=true || { abort_case "$c" "$wl" "kubectl delete pod $pod"; return 1; } ;;
    KillQuorumPod)
      kill_server "$pod" || { abort_case "$c" "$wl" "SIGKILL of $pod"; return 1; } ;;
    KillQuorumPod_NeverReturns)
      # Kill abruptly first; pod-failure then keeps the restarted container
      # from serving. The kubelet may start the server again for a moment
      # before pod-failure takes effect.
      kubectl exec "$pod" -- pkill -KILL -x woodpecker || { abort_case "$c" "$wl" "SIGKILL of $pod"; return 1; }
      apply_chaos PodChaos stability-pod-failure "  action: pod-failure
  mode: one
  selector: { pods: { ${NAMESPACE}: [$pod] } }
  duration: 10m"
      sleep 10
      lift_after=podchaos ;;  # stays down until the workload has finished
    KillQuorumPod_Rescheduled)
      # The server is frozen so it cannot leave, then the pod is force-deleted:
      # the kubelet's SIGTERM waits behind the stop and its SIGKILL follows,
      # and the StatefulSet recreates the pod under the same name with a new
      # IP. Peers see a node that died without leaving come back at another
      # address, which memberlist refuses until the old entry is reaped (#395).
      local old_ip new_ip t0
      old_ip=$(kubectl get pod "$pod" -o jsonpath='{.status.podIP}')
      kubectl exec "$pod" -- pkill -STOP -x woodpecker || { abort_case "$c" "$wl" "SIGSTOP of $pod"; return 1; }
      t0=$SECONDS
      kubectl delete pod "$pod" --grace-period=0 --force --wait=false || { abort_case "$c" "$wl" "force delete of $pod"; return 1; }
      wait_pods_ready; wait_converged
      new_ip=$(kubectl get pod "$pod" -o jsonpath='{.status.podIP}')
      log "CASE $c: $pod $old_ip -> $new_ip, readmitted by every peer $((SECONDS - t0))s after it died"
      echo "readmitted_after_seconds=$((SECONDS - t0)) old_ip=$old_ip new_ip=$new_ip" >"$out/readmission.txt" ;;
    VanishQuorumPod|VanishReadPod_SeparateReader) partition_pod "$pod" ;;
    RollingRestartAllPods)
      for i in $(seq $((REPLICAS-1)) -1 0); do
        kubectl delete pod "${CR_NAME}-server-$i" --wait=true \
          || { abort_case "$c" "$wl" "kubectl delete pod ${CR_NAME}-server-$i"; return 1; }
        wait_pods_ready; wait_converged
        sleep 2
      done ;;
    *) fail "unknown case $c" ;;
  esac
  if [ -z "$lift_after" ]; then wait_pods_ready; wait_converged; fi

  marker touch "$STATE_DIR/$c.done"
  wait "$wl" || rc=$?

  if [ -n "$lift_after" ]; then
    delete_chaos podchaos stability-pod-failure
    wait_pods_ready; wait_converged
  fi

  grep -E '^\s+\[.*\] submitted=|^\s+\[.*\] read=|append stalled|tail read lagged|report only|--- (PASS|FAIL)' "$out/workload.log" || true
  case "$c" in KillQuorumPod*) collect_artifacts "$c" "$pod" ;; esac  # the killed server's last log, as evidence
  if [ $rc -ne 0 ]; then collect_artifacts "$c" "$pod"; warn "CASE $c FAILED"; return 1; fi
  log "CASE $c PASSED"
}

main() {
  rm -rf "$ARTIFACTS"; mkdir -p "$ARTIFACTS"
  bringup; install_chaos_mesh; push_workload
  local rc=0 c results=""
  for c in $CASES; do
    if run_case "$c"; then results="$results\n  PASS $c"; else results="$results\n  FAIL $c"; rc=1; fi
  done
  log "Service Stability (K8s) results (latency budget: $LATENCY_MODE):$results"
  exit $rc
}

trap 'rc=$?; [ -n "${KEEP:-}" ] || { helm uninstall chaos-mesh -n chaos-mesh 2>/dev/null || true; minikube delete -p "$CLUSTER_NAME" 2>/dev/null || true; }; exit $rc' EXIT
main "$@"
