#!/bin/bash
# Chaos Mesh helpers shared by the k8s suites (tests/chaos_mesh, tests/k8s/stability).
# Source after deployments/operator/test/lib.sh; uses CLUSTER_NAME, log, fail.

# The minikube node can't reach external registries when the host uses a loopback proxy
# (HTTP_PROXY=127.0.0.1:xxxx — minikube logs "Local proxy ignored"). The HOST can pull (its
# proxy works), so for images we don't build locally we pull on the host and load into the node.
preload_image() {  # $1 = image ref
  log "preload: $1"
  docker pull "$1" || fail "host 'docker pull $1' failed (proxy/network?)"
  minikube -p "$CLUSTER_NAME" image load "$1" || fail "'minikube image load $1' failed"
}

install_chaos_mesh() {
  # chaos-daemon must point at the SAME container runtime the node actually runs: it bypasses the
  # Kubernetes API and resolves container IDs through that socket to enter a pod's netns. Point it
  # at the wrong one and injection fails ("unable to flush ip sets for pod ...") or is a silent
  # no-op. Ask the node instead of deriving it from MK_RUNTIME — MK_RUNTIME is empty by default,
  # which means "whatever minikube defaults to", and that default flipped from docker to containerd
  # in minikube v1.39.0 and left this suite red on every nightly (#304).
  local runtime_ver daemon_runtime daemon_socket
  runtime_ver=$(kubectl get node "$CLUSTER_NAME" -o jsonpath='{.status.nodeInfo.containerRuntimeVersion}')
  case "$runtime_ver" in
    containerd://*) daemon_runtime=containerd; daemon_socket=/run/containerd/containerd.sock ;;
    docker://*)     daemon_runtime=docker;     daemon_socket=/var/run/docker.sock ;;
    # Guessing a socket here buys nothing: a wrong one fails 5 minutes later with a message that
    # points at chaos, not at the runtime. Say what we actually saw.
    *) fail "unrecognized node container runtime '$runtime_ver' — cannot configure chaos-daemon" ;;
  esac
  log "installing chaos-mesh (node runtime=$runtime_ver -> chaosDaemon.runtime=$daemon_runtime socket=$daemon_socket)"
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
  # upgrade --install is idempotent so re-running against an existing cluster doesn't error.
  helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace \
    --set chaosDaemon.runtime="$daemon_runtime" \
    --set chaosDaemon.socketPath="$daemon_socket" \
    --set dashboard.create=false \
    --wait --timeout 5m
  kubectl -n chaos-mesh rollout status daemonset/chaos-daemon --timeout=180s
}
