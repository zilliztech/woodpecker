# wp CLI Manual Verification Guide

> **Purpose:** Step-by-step verification of all `wp` CLI features on a real
> docker-compose cluster. Each section has expected output patterns and a
> pass/fail checkbox.
>
> **Estimated time:** 30-40 minutes.

---

## Prerequisites

### 1. Build the server binary and Docker image

```bash
# Build server binary
make build

# Build wp CLI binary
make wpcli
./bin/wp version
```

- [X] Expected: `bin/woodpecker` and `bin/wp` both exist
- [X] `wp version` shows git commit, e.g. `wp version v0.1.25-64-g8e96810 (...)`

```bash
# Build Docker image (required for docker-compose)
./build/build_image.sh
```

- [X] Expected: `woodpecker:latest` image built successfully

> **Tip:** If you want a specific OS base, use `./build/build_image.sh ubuntu22.04`.
> To verify: `docker images | grep woodpecker`

### 2. Start the cluster

```bash
cd deployments
docker compose up -d
cd ..
```

Wait ~15 seconds for all nodes to join gossip.

> **Troubleshooting:** If nodes fail to start, check:
>
> - `docker compose logs woodpecker-node1` — look for etcd/minio connection errors
> - Ensure ports 9091-9094, 18080-18083 are not occupied
> - Run `docker compose ps` to verify all 4 nodes are "healthy"

### 3. Configure CLI

```bash
mkdir -p ~/.woodpecker
cat > ~/.woodpecker/cli.yaml <<'EOF'
current-context: local

contexts:
  local:
    endpoint: http://localhost:9091
    admin_port: 9091
EOF
```

### 4. Verify connectivity

```bash
./bin/wp cluster info
```

- [X] Expected: cluster topology tree showing 4 nodes (woodpecker-node1 through node4)

---

## Phase 1: Foundation Commands

### A. Node Lifecycle

#### A.1 node list

```bash
./bin/wp node list
```

- [X] Expected: table with columns NODE_ID, ADDRESS, STATE, AZ, RG showing 4 nodes
- [X] All nodes show state "active" (or numeric state 0)

```bash
./bin/wp node list -o json
```

- [X] Expected: JSON array with 4 node objects

#### A.2 node show

```bash
./bin/wp node show woodpecker-node1
```

- [X] Expected: sectioned output with Identity, Status, Network sections
- [X] Shows node_id, state, member_count, address, started_at, version

#### A.3 node decommission + cancel

```bash
# Start decommission (will block by default — use --async to return immediately)
./bin/wp node decommission woodpecker-node4 --async -y
```

- [X] Expected: "decommission initiated" message

```bash
# Check progress
./bin/wp node drain-status woodpecker-node4
```

- [X] Expected: progress output with safe_to_terminate field

```bash
# Cancel decommission
./bin/wp node cancel-decommission woodpecker-node4
```

- [X] Expected: success message

#### A.4 node restart (stub)

```bash
./bin/wp node restart woodpecker-node1; echo "exit=$?"
```

- [X] Expected: "not implemented" message
- [X] Exit code: 10

### B. Cluster Overview

#### B.1 cluster info

```bash
./bin/wp cluster info
```

- [X] Expected: cluster summary with member count + ASCII topology tree

#### B.2 cluster health

```bash
./bin/wp cluster health
```

- [X] Expected: GREEN/YELLOW/RED health status

```bash
./bin/wp cluster health; echo "exit=$?"
```

- [X] Exit code 0 if green, 8 if yellow, 9 if red

#### B.3 cluster gossip-diff

```bash
./bin/wp cluster gossip-diff
```

- [X] Expected: "identical" if no split-brain, or drift indicators

### C. Context Management

#### C.1 ctx list / use / view

```bash
./bin/wp ctx list
```

- [X] Expected: table showing "local" context with `*` marker for current

```bash
./bin/wp ctx view
```

- [X] Expected: resolved context details (endpoint, admin_port, timeout, etc.)

### D. Config & Env

#### D.1 config show

```bash
./bin/wp config show woodpecker-node1
```

- [X] Expected: YAML-formatted server configuration (etcd, minio, logstore sections)

#### D.2 config diff

```bash
./bin/wp config diff --all
```

- [X] Expected: "identical" for all nodes, or drift indication

#### D.3 env show

```bash
./bin/wp env show woodpecker-node1 --section build
```

- [X] Expected: build info section with version, commit, go_version

```bash
./bin/wp env show woodpecker-node1 --section runtime
```

- [X] Expected: Go runtime info (GOMAXPROCS, num_goroutine, etc.)

#### D.4 env diff

```bash
./bin/wp env diff
```

- [X] Expected: comparison across nodes

### E. Profile

```bash
./bin/wp profile woodpecker-node1 --type heap --output-file /tmp/wp-heap.pb.gz
ls -la /tmp/wp-heap.pb.gz
```

- [X] Expected: file downloaded, size > 0

```bash
./bin/wp profile woodpecker-node1 --type goroutine
```

- [X] Expected: goroutine dump printed to stdout

---

## Phase 2: Observability & Op Registry

### F. Dynamic Logging

#### F.1 logging get-level

```bash
./bin/wp logging get-level --all
```

- [X] Expected: table with NODE and LEVEL columns, all showing "info" (default)

```bash
./bin/wp logging get-level woodpecker-node1
```

- [X] Expected: single row showing node-1's log level

#### F.2 logging set-level

```bash
./bin/wp logging set-level woodpecker-node1 --level debug
./bin/wp logging get-level woodpecker-node1
```

- [X] Expected: level changed to "debug"

```bash
# Restore
./bin/wp logging set-level woodpecker-node1 --level info
./bin/wp logging get-level woodpecker-node1
```

- [X] Expected: level back to "info"

### G. Logstore Inspection

> **Note:** These commands require active segment writers. Run a write workload
> or use the test framework to create segments before testing.

#### G.1 logstore segments

```bash
./bin/wp logstore segments woodpecker-node1
```

- [X] Expected: table with LOG_ID, SEG_ID, BACKEND, WRITABLE, etc. (may be empty if no writes)

```bash
./bin/wp logstore segments woodpecker-node1 -o json
```

- [X] Expected: JSON with `{"segments": [...]}` structure

#### G.2 logstore buffer

```bash
./bin/wp logstore buffer woodpecker-node1
```

- [X] Expected: buffer summary sorted by size (may be empty)

#### G.3 logstore flush-queue

```bash
./bin/wp logstore flush-queue woodpecker-node1
```

- [X] Expected: flush queue depth summary (may be empty)

#### G.4 logstore segment-show (requires active segment)

```bash
# First find a segment:
./bin/wp logstore segments woodpecker-node1 -o json
# Then show details:
./bin/wp logstore segment-show woodpecker-node1 --log <LOG_ID> --seg <SEG_ID>
```

- [X] Expected: detailed JSON with buffer_bytes, flush_queue_depth, etc.

#### G.5 logstore force-flush

```bash
./bin/wp logstore force-flush woodpecker-node1; echo "exit=$?"
```

- [X] Expected: error message "force-flush is not yet supported" (known limitation)
- [X] Exit code: 4 (state conflict)

#### G.6 logstore fence (requires active segment, destructive)

```bash
# Only test if you have an expendable segment:
./bin/wp logstore fence woodpecker-node1 --log <ID> --seg <ID> --reason "manual test" -y
```

- [X] Expected: "fence completed" if segment exists, error if not

#### G.7 logstore compact (requires finalized segment)

```bash
./bin/wp logstore compact woodpecker-node1 --log <ID> --seg <ID>
```

- [X] Expected: "compact completed" or error if segment not in correct state

### H. Ops Registry

#### H.1 ops stats

```bash
./bin/wp ops stats woodpecker-node1
```

- [X] Expected: Capacity section (capacity=1024, in_use, warn_age=30000ms) + Eviction Totals

#### H.2 ops list

```bash
./bin/wp ops list woodpecker-node1
```

- [X] Expected: table with OP_TYPE, OP_ID, TRACE_ID, LOG:SEG (may be empty at idle)

**Under load test:**

```bash
# Run a write workload in another terminal, then:
./bin/wp ops list woodpecker-node1 --type logstore.add_entry
```

- [X] Expected: filtered list showing only add_entry ops

```bash
./bin/wp ops list woodpecker-node1 --longer-than 1000
```

- [X] Expected: only ops running > 1 second

#### H.3 ops show

```bash
# First get an op_id from ops list, then:
./bin/wp ops list woodpecker-node1 -o json
# Pick an op_id:
./bin/wp ops show woodpecker-node1 --op-id <OP_ID>
```

- [X] Expected: sectioned detail view (Identity, Context, Trace)

#### H.4 ops show — not found

```bash
./bin/wp ops show woodpecker-node1 --op-id nonexistent-12345; echo "exit=$?"
```

- [X] Expected: "not found" error, exit code 11

### I. Metrics Analysis

#### I.1 metrics list

```bash
./bin/wp metrics list woodpecker-node1
```

- [X] Expected: table with NAME, TYPE, SERIES, HELP — dozens of woodpecker_* metrics

```bash
./bin/wp metrics list woodpecker-node1 --filter logstore
```

- [X] Expected: only metrics containing "logstore" in the name

#### I.2 metrics snapshot

```bash
./bin/wp metrics snapshot woodpecker-node1 --metric woodpecker_server_logstore_active_logs
```

- [X] Expected: table with NODE, METRIC, LABELS, VALUE

```bash
./bin/wp metrics snapshot --all --metric woodpecker_server_logstore_active_logs
```

- [X] Expected: values from all 4 nodes

#### I.3 metrics top

```bash
./bin/wp metrics top --by woodpecker_server_logstore_instances_total
```

- [X] Expected: ranked list of nodes by metric value

#### I.4 metrics watch (interactive)

```bash
# Press Ctrl+C after a few lines:
./bin/wp metrics watch woodpecker_server_logstore_instances_total woodpecker-node1 --interval 2s
```

- [X] Expected: real-time stream with TIMESTAMP, VALUE, DELTA, TREND columns
- [X] Trend arrows: `^` (up), `v` (down), `=` (stable)
- [X] Ctrl+C exits cleanly

#### I.5 metrics report — list scenarios

```bash
./bin/wp metrics report --list
```

- [X] Expected: table with 12 scenarios (stuck-flush, hot-write, slow-write, etc.)

#### I.6 metrics report — run scenario

```bash
./bin/wp metrics report woodpecker-node1 --scenario stuck-flush --window 10s
```

- [X] Expected: collects samples, evaluates rules, shows findings or "OK"
- [X] Exit code 0 (OK), 8 (yellow), or 9 (red) based on findings

```bash
./bin/wp metrics report woodpecker-node1 --scenario hot-write --window 10s
```

- [X] Expected: runs and reports (likely "OK" for single-node test)

---

## Phase 3: K8s Integration & Release

> **Goal:** Verify all `wp k8s` commands work against a real Kubernetes cluster
> with the Woodpecker operator. We first test print mode (no cluster needed),
> then set up minikube and test execute mode end-to-end.

### J. Print Mode (no K8s cluster needed)

> These tests run without any K8s cluster. They verify the CLI correctly
> constructs kubectl commands and handles edge cases.

#### J.1 k8s status — print mode

```bash
./bin/wp k8s status --wp-cluster wp-test -n woodpecker
```

- [ ] Expected: 3 kubectl commands printed (get, describe, get pods)
- [ ] Contains `app.kubernetes.io/instance=wp-test` label selector
- [ ] Contains "add -x to execute" hint

#### J.2 k8s scale — print mode

```bash
./bin/wp k8s scale --replicas 5 --wp-cluster wp-test -n woodpecker
```

- [ ] Expected: kubectl patch command printed with `{"spec":{"replicas":5}}`

#### J.3 k8s logs — print mode + pod name resolution

```bash
# Ordinal → pod name resolution
./bin/wp k8s logs 0 --wp-cluster wp-test -n woodpecker
```

- [ ] Expected: `kubectl ... logs wp-test-server-0`

```bash
# Full flag passthrough
./bin/wp k8s logs wp-test-server-2 --wp-cluster wp-test -f --tail 50 --since 1h
```

- [ ] Expected: includes `-f`, `--tail 50`, `--since 1h` flags

#### J.4 k8s doctor — stub

```bash
./bin/wp k8s doctor; echo "exit=$?"
```

- [ ] Expected: "not implemented" message, exit code 10

#### J.5 kubectl not found fallback

```bash
./bin/wp k8s status --wp-cluster wp-test --kubectl /nonexistent/kubectl -x
```

- [ ] Expected: "kubectl not found, falling back to print mode" + commands printed

### K. Minikube Setup + Operator Deploy

#### K.1 Start minikube

```bash
minikube start --cpus=4 --memory=4096
```

- [ ] Expected: minikube cluster running

#### K.2 Deploy dependencies (etcd + MinIO)

```bash
# etcd (single-node for testing)
kubectl apply -f - <<'EOF'
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: etcd
spec:
  serviceName: etcd
  replicas: 1
  selector:
    matchLabels: { app: etcd }
  template:
    metadata:
      labels: { app: etcd }
    spec:
      containers:
      - name: etcd
        image: quay.io/coreos/etcd:v3.5.25
        command: ["etcd"]
        args:
        - --advertise-client-urls=http://etcd.default.svc:2379
        - --listen-client-urls=http://0.0.0.0:2379
        ports:
        - containerPort: 2379
---
apiVersion: v1
kind: Service
metadata:
  name: etcd
spec:
  ports:
  - port: 2379
  selector: { app: etcd }
EOF

# MinIO
kubectl apply -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
spec:
  replicas: 1
  selector:
    matchLabels: { app: minio }
  template:
    metadata:
      labels: { app: minio }
    spec:
      containers:
      - name: minio
        image: minio/minio:RELEASE.2024-12-18T13-15-44Z
        args: ["server", "/data"]
        env:
        - { name: MINIO_ROOT_USER, value: minioadmin }
        - { name: MINIO_ROOT_PASSWORD, value: minioadmin }
        ports:
        - containerPort: 9000
---
apiVersion: v1
kind: Service
metadata:
  name: minio
spec:
  ports:
  - port: 9000
  selector: { app: minio }
EOF
```

```bash
kubectl wait --for=condition=ready pod -l app=etcd --timeout=60s
kubectl wait --for=condition=ready pod -l app=minio --timeout=60s
```

- [ ] Expected: etcd and minio pods running

#### K.3 Install CRD + Operator

```bash
cd deployments/operator
make install    # Install CRD
make run &      # Run operator locally (or deploy to cluster)
cd ../..
```

- [ ] Expected: CRD `woodpeckerclusters.woodpecker.zilliz.io` registered
- [ ] Operator running (logs show "Starting workers")

```bash
kubectl get crd woodpeckerclusters.woodpecker.zilliz.io
```

- [ ] Expected: CRD listed

#### K.4 Create WoodpeckerCluster

```bash
kubectl apply -f deployments/operator/config/samples/woodpecker_v1alpha1_woodpeckercluster.yaml
```

```bash
# Wait for pods to be ready
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=woodpecker-sample --timeout=120s
```

- [ ] Expected: 3 pods running (`woodpecker-sample-server-{0,1,2}`)

```bash
kubectl get woodpeckerclusters
```

- [ ] Expected: shows `woodpecker-sample` with Ready=3, Replicas=3

```bash
kubectl get pods -l app.kubernetes.io/instance=woodpecker-sample -o wide
```

- [ ] Expected: 3 pods with STATUS=Running

### L. K8s Execute Mode — Real Cluster

> Now we configure the CLI to talk to the K8s-deployed Woodpecker cluster
> and test all k8s commands in execute mode.

#### L.1 Configure CLI for K8s cluster

```bash
cat > ~/.woodpecker/cli.yaml <<'EOF'
current-context: minikube

contexts:
  minikube:
    endpoint: http://localhost:9091
    k8s:
      namespace: default
      cluster: woodpecker-sample
EOF
```

#### L.2 k8s status — execute mode

```bash
./bin/wp k8s status -x
```

- [ ] Expected: runs 3 kubectl commands, shows real output:
  - `kubectl get woodpeckercluster woodpecker-sample -o wide` → shows phase, replicas
  - `kubectl describe woodpeckercluster woodpecker-sample` → full CR details
  - `kubectl get pods -l app.kubernetes.io/instance=woodpecker-sample -o wide` → 3 pods

#### L.3 k8s logs — execute mode

```bash
# Tail last 20 lines from pod 0
./bin/wp k8s logs 0 --tail 20 -x
```

- [ ] Expected: shows last 20 log lines from `woodpecker-sample-server-0`

```bash
# By explicit pod name
./bin/wp k8s logs woodpecker-sample-server-1 --tail 10 -x
```

- [ ] Expected: shows logs from pod 1

```bash
# Streaming mode (Ctrl+C after a few lines)
./bin/wp k8s logs 0 -f -x
```

- [ ] Expected: live log stream, Ctrl+C exits cleanly

#### L.4 k8s scale — execute mode

```bash
# Scale down to 2 replicas
./bin/wp k8s scale --replicas 2 -x
```

- [ ] Expected: kubectl patch executed, returns success

```bash
# Verify
kubectl get pods -l app.kubernetes.io/instance=woodpecker-sample
```

- [ ] Expected: 2 pods running (1 terminating or gone)

```bash
# Scale back to 3
./bin/wp k8s scale --replicas 3 -x
```

- [ ] Expected: kubectl patch success

```bash
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=woodpecker-sample --timeout=60s
kubectl get pods -l app.kubernetes.io/instance=woodpecker-sample
```

- [ ] Expected: 3 pods running again

#### L.5 k8s status — verify after scale

```bash
./bin/wp k8s status -x
```

- [ ] Expected: `get woodpeckercluster` shows Replicas=3, Ready=3

#### L.6 k8s commands with explicit flags (override cli.yaml)

```bash
# Override namespace and cluster via flags
./bin/wp k8s status --wp-cluster woodpecker-sample -n default -x

# Override kubectl path
./bin/wp k8s status --kubectl $(which kubectl) -x
```

- [ ] Expected: same results as L.2

#### L.7 wp admin commands via port-forward (optional)

> If the cluster admin port is not directly reachable, use port-forward:

```bash
# Port-forward to first pod's admin port
kubectl port-forward woodpecker-sample-server-0 9091:9091 &
PF_PID=$!

# Now wp admin commands work
./bin/wp --endpoint http://localhost:9091 cluster info
./bin/wp --endpoint http://localhost:9091 node list
./bin/wp --endpoint http://localhost:9091 logging get-level --all
./bin/wp --endpoint http://localhost:9091 ops stats woodpecker-sample-server-0

kill $PF_PID
```

- [ ] Expected: all admin commands work through port-forward
- [ ] cluster info shows the K8s-deployed nodes

### M. Release Build

```bash
make wpcli-release
ls -la bin/wp-*
```

- [ ] Expected: 4 binaries created:
  - `bin/wp-linux-amd64`
  - `bin/wp-linux-arm64`
  - `bin/wp-darwin-arm64`
  - `bin/wp-windows-amd64.exe`

### N. Global Flags

#### N.1 Output formats

```bash
./bin/wp node list -o json
./bin/wp node list -o yaml
./bin/wp node list -o table
```

- [ ] Expected: same data in 3 different formats

#### N.2 Timeout

```bash
./bin/wp --timeout 1ms cluster info; echo "exit=$?"
```

- [ ] Expected: timeout error, exit code 1

---

## Exit Code Summary Verification

| Command | Expected Exit Code | Meaning |
|---------|-------------------|---------|
| `wp cluster health` (healthy) | 0 | Success |
| `wp cluster health` (degraded) | 8 or 9 | Yellow/Red finding |
| `wp node restart <node>` | 10 | Not implemented |
| `wp k8s doctor` | 10 | Not implemented |
| `wp ops show <node> --op-id nonexistent` | 11 | Resource not found |
| `wp logging set-level <node>` (no --level) | 2 | Usage error |
| `wp logstore force-flush <node>` | 4 | State conflict (not yet supported) |
| `wp metrics report --scenario stuck-flush` (finding) | 9 | Red finding |

---

## Known Limitations

Check these are documented and behave as expected:

- [ ] `wp logstore force-flush` — returns error "not yet supported"
- [ ] `wp metrics report` — only `stuck-flush` scenario with evicted_old detection and latency-based rules fire reliably. Other scenarios have definitions but simplified evaluation.
- [ ] `wp metrics watch` — runs indefinitely until Ctrl+C (no --duration flag)
- [ ] `wp k8s scale -x` — no confirmation prompt in execute mode
- [ ] `tests/docker/wpcli/` E2E suite — not yet created

---

## Post-Verification Cleanup

```bash
# Delete WoodpeckerCluster
kubectl delete -f deployments/operator/config/samples/woodpecker_v1alpha1_woodpeckercluster.yaml

# Stop operator (if running locally)
# Ctrl+C the 'make run' process

# Delete dependencies
kubectl delete deployment minio
kubectl delete service minio
kubectl delete statefulset etcd
kubectl delete service etcd

# Stop minikube
minikube stop
minikube delete

# Stop docker-compose cluster (if still running)
cd deployments
docker compose down -v
cd ..

# Clean up config
rm ~/.woodpecker/cli.yaml

# Clean up downloaded profiles
rm -f /tmp/wp-heap.pb.gz
```

---

## Results Summary

| Category | Total Checks | Passed | Failed | Notes |
|----------|-------------|--------|--------|-------|
| Prerequisites | 4 | | | |
| Phase 1 — Foundation | 25 | | | |
| Phase 2 — Observability | 27 | | | |
| Phase 3 J — Print Mode | 7 | | | |
| Phase 3 K — Minikube Setup | 7 | | | |
| Phase 3 L — K8s Execute Mode | 13 | | | |
| Phase 3 M/N — Release & Flags | 3 | | | |
| Exit Codes | 8 | | | |
| Known Limitations | 5 | | | |
| **Total** | **94** | | | |

**Tester:** _______________
**Date:** _______________
**Build version:** _______________
**Verdict:** PASS / FAIL / PASS WITH NOTES
