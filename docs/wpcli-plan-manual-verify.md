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

- [ ] Expected: `bin/woodpecker` and `bin/wp` both exist
- [ ] `wp version` shows git commit, e.g. `wp version v0.1.25-64-g8e96810 (...)`

```bash
# Build Docker image (required for docker-compose)
./build/build_image.sh
```

- [ ] Expected: `woodpecker:latest` image built successfully

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

- [ ] Expected: cluster topology tree showing 4 nodes (woodpecker-node1 through node4)

---

## Phase 1: Foundation Commands

### A. Node Lifecycle

#### A.1 node list

```bash
./bin/wp node list
```

- [ ] Expected: table with columns NODE_ID, ADDRESS, STATE, AZ, RG showing 4 nodes
- [ ] All nodes show state "active" (or numeric state 0)

```bash
./bin/wp node list -o json
```

- [ ] Expected: JSON array with 4 node objects

#### A.2 node show

```bash
./bin/wp node show woodpecker-node1
```

- [ ] Expected: sectioned output with Identity, Status, Network sections
- [ ] Shows node_id, state, member_count, address, started_at, version

#### A.3 node decommission + cancel

```bash
# Start decommission (will block by default — use --async to return immediately)
./bin/wp node decommission woodpecker-node4 --async -y
```

- [ ] Expected: "decommission initiated" message

```bash
# Check progress
./bin/wp node drain-status woodpecker-node4
```

- [ ] Expected: progress output with safe_to_terminate field

```bash
# Cancel decommission
./bin/wp node cancel-decommission woodpecker-node4
```

- [ ] Expected: success message

#### A.4 node restart (stub)

```bash
./bin/wp node restart woodpecker-node1; echo "exit=$?"
```

- [ ] Expected: "not implemented" message
- [ ] Exit code: 10

### B. Cluster Overview

#### B.1 cluster info

```bash
./bin/wp cluster info
```

- [ ] Expected: cluster summary with member count + ASCII topology tree

#### B.2 cluster health

```bash
./bin/wp cluster health
```

- [ ] Expected: GREEN/YELLOW/RED health status

```bash
./bin/wp cluster health; echo "exit=$?"
```

- [ ] Exit code 0 if green, 8 if yellow, 9 if red

#### B.3 cluster gossip-diff

```bash
./bin/wp cluster gossip-diff
```

- [ ] Expected: "identical" if no split-brain, or drift indicators

### C. Context Management

#### C.1 ctx list / use / view

```bash
./bin/wp ctx list
```

- [ ] Expected: table showing "local" context with `*` marker for current

```bash
./bin/wp ctx view
```

- [ ] Expected: resolved context details (endpoint, admin_port, timeout, etc.)

### D. Config & Env

#### D.1 config show

```bash
./bin/wp config show woodpecker-node1
```

- [ ] Expected: YAML-formatted server configuration (etcd, minio, logstore sections)

#### D.2 config diff

```bash
./bin/wp config diff --all
```

- [ ] Expected: "identical" for all nodes, or drift indication

#### D.3 env show

```bash
./bin/wp env show woodpecker-node1 --section build
```

- [ ] Expected: build info section with version, commit, go_version

```bash
./bin/wp env show woodpecker-node1 --section runtime
```

- [ ] Expected: Go runtime info (GOMAXPROCS, num_goroutine, etc.)

#### D.4 env diff

```bash
./bin/wp env diff
```

- [ ] Expected: comparison across nodes

### E. Profile

```bash
./bin/wp profile woodpecker-node1 --type heap --output-file /tmp/wp-heap.pb.gz
ls -la /tmp/wp-heap.pb.gz
```

- [ ] Expected: file downloaded, size > 0

```bash
./bin/wp profile woodpecker-node1 --type goroutine
```

- [ ] Expected: goroutine dump printed to stdout

---

## Phase 2: Observability & Op Registry

### F. Dynamic Logging

#### F.1 logging get-level

```bash
./bin/wp logging get-level --all
```

- [ ] Expected: table with NODE and LEVEL columns, all showing "info" (default)

```bash
./bin/wp logging get-level woodpecker-node1
```

- [ ] Expected: single row showing node-1's log level

#### F.2 logging set-level

```bash
./bin/wp logging set-level woodpecker-node1 --level debug
./bin/wp logging get-level woodpecker-node1
```

- [ ] Expected: level changed to "debug"

```bash
# Restore
./bin/wp logging set-level woodpecker-node1 --level info
./bin/wp logging get-level woodpecker-node1
```

- [ ] Expected: level back to "info"

### G. Logstore Inspection

> **Note:** These commands require active segment writers. Run a write workload
> or use the test framework to create segments before testing.

#### G.1 logstore segments

```bash
./bin/wp logstore segments woodpecker-node1
```

- [ ] Expected: table with LOG_ID, SEG_ID, BACKEND, WRITABLE, etc. (may be empty if no writes)

```bash
./bin/wp logstore segments woodpecker-node1 -o json
```

- [ ] Expected: JSON with `{"segments": [...]}` structure

#### G.2 logstore buffer

```bash
./bin/wp logstore buffer woodpecker-node1
```

- [ ] Expected: buffer summary sorted by size (may be empty)

#### G.3 logstore flush-queue

```bash
./bin/wp logstore flush-queue woodpecker-node1
```

- [ ] Expected: flush queue depth summary (may be empty)

#### G.4 logstore segment-show (requires active segment)

```bash
# First find a segment:
./bin/wp logstore segments woodpecker-node1 -o json
# Then show details:
./bin/wp logstore segment-show woodpecker-node1 --log <LOG_ID> --seg <SEG_ID>
```

- [ ] Expected: detailed JSON with buffer_bytes, flush_queue_depth, etc.

#### G.5 logstore force-flush

```bash
./bin/wp logstore force-flush woodpecker-node1; echo "exit=$?"
```

- [ ] Expected: error message "force-flush is not yet supported" (known limitation)
- [ ] Exit code: 4 (state conflict)

#### G.6 logstore fence (requires active segment, destructive)

```bash
# Only test if you have an expendable segment:
./bin/wp logstore fence woodpecker-node1 --log <ID> --seg <ID> --reason "manual test" -y
```

- [ ] Expected: "fence completed" if segment exists, error if not

#### G.7 logstore compact (requires finalized segment)

```bash
./bin/wp logstore compact woodpecker-node1 --log <ID> --seg <ID>
```

- [ ] Expected: "compact completed" or error if segment not in correct state

### H. Ops Registry

#### H.1 ops stats

```bash
./bin/wp ops stats woodpecker-node1
```

- [ ] Expected: Capacity section (capacity=1024, in_use, warn_age=30000ms) + Eviction Totals

#### H.2 ops list

```bash
./bin/wp ops list woodpecker-node1
```

- [ ] Expected: table with OP_TYPE, OP_ID, TRACE_ID, LOG:SEG (may be empty at idle)

**Under load test:**

```bash
# Run a write workload in another terminal, then:
./bin/wp ops list woodpecker-node1 --type logstore.add_entry
```

- [ ] Expected: filtered list showing only add_entry ops

```bash
./bin/wp ops list woodpecker-node1 --longer-than 1000
```

- [ ] Expected: only ops running > 1 second

#### H.3 ops show

```bash
# First get an op_id from ops list, then:
./bin/wp ops list woodpecker-node1 -o json
# Pick an op_id:
./bin/wp ops show woodpecker-node1 --op-id <OP_ID>
```

- [ ] Expected: sectioned detail view (Identity, Context, Trace)

#### H.4 ops show — not found

```bash
./bin/wp ops show woodpecker-node1 --op-id nonexistent-12345; echo "exit=$?"
```

- [ ] Expected: "not found" error, exit code 11

### I. Metrics Analysis

#### I.1 metrics list

```bash
./bin/wp metrics list woodpecker-node1
```

- [ ] Expected: table with NAME, TYPE, SERIES, HELP — dozens of woodpecker_* metrics

```bash
./bin/wp metrics list woodpecker-node1 --filter logstore
```

- [ ] Expected: only metrics containing "logstore" in the name

#### I.2 metrics snapshot

```bash
./bin/wp metrics snapshot woodpecker-node1 --metric woodpecker_server_logstore_active_logs
```

- [ ] Expected: table with NODE, METRIC, LABELS, VALUE

```bash
./bin/wp metrics snapshot --all --metric woodpecker_server_logstore_active_logs
```

- [ ] Expected: values from all 4 nodes

#### I.3 metrics top

```bash
./bin/wp metrics top --by woodpecker_server_logstore_instances_total
```

- [ ] Expected: ranked list of nodes by metric value

#### I.4 metrics watch (interactive)

```bash
# Press Ctrl+C after a few lines:
./bin/wp metrics watch woodpecker_server_logstore_instances_total woodpecker-node1 --interval 2s
```

- [ ] Expected: real-time stream with TIMESTAMP, VALUE, DELTA, TREND columns
- [ ] Trend arrows: `^` (up), `v` (down), `=` (stable)
- [ ] Ctrl+C exits cleanly

#### I.5 metrics report — list scenarios

```bash
./bin/wp metrics report --list
```

- [ ] Expected: table with 12 scenarios (stuck-flush, hot-write, slow-write, etc.)

#### I.6 metrics report — run scenario

```bash
./bin/wp metrics report woodpecker-node1 --scenario stuck-flush --window 10s
```

- [ ] Expected: collects samples, evaluates rules, shows findings or "OK"
- [ ] Exit code 0 (OK), 8 (yellow), or 9 (red) based on findings

```bash
./bin/wp metrics report woodpecker-node1 --scenario hot-write --window 10s
```

- [ ] Expected: runs and reports (likely "OK" for single-node test)

---

## Phase 3: K8s & Release

### J. K8s Commands (Print Mode — no K8s cluster needed)

#### J.1 k8s status (print mode)

```bash
./bin/wp k8s status --wp-cluster wp-test -n woodpecker
```

- [ ] Expected: 3 kubectl commands printed (get, describe, get pods)
- [ ] Contains "add -x to execute" hint

#### J.2 k8s scale (print mode)

```bash
./bin/wp k8s scale --replicas 5 --wp-cluster wp-test -n woodpecker
```

- [ ] Expected: kubectl patch command printed with `{"spec":{"replicas":5}}`

#### J.3 k8s logs (print mode)

```bash
./bin/wp k8s logs 0 --wp-cluster wp-test -n woodpecker
```

- [ ] Expected: kubectl logs command with pod name `wp-test-server-0`

```bash
./bin/wp k8s logs my-custom-pod --wp-cluster wp-test -f --tail 50 --since 1h
```

- [ ] Expected: includes `-f`, `--tail 50`, `--since 1h` flags

#### J.4 k8s doctor (stub)

```bash
./bin/wp k8s doctor; echo "exit=$?"
```

- [ ] Expected: "not implemented" message, exit code 10

#### J.5 k8s execute mode — kubectl not found

```bash
./bin/wp k8s status --wp-cluster wp-test --kubectl /nonexistent/kubectl -x
```

- [ ] Expected: "kubectl not found, falling back to print mode" + commands printed

### K. Release Build

```bash
make wpcli-release
ls -la bin/wp-*
```

- [ ] Expected: 4 binaries created:
  - `bin/wp-linux-amd64`
  - `bin/wp-linux-arm64`
  - `bin/wp-darwin-arm64`
  - `bin/wp-windows-amd64.exe`

### L. Global Flags

#### L.1 Output formats

```bash
./bin/wp node list -o json
./bin/wp node list -o yaml
./bin/wp node list -o table
```

- [ ] Expected: same data in 3 different formats

#### L.2 Timeout

```bash
./bin/wp --timeout 1ms cluster info; echo "exit=$?"
```

- [ ] Expected: timeout error, exit code 1

#### L.3 Verbose

```bash
./bin/wp -vvv cluster info
```

- [ ] Expected: same output (verbose logging not yet implemented, flag accepted silently)

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
# Stop the cluster
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
| Phase 3 — K8s & Release | 10 | | | |
| Exit Codes | 8 | | | |
| Known Limitations | 5 | | | |
| **Total** | **79** | | | |

**Tester:** _______________
**Date:** _______________
**Build version:** _______________
**Verdict:** PASS / FAIL / PASS WITH NOTES
