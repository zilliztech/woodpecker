# Chaos Mesh Network-Jitter Nightly Test — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a nightly-only Chaos Mesh suite that injects network jitter (delay+jitter / loss / partition-blip) on Woodpecker's server↔MinIO, server↔etcd, and client↔server links and asserts that the system only degrades gracefully (transient errors recover) — never loses acked data, hangs, corrupts, crashes, or storms retries.

**Architecture:** Reuse the operator's minikube bring-up (`deployments/operator/test/smoke-test.sh`, refactored into a sourceable `lib.sh`), install Chaos Mesh, then a host-driven orchestrator (`tests/chaos_mesh/run_network_chaos.sh`) runs each scenario through phases `warmup → inject → under-chaos → remove → recovery → verify`. An in-pod Go program (`tests/chaos_mesh/workload/`) drives concurrent writers/readers, persists every acked `(logId, segmentId, entryId, payloadHash)` to a JSONL file, and asserts invariants I1–I7. The whole thing runs nightly only, never gating PRs.

**Tech Stack:** Bash, minikube (docker driver + containerd runtime), Helm, Chaos Mesh `NetworkChaos` CRD, Go (`go test`-driven workload using the `github.com/zilliztech/woodpecker` client), GitHub Actions.

**Source spec:** `docs/superpowers/specs/2026-06-04-chaos-mesh-network-jitter-design.md`

---

## Corrections to the spec (verified against the codebase)

Where these conflict with the spec, **these win** (they were verified file-by-file during research):

1. **Ports:** workload RPC = gRPC `:18080`; admin/healthz/node-status = `:9091` (`tests/e2e_operator/main_test.go:52-54`, `smoke-test.sh:399`). `NetworkChaos` is link-scoped, not port-scoped. **Consequence:** the I6 no-crash / health probes must run **from the host via `kubectl`**, because the client pod is itself under client↔server chaos.
2. **Real server pod labels:** `app.kubernetes.io/instance: my-woodpecker` **AND** `app.kubernetes.io/component: server` (`deployments/operator/internal/controller/labels.go:23-31`). Not `app: server`. etcd/minio use bare `app: etcd` / `app: minio` (`smoke-test.sh` step4).
3. **Client API (verified):** `LogWriter.WriteAsync(ctx, *log.WriteMessage) <-chan *log.WriteResult` (`woodpecker/log/log_writer.go:238`); `WriteResult{LogMessageId *LogMessageId; Err error}` (`:676`); `LogMessageId{SegmentId, EntryId int64}` (`woodpecker/log/log_message.go:29`); `LogReader.ReadNext(ctx) (*LogMessage, error)` with `LogMessage{Id *LogMessageId; Payload []byte; Properties map[string]string}` (`:71`); `log.EarliestLogMessageID()` (`:56`); `logHandle.GetId() int64` (`woodpecker/log/log_handle.go:46`). Client = `woodpecker.NewClient(ctx, cfg, etcdCli, true)` + `etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())`. **No app-level CRC field** → I3 uses an app-computed `payloadHash` stored as a message Property and recomputed on read-back.
4. **Error helpers:** `werr.IsRetryableErr` (`common/werr/errors.go:309`), `werr.IsTransportError` (`common/werr/utils.go:160`). `IsRetryableErr` returns **false** for non-Woodpecker errors (raw `context.DeadlineExceeded`, gRPC status). I5 ("permanent error") therefore means **a Woodpecker-typed, non-retryable error only** — raw ctx-deadline is an I4 (liveness budget) signal, not I5.
5. **In-pod code delivery:** `smoke-test.sh:439-441` `git clone`s a **stale GitHub branch** for the Go test code. We must instead ship **current local source** (tarball + `kubectl cp` + extract), so the test exercises code under review.
6. **Server image:** built from current source by `make docker`, tagged `zilliztech/woodpecker:v0.1.26`, `minikube image load`ed (`smoke-test.sh:34,140-142`). Keep the tag; the CR references it.
7. **No minikube/helm GitHub Action in the repo** — install inline in the nightly job.
8. **minikube runtime:** `smoke-test.sh:112` uses the default runtime; we need `--container-runtime=containerd` (+ 8 GiB / 6 CPU). Parameterize so smoke-test keeps its defaults.

**Plan refinements over the raw research (decided here):**

- **A. I2 soundness:** concurrency is **N logs × 1 writer each** (not multiple writers per log). With one writer per log, read-back `(segmentId, entryId)` is unambiguously monotonic, so the ordering check can't false-positive. Breadth comes from many logs.
- **B. Source delivery:** `tar --exclude=.git` of the current working tree → `kubectl cp` → extract in-pod → `go test` in-pod (pod has `golang:1.24`). Robust, current, correct platform.
- **C. Client pod labeled `role: wp-client`** in `wp_launch_client_pod`; the client↔server manifests select it by `labelSelectors: {role: wp-client}` (avoids the `fieldSelectors` portability gamble).

---

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `deployments/operator/test/lib.sh` | Create | Sourceable bring-up functions (minikube/operator/deps/CR/health/client-pod), parameterized by env vars. |
| `deployments/operator/test/smoke-test.sh` | Modify | Thin wrappers that source `lib.sh`; preserve current defaults. |
| `tests/chaos_mesh/README.md` | Create | How to run, prerequisites, scenario + invariant tables, "nightly-only." |
| `tests/chaos_mesh/manifests/*.yaml` | Create (7) | One `NetworkChaos` per scenario N1–N7, verified selectors, no `duration`. |
| `tests/chaos_mesh/workload/record.go` | Create | Acked-record JSONL persistence (append/load), concurrency-safe. |
| `tests/chaos_mesh/workload/classify.go` | Create | `payloadHash`, `isWoodpeckerPermanent` (I5 predicate). |
| `tests/chaos_mesh/workload/record_test.go` | Create | Unit tests for record round-trip. |
| `tests/chaos_mesh/workload/classify_test.go` | Create | Unit tests for the I5 predicate + hash determinism. |
| `tests/chaos_mesh/workload/main_test.go` | Create | Flags, client bring-up, `TestNetworkChaosWorkload` dispatcher, load+verify phases, I1–I7. |
| `tests/chaos_mesh/run_network_chaos.sh` | Create | Host orchestrator: bring-up + chaos-mesh install + smoke-check + scenario loop + artifacts + cleanup. |
| `.github/workflows/nightly-chaos-mesh.yaml` | Create | Reusable (`workflow_call`+`workflow_dispatch`) nightly job; no PR trigger, no ci-passed step. |
| `.github/workflows/nightly.yaml` | Modify | Add one job that calls the new workflow. |

---

## Task 1: Refactor reusable bring-up into `lib.sh`

**Files:**
- Create: `deployments/operator/test/lib.sh`
- Modify: `deployments/operator/test/smoke-test.sh`

- [ ] **Step 1: Create `lib.sh` with parameterized bring-up functions**

Extract the bodies of `do_step1`–`do_step7` (pod-create part), `do_step?` config heredoc from the current `smoke-test.sh` into functions. Add the resource/runtime knobs.

```bash
#!/bin/bash
# Shared Woodpecker operator bring-up library. Source me; do not execute.
# Consumers set these before sourcing (defaults preserve smoke-test.sh behavior):
: "${CLUSTER_NAME:=wp-operator-smoke}"
: "${CR_NAME:=my-woodpecker}"
: "${NAMESPACE:=default}"
: "${REPLICAS:=3}"
: "${OPERATOR_IMG:=woodpecker-operator:smoke}"
: "${WP_IMG:=zilliztech/woodpecker:v0.1.26}"
: "${MK_CPUS:=4}"            # chaos overrides -> 6
: "${MK_MEMORY:=4096}"      # chaos overrides -> 8192
: "${MK_RUNTIME:=}"         # chaos overrides -> containerd ; empty = minikube default
: "${CLIENT_POD:=wp-client-test}"

_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "$_LIB_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$OPERATOR_DIR/../.." && pwd)"

log()  { echo -e "\033[0;32m[$(date +'%H:%M:%S')]\033[0m $*"; }
warn() { echo -e "\033[1;33m[$(date +'%H:%M:%S')] WARN:\033[0m $*"; }
fail() { echo -e "\033[0;31m[$(date +'%H:%M:%S')] FAIL:\033[0m $*"; exit 1; }

wp_minikube_start() {
  if minikube status -p "$CLUSTER_NAME" &>/dev/null; then log "cluster $CLUSTER_NAME up"; return 0; fi
  local rt=""; [ -n "$MK_RUNTIME" ] && rt="--container-runtime=$MK_RUNTIME"
  minikube start -p "$CLUSTER_NAME" --cpus="$MK_CPUS" --memory="$MK_MEMORY" --driver=docker $rt
  kubectl config use-context "$CLUSTER_NAME"
}
wp_deploy_operator() {  # body of current do_step2
  cd "$OPERATOR_DIR"
  make docker-build IMG="$OPERATOR_IMG"
  minikube -p "$CLUSTER_NAME" image load "$OPERATOR_IMG"
  make deploy IMG="$OPERATOR_IMG"
  kubectl wait --for=condition=Available deployment -l control-plane=controller-manager \
    -n woodpecker-operator-system --timeout=120s
}
wp_build_wp_image() {   # body of current do_step3
  cd "$PROJECT_ROOT"
  make docker 2>/dev/null || docker build -t woodpecker:latest -f build/docker/ubuntu22.04/Dockerfile .
  docker tag woodpecker:latest "$WP_IMG" 2>/dev/null || true
  minikube -p "$CLUSTER_NAME" image load "$WP_IMG"
  docker pull busybox:1.36 2>/dev/null || true; minikube -p "$CLUSTER_NAME" image load busybox:1.36
}
wp_deploy_deps()  { :; }   # paste current do_step4 etcd+minio apply + wait verbatim
wp_create_cr()    { :; }   # paste current do_step5 ConfigMap+CR apply + wait verbatim (uses $REPLICAS)
wp_wait_healthy() { :; }   # paste current do_step6 gossip/health loop verbatim (curl :9091)
wp_launch_client_pod() {   # pod-create part of current do_step7, PLUS a role label (refinement C)
  kubectl get pod "$CLIENT_POD" &>/dev/null && return 0
  docker pull golang:1.24 2>/dev/null || true; minikube -p "$CLUSTER_NAME" image load golang:1.24
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${CLIENT_POD}
  labels: { role: wp-client }
spec:
  containers:
    - name: test
      image: golang:1.24
      imagePullPolicy: IfNotPresent
      command: ["/bin/bash","-c","sleep infinity"]
      resources: { requests: { cpu: "500m", memory: "1Gi" } }
  restartPolicy: Never
EOF
  kubectl wait --for=condition=Ready pod/"$CLIENT_POD" --timeout=300s
}
wp_write_client_config() {  # heredoc from current do_step7; param $1 = replicas
  local replicas="${1:-$REPLICAS}" seeds=""
  for i in $(seq 0 $((replicas-1))); do
    seeds="$seeds
            - ${CR_NAME}-server-${i}.${CR_NAME}-server-headless.${NAMESPACE}.svc:18080"
  done
  kubectl exec "$CLIENT_POD" -- bash -c "cat > /tmp/test-config.yaml <<'CFGEOF'
woodpecker:
  meta: { type: etcd }
  client:
    segmentRollingPolicy: { maxSize: 1000 }
    quorum:
      replicaCount: ${replicas}
      quorumBufferPools:
        - name: default-region-pool
          seeds:${seeds}
  logstore: { retentionPolicy: { ttl: 10 } }
  storage: { type: service, rootPath: /tmp/wp-test-data }
log: { level: info, format: json, stdout: true }
etcd: { endpoints: [ etcd.${NAMESPACE}.svc:2379 ], rootPath: by-dev }
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
```

> When pasting the `:;` placeholder bodies, copy the **exact** blocks from the current `smoke-test.sh` (`do_step4` lines ~152-209, `do_step5` ~214-296, `do_step6` ~301-364). Do not paraphrase — they are known-good.

- [ ] **Step 2: Refactor `smoke-test.sh` to source `lib.sh`**

Replace the `do_step1`..`do_step7` bodies with wrappers, keeping smoke-test's existing defaults (no chaos knobs set, so `MK_RUNTIME` stays empty and resources stay 4/4096):

```bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
do_step1() { wp_minikube_start; }
do_step2() { wp_deploy_operator; }
do_step3() { wp_build_wp_image; }
do_step4() { wp_deploy_deps; }
do_step5() { wp_create_cr; }
do_step6() { wp_wait_healthy; }
# do_step7..do_step9 keep their existing test-running logic, but call
# wp_launch_client_pod + wp_write_client_config for the pod/config setup.
```

- [ ] **Step 3: Verify syntax + bring-up parity**

Run: `bash -n deployments/operator/test/lib.sh && bash -n deployments/operator/test/smoke-test.sh`
Expected: no output (syntax OK).

Run (requires minikube/docker): `cd deployments/operator/test && ./smoke-test.sh step1 && ./smoke-test.sh step6 && kubectl get pods -l app.kubernetes.io/instance=my-woodpecker`
Expected: 3 server pods `Running`. Then `./smoke-test.sh clean`.

- [ ] **Step 4: Commit**

```bash
git add deployments/operator/test/lib.sh deployments/operator/test/smoke-test.sh
git commit -m "test(operator): extract reusable bring-up into lib.sh

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Scaffold `tests/chaos_mesh/` + README

**Files:**
- Create: `tests/chaos_mesh/README.md`, `tests/chaos_mesh/manifests/.gitkeep`, `tests/chaos_mesh/workload/.gitkeep`

- [ ] **Step 1: Create the directory tree and README**

```bash
mkdir -p tests/chaos_mesh/manifests tests/chaos_mesh/workload
touch tests/chaos_mesh/manifests/.gitkeep tests/chaos_mesh/workload/.gitkeep
```

README contents: a short intro, prerequisites (`minikube, docker, kubectl, helm, go`), `./run_network_chaos.sh` usage, the N1–N7 scenario table and I1–I7 invariant table copied from the spec, and a bold note: **"Nightly only — never a PR-gating check."** The Go workload is part of the root module `github.com/zilliztech/woodpecker` (no new `go.mod`).

- [ ] **Step 2: Verify + commit**

Run: `ls -R tests/chaos_mesh`
Expected: shows `README.md`, `manifests/`, `workload/`.

```bash
git add tests/chaos_mesh
git commit -m "test(chaos-mesh): scaffold tests/chaos_mesh layout

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: NetworkChaos manifests (N1–N7)

**Files:**
- Create: `tests/chaos_mesh/manifests/{server-minio-delay,server-minio-loss,server-minio-blip,server-etcd-delay,server-etcd-blip,client-server-delay,client-server-loss}.yaml`

- [ ] **Step 1: Write all seven manifests**

Selectors use verified labels (Correction #2 + refinement C). `duration` omitted (orchestrator owns lifetime).

`server-minio-delay.yaml`:
```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata: { name: server-minio-delay, namespace: default }
spec:
  action: delay
  mode: all
  selector:
    namespaces: [default]
    labelSelectors: { app.kubernetes.io/instance: my-woodpecker, app.kubernetes.io/component: server }
  direction: to
  target:
    mode: all
    selector: { namespaces: [default], labelSelectors: { app: minio } }
  delay: { latency: "300ms", jitter: "100ms", correlation: "50" }
```

`server-minio-loss.yaml`: identical to N1 but:
```yaml
  action: loss
  loss: { loss: "10", correlation: "25" }
```
(remove the `delay:` block)

`server-minio-blip.yaml`:
```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata: { name: server-minio-blip, namespace: default }
spec:
  action: partition
  mode: all
  selector:
    namespaces: [default]
    labelSelectors: { app.kubernetes.io/instance: my-woodpecker, app.kubernetes.io/component: server }
  direction: both
  target:
    mode: all
    selector: { namespaces: [default], labelSelectors: { app: minio } }
```

`server-etcd-delay.yaml`: copy N1, change `metadata.name: server-etcd-delay` and `target...labelSelectors: { app: etcd }`.

`server-etcd-blip.yaml`: copy N3, change `metadata.name: server-etcd-blip` and `target...labelSelectors: { app: etcd }`.

`client-server-delay.yaml` (selects the client pod by its `role` label — refinement C):
```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata: { name: client-server-delay, namespace: default }
spec:
  action: delay
  mode: all
  selector:
    namespaces: [default]
    labelSelectors: { role: wp-client }
  direction: to
  target:
    mode: all
    selector:
      namespaces: [default]
      labelSelectors: { app.kubernetes.io/instance: my-woodpecker, app.kubernetes.io/component: server }
  delay: { latency: "300ms", jitter: "100ms", correlation: "50" }
```

`client-server-loss.yaml`: copy N6, change `metadata.name: client-server-loss`, replace `delay:` with:
```yaml
  action: loss
  loss: { loss: "10", correlation: "25" }
```

- [ ] **Step 2: Verify YAML + commit**

Run:
```bash
for f in tests/chaos_mesh/manifests/*.yaml; do
  python3 -c "import yaml,sys; list(yaml.safe_load_all(open(sys.argv[1])))" "$f" && echo "OK $f"
done
```
Expected: `OK` for all seven. (Server-side CRD validation `kubectl apply --dry-run=server` happens later in Task 12 once Chaos Mesh is installed.)

```bash
git add tests/chaos_mesh/manifests
git commit -m "test(chaos-mesh): add NetworkChaos manifests N1-N7

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Workload — record persistence (TDD)

**Files:**
- Create: `tests/chaos_mesh/workload/record.go`
- Test: `tests/chaos_mesh/workload/record_test.go`

- [ ] **Step 1: Write the failing test**

```go
package workload

import ("os"; "path/filepath"; "testing"; "github.com/stretchr/testify/require")

func TestRecorderRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "acked.jsonl")
	r, err := openRecorder(path, true)
	require.NoError(t, err)
	in := []AckRecord{
		{LogName: "chaos-n152-0", LogId: 1, SegmentId: 0, EntryId: 0, PayloadHash: "aa", Seq: 1},
		{LogName: "chaos-n152-0", LogId: 1, SegmentId: 0, EntryId: 1, PayloadHash: "bb", Seq: 2},
	}
	for _, rec := range in { require.NoError(t, r.append(rec)) }
	require.NoError(t, r.close())
	out, err := loadRecords(path)
	require.NoError(t, err)
	require.Equal(t, in, out)
	_ = os.Remove(path)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd tests/chaos_mesh/workload && go test -run TestRecorderRoundTrip .`
Expected: FAIL — `undefined: openRecorder/AckRecord/loadRecords`.

- [ ] **Step 3: Implement `record.go`**

```go
package workload

import ("bufio"; "encoding/json"; "os"; "sync")

// AckRecord is the persisted unit of truth for I1/I2/I3 read-back.
type AckRecord struct {
	LogName     string `json:"log"`
	LogId       int64  `json:"logId"`
	SegmentId   int64  `json:"seg"`
	EntryId     int64  `json:"entry"`
	PayloadHash string `json:"hash"`
	Seq         int64  `json:"seq"`
}

type recorder struct {
	mu sync.Mutex
	f  *os.File
}

func openRecorder(path string, truncate bool) (*recorder, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if truncate { flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC }
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil { return nil, err }
	return &recorder{f: f}, nil
}

func (r *recorder) append(rec AckRecord) error {
	b, err := json.Marshal(rec)
	if err != nil { return err }
	r.mu.Lock(); defer r.mu.Unlock()
	_, err = r.f.Write(append(b, '\n'))
	return err
}

func (r *recorder) close() error { return r.f.Close() }

func loadRecords(path string) ([]AckRecord, error) {
	f, err := os.Open(path)
	if err != nil { return nil, err }
	defer f.Close()
	var out []AckRecord
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 0, 1024*1024), 8*1024*1024)
	for s.Scan() {
		line := s.Bytes()
		if len(line) == 0 { continue }
		var rec AckRecord
		if err := json.Unmarshal(line, &rec); err != nil { return nil, err }
		out = append(out, rec)
	}
	return out, s.Err()
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd tests/chaos_mesh/workload && go test -run TestRecorderRoundTrip .`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/chaos_mesh/workload/record.go tests/chaos_mesh/workload/record_test.go
git commit -m "test(chaos-mesh): acked-record JSONL persistence

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Workload — hash + I5 classifier (TDD)

**Files:**
- Create: `tests/chaos_mesh/workload/classify.go`
- Test: `tests/chaos_mesh/workload/classify_test.go`

- [ ] **Step 1: Write the failing test**

```go
package workload

import ("context"; "errors"; "testing"; "github.com/stretchr/testify/require")

func TestPayloadHashDeterministic(t *testing.T) {
	require.Equal(t, payloadHash([]byte("abc")), payloadHash([]byte("abc")))
	require.NotEqual(t, payloadHash([]byte("abc")), payloadHash([]byte("abd")))
}

func TestIsWoodpeckerPermanent_TransientAndNil(t *testing.T) {
	require.False(t, isWoodpeckerPermanent(nil))
	require.False(t, isWoodpeckerPermanent(context.DeadlineExceeded))
	require.False(t, isWoodpeckerPermanent(context.Canceled))
	require.False(t, isWoodpeckerPermanent(errors.New("some random non-wp error")))
}
```
> The Woodpecker-typed permanent/retryable cases are exercised live in Task 12 (constructing real `werr` errors here would couple the unit test to internal constructors). These cases pin the critical "raw ctx / nil / unknown error is NOT charged as I5" behavior, which is the subtle part.

- [ ] **Step 2: Run to verify it fails**

Run: `cd tests/chaos_mesh/workload && go test -run 'TestPayloadHash|TestIsWoodpeckerPermanent' .`
Expected: FAIL — `undefined: payloadHash/isWoodpeckerPermanent`.

- [ ] **Step 3: Implement `classify.go`**

```go
package workload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/zilliztech/woodpecker/common/werr"
)

func payloadHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// isWoodpeckerPermanent reports true ONLY for a Woodpecker-typed, non-retryable error.
// Raw context deadline / cancellation / transport errors are transient (owned by the
// client's own retry budget) and must NOT be charged as I5 violations (Correction #4).
func isWoodpeckerPermanent(err error) bool {
	if err == nil { return false }
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) { return false }
	if werr.IsTransportError(err) { return false }
	// A retryable Woodpecker error is acceptable degradation, not a permanent failure.
	if werr.IsRetryableErr(err) { return false }
	// Permanent iff it is a Woodpecker-typed error that is NOT retryable.
	var wpErr interface{ IsRetryable() bool }
	if errors.As(err, &wpErr) { return !wpErr.IsRetryable() }
	return false // unknown / non-WP error: not our I5 target
}
```
> **At execution time, confirm** the `interface{ IsRetryable() bool }` assertion matches the real `*werr.woodpeckerError` method (research cited `errors.go:309` for `IsRetryableErr`; verify the method name with `go doc github.com/zilliztech/woodpecker/common/werr` — if the method differs, adjust the interface and re-run the test). The test in Step 1 does not depend on this branch.

- [ ] **Step 4: Run to verify it passes**

Run: `cd tests/chaos_mesh/workload && go test -run 'TestPayloadHash|TestIsWoodpeckerPermanent' .`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/chaos_mesh/workload/classify.go tests/chaos_mesh/workload/classify_test.go
git commit -m "test(chaos-mesh): payload hash + I5 permanent-error classifier

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Workload — flags + client bring-up

**Files:**
- Create: `tests/chaos_mesh/workload/main_test.go`

- [ ] **Step 1: Write flags + `newClient` + an empty dispatcher**

```go
package workload

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/woodpecker"
)

var (
	configFile = flag.String("config-file", "/tmp/test-config.yaml", "woodpecker config path")
	phase      = flag.String("phase", "warmup", "warmup|under-chaos|recovery|verify")
	recordFile = flag.String("record-file", "/tmp/wp-chaos-acked.jsonl", "acked records JSONL")
	numLogs    = flag.Int("logs", 8, "number of concurrent logs (1 writer each — refinement A)")
	windowSecs = flag.Int("window", 45, "load-phase duration seconds")
	logPrefix  = flag.String("log-prefix", "chaos-n152", "stable log name prefix across phases")
)

func newClient(ctx context.Context, t *testing.T) (woodpecker.Client, func()) {
	cfg, err := config.NewConfiguration(*configFile)
	require.NoError(t, err)
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	cli, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	return cli, func() { _ = cli.Close(ctx); _ = etcdCli.Close() }
}

func TestNetworkChaosWorkload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cli, closeFn := newClient(ctx, t)
	defer closeFn()
	switch *phase {
	case "warmup":
		runLoadPhase(ctx, t, cli, true)
	case "under-chaos", "recovery":
		runLoadPhase(ctx, t, cli, false)
	case "verify":
		runVerifyPhase(ctx, t, cli)
	default:
		t.Fatalf("unknown phase %q", *phase)
	}
}
```

- [ ] **Step 2: Verify it compiles (functions defined in Tasks 7-8)**

This step intentionally won't build until Tasks 7–8 add `runLoadPhase`/`runVerifyPhase`. Confirm only that imports resolve:
Run: `cd tests/chaos_mesh/workload && go vet ./... 2>&1 | head`
Expected: errors are limited to `undefined: runLoadPhase` / `undefined: runVerifyPhase` (no import/type errors). If an import path or `NewClient`/`NewConfiguration`/`GetRemoteEtcdClient` signature is wrong, fix it now against `go doc`.

- [ ] **Step 3: Commit**

```bash
git add tests/chaos_mesh/workload/main_test.go
git commit -m "test(chaos-mesh): workload flags + client bring-up + phase dispatcher

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Workload — load phase (warmup/under-chaos/recovery) + I4/I5/I7

**Files:**
- Modify: `tests/chaos_mesh/workload/main_test.go`

- [ ] **Step 1: Add `runLoadPhase`**

One writer per log (refinement A) + one tail reader per log. Per-op 30s bound (I4). I5 via `isWoodpeckerPermanent`. I7 via `ok > 0`.

```go
import (  // add to the existing import block
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/zilliztech/woodpecker/woodpecker/log"
)

type metrics struct{ ok, errRetryable, errPermanent int64 }

func runLoadPhase(ctx context.Context, t *testing.T, cli woodpecker.Client, truncate bool) {
	rec, err := openRecorder(*recordFile, truncate)
	require.NoError(t, err)
	defer rec.close()

	var m metrics
	deadline := time.Now().Add(time.Duration(*windowSecs) * time.Second)

	type lh struct {
		name   string
		id     int64
		handle log.LogHandle
		writer log.LogWriter
		seq    int64
	}
	logs := make([]*lh, *numLogs)
	for i := range logs {
		name := fmt.Sprintf("%s-%d", *logPrefix, i)
		_ = cli.CreateLog(ctx, name) // idempotent across phases
		h, err := cli.OpenLog(ctx, name)
		require.NoError(t, err)
		w, err := h.OpenLogWriter(ctx)
		require.NoError(t, err)
		logs[i] = &lh{name: name, id: h.GetId(), handle: h, writer: w}
	}

	var wg sync.WaitGroup
	// ---- one WRITER per log (refinement A: unambiguous ordering) ----
	for _, L := range logs {
		wg.Add(1)
		go func(L *lh) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				seq := atomic.AddInt64(&L.seq, 1)
				payload := []byte(fmt.Sprintf("%s|%d|%d", L.name, time.Now().UnixNano(), seq))
				ph := payloadHash(payload)
				opCtx, opCancel := context.WithTimeout(ctx, 30*time.Second) // I4 bound
				res := <-L.writer.WriteAsync(opCtx, &log.WriteMessage{
					Payload:    payload,
					Properties: map[string]string{"hash": ph},
				})
				opCancel()
				if res.Err != nil {
					if isWoodpeckerPermanent(res.Err) {
						atomic.AddInt64(&m.errPermanent, 1)
						t.Errorf("I5 VIOLATION (write): permanent error under transient fault: %v", res.Err)
					} else {
						atomic.AddInt64(&m.errRetryable, 1) // acceptable degradation
					}
					continue
				}
				atomic.AddInt64(&m.ok, 1)
				_ = rec.append(AckRecord{
					LogName: L.name, LogId: L.id,
					SegmentId: res.LogMessageId.SegmentId, EntryId: res.LogMessageId.EntryId,
					PayloadHash: ph, Seq: seq,
				})
			}
		}(L)
	}
	// ---- one tail READER per log (liveness / no-hang) ----
	for _, L := range logs {
		wg.Add(1)
		go func(L *lh) {
			defer wg.Done()
			earliest := log.EarliestLogMessageID()
			r, err := L.handle.OpenLogReader(ctx, &earliest, "chaos-tail-"+L.name)
			if err != nil { return }
			defer r.Close(ctx)
			for time.Now().Before(deadline) {
				rCtx, rCancel := context.WithTimeout(ctx, 30*time.Second)
				_, err := r.ReadNext(rCtx)
				rCancel()
				if err != nil && isWoodpeckerPermanent(err) {
					t.Errorf("I5 VIOLATION (read): permanent error under transient fault: %v", err)
				}
			}
		}(L)
	}
	wg.Wait()
	for _, L := range logs { _ = L.writer.Close(ctx) }

	require.Greater(t, m.ok, int64(0),
		"I7 VIOLATION: zero successful writes in phase %s (no forward progress)", *phase)
	t.Logf("phase=%s ok=%d retryableErr=%d permanentErr=%d",
		*phase, m.ok, m.errRetryable, m.errPermanent)
}
```
> I4 note: each op has a 30s bound → a stuck op returns `DeadlineExceeded` (definitive), never hangs; `wg.Wait()` returning within the `go test -timeout` is itself the no-deadlock witness. At execution time, confirm `OpenLogReader(ctx, *LogMessageId, name)` and `OpenLogWriter(ctx)` signatures against `go doc` (research cited them; adjust if needed).

- [ ] **Step 2: Verify build/vet**

Run: `cd tests/chaos_mesh/workload && go vet ./... 2>&1 | head`
Expected: only `undefined: runVerifyPhase` remains.

- [ ] **Step 3: Commit**

```bash
git add tests/chaos_mesh/workload/main_test.go
git commit -m "test(chaos-mesh): load phase + I4/I5/I7 assertions

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Workload — verify phase + I1/I2/I3

**Files:**
- Modify: `tests/chaos_mesh/workload/main_test.go`

- [ ] **Step 1: Add `runVerifyPhase`**

```go
import ("sort") // add to import block

func runVerifyPhase(ctx context.Context, t *testing.T, cli woodpecker.Client) {
	recs, err := loadRecords(*recordFile)
	require.NoError(t, err)
	require.Greater(t, len(recs), 0, "no acked records to verify")

	byLog := map[string][]AckRecord{}
	for _, r := range recs { byLog[r.LogName] = append(byLog[r.LogName], r) }

	for name, want := range byLog {
		sort.Slice(want, func(i, j int) bool { return want[i].Seq < want[j].Seq })
		h, err := cli.OpenLog(ctx, name)
		require.NoError(t, err)
		earliest := log.EarliestLogMessageID()
		r, err := h.OpenLogReader(ctx, &earliest, "verify-"+name)
		require.NoError(t, err)

		ackedHash := map[[2]int64]string{}
		for _, w := range want { ackedHash[[2]int64{w.SegmentId, w.EntryId}] = w.PayloadHash }

		seen := map[[2]int64]bool{}
		var lastSeg, lastEntry int64 = -1, -1
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Minute)
		for len(seen) < len(ackedHash) {
			msg, err := r.ReadNext(rctx)
			require.NoError(t, err, "I1/I4 read-back failed for %s (%d/%d)", name, len(seen), len(ackedHash))
			k := [2]int64{msg.Id.SegmentId, msg.Id.EntryId}
			// I2 ordering: strictly increasing (seg,entry) in read order (one writer per log).
			if msg.Id.SegmentId == lastSeg {
				require.Greater(t, msg.Id.EntryId, lastEntry, "I2 VIOLATION: non-increasing entryId in %s", name)
			} else if lastSeg >= 0 {
				require.Greater(t, msg.Id.SegmentId, lastSeg, "I2 VIOLATION: non-increasing segmentId in %s", name)
			}
			lastSeg, lastEntry = msg.Id.SegmentId, msg.Id.EntryId
			if wantHash, ok := ackedHash[k]; ok {
				require.Equal(t, wantHash, payloadHash(msg.Payload),
					"I3 VIOLATION: payload hash mismatch %s seg=%d entry=%d", name, k[0], k[1])
				require.Equal(t, wantHash, msg.Properties["hash"],
					"I3 VIOLATION: property hash mismatch %s", name)
				seen[k] = true
			}
		}
		rcancel()
		require.Equal(t, len(ackedHash), len(seen),
			"I1 VIOLATION: %d acked entries missing in %s", len(ackedHash)-len(seen), name)
		_ = r.Close(ctx)
		t.Logf("VERIFY %s: %d acked entries durable, ordered, hash-matched", name, len(seen))
	}
}
```

- [ ] **Step 2: Verify full build + unit tests**

Run: `cd tests/chaos_mesh/workload && go build ./... && go vet ./... && go test -run 'TestRecorder|TestPayloadHash|TestIsWoodpeckerPermanent' .`
Expected: build clean; the three unit tests PASS. (The `TestNetworkChaosWorkload` integration test needs a cluster — exercised in Task 12.)

- [ ] **Step 3: Commit**

```bash
git add tests/chaos_mesh/workload/main_test.go
git commit -m "test(chaos-mesh): verify phase + I1/I2/I3 read-back assertions

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Orchestrator — bring-up + Chaos Mesh install + injection smoke-check

**Files:**
- Create: `tests/chaos_mesh/run_network_chaos.sh`

- [ ] **Step 1: Write the orchestrator top half**

```bash
#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Chaos knobs (override lib.sh defaults) — spec Layer 1 + Correction #8.
export CLUSTER_NAME="wp-chaos"
export CR_NAME="my-woodpecker"
export NAMESPACE="default"
export REPLICAS=3
export MK_CPUS="${MK_CPUS:-6}" MK_MEMORY="${MK_MEMORY:-8192}" MK_RUNTIME="${MK_RUNTIME:-containerd}"
export WP_IMG="zilliztech/woodpecker:v0.1.26"
export CLIENT_POD="wp-client-test"

source "$PROJECT_ROOT/deployments/operator/test/lib.sh"

WORKLOAD_IN_POD=/root/woodpecker/tests/chaos_mesh/workload
RECORD_FILE=/tmp/wp-chaos-acked.jsonl

bringup() {
  wp_minikube_start; wp_deploy_operator; wp_build_wp_image
  wp_deploy_deps; wp_create_cr; wp_wait_healthy
  wp_launch_client_pod; wp_write_client_config "$REPLICAS"
}

install_chaos_mesh() {
  helm repo add chaos-mesh https://charts.chaos-mesh.org 2>/dev/null || true
  helm repo update
  helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace \
    --set chaosDaemon.runtime=containerd \
    --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
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
```

- [ ] **Step 2: Verify syntax**

Run: `bash -n tests/chaos_mesh/run_network_chaos.sh`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add tests/chaos_mesh/run_network_chaos.sh
git commit -m "test(chaos-mesh): orchestrator bring-up + chaos-mesh install + injection smoke-check

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Orchestrator — scenario loop + blips + I6 + artifacts + cleanup

**Files:**
- Modify: `tests/chaos_mesh/run_network_chaos.sh`

- [ ] **Step 1: Append the run/verify machinery + `main`**

```bash
run_phase() {  # $1 = phase
  kubectl exec -i "$CLIENT_POD" -- /bin/bash -c "
    set -e; cd $WORKLOAD_IN_POD
    go test -v -count=1 -timeout 16m -run TestNetworkChaosWorkload \
      -config-file /tmp/test-config.yaml -phase $1 -record-file $RECORD_FILE ."
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
  run_phase warmup
  kubectl apply -f "$m"
  kubectl wait --for=condition=AllInjected "networkchaos/$1" -n "$NAMESPACE" --timeout=60s
  run_phase under-chaos || { collect_artifacts "$1"; kubectl delete -f "$m" --ignore-not-found; return 1; }
  kubectl delete -f "$m" --ignore-not-found
  run_phase recovery || { collect_artifacts "$1"; return 1; }
  run_phase verify   || { collect_artifacts "$1"; return 1; }
  [ "$(restart_sum)" -eq "$rc0" ] || { collect_artifacts "$1"; fail "I6 VIOLATION: server restarted during $1"; }
  log "SCENARIO $1 PASSED"
}

run_blip() {  # $1 = manifest basename (N3/N5)
  local m="$SCRIPT_DIR/manifests/$1.yaml" rc0; rc0=$(restart_sum)
  run_phase warmup
  ( run_phase under-chaos ) & local wl=$!
  for _ in $(seq 1 6); do kubectl apply -f "$m"; sleep 5; kubectl delete -f "$m" --ignore-not-found; sleep 5; done
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
```
> The blip window is `6×(5s+5s)=60s`; pass `-window 60` to the under-chaos `go test` for blips if needed (tune in Task 12). `KEEP=1 ./run_network_chaos.sh` leaves the cluster up for inspection.

- [ ] **Step 2: Verify syntax + commit**

Run: `bash -n tests/chaos_mesh/run_network_chaos.sh`
Expected: no output.

```bash
git add tests/chaos_mesh/run_network_chaos.sh
git commit -m "test(chaos-mesh): scenario loop, blip loops, I6 check, artifacts, cleanup trap

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: Nightly-only CI workflow

**Files:**
- Create: `.github/workflows/nightly-chaos-mesh.yaml`
- Modify: `.github/workflows/nightly.yaml`

- [ ] **Step 1: Create the reusable workflow (no PR trigger, no ci-passed step)**

```yaml
name: Chaos Mesh Test
on:
  workflow_call:
  workflow_dispatch:        # manual only; deliberately NO pull_request trigger -> never PR-gating
concurrency:
  group: nightly-chaos-mesh-${{ github.ref }}
  cancel-in-progress: true
jobs:
  Chaos-Mesh-Test:
    name: Chaos Mesh Test
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: "1.25", cache: true }
      - name: Install minikube, helm
        run: |
          curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
          sudo install minikube /usr/local/bin/
          curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
          kubectl version --client
      - name: Run network-chaos suite
        run: ./tests/chaos_mesh/run_network_chaos.sh
      - name: Upload chaos artifacts on failure
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: chaos-mesh-artifacts
          path: tests/chaos_mesh/artifacts/
          retention-days: 7
          if-no-files-found: ignore
```

- [ ] **Step 2: Add a job to `nightly.yaml`**

Append (matching the existing `uses:` job style in that file):
```yaml
  chaos-mesh-test:
    name: Chaos Mesh Test
    uses: ./.github/workflows/nightly-chaos-mesh.yaml
    secrets: inherit
```

- [ ] **Step 3: Verify non-gating + lint**

Run: `grep -R "REQUIRED_CHECKS" .github/workflows/ | grep -i mesh`
Expected: **no output** (the new check is absent from every gating array).

Run: `grep -rn "pull_request" .github/workflows/nightly-chaos-mesh.yaml`
Expected: **no output** (no PR trigger).

Run (if available): `actionlint .github/workflows/nightly-chaos-mesh.yaml .github/workflows/nightly.yaml`
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/nightly-chaos-mesh.yaml .github/workflows/nightly.yaml
git commit -m "ci(nightly): add nightly-only Chaos Mesh network-jitter job (#152)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: Run on minikube and hunt bugs (acceptance §5)

**Files:** none — live execution + triage.

- [ ] **Step 1: Full run**

Run: `tests/chaos_mesh/run_network_chaos.sh 2>&1 | tee /tmp/chaos-run.log`
Expected: `Injection smoke-check PASSED`, each scenario reaches `AllInjected`, each `verify` logs `VERIFY ... durable, ordered, hash-matched`, final `ALL SCENARIOS PASSED`.

- [ ] **Step 2: Confirm the gate actually fired**

Run: `grep -E "Injection smoke-check PASSED|AllInjected|VERIFY .* durable|I[0-9] VIOLATION" /tmp/chaos-run.log`
Expected: a smoke-check PASS line, 7 `AllInjected`, ≥1 `VERIFY` per scenario, and ideally zero `VIOLATION`.

- [ ] **Step 3: Validate I2 assumption without chaos (Risk #4)**

Before trusting I2, run `warmup` then `verify` with **no** chaos (`KEEP=1`, then exec the two phases manually). If I2 fails even without chaos, the global `(seg,entry)` read order isn't monotonic under this config — drop `-logs` interaction or keep the single-writer-per-log design (already chosen) and, if still failing, weaken I2 to per-`Seq` subsequence. Document the outcome in the README.

- [ ] **Step 4: Triage any failure → acceptable vs bug**

```
I1/I2/I3 fail -> durability/ordering/corruption BUG (file issue w/ acked.jsonl + offending (seg,entry))
I4 fail       -> hang/deadlock BUG (capture goroutine dump: kubectl exec wp-client-test -- kill -QUIT <pid>)
I5 fail       -> transient fault became permanent WP error BUG (error string in server-*.log)
I6 fail       -> crash/OOM BUG (server-*.log tail + kubectl describe pod)
I7 fail       -> retry storm / no progress BUG (ok==0 with high retryableErr)
elevated retryableErr/latency but ok>0 and verify PASS -> ACCEPTABLE degradation (the expected result)
```

- [ ] **Step 5: Gauge flakiness, then record outcome**

Re-run once. Then append a short "Run results" section to `tests/chaos_mesh/README.md` (date, pass/fail per scenario, any bug candidates) and commit.

```bash
git add tests/chaos_mesh/README.md
git commit -m "test(chaos-mesh): record first network-jitter run results (#152)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Remaining unknowns / risks (carry into execution)

1. **GitHub `ubuntu-latest` resourcing.** `--cpus=6 --memory=8192` + chaos-daemon + 3 servers + etcd/minio + workload may OOM the runner. Fallback: `--cpus=4 --memory=8192`, or trim nightly to N1/N4/N6 with the full set weekly. The smoke-check still surfaces silent no-ops early.
2. **`werr` permanent-error method name.** Task 5 asserts `interface{ IsRetryable() bool }`; verify against `go doc .../common/werr` and adjust if the method differs.
3. **Client API signature drift.** Tasks 6–8 use researched signatures (`WriteAsync`, `ReadNext`, `OpenLogReader(ctx,*LogMessageId,name)`, `OpenLogWriter(ctx)`); confirm with `go build` and fix imports/args if needed.
4. **I2 across segments under load** — see Task 12 Step 3. Single-writer-per-log (chosen) is the safe design; fall back to per-`Seq` subsequence if needed.
5. **30s per-op bound vs internal budgets** (append callback ~30s, retry budget ~21.8s). May clip an op that would have succeeded; raise after observing p99 in Task 12.
6. **Single-pod etcd/minio** means N3/N5 blips are full dependency outages, not multi-AZ jitter — larger blast radius than prod (spec non-goal). Interpret accordingly.
7. **`make docker` target / image path drift** — keep the `|| docker build -f build/docker/ubuntu22.04/Dockerfile` fallback already in `wp_build_wp_image`.

---

## Self-Review

- **Spec coverage:** Layer-1 bring-up → Task 1; Chaos Mesh install + smoke-check → Task 9; scenario matrix N1–N7 → Task 3; three phases + I1–I7 → Tasks 7–8 (+I6 host-side Task 10); deliverables/layout → Tasks 2–11; nightly-only wiring → Task 11; run-and-hunt → Task 12. All spec sections map to a task.
- **Placeholders:** the `:;` bodies in `lib.sh` are explicit "paste exact block from smoke-test.sh lines X-Y" instructions, not vague TODOs. No "add error handling" / "TBD" remain.
- **Type consistency:** `AckRecord`, `recorder`, `openRecorder/append/close/loadRecords`, `payloadHash`, `isWoodpeckerPermanent`, `runLoadPhase`, `runVerifyPhase`, `metrics` are defined once and referenced consistently across Tasks 4–8.
