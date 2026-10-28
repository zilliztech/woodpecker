# Native WP service diagnosis acceptance

This independent [#489](https://github.com/zilliztech/woodpecker/issues/489)
scenario exercises real WP gRPC append/read work, current Admin/metrics evidence,
the focused standalone adapter and integrated historical/current separation.
See [NATIVE_VALIDATION.md](NATIVE_VALIDATION.md) for actual execution evidence.

The fixture runs a released **WP server with its supported MinIO backend**. An
HTTP proxy holds this run's object requests while Admin and metrics stay live.
Writes wait while opening new object-backed segment writers; reads wait on a
cold object-backed reader. These are real operations and MinIO data, using
unchanged diagnostic rules and default Prometheus policy. This representative
case does not validate staged WAL local fsync, SDK quorum/leased reader progress,
every native rule or a Milvus E2E join.

## Build and prepare externally

Use Go 1.25+, Python 3.9+, kubectl and an existing disposable namespace. The
harness never changes the current context, Helm releases or cluster lifecycle.
It creates one private Pod and two ConfigMaps, including the exclusive acceptance
lock. WP, MinIO, a transport gate and Prometheus share the Pod; data lives in
new bounded emptyDirs. No Service, PVC, tenant metadata or existing Milvus/WP
resource is selected or changed.

From the repository root:

```sh
make wpcli wpdiag
go build -o bin/wp-native-fixture ./tests/integration/diagnostics/nativeclient

TASK_BUILD=$(mktemp -d)
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build \
  -o "$TASK_BUILD/native-fixture-linux" ./tests/integration/diagnostics/nativeclient
# Use GOARCH=amd64 for x86 Kubernetes nodes.
docker build -f tests/integration/diagnostics/nativeclient/Dockerfile \
  -t wp-native-acceptance:local "$TASK_BUILD"
```

Make that scratch image available to the target nodes through your normal
registry or local workflow. For an externally managed minikube profile:

```sh
minikube -p YOUR_PROFILE image load wp-native-acceptance:local --daemon
```

Default other images are `milvusdb/woodpecker:v0.1.46`,
`milvusdb/minio:RELEASE.2024-12-18T13-15-44Z` and
`quay.io/prometheus/prometheus:v3.0.1`. Explicit image flags support replacements;
actual resolved image/container identities are retained. Budget approximately
1.3 GiB of aggregate memory limits, plus Kubernetes and other workloads.
Initial dependency-startup retries are recorded; a measured restart invalidates
the comparison. The fixture sets no CPU limit.

Opt in a disposable namespace explicitly:

```sh
kubectl --context YOUR_CONTEXT create namespace native-diagnostics
kubectl --context YOUR_CONTEXT label namespace native-diagnostics \
  diagnostics.woodpecker.io/native-acceptance=cli-native-v1
```

Execution requires namespace get, Pod/ConfigMap create/get/delete, Pod port-forward
and failure-context Pod logs. No exec, RBAC changes, etcd or controller patch is
needed. The fixed create-only `wpdiag-cli-acceptance-lock` serializes runs with
the other acceptance scenarios. Host forwards bind only to loopback.

## Preview and execute

```sh
python3 tests/integration/diagnostics/native_acceptance.py \
  --context YOUR_CONTEXT --namespace native-diagnostics \
  --gate-image wp-native-acceptance:local
# Offline default: no connection, commands or output directory.

python3 tests/integration/diagnostics/native_acceptance.py \
  --context YOUR_CONTEXT --namespace native-diagnostics \
  --gate-image wp-native-acceptance:local \
  --output /tmp/wp-native-acceptance --execute
```

`--wp`, `--wpdiag` and `--worker` override host binaries. No global CLI config is
written. Each mode-0700 run directory holds its private WP service context and
exact node-admin mapping. The isolated service uses a self seed and explicit
loopback advertise addresses: HTTP readiness alone does not establish membership.
This topology is not a production seed configuration example.

The sequence covers:

1. Check original namespace UID/opt-in; acquire the exclusive lock; journal
   creation intent before API calls and retain actual resource UIDs.
2. Observe initially absent lazy completion series, then perform acknowledged
   writes and exact payload reads. Run expanded write checks, idle read and real
   EOF polling; expanded read activity must recognize EOF as progress.
3. Hold object requests and launch twenty appends to new segments. Require the
   existing `write_stall` rule to identify twenty persistent operations with
   matching-log completion coverage. Snapshot cannot establish sustained stall.
4. Repeat with twenty real reads of previously acknowledged, uncached data.
   Focused `read_stall` can express a suspicion; expanded read cannot invent a
   leased reader's catch-up expectation without explicit client metadata.
5. Release each gate, require all RPCs to succeed and transport to drain, then
   verify that current pending-operation suspicions clear.
6. Verify all **112 distinct acknowledged entries**, with exact log, segment,
   entry ID and bytes. Repeated reads do not inflate the unique-entry count.
7. Replay both retained intervals through actual Prometheus and integrated
   `--current-native`. Default-qualified slow completions must remain while the
   native context is a separate current snapshot. Earlier native incident
   receipts remain unchanged.
8. Require unchanged Pod/image/container/spec/restart identity, then remove only
   the original owned Pod and ConfigMaps using UID preconditions, lock last.
   All ephemeral fixture data is removed.

The worker uses the repository's real WP protobuf API. Append requires correctly
ordered `Buffered` then `Synced` responses with exact IDs and successful status;
buffering alone is not acknowledgement. Reads request one extra entry to detect
extras. EOF accepts only WP entry-not-found/end-of-file codes, never an arbitrary
failure or timeout. No tenant etcd or metadata prefix is selected automatically.

## Interpret the evidence

`acceptance.json` records bounds, pass/failure, phases, verified entries and
binary/source hashes. `fixture.json` journals the namespace, lock, resource
intents/UIDs, baseline identities and cleanup. Failure-time container state and
bounded fixture logs are retained when available.

Inspect `*-native.json`, `*-adapter.json`, `*-integrated-live.json`,
`*.original-native.json`, gate receipts and `*.workload.ndjson` for each fault.
A test-only **recording wrapper around the real wp binary** forwards unmodified
stdout/exit and retains that exact response. Assertions compare each import with
its own actual original run: finding/observation IDs, source IDs, timestamps,
targets, rule versions, supporting/contradicting references and unknowns. A
second live sample is not falsely treated as the same observation.

The adapter still imports only `write_stall`/`read_stall`, not every expanded
native rule. Raw reports retain missing/unsupported APIs, build/process identity,
registry capacity/eviction, non-atomic sampling and absent-series gaps. Wrong-node
selection must fail, not become a successful empty diagnosis. Existing semantic
fixtures cover additional API failure, truncation, reset and normally blocking
SDK-tail cases; they are not counted as deployed fault reproductions here.

Prometheus scrapes the actual fixture every two seconds with explicit component
and run labels. Incident windows include completion after release: this is
**completed server operation latency**, not every pending request's latency,
exclusive object-storage service time, SDK latency or a disk root cause. The
six-second native window is a reproduction setting, not a production SLO.
Silence, missing counters and exit 0 alone cannot pass positive acceptance.

## Bounds, cancellation and crash recovery

The default workload budget is 900 seconds (allowed 300–1800), with separate
bounded cleanup. Diagnostics have 25/30-second budgets, four live rounds and
bounded response/output sizes. Workers have a 75-second total deadline and cancel
on parent stdin EOF or SIGINT/TERM. There are twenty requests per case and at
most 64 held HTTP requests; the gate lease automatically releases within 70
seconds. Automatic release invalidates the contrast rather than silently passing.
Each diagnostic file is limited to 16 MiB and the artifact budget is 96 MiB.
No threshold or minimum-observation policy is lowered.

SIGINT/TERM attempts early release and owned cleanup. If control is unavailable,
removing the original ephemeral Pod also removes the fault. After SIGKILL/host
loss the gate expires and workers cancel; remove the surviving original fixture
using its journal:

```sh
python3 tests/integration/diagnostics/native_recover.py \
  --run /tmp/wp-native-acceptance/EXACT_RUN_ID \
  --context YOUR_CONTEXT --namespace native-diagnostics
# Inspect the offline recovery plan, then repeat with --execute.
```

Recovery needs neither a Ready server nor gate/etcd access. It checks the
original namespace, lock, resource UIDs, owner references and intended settings.
Foreign replacements/edits fail closed and preserve the lock. An ambiguous create
is resolved only through original run labels and exact intended settings. Pod
removal precedes ConfigMaps, lock last; API failure preserves the journal/lock
for retry. `manual-recovery.json` never rewrites a failed acceptance receipt.

## Offline checks

```sh
go test -race ./tests/integration/diagnostics/nativeclient
python3 -m unittest discover -s tests/integration/diagnostics -p 'test_native_acceptance.py' -v
```

These protocol, cancellation, provenance and ownership checks run in unit CI
without Kubernetes. They complement the real deployed receipt.
