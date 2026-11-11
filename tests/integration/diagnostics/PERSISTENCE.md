# Deployed persistence completion and recovery acceptance

[#488](https://github.com/zilliztech/woodpecker/issues/488) validates one real
persistence contrast in the original #458 matrix: working sync completions,
idle, a short in-place CPU quota on the PVC-backed object store, and normal-workload recovery.
The existing Prometheus collector, persistence assessor and integrated runner
are reused at their default policies. No diagnostic rules or thresholds change.

The object-store CPU quota is an explicit test condition. It does not prove
a naturally occurring storage fault, physical disk/fsync latency, an exclusive
upload timer or crash durability. Sync execution includes work and callbacks;
pending tasks include **queued and executing** tasks. Checkpoint age or a
flusher-seen TimeTick alone cannot prove a persistence stall.

## Disposable target and build

Use a **dedicated namespace and Milvus/WP instance with no other clients**.
The selected MinIO backs deployment-wide storage, so its temporary CPU limit
can affect unrelated activity in that deployment. The harness never manages a
cluster, current kubecontext, Helm release, PVC data or another run's resources.

The initial profile requires:

- Ready service-mode WP, exactly one StreamingNode and one mix coordinator,
  reachable Milvus REST/gRPC and Prometheus with the example's 15-second scrapes
  and exact `namespace`, `cluster=local`, `woodpecker_id=RELEASE` labels.
- One explicit Ready MinIO StatefulSet replica using Bound persistent claims.
  `persistentVolumeClaimRetentionPolicy.whenScaled` must be `Retain` (including
  Kubernetes's default). Ephemeral or delete-on-scale storage is rejected.
- An explicit pre-existing CPU limit on MinIO, for example 1 CPU, with a stable
  applied baseline. Kubernetes forbids removing a limit added through in-place
  resize, so an unlimited baseline is rejected before workload or mutation.
  Provision this CPU budget as a separate reviewed fixture preparation step;
  the harness does not rewrite the StatefulSet template to establish it.
- One shared mounted Milvus ConfigMap containing `default.yaml` and `user.yaml`.
  The latter overrides the former. The selected address must name the explicit
  storage Service on port 9000, and that Service must select exactly the original
  storage Pod owned by the selected StatefulSet. These are object-store bindings,
  not Milvus-channel-to-WP-client metadata associations.
- A compatible mix coordinator management configuration API. If it masks the
  address/port, the mounted-file check remains verified while effective runtime
  overrides remain an explicit gap. Any readable contradictory value fails.
- An isolated development environment with sufficient disk/resource headroom.
  Do not adjust production guards or diagnostic thresholds to force a pass.

Provision or resume that environment separately using the stack's documented
native Helm/kubectl workflow. No Jaeger, Loki, Grafana, Alertmanager or wpcli
connection is required. Cluster lifecycle is an external operator action. The existing Kubernetes API
and kubelet must support in-place Pod CPU resize; the harness checks discovery
and actual applied resources rather than enabling a cluster feature.
The initial profile also requires read-only `exec` access to `cat` and Linux
cgroup v2's `/sys/fs/cgroup/cpu.max` in the selected storage container. Both
the request and its observed kernel value are retained; an unreadable quota
cannot establish a baseline or pass the controlled phase.

If the example MinIO has no CPU limit, prepare a disposable fixture explicitly.
Save its controller UID and original resource object to a fresh local file:

```sh
kubectl --context YOUR_CONTEXT -n diag-acceptance get statefulset full-stack-minio -o json \
  | python3 -c 'import json,sys; x=json.load(sys.stdin); print(json.dumps({"uid":x["metadata"]["uid"],"resources":x["spec"]["template"]["spec"]["containers"][0]["resources"]}))' \
  > minio-resources.before.json
kubectl --context YOUR_CONTEXT -n diag-acceptance patch statefulset full-stack-minio \
  --type=strategic -p '{"spec":{"template":{"spec":{"containers":[{"name":"minio","resources":{"limits":{"cpu":"1"}}}]}}}}'
kubectl --context YOUR_CONTEXT -n diag-acceptance rollout status statefulset/full-stack-minio --timeout=150s
```

This separate preparation rolls the MinIO Pod; the measured CPU-only scenario
starts after that rollout is Ready. It retains the existing memory/requests,
controller and PVC. After all test locks are clear, restore the saved resource
object with an ordinary guarded template patch and wait for its rollout:

```sh
python3 - <<'PY'
import json
from pathlib import Path
before = json.loads(Path("minio-resources.before.json").read_text())
patch = [{"op":"test","path":"/metadata/uid","value":before["uid"]},
         {"op":"test","path":"/spec/template/spec/containers/0/name","value":"minio"},
         {"op":"replace","path":"/spec/template/spec/containers/0/resources","value":before["resources"]}]
Path("minio-resources.restore.patch.json").write_text(json.dumps(patch))
PY
kubectl --context YOUR_CONTEXT -n diag-acceptance patch statefulset full-stack-minio \
  --type=json --patch-file minio-resources.restore.patch.json
kubectl --context YOUR_CONTEXT -n diag-acceptance rollout status statefulset/full-stack-minio --timeout=150s
```

Inspect the target and saved values before applying these preparation/teardown
commands; do not overwrite another operator's intervening configuration change.

From the repository root, with Go 1.25 or later:

```sh
make wpdiag
go -C tests/integration/diagnostics/queryclient build \
  -o ../../../../bin/query-requests .
python3 -m venv tests/integration/diagnostics/.local/venv
. tests/integration/diagnostics/.local/venv/bin/activate
python3 -m pip install \
  -r deployments/milvus-full-stack-helm/scripts/requirements-connections.txt

kubectl --context YOUR_CONTEXT label namespace diag-acceptance \
  diagnostics.woodpecker.io/persistence-acceptance=cli-persistence-v1 --overwrite
```

Substitute your actual context/namespace/release and storage resource names.
The pinned PyYAML preparation dependency parses only the mounted configuration;
credentials and other storage settings are never exported.

## Plan and execute

```sh
# Offline: no subprocesses, requests, directories or mutations.
python3 tests/integration/diagnostics/persistence_acceptance.py \
  --context YOUR_CONTEXT --namespace diag-acceptance \
  --release milvus --storage-statefulset full-stack-minio \
  --storage-service full-stack-minio

# Inspect the plan and opt in to this disposable deployment.
python3 tests/integration/diagnostics/persistence_acceptance.py \
  --context YOUR_CONTEXT --namespace diag-acceptance \
  --release milvus --storage-statefulset full-stack-minio \
  --storage-service full-stack-minio --execute
```

Default budgets are 24 batches of 16 marker rows per active phase, scheduled over
30 seconds, 20 additional seconds at the CPU quota, 16-second scrape padding and a
1200-second workload budget. Each actual RPC has an eight-second deadline;
the child also has a hard 180-second limit and parent-pipe cancellation. The
REST final-flush wait is capped at 120 seconds. Bounds cap batches at 48 and
acknowledged IDs at 4096 for this persistence profile (the query profile retains
its 512-ID bound). Raw JSON is capped at 16 MiB/file and 96 MiB/run, with
a separate bounded final receipt. Restoration has its own bounded allowance
after workload expiry. Source requests and encoded reports keep their own limits.

The runner acquires the shared create-only `wpdiag-cli-acceptance-lock`, records
controller/spec/claim identity before data creation, and creates one unique
`persist_accept_<timestamp>_<suffix>` collection after checking absence. Creation
can commit before an HTTP timeout, so ownership is journaled before the request.

It creates two shards and 12 owned partitions, seeds 64 rows, waits for a completed flush and verifies
every ID and vector search. Each active phase sends a real 16-row Insert and
Strong Query/Search per batch, with one official gRPC Flush after each half of
the workload. The two seals are normally 15 seconds apart, with an actual minimum 11-second
quota wait even when work overruns its planned schedule, respecting the default
per-collection flush quota of one per 10 seconds. The final REST flush waits
another 11 seconds before submission. No flush quota is disabled or raised.
The Flush response supplies its actual
timestamp and sealed segment IDs as decimal strings; it is a sealing
acknowledgement, **not a persistence-completion response**. Two boundaries across
the populated partitions/shards produce enough actual sync tasks for the source's unchanged
20-completion minimum. The default total is 1216 acknowledged rows (64 + 3 × 384),
about 76 KiB of logical float-vector payload, plus storage/index/protocol overhead.

Baseline and recovery require qualified normal sync completions; idle requires
positively observed idle populations. Missing lazy series cannot pass either
gate. During the controlled slow phase, the runner changes only the original
MinIO Pod's CPU request and limit to 25m using the native `pods/resize` subresource.
It preserves all memory settings, controller template, replica count, Pod UID,
image, persistent claims and service selection. Its atomic JSON patch tests the
original Pod UID and current resource object before replacement. No process is
frozen and no cluster feature or Milvus setting changes.

The runner waits for the actual kernel CPU quota to reflect the declared limit and
for the original Pod to remain Ready. After the workload and bounded hold it
restores the **exact original resource object** and waits for the original
enforced CPU quota and Ready again. The kubelet can retain an earlier CPU
allocation in `status.resources`; raw desired/status fields remain recorded.
The kernel observation verifies enforcement separately. The unchanged memory
settings remain checked without interpreting cached CPU allocation as current.
A restart invalidates the
contrast, while resource restoration still runs before cleanup when Pod/image
ownership remains intact. A replacement Pod, controller, claim or unexpected
resource change is refused.

The slow-phase report interval includes delayed completions after CPU recovery.
Completion histograms do not time unfinished work. A successful case requires
actual pending sync work and qualified slow sync-execution completions, followed
by a completed flush and exact data verification. It does not claim that every
step is timed exclusively inside the quota interval, that queue pressure remains
sustained, or that the aggregate timer isolates object-store CPU time.

Standalone `milvus persistence` and `run --flow write --stages persistence
--sources prometheus` must preserve conclusions, targets, uncertainty and original
source/observation lineage. Historical replay after normal recovery must retain
the earlier slow-completion finding. Absent namespace, wrong exporter and
unreachable backend controls must reject the working gate, keeping missing
series separate from a failed source. Pod/image/restart and mounted configuration
fingerprints must stay unchanged for Milvus/WP; the selected MinIO Pod, container/image and claims must also retain their
identities while its exact original resources are restored.

## Failure, recovery and artifacts

The unique run directory defaults to `.local/persistence/` beside this guide.
It contains `acceptance.json`, `storage-restoration.json`, workload RPC receipts,
standalone/integrated source reports, negative controls and private connection
files. Treat private configuration and operational labels as local data. A
failed/non-reproducing attempt stays failed and must be retained in the receipt.

Cleanup restores the original CPU resources before dropping the original owned collection and
releasing the original lock. Replacement controllers, claims, specs or locks and
external replica/resource changes are refused. Failed restoration retains data and lock
for investigation. A failed resize call may have committed; the pre-write journal
supports idempotent recovery. No arbitrary process IDs are killed.

After stopping the original runner, inspect its receipt and recover explicitly:

```sh
python3 tests/integration/diagnostics/persistence_recover.py \
  --run PATH_TO_ORIGINAL_RUN --context YOUR_CONTEXT --namespace diag-acceptance
# Inspect the original target, retained claims and restore/drop plan, then:
python3 tests/integration/diagnostics/persistence_recover.py \
  --run PATH_TO_ORIGINAL_RUN --context YOUR_CONTEXT --namespace diag-acceptance --execute
```

Recovery validates the original profile, target, run-owned name, lock and storage
provenance. It preserves the original failed `acceptance.json` and writes a
separate `manual-recovery.json`. A preflight name collision does not authorize
dropping that collection. Unresolved lock acquisition or unexpected ownership
requires manual investigation rather than deleting a competing lock.

Verify protocol and lifecycle counterexamples without a cluster:

```sh
go -C tests/integration/diagnostics/queryclient test -race ./...
python3 -m unittest discover -s tests/integration/diagnostics -p 'test_*.py' -v
```

These fixtures are distinct from an actual deployment receipt. Other persistence
hypotheses, native faults, query scheduling/execution, source/UI and resilience
acceptance remain their existing #458/#489–#493 tasks; full trace releases stay
in #459. This child does not close the complete acceptance program.
