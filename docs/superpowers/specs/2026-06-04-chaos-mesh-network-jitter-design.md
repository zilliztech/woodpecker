# Chaos Mesh — Network-Jitter Fault Injection (Nightly)

- **Issue:** [zilliztech/woodpecker#152](https://github.com/zilliztech/woodpecker/issues/152)
- **Date:** 2026-06-04
- **Status:** Approved design, ready for implementation plan

## Problem

Woodpecker leans heavily on object storage (S3/MinIO) and on network stability for
WAL write/read, segment completion, quorum replication, and metadata (etcd). In
multi-AZ production, transient network degradation — latency spikes, jitter, packet
loss, brief disconnects — routinely causes elevated write latency, amplified retries,
LAC-progression delays, and temporary read unavailability.

The existing chaos suite (`tests/docker/chaos/`) only exercises **hard** failures:
node `kill`/`stop`/`restart`, full Docker-network **partition** (disconnect/connect),
and MinIO/etcd **stop**. There is **no soft network degradation** anywhere in the
repo — confirmed: no `tc`/netem, no toxiproxy, no latency/jitter/loss injection. The
chaos matrix (`tests/docker/chaos/chaos_matrix.md`) lists "MinIO slow / high latency"
(S4), "slow disk I/O" (D5), and "MinIO slow + server crash" (X5) as **P2 and
unimplemented**.

This is exactly the gap issue #152 targets. We want a test that injects **network
jitter** across the key links and verifies the system behaves as expected under it:
**occasional internal errors are acceptable as long as retries recover** — but data
loss, permanent hangs, corruption, crashes, or retry storms are serious bugs.

## Goals

- Inject realistic **soft** network faults (latency + jitter, packet loss, transient
  partition blips) on Woodpecker's key links using **Chaos Mesh `NetworkChaos`** on a
  Kubernetes cluster.
- Drive a **sustained concurrent read/write workload** during injection and assert a
  fixed set of **invariants** (durability, ordering, CRC, liveness, bounded retry).
- Distinguish **acceptable degradation** ("eventual success after retry") from
  **serious bugs** (lost acked writes, permanent hang, corruption, crash, retry storm).
- **Reuse the existing minikube operator bring-up** (`deployments/operator/test/smoke-test.sh`
  steps 1–6) so the new work is "add chaos + add a chaos-aware verifier," not "build a
  whole k8s e2e from scratch."
- Run **nightly only** (hook into `nightly.yaml`), never as a PR-gating check.
- Build the Woodpecker image **from current source**, so the test exercises the code
  under review — not a released image.

## Non-Goals

- **Not a PR-CI check.** Chaos Mesh + minikube + sustained injection takes
  ~25–40 min/run and is inherently non-deterministic; gating PRs on it would be
  painful. Nightly cadence only.
- **No Toxiproxy / `tc` path.** Issue #152 lists Toxiproxy (Docker) as an alternative;
  we deliberately choose Chaos Mesh on k8s and do not build the Docker/Toxiproxy
  variant in this work.
- **No `server↔server` quorum/gossip link in v1.** Scoped to three links (below).
  Quorum-replication jitter is a fast-follow, explicitly deferred.
- **No physical multi-AZ topology.** Single-node minikube; `NetworkChaos` shapes per
  pod network namespace, so injection correctness is unaffected, but AZ separation is
  only logical, not physical.
- **No new fault types beyond delay+jitter / loss / blip in v1.** `corrupt`, `reorder`,
  `duplicate`, bandwidth throttle, asymmetric partition are deferred to the
  "full-link + full-fault" follow-up.
- **No change to production code** is *planned*. If the verifier surfaces a real bug,
  fixing it is separate follow-up work, not part of this test harness.

## Key Decisions (locked in)

| Decision | Choice | Rationale |
|---|---|---|
| Injection tool | **Chaos Mesh** (`NetworkChaos` CRD) | Purpose-built for delay+jitter+loss; closest to multi-AZ prod; user's explicit ask. |
| Cluster | **minikube**, `--container-runtime=containerd` | The repo's only k8s deploy path (`smoke-test.sh`) is already minikube; we reuse steps 1–6. containerd is the most reliably-supported runtime for the Chaos Mesh `chaos-daemon`. |
| Cadence | **Nightly only**, not PR CI | Slow + non-deterministic; hook into `nightly.yaml`. |
| Scope (links) | **server↔MinIO, server↔etcd, client↔server** | Covers the object-storage focus of #152 plus the metadata and RPC paths; quorum link deferred. |
| Scope (faults) | **delay+jitter, loss, partition blip** | The "jitter" surface #152 describes. |
| Orchestration | **Host-driven phases + in-pod Go verifier** (Approach ①) | Reuse proven bring-up; keep the hard invariant-checking logic in Go; chaos stays declarative YAML; no in-pod RBAC / client-go dependency. |

## Architecture

### Layer 1 — Cluster bring-up (reuse)

Reuse the bring-up from `deployments/operator/test/smoke-test.sh` (steps 1–6):

1. `minikube start` (bumped to **8 GiB / 6 CPU**, `--container-runtime=containerd`).
2. Build + deploy the operator (`make docker-build` / `make deploy`, image loaded via
   `minikube image load`).
3. Build the **Woodpecker server image from current source** and load it.
4. Deploy `etcd` and `minio` pods/services.
5. Create the `WoodpeckerCluster` CR (3 replicas) → operator creates the server
   StatefulSet; gossip cluster forms.
6. Verify all nodes healthy + gossip converged (admin API `/admin/node/status`).

To avoid duplicating this logic, the reusable steps are refactored into a sourceable
shared library (e.g. `deployments/operator/test/lib.sh`) parameterized by
`CLUSTER_NAME` / `CR_NAME` / `REPLICAS`; both `smoke-test.sh` and the new chaos
orchestrator source it. (Refactor-vs-invoke is a plan-time detail; the shared-lib
refactor is preferred and stays narrowly scoped to the bring-up steps.)

Known identifiers (from `smoke-test.sh`), used by the chaos selectors:

| Component | Pod label | Service / port |
|---|---|---|
| Woodpecker servers | `app.kubernetes.io/instance: my-woodpecker` | `my-woodpecker-server-headless`, gRPC `18080`, admin/healthz `9091` |
| etcd | `app: etcd` | `etcd:2379` |
| MinIO | `app: minio` | `minio:9000` |
| Client/workload | `wp-client-test` (pod name) | in-cluster, uses `*.svc` DNS seeds |

### Layer 2 — Chaos Mesh install

After bring-up, install Chaos Mesh via Helm into its own namespace, with the
`chaos-daemon` pointed at the containerd socket:

```
helm install chaos-mesh chaos-mesh/chaos-mesh \
  -n chaos-mesh --create-namespace \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock
```

**Injection smoke-check before the full run:** apply one minimal `NetworkChaos`
(e.g. delay on `client↔server`) and confirm from inside the client pod that observed
latency actually rises, then remove it. This proves the daemon/runtime wiring works
*before* we trust any pass/fail result. If injection is silently a no-op, every test
"passes" — this guard prevents that false-green.

### Layer 3 — Orchestration model (host-driven phases)

The orchestrator (`run_network_chaos.sh`) owns the **timeline**; the in-pod Go
program owns the **integrity logic**. For each scenario:

```
warmup (no chaos) → inject NetworkChaos → under-chaos window → remove chaos → recovery → verify
```

- The Go workload runs **inside** `wp-client-test` (it needs in-cluster `*.svc` DNS
  for the quorum seeds), driven by `kubectl exec`.
- Chaos is applied/removed from the **host** with `kubectl apply -f manifests/<scenario>.yaml`
  / `kubectl delete`. No client-go, no in-pod RBAC.
- The workload **persists every acked `(logId, entryId, payloadHash)`** to a file in
  the pod so the `recovery`/`verify` phase can read it back even across separate
  `go test` invocations.

## Fault-injection scenario matrix (v1)

Three links × fault types ≈ 7 scenarios. Each is one `NetworkChaos` manifest selected
by pod label + `direction`/`target`.

| # | Link | Fault | Key params | Stresses |
|---|---|---|---|---|
| N1 | server→MinIO | delay+jitter | `latency=300ms jitter=100ms correlation=50` | compaction/flush upload, read-from-object |
| N2 | server→MinIO | loss | `loss=10% correlation=25` | upload retry, partial-progress handling |
| N3 | server→MinIO | partition blip | `partition`, `duration=5s`, repeated | transient outage → compaction retry |
| N4 | server→etcd | delay+jitter | `latency=300ms jitter=100ms` | segment completion, session lease, CAS retry |
| N5 | server→etcd | partition blip | `partition`, `duration=5s` | metadata update retry, lease survival |
| N6 | client→server | delay+jitter | `latency=300ms jitter=100ms` | append/read RPC retry |
| N7 | client→server | loss | `loss=10%` | RPC retry, no spurious permanent error |

Representative manifest (N1, server→MinIO delay+jitter):

```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: server-minio-delay
  namespace: default
spec:
  action: delay
  mode: all
  selector:
    namespaces: [default]
    labelSelectors:
      app.kubernetes.io/instance: my-woodpecker   # server pods
  direction: to
  target:
    mode: all
    selector:
      namespaces: [default]
      labelSelectors:
        app: minio
  delay:
    latency: "300ms"
    jitter: "100ms"
    correlation: "50"
  # duration omitted → orchestrator controls lifetime via kubectl delete
```

`NetworkChaos` shapes traffic between the selected pods on the chosen `direction`; it
is link-scoped (server↔minio), not port-scoped, which is sufficient because each
target pod hosts exactly one relevant service.

## Workload + Verifier (the core)

A new Go program/test in `tests/chaos_mesh/workload/` drives the workload and runs the
assertions (reusing `tests/e2e_operator`'s config-loading / client-creation helpers
where useful). It is **chaos-aware via phases** but does not itself toggle chaos.

### Phases

1. **warmup** (no injection): N concurrent writers across M logs append continuously;
   tail readers read concurrently. Record every acked `(logId, entryId, payloadHash)`.
   Establish a healthy baseline (throughput, p50/p99 latency, error rate ≈ 0).
2. **under-chaos** (injection active): keep writing/reading. Assert **every operation
   eventually succeeds within a bounded retry/timeout budget**; no operation returns a
   **non-retryable permanent error** for a purely transient network fault; no operation
   hangs past the liveness bound. Elevated latency / raised retry counts / brief read
   stalls are expected and recorded, not failed.
3. **recovery** (injection removed): assert throughput/latency return toward baseline,
   then **full read-back** of all acked entries from phases 1+2 and verify
   durability / ordering / hash.

### Invariants (asserted; any violation = serious bug)

| # | Invariant | Check |
|---|---|---|
| I1 | Durability | every acked `(logId, entryId)` is readable post-test with matching `payloadHash` — **zero loss** |
| I2 | Ordering | within a log, returned entryIds strictly increasing; read-back order matches write order |
| I3 | CRC / integrity | app-level `payloadHash` matches on read-back (on top of Woodpecker's internal CRC) |
| I4 | Liveness / no hang | every op completes (success or definitive error) within a bounded wall-clock; no permanent hang/deadlock |
| I5 | No spurious permanent error | a transient network fault never surfaces as a non-retryable error to the client |
| I6 | No crash | no server pod panic / OOM / crashloop — `restartCount` stays flat through the run |
| I7 | Bounded retry / forward progress | injection-window throughput stays `> 0`; retries do not amplify into a storm |

### "Acceptable" vs "serious bug" (maps to the user's expectation)

- **Acceptable** ("偶发内部错误但重试能正常跑通"): raised error/retry rate, higher
  latency, brief read unavailability — **as long as operations eventually succeed and
  all invariants hold** (this is exactly phase-2's success condition).
- **Serious bug**: any I1–I7 violation. The harness fails the scenario and dumps the
  scene.

### Failure artifacts

On any violation, collect (mirroring the existing chaos CI's log-collection step):
server logs (all replicas), `kubectl get networkchaos -o yaml` + chaos events, pod
`restartCount`s, etcd/minio pod state, and the failing operation's record. Upload as a
nightly artifact.

## Deliverables & file layout

```
tests/chaos_mesh/
├── README.md                       # how to run locally + scenario descriptions
├── run_network_chaos.sh            # orchestrator: source bring-up lib + install chaos-mesh
│                                   #   + injection smoke-check + scenario loop + artifact collection
├── manifests/                      # one NetworkChaos per scenario (N1–N7)
│   ├── server-minio-delay.yaml
│   ├── server-minio-loss.yaml
│   ├── server-minio-blip.yaml
│   ├── server-etcd-delay.yaml
│   ├── server-etcd-blip.yaml
│   ├── client-server-delay.yaml
│   └── client-server-loss.yaml
└── workload/                       # Go workload + phase-based verifier (invariants I1–I7)

deployments/operator/test/lib.sh    # (refactor) shared bring-up steps, sourced by
                                    #   both smoke-test.sh and run_network_chaos.sh

.github/workflows/nightly-chaos-mesh.yaml   # nightly-only job (or a new job in nightly.yaml)
```

## Nightly CI integration

- A nightly job (new `nightly-chaos-mesh.yaml`, or a job added to `nightly.yaml`) on
  `ubuntu-latest` that: sets up minikube (containerd) → runs `run_network_chaos.sh` →
  uploads artifacts on failure.
- **Not** added to the PR `REQUIRED_CHECKS` list; no `ci-passed` gating.
- Generous `timeout-minutes` (e.g. 60) to absorb image build + 7 scenarios.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Chaos Mesh × minikube runtime/socket mismatch → injection silently no-ops | Use `--container-runtime=containerd` + matching `chaosDaemon.socketPath`; **injection smoke-check** (Layer 2) fails fast if injection isn't real. |
| Single-node minikube ≠ physical multi-AZ | Accepted; `NetworkChaos` is per-pod-netns so injection is still correct. Documented as a non-goal. |
| Runtime too long for nightly | ~25–40 min budget; scenarios are independent and can be trimmed/parallelized later; image build is the slow part and is cached. |
| Resource pressure (workload + chaos-daemon + 3 servers + etcd/minio) | minikube bumped to 8 GiB / 6 CPU. |
| Flakiness misread as a bug | Thresholds (latency/loss/timeout budgets) are explicit and tunable; phase-2 only fails on *invariant* violations, not on elevated-but-recovering metrics. |

## Acceptance criteria

1. `run_network_chaos.sh` brings up the cluster, installs Chaos Mesh, passes the
   injection smoke-check, and runs scenarios N1–N7 end to end on a clean machine.
2. Under each scenario, the verifier asserts invariants I1–I7 and reports per-scenario
   pass/fail with metrics (error rate, retry counts, p50/p99 latency, throughput).
3. On failure, scene artifacts are collected.
4. A nightly workflow runs the suite and is **not** a PR-gating check.
5. The harness has actually been **run locally** at least once and either confirms
   "only transient errors, retries recover, all invariants hold" or surfaces concrete
   bug candidates with reproductions.

## Out of scope / future work

- `server↔server` quorum-replication and gossip jitter.
- Additional fault types: `corrupt`, `reorder`, `duplicate`, bandwidth throttle,
  asymmetric/partial partition.
- Chaos Mesh `Workflow`/`Schedule` CRDs for declarative multi-fault timelines.
- A Toxiproxy/Docker variant for fast local/PR-CI smoke (the #152 alternative).
- Physical multi-node minikube/kind for true topology separation.
