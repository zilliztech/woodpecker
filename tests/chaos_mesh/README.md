# Chaos Mesh — Network-Jitter Fault Injection (Nightly)

This suite injects **soft network faults** (latency+jitter, packet loss, transient partition blips) on Woodpecker's three key links — server↔MinIO, server↔etcd, and client↔server — using **Chaos Mesh `NetworkChaos`** on a minikube cluster. A phase-based Go workload runs concurrent reads and writes throughout each scenario while a verifier asserts a fixed set of invariants: acked writes are never lost, ordering is preserved, CRC matches on read-back, no server crashes, and retry counts stay bounded. Elevated latency and transient errors that recover are acceptable; any invariant violation (I1–I7) is a serious bug.

**Nightly only — never a PR-gating check.**

## Prerequisites

- [minikube](https://minikube.sigs.k8s.io/) (docker driver; uses the docker runtime by default)
- docker
- kubectl
- helm
- go 1.24+

## How to Run

```bash
./tests/chaos_mesh/run_network_chaos.sh
```

Pass `KEEP=1` to leave the cluster running after the run for manual inspection:

```bash
KEEP=1 ./tests/chaos_mesh/run_network_chaos.sh
```

The orchestrator sources the shared cluster bring-up library at
`deployments/operator/test/lib.sh` (minikube start, operator deploy, Woodpecker CR
creation, gossip convergence check — reusing the same steps as `smoke-test.sh`), then
installs Chaos Mesh via Helm:

```bash
# chaosDaemon.runtime/socketPath follow the minikube runtime (docker by default):
helm install chaos-mesh chaos-mesh/chaos-mesh \
  -n chaos-mesh --create-namespace \
  --set chaosDaemon.runtime=docker \
  --set chaosDaemon.socketPath=/var/run/docker.sock
```

> The suite defaults to minikube's **docker** runtime. `--container-runtime=containerd`
> makes minikube deploy a kindnet CNI whose image often can't be pulled on constrained
> hosts (no pod networking); set `MK_RUNTIME=containerd` only where that image is reachable.

An injection smoke-check (a brief delay applied to the client→server link, latency
measured from inside the pod, then removed) runs before the scenario loop to confirm
`chaos-daemon` is actually shaping traffic — a silent no-op would otherwise make every
test appear to pass.

## Scenario Matrix (N1–N7)

| # | Link | Fault | Key params | Stresses |
|---|---|---|---|---|
| N1 | server→MinIO | delay+jitter | `latency=300ms jitter=100ms correlation=50` | compaction/flush upload, read-from-object |
| N2 | server→MinIO | loss | `loss=10% correlation=25` | upload retry, partial-progress handling |
| N3 | server→MinIO | partition blip | `partition`, `duration=5s`, repeated | transient outage → compaction retry |
| N4 | server→etcd | delay+jitter | `latency=300ms jitter=100ms` | segment completion, session lease, CAS retry |
| N5 | server→etcd | partition blip | `partition`, `duration=5s` | metadata update retry, lease survival |
| N6 | client→server | delay+jitter | `latency=300ms jitter=100ms` | append/read RPC retry |
| N7 | client→server | loss | `loss=10%` | RPC retry, no spurious permanent error |

Each scenario follows the phases: **warmup** (no chaos) → **inject** → **under-chaos window** → **remove chaos** → **recovery** → **verify**.

## Invariants (I1–I7)

Any violation is a serious bug; the harness fails the scenario and collects artifacts.

| # | Invariant | Check |
|---|---|---|
| I1 | Durability | every acked `(logId, entryId)` is readable post-test with matching `payloadHash` — zero loss |
| I2 | Ordering | within a log, returned entryIds strictly increasing; read-back order matches write order |
| I3 | CRC / integrity | app-level `payloadHash` matches on read-back (on top of Woodpecker's internal CRC) |
| I4 | Liveness / no hang | every op completes (success or definitive error) within a bounded wall-clock; no permanent hang/deadlock |
| I5 | No spurious permanent error | a transient network fault never surfaces as a non-retryable error to the client |
| I6 | No crash | no server pod panic / OOM / crashloop — `restartCount` stays flat through the run |
| I7 | Bounded retry / forward progress | injection-window throughput stays > 0; retries do not amplify into a storm |

Elevated latency, raised retry counts, and brief read stalls during the under-chaos
window are **acceptable** — operations are expected to eventually succeed. Only I1–I7
violations trigger a failure.

## Layout

```
tests/chaos_mesh/
├── README.md                 # this file
├── run_network_chaos.sh      # orchestrator: source lib.sh, install Chaos Mesh,
│                             #   smoke-check, run N1–N7, collect artifacts
├── manifests/                # one NetworkChaos CR per scenario (N1–N7)
│   ├── server-minio-delay.yaml
│   ├── server-minio-loss.yaml
│   ├── server-minio-blip.yaml
│   ├── server-etcd-delay.yaml
│   ├── server-etcd-blip.yaml
│   ├── client-server-delay.yaml
│   └── client-server-loss.yaml
└── workload/                 # phase-based Go workload + verifier (invariants I1–I7)
                              #   part of the root github.com/zilliztech/woodpecker
                              #   module — no separate go.mod
```

`manifests/` and `workload/` are currently empty (`.gitkeep` placeholders); they will
be populated by subsequent tasks.

The workload binary runs **inside** the `wp-client-test` pod (it needs in-cluster DNS
for the quorum seeds); chaos is applied and removed from the **host** via
`kubectl apply/delete` — no client-go dependency, no in-pod RBAC.

## Further Reading

Detailed design and implementation plan live in:

- `docs/superpowers/specs/2026-06-04-chaos-mesh-network-jitter-design.md`
- `docs/superpowers/plans/` (implementation plan)
