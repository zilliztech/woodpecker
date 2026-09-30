# Service Stability (K8s)

K8s tier of the service stability suite, run nightly (`.github/workflows/nightly-service-stability-k8s.yaml`). It uses:
- minikube, the operator, and a 5-pod WoodpeckerCluster (ensemble 3), so one pod down still leaves enough nodes for new segments;
- Chaos Mesh;
- a client pod running `workload/`.

A deleted pod keeps its DNS name and gets a new IP, as in production. A killed one restarts in place, as after an OOM kill.

"Kill" is a SIGKILL of the server process: no gossip leave, no drain.
- Deleting the pod, even with `--grace-period=0 --force`, is not a kill. The kubelet still sends SIGTERM first, and the server handles SIGTERM with a graceful stop.
- PID 1 is tini, which the kernel does not let a SIGKILL from inside the pod reach. So the server process itself is killed.

| Case | Fault (by `run_service_stability.sh`) |
|---|---|
| `Baseline` | none |
| `RestartIdlePod` | `kubectl delete pod` of a pod in no writable quorum |
| `RestartQuorumPod` | `kubectl delete pod` (graceful) of the busiest quorum pod |
| `KillQuorumPod` | SIGKILL of the server process (`pkill -KILL -x woodpecker`); the container restarts in place |
| `KillQuorumPod_NeverReturns` | SIGKILL, then PodChaos `pod-failure`, lifted only after the workload has finished |
| `KillQuorumPod_Rescheduled` | SIGSTOP the server, then force delete the pod. It dies without a leave and comes back with a new IP. The time until every peer readmits it is written to `artifacts/KillQuorumPod_Rescheduled/readmission.txt`; over `READMIT_BUDGET` (25s by default) fails the case (#395). |
| `VanishQuorumPod` | NetworkChaos `partition` (both directions, every pod) for 10s |
| `VanishReadPod_SeparateReader` | the same on the pod the tail readers use, readers in their own client |
| `RollingRestartAllPods` | graceful delete of every pod, highest ordinal first, waiting for Ready and gossip |

Each case is `TestServiceStabilityK8s_<Case>` in `workload/`:
1. The test starts the workload and writes the address of the pod to fault to `<state-dir>/<Case>.target`.
2. The script injects the fault, waits for every pod to be Ready and for memberlist to reconverge, then touches `<Case>.done`.
3. The test stops the workload and checks it.

The checks are those of the other tiers. Completeness and order always fail the run. The stall budget is reported, not enforced (`WP_STABILITY_LATENCY=report`), until nightly baselines exist; set `WP_STABILITY_LATENCY=enforce` to fail on it.

```bash
./run_service_stability.sh                               # all cases, then delete the minikube profile
CASES="KillQuorumPod VanishQuorumPod" KEEP=1 ./run_service_stability.sh
make integration-test-service-stability-k8s
```

Per-case logs (workload, servers, chaos objects, events) are written to `artifacts/<Case>/`.
