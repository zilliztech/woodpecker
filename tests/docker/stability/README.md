# Service Stability (Docker)

Docker tier of the service stability suite. It runs the same cases as the process tier (`tests/integration/service_stability_process_test.go`) against a 4-node Docker Compose cluster. Faults are made with docker itself:

| Case | Fault |
|---|---|
| `Baseline` | none |
| `RestartIdlePod` | `docker stop -t 10` + `start` of a node in no writable quorum |
| `RestartQuorumPod` | `docker stop -t 10` + `start` of the busiest quorum node (gossip leave, drain) |
| `KillQuorumPod` | `docker kill`, started again after 10s (no gossip leave) |
| `KillQuorumPod_NeverReturns` | `docker kill`, not started while the workload runs |
| `VanishQuorumPod` | `docker pause` for 10s: connections stay open and nothing answers |
| `VanishReadPod_SeparateReader` | `docker pause` of the node the tail readers use, readers in their own client |
| `RollingRestartAllPods` | `stop` + `start` of every node, highest first, waiting for gossip to reconverge |

The workload and checks are shared (`tests/utils/stability`):
- 3 logs, each appending every 20ms with retries, as Milvus does, plus a tail reader.
- Every append must complete within 2s end to end (`WP_STABILITY_MAX_STALL`), plus 2s for the vanish cases.
- Every acked entry must reach the reader once, in order, at its acked position, within the same budget.

A restarted node keeps its host-mapped address here. The window where a pod's name still points at an address nothing answers on is covered by the process tier (proxies) and the K8s tier (a new pod IP).

```bash
./run_service_stability.sh                     # build image if missing, start cluster, run, clean up
./run_service_stability.sh -run TestServiceStabilityDocker_KillQuorumPod --no-cleanup
make integration-test-service-stability-docker
```

The tests assert latency, so they run without `-race`.
