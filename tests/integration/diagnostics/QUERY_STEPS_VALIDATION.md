# Query scheduling/execution deployed acceptance — 2026-10-28

This [#490](https://github.com/zilliztech/woodpecker/issues/490) receipt validates
the existing query stage on real successful FLAT/L2 search work, separately from
the completed Customized TSafe case. [QUERY_STEPS.md](QUERY_STEPS.md) covers
reproduction, bounds and original-owned cleanup/recovery.

## Fixture and provenance

- Accepted run `20261028T034205Z-c06cfd79`, UTC
  `2026-10-28T03:42:05.919604Z`–`03:51:07.231303Z`; exit **0**,
  `passed=true`, no failures, collection dropped and original lock released.
- Base master `e48504fb43c3cf7c870a257357ec7626557615af` (merged #487), independent
  of pending #494/#495/#496. Production diagnosis, policies, dependencies and
  deployment configuration were unchanged.
- Existing externally managed context/namespace `wp-diag-458`; Kubernetes
  v1.33.1, Docker 28.1.1, Linux arm64, four CPUs/6144 MiB. The existing example
  Prometheus supplied actual 15-second scrapes. Cluster lifecycle was external:
  the task resumed the initially stopped dedicated profile and stopped it again
  after cleanup. The harness never performs cluster/Helm lifecycle actions.
- Milvus image
  `harbor-us-vdc.zilliz.cc/milvusdb/milvus:enhance-woodpecker-v0.1.46-3.0-20261012-27bd28d206-v2-arm64`,
  actual image ID
  `56f66c0b875ae2a49ff54a2b1f43e81441a92d3cecc9d938c68e17434b51dea2`.
  Three WP servers used `milvusdb/woodpecker:v0.1.46`, actual image ID
  `4bef58cf01716909e6b2ab2093eb06c98489d8980b9822f4a132ca8e27d2d5f2`.
- Namespace UID `e39652ca-1bdb-4cdf-b987-f60e0f916941`; lock UID
  `0116ec49-0582-4e1e-aadc-2bd40017cae6`; collection ID
  `470065155878896386`, with exact run-owned schema description.
- Core Pod UIDs, images, restart counts, resources and mounted ConfigMap
  fingerprints were identical before/after. Existing restart counts were 10–12
  on Milvus and four on WP; none increased during the measured run. The mounted
  `milvus` ConfigMap UID was `52739102-922c-4c3c-bfd9-0acd2bfc19c8`, resource
  version `10056`, data SHA-256
  `2fc303771ab68f6acfd473faa70b8a555dc4f6fe019ef988f3c0ca47964da560`.

The isolated official protobuf module used go-api/v3 v3.0.0, gRPC v1.80.0,
protobuf v1.36.11 and Go 1.25.0. Python 3.9 ran the harness. No production Go
module/dependency file or previous TSafe client was modified.

## Workload and actual outcomes

```sh
python3 tests/integration/diagnostics/query_steps_acceptance.py \
  --context wp-diag-458 --namespace wp-diag-458 \
  --queryclient /tmp/wp-query-steps-490-v3 \
  --output /tmp/wp-query-steps-490-live --execute
```

The owned collection contained **65,536 distinct acknowledged IDs and 256-D
float32 vectors**, 64 MiB of raw vector bytes. All 128 insert batches checked
exact IDs/counts and nonzero server timestamps. Flush completed at timestamp
`470065854829298666`; index `vector_flat` reported `FLAT`/`L2`, Finished. The
physical loaded snapshot showed one Sealed segment `470065155878896415`, all
65,536 rows, on QueryNode **40**. Searches used Strong consistency, no future
fence, `id >= 0`, topK=1 and exact deterministic vector self-match.

| Phase | NQ / concurrency | Actual successful search requests | Client P95 | Qualified metric observations |
| --- | --- | --- | --- | --- |
| Baseline | 1 / 1 | 96 | 44.198 ms | Proxy/core P95 bounds 32–64 ms; queue 0–0.1 ms; TSafe 0–1 ms |
| Idle | no search work | observed zero completions | no distribution inferred | Existing search population reported idle, not missing/healthy-zero substitution |
| Execution | 512 / 1 | 43 | 3,068.395 ms | Core and Sealed segment P95 bounds 2,048–4,096 ms; normal same-QN queue; zero ready backlog |
| Scheduling | 256 / at most 8 | 90 over complete offering/drain | 12,260.727 ms | Selected retained interval: 42 queue completions, P95 bounds 8,192–16,384 ms, ready 4–5 tasks, latest 5 |
| Recovery | 1 / 1 | 96 | 38.511 ms | Proxy/core P95 bounds 32–64 ms; queue 0–0.1 ms; TSafe 0–1 ms |

These are controlled workload observations, not a normal production benchmark.
Client latency includes serialization, transport and port-forward overhead.
P95 metric values are histogram **bucket bounds**, not exact samples or values
to add/subtract. The independent default policies remained queue 100 ms, core
and segment 500 ms, TSafe 250 ms, and Proxy 1,000 ms, with minimum twenty
observed completions.

Actual selected windows:

- Baseline: `03:42:39.263756Z`–`03:43:15.159232Z`.
- Idle: `03:43:31.578195Z`–`03:44:33.629340Z`.
- Execution: `03:44:50.051925Z`–`03:47:22.411334Z`.
- Scheduling: `03:47:56.060645Z`–`03:49:11.169112Z`.
- Recovery: `03:50:21.960329Z`–`03:50:57.896619Z`.

Execution emitted existing `suspected_execution_work` and
`observed_slow_segment_work`. QueryNode 40 (`10.244.0.108:9091`) had normal
queue P95 bounds 0.1–0.25 ms and ready first/minimum/peak/latest all zero.
StreamingNode/delegator 38 (`10.244.0.110:9091`) independently exported TSafe
P95 bounds 0–1 ms over 43 completions. That timer was not falsely required to
exist on the sealed QueryNode, nor joined to an individual request.

Scheduling emitted existing `suspected_scheduling_pressure`: the original
qualified queue finding referenced the same exporter as its actual ready gauge.
Both facts had observed sample bounds `1793159290215`–`1793159350164` ms since
epoch, approximately 60 seconds within the requested 75.108-second interval.
Every retained ready sample was positive (minimum 4, peak/latest 5). Priming
probes before this interval included zero and are not counted as pressure proof.
No continuous between-scrape assertion is made.

The complete scheduling worker offered/completed 90 requests over its bounded
120-second offering and drain. Only **42 queue completions** fell in the selected
retained metric population; Proxy had 43 and TSafe had 42. These are different
timer populations/windows, not ninety matched requests or a one-to-one join.
Concurrent requests also incurred execution work; the scheduling finding does
not claim queueing was their exclusive cause or establish configured capacity
from an executing-concurrency gauge.

## Recovery, integrity and uncertainty

All searches verified every NQ result's expected ID, topK and finite near-zero
L2 self-distance. Before and after the contrasts, all **65,536 IDs and original
vector bytes** were checked in 128 bounded range queries, with duplicate/extra
and outside-range checks. The final verification completed all 128 requests.
The same collection stayed Loaded in sampled observations; no cold loading,
TSafe future fence, shared tuning, process freezing or resource change was used.

Both original incident windows were re-evaluated after normal recovery and
retained their qualified findings. Standalone and integrated diagnoses matched
stable semantics, targets and unknowns. All supporting/derived references
resolved to original source observations/findings; integrated summaries retained
their original child IDs. Fresh report/observation IDs from separate commands
were not treated as identical observations. No timer quantiles were added/subtracted.

Controls distinguished:

- Absent series: exit 0, completed collection, baseline qualification rejected.
- Wrong instance `192.0.2.1:9091`: exit 0, completed, baseline rejected.
- Unreachable source: exit 1, failed collection, baseline rejected.

The workload cannot establish a Milvus collection/channel/WP-client metadata/log
association or implicate WP storage. It does not validate every query plan,
trace/Loki/native/WebUI integration, production SLO or crash durability. Exact
per-request and exclusive hardware-cause claims remain unavailable. Missing,
reset, sparse or incompatible evidence stays explicit under the existing source
rules; actual restart/scrape-gap coverage remains #493.

Cleanup verified the original schema description and collection ID, dropped only
that collection, checked absence, and deleted the original lock with a UID
precondition. An independent Kubernetes read confirmed the lock was absent.
The task then returned the dedicated profile to its initial stopped state.
The accepted local artifacts total approximately **14.26 MB**, below 96 MiB;
raw/private connection files and workload artifacts are not committed. Physical
object/log reclamation timing after collection drop is not asserted.

## Retained failed attempts

| Run | Actual result and correction |
| --- | --- |
| `20261028T031602Z-fa4e18ad` | NQ 8192 serial searches all succeeded, approximately 37–44 seconds each, but only four completed. Workload qualification failed before a positive source claim; owned collection/lock were cleaned. NQ was reduced, without changing any diagnostic policy. |
| `20261028T033251Z-ce9e522e` | NQ 512 produced 43 correct successful searches, qualified slow core/Sealed work and normal QN scheduling. A harness assertion wrongly required the independent SN TSafe timer on the QN exporter; the attempt stayed failed and was cleaned. Source evidence established the actual SN/QN boundary; the assertion was corrected and regression-tested. |
| `20261028T034205Z-c06cfd79` | Complete final baseline/idle/execution/scheduling/recovery/history/negative-control run passed and cleaned. |

Failed receipts remain unchanged in `/tmp/wp-query-steps-490-live/`. They are
not relabeled successful, treated as transport failures when RPCs succeeded,
or used to lower source thresholds. Protocol-only cancellation and ownership
counterexamples remain separately labeled offline tests.

## Fingerprints and verification

| Executed binary/source | SHA-256 |
| --- | --- |
| wpdiag | `85972164226dccb90a6cc6742247702afa2e52e7b08dd86ace1719bb26c9a32b` |
| query-steps worker | `42eb22f6b016b681efcde814bbe3ba69ef227c408e4bf82b2c04a2e254e476ed` |
| query_steps_acceptance.py | `0f3dbcc635abec2d9be393a421bffdcbb964aafa10d229b45fd3e416ee3eb8e5` |
| shared cli_acceptance.py | `8dc625af222fb3ee2c3666ffa483b53de41fac97fe61bd45f5719b1565011313` |
| shared query_acceptance.py | `027be354727fa50526979fe64261939ee855facde0e5849c4374c0f5157399ef` |
| worker steps/main.go | `6bd288235656221849da825b64def1d3718051e5256119b67c706eee28c84d8f` |

| Local raw artifact | SHA-256 |
| --- | --- |
| acceptance.json | `bb29b2a6d1f1d3f9e548b7ba92b9cf61f493af511883d5d22c2fb5f603d4224c` |
| execution.query.json | `e3de36c437968242c71542ae2e9b495cb8eb437b9e1cad380afba356dc0bfeb0` |
| scheduling.query.json | `df7b8cbea4e90f41013a5cae4751f73b155f09511b03bd09ac9ff710f7e8789d` |
| execution_historical_after_recovery.query.json | `ebf6229ea904dbe1d9e7a9c6b47681c3975065e5e076a0261b8b7a06c032b83b` |
| scheduling_historical_after_recovery.query.json | `349b00b4ff92d97d7140c91ad469e931b91a9a541556a2af1cea4b3a6426f457` |
| final-verify.workload.json | `28c99df1bdeff779b071d0a184750882f37e54b1528cdc05c9d6730002cd047a` |

Offline checks passed: **15 Go race tests** in the isolated protobuf module
(seven original TSafe plus eight new workload/protocol/bounds/cancellation tests),
**59 Python acceptance regressions** (including fourteen new source/lineage/
ownership/recovery cases), and targeted golangci-lint. Documentation links,
generated-site idempotence and whitespace checks are recorded with the PR.
