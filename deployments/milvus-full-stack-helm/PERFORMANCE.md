# Write performance and operational diagnosis

The normal REST runner now records a W3C `trace_id`, sampling decision and Jaeger
link for every insert; `summary.md` links the slowest sampled requests. Use
`PERF_TRACE_SAMPLE_FRACTION` (0..1, default 1) to control client sampling. Jaeger
history is ephemeral, and the supplied image's Insert trace does not yet connect
through SN/WP. Read [TRACING.md](TRACING.md) before comparing tracing-on/off
performance or treating an incomplete trace as a latency breakdown.

`perf` makes a repeatable write experiment part of the full stack. It drives the
real Milvus REST endpoint, so writes pass through Milvus and the embedded WP client
to the independent Woodpecker services. It collects evidence on successful and
failed runs and returns nonzero for write/correctness failures, failed performance
gates, or diagnostic collection errors.

## Run locally

Core-only `perf` still verifies writes, queries/search and retains request/Pod-log evidence.
Metrics and alert snapshots are explicitly recorded as skipped when their backend is
not selected. Install `prometheus,grafana,alertmanager` for the full metric/alert
workflow, and `jaeger` for trace lookup; selected but unreachable backends remain failures.

With the stack deployed, keep `./stack.sh forward -x` running in another terminal:

```bash
./stack.sh perf -x
```

Defaults: 5 seconds of warm-up, 60 seconds of measurement, two workers, batches of
100 vectors with 128 dimensions, and a total target of five insert requests/second
(500 rows/second). A 35-second observation period follows the writes to allow final
samples to reach Prometheus. Expect about two minutes including evidence collection.

The test creates its own collection. It does not overwrite existing collections.
Both the collection and the reports are retained for inspection; `uninstall` removes
the collection with the stack's stored data, while local reports survive.

To change the load or run a longer observation window:

```bash
PERF_DURATION=300 PERF_CONCURRENCY=4 PERF_BATCH_SIZE=200 PERF_RPS=10 ./stack.sh perf -x
```

The rate is a **total batch request rate**, not a per-worker rate. At most the
configured number of workers have requests in flight. A slower server or load
generator may achieve less than the target. Scheduling lag and vector generation
time are reported separately from the insert RPC latency. The runner does not
build an unbounded request queue or silently retry failed writes: a timed-out
insert might already have committed.

## Parameters and gates

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `PERF_DURATION` | `60` | Measurement seconds |
| `PERF_WARMUP` | `5` | Warm-up seconds, excluded from measurement statistics |
| `PERF_COOLDOWN` | `35` | Final observation seconds for scraping |
| `PERF_CONCURRENCY` | `2` | Maximum in-flight insert workers |
| `PERF_BATCH_SIZE` | `100` | Vectors per insert request |
| `PERF_DIMENSION` | `128` | Vector dimension |
| `PERF_RPS` | `5` | Total target insert requests/second |
| `PERF_MAX_REQUESTS` | `10000` | Request cap per phase |
| `PERF_MAX_ERRORS` | `5` | Stop scheduling after this many failed requests; in-flight requests finish |
| `PERF_REQUEST_TIMEOUT` | `30` | Timeout seconds per Milvus REST request |
| `PERF_SAMPLE_INTERVAL` | `10` | Seconds between metric/AM snapshots |
| `PERF_MIN_ROWS_PER_SECOND` | `0` | Minimum acknowledged measurement throughput; zero disables this gate |
| `PERF_MAX_P99_MS` | `0` | Maximum successful insert p99; zero disables this gate |

These defaults establish a small local baseline, not maximum cluster capacity or
a production SLO. Enable your own gates after measuring the selected hardware and
image. For example, the following are explicit test inputs rather than recommended
production thresholds:

```bash
PERF_MIN_ROWS_PER_SECOND=400 PERF_MAX_P99_MS=500 ./stack.sh perf -x
```

`MILVUS_URL`, `PROMETHEUS_URL`, `ALERTMANAGER_URL`, and their existing port overrides
are honored. Kubernetes collection uses `CONTEXT` and `NAMESPACE`; output follows
`STATE`. The default example uses unauthenticated local endpoints. For the full
Python options, run `python3 scripts/perf.py --help`.

## What gets verified and recorded

Each phase records acknowledged rows/second, successful batch requests/second,
failed requests, and nearest-rank p50/p95/p99 insert latency. Latency includes JSON
serialization and REST transport, including port-forward overhead. Vector
generation and scheduling delay are separate distributions. Warm-up and
measurement have separate statistics and UTC start/end times.

After writing, strong-consistency reads check the exact row count when every
insert was acknowledged. Sampled first/last IDs across batches are queried, and
a sample vector is searched with its ID filter. If a request failed, acknowledged
rows are only a lower bound on committed rows; the test still fails regardless of
the subsequent count. These checks establish live visibility, not durability
through a crash, restart or storage fault.

The report checks for positive write-counter changes at the Milvus proxy, WP
client and WP server. Milvus row metrics select this collection; WP/resource
metrics cover the deployment and can include other workloads. Counter changes,
histograms and correlated logs do not provide an end-to-end distributed trace.

Other signals include Milvus mutation, WP append, file flush and gRPC latency,
pending appends, disk usage/backpressure, object-store latency/errors, pod CPU and
memory, and current/historical alert states. Resource queries prefer container
metrics and fall back to pod cgroups when local cAdvisor lacks container labels,
without summing both. An absent lazy error counter or an idle histogram is
recorded as a coverage gap rather than interpreted as a zero/healthy value.
An empty `ALERTS` result is expected when no rule is pending/firing; it is recorded
as `alert_states_observed: false`, not an exporter coverage gap.

Diagnostic API/query/log errors fail the test and remain visible separately from
write results. An entirely missing required write-counter change also fails it.
Optional signals without finite samples are listed under `coverage_gaps` and can
require an exercised operation, instrumentation or enough rate-window history.

## Evidence for humans and agents

Each run has a unique directory under `.local/perf/<UTC-run-id>/`:

| File | Purpose |
| --- | --- |
| `summary.md` | Human-readable result, investigation leads and dashboard links |
| `report.json` | Versioned machine-readable parameters, phase statistics, collection metadata, correctness, gaps, failures and triage |
| `requests.jsonl` | One record per insert attempt, including local request ID, phase, worker, row-ID range, timestamps, latency and error |
| `signals.jsonl` | Periodic metrics and Alertmanager snapshots |
| `metric-*.json` | Prometheus range results with the exact query, from one minute before the run through final observation |
| `pods.json` | Pod identities, images, resource settings and status; excludes Pod environment values |
| `events.txt` | Kubernetes events for the selected namespace |
| `*.log` | Milvus/WP/etcd/MinIO logs since the run began, with timestamps; up to 2000 lines / 2 MiB per container |
| `*-previous.log` | Previous container logs where a restart was observed |

Dashboard links include all four [E2E views](E2E-DASHBOARDS.md) and three reference
dashboards, with the run's time window and namespace/collection selection where
the dashboard supports that filter. E2E channel/log scopes are independent; a
collection selection does not automatically isolate its WP log.
Local request IDs help join test records; the runner does not claim they are
propagated as server trace IDs. Use the collection ID from `collection_info`, log
and node identities in metrics, and timestamps when narrowing server-side evidence.
Logs from a pod deleted during the run may be unavailable without centralized log
storage; collection errors report that limitation. Review logs before sharing.

## Investigation workflow

Start with `summary.md`, then the phase statistics and `failures` in `report.json`.
The `triage` entries are investigation leads supported by observed signals; they
do not assert root causes or prove that an alert's hold period was satisfied.

| Observation | Investigate next |
| --- | --- |
| Insert errors or long REST latency | Request timestamps/errors, proxy and streaming node logs, mutation latency, WP client append latency |
| WP pending append queue grows | Client backpressure, replica availability, per-node RPC and flush latency; C3 |
| One node has high flush latency | Node resource pressure, local disk and concurrent compaction; F4/F5/S4 |
| Disk admission is hard-blocked | Disk watermarks, object-store health and compaction/reclamation progress; R5 |
| Object-store errors/latency grow | MinIO logs, connectivity/authentication, compaction and local WAL growth; F7/F8/F9 |
| Writes succeed but a chain counter does not change | Scrape targets, exporter families and deployment/collection labels |
| Target throughput is missed with high schedule/generation time | Load generator CPU, serialization, concurrency and REST/port-forward overhead |

For additional alert-path validation and wider diagnostics:

```bash
./stack.sh alerts
./stack.sh alerts-smoke -x
./stack.sh collect
```

Performance testing alone does not exercise every fault, every reference alert or
its 1–15 minute hold period. Controlled fault injection and restart/recovery
scenarios can build on these artifacts; they are not run automatically here.

## Runner tests

```bash
python3 tests/perf.test.py
```

These tests use loopback HTTP services and a fake kubectl. They cover concurrent
unique IDs across warm-up/measurement, count/search checks, HTTP-200 business
errors without retry, missing acknowledged data, missing monitoring, latency gate
failure, and evidence retention. They do not stop or mutate the real stack.

## Local validation, 2026-10-19

On the supplied ARM64 Milvus/WP images in single-node minikube, the default run
acknowledged 30,000 measurement rows in 60 seconds (499.96 rows/s at the configured
500-row/s target), with zero failed batches. Insert p50/p95/p99 were approximately
47.48/88.58/96.37 ms. Including 2,500 warm-up rows, strong reads found all 32,500
rows; 110 sampled IDs and the filtered vector search passed. All three write
counters advanced, and 10 business/dependency Pod logs were collected. The runner
used the Pod-cgroup resource fallback. The lazy object-store error counter had no
finite samples; that remains an explicit coverage gap.

A separate 1,000-row, five-second run used an intentionally impossible 0.001-ms
p99 gate. Writes and correctness passed, the CLI exited 1 for that gate failure,
and diagnostic collection still completed without errors. Six fake-service tests
and seven lifecycle CLI tests passed. These are local baseline/runner checks,
not maximum-throughput, production SLO, or fault-recovery results.
