# Independent Milvus query/search diagnosis

`wpdiag milvus query` implements the bounded initial stage catalog from
[#466](https://github.com/zilliztech/woodpecker/issues/466). It identifies observed
slow steps, their supporting/contradicting evidence and remaining gaps using
only a [Prometheus connection](../prometheus/README.md). No WP Admin, trace,
Loki, integrated runner or WebUI is required. Collection is read-only.

## Run

Use your existing `sources.prometheus.connections.stack` in wpdiag.yaml:

```bash
make wpdiag
bin/wpdiag capabilities
bin/wpdiag milvus query --list
bin/wpdiag milvus query --list -o json

# Replace UTC bounds with a retained interval for your workload.
bin/wpdiag milvus query --config wpdiag.yaml --connection stack \
  --scope milvus --source prometheus --mode historical \
  --from 2026-10-26T01:00:00Z --to 2026-10-26T01:05:00Z

bin/wpdiag milvus query --config wpdiag.yaml --connection stack \
  --scope e2e --mode historical \
  --from 2026-10-26T01:00:00Z --to 2026-10-26T01:05:00Z -o json > query.json

# Observe starting now, then evaluate retained scrapes.
bin/wpdiag milvus query --config wpdiag.yaml --connection stack \
  --window 2m --interval 30s --budget 3m
```

Flow is fixed to `read`, stage to `query`, and scope to `milvus|e2e`.
The only implemented `--source` is Prometheus; other requested sources fail
explicitly. Snapshot and collection/channel/log/segment/reader/node filters
are unsupported. Supported target filters are the deployment binding and exact
exporter instance. Selecting a single Proxy instance leaves consumer steps
uncovered; no other process addresses are guessed. Use deployment scope for
the whole overview.

Every scope=e2e assessment includes `wp_visibility_mapping_unavailable`: this
catalog collects no authoritative, time-valid collection/channel/WP-client
metadata association. A query may execute without reading WP WAL. Query/TSafe
delays do not establish a WP problem. Optional sources are not queried or counted
as failed services. Mapping/enrichment remains a later extension.

## Five groups and hypotheses

All histogram signals use milliseconds. A high-latency source finding requires
sufficient observed completions, valid temporal coverage and a P95 bucket lower
bound above the configured source policy. Empty/idle, reset, stale, sparse,
unsupported or unresolved buckets stay inconclusive. These source rules are
reused, not implemented again by the stage assessor.

| Group | Evidence / initial hypotheses | Exclusions and next checks |
| --- | --- | --- |
| 1. Proxy completion | `milvus_proxy_sq_latency`: observed slow successful completions | Excludes SDK/network round trip, failures and pending work. Check client latency/error counts and query type. |
| 2. Visibility / handler | `milvus_querynode_sq_wait_tsafe_latency`, `sq_req_latency`, `consume_tt_lag_ms`: suspected visibility wait; handler and collection TimeTick observations | Check requested consistency and authoritative consumption mappings. Collection TT lag is not the query's required timestamp or a WP binding. |
| 3. Scheduling | `milvus_querynode_sq_queue_latency`, `read_task_ready_len`, `read_task_concurrency`: slow scheduling, persistent/intermittent backlog or recovery | Compare actual overlapping observations on the same exporter. Executing concurrency is not configured capacity and does not prove CPU saturation. |
| 4. Execution / loading | `milvus_querynode_sq_core_latency`, `sq_segment_latency`, `load_segment_latency`: observed core/per-segment/loading cost | Inspect plan, NQ/topK, growing/sealed segments and cold loading. Different timer populations cannot be added. |
| 5. Result return | `milvus_proxy_sq_wait_result_latency`, `milvus_querynode_sq_reduce_latency`, `milvus_proxy_sq_reduce_result_latency`, `milvus_proxy_sq_decode_result_latency`: downstream wait/reduction/decode | Inspect fan-out, result size and processing. Proxy wait includes execution and transport; it is not network-only latency. |

The catalog follows Milvus v3.0.3's
[Proxy](https://github.com/milvus-io/milvus/blob/v3.0.3/pkg/metrics/proxy_metrics.go)
and [QueryNode](https://github.com/milvus-io/milvus/blob/v3.0.3/pkg/metrics/querynode_metrics.go)
definitions and the Helm example's relabeling. QueryNode-prefixed series may be
exported by `streamingnode` or `querynode`; actual identities remain visible.

`suspected_scheduling_pressure` requires high queue-completion latency plus
positive ready backlog throughout retained scrapes, matching exporter identity
and overlapping actual coverage. Unknown label dimensions remain in identity;
only documented request dimensions are projected away for this exporter-level
comparison. It is not a request/tenant join, and ambiguous/different exporters
are not borrowed as corroboration.

If ready backlog is zero, or its last two samples are zero after earlier backlog,
the result becomes `slow_queue_completions_without_observed_backlog` or
`slow_queue_completions_with_backlog_recovery`. Strength becomes inconclusive:
these observations contradict sustained current pressure, not the recorded past
delay. Original slow-queue source findings stay unchanged. Neither queue length
nor a gauge peak alone proves workload failure or a continuously blocked task.

Low latency in another population is not an automatic contradiction. P95s are
never added/subtracted or assembled into one critical path. Healthy/idle controls
only describe sampled populations and do not establish application-wide health.

## Configuration and provenance

The source owns authentication, histogram thresholds, reset/identity/gap checks,
raw sample times and all query/data budgets. This command collects 14 signals;
its default `max_queries` is 24 while the ordinary source default remains 8.
An explicit smaller limit is rejected, never silently raised. `--interval` is
the maximum tolerated retained-scrape gap, not a new server scrape cadence.

Override source policy in the existing connection, using your own SLOs:

```yaml
limits:
  max_queries: 24
policy:
  latency_ms:
    milvus.query: 1000
    milvus.query.tsafe: 250
    milvus.query.queue: 100
    milvus.query.core: 500
    milvus.query.loading: 1000
```

`--list -o json` publishes all source-owned example thresholds and inputs. They
are configurable examples, not universal fault thresholds. Missing lazy/version
families remain explicit rather than triggering a compatibility/health claim.

The output remains `wpdiag.report/v1` with additive optional fields:

- `results[]` keeps original source findings/observations/IDs and exact queries.
- `assessments[]` contains profile `milvus.query`, version 1, findings and gaps.
- `derived_from_findings` references source finding IDs; supporting/contradicting
  references resolve to original observation IDs.
- `capabilities.stages` declares the command and supported source collection
  profiles: `milvus.query.steps.metrics` / `e2e.query.steps.metrics`.

The text view prioritizes stage semantics; JSON retains full lineage. Collection
completion/exit 0 is not a health verdict. Failures, partial/cancelled collection
and byte limits preserve normal statuses. Output overflow emits a compact valid
budget error without oversized source data or assessments.

Reuse `prometheus.QueryModule` for source facts and `milvusquery.Assess(ctx, report)`
for stage rules. Assess has no connector argument or I/O, preserves its input,
validates schema/profile/time/identity, deduplicates equivalent lineage and fails
closed on conflict. Repeated assessment keeps IDs stable. Future integration/UI
can reuse it without copying rules or querying again; #467 orchestration is not
implemented here.

## Verify

```bash
go test -race -cover ./diagnostics/... ./cmd/wpdiag/cli
make wpdiag
docker pull quay.io/prometheus/prometheus:v3.0.1
python3 cmd/wpdiag/tests/query_smoke.py
```

The real-server test checks 22 cases across both scopes: visibility, queue
pressure/recovery, execution, loading, return waiting, healthy/idle, reset,
missing data and contrasting components. Its temporary Docker/TSDB fixture is
removed; existing Kubernetes is unchanged. Tests also cover wrong scope/time/
identity, duplicates/conflicts, incompatible versions, requested-source errors,
cancellation and budgets. See the [recovery fixture](testdata/recovery-v1.json).
