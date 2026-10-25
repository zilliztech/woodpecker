# Prometheus source diagnosis

`wpdiag prometheus diagnose` evaluates retained metric evidence without WP
Admin, wpcli, trace or Loki access. This is the bounded initial catalog from
[#460](https://github.com/zilliztech/woodpecker/issues/460), using the public
[diagnostic contracts](../README.md). It is not an incident root-cause engine.

## Connect and run

Build `make wpdiag`, then create a source-owned connection in `wpdiag.yaml`:

```yaml
schema_version: wpdiag.config/v1
sources:
  prometheus:
    connections:
      stack:
        settings:
          endpoint: http://127.0.0.1:9090
          deployment: my-stack
          match_labels:
            cluster: wp-ops-v0146
            namespace: milvus-stack
            woodpecker_id: milvus-stack
          # Optional; environment variable reference, not a token literal.
          # bearer_token_env: PROMETHEUS_TOKEN
```

Use actual selectors from your scrape configuration. `deployment` is an explicit
operator-defined connection binding; it does not discover Kubernetes resources
or establish a WP-client metadata namespace. `match_labels` are exact deployment
selectors applied to every query. Metric-specific component/operation/status
filters are added by the profile, with no user-supplied PromQL interpolation.
No instance/channel/log association is inferred across components.

The endpoint accepts an HTTP(S) base URL, optionally with a reverse-proxy path,
without embedded credentials, query or fragment. Authentication supports
`bearer_token_env` or `basic_auth: {username: YOUR_USER, password_env: PASSWORD_ENV}`.
Secrets must exist in the environment at execution time. TLS uses normal
system certificate verification. Redirects are not followed. Errors, backend
annotations, endpoint URLs and private connection/auth settings are not exported.
Keep credentials out of public resource labels and selectors.

```bash
bin/wpdiag capabilities

# Replace these UTC times with a retained interval for your own workload.
bin/wpdiag prometheus diagnose --config wpdiag.yaml --connection stack \
  --scope e2e --flow write --mode historical \
  --from 2026-10-25T08:00:00Z --to 2026-10-25T08:05:00Z -o json > write.json

bin/wpdiag prometheus diagnose --config wpdiag.yaml --connection stack \
  --scope milvus --flow read --stage query --mode historical \
  --from 2026-10-25T08:00:00Z --to 2026-10-25T08:05:00Z

# Observe from now, wait the whole window, then retrieve retained scrapes.
bin/wpdiag prometheus diagnose --config wpdiag.yaml --connection stack \
  --scope wp --flow write --mode live --window 2m --interval 30s --budget 3m
```

For this source, `--interval` is the **maximum tolerated retained-scrape gap**,
not a command to change Prometheus's scrape cadence. Its live default is 30s;
historical queries default to the connection policy. The effective bound is
the smaller of the requested interval and `policy.max_gap_ms`. A live window
must fit the execution budget plus query time. Cancellation during the wait
prevents queries. Snapshot profiles are explicitly unsupported: one lifetime
histogram cannot establish interval completion latency or progress.

## Implemented catalog and metric contract

All six profiles support historical and live windows. Only the listed stages
and target filters are implemented; other combinations fail explicitly.

| Scope / flow | Profile / stage | Metric populations | Target filters |
| --- | --- | --- | --- |
| wp / write | wp.write.metrics / append | Successful server append histogram; exported disk admission state | deployment, instance, node_id |
| wp / read | wp.read.metrics / wal_read | Successful server get_batch_entries histogram | deployment, instance, node_id |
| milvus / write | milvus.write.metrics / append | Proxy mutation histogram by exported population | deployment, instance |
| milvus / read | milvus.read.metrics / query | Proxy Search/Query histogram by exported population | deployment, instance |
| e2e / write | e2e.write.metrics / append | Proxy mutation, SN WAL append, WP server append and WP admission, assessed independently | deployment |
| e2e / read | e2e.read.metrics / read_overview | WP backend WAL reads and Proxy Search/Query as separate read branches | deployment |

Applicability follows the Helm example's Milvus v3.0/WP v0.1 metric names and
relabeling contract: `component=proxy|streamingnode|woodpecker`, a nonempty
`instance`, and `node_id` on WP series. Missing or changed families are coverage
gaps, not an automatic compatibility claim. Exact families are:

| Signal | Classic histogram base / gauge | Unit and filter |
| --- | --- | --- |
| wp.append | woodpecker_server_logstore_operation_latency | milliseconds; operation add_entry/add_entry_batch, status success |
| wp.wal_read | woodpecker_server_logstore_operation_latency | milliseconds; operation get_batch_entries, status success; EOF excluded |
| milvus.append | milvus_proxy_mutation_latency | milliseconds; msg_type insert/upsert/delete, component proxy |
| milvus.query | milvus_proxy_sq_latency | milliseconds; component proxy; retain query_type when exported |
| milvus.wal_append | milvus_wal_append_message_duration_seconds | seconds, converted to milliseconds; preserve status/message population labels |
| wp.admission | woodpecker_server_logstore_write_backpressure_state | gauge: 0 normal, 1 warning, 2 blocked under deployed policy |

No native pending-op registry is reconstructed from counters. A normal tailing
reader may have no successful backend-read samples; zero completion increase
does not establish expected work, a stalled reader or an outage. Server timers
exclude SDK/network round-trip time. SN WAL timers and WP timers can overlap;
their quantiles are never added, subtracted or joined as one request.

E2E here means **source-only, partial stage coverage**. The read overview does
not put WAL reads and user queries into one serial timer. These profiles do not
diagnose SN scanner progress, visibility/TSafe, WP-client queues, query execution
or persistence. Every E2E report includes `source_only_partial_stage_coverage`.
Those business-stage relationships remain #463–#466, and multi-source synthesis
remains #467.

## Rules and interpretation

Each histogram population retains its full exporter label identity internally;
there is no sum across nodes, jobs, logs or tenants. Reports allowlist public
labels and retain a fingerprint for additional dimensions, so redaction cannot
accidentally merge distinct populations. Equal duplicate series/samples are
counted once; conflicting duplicates or wrong scope fail the query. This is
one source of evidence, even when several findings reference it.

The collector requests classic `_count`, `_sum` and `_bucket` families. It
requires aligned sample timestamps, cumulative bucket consistency, a matching
infinite bucket/count, monotonic counters, integer/exact histogram counts,
enough scrapes, fresh window boundaries and no excessive interior gap. A visible
reset/restart invalidates comparison rather than being silently repaired. A
restart/reset that occurred entirely between scrapes may remain unobservable.
Warnings/info annotations qualify the query and withhold conclusions; arbitrary
backend annotation text is not exported.

For the actual first/last retained samples, it records completion count/rate,
mean latency and the P95 bucket's lower/upper bounds. It does **not** interpolate
an exact quantile. A lower bound at/above the configured threshold yields
`suspected_high_latency`; an upper bound within the threshold yields
`within_configured_latency_for_observed_completions`. A threshold inside the
bucket is inconclusive. An infinite tail retains its finite lower bound and an
explicit unbounded-tail gap. Low event counts, idle data and missing families
have distinct inconclusive outcomes. Success-completion latency does not cover
pending work, errors or global application health.

A completion-rate drop compares rates across the actual first/second parts of
the sampled population, requiring at least five scrapes and sufficient events.
`observed_throughput_drop` remains inconclusive about failure: demand, arrivals,
batching or errors may explain it. No throughput baseline is silently borrowed
from another log, process or time window. The admission rule reports nonzero
exported 0/1/2 state under deployed policy; it does not claim that this proves
the cause of a latency finding. Impact and evidence strength stay separate.

## Time, provenance and budgets

The API request is `/api/v1/query` with a range-vector selector anchored using
`@ end`. This retrieves original retained scrape timestamps in `(from,to]`.
It intentionally avoids range-query resampling/lookback, which can hide gaps by
repeating old samples at new evaluation timestamps. Deltas cover actual sample
endpoints without extrapolation. See the official
[range-vector semantics](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors)
and [HTTP matrix format](https://prometheus.io/docs/prometheus/latest/querying/api/#range-vectors).

Each executed observation carries the exact credential-free PromQL expression,
original samples/units, derived facts, query acquisition times and stable IDs.
Metadata records source schema/run, bound deployment, requested interval,
effective policy and consumed budgets. Suggested next checks remain unexecuted.
The query acquisition time is separate from historical sample time. Reports use
`wpdiag.report/v1`, with source metadata `wpdiag.prometheus/v1`.

Defaults, all adjustable in the source connection:

```yaml
limits:
  max_queries: 8           # hard cap 32, sequential requests
  max_series: 500          # hard cap 5000, total before deduplication
  max_samples: 50000       # hard cap 200000, total before deduplication
  max_response_bytes: 2097152  # hard cap 8 MiB, per response before decode
  max_window_ms: 3600000   # hard cap 24 hours
  timeout_ms: 10000        # hard cap 60s per request; global --budget also applies
policy:
  max_gap_ms: 30000
  min_observations: 20
  throughput_drop_ratio: 0.5
  latency_ms:             # illustrative defaults; set your own SLOs
    wp.append: 250
    wp.wal_read: 250
    milvus.append: 1000
    milvus.query: 1000
    milvus.wal_append: 500
```

Shared `--max-input-bytes` caps total HTTP body bytes; `--max-output-bytes` caps
the complete JSON report; `--max-targets` caps distinct exporter instances.
The response series limit requests one extra series to detect overflow; local
limits still apply if the backend ignores this parameter. Client budgets bound
returned data and duration, not the backend's global TSDB resources. Prometheus
owns server-side query resource limits. Unimplemented filtering, budget overflow,
auth/transport failure and partial collection never become healthy zeros.

## Validation

```bash
go test -race -cover ./diagnostics/prometheus ./cmd/wpdiag/cli
make wpdiag
docker pull quay.io/prometheus/prometheus:v3.0.1
python3 cmd/wpdiag/tests/prometheus_smoke.py
```

The real-server test imports deterministic OpenMetrics samples with promtool
into a temporary TSDB and runs 30 cases: six profiles × slow/healthy/idle/reset/
sparse controls. It creates and removes one Docker container; no Kubernetes or
existing stack is changed. HTTP tests additionally cover auth, cancellation,
qualified/error responses, redirects, identity mismatches, duplicate/conflicting
lineage, histogram resolution/saturation and data budgets. Tests run in CI.
