# Independent WAL read, consume and visibility diagnosis

`wpdiag milvus consume` implements the bounded initial catalog for
[#464](https://github.com/zilliztech/woodpecker/issues/464). It uses a
[Prometheus connection](../prometheus/README.md) independently of WP Admin,
Jaeger, Loki, sibling stages and the integrated runner. Queries are read-only.
Findings identify suspicious steps, supporting/contradicting observations,
remaining gaps and next checks. Completion is not a cluster-health verdict.

## Run

Use `sources.prometheus.connections.stack` in your wpdiag.yaml:

```bash
make wpdiag
bin/wpdiag capabilities
bin/wpdiag milvus consume --list
bin/wpdiag milvus consume --list -o json

# Substitute an actual retained workload interval.
bin/wpdiag milvus consume --config wpdiag.yaml --connection stack \
  --scope milvus --mode historical \
  --from 2026-10-26T01:00:00Z --to 2026-10-26T01:05:00Z

# Include separately observed WP retrieval populations.
bin/wpdiag milvus consume --config wpdiag.yaml --connection stack \
  --scope e2e --mode historical \
  --from 2026-10-26T01:00:00Z --to 2026-10-26T01:05:00Z -o json > consume.json

# Wait for a new interval, then query its retained scrapes.
bin/wpdiag milvus consume --config wpdiag.yaml --connection stack \
  --window 2m --interval 30s --budget 3m
```

Flow is fixed to `read`, stage to `consume`, scope to `milvus|e2e` and source to
Prometheus. Snapshot, WP scope and node/collection/channel/log/segment/reader
filters are explicitly unsupported. Target filters are the deployment binding
and exact exporter instance. A single instance can leave other steps uncovered;
use deployment scope for an overview. `--interval` bounds acceptable gaps between
retained scrapes; it does not change the server scrape configuration.

## Five groups and initial hypotheses

| Group | Inputs / applicability | Interpretation and next checks |
| --- | --- | --- |
| 1. Read mode / source | Scanner count by `scanner_model=catchup|tailing` | Observed population, not per-reader progress. Memory tailing can have no WP service traffic. Verify actual client metadata namespace/log and the desired frontier. |
| 2. WP retrieval (E2E only, unassociated) | ReadNext, server read-batch and direct-read completion histograms; quorum bytes; EOF counters | Slow qualified batch/direct completions are observations. Blocking ReadNext and EOF are normal at the tail. Use native reader reporting/progress/quorum evidence before claiming a stalled reader. |
| 3. Scanner / filtering / buffers | Scanned/passed counters by mode/message type; pause flag and pending bytes by channel; transaction and TimeTick buffers | Continuous sampled pause plus continuous positive pending bytes on the same population supports `suspected_scanner_downstream_backpressure`. Last two pause/pending samples at zero after a pause indicate observed recovery. Check downstream consumption, ordering and transaction completion before blaming storage. |
| 4. Subscription / dispatch | Local/remote consumer counts, unavailable consumers, delivered bytes and remote in-flight calls | Distinguish persistent, intermittent and recovered unavailability. Local subscriptions bypass the remote gRPC handler, so missing remote-only series is not a fault. Inspect routing/retry state; counts do not establish service capacity. |
| 5. Processing / visibility | Insert/delete processing histogram, waiting messages, collection TimeTick lag and consumed MsgPacks | Slow completions plus positive waiting work throughout aligned coverage support `suspected_consumer_processing_pressure`. Zero/recovered waiting work contradicts sustained pressure without erasing recorded past delay. Increasing lag with MsgPack activity at every sampled interval is an observation, not a reader-stall or per-row visibility verdict. Follow with `wpdiag milvus query` for query consistency/TSafe/scheduling/execution. |

For both suspicion rules, observed identity and actual retained time bounds must
match. Pause/pending samples must also align individually. Processing/waiting
uses the same full label population and matching first/last sample bounds; the
completion distribution cannot identify individual queued messages. Scrapes do
not prove continuous state between observations. The pause gauge is shared by
scanners on a channel, so it is not a per-reader pause registry.

Gauge buffers/unavailability use positive minimum for persistent sampled state,
last two zero samples after a positive sample for recovery, and otherwise
intermittent state. Those observations alone do not establish a stall, missing
commit or corruption. Scanned/passed populations include control filtering;
their ratio is never interpreted as data loss.

Collection lag is exported with `msg_type=timetick`; the producer increments
consumed MsgPacks using `msg_type=all`. Only this documented label dimension is
projected away when comparing these two metrics. Collection, exporter and every
other label, including hidden dimensions, remain in the association identity.
Samples must align. No fresh MsgPacks, resets, intermittent activity, wrong
collection/exporter or missing data withhold corroboration. MsgPacks can contain
control traffic, so even continuous activity does not prove fresh payload demand.
Lag can grow simply because no new pack updates the last timestamp.

## Source-owned metric catalog v1

The Milvus definitions follow the Helm example's pinned
[streaming metrics](https://github.com/milvus-io/milvus/blob/27bd28d20688ab00cdd74232a20e988fb2162aed/pkg/metrics/streaming_service_metrics.go),
[QueryNode metrics](https://github.com/milvus-io/milvus/blob/27bd28d20688ab00cdd74232a20e988fb2162aed/pkg/metrics/querynode_metrics.go),
[scanner producer](https://github.com/milvus-io/milvus/blob/27bd28d20688ab00cdd74232a20e988fb2162aed/internal/streamingnode/server/wal/metricsutil/wal_scan.go)
and [filter-node producer](https://github.com/milvus-io/milvus/blob/27bd28d20688ab00cdd74232a20e988fb2162aed/internal/querynodev2/pipeline/filter_node.go).
QueryNode-prefixed series can be exported by StreamingNode or QueryNode; their
actual identities remain visible. Version/lazy-series gaps stay explicit.

| Signal | Metric | Unit / population |
| --- | --- | --- |
| milvus.consume.mode | milvus_wal_scanner_total | Gauge; scanners by channel/mode |
| milvus.consume.scanned | milvus_wal_scan_message_total | Counter; messages by channel/mode/type |
| milvus.consume.passed | milvus_wal_scan_pass_message_total | Counter; passed messages by channel/mode/type |
| milvus.consume.pending | milvus_wal_scanner_pending_queue_bytes | Gauge; channel bytes |
| milvus.consume.paused | milvus_wal_scanner_pause_consumption | Gauge; channel state 0/1 |
| milvus.consume.txn | milvus_wal_scanner_txn_buf_bytes | Gauge; channel bytes |
| milvus.consume.ordering | milvus_wal_scanner_time_tick_buf_bytes | Gauge; channel bytes |
| milvus.consume.subscription | milvus_streaming_consumer_total | Gauge; channel/access-model consumers |
| milvus.consume.unavailable | milvus_streaming_resuming_consumer_total | Gauge; consumers, status=unavailable |
| milvus.consume.delivered | milvus_streaming_consume_bytes_sum | Counter; histogram sum of delivered message bytes, not latency |
| milvus.consume.remote | milvus_streamingnode_consume_inflight_total | Gauge; remote handler calls |
| milvus.consume.waiting | milvus_querynode_wait_processing_msg_count | Gauge; waiting messages by msg_type |
| milvus.consume.processing | milvus_querynode_process_insert_or_delete_latency | Histogram; milliseconds by msg_type; example threshold 250 ms |
| milvus.consume.lag | milvus_querynode_consume_tt_lag_ms | Gauge; milliseconds by collection, msg_type=timetick |
| milvus.consume.packs | milvus_querynode_consume_msg_count | Counter; MsgPacks by collection, msg_type=all |
| wp.read_next | woodpecker_client_reader_operation_latency | Histogram; milliseconds, operation=read_next/status=success; 1000 ms source example, no stage fault threshold |
| wp.read_batch | woodpecker_server_read_batch_latency | Histogram; milliseconds; example threshold 250 ms |
| wp.direct_read | woodpecker_client_direct_read_latency | Histogram; milliseconds; example threshold 250 ms |
| wp.quorum_bytes | woodpecker_client_quorum_read_bytes_total | Counter; bytes by log namespace/ID/AZ scope |
| wp.eof | woodpecker_server_logstore_operations_total | Counter; operation=get_batch_entries/status=end_of_file |

The stage reuses source-qualified histogram findings: sufficient completions,
monotonic coherent buckets, sample freshness/coverage and a P95 bucket lower
bound above the configured policy. Empty, idle, reset, sparse or unresolved
histograms stay inconclusive. Quantiles are never added/subtracted. Slow
ReadNext source findings are preserved as source evidence but downgraded in the
stage assessment because the timer includes normal waiting for new data.

Counters retain actual samples, first/last delta and delta divided by actual
elapsed seconds. No extrapolated PromQL rate is substituted. At least two
scrapes, fresh boundaries, bounded gaps and no visible reset are required;
resets withhold facts. Between-scrape resets/events remain unobservable. Zero
activity does not establish expected work or an outage.

The query count is 15 for Milvus, 20 for E2E. Default `max_queries` is 24 for this
stage; the ordinary source default stays 8. An explicit smaller budget is
rejected before I/O. Configure deployment labels/auth/budgets in the existing
Prometheus connection; tune source-owned examples to your SLOs:

```yaml
limits:
  max_queries: 24
policy:
  latency_ms:
    milvus.consume.processing: 250
    wp.read_batch: 250
    wp.direct_read: 250
```

## Boundaries and reusable report contract

Every E2E assessment has `wp_reader_mapping_unavailable`: this initial catalog
does not collect authoritative, time-valid instance/collection/channel to
WP-client metadata namespace/log/segment/node mappings. WP retrieval is parallel
context, not corroboration for a particular Milvus reader or a serial duration.
A WP service context does not establish the client's metadata namespace.

Native rules from [#453](https://github.com/zilliztech/woodpecker/issues/453)
remain in `wp diagnose --scenario read`; use them for reporting cadence,
expected catch-up progress and explicit quorum evidence. This command does not
import those expanded native findings or query Admin. Trace/Loki/native stage
enrichment is an extension; the current native adapter has its own narrower
declared profiles. No unsupported requested source is silently ignored. No
source absence is relabeled as a failed live service or healthy result.

Output uses `wpdiag.report/v1`. `results[]` retains original source observations,
executed queries, actual sample times, findings and IDs. `assessments[]` adds
`milvus.consume` version 1; `derived_from_findings` references the source IDs,
and support/contradiction references resolve to original observations.
`capabilities.stages` publishes `milvus.consume.steps.metrics` and
`e2e.consume.steps.metrics` v1. Suggested next checks are not executed evidence.

Use `prometheus.ConsumeModule` to collect source facts and
`milvusconsume.Assess(ctx, report)` to apply these same stage rules without
I/O, mutation or threshold duplication. Equivalent repeated assessment keeps
IDs stable; schema/profile/window/identity/reference conflicts fail closed.
The [integrated runner](../integrated/README.md) reuses this owner implementation;
the WebUI invokes the same runner. This package does not implement runtime
intervention or client end-to-end latency measurement. Ordinary cancellation,
partial/error status and byte/query limits apply.

## Verify

For an explicitly opted-in real deployment, see the
[scanner-startup / initial visibility acceptance guide](../../tests/integration/diagnostics/CONSUME.md).
It uses acknowledged data and a native configuration control to corroborate
retained source observations; the consume command itself remains read-only and
does not query management settings or infer a per-reader frontier from pause alone.

```bash
go test -race -cover ./diagnostics/... ./cmd/wpdiag/cli
make wpcli wpdiag
docker pull quay.io/prometheus/prometheus:v3.0.1
python3 cmd/wpdiag/tests/consume_smoke.py
```

The real-server test exercises deterministic retained TSDB samples across both
scopes: scanner/processing pressure and recovery, lag with fresh/idle/reset or
mismatched collection evidence, tailing/local access, unavailability, counter
progress, healthy/idle/missing/reset controls, normal blocking ReadNext/EOF,
slow batch/direct reads and zero RPC traffic. It cleans up its temporary Docker
container without changing an existing Kubernetes environment. Unit tests also
cover invalid selection, hidden identity dimensions, wrong instance/time,
duplicates/conflicts, version gaps, source errors and cancellation. See the
[sanitized lineage example](testdata/recovery-v1.json).
