# Independent Jaeger trace diagnosis

`wpdiag trace diagnose` (`wpdiag jaeger diagnose` is an alias) reads Jaeger Query
v1 JSON independently of wpcli, Prometheus, Loki, the example stack or a WebUI.
The compatibility target is `jaegertracing/all-in-one:1.53.0`. It identifies
suspicious observed spans and relationship gaps; it does not repair tracing,
install backends or guarantee complete request coverage.

## Connection and commands

```yaml
schema_version: wpdiag.config/v1
sources:
  trace:
    connections:
      local:
        settings:
          endpoint: http://127.0.0.1:16686  # Query/UI endpoint, not OTLP collector.
          deployment: milvus-stack
          services:                       # Exact operator-supplied bindings.
            proxy: proxy
            streamingnode: streamingnode
            segcore: segcore
            mixture: other                # Context only, not a business stage.
            milvus-woodpecker-0: wp_server
            milvus-woodpecker-1: wp_server
            milvus-woodpecker-2: wp_server
          search:
            service: proxy
            operation: Proxy-Insert       # Optional exact search operation.
          # Optional if the configured backend exports deployment resource tags.
          # Every selected span/process must satisfy configured match_tags.
          # match_tags: {namespace: milvus-stack}
          # Optional exact request-ID attribute, needed for --request-id.
          # request_id_tag: request_id
          # bearer_token_env: JAEGER_TOKEN
          # basic_auth: {username: operator, password_env: JAEGER_PASSWORD}
          limits:
            max_traces: 20
            max_spans: 2000
            max_queries: 8
            max_response_bytes: 2097152
            max_window_ms: 3600000
            timeout_ms: 10000
            export_delay_ms: 0
          policy:
            slow_span_ms: 1000             # Operator policy, not a universal SLO.
            clock_skew_ms: 100
```

Use actual exported service names. Supported roles are `proxy`, `streamingnode`,
`querynode`, `segcore`, `wp_client`, `wp_server` and context-only `other`.
A streamingnode can contain WP-client spans; recognized `LogWriter`/`LogReader`
instrumentation scopes distinguish them from Milvus work. A service binding is
configuration, not an automatically proven deployment identity. Without explicit
deployment tags, `deployment_binding_configured_only` remains a gap. Optional
`match_tags` accepts deployment selectors (`service.namespace`,
`deployment.environment[.name]`, `cluster`, `namespace`, `woodpecker_id`);
missing/contradictory values exclude the affected spans from findings.

```bash
make wpdiag
bin/wpdiag capabilities

# Immediate lookup: replace with an actual nonzero 16/32-digit hexadecimal ID.
bin/wpdiag trace diagnose --config wpdiag.yaml --connection local \
  --scope e2e --flow write --mode snapshot --trace-id YOUR_TRACE_ID -o json

# Bounded retained search: replace the UTC interval with your incident window.
bin/wpdiag trace diagnose --config wpdiag.yaml --connection local \
  --scope milvus --flow read --mode historical \
  --from 2026-10-26T01:00:00Z --to 2026-10-26T01:05:00Z -o json

# Exact configured request-ID tag, still not a uniqueness guarantee.
bin/wpdiag jaeger diagnose --config wpdiag.yaml --connection local \
  --scope e2e --flow write --mode historical \
  --from 2026-10-26T01:00:00Z --to 2026-10-26T01:05:00Z \
  --request-id YOUR_REQUEST_ID

# Observe a new interval, allow asynchronous export, then query once.
bin/wpdiag trace diagnose --config wpdiag.yaml --connection local \
  --scope wp --flow write --mode live --window 30s --interval 1s --budget 1m
```

Snapshot requires an exact trace ID and retrieves the currently retained trace,
without claiming its spans occurred now. Historical bounds cannot be future and
must fit `max_window_ms`. Live starts now, waits for the duration and optional
export delay, then executes one retained query. Its common `interval` field is
validated but does not poll updates. Use an adequate total budget. Exact trace
lookups can also have live/historical bounds; only supported spans whose start
is inside `[from,to]` are selected, while other spans remain graph context.

`--operation-id` is the common alias of trace `--request-id`; it is not a native
WP ops-registry lookup. Service/operation search is configured per connection.
Collection/channel/log/segment/node filters are unsupported in these initial
profiles and fail explicitly. Trace IDs are normalized losslessly; other source
modules cannot accept them as unverified selection assertions.

## Initial profile matrix

| Profile | Stage | Selected evidence / minimum connected stages |
| --- | --- | --- |
| `wp.write.trace` v1 | append | WP Write/WriteAsync, AddEntry/AddEntryBatch, AppendAsync, Sync, buffer wait; client ancestor of server |
| `wp.read.trace` v1 | wal_read | WP ReadNext/GetBatchEntriesAdv/ReadBatch variants; client ancestor of server |
| `milvus.write.trace` v1 | append | Proxy mutation/InsertTask and supported SN append operations; proxy ancestor of streamingnode |
| `milvus.read.trace` v1 | query | Proxy Search/Query, QueryNode RPC and recognized query/segcore processing; proxy ancestor of executor |
| `e2e.write.trace` v1 | append | Observed proxy → SN → WP server ancestry; absence remains partial |
| `e2e.read.trace` v1 | query | User query path across proxy/executor; this does not establish WAL consumption |

All support snapshot/live/historical with the prerequisites above. Operation
matching is exact and versioned in `model.go`, initially informed by WP v0.1.x,
Milvus v3.0.3 and captured example traces. A server/service name does not prove
the emitting application version; unsupported operation vocabulary remains
explicit. `consume`, persistence, whole-program orchestration and automatic
propagation fixes are separate work (#464/#465/#467/#459). Existing
`wpdiag milvus query` remains Prometheus-backed; this PR does not add a trace
provider to that stage command.

## Interpretation and evidence contract

| Finding | Trigger / units | Exclusions and limitations |
| --- | --- | --- |
| `trace.span_error` v1 | Supported selected span has explicit error/OTel error status, nonzero gRPC status or HTTP error status | Arbitrary log/error-message strings are not interpreted. Conflicting/unknown flag encodings remain gaps. An observed error is not a root cause. |
| `trace.slow_span` v1 | Supported synchronous selected span duration exceeds configured `slow_span_ms` | Jaeger times/durations are microseconds; reports show milliseconds. ReadNext may normally tail/wait; async work is not synchronous request latency. |
| `trace.relationship_coverage` v1 | Expected component ancestry within one selected trace/request subtree | Requires `CHILD_OF` links; service presence or time proximity alone is insufficient. FOLLOWS_FROM/cross-trace links, missing/multiple parents, cycles and clock/lifetime inconsistencies are explicit. |
| `trace.observed_span_summary` v1 | No supported error/slow policy matched the observed selection | Sampled/retained observations do not establish population health. Unverified error flags produce inconclusive results. |
| `trace.no_retained_evidence` v1 | Query returned no retained matching trace | Sampling, export delay, retention/eviction, idle traffic or selectors may explain absence. No-match is never healthy. |

Distinct candidate trace IDs are never merged, even with the same request tag or
timestamp. Request-tag selection marks the matched anchor spans and restricts
findings to their synchronous descendants; ambiguous parents and async links do
not establish that association. Duplicate trace/span/reference views do not
increase confidence; contradictory duplicates fail collection. Work counts are
bounded before deduplication. Parent/child and async references are retained.

Durations are inclusive and overlapping spans are not summed. No critical-path
duration, queue remainder, disk/fsync cause, or client-observed latency is derived
by subtracting unrelated timers. Minimum observed stages can be connected while
sampling/export/parents/instrumentation are still incomplete. In particular,
the committed real Insert fixture remains partial; it never becomes an invented
complete write path or a healthy result.

Reports use `wpdiag.report/v1`, source `trace`, metadata schema
`wpdiag.jaeger/v1`, exact IDs, actual HTTP observation times, span event times,
relative `/trace/ID` UI paths, sanitized executed query metadata and evidence
references. Query/UI endpoint credentials, arbitrary attributes, span logs,
status messages and backend warning/error bodies are withheld. Unsupported
operation/service names are withheld; use the relative UI path in your own
Jaeger connection for inspection. No source-count promotion is performed.

Default budgets: 20 returned traces, 2,000 spans, 2 MiB/response, one-hour query
window, ten-second request timeout; hard limits are 100 traces, 20,000 spans,
32 query attempts, 8 MiB/response and a 24-hour window. These share the core total
input/output, target and wall-clock budgets. Search asks for max-traces + 1 to
detect overflow; it does not silently paginate or claim exhaustive retention.
Authentication uses environment references, default TLS verification and no
redirect following. Missing auth/unsupported selection fails before fetching.

Exit 0 means evaluation completed, including suspicious/inconclusive results;
it is not healthy. Source failures, limits and cancellation use the common
1/5/6/7 codes; invalid/unsupported selection uses 2/3. The same `Module` owns the
profiles and results for future stage/integration/UI callers.

## Verification

`module_test.go` / `contract_test.go` cover scope/flow, connected and partial
paths, normal waiting, duplicate/conflicting events, async/missing parents,
time/identity mismatch, bounds, auth and backend failures. The sanitized
`testdata/partial-insert-v1.json` comes from the example's previously captured
Milvus v3.0.3 Insert request; network details, arbitrary tags and logs were removed.

```bash
go test -race ./diagnostics/jaeger ./diagnostics ./cmd/wpdiag/cli
make wpdiag
python3 cmd/wpdiag/tests/jaeger_smoke.py
```

The smoke script creates/removes an isolated Jaeger 1.53 Docker container,
imports OTLP fixture spans, then runs the real diagnostic binary. It does not
touch an existing stack. See the pinned primary API definitions for
[Jaeger JSON units/references](https://github.com/jaegertracing/jaeger/blob/v1.53.0/model/json/model.go)
and [bounded query parameters](https://github.com/jaegertracing/jaeger/blob/v1.53.0/cmd/query/app/query_parser.go).

## Queued WP append compatibility (#459)

The [actual queue/gRPC exporter probe](../../tests/integration/tracing/README.md)
verifies parent propagation for single entries and independent batch traces
linked to contributing input spans. The catalog recognizes exact scopes
`AppendOp`/`BatchAppendOp` with operation `Execute` and qualified WP
`LogStore/AddEntry`/`LogStore/AddEntries` RPC names. A generic Execute is not a
recognized stage. Embedded client scopes count as WP work, not evidence of the
Milvus WAL stage merely because their process is named StreamingNode. Batch
Execute is asynchronous; it is not a serial timer or proof of all linked
requests' progress. External traces are not automatically fetched/merged.
Older Milvus builds can remain partial even with the WP propagation fix.

A qualified WP append **client** RPC with an unambiguous gRPC Cancelled status
returns `observed_rpc_cancellation` with unknown impact. WP cancels an
acknowledged stream without reading another EOF; genuine interruption is also
possible. The raw normalized error flag remains true and no write-success or
write-failure conclusion is inferred. Server cancellations, other errors and
ambiguous tags are not downgraded by this compatibility rule.
