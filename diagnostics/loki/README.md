# Standalone Loki diagnosis (catalog v1)

`wpdiag loki diagnose` reads Loki directly. It needs no Grafana, Prometheus,
Jaeger, wpcli or integrated runner. It never pushes logs or changes the cluster.

```yaml
schema_version: wpdiag.config/v1
sources:
  loki:
    connections:
      stack:
        settings:
          endpoint: http://127.0.0.1:3100
          deployment: milvus-stack
          match_labels:
            cluster: wp-ops-v0146
            namespace: milvus-stack
          # bearer_token_env: LOKI_TOKEN
          # basic_auth: {username: reader, password_env: LOKI_PASSWORD}
          # tenant_env: LOKI_TENANT
```

Use exact labels from your collector. The Helm example's Alloy exports `cluster`,
`namespace`, `pod`, `container`, `component`; it does **not** export the metrics
label `woodpecker_id`. `deployment` is the explicit connection binding, not an
invented Loki label. At least `match_labels.namespace` is required. Component/Pod
are reserved selectors. `--instance` means Kubernetes **Pod**, not a metrics
endpoint. `--operation-id` requires an exact parsed `requestID`/`request_id` match;
it is applied to retrieved content and does not establish a cross-source mapping.

```bash
make wpdiag
bin/wpdiag capabilities -o json
# Replace these UTC timestamps with your retained workload interval.
bin/wpdiag loki diagnose --config wpdiag.yaml --connection stack \
  --scope e2e --flow write --mode historical \
  --from 2026-10-26T01:00:00Z --to 2026-10-26T01:01:00Z -o json
bin/wpdiag loki diagnose --config wpdiag.yaml --connection stack \
  --scope wp --flow read --instance YOUR_POD --window 30s --budget 1m
```

## Supported matrix

All profiles are version 1 and support bounded live and historical windows only.
Live waits for the selected interval, then queries it; ingestion after that query
is not covered. Snapshot, arbitrary LogQL, metadata/log/channel selectors, other
stages and alternative component label conventions are explicitly unsupported.

| Scope | Flow | Profile / stage | Initial interpretation |
|---|---|---|---|
| wp | write | wp.write.logs / append | Server append/batch failure; local WAL disk pressure and recovery |
| wp | read | wp.read.logs / wal_read | Server batch read failure; embedded WP reader read error |
| milvus | write | milvus.write.logs / append | SN slow WAL append |
| milvus | read | milvus.read.logs / read_overview | Proxy result reduction/decoding failure |
| e2e | write | e2e.write.logs / append | Union of selected WP/Milvus write messages; actual shared request/trace identity when present |
| e2e | read | e2e.read.logs / read_overview | Union of selected WP/Milvus read messages; same identity condition |

`Catalog()` in [catalog.go](catalog.go) is the single exact-message rule catalog.
The patterns reference WP `server/logstore.go`, `server/maintenance.go`,
`woodpecker/log/log_reader.go`, and Milvus `metricsutil/wal_write.go` and
`proxy/search_reduce_util.go`. They recognize JSON `msg`/`message`, or bracket
Zap `[timestamp] [WARN|ERROR|INFO] [caller] ["message"]` output. Version 1 is a
pattern contract, not a claim to support every application release. Stream components
and the escaped LogQL candidate-message filter are derived from this same selected
scope/flow catalog. Components with no applicable rules (including querynode in
v1) are not queried. Ordinary INFO/DEBUG traffic without candidate messages is
filtered by Loki before its returned-line limit, so it cannot hide a later known
error by exhausting that limit. Exact message/format validation still runs locally:
a substring inside an error body is only a candidate, not a matching event. Unknown
messages/formats remain uninterpreted. Read progress, timeouts, producer quota,
flush and channel/log identity joins remain outside this initial catalog; inspect
metrics/native evidence with their independent commands. Integration is #467.

## Evidence and interpretation

Each finding cites actual executed `logql` page observations, with exact start
and exclusive end nanoseconds, unique event IDs, timestamps, safe stream labels,
pattern/version, counts and the **actual matching message excerpt**. Raw lines,
error bodies, arbitrary attributes, structured metadata and authenticated URLs
are withheld. Only validated request/trace IDs are retained. Text IDs are read from top-level
fields after the message, respecting quotes, escapes and nested brackets. IDs
inside errors/objects/arrays are ignored; the first valid top-level field wins
over later duplicates. JSON uses top-level structured fields. Keep IDs and selected
labels free of secrets. Auth credentials and optional tenant headers are read from
environment references, never exported; redirects are not followed.

A warning/error is a symptom, not a confirmed cause. A later disk recovery event
on the same stream is contradicting evidence for *persistent* pressure, not proof
that all writes recovered. Only identical observed request/trace IDs across roles
produce an association finding; this does not establish causation or guarantee ID
uniqueness. Same-window errors are not joined. No collection-to-WP namespace/log
mapping is inferred. Duplicate stream/timestamp/line identities are collapsed,
with occurrence counts and a gap explaining replay versus producer ambiguity.
Both request and trace identities are indexed when present. Identical event sets
matched through both IDs produce one association. Result metadata's `associations`
records each finding's exact event IDs and identity kinds. Partially overlapping
sets retain separate associations and explicit shared-event lineage, rather than
being transitively merged or counted as independent confirmations. Sharing an
HTTP page observation alone is not enough to deduplicate different event sets.
One event does not become multiple independent confirmations.

Normal, idle, empty, no-match and unknown-format responses yield **inconclusive**.
The command cannot distinguish missing collectors, retention, rotation, delivery
lag or absent expected traffic from no-match. A successful query is execution
success, not a healthy verdict. Suggested next checks were not executed.

## Bounds and failure behavior

Source defaults: 8 queries, 500 lines/page, 2,000 returned candidate lines including replay,
2 MiB/response, 1 hour/window, 10 seconds/query. Configure under `settings.limits`:
`max_queries` (1–32), `page_size` (1–5000), `max_lines` (1–20000, candidates including replay),
`max_response_bytes` (1 KiB–8 MiB), `max_window_ms` (1–86400000),
`timeout_ms` (1–60000). Shared input/output/time budgets and `--max-targets`
(distinct streams, including container dimensions) also apply. There is no
unbounded full-history fallback or raw-line export option.

Pagination re-queries the inclusive last timestamp and deduplicates replay. A
full page at one timestamp cannot safely advance: the result is partial rather
than skipping `+1ns`. Query/line/byte/stream exhaustion is explicit. An API may
omit data without indicating it; `pagination_complete` describes pagination,
not collector completeness. Returned labels/times are validated against the
requested scope/window. Errors retain safe codes, not backend response bodies.
Exit codes: completed 0, failed 1, invalid 2, unsupported/disabled 3,
budget exceeded 5, partial 6, cancelled 7. Partial observations remain available.

Tests include all six profiles, exact/changed formats, normal/idle/no-match,
wrong scope/time, recovery, unrelated identities, replay, tied timestamps,
authentication, redirects, limits and cancellation. Regressions cover an actual WP
text-encoder error body carrying fake IDs, both-ID/request-only peers, exact-set
association deduplication and partially overlapping sets. The isolated real Loki test
uses the Helm example's 3.5.7 image and 26 synthetic fixture cases, including
2,500 ordinary lines before a known error and 12 unsupported-component streams
with a single-target budget:

```bash
go test -race ./diagnostics/loki ./cmd/wpdiag/cli
make wpdiag
python3 cmd/wpdiag/tests/loki_smoke.py
```

API contract: [official Loki HTTP API](https://grafana.com/docs/loki/latest/reference/loki-http-api/).
