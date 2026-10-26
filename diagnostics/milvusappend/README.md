# Independent Milvus append-stage diagnosis v1

`wpdiag milvus append` explains selected write-step evidence using an independent
Prometheus connection. No wpcli/Admin, trace, Loki, integrated runner or WebUI is
required. It performs bounded read-only queries and returns source observations
plus a reusable business-stage assessment, in text or `wpdiag.report/v1` JSON.

```bash
make wpdiag
bin/wpdiag milvus append --list
# Configure sources.prometheus.connections.stack in wpdiag.yaml. Replace the
# times with an actual retained workload interval and use your own labels.
bin/wpdiag milvus append --config wpdiag.yaml --connection stack \
  --scope e2e --mode historical \
  --from 2026-10-26T01:00:00Z --to 2026-10-26T01:01:00Z -o json
# Observe the interval starting now; run a separate bounded workload alongside it.
bin/wpdiag milvus append --config wpdiag.yaml --connection stack \
  --scope milvus --window 30s --budget 1m
```

Use [the connection example](../../cmd/wpdiag/wpdiag.example.yaml) and
[Prometheus authentication/limits](../prometheus/README.md). The shared collector
uses exact deployment labels, raw retained scrapes, bounded query/input/output/
series/sample/instance budgets, cancellation and safe per-query errors. No
configuration or Kubernetes resources are changed. `--interval` bounds tolerated
scrape gaps; it does not resample or reconfigure Prometheus.

## Capability and boundary

| Profile | Scope | Flow / stage | Modes | Target filters |
|---|---|---|---|---|
| milvus.append.steps.metrics v1 | milvus | write / append | live, historical | deployment, instance |
| e2e.append.steps.metrics v1 | e2e | write / append | live, historical | deployment, instance |

`milvus` queries the first three groups below. `e2e` additionally reports WP
client/server populations as **unassociated context**. `instance` is an exact
Prometheus exporter identity and applies to every selected query; selecting one
Proxy exporter means SN/WP evidence can be missing. It is not an instruction to
infer downstream mappings. WP-only scope, read flow, other stages, snapshot,
collection/channel/log/segment filters and non-Prometheus sources are unsupported
and fail explicitly. It never silently falls back to a configured source.

A meaningful Prometheus-only diagnosis is available now. Native write-stall,
flush buildup, disk admission and replica participation remain owned by
`wp diagnose --scenario write` (#452); this stage does not copy those runtime
rules. Native, trace and Loki collection/enrichment are **not** implemented by
this initial stage. Use their independent commands for bounded next checks.
Multi-source collection and correlation belong to #467. The stage always
reports missing optional-source/mapping coverage.

## Initial hypothesis catalog

The source-owned [AppendCatalog](../prometheus/append.go) is pinned to the Helm
example's Milvus `27bd28d20688ab00cdd74232a20e988fb2162aed` metric definitions and
WP append/flush definitions. `--list -o json` exposes exact names, component,
selectors, units and default thresholds. New/missing metric versions do not
become healthy zeros. Unknown dimensions remain distinct populations.

| Step / source signals | Trigger and resulting suspicion | Applicability, exclusions and next bounded check |
|---|---|---|
| 1 Proxy: mutation completion (`milvus.append`), PK, timestamp | Source-qualified high completion latency → slow Proxy completions or suspected preparation work | Mutation success timer excludes SDK/network and failed/pending requests. PK/TSO populations can be broader. Compare actual client latency/error counts, demand and Proxy resources; no preparation residual is calculated. |
| 2 Producer: produce success, quota delay | High successful produce time → slow producer completions; high quota delay → suspected admission wait | Quota timer measures waiting before produce, not SN/backend storage. Inspect the exported channel's configured quota and resource protection; do not infer the limiting reason from latency alone. |
| 2 Routing: unavailable resuming producer count | Minimum >0 across retained scrapes → persistent unavailability observed; positive peak then final two zeros → recovery observed; otherwise intermittent/zero observations | Startup, reconnect and retries can explain this state without user demand. Inspect channel ownership/producer logs and expected traffic; this is not proof of a write stall. |
| 3 SN: successful full WAL append, backend append, before/after interceptor | High source-qualified timer → slow WAL/backend completions or suspected named interceptor work | `status=ok` for WAL/backend. Interceptor timers are per named interceptor and do not prove success of the user request. Timers can overlap, include control traffic and have different populations. Inspect retries, resources and actual matching spans. |
| 3 SN: inflight produce | Retained inflight count only | Not configured capacity or proof of saturation. Inspect expected demand and configured capacity separately. |
| 4 WP embedded client: append latency, pending append count | High source-qualified latency → slow WP client completions; pending gauge yields persistent/intermittent/recovered/zero observations | WP client runs in SN. Pending alone is not a stall or fresh demand. Resolve client metadata namespace/log identities and their time validity before associating a Milvus channel. |
| 5 WP server: successful append, buffer notification, file flush | High source-qualified timer → corresponding slow server completions/notifications/flush | Per exporter/node/log population. File flush is a local block flush timer, not all storage durability. Compare native WP evidence and mapped node ops/logs to distinguish batching/scheduling/sync/disk causes. |
| 5 WP server: disk admission 0=normal, 1=warn, 2=blocked | Nonzero peak → exported pressure observed; final two zero scrapes after pressure → recovery observed | Every retained state must be 0/1/2; other values are inconclusive. A sampled policy indicator alone does not establish latency cause or pressure between scrapes. |

Histogram judgments are **reused from the Prometheus source**, including
configured millisecond thresholds, minimum completed observations, bucket bounds,
scrape coverage, counter reset and quantile uncertainty. Seconds are converted
by source signal factors. Example defaults: mutation 1000ms; produce and WAL/
backend 500ms; quota/interceptors/WP append 250ms; PK/timestamp/buffer/flush 100ms.
They are tunable example policies, not product SLOs. `policy.latency_ms` accepts
the catalog's histogram IDs; the stage never compares a copied threshold.
Missing, idle, insufficient, reset and uncovered series remain inconclusive or
idle; a normal completion population does not establish full application health.

## Association, evidence and reuse

The report retains source IDs, actual scrape times/window, exact executed PromQL,
source findings/thresholds and coverage gaps. Stage findings reference original
source finding IDs through `derived_from_findings`, and original observations
through support. They are interpretations of the same evidence, not independent
corroboration. Per-step findings remain useful when another step fails collection.
Normal, recovery and suspicious populations are not merged into one red/green
cluster verdict. Completion status describes execution, not health.

No stage quantiles are added or subtracted. `Proxy p99 - SN p99` is not queue
time, and SN minus WP p99 is not an unmeasured network/interceptor residual.
Client-measured latency is not collected here. High internal latency is a suspect
step, not a root cause. Expected traffic and the timer's actual population must
be checked before claiming a stall or end-user impact.

Shared deployment/time, instance labels and a WP log number do not establish an
authoritative Milvus instance/collection/channel → WP-client metadata namespace/
log/segment/node mapping. A WP **service** context is separate from the client
metadata authority. This initial catalog does not collect such a time-valid
mapping, so E2E WP results stay explicitly unassociated, even if WP and WAL both
look slow. Collection/channel selection and causal cross-layer joins remain
unsupported until that mapping exists.

`milvusappend.Run(ctx, config, request)` collects with `prometheus.AppendModule`.
`milvusappend.Assess(ctx, report)` performs no I/O or mutation and can assess an
already collected compatible append-step report. Version/profile/window/identity/
reference checks fail closed. Exact duplicate evidence is collapsed; conflicting
IDs, future schema versions, dangling references and wrong scope/time are rejected.
Integration/UI should call this same assessor, preserving IDs and exclusions.

Tests exercise each latency step, unit conversion, normal/idle/reset/missing,
routing/pending/disk recovery, unknown enum states, wrong instance/time,
duplicates/conflicts, cancellation and pure assessment lineage. The real backend
smoke uses temporary Prometheus 3.0.1 TSDB fixtures, with no application writes:

```bash
go test -race ./diagnostics/... ./cmd/wpdiag/cli
make wpdiag
python3 cmd/wpdiag/tests/append_smoke.py
```
