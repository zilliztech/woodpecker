# WP-native diagnosis contract v1

`wp diagnose` performs bounded, read-only WP service checks using existing
metrics/Admin APIs. It is part of wpcli. External monitoring, historical incident
reconstruction, Milvus rules and UI belong to the separate diagnostic tool.

This capability requires a binary built from this source revision. It does not
change existing commands or require deploying new server APIs.

## Invocation and connection

```bash
wp diagnose --list
wp --context local diagnose node-0 --scenario stuck-write --window 30s
wp --context local diagnose node-0 --scenario stuck-read -o json > read-report.json
wp --context local diagnose node-0 --mode snapshot -o json > snapshot.json
wp --context local diagnose --scenario all -o json > nodes-report.json
```

Reuse the selected `cli.yaml` WP service context, seed `--endpoint`, environment
override and `--node-admin-url` mappings. A seed must be an HTTP(S) origin without
embedded credentials, paths, queries or fragments. Existing wpcli does not provide
an authenticated peer-mapping mechanism; no new one is introduced here.

Without a node argument all discovered nodes are inspected, subject to
`--max-nodes`. Exceeding the limit fails rather than silently selecting a subset.
An explicit undiscovered address/mapping remains unverified; missing matching
node labels cannot establish a node-specific completion rate.

Suggested `wp ops` checks must reuse the original service context and node
mappings. Service membership is separate from a Milvus WP-client's metadata
namespace; this command does not choose or connect tenant etcd automatically.

## Supported checks and limits

The implemented typed rules are `write_stall`/`stuck-write` and
`read_stall`/`stuck-read`. `--list -o json` exports their versions, flow/stage,
required/optional evidence, applicability, comparison and exclusions.

- **Snapshot:** captures current evidence; cannot establish sustained no-progress.
- **Live window:** collect a baseline, then sample until the requested interval
  after that baseline. Source timestamps describe actual HTTP observation times;
  collection is neither atomic nor continuous. Discovery/baseline work also
  consumes the total budget.
- **History:** not supported here. `--window` starts observation now; it does not
  retrieve past values. Historical range/time flags are rejected.

A suspicion requires the same operation identity (including its start time) in
every sample and covered matching completion series with no increase. Each
series is compared between consecutive samples, so an intermediate reset or a
reset offset by another series' growth cannot be hidden by first/last totals.

Read EOF is progress. Unrelated logs or write traffic do not mask a pending
read. If same-log completions advance while an operation stays pending, the
report is `conflicting`: those completions may belong to other work or tenants.
It does not declare the pending operation completed or healthy.

Missing lazy counters, changed series, process restarts, insufficient samples,
unavailable APIs and unsupported build/interface versions remain explicit.
Explicit exporter metric timestamps are retained but treated as temporally
unverified for these live rules; cached/past measurements are not relabeled as
fresh evidence merely because the HTTP request succeeded.
The implemented API profile covers v0.1.x/dev service shapes; unknown build
availability is qualified and other reported version lines are unsupported.
Build version alone is not proof of capability compatibility.

Registry capacity, listing limits and eviction changes qualify absent-operation
results. More in-use records than listed ops also means incomplete coverage or
non-atomic sampling skew, even below the requested list limit. A disappeared op
may have completed or been evicted. No finding proves
that every client wait is observable, establishes corruption, confirms a disk
cause or triggers intervention. Use an observation window suitable for the
operation before interpreting a suspicion; there is no universal production SLO.

`/admin/env` is read once per node for `build.version` only. Environment, host,
endpoint URLs/credentials and HTTP error bodies are not exported. Only the
completion-counter family and process start gauge are retained from `/metrics`.
Existing op trace/span IDs are retained where provided; they do not establish
connection to a Milvus user trace by themselves.

Default limits are 8 nodes, 31 rounds/node, 100 ops/sample, 500 retained counter
series/sample, 2 MiB/response and a 1-minute total budget. `--timeout` bounds each
request, and `--concurrency` bounds parallel node collection. Hard limits also
bound the products of nodes × samples × records. Increase limits explicitly for
longer windows; invalid/unbounded requests are usage errors.
Time settings have a minimum of 1 ms to match the contract's requested-duration
resolution. Sampling and post-collection evaluation share the total budget.

## Machine contract

The JSON envelope is `schema_version: "wp.diagnosis/v1"`. YAML uses the same field
names and preserves int64 identities. The contract lives in `model.go`; committed
fixtures are [`testdata/scoped-write-v1.json`](testdata/scoped-write-v1.json) and
[`testdata/ambiguous-read-v1.json`](testdata/ambiguous-read-v1.json).

| Field | Meaning |
| --- | --- |
| `run_id`, `source`, `scope`, `service_context` | Report identity and WP runtime selection; context is a local connection reference, not a tenant/deployment join |
| `observation_mode`, requested timing, start/end | Requested versus actual observation timing, including partial collection |
| `collection_status`, `limits` | Execution completeness and explicit budgets; not a health verdict |
| `targets`, `rules` | Discovered/unverified identities and applied rule versions/semantics |
| `observations` | Stable within-report IDs, source IDs, rounds, timestamps/status, counters/labels, ops, registry stats or build version |
| `findings` | Stable IDs; rule/flow/stage; established node/log/segment/op and optional trace ID; outcome/impact/strength; supporting/contradicting IDs, unknowns and next checks |
| `coverage_gaps` | Unavailable/unsupported/invalid/truncated evidence and inference limits |

Numeric log/segment IDs are signed 64-bit integers. Consumers must decode them
losslessly rather than through JavaScript/float64 numbers. Counter labels carry
decimal log IDs as strings. Observation/finding IDs must be retained when wrapping
the report; source names or repeated wrappers do not create independent evidence.

An observed ops source with no `ops` entries means the decoded list was empty;
an unavailable/invalid source is never an empty-list assertion. A missing counter
family is not zero. Target log/segment IDs come from the ops record. Its absent
tenant field remains unresolved even if only one metric namespace was seen.

Initial outcomes are `suspected_stall`, `conflicting`,
`no_pending_work_observed`, `no_persistent_operation_observed` and `inconclusive`.
Evidence strength is separate: `inconclusive`, `suspected`, `corroborated` or
`conflicting`; these two conservative rules do not emit confirmed root causes or
promote findings by counting sources. Normal/idle observations apply only to the
sampled operation family and are not a global health assertion. Impact is
`unknown` when evidence is inconclusive/conflicting, `warning` for a suspected
stall, and `none` only when no persistent/pending work was observed within the
covered operation family. Unknown impact is never encoded as healthy/none.

The external tool can consume text-free stdout plus the documented exit code,
validating `schema_version` and preserving provenance. It needs no import of
wpcli internal Go packages. Breaking JSON changes require a new schema version.

## Exit codes and partial evidence

| Code | Meaning for `diagnose` |
| --- | --- |
| 0 | Evaluation completed, including suspicions/inconclusive/conflicting findings; **not healthy** |
| 1 | All required collection failed, or no targets discovered |
| 2 / 3 / 12 | Invalid usage / missing target / invalid connection configuration |
| 5 / 7 | Total budget exhausted / cancellation; collected report retained |
| 6 | Some required metrics/ops collection failed; report retained even without `--strict` |
| 8 | `--fail-on-findings` explicitly enabled and a suspected stall found |

Optional registry/build gaps remain visible without turning completed collection
into a transport failure. Required source failures take precedence over finding
exit codes. Usage/target-selection failures can occur before a report exists.
Stdout remains parseable in JSON/YAML modes; command errors go to stderr.

## Audit of legacy `metrics report`

Legacy formats, command names and finding exits remain unchanged. Operation
matching/persistence primitives are shared; legacy still uses its first/last
aggregate comparison. Native v1 adds per-sample evidence/coverage and conservative
conflict handling rather than silently changing that older output contract.

| Legacy scenario definitions | Actual evaluator behavior |
| --- | --- |
| `stuck-write`, `stuck-read` | Matching persistent first/last ops plus aggregate completion totals; EOF progress/reset exclusions; can mask one slow op with same-log progress |
| `stuck-flush` | Generic declared-histogram mean-delta check and a lifetime old-eviction counter check; not the literal condition strings |
| `slow-write`, `slow-read`, `slow-compact`, `slow-slot` | Generic histogram delta mean above fixed 5,000 emitter units when data is present; **not** the declared p99/baseline or cross-node ratio calculation |
| `hot-write`, `fencing`, `read-amplification`, `quorum-degraded`, `under-replication` | Descriptive definitions without their stated evaluators in the current implementation |

The legacy catalog's twelve names are not twelve completed diagnostic algorithms.
Only the two implemented native stall rules are exposed by `diagnose`. Expanding
WP rules belongs to #452/#453; Prometheus, trace, Loki, Milvus stages, integration
and WebUI remain separate program children under #450.
