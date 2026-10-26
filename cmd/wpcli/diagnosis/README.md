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
wp --context local diagnose --scenario write --window 30s --max-writers 16 -o json
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
Optional `--log-name NAME --segment ID --etcd ENDPOINTS --meta-prefix PREFIX`
explicitly selects a WP-client namespace and reads that segment's inline quorum
in one read-only etcd transaction. No fallback to a server namespace is allowed.
Missing/invalid metadata does not broaden writer collection to unrelated logs.
This selection focuses writer details and replica checks; append-stall checks
still cover the selected nodes' sampled operations. Do not interpret the metadata
selection as establishing tenant ownership of every runtime operation.

## Supported checks and limits

The focused typed rules are `write_stall`/`stuck-write` and
`read_stall`/`stuck-read`. `write` includes `write_stall`, `flush_buildup`,
`disk_admission` and `replica_participation`; `all` includes all five.
`--list -o json` exports their versions, flow/stage,
required/optional evidence, applicability, comparison and exclusions.

- **Snapshot:** assesses current disk admission and captures evidence; cannot
  establish sustained no-progress, queue buildup or replica lag.
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
completion-counter family and process start gauge are retained from `/metrics`
for the focused stall scenarios. `write`/`all` additionally retain the four
allowlisted disk-policy gauges, with node/path labels and explicit timestamps.
Existing op trace/span IDs are retained where provided; they do not establish
connection to a Milvus user trace by themselves.

Default limits are 8 nodes, 31 rounds/node, 100 ops/sample, 500 retained counter
series/sample, 2 MiB/response and a 1-minute total budget. `--timeout` bounds each
request, and `--concurrency` bounds parallel node collection. Hard limits also
bound the products of nodes × samples × records. Increase limits explicitly for
longer windows; invalid/unbounded requests are usage errors.
`write`/`all` additionally permit 16 writer details/node/round by default, set by
`--max-writers` (1..64). Nodes × rounds × writers must not exceed 20,000.
The server has no list pagination: its response is byte-bounded, then details are
requested in deterministic log/segment order up to the writer limit. Exceeding
the limit is explicit partial evidence, never a healthy subset. Writer APIs are
required for these scenarios; failures return 6 (or 1 if all required sources
fail). Unsupported older server APIs remain gaps. Each round performs three base
requests, one writer list and at most `max-writers` detail requests; build is read
once. Optional metadata uses one bounded read-only transaction. All share the
same time, concurrency and response-byte limits.
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
| 8 | `--fail-on-findings` explicitly enabled and a warning/critical finding found |

Optional registry/build gaps remain visible without turning completed collection
into a transport failure. Required source failures take precedence over finding
exit codes. Usage/target-selection failures can occur before a report exists.
Stdout remains parseable in JSON/YAML modes; command errors go to stderr.

## Write rule interpretation

| Rule | Evidence and trigger | Exclusions and limitations | Next read-only checks |
| --- | --- | --- | --- |
| `write_stall` v1 | Same append operation across samples, without matching-log completion growth (counts) | Existing conservative stall contract above; same-log progress is conflicting, not completion of this op | `ops list/show/stats`, matching log metrics |
| `flush_buildup` v1 | Same node/log/segment/backend throughout the live window; queue stays full, stays nonempty without durable advance, or queue grows/buffer grows with persistent queue occupancy | Buffer growth without a continuously pending queue may be normal batching and is not a pressure verdict. Recovered transient growth is identified separately. Restart, durable reset, writer/queue-capacity/lifecycle change, partial detail coverage and snapshot are inconclusive. Idle/draining is not a stall. A persistent matching `file.flush` is context, not an independent cause confirmation | `logstore buffer`, `flush-queue`, `segment-show`, `ops list --type file.flush` |
| `disk_admission` v1 | Latest node-labelled usage versus exported soft/hard ratios and state 0/1/2 | Disabled policy is explicit. Unverified timestamps, missing/ambiguous gauges and policy/state contradictions remain gaps. State 2 below hard may be an absolute free-space floor: do not call it a ratio breach. State 1 does not establish rejected traffic | `metrics snapshot`, `config show`, client errors/rejected-write evidence |
| `replica_participation` v1 | Explicit Active-segment quorum, complete stable writer observations from its discovered replicas; one stays behind as peers advance or its gap grows | Compare durable entry IDs, not latency. Missing/unloaded replica, reset, unavailable API, unselected member or unresolved identity is incomplete observation, not an outage. Idle is not faulty. Metadata may change after its snapshot; runtime tenant mapping is operator-selected | `logstore lac NAME SEGMENT` with the same explicit etcd/prefix; client acknowledgement evidence |

New outcomes include `observed_buildup`, `queue_saturated`,
`no_durable_progress_observed`, `no_buildup_observed`,
`no_loaded_writer_observed`, `buffer_growth_observed`, `transient_buildup_observed`,
`observed_admission_pressure`, `policy_disabled`,
`below_configured_watermarks`, `suspected_replica_lag` and
`no_replica_lag_observed`. Positive pressure findings are **suspected**, with
warning impact (critical for reported blocked admission); normal sampled state
is local to that observation and never a global health assertion. No rule promotes
confidence by counting duplicate API views. Queue tasks, bytes, entries and
durable entry IDs are reported in their original units. Sampling windows are
operator-selected, without a universal latency SLO.

`maintenance_object_store_pressure_not_assessed` deliberately remains a gap.
The deployed backends expose different maintenance paths; a queue or slow flush
does not prove compaction contention, object-store throttling, local disk latency
or fsync. Inspect existing `file.compact` operations and backend-specific storage
evidence before attributing cause. No server instrumentation is added here.

The new writer, signal and quorum observation fields and writer limit are additive
v1 fields, emitted only by `write`/`all`. Focused v1 stall calls made by the
standalone tool remain unchanged and are verified by its subprocess smoke test.
The standalone tool's existing `wp.write_stall` profile continues importing only
that focused assessment; it does not advertise the expanded write scenario yet.
Future stage integration can import the new contracts without copying WP rules.

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
`diagnose` exposes the two native stall rules plus the three write-pressure rules
above. Native read expansion belongs to #453; Prometheus, trace, Loki, Milvus stages, integration
and WebUI remain separate program children under #450.
