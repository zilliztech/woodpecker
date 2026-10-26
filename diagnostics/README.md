# Diagnostic module contract v1

The public Go package `github.com/zilliztech/woodpecker/diagnostics` provides
the common interface for the standalone `cmd/wpdiag` frontend and later
integration/UI work. It does not contain a diagnostic rule engine. The first
adapter (`diagnostics/wpcli`) imports `wp.diagnosis/v1` through a subprocess,
without importing wpcli Go packages or parsing terminal prose.

## Registration and execution

Implement `Module.Descriptor`, `Module.ValidateConnection` and `Module.Run`.
Register the module with `NewEngine(module, ...)` and expose its source command
in the frontend. Registration is compile-time; there is no dynamic plugin
loader, expression language or automatic backend discovery.

`Descriptor` declares the source, issue, profiles and required private
connection settings. A versioned profile declares scope, flow, stage, modes and
target filter fields. Optional stage/profile selection must resolve exactly
one profile; an ambiguous match is unsupported rather than an arbitrary choice.
`Engine.Catalog()` reports available profiles and explicitly unimplemented
source slots. No configuration or network access is needed to obtain it.

`Config` uses `wpdiag.config/v1`, named connections under each source, an optional
enabled switch and an opaque YAML settings map. `DecodeSettings` strictly
decodes a module-owned schema. Unknown envelope/settings fields fail without
echoing their values. Local config reads are capped at 1 MiB; no config writes
are performed. Settings and base directories are not JSON-serializable.
ValidateConnection must be local and side-effect free. Run receives a validated,
resolved Request, selected private Connection and a cancellable execution
context. Use only bounded read-only operations and honor the context.

The first Engine version runs one source, with no fallback to another. A source
module can assess its own cross-component evidence under scope=e2e; that does
not grant it evidence from other sources. Multi-source execution and synthesis
remain #467; Milvus stage rules are separate modules (#463–#466).

## Selection

| Field | Meaning |
| --- | --- |
| sources | wpcli, prometheus, trace, loki; deduplicated, but v1 runner accepts exactly one |
| scope | wp, milvus, e2e |
| flow | write, read |
| stage | Stable profile-declared stage ID; append/persistence are write, wal_read/consume/query are read |
| target | Explicit deployment, instance, node, log, segment, reader, collection, channel or operation identities |
| window | snapshot; live starting now with positive duration/interval; historical from < to with actual retained timestamps |

Consume and query are Milvus stages and cannot be declared WP-native.
Additional stage IDs use lowercase letters, digits and underscores and must
match a registered profile. Target filters are validated against that profile;
they are never silently dropped. Log/segment IDs are canonical nonnegative
int64 decimal **strings**, retaining precision for browser clients.

Observed service cluster, identity verification and tenant association are
evidence annotations, not authoritative selection assertions. The trace source
also accepts a nonzero 16/32-digit hexadecimal trace-ID selector, normalizes it
losslessly and checks the returned identity. Other sources cannot accept that
selector as an unverified association assertion.
In particular, a metric's log_ns plus an ops log ID does not establish the
authoritative Milvus-client metadata namespace. Native import preserves this
association as unresolved.

`diagnostics/jaeger.Module` owns the source-only Jaeger profiles, connection,
read-only query, span normalization and relationship rules. `trace`/`jaeger`
CLI aliases use that same module; future stage/UI callers must reuse its public
results rather than copying thresholds or inferring a critical path. See the
[trace contract](jaeger/README.md). Request-tag selection remains a candidate,
with anchor/descendant identity and uniqueness gaps preserved.

Snapshot omits live/historical endpoints. Live cannot specify historical
endpoints; historical requires explicit from/to and no live duration. Modules
declare which modes they can actually retrieve. Never substitute current ops
for historical evidence or conflate WAL read progress with query latency.

## Independent query-stage assessment

`milvusquery.Run` uses the public request/config/report interfaces with a
Prometheus query-step collector. `milvusquery.Assess(ctx, report)` applies the
same stage business rules to an existing supported source report without I/O
or mutation. Source rule implementations, thresholds and IDs stay source-owned.
Optional `Report.Assessments`, `Finding.DerivedFrom` and `Catalog.Stages` are
additive v1 contract fields; existing source-only reports are unchanged. See
[the query-stage guide](milvusquery/README.md) for applicability and reuse.

## Independent consume-stage assessment

`prometheus.ConsumeModule` collects bounded scanner, subscription, consumer and
optional WP retrieval facts. `milvusconsume.Assess(ctx, report)` interprets those
facts without I/O, mutation or copied histogram thresholds. It preserves IDs and
requires matching sample identities/times for corroboration; normal blocking
ReadNext/EOF and idle are not stalled-reader evidence. E2E WP populations remain
unassociated when no authoritative client-metadata mapping exists. See the
[consume-stage guide](milvusconsume/README.md) for catalog, counter/gauge contracts
and optional-source boundaries. This module is reusable by later integration/UI.

## Reports, findings and lineage

`wpdiag.report/v1` includes the normalized selection, limits, execution run/time
and source Result. The outer run timestamps describe execution. Each
Observation keeps its actual timestamps and ID/source_id. Result metadata
identifies the producer's original run/schema. Identity is therefore scoped by
`(source, native_run_id, observation_id)`; reused source IDs across sample rounds
are not independent sources of corroboration.

Findings keep rule ID/version, flow/stage/target, outcome, severity (impact),
evidence strength, summary, supporting/contradicting observation IDs, unknowns
and suggested next_read_only_checks. Impact and strength are separate. A source
result is not a second independent proof of its own raw observations. Do not
turn missing evidence, zero matches, stale data or a successful request into a
health assertion. Unsupported, disabled, invalid, failed, partial,
budget_exceeded and cancelled are explicit execution states.

Observation.Data is module-owned sanitized JSON; it is not arbitrary raw backend
output. Optional SourceQuery describes reproducibility and whether a query was
executed. Suggested checks are not executed evidence. Native v1 supplies source
kinds and its own collection/operation data but no exact query records, so its
adapter leaves Query absent rather than inventing executed queries. Future
clients may include credential-free query/link expressions and actual source
timestamps through these same fields. Do not export authenticated URLs, config
settings, raw errors or credentials.

The native adapter validates schema, rule version/profile, context/window,
status/exit, target membership, timestamps, limits and evidence references. It
retains IDs, rule applicability/comparison/exclusions and actual observations;
it does not re-evaluate the rules. Identical repeated observation IDs are
deduplicated with a coverage gap. Conflicting IDs, ambiguous node/sample/kind
lineage, duplicate finding IDs and dangling/cross-node evidence fail closed.
Native read ambiguity, contradictory evidence and unknown tenant association
remain visible. IDs above 2^53 remain exact strings after import.

## Budgets and extension acceptance

Core Limits bound execution time, input bytes, output report bytes and targets.
Modules own additional sample, series, query and concurrency budgets. They must
bound data **before** decoding/retaining it and pass cancellation to every
request/process. EncodeReport emits a full report or compact valid budget error,
never truncated JSON. The native adapter runs one subprocess, caps both pipes,
attempts interrupt cancellation and kills after a short grace period.

A new module should ship capability/selection fixtures, source failure and
unsupported-mode tests, real bounded-client cancellation, schema/identity and
deduplication checks, a credential-leak fixture and versioned example results.
Use the common contracts rather than importing another module's internal rules.
See [the standalone tool](../cmd/wpdiag/README.md) for runnable examples and
[the native producer fixtures](../cmd/wpcli/diagnosis/testdata/) for the real
ambiguous-read/scoped-write import cases tested by this adapter.
The [normalized v1 report fixture](wpcli/testdata/scoped-write-import-v1.json)
shows the consumer envelope. Regenerate it after an intentional compatible
contract change with `go test ./diagnostics/wpcli -run TestImportedReportFixture -update`.

Standalone [Loki log diagnosis](loki/README.md) owns its connection, six initial profiles, bounded queries and known-message evidence catalog.

`milvusappend.Run` and pure `milvusappend.Assess` provide the independently runnable [append-stage assessment](milvusappend/README.md), reusing source-owned metric rules with explicit WP mapping gaps.

`milvuspersistence.Run` and pure `milvuspersistence.Assess` provide the independently runnable [persistence/flush/checkpoint assessment](milvuspersistence/README.md), preserving source-owned rules and explicit WP parallel-path/mapping gaps.
