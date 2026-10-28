# Independent Milvus persistence-stage diagnosis v1

`wpdiag milvus persistence` assesses growing-data pressure, flush scheduling,
sync/upload timing and persisted frontier/checkpoint progress using an independent
Prometheus connection. No native wpcli/Admin, trace, Loki, integrated runner or
WebUI is required. It performs bounded read-only queries, with text or versioned
JSON findings, original evidence, coverage gaps and next checks.

```bash
make wpdiag
bin/wpdiag milvus persistence --list
# Configure sources.prometheus.connections.stack in wpdiag.yaml. Replace these
# example UTC times with an actual retained workload interval.
bin/wpdiag milvus persistence --config wpdiag.yaml --connection stack \
  --scope e2e --mode historical \
  --from 2026-10-26T01:00:00Z --to 2026-10-26T01:01:00Z -o json
# Observe the interval starting now, with a separately run bounded workload.
bin/wpdiag milvus persistence --config wpdiag.yaml --connection stack \
  --scope milvus --window 30s --budget 1m
```

Use the [connection example](../../cmd/wpdiag/wpdiag.example.yaml) and
[Prometheus connection/authentication guide](../prometheus/README.md). Exact
`match_labels` bind a deployment, such as cluster/namespace/woodpecker_id in the
Helm example. A connection's `deployment` is an explicit binding, not an invented
Prometheus label. Authentication secrets and backend errors are not exported.

## Capabilities and boundary

| Profile | Scope | Flow / stage | Modes | Supported target filters |
|---|---|---|---|---|
| milvus.persistence.steps.metrics v1 | milvus | write / persistence | live, historical | deployment, instance |
| e2e.persistence.steps.metrics v1 | e2e | write / persistence | live, historical | deployment, instance |

Milvus scope collects 14 signals in groups 1–4 below. E2E additionally collects
four WP signals as **parallel, unassociated maintenance context**. `instance`
is an exact exporter filter applied to every query; selecting one SN instance
may leave DataCoord/WP evidence absent. WP-only scope, read flow, snapshot,
other stages, arbitrary queries and collection/channel/log/segment selection
are unsupported and fail explicitly. A requested unavailable/nonimplemented
source does not silently fall back to another configured connection.

The initial collector supports Prometheus only. Native write-stall, flush,
disk and replica rules remain owned by `wp diagnose --scenario write` (#452).
Native, trace and Loki enrichment is not collected by this stage; use those
independent commands for the suggested bounded next checks. No runtime rules
are copied, and no external client is added to wpcli/stack.sh. #467 owns future
cross-source orchestration. There are no force-flush, fencing, compaction,
restart, workload injection or crash-durability operations here.

## Versioned hypothesis catalog

[PersistenceCatalog](../prometheus/persistence.go) owns exact metric names,
selectors, example thresholds and units. `--list -o json` exposes that contract.
The initial Milvus definitions are pinned to the Helm example's
`27bd28d20688ab00cdd74232a20e988fb2162aed`, alongside WP metric definitions.
Dispatcher/recovery metrics may be absent on other builds; absent/lazy series
remain gaps, not healthy zeros.

| Group / signals | Trigger and interpretation | Applicability, exclusions and next checks |
|---|---|---|
| 1 Growing data: growing bytes, flush-pressure bytes, HWM | Exact same population and sample times: pressure ≥ actual configured positive HWM throughout retained scrapes → suspected growing flush pressure | Growing bytes alone can be normal batching. Intermittent crossings remain observations. After a crossing, final two samples below HWM qualify earlier pressure; this does not establish full LWM/hysteresis recovery. Zero HWM is uninitialized/disabled, not a valid threshold. Check configured policy and demand. |
| 2 Flusher state / pending sync work | State and pending task observations; persistent/intermittent/zero/final-two-zero pending states are distinguished | Pending includes **queued and executing** work. It is neither pure queue length nor configured capacity. Flusher state can indicate recovery; it does not prove completion or durability. Inspect recovery state and intended traffic. |
| 2 Sync queue time | Source-qualified high queue latency plus exactly one same-population pending gauge with matching actual interval and positive minimum → suspected scheduling pressure | Zero/recovered pending evidence contradicts sustained current pressure without disproving earlier delay. Missing/ambiguous/differently timed evidence leaves only slow queue completions. No request-specific or capacity claim. |
| 3 Sync execution / save / storage put | Source-qualified high completion timer → the corresponding suspect execution/save/upload step | Sync execution includes object upload **and callbacks**; save and successful put timers have different scopes. Put is the process's successful client-side storage timing, not isolated network or object-store server time. Errors and pending operations are not represented by these timers. Inspect bounded logs/trace before assigning cause. |
| 4 Seen / in-memory frontiers | Report advancement or no change, with actual channel/term and sample times | Flusher-seen and in-memory markers are not durable checkpoints. TimeTicks can advance without payload demand. Do not derive user durability or request latency from them. |
| 4 Persisted recovery frontier | Same exact population including channel/term and aligned sample times: frontier remains below advancing in-memory frontier, stays unchanged, and persisting=1 throughout → suspected recovery snapshot persistence delay | This identifies ongoing snapshot work with no exported frontier advance, **not a stall/storage failure verdict**. Snapshot cadence/duration and user demand remain unknown. No universal time SLO is applied. Equal final two exported physical-second markers or advancing persisted markers are reported separately; equality is not payload verification or complete log durability. Missing busy/mapping/times cannot trigger this suspicion. |
| 4 DataCoord checkpoint | Report its movement and age at the **final retained scrape**, not the current wall clock | Idle age/unchanged position is not a stall. Virtual DataCoord channels are not joined to physical WAL channels. Zero/future timestamps and regressions remain inconclusive. Check expected data and checkpoint cadence separately. |
| 5 WP background context | WP local flush, compaction and successful object-storage completion timers; exported 0=normal/1=warn/2=blocked disk admission | A parallel WP maintenance/offload path, not the next serial Milvus flush step. No inferred Milvus/WP tenant association. Slow compaction/flush alone does not prove object-store contention, disk/fsync failure or WAL durability loss. Inspect native #452 findings and mapped ops/logs separately. |

The gauge associations above use the source's opaque **full population hash**,
which preserves all label dimensions including unrecognized ones. Different
instances, channels, terms or hidden dimensions do not join. Exact gauge sample
timestamps must match; there is no interpolation, extrapolation or nearest-time
join. Histograms compare actual interval bounds with pending gauges, not
unrelated p95/p99 values. Identical API views are not independent evidence.

Histogram thresholds and temporal checks are reused from the source, not
recomputed in the stage: examples are queue 250ms, sync/save/put and WP compaction/
storage 1000ms, WP local flush 100ms. Override supported histogram IDs under
`settings.policy.latency_ms`. These are example policies, not product SLOs.
Queue/execute metric seconds are converted to milliseconds; save/put/WP timers
use milliseconds. Source minimum completion count, quantile bucket bounds,
scrape coverage and reset checks still apply. A normal completion population
is not a whole-service health assertion; idle/missing/insufficient evidence
stays idle or inconclusive.

Recovery, seen and DataCoord gauges export **physical Unix seconds**, not raw
uint64 hybrid timestamps. The stage checks every retained sample for zero,
future/clock-incompatible values and backwards movement before interpreting
progress. Physical-second equality loses logical timestamp information. Restarts
or activity between scrapes can remain unobservable. State indicators outside
the documented 0/1 or 0/1/2 sets are unsupported, not normal states.

## Evidence, mapping and reuse

Reports retain exact executed PromQL, raw retained samples, source facts/policies,
actual query/sample times and collection errors. Persistence gauge facts include
`retained_samples` so the assessor can compare actual times without reconstructing
history. Existing overview/query gauge facts keep their earlier serialization.
Stage findings cite original observation IDs and `derived_from_findings`; they
are interpretations of the same evidence, not a second independent confirmation.
Suggested checks were not executed. Partial errors preserve usable other steps.

A shared deployment/window/exporter or numeric WP log ID is not an authoritative,
time-valid Milvus instance/collection/channel → WP-client metadata namespace/log/
segment/node mapping. A WP service context is separate from the client's metadata
authority. This initial collector does not establish that mapping: E2E WP results
remain explicitly unassociated, even if both Milvus and WP look slow. No timer
quantiles are added or subtracted to invent queue, callback or network residuals.

`milvuspersistence.Run(ctx, config, request)` uses `prometheus.PersistenceModule`.
`milvuspersistence.Assess(ctx, report)` applies the same business rules with no I/O
or mutation to a compatible source report. It validates versions, profile,
selection/window, identities, references and retained gauge-sample consistency.
Exact duplicates are collapsed; conflicting evidence fails closed. Integration
and UI should call the same assessor and preserve IDs/coverage.

The [sanitized v1 example](testdata/snapshot-delay-v1.json) projects one actual
fixture finding together with its source findings and referenced observations.
It is an example projection, not the complete report envelope.

## Bounds and verification

Persistence plans default to a 24-query limit (14 Milvus / 18 E2E actual queries).
Shared total wall-clock/input/output/target budgets and source series/sample/
response/window/timeout limits apply. `--interval` bounds tolerated retained
scrape gaps; it does not resample or reconfigure Prometheus. Output overflow
returns a compact valid budget failure. Existing overview/query defaults stay
unchanged. Execution completion does not mean healthy.

```bash
go test -race ./diagnostics/... ./cmd/wpdiag/cli
make wpcli wpdiag
python3 cmd/wpdiag/tests/persistence_smoke.py
```

The isolated real Prometheus 3.0.1 test uses synthetic retained TSDB fixtures,
covering 40 scope/scenario selections including pressure, recovery, idle,
wrong term, no ongoing persisting, incomplete series and resets. It sends no
application traffic and cleans up its temporary backend. Unit tests additionally
cover exact-time/hidden-dimension mismatch, source failures, wrong targets/time,
contradictions, duplicate/conflicting lineage, unsupported selections, credential
redaction, cancellation and immutable repeated assessment. To intentionally
update the versioned example, run `go test ./diagnostics/milvuspersistence -run
TestVersionedExample -update-persistence-fixture`.

For real traffic, retained default-policy sync completion findings and reversible
object-store slowdown, use the separate opted-in
[deployed acceptance guide](../../tests/integration/diagnostics/PERSISTENCE.md)
and [actual receipt](../../tests/integration/diagnostics/PERSISTENCE_VALIDATION.md).
Only that disposable-target harness mutates workload/storage state; diagnosis
remains read-only. Other persistence hypotheses retain their documented fixture
coverage; this representative case does not prove physical disk or crash durability.
