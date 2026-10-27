# Integrated diagnosis CLI

`wpdiag run` implements the orchestration boundary of [#467](https://github.com/zilliztech/woodpecker/issues/467).
It plans and concurrently runs existing source collectors and Milvus stage
assessors, then presents their original findings in stage order. It works
headlessly with the operator's existing connections. Kubernetes, Helm and the
example stack are optional.

The runner owns selection, execution limits, progress and report composition.
Source modules own queries, thresholds and evidence interpretation. Stage
assessors own their hypotheses. The runner does not create additional health
rules or increase confidence because multiple sources repeat the same event.

## Start with a local plan

Build with `make wpcli wpdiag`. Copy and adapt
[`wpdiag.example.yaml`](../../cmd/wpdiag/wpdiag.example.yaml): each source keeps
its own named connection, endpoint, credentials and bindings. Configuring only
Prometheus is enough for metric diagnosis; native WP diagnosis additionally
needs an executable and an explicit WP service context.

```sh
bin/wpdiag run --list
bin/wpdiag run --list -o json
bin/wpdiag run --config wpdiag.yaml --scope e2e --flow write --plan
bin/wpdiag run --config wpdiag.yaml --scope e2e --flow write \
  --window 30s --budget 1m --progress -o json > run.json
```

`--list` needs no configuration. `--plan` validates the selection and private
connection settings locally, showing module/profile versions, child scope,
window, role, connection names and budgets. It does not contact backends or
spawn wp. A completed plan means its entries are ready; it does not establish
backend reachability or cluster health. Executed reports retain actual source
queries and times. Links and next checks remain suggestions until executed.

Text mode shows each module's execution status, up to five suspicious or
inconclusive findings, evidence counts and next checks. Critical/warning findings
come first within a module. `--all-findings` shows the rest; JSON retains the
original reports, support/contradiction and coverage gaps. `completed` means
execution completed, including when a source can only conclude `inconclusive`.

## Supported module matrix

`--sources auto` is the default. It considers configured, enabled connections,
omits inapplicable current-native/historical and interval/snapshot combinations
with explicit reasons, and selects the following modules:

| Scope / flow | Prometheus selection | Native WP | Trace / Loki |
|---|---|---|---|
| wp / write | `prometheus` WP overview | `wpcli` WP write stall | WP source overview |
| wp / read | `prometheus` WP overview | `wpcli` WP read stall | WP source overview |
| milvus / write | `milvus.append`, `milvus.persistence` | Auto omitted | Milvus source overview |
| milvus / read | `milvus.consume`, `milvus.query` | Auto omitted | Milvus source overview |
| e2e / write | `milvus.append`, `milvus.persistence` | WP write context | E2E source overview |
| e2e / read | `milvus.consume`, `milvus.query` | WP read context | E2E source overview |

All registered profiles currently have version 1. The machine-readable catalog
is authoritative for exact profile IDs, modes and target filters:

- `wpcli`: WP scope, snapshot/live, current runtime, node selection only. This
  adapter retains the focused `write_stall`/`read_stall` profiles; the expanded
  native `wp diagnose --scenario write|read` rules remain independently usable.
- `prometheus`: WP/Milvus/E2E overview, live/historical.
- `milvus.append|persistence|consume|query`: Milvus/E2E, live/historical,
  exact deployment/instance selection. Each uses its existing collector and
  pure stage assessor once; no second collection or copied rules.
- `trace`: WP/Milvus/E2E, snapshot/live/historical (snapshot requires an exact
  trace ID). `--trace-id` selects only the trace module.
- `loki`: WP/Milvus/E2E, live/historical.

Trace/log source overviews keep their declared profiles. Selecting a metric
stage does not turn a trace/log overview into a stage-specific diagnosis.
For example, the current Milvus trace read overview is a query profile, not a
complete WAL-consumption profile. Read the plan before executing a narrow run.

Use `--sources prometheus,wpcli` to require those sources. Missing, disabled,
unsupported or failed explicitly selected sources produce an explicit child
status; a successful sibling makes the aggregate `partial`, not a skipped
success. Auto selection omits only the documented locally inapplicable cases;
it never hides connection-validation or execution failures for selected entries.

`--stages append,persistence` or `--stages consume,query` selects metric stages
compatible with the flow and requires Prometheus with Milvus/E2E scope. A single
`--stage` is an alternative. `--modules milvus.append,trace` selects exact module
IDs and is mutually exclusive with stage selection. A source-only run is valid,
as is `--modules prometheus` for an overview. An exact `--profile` requires one
module. Unsupported filters/combinations are rejected before that child does IO.

## Connections and target identities

A source uses `--connections source=name`, then the shared `--connection`, then
its sole configured connection. Multiple connections require an explicit name.
There is no implicit fallback from a failed connection to another one.

```sh
bin/wpdiag run --config wpdiag.yaml --scope e2e --flow write \
  --sources prometheus,wpcli --stages append \
  --connections prometheus=metrics,wpcli=native \
  --target prometheus.deployment=example \
  --target wpcli.node_id=node-1 --plan
```

Common target flags apply to every module and are never silently dropped.
Repeated `--target source.field=value` builds a **complete replacement target**
for that source, rather than merging incompatible global filters. Include every
field needed by that source. `--trace-id` sets the trace-specific replacement.

Exporter `instance`, Loki Pod, native node ID and WP service context are distinct
identities. A WP context identifies the service cluster; it does not identify a
Milvus instance's WP-client etcd metadata prefix or tenant/log authority. Equal
numeric IDs and overlapping time ranges do not provide that mapping.

## Historical incident and current context

Native Admin/ops metrics are current observations, not retained history. Auto
historical runs omit native WP. To explicitly collect a separate current
snapshot alongside at least one historical source:

```sh
bin/wpdiag run --config wpdiag.yaml --scope e2e --flow write \
  --sources prometheus,wpcli --stages append --current-native \
  --connections prometheus=metrics,wpcli=native \
  --mode historical --from 2026-10-26T01:00:00Z --to 2026-10-26T01:05:00Z \
  --target wpcli.node_id=node-1 -o json > incident.json
```

The native child is labeled `current_context`, uses snapshot mode and retains
its actual collection time. It cannot contradict a historical slow append or
prove that a past incident did not happen. Without `--current-native`, explicitly
selecting historical native WP returns unsupported; it never queries current
state under a historical label. Current context alone is not a historical run.

## Evidence, associations and uncertainty

The `wpdiag.run/v1` envelope nests complete `wpdiag.report/v1` child reports.
`summary.findings` references `(module_id, original finding ID)`; stage reports
retain `derived_from_findings`. Rule versions, severity, evidence strength,
supporting/contradicting observation IDs, timestamps, query expressions,
unknowns and next checks stay source-owned. Summary views prefer the stage
assessment over displaying its underlying source findings a second time; both
remain in the child report. `Resolve` retrieves an original finding by reference.

Duplicate module reports are deduplicated. Conflicting module identities and
incompatible/dangling lineage produce explicit gaps and no derived association.
Independent hypotheses, including contradictory source evidence, remain intact.
Original UUID-based finding IDs are preserved within a run; separate collection
runs naturally have different IDs even when their diagnoses are equivalent.

The initial cross-source association supports **observed trace identity**:
Jaeger and Loki must bind the same configured deployment, report the same
nonzero canonical trace ID, and have compatible actual selected-span/log-event
times. It uses event timestamps, not HTTP query collection times. A trace reference retains the exact span ID. A log reference
includes its exact event ID as well as its page/finding IDs, so grouped log
findings cannot impersonate independent confirmations. This is related evidence,
not proof of causation or a new severity/confidence verdict.

No authoritative channel/collection to WP-client namespace/log mapping provider
is registered yet. Metric/native facts stay independent with an explicit gap;
the runner does not guess a join or clear an incident using unrelated healthy
facts. No latency quantiles are subtracted or added into an invented request
breakdown. Extended telemetry propagation/coverage remains tracked in
[#459](https://github.com/zilliztech/woodpecker/issues/459); it is not a dependency
of this runner. Report-file import is currently unsupported and explicitly
rejected without reading report files. In-process source reports supply the
validated lineage for this delivery.

## Bounds, API and future UI

`integrated.Standard().Plan(config, request)` and
`Run(ctx, config, request, events)` are the public interfaces for the CLI and
future [#457 WebUI](https://github.com/zilliztech/woodpecker/issues/457).
`New(definitions...)` registers another independent collector and optional pure
assessor without changing orchestration or putting UI logic into a source.
Cancel via the parent context (CLI SIGINT/SIGTERM). No HTTP server is introduced.

A run has at most eight concurrent modules. Its wall-clock budget is shared,
including live waits and analysis. Input/output byte allowances are partitioned
across selected modules; the final encoded envelope also honors the total output
bound. Each child requires at least 1 KiB input/4 KiB output. `--max-targets` is
per module, so the explicit total target ceiling is eight times that limit.
Sources retain their own request/page/sample bounds. A live run uses the same
requested duration but preserves each module's actual window; default native
interval is 1s and retained-source interval/gap limit is 30s, capped by duration.

Progress events are advisory, nonblocking and may be dropped. The caller owns
and drains the event channel; the final report is authoritative. The CLI emits
progress JSON on stderr and the final report on stdout. A bounded summary shows
at most 2048 finding references, 2048 identity anchors and 128 associations, with
at most 10000 comparisons per identity group; further evidence stays in child
reports unless an output budget requires a compact failure report.

Exit codes retain the core contract: completed 0, failed 1, invalid 2,
unsupported/disabled 3, budget_exceeded 5, partial 6, cancelled 7. A budget failure
never emits truncated JSON or claims the omitted evidence was checked healthy.

## Validation

```sh
go test -race ./diagnostics/... ./cmd/wpdiag/...
make wpcli wpdiag
python3 cmd/wpdiag/tests/integrated_smoke.py
```

The smoke fixture starts an isolated real Prometheus TSDB plus a local Admin
HTTP fixture, calls real wpdiag/wp binaries, checks all six metric selections
against standalone diagnoses, and checks historical/current separation,
source errors, evidence lineage, bounds and cancellation. It removes its own
container and temporary data. Unit tests additionally cover parallel execution,
exact assessor reuse, duplicate/conflicting evidence, compatible trace/log event
identity and unrelated same-time anomalies. This is orchestration acceptance;
full deployed Milvus incident validation belongs to
[#458](https://github.com/zilliztech/woodpecker/issues/458).
