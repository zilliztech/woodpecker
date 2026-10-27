# Independent diagnostics (`wpdiag`)

`wpdiag` is a separate diagnostic binary for the modular operations roadmap
in [#450](https://github.com/zilliztech/woodpecker/issues/450). It exposes common
connection, capability, selection and report contracts outside wpcli and the
Helm stack. The native adapter (#454) runs the WP evidence collector from #451.
The independent Prometheus module (#460) queries retained metrics and evaluates
bounded WP/Milvus/E2E read/write profiles. The independent Milvus query-stage
command (#466) interprets query-step metric facts. The independent consume-stage
command (#464) interprets scanner, subscription, processing and visibility
evidence. The independent Jaeger module
(#461) diagnoses retained spans and relationship coverage. The independent Loki
module (#462) diagnoses known log events with bounded candidate queries and
explicit event/identity lineage. The integrated runner (#467) composes these
modules while preserving their independent evidence and coverage limits.

## Local interactive WebUI

```sh
make wpcli wpdiag
bin/wpdiag serve --config wpdiag.yaml
# Open the printed loopback URL, normally http://127.0.0.1:8787.
```

Choose an independent source/stage or an integrated diagnosis, preview its plan,
run/cancel collection, and inspect the same findings and original evidence as
CLI/JSON. Connections are selected from the existing private YAML; restart after
editing it. The binary embeds its UI assets and needs no separate frontend
service or npm runtime. Recent reports are bounded and held in memory only.
See the [WebUI guide](webui/README.md) for the operator workflow, local API,
limits, evidence interpretation and browser tests.

## Integrated run

`wpdiag run` combines the independent modules using the same source-owned
connections and rules. It needs no example stack or UI. Start with a local plan:

```sh
bin/wpdiag run --list
bin/wpdiag run --config wpdiag.yaml --scope e2e --flow write --plan
bin/wpdiag run --config wpdiag.yaml --scope e2e --flow write \
  --sources prometheus,wpcli --stages append --window 30s --progress -o json
```

Choose read stages with `--flow read --stages consume,query`; select exact modules
with `--modules milvus.query,trace,loki`. Explicit source failures remain visible
and yield partial/error status. Historical native context requires the explicit
`--current-native` option and stays a separately labeled current snapshot.

See the [integrated guide](../../diagnostics/integrated/README.md) for the auto
plan, supported matrix, connection/target overrides, byte/time budgets,
`wpdiag.run/v1` lineage, association limits and the reusable run/cancel/progress
API shared with the local WebUI. Missing client-metadata mappings are reported;
current observations do not clear historical incidents. Raw report import is
currently unsupported.

The [deployed CLI acceptance scenario](../../tests/integration/diagnostics/README.md)
checks normal traffic, a controlled admission slowdown, and recovery against an
explicitly opted-in disposable stack. It verifies client correctness/latency,
standalone/composed findings and retained history with current native context.
This is the first subset of #458; it does not require the WebUI.
The [loaded query/search acceptance guide](../../tests/integration/diagnostics/QUERY.md)
uses a server-derived Customized guarantee timestamp and paired Strong controls to
validate TSafe-wait findings, exact results and historical/composed consistency.
It does not change diagnostic thresholds or shared deployment configuration.

## Build and run

Run from the repository root, using Go 1.25 or later:

```bash
make wpcli wpdiag
bin/wpdiag --help
bin/wpdiag capabilities
bin/wpdiag capabilities -o json
```

Both binaries are source builds. An older published `wp` without
`wp diagnose` / `wp.diagnosis/v1` is not compatible.

Prepare a reachable WP service context using the existing
[wpcli setup](../wpcli/README.md) or
[stack connections workflow](../../deployments/milvus-full-stack-helm/README.md).
The stack and external monitoring systems are optional. `wpdiag` needs a named
connection referring to an executable, a wpcli context and optionally a cli.yaml:

```yaml
schema_version: wpdiag.config/v1
sources:
  wpcli:
    connections:
      local:
        settings:
          executable: /absolute/path/to/woodpecker/bin/wp
          context: wp-ops-v0146/milvus-stack
          cli_config: /absolute/path/to/stack/.local/wpcli.yaml
```

Save this as `wpdiag.yaml` and substitute your actual paths/context. See
[wpdiag.example.yaml](wpdiag.example.yaml) for configurable native collection
limits and independent Prometheus, Jaeger and Loki settings. Connection references
are source-local; they are not another Kubernetes or wpcli context.

```bash
# Read current native evidence; a snapshot cannot establish sustained progress.
bin/wpdiag wpcli diagnose --config wpdiag.yaml --connection local \
  --scope wp --flow write --mode snapshot

# Observe writes starting now, then retain the complete versioned report.
bin/wpdiag wpcli diagnose --config wpdiag.yaml --connection local \
  --scope wp --flow write --stage append \
  --mode live --window 30s --interval 1s --budget 1m -o json > write-report.json

# Observe native WAL reads, optionally selecting an actual discovered node ID.
bin/wpdiag wpcli diagnose --config wpdiag.yaml --connection local \
  --flow read --stage wal_read --node-id YOUR_NODE_ID \
  --window 30s --interval 1s -o json > read-report.json
```

The native adapter invokes exactly one read-only `wp diagnose` subprocess per
native run. No
shell parses the arguments. It does not read, rewrite or attach etcd metadata
to cli.yaml. The optional cli_config path is passed to wpcli via
`WOODPECKER_CLI_CONFIG`; if omitted, wpcli resolves its normal default file.
Ambient `WOODPECKER_ENDPOINT` and `WOODPECKER_CLI_CONFIG` are removed from the
child environment so they cannot silently select a different cluster. The
reported native context must match the configured context. Relative executable
paths containing a separator and cli_config paths resolve against wpdiag.yaml's
directory; a bare executable uses PATH.

## Prometheus diagnosis

Configure the independent Prometheus connection in
[wpdiag.example.yaml](wpdiag.example.yaml), using your actual deployment labels.
It requires no wpcli executable or Admin access:

```bash
bin/wpdiag prometheus diagnose --config wpdiag.yaml --connection local \
  --scope e2e --flow write --mode historical \
  --from 2026-10-25T08:00:00Z --to 2026-10-25T08:05:00Z -o json
```

Replace the example UTC times with your retained workload interval. See the
[Prometheus guide](../../diagnostics/prometheus/README.md) for all six profiles,
metric units, policy thresholds, authentication and evidence limits.

## Jaeger trace diagnosis

Use a separate trace connection with the Jaeger Query endpoint, deployment and
exact service/component bindings. No wpcli or Prometheus connection is needed:

```bash
bin/wpdiag trace diagnose --config wpdiag.yaml --connection local \
  --scope e2e --flow write --mode snapshot --trace-id YOUR_TRACE_ID -o json
```

Replace the ID with an actual retained 16/32-digit hexadecimal ID. Snapshot reads
immediately; live/historical modes have explicit event-time bounds. Service/
operation searches are bounded and configured per connection; `--request-id`
requires a configured request-ID tag. The `jaeger` command is an alias of `trace`.
See the [Jaeger guide](../../diagnostics/jaeger/README.md) for the six profiles,
auth, thresholds, limits and partial-trace interpretation. Long ReadNext tailing
is not automatically a fault; overlapping/async spans are not added into client
latency, and separate traces are never merged by time proximity.

## Supported boundaries

For a query/search symptom, use the separate stage command. It works with the
same Prometheus connection and adds step-level findings without requiring other
monitoring sources:

```bash
bin/wpdiag milvus query --list
bin/wpdiag milvus query --config wpdiag.yaml --connection local \
  --scope milvus --mode historical \
  --from 2026-10-26T01:00:00Z --to 2026-10-26T01:05:00Z
```

Replace the bounds with your retained workload interval. See the
[Milvus query guide](../../diagnostics/milvusquery/README.md) for the five groups,
queue recovery interpretation, source policies and E2E mapping limitations.

For WAL consumption or growing visibility delay, use the separate consume
command with the same Prometheus connection:

```bash
bin/wpdiag milvus consume --list
bin/wpdiag milvus consume --config wpdiag.yaml --connection local \
  --scope e2e --mode historical \
  --from 2026-10-26T01:00:00Z --to 2026-10-26T01:05:00Z
```

See the [consume-stage guide](../../diagnostics/milvusconsume/README.md) for the
five groups and 20-signal catalog. Scanner/processing pressure requires matching
evidence; normal blocking ReadNext, EOF, idle and local/memory paths are not
reader-stall verdicts. WP observations remain unassociated until authoritative
client-metadata mappings exist. Native/trace/Loki enrichment is not collected.

| Source | Scope / flow / stage | Modes | Target filters | Status |
| --- | --- | --- | --- | --- |
| wpcli | wp / write / append | snapshot, live | node_id, or discovered nodes within budget | Implemented: native write_stall v1 |
| wpcli | wp / read / wal_read | snapshot, live | node_id, or discovered nodes within budget | Implemented: native read_stall v1 |
| Prometheus | WP/Milvus/E2E read/write profiles | live, historical | Explicit profile-declared deployment/instance/node filters | [Implemented catalog and limits](../../diagnostics/prometheus/README.md) |
| Trace (Jaeger) | WP/Milvus/E2E read/write profiles | snapshot with exact ID; live, historical | deployment, trace_id, configured request-ID tag | [Implemented catalog and limits](../../diagnostics/jaeger/README.md) |
| Loki | WP/Milvus/E2E read/write profiles | live, historical | deployment, instance (Pod), operation_id (request ID) | [Implemented catalog and limits](../../diagnostics/loki/README.md) |

`capabilities` is authoritative for implemented profiles. Invalid or unsupported
profile/source selections remain explicit; missing evidence is not healthy.
Historical input is represented by `--mode historical --from RFC3339 --to
RFC3339`; wpcli rejects it before spawning a process. It never relabels current
ops or metrics as historical. Log/segment/reader/collection/channel filters
exist in the shared selection contract but are explicitly unsupported by the
native profiles. `--node-id` requires the actual node ID, not an address alias.

WAL consumption, Milvus visibility and search are distinct steps. Background
persistence is a write-related branch, not another duration to add to an
append request. A single-source E2E result only assesses evidence from
that source. The [integrated runner](../../diagnostics/integrated/README.md)
composes available modules with explicit identity/time and mapping limits.
The local WebUI uses these same module and integration boundaries.

## Read a result

The text view summarizes outcome, evidence strength, impact and unknowns.
Use JSON for observations, original finding IDs, source/sample lineage,
supporting and contradicting references, coverage gaps and unexecuted next
checks. `results[].metadata` records the native schema/run/context, actual
collection times, limits, exit code, targets and rule contracts.

Collection `completed` / exit 0 means the command evaluated the covered native
rules, including suspected, conflicting or inconclusive findings. It is not a
cluster-health assertion. A snapshot cannot establish a stall. Empty or missing
evidence is never converted to healthy zeros. Registry loss, counter resets,
unknown tenant association and non-atomic sampling retain their native caveats.
Neither this adapter nor native v1 confirms a root cause. Trace IDs already
present on operations do not establish a connected E2E trace.

| Exit | Execution status |
| --- | --- |
| 0 | completed, independently of findings |
| 1 | failed collection, incompatible native protocol or executable failure |
| 2 | invalid selection, connection, budget or command arguments |
| 3 | unsupported profile/source/filter or disabled connection |
| 5 | wall-clock, input/output or report byte budget exhausted |
| 6 | partial required-source collection |
| 7 | cancelled |

For native report exits 0/1/5/6/7, the collection status must agree with the
exit code. The adapter never requests `--fail-on-findings`; an unexpected native
exit 8 or an unknown schema/rule version fails closed. Raw stdout from invalid
reports and raw stderr/process errors are not exported.

## Bounds and cancellation

The default module budget is 60 seconds (maximum 10 minutes), native stdout
8 MiB, stderr 64 KiB, report JSON 16 MiB and targets 8 (maximum 32). The native
live window is at most 5 minutes. `native_limits.max_samples` defaults to 121;
the actual requested sample count is `ceil(window / interval) + 1` (snapshot
passes the native minimum limit of 2, but collects once). Work-product limits
also apply: nodes × samples × ops ≤ 100,000; nodes × samples × series ≤ 200,000.
Increase the interval, select a node or explicitly adjust bounded limits if a
request does not fit; collection is not silently shortened.

The wall-clock budget starts before module execution, covering subprocess
startup and native discovery/collection/evaluation. Local config parsing and
final formatting are outside that execution budget. Input overflow cancels the
child and withholds incomplete JSON. Output overflow emits a compact valid
budget-failure report. Interrupt requests cooperative native cancellation with
a 250 ms grace period before forced termination, retaining valid native partial
evidence when available. Use the trusted `wp` binary directly: process-tree
management for arbitrary wrapper scripts is not provided. On platforms without
interrupt delivery, cancellation falls back to killing the child.

Connection settings and executable/config paths stay outside reports. Native
JSON is decoded using an allowlist of known fields and metric label keys;
unknown env/auth fields are not forwarded. Public connection/context names,
resource IDs, summaries and diagnostic queries are report data: do not use
credentials as names or resource labels. Future source modules must sanitize
their own queries/results before returning them.

## Verify and extend

```bash
go test -race ./diagnostics/... ./cmd/wpdiag/...
make wpcli wpdiag
python3 cmd/wpdiag/tests/smoke.py
python3 cmd/wpdiag/tests/integrated_smoke.py  # Real Prometheus + wp native fixture.
python3 cmd/wpdiag/tests/jaeger_smoke.py  # Isolated Jaeger 1.53 Docker fixture.
python3 cmd/wpdiag/tests/loki_smoke.py    # Isolated Loki 3.5.7 Docker fixture.
```

The smoke test runs both real binaries against a temporary loopback HTTP
fixture. It verifies write/read suspicion, idle/snapshot interpretation,
partial/discovery failures, deadline/cancellation, context selection and
credential filtering, and historical rejection without a current-state query.
It does not benchmark or assert the health of a live deployment.

The [public module contract](../../diagnostics/README.md) describes how the
source modules in #460–#462 and stage modules in #463–#466 reuse this foundation.

## Loki diagnosis

`wpdiag loki diagnose` connects directly using its own Loki settings. Select
`--scope wp|milvus|e2e`, `--flow write|read` and a live or historical interval.
It reports known log symptoms, actual sanitized excerpts and identity-based
associations. No-match remains inconclusive. See the
[Loki profiles, configuration and evidence limits](../../diagnostics/loki/README.md).

## Milvus append-stage diagnosis

Use `wpdiag milvus append --list` for the versioned write-step metric catalog.
The command needs only its own Prometheus connection:

```bash
bin/wpdiag milvus append --config wpdiag.yaml --connection local \
  --scope e2e --window 30s --budget 1m -o json
```

It distinguishes Proxy completion/preparation, producer quota/routing and SN
WAL/interceptor symptoms; E2E also reports separately observed WP append/flush/
disk context. No collection/channel/WP mapping or latency residual is invented.
See the [append-stage hypotheses and limits](../../diagnostics/milvusappend/README.md).

## Milvus persistence-stage diagnosis

`wpdiag milvus persistence --list` exposes the versioned flush/checkpoint metric
catalog. Use an independent Prometheus connection:

```bash
bin/wpdiag milvus persistence --config wpdiag.yaml --connection local \
  --scope e2e --window 30s --budget 1m -o json
```

The command distinguishes growing pressure, sync scheduling/recovery, execution/
upload timing and persisted frontier/checkpoint progress. WP offload remains a
parallel, unassociated context. Idle checkpoint age, missing lazy metrics and
flusher-seen TimeTicks are not durability/stall verdicts. See the
[persistence-stage hypotheses and evidence limits](../../diagnostics/milvuspersistence/README.md).
