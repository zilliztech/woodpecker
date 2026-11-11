> Migration note: the old problem-workbench executor below is historical.
> Current atomic checks/scenarios live in `diagnostics/checks`; this package
> supplies Agent/report compatibility. Use `wp diag`, `wp report`, `wp webui`.

# WAL problem workbench

The default `wpdiag serve` page is a problem-oriented WAL workbench (#503).
Choose **Slow reads**, **Slow writes**, **Reads not advancing**, **Writes not
completing**, **Open Reader fails**, **Open Writer fails**, or **Metadata checks**.
The old source/stage UI remains at `/advanced`, with its existing CLI commands,
P95 rules, IDs and report contracts. General Milvus query/search, indexing and
data-file persistence are outside the new workbench.

## Run locally

The preferred entry points are now `wp diag metrics check`, `wp diag trace
check`, `wp diag scenario` and `wp webui`. See the
[CLI/workspace guide](../../docs/admin-guides/wpcli/diagnostics.md).
`diagnostics/checks` owns the new atomic/scenario contracts; this package keeps
the legacy `wpdiag.wal/v1` frontend and optional Agent adapter compatible.
The new scenario path never invokes this package's legacy corroboration rules.
Metric calculation/inspection is shared through `diagnostics/evidence`.

The legacy standalone frontend is also supported:

```sh
make wpcli wpdiag
cp cmd/wpdiag/wpdiag.example.yaml /path/to/private/wpdiag.yaml
# Edit sources and workbench.targets in that private file.
bin/wpdiag serve --config /path/to/private/wpdiag.yaml
```

Open the printed loopback URL. No Node runtime, frontend service, model account
or example Kubernetes stack is needed by operators. The UI embeds local assets.
It does not start/stop Kubernetes or install monitoring services.

1. Select the affected problem and configured WP target.
2. Select a relative window or an explicit **From / To** interval. Browser local
   time and UTC are supported. Relative windows become fixed timestamps when
   the run starts. Prometheus defaults to at most one hour; backend config may
   allow up to 24 hours, within its query/sample budgets.
3. Select **Run checks**. The server collects bounded source evidence; progress
   and cancellation share the existing local job/history infrastructure.
4. Start with **Findings**, then inspect **Missing evidence**. Open any row to
   see its metric name and meaning, calculation with the applied threshold,
   run result, abnormal measurements and time curve. Source queries and raw
   observations are collapsed by default. Curves use retained measurements;
   unavailable evidence has no synthetic curve.
5. Export the immutable JSON report or a readable Markdown summary. **Preview
   Agent input** exports only abnormal findings, relevant context and gaps.
   **Recent runs** retains bounded history for this server session.

Stopping a run keeps partial evidence. Closing a tab does not cancel a run.
Restarting the server discards in-memory history. There are no background runs
or automatic model calls. Export reports before restarting if needed.

## Target identity and source boundaries

Sources use the existing `wpdiag.config/v1` private connection definitions.
The new optional `workbench` section chooses stable, named targets:

```yaml
workbench:
  targets:
    example-log:
      label: Example / log 1001 / replica 0
      prometheus: local
      log_namespace: your-bucket/your-client-root
      log_id: "1001"                 # Decimal string; never a JS number.
      node_id: your-mapped-wp-node   # Optional, explicit replica/node binding.
      # path: /your/wal/path         # Optional filesystem scope.
      # reader_name: your-reader    # Optional Reader scope where exported.
      wpcli: local
      log_name: your-log-name
      metadata:
        etcd: http://127.0.0.1:2379
        prefix: your-milvus-client-prefix/woodpecker
      # Optional; UI checkbox is off by default:
      # milvus:
      #   prometheus: milvus-local
      #   channel: your-explicitly-associated-channel
      #   node_id: your-streamingnode-id
  policy:
    latency_ms: 100
    min_observations: 100
    sustain_ms: 180000
    max_gap_ms: 30000
    # Per-point thresholds use the metric's native unit:
    # thresholds: {W01: 250, MV02: 0.25, H01: 90}
```

Without explicit targets, each configured Prometheus connection becomes an
all-matching-populations target. Exact populations remain separate; no averages
or sums across exporters conceal an abnormal population. Node-only checks
require an explicit `node_id`; a log-to-node association is not guessed. A node
selection narrows server evidence, not client counters that have no node label.
Use separate named targets to inspect different replicas.

The same Kubernetes namespace or Grafana does **not** associate a Milvus channel
with a WP log. The optional Milvus extension needs an explicit configured
channel and source. Metrics without the channel identity remain Unknown rather
than being joined by deployment alone. Embedded WP and service WP use the same
identity requirements. Unconfigured trace/log correlation is not simulated.

The UI displays configured source references, not a claim that connections are
healthy. A run records actual connection/query failures. Private endpoints,
executables, configuration paths and credentials are not editable via HTTP.

## What a result means

- **Abnormal**: a named deterministic rule matched observed evidence. It is an
  initial finding, not a confirmed root cause.
- **Within rule**: qualified samples did not match this rule; this does not prove
  the entire WAL path healthy or cover uninstrumented errors.
- **Context**: measured rate, position, pending work or a lifecycle observation.
  Zero rate may mean idle; unchanged Reader position does not establish backlog.
- **Unknown**: unavailable source/metric, insufficient completions, reset, gap,
  unresolved identity, or an explicitly deferred evidence point.
- **Not applicable**: a branch is positively established as unused by the
  matching qualified branch counters. Missing/lazy counters are not zero.

Latency uses **P99 bucket intervals**, not an interpolated exact latency or a
renamed P95. Defaults require at least 100 completed operations per assessed
minute and a lower bound above the 100 ms reference in three consecutive
one-minute blocks. Full-window bucket/count/sum alignment, cumulative integrity,
integer precision, scrape coverage and reset checks reuse the existing source
client. The reference is an operator-configurable heuristic, not a product SLO.
An unresolved bucket or short high interval remains Unknown under this sustained
rule. Quantiles from unrelated stages cannot be subtracted to estimate a missing
stage. The aggregate server flush timer is not isolated fsync time.

A write-stall finding additionally requires a paired acknowledgment position
unchanged for at least 120 seconds of actual retained samples, positive append
submissions and continuously nonzero pending work in the **same** exporter/log
population and sample interval. Queue growth requires nondecreasing pending
samples with an increase over the sustained interval and same-population offered
work. Reader positions remain context until time-aligned readable-tail evidence
can establish actual backlog; that capture is deferred here.

Node disk usage is compared only with stable watermarks from the same exporter
and node. System CPU and memory are system resource evidence, not process-level
attribution or a claim that they caused the WAL symptom. H02 deliberately shows
cumulative I/O-wait seconds per wall second as Context: source code exports
`cpu.Times(false).Iowait`, despite its percentage HELP text. The exported CPU
count is GOMAXPROCS, not a verified host CPU denominator. No I/O-wait percentage
threshold is applied. Resets withhold the rate.

## Metadata stays in wpcli

The metadata entry is a **current snapshot**, regardless of the historical form.
It calls only the existing `wp meta log show`, `wp meta segment list`,
`wp meta quorum list` and `wp meta check` commands, with the explicit configured
client endpoints/prefix and log name. Views are Context. Only the existing
`meta check` output may return an abnormal constraint finding. Each command's
original JSON and observation time remain available; the four commands are not
claimed to share one etcd revision. Quorum listing is namespace-scoped, not a
per-log relationship proof. No dynamic metadata engine or repair is added.

A WP service context identifies the service, **not** a Milvus client's metadata
prefix. Credentials/TLS continue to use wpcli's existing configuration behavior.

## Optional supplied-findings Agent

Export is the default. The backend supports **Codex CLI** and **Claude Code**.
Select the provider in the private YAML, retaining your existing targets.

### Codex with an existing local login

```sh
codex --version
codex login status
# Only if the local login is missing:
codex login
```

Run these commands as the OS user who starts wpdiag. Codex CLI 0.166.0 was
validated. The adapter requires `--strict-config`, `--ignore-user-config`,
`--ignore-rules`, `--ephemeral` and the selected feature settings. An incompatible
runtime fails the analysis rather than falling back to broader permissions.

```yaml
workbench:
  # Keep your existing targets here.
  agent:
    enabled: true
    provider: codex
    auth: cli
    executable: codex              # Or the absolute local executable path.
    timeout_ms: 120000
    max_output_bytes: 1048576
    # model: an-available-model
```

```sh
make wpdiag
bin/wpdiag serve --config /path/to/private/wpdiag.yaml
```

The default model is selected by Codex when `model` is omitted. Saved local CLI
login is reused through normal auth storage (`CODEX_HOME` is respected); the
adapter does not copy credentials into YAML or the browser. Local/project Codex
configuration, MCP servers, plugins, hooks and project instructions are not
imported. Custom model gateways from user config are not imported either.

Open **Local Agent** and select **Check local login / executable** to check local
authentication without a model request. **Ready** is a local readiness check,
not a guarantee that the remote model is reachable or the account has quota.
Run checks and select **Analyze findings** on an abnormal result. Analysis,
cancellation, errors and exports use the same bounded job/history mechanism.

### API-key authentication

For Codex, use `provider: codex`, `auth: api_key`, and
`api_key_env: CODEX_API_KEY`. Set the referenced environment variable only for the
server process. The backend passes it to the Codex process, not to the browser.

For Claude Code:

```yaml
workbench:
  agent:
    enabled: true
    provider: claude
    auth: api_key
    executable: claude
    api_key_env: ANTHROPIC_API_KEY
    timeout_ms: 60000
    max_output_bytes: 1048576
```

Omitting `provider` preserves the original Claude/API-key configuration. Claude
Code must support `--bare`, `--tools`, strict MCP configuration and stream JSON.
No credentials are returned by the setup/status API. Restart wpdiag after editing
private settings or environment references.

### Analysis boundary

The server derives a packet from immutable stored report bytes and records their
SHA-256. It includes abnormal findings, relevant context and gaps with original
observation references. Raw matrices and plotting windows are omitted; input is
bounded at 256 KiB. Normal/missing-only results cannot start a model call.

Codex runs noninteractively in an isolated temporary directory with a read-only
sandbox and explicitly disabled shell, image, browser/search, multi-agent,
plugin, MCP, hook and memory capabilities. Its model-visible tool list was
checked against a fake local Responses endpoint: the remaining built-in
`request_user_input` has no interactive support in exec mode. The adapter rejects
interaction/tool events; it never answers approvals or questions automatically.
Claude runs with an empty tool list and strict empty MCP config. Both adapters
use bounded output, timeout/cancellation and nonpersistent sessions.

These are report interpretation adapters. They cannot automatically query
Prometheus, run wpcli, inspect runtime, repair anything or add dynamic metadata
diagnosis. The answer is limited to 120 words in three sections: Finding, Evidence (at
most three bullets), and Next steps (at most two checks). It cites short evidence
IDs such as [S06], which open retained evidence in the WebUI. UUIDs, observation
ID prefixes and repeated disclaimers are omitted from the answer; original
provenance remains in the exported report and source views. A timeout, process error, tool request or output
overflow withholds a partial conclusion; the source report remains available.
Unit tests use fake executables and do not spend model tokens.

## Audit and verification

The [52-point audit](EVIDENCE.md) records reuse/adapt/defer for every proposed
point. The embedded catalog is the source of the UI definitions; **52 design
points does not mean 52 automated checks**. Existing clients, bounded HTTP,
authentication, label validation and parsing are reused. New problem rules and
new report contracts belong here; old module contracts remain unchanged.

```sh
go test -race ./diagnostics/... ./cmd/wpdiag/...
make wpcli wpdiag
python3 cmd/wpdiag/tests/webui_smoke.py    # Existing Advanced workflow.
python3 cmd/wpdiag/tests/wal_acceptance.py # New real UI + bounded HTTP fixtures.
```

Browser acceptance needs Node 22+ and Chrome (`CHROME_BIN` may override the
browser path). The Diagnostics WebUI CI job runs both suites. HTTP fixtures
validate deterministic abnormal/cancel/export behaviors; they are not a claim
that a deployed fault or a real paid model invocation was exercised.

The evidence sidebar distinguishes the full-window reading from the rule's
assessment interval: window average for rates, window increase for counts,
full-window P99 with per-minute P99 curves for latency, and latest sample for
system gauges. Abnormal latency rows identify qualifying minute blocks, not
isolated peaks. CPU/memory rows identify the actual sustained high interval,
even after the latest sample has recovered. Inspection performs no source IO;
it uses the immutable stored report. Agent input omits the plotting windows.
