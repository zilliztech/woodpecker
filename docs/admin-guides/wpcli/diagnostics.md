# Woodpecker diagnostics and operations workspace

Build the current `wp` CLI with `make wpcli`. The CLI and local WebUI share the
same checks, native commands, configuration and JSON reports. The WebUI is optional.

## Scope and prerequisites

The tool inspects **Woodpecker client and server behavior**: Writer/Reader APIs,
append, buffer/flush, reads, selected maintenance signals and resource pressure.
A WP client embedded in another application remains a WP client. The tool does
not diagnose Milvus components, query/search, indexing or data-file persistence.
There is no optional Milvus mode. Loki diagnosis is outside this product.

The new CLI can connect to v0.1.46 services without requiring a kernel upgrade.
Available checks depend on the actual Metrics/Admin interfaces and exported spans.
Older instrumentation may omit some spans, including context propagation fixed
in #479/#483. Such gaps affect their checks, not unrelated checks. This is an
interface compatibility expectation, not a blanket certification of every check
on every v0.1.46 image. No kernel changes are introduced by this CLI/WebUI change.

| Capability | Required configuration | Not required |
| --- | --- | --- |
| List checks / read a saved report | No reachable service | WP Admin, Prometheus, Jaeger, Agent |
| `wp diagnose` | WP Admin context; client metadata for scoped reader/quorum evidence | Historical monitoring |
| Metrics check | Prometheus connection and incident window; required scope fields | Jaeger, Agent, etcd |
| Trace check | Jaeger connection, configured WP service and incident window | Prometheus, Agent, etcd |
| Scenario | At least one Metrics or Trace connection and incident window | Both sources together, Agent |
| Native metadata operations | Etcd endpoints and full client metadata prefix, from flags, a parameter file or session JSON | Monitoring connections |
| Agent analysis | Saved findings and explicit Agent configuration | Automatic source queries or repairs |

`wp diagnose --window 30s` starts native sampling **now**. `wp diag --window 15m`
queries retained evidence from the **past**. Historical diagnosis still requires
reachable source APIs; it is not disconnected file analysis.

## Configure server contexts

Copy [cli.diagnostics.example.yaml](../../../cmd/wpcli/cli.diagnostics.example.yaml)
to a private `cli.yaml`, then edit the addresses and population labels:

```yaml
current-context: local
contexts:
  local:
    endpoint: http://127.0.0.1:19091
    node_admin_urls:
      woodpecker-0: http://127.0.0.1:19091
    observability:
      prometheus:
        endpoint: http://127.0.0.1:9090
        match_labels: {namespace: woodpecker}
      trace:
        endpoint: http://127.0.0.1:16686
        services: {woodpecker: wp_server}
        search: {service: woodpecker}
      grafana: http://127.0.0.1:3000
    agent:
      enabled: false
```

Use `WOODPECKER_CLI_CONFIG=/path/to/cli.yaml`, or pass `--config` to `diag`,
`report` and `webui`. The normal CLI context selection still applies. Restart `wp webui` after editing
`cli.yaml`; assessment files are loaded for each diagnostic run.
Context configuration contains server addresses, host maps, Kubernetes settings
and evidence connections. The context name identifies the environment; a separate
`deployment` setting is not required for Metrics or Trace. It does not contain
tenant metadata prefixes, bucket, root path or log namespace. Applicable commands
accept client metadata and storage from explicit inputs or the independent
sections of [session JSON](configuration.md#session-connection-configuration).

### Session connections

Cloud Terminal can supply `WOODPECKER_SESSION_CONFIG=/path/to/session.json`.
Place the same `observability` object shown above under `service_context`, using
JSON syntax. `wp diag` then selects its Prometheus and Trace connections without
`--config` or `--context`; explicit configuration selection takes precedence.
The command still selects the check, incident window and optional log/node filters:

```bash
wp diag metrics check writer-throughput --window 15m
wp diag trace check server-append-duration --window 15m
```

Provider settings retain their existing structure, including `match_labels`,
`match_tags`, `services`, `search`, authentication environment references and TLS.
Supply referenced environment variables alongside the JSON file. Use absolute
certificate paths when also using `wp shell`. Session `scope` identifiers are
descriptive; configure the appropriate provider selectors explicitly in the file.

The configuration layer can preserve additional provider objects, including
`observability.loki`. This is forward-compatible connection transport: the
current `wp diag` command exposes Metrics and Trace analysis, not Loki analysis.

### Direct Prometheus connection

For the Helm example, use the actual local address reported by
`stack.sh connections`. Its default is port 9099; other installations may use
9090 or a reverse-proxy prefix. Configure the query API, not a `/metrics` exporter:

```yaml
current-context: local
contexts:
  local:
    observability:
      prometheus:
        endpoint: http://127.0.0.1:9099
        match_labels: {namespace: milvus-stack}
      grafana: http://127.0.0.1:3000
    agent:
      enabled: false
```

Replace `milvus-stack` with the actual exported namespace label. This context can
run historical Metrics checks without WP Admin or Trace. Add their connections
only when using the corresponding capabilities. If the query API needs a Bearer
token, add `bearer_token_env: WP_PROMETHEUS_TOKEN` to its Prometheus connection
and set that environment variable before starting `wp`.

### Prometheus or VictoriaMetrics through Grafana

Copy [cli.grafana.example.yaml](../../../cmd/wpcli/cli.grafana.example.yaml)
to a private file. Its essential connection settings are:

```yaml
observability:
  prometheus:
    endpoint: https://grafana.example/api/datasources/proxy/uid/YOUR_DATASOURCE_UID
    bearer_token_env: WP_GRAFANA_TOKEN
    match_labels:
      cluster: YOUR_CLUSTER
      namespace: YOUR_NAMESPACE
      woodpecker_id: YOUR_WP_SERVICE_ID
  grafana: https://grafana.example
```

Use the UID of the Prometheus-compatible data source actually selected in the
dashboard or Explore view. A dashboard UID, a different data source UID, or the
dashboard URL with `orgId`/variable query parameters does not select that API.
`observability.grafana` is a navigation link; the tool queries
`observability.prometheus.endpoint`. Do not append `/api/v1/query` to the base
URL: the collector adds the query path. Grafana proxies it to the selected source.

Supply a service-account token with access to query that data source. Set it in
the same environment that launches the CLI or WebUI, for example by using an
interactive hidden prompt rather than recording the token in a shell command:

```bash
export WP_GRAFANA_TOKEN="$(python3 -c 'import getpass; print(getpass.getpass("Grafana token: "))')"
bin/wp webui --config /path/to/cli.yaml --listen 127.0.0.1:8795
```

The YAML contains only the environment variable's name. The server reads the
value and sends `Authorization: Bearer ...` to Grafana. The token is not part of
the URL, metric labels or diagnostic reports. The WebUI address field has no
token editor: select the configured context to use its saved authentication,
then click **Check connections**. **View cli.yaml** shows the environment
reference, not the token. Restart the process after changing its environment
or context configuration. Trace is optional and can remain unconfigured.

`match_labels` are exact equality selectors for the intended metric population,
not a copy of Grafana's expanded All/regex selectors. A shared namespace may
contain multiple clusters or WP service instances: use the actual `cluster` and
`woodpecker_id` labels when present. WP client metrics may live in a different
tenant namespace from the service pool; that association remains explicit.
The context name identifies the environment in reports; no separate
`deployment` setting is needed. Older files containing that field remain
readable, but the context name takes precedence. This name is not a PromQL
selector: actual population isolation comes from `match_labels`. Node ID and log
filters are per-run parameters, not context defaults. A successful connection
check confirms query access; a diagnosis still needs matching series for the
chosen labels, filters and incident window.
VictoriaMetrics responses declaring `isPartial: true` are rejected as incomplete
evidence rather than assessed as a complete population. Missing instrumentation
and zero completed operations remain distinct from connection failures.

Client Metrics must also be scraped. A server's `/metrics` does not contain all
client Metrics. Configure population labels that identify the intended exporters;
sharing Kubernetes or Grafana does not prove a client/server or tenant association.
Trace identity filters need mappings for tags the instrumentation actually emits;
a service name alone does not establish a log/request association.

The WebUI provides **Check connections**, and links to Prometheus, Jaeger and
Grafana. A successful connection test proves API access now, not data availability
throughout an incident window. **View cli.yaml** shows the actual source file
without rewriting it.

## Run an atomic check or scenario

```bash
# Discovery does not query evidence sources or call a model.
bin/wp diag catalog
bin/wp diag catalog --source metrics --check writer-latency
bin/wp diag catalog --output json

# One source-local check.
bin/wp --context local diag metrics check writer-latency --window 15m
bin/wp --context local diag trace check server-append-duration --window 15m

# A scenario runs independent Metrics/Trace checks, then summarizes their results.
bin/wp --context local diag scenario slow-writes --window 15m --node-id woodpecker-0
bin/wp --context local diag scenario all --window 15m --node-id woodpecker-0 \
  --details all --output json > diagnosis.json

# Inspect an exact historical interval, with explicit time zones.
bin/wp --context local diag scenario slow-reads \
  --from 2026-11-09T09:00:00+08:00 --to 2026-11-09T10:00:00+08:00 \
  --node-id woodpecker-0 --output json > reads.json
```

There are **39 Metrics checks and 16 Trace checks**. Scenarios are `slow-reads`,
`slow-writes`, `stuck-reads`, `stuck-writes`, `open-reader`, `open-writer` and `all`.
See the [check inventory](../../../diagnostics/checks/INVENTORY.md) for the exact
metric/operation, formula and scenario membership. Keys such as `writer-latency`
match the rules and UI. Original IDs such as `W01` remain report references.

Scenarios do not run a second inference engine: the summary lists Metrics and
Trace counts, abnormalities, failed/unassessed/skipped checks and per-step results.
A Trace-only scenario does not require Node ID for unavailable Metrics checks.
With neither source configured, the command reports a configuration error.

Use `--log-namespace`, `--log-id`, `--reader-name`, `--node-id` and `--wal-path`
only where the catalog supports them. Node-level Metrics require Node ID.
The emitted `log_ns` commonly combines `bucket + "/" + root_path`; it is not the
etcd prefix. None of these 55 historical checks connects to etcd or requires
bucket/root path. Native physical inspection may require those storage inputs.

Default printing is concise text. `--output json` produces `wp.diag/v1` reports.
`--details none|abnormal|all` controls retained inspection detail; use `all` when
you need subsequent curves/raw evidence. `--progress` writes advisory events to
stderr. `--fail-on-findings` returns exit 8 for a completed abnormal result.
Otherwise completed findings can return 0; exit 0 is not a global health verdict.
Configuration errors return 12, invalid requests 2, source failures 1, query
budgets 5, partial collection 6 and cancellation 7.

## Read results and configure rules

Each check shows its measurement name, meaning, formula, effective assessment,
result, abnormal samples and available curve or spans. Interpret execution
separately from health:

- **Abnormal:** observed data matched this check's configured criterion.
- **Context:** measured activity or position information without an independent health verdict.
- **No data:** no matching series/spans; it is not a measured zero or a healthy result.
- **Unassessed:** coverage, samples or identity were insufficient for this assessment.
- **Source unavailable / collection incomplete:** a query failed or exceeded its budget.

A zero observed counter delta differs from an absent series. No slow/error Trace
candidates means zero matching retained candidates, not zero application latency.
Normal EOF/tail waits and unchanged positions alone do not prove stuck reads.
Metrics and Trace timers may cover different populations and inclusive spans;
do not subtract their percentiles to calculate an unmeasured intermediate wait.

Optional assessment files live beside the selected `cli.yaml`:

- [diag-metrics.yaml example](../../../cmd/wpcli/diag-metrics.example.yaml)
- [diag-trace.yaml example](../../../cmd/wpcli/diag-trace.example.yaml)

Override their locations with `--metrics-rules` and `--trace-rules`. Missing files
use built-in defaults; invalid existing files fail explicitly. Examples list all
current canonical keys. A numeric atomic check can use `--threshold` in its native
unit. Observation-only checks do not acquire invented thresholds. Results retain
effective rules, rule-file hashes and override provenance.

Metrics latency defaults use completed-operation histograms, minimum sample counts,
calculation blocks and a sustained duration. They are illustrative references,
not universal SLOs. Trace duration/error checks query applicable operation,
duration and error candidates, with explicit limits on queries/traces/spans/bytes.
When those limits prevent complete assessment, narrow the interval, select an
exact operation/Trace ID or deliberately adjust source limits. Truncated data is
never silently assessed as healthy. Spans lost to sampling or retention cannot
be reconstructed by the tool.

## Open the workspace

```bash
bin/wp webui --config /path/to/cli.yaml --listen 127.0.0.1:8795
```

The server binds to loopback. Open its printed URL. The overview has three modules:

1. **Diagnostics:** select a scenario/check, review the required connections and
   scope, choose a time interval, then run. Input is on the left; the plan,
   progress, summary, check details and optional analysis share one output card.
2. **Operations:** confirm a suspected stalled write/read or use the existing
   corruption/skip-range workflow. Steps expand independently. Select a command,
   enter its parameters and execute it; the right side shows that step's real output.
3. **Command Explorer:** select the actual `wp` command hierarchy. Common values
   and compatible expanded fields persist across selections. Selecting a command
   never executes it. Required fields highlight only after an invalid submission.

Native commands retain their own effects and confirmation requirements. A completed
operation is not proof of workload recovery. Diagnosis does not automatically
fence, truncate, delete, repair, scale or decommission anything.

Saved diagnostic runs appear newest-first within Diagnostics. Opening one does
not overwrite current inputs or rerun checks. Each retained result can be exported
or analyzed; completed analyses remain attached to their original evidence, and
an explicit follow-up can use one prior analysis as interpretation, not new evidence.
History is bounded and in memory: export results you need to retain across restarts.

Recent input suggestions are stored separately from `cli.yaml`, as independent
key/value candidates. Inputs show up to five successful values; choosing one fills
only that field, and the value remains editable. You can delete an individual
suggestion. **View recent config** shows the complete candidate document and its
cache path. The cache does not infer relationships between endpoints, prefixes,
buckets or log IDs. `WOODPECKER_RUN_CACHE_DIR` can select its location.

## Saved reports and optional Agent analysis

Report operations are siblings of diagnosis; they consume JSON without rerunning
checks. Inspection/export need no source connection or Agent credentials:

```bash
bin/wp report inspect diagnosis.json server-disk-watermark
bin/wp report export diagnosis.json --packet > findings.json
bin/wp report analyze diagnosis.json --config /path/to/cli.yaml
bin/wp report analyze diagnosis.json --config /path/to/cli.yaml --output json
```

Configure a backend-local Agent explicitly, for example in the selected context:

```yaml
agent:
  enabled: true
  provider: codex
  auth: cli
  executable: codex
```

The local Codex CLI must already be authenticated. Claude with a backend API-key
environment reference is also supported. No credentials enter browser storage,
reports or findings packets. Ordinary checks, exports and history require no model
call. Analysis is an explicit action for abnormal findings, using only the supplied
packet. The initial integration cannot query sources, inspect files/runtime,
execute commands or repair the cluster. Its response explains findings, cites
check IDs and suggests the next evidence to collect. Prior analyses are retained
as interpretations and cannot increase evidence coverage.

## Migration and acceptance

The former `wpdiag` executable is retired. It prints migration guidance and refuses
old source/stage, integrated-run and standalone-UI execution. Use `wp diag`,
`wp report` and `wp webui`. Milvus checks, `--include-milvus` and association forms
are removed; the backend rejects retired selections as well. Existing accepted
issue/PR records and historical reports remain records; they do not advertise
support for those retired capabilities.

Build from a complete source checkout, then run:

```bash
make wpcli wpdiag
make unit-test-wpcli unit-test-diagnostics
python3 cmd/wpdiag/webui/generate_command_catalog.py --binary ./bin/wp
python3 cmd/wpdiag/tests/workspace_acceptance.py
```

Acceptance fixtures use isolated HTTP services and a fake Agent. They make no real
model calls or cluster changes. Real deployment compatibility and actual source
coverage must be recorded separately from fixture/browser validation.
