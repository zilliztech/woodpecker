# Interactive local diagnosis

`wpdiag serve` is the operator WebUI for the existing integrated runner
([#457](https://github.com/zilliztech/woodpecker/issues/457)). Run one independent
source, one Milvus stage, or a supported combination of WP/Milvus/E2E read/write
checks. Plans, queries, diagnoses, associations and evidence remain owned by the
same modules as the headless CLI. This package adds no diagnosis rules.

## Start

Build from the repository root and adapt the existing private connection file:

```sh
make wpcli wpdiag
cp cmd/wpdiag/wpdiag.example.yaml /path/to/private/wpdiag.yaml
# Edit paths, source endpoints, credentials and deployment/service bindings.
bin/wpdiag serve --config /path/to/private/wpdiag.yaml
```

Open the printed URL, normally **http://127.0.0.1:8787**. A source build embeds all
HTML/CSS/JavaScript; the operator needs no npm installation, CDN or separate UI
service. Existing monitoring systems are sufficient. Helm and the example stack
are optional. To choose another local port:

```sh
bin/wpdiag serve --config wpdiag.yaml --listen 127.0.0.1:8989
# Or select an available port and read the startup URL:
bin/wpdiag serve --config wpdiag.yaml --listen 127.0.0.1:0
```

Only loopback addresses are supported. Stop with Ctrl+C/SIGTERM to cancel active
collection and discard this session's in-memory history. Private configuration
is loaded once at startup; restart after editing it. The page selects named
connections and public target identities. It does not expose or edit endpoint,
credential, executable, cli.yaml or etcd settings over HTTP.

A native connection needs a reachable WP service context and compatible `wp`
executable. Prometheus, Jaeger and Loki use their own source-local connections.
See [the configuration example](../wpdiag.example.yaml) and
[the integrated guide](../../../diagnostics/integrated/README.md) for the current
profile matrix, configuration and evidence limits. A WP service context does not
identify a Milvus instance's WP-client metadata namespace.

## Investigate

1. Select **Integrated diagnosis** or **Choose individual modules**. Choose the
   scope and read/write flow. Individual modules include the independent source
   overviews and four Milvus stage assessors. An exact profile can be selected
   when choosing a single module.
2. Select a snapshot, live duration, or retained historical interval. Historical
   inputs are explicitly **UTC**. Only an explicit **Include a separate current
   WP snapshot** adds current native context to a historical run.
3. Choose source-local named connections, especially when a source has several.
   For integrated runs, auto selection uses the runner's documented auto plan;
   explicit selections retain source errors. Optional stage selection narrows
   metric assessors, while other sources retain their own declared coverage.
4. Optionally narrow targets using fields advertised by the source profiles.
   Each source target is a complete replacement for that source. Unsupported
   filters are reported, not silently dropped by the runner. Unavailable module
   combinations are disabled for new selection; a previously selected module
   remains visible so its incompatible selection can be removed or explained.
5. **Preview plan** validates locally without backend queries or spawning wp.
   Read the module/profile version, scope/window/role, connection, budgets and
   omissions. A partial plan can execute ready modules while retaining the
   unavailable siblings. Invalid plans or plans without a ready module cannot
   start. Editing the form invalidates its preview.
6. **Run diagnosis** starts the previewed selection. Progress is advisory. Use
   **Cancel run** to stop collection; already gathered evidence remains in the
   final report. Changing the form does not alter an active or historical run.
7. Read ordered module findings, impact and evidence strength separately. Follow
   supporting/contradicting observation buttons to see exact original IDs,
   attempted source queries, collection times, retained evidence intervals,
   available metric population facts, selected spans/log excerpts and raw JSON.
   Suggested next checks are labeled **not executed**.
8. Inspect source/stage and cross-source coverage gaps, original source findings,
   and any identity/time associations. Export the original JSON for a notebook,
   issue or agent; exports preserve the stored report bytes, including large
   integer identities. The exact observation viewer also preserves raw numbers
   on the server rather than reserializing a browser-rounded payload.

A completed run is an **execution status**, not a health verdict. No-match,
missing metrics, partial traces or unsupported profiles remain inconclusive or
explicit gaps. A current snapshot cannot clear a historical incident. Native
context, metric populations and Milvus client metadata are not guessed joins.
Trace/log associations show their original span/event references and owner
uncertainty; matching identity does not establish causation. The UI does not
refresh an old incident with new scrapes or launch background diagnostic runs.

The page polls existing run state; closing a tab or losing its HTTP request does
not cancel collection. Recent runs are available to other tabs in the same local
server session. Reloading the page can inspect a retained run, but does not
restore a run selection into the editable form. The executed plan always stays
with the original result. There is no persistent or cross-process run database.

## Public local API

All requests use the same `integrated.Request` as `wpdiag run`; no second runner
or rule engine is implemented. Source settings stay private in `Config`.

| Method / path | Behavior |
| --- | --- |
| `GET /api/catalog` | Registered modules/profiles, named connection availability, default limits and session retention bounds; no private settings |
| `POST /api/plan` | Local `Runner.Plan`; returns its original ready/unsupported/invalid/partial plan |
| `POST /api/runs` | Validate the plan, start `Runner.Run` and return an HTTP 202 job handle |
| `GET /api/runs` | Brief recent-run list |
| `GET /api/runs/{id}` | Job status, immutable plan, bounded progress events and final-report availability |
| `POST /api/runs/{id}/cancel` | Idempotent context cancellation; a completed report is unchanged |
| `GET /api/runs/{id}/report` | Exact bounded `wpdiag.run/v1` bytes; `?download=1` adds an attachment header |
| `GET /api/runs/{id}/evidence?module_id=...&observation_id=...` | An original observation, resolved by its module and exact observation ID |

The HTTP job handle is distinct from the diagnostic `run_id` in the final
report. Source finding/observation/rule IDs and versions are unchanged. Separate
collections can have different UUIDs; the UI renders the same IDs as its stored
CLI-compatible report, not invented replacements.

The local page supplies a per-session header for POST requests. Host/origin
checks and a same-origin content policy prevent cross-site use of this local
collection API. There is no cookie/login or remote multi-user control plane.
Bodies are strict JSON capped at 64 KiB; unknown fields fail without echoing
private values. Structured API errors distinguish invalid plans (422), active
run limit (429), pending reports (409), expired/unknown IDs (404) and a stopping
server (503). An unavailable source inside a run stays a diagnostic child
status, not an HTTP success claim about that source.

The server permits **two active runs**, keeps at most **eight run handles** and
**64 MiB of completed report bytes**, and evicts oldest completed reports under
retention pressure. Active collection is never evicted. Each run additionally
honors the runner's time/input/output/target limits. Report encoding is bounded
before storage; progress keeps at most 64 advisory events. The browser initially
displays 25 findings per module and 50 gaps per group, with explicit expansion;
full evidence is retained in the report subject to the runner's output limit.
No collector, repair operation, cluster lifecycle action or data mutation is
introduced by the API. Runtime version/profile limits remain in the owning
module guides.

## Validate

```sh
go test -race ./cmd/wpdiag/... ./diagnostics/...
make wpcli wpdiag
python3 cmd/wpdiag/tests/webui_smoke.py
```

Browser development/CI additionally needs Node 22+ and Chrome. Set `CHROME_BIN`
if Chrome is installed elsewhere. The browser fixture starts the real binary
with a temporary private configuration and local Prometheus, Jaeger and Loki HTTP API fixtures.
It exercises selectors, positive append/query and consume/source cases,
independent Jaeger/Loki adapters and genuine runner trace/log associations,
unsupported combinations, partial/failed sources, progress/cancel, budgets,
original evidence navigation/export, conflicting-evidence presentation, safe
log text, accessible controls and desktop/mobile light/dark layouts. It writes
bounded fixture screenshots and JSON under `/tmp/wpdiag-webui-evidence` by
default, then stops its processes. Real retained Prometheus/native integration
is covered separately by `integrated_smoke.py`. The [deployed browser acceptance](../../../tests/integration/diagnostics/WEBUI.md)
reuses actual WP/Prometheus/Jaeger/Alloy/Loki sources, checks original evidence and
CLI parity, and verifies cancellation and explicit limits. Its [receipt](../../../tests/integration/diagnostics/WEBUI_VALIDATION.md)
separates deployed evidence from controlled transport counterexamples. The full
program remains [#458](https://github.com/zilliztech/woodpecker/issues/458).
