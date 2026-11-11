# Interactive WebUI acceptance against deployed sources (#492)

This scenario uses the shipped `wpdiag serve`, real Chrome and the existing
[#491 source fixture](SOURCES.md). WP emits actual RPC spans/error events;
Prometheus scrapes its exporter, Alloy collects unchanged stdout into Loki, and
Jaeger retains instrumented RPCs. No rule, threshold, catalog or UI inference is
added. See the [deployed receipt](WEBUI_VALIDATION.md).

The acceptance reuses `source_acceptance.Run`, its source checks and exclusive
resource journal. A consumer hook runs the browser before the same fixture is
cleaned. It does not deploy a second fault harness. The original source command
has no consumer by default and retains its existing behavior.

## Prepare, plan, execute

Prepare the seven images and actual RPC worker as described in [SOURCES.md](SOURCES.md).
Use an existing disposable Kubernetes namespace. The scripts do not start/stop a
cluster, switch context, change a Helm release or mutate existing workloads.

```sh
make wpdiag
# Requires Node 22+ and Chrome; optionally set CHROME_BIN or pass --chrome.
python3 tests/integration/diagnostics/webui_acceptance.py \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE
```

The default is an offline plan: no network, subprocess, browser, artifact write
or Kubernetes change. Inspect the images, cases and budgets before execution:

```sh
kubectl --context YOUR_CONTEXT label namespace YOUR_NAMESPACE \
  diagnostics.woodpecker.io/source-acceptance=cli-source-v1

python3 tests/integration/diagnostics/webui_acceptance.py \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE \
  --output /tmp/wp-webui-492 --execute
```

The reused workload writes/read-verifies two small values and emits ten
independently tagged rejected empty appends. Recovery comes from acknowledged
and verified values, not absent logs. This is a representative WP source/UI
workflow, not another Milvus performance experiment or disk durability proof.

## Browser coverage

| Selection/control | Required behavior |
| --- | --- |
| Actual catalog/local preview | Choose named connections, targets, WP/E2E read/write and individual sources/stages. Every preview makes zero recorded backend calls. |
| WP metrics write/read | Query real retained scrapes; inspect original PromQL, samples, IDs and uncertainty. Lazy/small completion populations stay unknown. |
| E2E append on WP-only producer | Execute the real assessor; absent Milvus evidence stays inconclusive. This is not positive Milvus admission coverage. |
| Integrated metrics/trace/Loki | Display actual rejection suspicions and a strict original event/span association after successful data recovery. |
| Partial E2E write and independent WP read trace | Preserve selected actual spans and missing Milvus producer/path coverage. |
| Evidence/export/history | Click original metric/span/log evidence, preserve IDs and nanosecond integers, download byte-identical JSON and reload without recollection. |
| Selected unreachable/disabled source | Retain failed sibling and partial status; unselected unreachable sources do not block independent diagnosis. |
| Unsupported snapshot metrics | Explain unsupported profile and disable Run before querying. |
| Byte limits | Actual trace response exceeds 1 KiB input; report exceeds 4 KiB and becomes the shipped compact budget failure. |
| Live cancellation | Observe module-start progress, cancel a real 30-second window and finish before querying. |
| In-flight cancellation/deadline | Query real Prometheus through a relay, delay the completed response, then observe transport disconnect on browser cancellation and independently on deadline expiry. |
| Local/private configuration | Reject remote binding, foreign origin and missing session token; actually use an environment credential while withholding its value, private endpoints and raw error body from browser/export artifacts. |

Five separate CLI collections use the browser's exact source selections,
historical intervals, connections, targets and limits. Compare owner findings,
uncertainty, selected actual spans/events and lineage, not fresh UUIDs. Within
one UI run, displayed IDs and evidence/export bytes match the original report.
No actual contradictory event is manufactured; existing browser fixtures retain
the counterexample test for contradiction rendering.

The relay is a read-only test observation tool. Normal responses are unchanged
backend bytes. It records bounded counts/status/timing and authorization-used
booleans, never credentials, queries or bodies. Only source-query GET routes are
allowed; no telemetry is pushed. Two explicit controls remain separate:

- `hold`: forward a real query, then delay its completed response. Disconnect
  proves collector response-transport cancellation, not cancellation of still
  running Prometheus computation.
- `deny`: return synthetic 401/private marker for error-body withholding. It
  does not claim the deployed backend rejected authentication.

## Bounds, artifacts and recovery

Default whole-work budget: 600 seconds (360–900 allowed); browser: 210 seconds;
source startup: 180 seconds. Fixture cleanup has separately bounded calls after
work expiry. Relay: at most 512 requests, 4 MiB per response and bounded waits.
Whole-run artifacts: 96 MiB. The driver owns Node/Chrome and `wpdiag serve`;
normal completion, interruption and driver timeout stop owned processes.
Cluster lifecycle remains external.

Artifacts are private local investigation data; review before sharing:

- `acceptance.json`/`fixture.json`: source/browser results, provenance, original
  existing-resource inventory and verified cleanup.
- `browser/*.json`: exact reports, previewed requests, executed job plans,
  original evidence and five separate CLI reports.
- Downloaded `wpdiag-*.json`: original stored bytes, not browser re-encoding.
- `browser/browser.stdout`: case results, Chrome/Node versions, displayed IDs,
  module progress and cancellation timing.
- Four real-source screenshots, bounded Chrome/server logs and `transport.json`.
- Failure receipts/screenshots/reports remain failed; reruns use fresh IDs.

The WebUI keeps at most eight jobs and 64 MiB of completed reports in process
memory. Refresh can inspect a retained report but does not restore the editable
selection or collect again. Restart/eviction loses reports; export provides
user-controlled retention. Fixture Jaeger memory and Loki/Prometheus emptyDirs
are removed by cleanup. This test proves neither telemetry completeness nor a
persistent UI database.

For SIGKILL-interrupted Kubernetes cleanup, reuse the original recovery tool. It
validates the same owner/UID/settings journal and refuses foreign resources:

```sh
python3 tests/integration/diagnostics/source_recover.py \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE \
  --receipt /tmp/wp-webui-492/RUN/fixture.json
# Inspect first, then repeat with --execute.
```

Offline tool regressions run in **Diagnostics Harness** in Diagnostics CI. They
do not boot a cluster or count as deployed acceptance:

```sh
python3 -m unittest discover -s tests/integration/diagnostics -p 'test_*.py' -v
go test -race ./cmd/wpdiag/webui ./cmd/wpdiag/cli ./diagnostics/integrated
```

Restart/reset, missing-scrape controls and final audit remain #493; published
full Milvus-to-WP tracing remains #459. Neither gate is closed by this scenario.
