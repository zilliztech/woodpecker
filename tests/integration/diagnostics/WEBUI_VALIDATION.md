# Deployed interactive WebUI receipt — 2026-10-28

Passed #492 run: **`source-20261028t101721z-4bbd1947`**, UTC
**10:17:21.187249–10:18:31.581906**. Reproduction: [WEBUI.md](WEBUI.md).
The final browser exercised the shipped server/runner against actual deployed
Prometheus, Jaeger and Alloy/Loki evidence from real WP RPCs. No synthetic
telemetry, imported spans, production rule/threshold edits or UI-only diagnoses
were used. Explicit transport controls are identified separately below.

## Builds, ownership and observation interval

- Production code: merged **`23d6bb6`** (PR #499). Only acceptance code and
  documentation were edited. `wpdiag` was built with Go 1.25.0 on Darwin ARM64;
  binary SHA-256: `fedc4d7fd833808c07264e8ffe33c5c3eb6dbdfb26f8167c8dde48a7c12669db`.
- Real browser: **Chrome 156.0.8078.73**, Node **v22.13.0**. Executed driver
  SHA-256: `33581879f741adc04d55a59fcae768aafeb2c6d1a0b33ab7dec42ce473f1fc62`.
- Externally resumed disposable profile `wp-diag-458`: Kubernetes v1.33.1,
  Docker 28.1.1, Linux ARM64, four CPUs/6144 MiB. Namespace UID:
  `e39652ca-1bdb-4cdf-b987-f60e0f916941`.
- Shared source opt-in/exclusive lock and source fixture/recovery were reused.
  Seven containers had zero restarts and unchanged before/after identities.
  The **27-item existing workload/configuration inventory was unchanged**.
- Historical browser interval: **10:17:40–10:17:58 UTC**, using the shipped
  one-second UTC input controls. Each compared CLI used that exact interval.
- The original workload acknowledged/read-verified both values and exact EOF,
  and rejected ten individually tagged empty appends. Five identities had strict
  trace/log event-time overlap; five remained explicitly unassociated.

Actual Kubernetes `imageID` SHA-256 values:

| Container | Image | Runtime digest |
| --- | --- | --- |
| WP | `milvusdb/woodpecker:v0.1.46` | `4bef58cf01716909e6b2ab2093eb06c98489d8980b9822f4a132ca8e27d2d5f2` |
| MinIO | `milvusdb/minio:RELEASE.2024-12-18T13-15-44Z` | `8ce3a2b6c83521fabbde58b4f06e5fb7f95e64bfc05bc10def264479327c407b` |
| Prometheus | `quay.io/prometheus/prometheus:v3.0.1` | `57847a717fc6727452cd1ed5b02200e3030aba04ca13cbdc536ee99402643440` |
| Jaeger | `jaegertracing/all-in-one:1.53.0` | `894a0d7326581e198bc594c2409b9d670112fdf1ca74e56bce982a23624c860e` |
| Loki | `grafana/loki:3.5.7` | `1abe5e0900c0cacd81a3d52fa103749c222ff8abba7a93af57f44ff56728cacd` |
| Alloy | `grafana/alloy:v1.10.2` | `cff945593440273c7a3c4a2ffb5298a418011ebd3e7e75d4ee1e90f8d75f3dc2` |
| Actual RPC worker | `wp-source-client-491:local` | `1565e131b08c23ea1fa508d2ee9ed1f80420b00fec921b9285969dc7a0e8a8ce` |

Worker source SHA-256:
`72717e69d549b08d473e3009e4b8817bda47f0fb9cfe914721a844e17dbd3bd0`.
The source fixture uses the previously documented sampling=1, bounded ephemeral
retention and unmodified API-based Kubernetes stdout collection. Those settings
do not establish complete export, ingestion or retention.

## Actual browser results

**15 diagnostic runs and 16 interaction/control checks passed**, with no uncaught
browser exceptions. Both individual source/stage and integrated selectors used
the actual eight-module catalog. Every preview left recorded backend-call counts
unchanged. Screenshots captured real-source results in desktop/mobile and
light/dark modes; they are observation artifacts, not an additional visual audit.

| Browser case | Actual outcome |
| --- | --- |
| `metrics-write`, `metrics-read` | Completed actual retained WP histogram queries. Original query/sample/evidence navigation passed. Lazy-created small populations remained unknown; lifetime counts were not substituted for historical completions. |
| `append-missing-producer` | Real E2E append assessor executed; absent Milvus metrics remained inconclusive. No positive Milvus fault coverage was claimed. |
| `multi-source` | Actual WP rejection suspicions and one selected strict trace/log association. Trace ID **`cb57a80c9996641904a8c964c4c94281`** retained original span/event IDs and actual overlap. Recovery had already succeeded; historical errors were not cleared. |
| `e2e-partial` | Same actual WP evidence, with `partial_observed_path` and missing Milvus producer coverage. |
| `read-trace` | Actual selected WP read spans, independently usable. |
| `selected-unreachable`, `selected-disabled` | Partial; unavailable Loki remained visible while trace succeeded. |
| `unselected-unreachable` | Completed trace-only selection without depending on unreachable Loki. |
| `transport-denied` | Explicit synthetic 401/private error-body control failed conservatively; marker did not reach browser/export artifacts. |
| `output-limit` | Actual oversized report became the shipped `budget_exceeded` compact report within 4096 bytes; the immutable job plan retained the executed selection. |
| `input-limit` | Actual trace response exceeded 1024-byte input allowance; explicit `budget_exceeded`. |
| `cancel-live` | Actual module-start progress, then cancellation before source queries; final report cancelled. |
| `cancel-inflight` | Real Prometheus query completed; its return was deliberately held. UI cancellation completed in **527 ms**, and the collector closed the held response connection. |
| `deadline-inflight` | Separate 500 ms run budget expired and closed the held response connection. |

The relay recorded **47 source requests**: 43 normal byte-preserving queries,
two held real-query returns and two synthetic 401 responses. Neither held
return proves upstream computation cancellation; neither synthetic 401 proves
deployed authentication failure. No actual conflicting event was manufactured;
existing browser fixtures retain contradiction-rendering coverage.

Additional checks passed:

- Original finding IDs matched rendered IDs; original metric/span/log evidence
  matched server observations, including integer precision. Downloaded JSON was
  byte-identical to the original stored report.
- Five separate CLI collections (`metrics-write`, `metrics-read`, `multi-source`,
  `e2e-partial`, `read-trace`) preserved owner outcomes, uncertainty and actual
  selected span/event lineage. Fresh UUIDs were not required to match.
- Reload selected an exact retained in-session result with no recollection.
  Unsupported snapshot metrics were explained before I/O. No active run remained.
- The shipped command rejected remote binding. The actual server rejected foreign
  origin and missing session header. All relay requests used the private
  environment credential; its value, private source endpoints and raw error
  marker were absent from browser/export artifacts.

## Cleanup, failed attempts and limits

The original source UID/configuration journal resolved all owned resources and
exclusive lock. A separate post-run Kubernetes read confirmed no owned source
Pod or acceptance lock remained. No existing release, PVC, configuration or
current Kubernetes context was changed. The dedicated profile's lifecycle is an
external operator action, separate from the scripts. After final Kubernetes
verification, the external operator restored its original **Stopped** state.

Final local artifacts were approximately **4.04 MB** at the recorded byte check,
below 96 MiB, under `/tmp/wp-webui-492-live/<run-id>/`. This folder contains private
investigation data and is not committed. Its original reports, logs and four
screenshots remain inspectable locally; fingerprints are below.

Earlier failed receipts remain failed and cleaned:

| Run suffix (UTC, 2026-10-28) | Failed test assumption / correction |
| --- | --- |
| `100815z-7b739d72` | Driver expected a query marker on a trace-span observation. Query evidence and selected-span evidence are distinct; corrected the test. |
| `101001z-25601c23` | Driver expected `report_ready` in the brief history list. Corrected it to inspect each actual job detail. |

Intermediate run `101223z-3e1318e0` also passed. The final run above adds explicit
module-progress capture and stronger stored-report/plan checks. No diagnostic
rule, production test threshold or supported profile was relaxed.

WebUI history remains bounded in-memory state (eight jobs/64 MiB), not persistent
storage. This representative receipt does not establish performance SLOs, full
Milvus E2E propagation, crash durability or complete telemetry. #493 still owns
actual restarts/resets/scrape gaps and the final acceptance audit; #459 owns the
published full-trace image gate. #458/#450 are not completed by this receipt.

Offline verification: **117 Python acceptance-tool regressions**, including 15
new browser-tool counterexample tests; Go race tests for WebUI/CLI/integrated
runner; Python/Node syntax and generated-site/diff consistency checks passed.
These regressions automatically run in Diagnostics CI's existing Harness job;
they are not another deployed acceptance run.

## Artifact fingerprints

| Local raw artifact | SHA-256 |
| --- | --- |
| `acceptance.json` | `9e8c54d718818d976189b4ea400196c429aaa6d7fdad7264daf4babab884901f` |
| `fixture.json` | `196526e952a3f37d25a6dba0de2fc847a37159c8ee963f20274c737c4fd08e83` |
| `browser/browser.stdout` | `ffe2d49a33c8ccbdbc445dbef9ca6277712431ce684e1b30536fefac382c6c27` |
| `browser/transport.json` | `ec6a7bb6f50175207da27cf630d2f4b67428867d0d3141235be4187b4b10266f` |
| `browser/metrics-write.json` | `a4ca7ec06c09d0242fc0aa9c92518844690dea8640cc87a8366a286c0a4593ed` |
| `browser/multi-source.json` | `a68064372cff38e17e4fe599133d163deca85fea8d9319e59973ecf0b9d58810` |
| `browser/e2e-partial.json` | `2972c9cc30247b13efb1edabcf04376e89c59585ab4ee3750445a1a3eefbcfa2` |
| `browser/cancel-inflight.json` | `8741d12c46f530eb86f1c3895f1e3179cc4f6d9b680775206ab50e4da6896009` |
| `browser/deadline-inflight.json` | `79273178cce927c3b28a11a997635f3c520b98b96bf41eb4637bac18170cdc30` |
| `browser/deployed-light-1440.png` | `99f5c49320c97b0cdeaebe90e7512529a11cb1d3c923817b9bac94717a711cd2` |
