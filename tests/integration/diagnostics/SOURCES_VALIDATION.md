# Real source/corroboration receipt — 2026-10-28

This is a representative deployed acceptance for #491 under #458/#450. A real
MinIO-backed WP service emitted the application events; Alloy collected its
unmodified Kubernetes stdout into Loki. Jaeger retained actual instrumented
gRPC client/server requests. No synthetic Loki pushes, imported OTLP fixtures,
private trace-field insertion, changed source catalogs or relaxed diagnostic
policies were used. Reproduction is in [SOURCES.md](SOURCES.md).

## Run and builds

Passed run: **`source-20261028t063157z-74db9f9f`**, UTC
**06:31:57.450732–06:32:51.658540**. Primary historical interval:
**06:32:17.035935–06:32:35.248653 UTC**. Local artifacts remain under
`/tmp/wp-source-491-live/<run-id>/`; the committed receipt contains reviewed
facts/hashes rather than publishing raw private investigation data.

The externally resumed `wp-diag-458` test profile ran Kubernetes v1.33.1 with
Docker 28.1.1 on Linux ARM64 (four CPUs, 6144 MiB). Namespace
`wp-diag-458` UID: `e39652ca-1bdb-4cdf-b987-f60e0f916941`.
The explicitly opted-in harness created a seven-container Pod and owned
ConfigMap/ServiceAccount/Role/RoleBinding, protected by the shared exclusive lock.
It did not change an existing Helm release, core settings, PVC or Kubernetes
context. The cluster lifecycle was an external operator action.

Actual Kubernetes `imageID` SHA-256 values (not assumed from tags):

| Container | Image | Runtime image/config digest |
| --- | --- | --- |
| WP | `milvusdb/woodpecker:v0.1.46` | `4bef58cf01716909e6b2ab2093eb06c98489d8980b9822f4a132ca8e27d2d5f2` |
| MinIO | `milvusdb/minio:RELEASE.2024-12-18T13-15-44Z` | `8ce3a2b6c83521fabbde58b4f06e5fb7f95e64bfc05bc10def264479327c407b` |
| Jaeger | `jaegertracing/all-in-one:1.53.0` | `894a0d7326581e198bc594c2409b9d670112fdf1ca74e56bce982a23624c860e` |
| Loki | `grafana/loki:3.5.7` | `1abe5e0900c0cacd81a3d52fa103749c222ff8abba7a93af57f44ff56728cacd` |
| Alloy | `grafana/alloy:v1.10.2` | `cff945593440273c7a3c4a2ffb5298a418011ebd3e7e75d4ee1e90f8d75f3dc2` |
| Prometheus | `quay.io/prometheus/prometheus:v3.0.1` | `57847a717fc6727452cd1ed5b02200e3030aba04ca13cbdc536ee99402643440` |
| Actual RPC worker | `wp-source-client-491:local` | `1565e131b08c23ea1fa508d2ee9ed1f80420b00fec921b9285969dc7a0e8a8ce` |

The worker was built with Go 1.25.0, `CGO_ENABLED=0 GOOS=linux GOARCH=arm64`,
from `tests/integration/diagnostics/sourceclient/main.go` SHA-256
`72717e69d549b08d473e3009e4b8817bda47f0fb9cfe914721a844e17dbd3bd0`.
It uses standard actual gRPC instrumentation, not generated span fixtures.
The diagnostic binary used merged production rules at `e48504f`; its SHA-256
was `eba0cd05f5887fc904089633b39cd2abc2a1cd720e9378beb19770e4b836055c`.
All seven containers had zero restarts and unchanged before/after identities.

Configuration and collection provenance:

- WP supported `storage.type=minio`, isolated bucket/root and emptyDirs; two
  acknowledged values, no shared Milvus/WP metadata or object store.
- Standard W3C sampled context and actual client/server OTLP exporters, sampling
  fraction 1. The unexported input parent remains `parent_not_retained`.
- Jaeger memory storage, `MEMORY_MAX_TRACES=10000`; this is an eviction bound,
  not a retention-duration or lossless-export guarantee.
- Alloy API-based Kubernetes collection, restricted to the owned Pod label and
  WP container. Original lines/trace IDs/timestamps are not rewritten. Exported
  labels: `cluster`, `namespace`, `pod`, `container`, `component`.
- Loki 3.5.7 single-node TSDB/filesystem in an owned emptyDir, configured
  `retention_period=24h`. This short run does not verify expiry; cleanup removes
  the owned history. Pagination completion is not ingestion completeness.
- Prometheus scrapes the actual WP exporter every two seconds, one-hour TSDB
  retention in an owned emptyDir. Current snapshots are separate artifacts.
- Six-second initial scrape wait, twelve-second bounded export/ingestion grace;
  final idle no-match uses a new two-second post-grace observation window.

## Actual workload and selected source matrix

Fourteen real RPCs: entry 0 received Buffered then Synced acknowledgements;
ten separately tagged empty attempts for entry 1 received explicit Failed
acknowledgements with WP code **8101**; a valid entry 1 then received Buffered
and Synced. The read returned exactly both IDs and original payload bytes;
the following read returned exact EOF code **8102**. No other values were written.
This tests input rejection and recovery, not storage failure or crash durability.

| Executed selection/control | Actual result |
| --- | --- |
| WP write: independent trace/Loki/Prometheus, then all three composed | Source findings, uncertainty, exact selected spans/events and original lineage agree. Twelve retained append traces (two successful, ten rejected); ten real `wp.append_failure` events. |
| Genuine event associations | **Two of ten** rejected requests have strict identity/time overlap. The other **eight remain unassociated**, explicitly listed in the receipt; no added skew tolerance or source-count confidence. |
| E2E write: trace+Loki | Same two associations; every trace retains `partial_observed_path` and missing Milvus-stage evidence. WP-only traces do not prove Proxy/SN propagation. |
| Unrelated successful request, same incident window | Zero associations despite the ten surrounding error events. |
| WP read: independent trace/Loki/Prometheus, then composed | Actual retained read spans and exact data correctness; read relationship remains partial, Loki no-match remains inconclusive. |
| E2E read: trace+Loki | Partial observed path and `query_trace_does_not_establish_wal_consumption`; no invented user-query/WAL serial path. |
| Milvus write/read selections against this WP-only producer | `supported_span_evidence_absent`, inconclusive logs and zero associations. These are explicit negative controls, not positive Milvus producer coverage. |
| Empty retained search for a supported but un-emitted `Write` operation | `trace.no_retained_evidence`, inconclusive/unknown. |
| Random exact missing trace ID | Jaeger HTTP 404 remains an explicit failed source lookup; standalone/composed semantics agree. |
| Post-recovery idle log window | No selected event, inconclusive; actual recovery is established by acknowledged/read-verified values, not log absence. |
| One-query/one-line Loki budget | Loki `budget_exceeded`; successful trace sibling makes the run partial. The retained page/association does not imply complete coverage. |
| SIGINT during a 30-second trace live window | Cancelled, exit 7, before backend collection. |
| Explicitly requested unreachable Loki + successful trace | Partial with the failed source retained. |
| Unselected unreachable Loki, trace only | Completed independently; no Loki dependency. |

After owned backends were removed, the real CLI additionally rejected trace
`e2e/read/stage=consume` as **unsupported, exit 3**, before any query. That
post-cleanup result is retained in `unsupported-read-stage.json`; this control
is also part of the reproducer. There is no trace-based WAL-consume profile.

Prometheus was reachable (`up=1`), and separately captured **current** success
counters were append **2** and read **1**. The burst created its histogram
families lazily before a pre-completion scrape existed. Historical rate findings
therefore stayed `idle_or_no_completions` with unknown severity and
`expected_work_unknown`, not “no requests happened” or healthy latency. The
admission series was absent/inconclusive for this backend/build. Neither current
counts nor trace timings were substituted for missing interval evidence. Larger
warm-baseline/performance scenarios remain the separate existing acceptance cases.

## Inspect one genuine association

Canonical trace ID: **`f16318c13353364b6717e8ef0b3c0fcc`**.
Original Loki event:
`loki:event:a8d325c8704cf755629c52328f6f17c9829a53e11b5019ac966e3743f46a55c7`,
timestamp **`1793169143219202761` ns**, excerpt **`add entry failed`**.
The original producer line included this top-level trace ID and `empty payload`;
it reached Loki through Alloy without content rewriting.

| Referenced actual RPC span | Start (µs) | Inclusive duration (µs) |
| --- | --- | --- |
| `5e62fe6e6d0fe149` | `1793169143218250` | `1285` |
| `7099c57ea1e5f204` | `1793169143218586` | `647` |

Both are actual qualified `LogStore/AddEntry` spans. The original event timestamp
overlaps both. The association retains both span IDs, original page/event/finding
references and the configured deployment. It describes **one event**, not two
independent confirmations or a proven root cause. No metric/native fact is
joined; deployment configuration does not establish a Milvus-client metadata map.
Inclusive timings are not summed/subtracted into a critical path or disk timer.

The pure production summarizer was replayed against this exact real report.
Identical replay preserved the same associations/finding references. Mutated
copies with wrong trace ID, outside-span event time, other deployment,
conflicting module identity, dangling support and current-versus-historical role
all withheld association. These seven counterexamples are offline replay
fixtures, never additional producer events or backend pushes.

## Failed attempts remain failed

Earlier local receipts were not overwritten or counted as passed:

| Run suffix (all 2026-10-28 UTC) | Retained failure / correction |
| --- | --- |
| `060549z-7dbadb2a` | Server-only strict association failed: CRI time was about 26 µs after the outer server span ended. No tolerance was added. |
| `060910z-792ca908`, `061119z-681c54b6` | Exact trace lookup returned 404 with the 1,000-trace memory trial. Background WP spans and later search evidence showed retention/query instability; it is not proof of lossless export or a sampling diagnosis. The final fixture uses the stack's 10,000-trace bound. |
| `061310z-13f95d32` | Actual association succeeded, but the verifier incorrectly expected a missing exact lookup to behave like an empty successful search. The source's explicit 404 result was preserved. |
| `061422z-6190bbf1` | Verifier inspected stage assessments instead of source findings for no-retained evidence. Source comparisons/assertions now check actual result findings, targets and unknowns, with a regression. |
| `061634z-aadf48ad` | Laptop/VM clock difference prevented strict actual-time overlap. Worker moved to the Linux fixture; no timestamp adjustment was made. |
| `062102z-d2643577` | Even on one clock, one very short RPC did not contain the CRI event time. The final fixed workload records ten separately tagged real attempts and preserves every unresolved identity. |
| `062841z-b184c1dd` | Six genuine associations, but a rejection's delayed CRI timestamp crossed the recovery RPC-start boundary. Final no-match control uses a separate post-grace idle interval, without clearing the original incident. |

This demonstrates why observed IDs, collection readiness and successful requests
cannot guarantee useful retained event-time overlap for every request.

## Cleanup and verification

Fixture Pod UID: `e39e89d8-cf92-492d-bd10-f00cb5bb4fa3`;
lock UID: `d7beda5c-6d94-4891-a605-533d201d1618`. Exact original-owner cleanup
completed. Independent kubectl queries found no owned Pod, ConfigMap, Role,
RoleBinding, ServiceAccount or shared acceptance lock. The **27 existing**
Pod/Deployment/StatefulSet/ConfigMap inventory entries had identical before/after
identities, images/restarts, specifications and configuration hashes. The external
operator restored the dedicated profile's original **Stopped** state; the harness
never performed cluster lifecycle operations. Cached test images/receipts remain.

Verification: 24 original standalone/composed source pairs matched execution
status, findings, gap codes and exact event/span lineage. Also passed: 56 Python
lifecycle/evidence regressions; real-client race tests,
the seven actual-report replay counterexamples, unchanged Jaeger/Loki/integrated
race suites; zero lint issues; website generation idempotent; diff checks passed.
Loopback test sockets required execution outside the filesystem/network sandbox;
the initial permission failures were rerun successfully, not product failures.

Final receipt SHA-256 values:

```text
acceptance.json           ca22eba3cab03945ac52a9e69d99b5fcd61ecb7201bca8903d46df3470c35dbe
wp-write.integrated.json  f3b248f5bf46a6923fa362d9a8cac379765bc11e859fc655b9fdc444bcf5d153
requests.json             0682008be2a228593bc719dc84febfeddbc862984d66c3acb73f7794269c4452
fixture.json              eac154e4a8739c61433800a18cd279360ad9d5d88f4f871ca5255ca9501c4912
```

The initial final-run artifacts totaled 2,359,088 bytes before the separate
post-cleanup unsupported-profile/replay checks, within the 96 MiB bound. These
results supply a real source setup for #492, not its browser acceptance. #493
still owns discontinuity controls/final audit; #459 retains the published full
Milvus tracing release gate. No new mandatory issue or producer rule is added.
