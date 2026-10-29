# Diagnostic acceptance plan and completion ledger

Program: [#450](https://github.com/zilliztech/woodpecker/issues/450).
Acceptance owner: [#458](https://github.com/zilliztech/woodpecker/issues/458).
Final audit baseline: merged `389ad6b` (PR #500), reviewed on 2026-10-28.

The diagnostic commands, integrated CLI and interactive WebUI are implemented.
Their unit/protocol/backend-fixture tests do not establish every deployed fault
scenario. This ledger separates those evidence levels and fixes the remaining
acceptance scope. It is not a new feature roadmap or an exhaustive Cartesian
product of sources, stages, versions and faults.

The [final audit](FINAL_AUDIT.md) maps every original requirement to deployed
and/or explicitly labeled fixture evidence, with pass/fail dispositions and
retained build/profile limits. #488–#492 are merged; #493's actual restart/gap
receipt and final audit are this last delivery, pending review/merge. No new
mandatory child is introduced. Keep #450 open for the independent #459 gate.

## What is already delivered

| Original implementation | Accepted PRs | Boundary |
| --- | --- | --- |
| Native WP #451/#452/#453 | #468/#472/#473 | Current bounded metrics/Admin/ops; no historical backend clients |
| Common tool #454 | #469 | Connection, selection, capability and versioned evidence contracts |
| Sources #460/#461/#462 | #470/#474/#475 | Independent Prometheus, Jaeger and Loki commands with bounded initial catalogs |
| Stages #463/#464/#465/#466 | #476/#478/#477/#471 | Independent append, consume, persistence and query assessment |
| Integrated CLI #467 | #480 | Reuses source/assessor owners and original evidence |
| Interactive WebUI #457 | #481 | Browser selection, execution, cancellation and evidence; deployed acceptance remains below |

Tracking parents #455/#456 summarize implementation and shared contracts, not
the entire deployed acceptance program. Their close-out evidence is recorded
below. Closing them must not close #458 or imply full trace propagation.

## Completed deployed subsets

| Case | Merged delivery | Committed receipt | What it does not prove |
| --- | --- | --- | --- |
| Write admission slowdown and recovery | #482, first subset of #458 | [VALIDATION.md](VALIDATION.md) | All write/storage faults, full-source/UI acceptance or crash durability |
| New-scanner consumption pause and initial visibility recovery | #484 / #485 | [CONSUME_VALIDATION.md](CONSUME_VALIDATION.md) | An already-loaded reader stall or downstream/storage pressure from pause alone |
| Loaded query/search consistency wait and normal-request recovery | #486 / #487 | [QUERY_VALIDATION.md](QUERY_VALIDATION.md) | A Strong-query production incident, query scheduler saturation or expensive execution |
| Persistence slowdown, native pending operations, query scheduling/execution and actual source corroboration | #495 / #496 / #497 / #498 (merged) | [Persistence](PERSISTENCE_VALIDATION.md), [native](NATIVE_VALIDATION.md), [query steps](QUERY_STEPS_VALIDATION.md), [sources](SOURCES_VALIDATION.md) | Every fault/profile/version or full Milvus trace propagation |
| Real-source interactive browser selection, evidence, export and cancellation | #492 / #500 (merged) | [WEBUI_VALIDATION.md](WEBUI_VALIDATION.md) | A persistent UI database, positive Milvus producer fault in the WP-only fixture, upstream query-computation cancellation |
| Actual restart/counter reset and scrape gap, restoration and historical replay | #493, final delivery (pending acceptance) | [DISCONTINUITIES_VALIDATION.md](DISCONTINUITIES_VALIDATION.md) | Disk/power-loss durability, every restart timing or lossless telemetry |
| Connected write tracing on local development images | Partial #459, #479/#483 | [FULL_STACK_VALIDATION.md](../tracing/FULL_STACK_VALIDATION.md) | A published supported Milvus/embedded WP client/server release combination |

#458 was closed when the first subset (#482) merged. That closure was premature:
its original acceptance matrix included all sources, stages and interactive UI.
It is reopened as the acceptance parent. These existing receipts retain their
original results, failed attempts and exact executed-build/artifact provenance.
Updating this ledger does not rerun or relabel an old observation.

## Six fixed acceptance outcomes

These six children decompose the original #458 matrix. Intermediate PRs reference
their child; only accepted final delivery closes it. All five earlier children
are now merged. The last #493 receipt passed its actual controls; this PR's
review/merge remains the final acceptance gate. The original prior evidence is
retained below so fixture and deployed claims are distinguishable.

| Order / issue | Required outcome | Prior evidence | Current delivery |
| --- | --- | --- | --- |
| 1. [#488 Persistence](https://github.com/zilliztech/woodpecker/issues/488) | Growing/sync/upload/frontier observations, reversible symptom and recovery | #465 semantic/TSDB fixtures | PASS: #495 merged, [deployed receipt](PERSISTENCE_VALIDATION.md). |
| 2. [#489 Native WP](https://github.com/zilliztech/woodpecker/issues/489) | Current write/read, idle/tailing exclusions and recovery | Native/subprocess fixtures; #482 current-context subset | PASS: #496 merged, [deployed receipt](NATIVE_VALIDATION.md). |
| 3. [#490 Query scheduling/execution](https://github.com/zilliztech/woodpecker/issues/490) | Scheduling pressure versus execution-heavy queries | Query semantic/TSDB fixtures; #486 TSafe fence | PASS: #497 merged, [deployed receipt](QUERY_STEPS_VALIDATION.md). |
| 4. [#491 Sources/corroboration](https://github.com/zilliztech/woodpecker/issues/491) | Actual logs/traces, independent sources and genuine association | Source fixtures and integrated identity counterexamples | PASS: #498 merged, [deployed receipt](SOURCES_VALIDATION.md); full propagation remains #459. |
| 5. [#492 Deployed WebUI](https://github.com/zilliztech/woodpecker/issues/492) | Real-source browser workflow, cancellation, exact evidence and CLI parity | #481 HTTP/browser fixtures | PASS: #500 merged, [deployed receipt](WEBUI_VALIDATION.md). |
| 6. [#493 Discontinuities/final audit](https://github.com/zilliztech/woodpecker/issues/493) | Actual restart/reset, scrape gap and finite original-matrix audit | Existing temporal/identity/lifecycle fixtures | PASS controls and [receipt](DISCONTINUITIES_VALIDATION.md); [final audit](FINAL_AUDIT.md), pending this PR's acceptance. |

As of 2026-10-28, #488–#492 are merged. #493's final delivery is above. #499
independently split core/WPCLI/diagnostics CI; offline Harness checks do not
replace deployed acceptance. No acceptance child remains beyond #493 in this
fixed plan; #459 remains independently open under #450.

Original implementation sequence: #488, #489, #490, #491, #492, then #493 final
audit. This is an execution preference, not a dependency chain. #488/#489/#490/
#491 and #493's independent controls can proceed from merged master; their
mutating deployments still share an exclusive acceptance lock and must not run
concurrently against one target. #491 supplies #492's final real-source case.

[#459](https://github.com/zilliztech/woodpecker/issues/459) remains a separate
release gate: identify published compatible Milvus/embedded WP client/server
images and rerun the existing single/batch/slow/recovery trace controls. Honest
partial-trace acceptance in #458 does not depend on that full-propagation release.
The overall #450 trace outcome remains incomplete until #459 passes or its scope
is explicitly reassigned with a linked decision.

## Coverage of the original #458 requirements

| Original dimension | Evidence owner / disposition |
| --- | --- |
| Sources: native, metrics, trace and logs; standalone and selected combinations | PASS: source contracts, #482/#489/#491/#492 receipts; exact limits in FINAL_AUDIT.md. |
| WP/Milvus/E2E read/write and append/consume/persistence/query | PASS: declared capability/selection contracts and representative #482/#485/#487/#488–#492 receipts. |
| Snapshot, live and retained historical incident with healthy current context | PASS: #482/#489 current/historical separation, source/UI live controls and #493 actual discontinuities. Unsupported modes remain unsupported. |
| Native pending/progress; admission versus append; consumption/visibility; persistence; query scheduling versus execution | #489; #482; #485/#487; #488; #490 respectively. Successful source observations do not prove every root cause. |
| Missing sources, scrape/log gaps, partial traces, resets/restarts, eviction and uncertain identity | PASS: actual #491/#493 controls, native coverage and explicitly labeled semantic/replay fixtures; no lossless-retention claim. |
| CLI, integrated JSON/report and interactive selector/run/cancel/evidence | PASS: deployed #492 UI/CLI parity, source/stage/native lineage and shared contract tests. |
| Workload correctness, bounds, restoration, failed receipts and crash recovery | PASS: actual per-scenario recovery and labeled ambiguous/lost-run lifecycle counterexamples, indexed in FINAL_AUDIT.md. |

No original requirement is silently dropped. Fixture evidence is sufficient for
pure semantic counterexamples when explicitly identified; it cannot substitute
for a child's named deployed gate. A version that cannot expose a required
trigger/identity remains a documented blocker. Full telemetry retention, every
possible fault, every version and universal root-cause guarantees are not goals.

## Shared-contract close-out for #455 and #456

The audit used merged `e48504f`, the documents below and this command:

```sh
go test -race -short ./diagnostics/... ./cmd/wpdiag/... ./cmd/wpcli/diagnosis/...
```

All 13 tested packages passed; `cmd/wpdiag` itself has no test files. These are
unit/protocol and in-process HTTP fixtures, not another deployed acceptance run.
No deployment/configuration was changed for this audit.

| Parent requirement | Inspectable implementation/documentation and regression evidence |
| --- | --- |
| #455 independent source connections, WP/Milvus/E2E read/write profiles and versions | [Prometheus](../../../diagnostics/prometheus/README.md), [Jaeger](../../../diagnostics/jaeger/README.md), [Loki](../../../diagnostics/loki/README.md); each has its own settings, catalog and source-owned rules. Source scope/flow and connection/selection tests execute independently. |
| #455 suspicious findings and honest unknown/partial states | Jaeger `TestPrivacyAndPartialRealInsertFixture` / `TestLongReadNextIsNotAutomaticallyReadLatency`; Loki `TestControlsNoMatchIsNeverHealthy` / `TestBackendFailuresAndWrongEvidence`; Prometheus contract/temporal tests. A backend response or zero matches is not health. |
| #456 four independent stage commands, supported selections and no copied rules | [Append](../../../diagnostics/milvusappend/README.md), [consume](../../../diagnostics/milvusconsume/README.md), [persistence](../../../diagnostics/milvuspersistence/README.md), [query](../../../diagnostics/milvusquery/README.md). Each owns a pure `Assess` and reuses a source-owned collector. Stage purity/lineage, rule/units, healthy/idle/recovery/missing-source and unsupported-selection tests passed. |
| #455/#456 common contracts, independent text/JSON and reuse by CLI/UI | [Public contract](../../../diagnostics/README.md), [CLI](../../../cmd/wpdiag/README.md), [integrated guide](../../../diagnostics/integrated/README.md). CLI source/stage tests, `TestRunCLIStageReuseAndPartialSources`, runner composition/cancellation/identity tests and WebUI server tests passed. |
| #455/#456 explicit remaining combinations, modes, mappings and evidence limits | Boundary below plus #488–#493 and #459. Further source-specific stage catalogs, metadata association providers and other features are not asserted as implemented. |

Implementation boundaries verified during the audit:

- Native adapter: WP scope only, focused write/read stall profiles,
  snapshot/live. Expanded native `--scenario write|read` remains independently
  available in wpcli; the adapter does not import every expanded rule. Historical
  native evidence is unsupported, and current context is labeled separately.
- Source overviews: Prometheus and Loki cover their six documented WP/Milvus/E2E
  read/write profiles with live/historical windows; snapshot is unsupported.
  Jaeger supports its declared snapshot/live/historical profiles; snapshot needs
  an exact trace ID. Snapshot is not a metrics latency reconstruction.
- Four Milvus stage commands: Milvus/E2E scope, Prometheus-backed live/historical
  collection; append/persistence are write, consume/query are read. Snapshot,
  native/trace/log collection directly in those stage commands, and arbitrary
  collection/channel/log/segment mapping are unsupported. Source overviews in an
  integrated run do not become stage-specific trace/log rules.
- Exporter instance, Loki Pod, WP node/service context and the Milvus client's
  metadata namespace are different identities. No authoritative time-valid
  Milvus-to-WP metadata mapping provider exists. Correlation uses only supported
  observed identities; shared timestamps or numeric IDs cannot fill the gap.
- Integration/UI reuse the same owners and original evidence. Requested source
  failures, partial sampling/retention, asynchronous spans and contradictory
  observations remain visible. Neither component invents root causes or sums
  unrelated timer quantiles.

This evidence satisfies the bounded implementation/shared-contract close-out of
#455/#456 when this ledger is accepted. The six deployed gates remain #458's
responsibility; do not reopen their already-merged implementation children just
because a representative deployment test is still missing.

## Closure and scope rules

1. Close a scenario child only after its required harness/tests, English guide
   and actual receipt are accepted. A failed trigger stays failed; raw artifacts
   may remain local but committed provenance/fingerprints must be inspectable.
2. Close #458 only after #488–#493 pass and #493 maps every original requirement
   to evidence or an explicitly agreed scope decision. A closed implementation
   issue, a successful connection or one scenario does not close the program.
3. Close #450 only after implementation close-out, #458 and #459 are complete,
   or a remaining outcome has an explicit linked reassignment decision.
4. Do not add mandatory children during execution without an explicit scope
   review. Use more focused PRs under these six issues for implementation detail.
   New product features belong to separately proposed follow-ups.

Future features outside this acceptance baseline include new trace/log stage
providers, authoritative metadata joins, loaded-reader fault variants beyond the
representative native cases, additional query plans, autonomous repair/tuning,
operator deployment, remote UI hosting and TUI. They are not hidden prerequisites
for #458 and are not being claimed as delivered.
