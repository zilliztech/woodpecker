# Final bounded diagnosis acceptance audit (#493 / #458)

Audit baseline: merged **`389ad6b`** (PR #500), 2026-10-28. This audit maps the
original #458 requirements to their owning evidence; it does not rerun or rewrite
older observations. #488–#492 are merged. The #493 restart/gap delivery below is
the last agreed acceptance child, pending this PR's acceptance/merge.

**Disposition: PASS for the agreed representative scenarios and implemented
profile contracts.** Close #493/#458 only when this delivery is accepted. Keep
#450 open for #459's separate published full-trace image gate. No required
scenario is transferred to an invented new issue or hidden behind a future task.

## Accepted deployed evidence

Each linked receipt records exact builds, configuration, observation windows,
original outcomes, cleanup, failed attempts and local artifact fingerprints.
Those builds are the acceptance scope; a chart tag or current binary is not
substituted for an earlier executed image.

| Delivery | Actual observation / committed receipt | Disposition and boundary |
| --- | --- | --- |
| #482, admission | [VALIDATION.md](VALIDATION.md), run `20261027T063536Z-ba089bbc` | PASS: normal/slow/recovered admission, exact rows/search and historical suspicion with separate current native context. No universal storage/root-cause proof. |
| #484 / #485, consumption | [CONSUME_VALIDATION.md](CONSUME_VALIDATION.md) | PASS: actual new-scanner startup pause, initial visibility and recovery. Does not claim an already-loaded reader stall. |
| #486 / #487, query visibility | [QUERY_VALIDATION.md](QUERY_VALIDATION.md), run `20261027T105127Z-a5dc6576` | PASS: Loaded Customized future TSafe fence versus paired Strong controls, exact results and recovery. Not a Strong-query production fault. |
| #488 / #495, persistence | [PERSISTENCE_VALIDATION.md](PERSISTENCE_VALIDATION.md), run `20261028T004636Z-c8f2fb37` | PASS: real MinIO CPU slowdown, sync/progress and exact-ID recovery. Does not establish physical fsync or power-loss durability. |
| #489 / #496, native WP | [NATIVE_VALIDATION.md](NATIVE_VALIDATION.md), run `native-20261028t021940z-49825a53` | PASS: 20 pending writes/20 pending reads, focused native/adapter lineage, EOF/idle exclusions, all 112 acknowledged entries verified. Expanded metadata-dependent rules remain bounded. |
| #490 / #497, scheduling/execution | [QUERY_STEPS_VALIDATION.md](QUERY_STEPS_VALIDATION.md), run `20261028T034205Z-c06cfd79` | PASS: Loaded FLAT/L2 execution versus scheduling contrast, independent timer populations, recovery and all 65,536 IDs/vector bytes verified. No request-level quantile arithmetic. |
| #491 / #498, actual sources | [SOURCES_VALIDATION.md](SOURCES_VALIDATION.md), run `source-20261028t063157z-74db9f9f` | PASS: actual WP errors through Alloy/Loki, actual RPC spans, strict trace/event association and independent/composed source controls. WP-only E2E traces remain partial. |
| #492 / #500, deployed WebUI | [WEBUI_VALIDATION.md](WEBUI_VALIDATION.md), run `source-20261028t101721z-4bbd1947` | PASS: 15 browser runs/16 interaction checks, original evidence/export, five CLI comparisons, cancellation and budgets. Synthetic transport controls are explicitly labeled. |
| #493, discontinuities | [DISCONTINUITIES_VALIDATION.md](DISCONTINUITIES_VALIDATION.md), run `source-20261028t113258z-ff70b508` | PASS: actual process/container restart and 4→1 counter reset; separate 42.044-second sample gap, up=0→1, same-PID resume, read correctness and historical/current separation. Pending PR acceptance. |

## Every original #458 dimension

| Original requirement | Evidence / level | Disposition |
| --- | --- | --- |
| Native, Prometheus, trace and Loki independently; selected combinations | Deployed native/source/UI receipts above; standalone commands and composed owner reports compared. Source capability/contract tests cover their declared scope/flow/mode matrices. | PASS within declared profiles; no unrelated backend requirement. |
| WP/Milvus/E2E write/read; append, consume, persistence and query | Deployed admission/consume/TSafe/persistence/native/query/source cases, plus source/stage capability tests and unsupported selections. | PASS as representative deployed cases plus complete declared-profile contracts, not an exhaustive Cartesian fault/version matrix. |
| Current snapshot, live sampling, retained history with recovered current state | Native live/insufficient snapshot controls; admission/native historical replay with separately labeled current snapshot; source/WebUI live cancellation; new reset/gap retained-window replay. | PASS. Current counters/registries do not reconstruct or clear historical evidence. |
| Native pending/progress/queue and admission versus append delay | Actual native pending-operation and admission receipts; default native/source/stage rule tests distinguish queue/replica/disk admission and preserve unsupported metadata. | PASS for focused adapter/native and admission scenarios; no invented Milvus/WP metadata joins. |
| WAL consumption/visibility | Actual scanner-startup pause and Loaded TSafe request contrast; native read/EOF controls and consume rule fixtures. | PASS. Startup, WAL retrieval and user-query visibility remain separate paths. |
| Persistence/checkpoint | Actual CPU-quota sync slowdown, retained growing/sync/frontier evidence, idle and restoration; stage freshness/coverage fixtures. | PASS. Background persistence is not a serial write-request timer. |
| Query scheduling versus execution | Actual Loaded FLAT/L2 contrasts and same-exporter queue/ready evidence, independent SN visibility observations and recovery. | PASS. No unrelated population subtraction or guessed request join. |
| Missing backends; stale/sparse metrics; resets/restarts | Actual selected/unselected unreachable-source and byte/time controls; actual restart/reset and scrape gap; Prometheus temporal-quality fixtures for uncovered boundaries, sparse data and missing families. | PASS. Absence and incomplete coverage remain unknown, not zeros/health. |
| Log gaps, partial traces, sampling/retention and registry eviction | Deployed no-match/partial/404/ingestion-grace cases; native reports preserve bounded/evicted registry coverage; Jaeger/Loki fixture exclusions and explicit collector/retention qualifications. | PASS for conservative interpretation. No expiry-duration, lossless ingestion/export or complete-registry claim is made. |
| Wrong/ambiguous identities; duplicate/conflicting or same-window unrelated facts | Actual unrelated request in the incident window and exact event/span matching; production summarizer replay of the new real report with wrong identity/time/deployment, conflict, dangling lineage, current/history and duplicate views; existing source identity fixtures. | PASS. Replay counterexamples are labeled offline mutations, never additional producer events. |
| Individual CLI, integrated CLI and exported JSON | Deployed source/stage/native comparisons and original lineage checks; CLI contract/smoke tests and byte-bounded exports. | PASS. Same-run IDs resolve exactly; fresh independent collections compare stable semantics rather than UUIDs. |
| Interactive selectors/run/progress/cancel/errors/evidence | Actual #492 Chrome/shipped-server receipt, exact evidence/download/reload, actual-source and labeled transport controls; existing browser/server fixtures cover contradiction rendering and retention/origin bounds. | PASS. No second UI rule engine or persistent session database. |
| Workload correctness, bounds, restoration and failed runs | Exact counts/IDs/payloads/vector searches in each actual receipt; original-resource and owned-data journals; finite time/byte/target limits, failed-receipt preservation and cleanup. | PASS. Fixture mutation is explicitly opted in; diagnosis stays read-only. |
| Ambiguous mutation, interruption and lost-run recovery | Existing admission/consume/persistence/source lifecycle counterexamples verify journal-before-mutation, exact override absence, replaced-lock/UID refusal and original-receipt recovery; persistence receipt retains actual manual recovery. New signal tests journal ambiguity and refuse replaced PID/start ticks, even after work expiry. | PASS with explicit mixed evidence levels: deployed restoration plus offline lifecycle counterexamples, not a claim that every crash timing was physically injected. |

## Final contract and replay checks

The final audit runs the existing native, source, four stage, integrated and UI
tests with race detection, all acceptance-tool regressions, the isolated official
protocol module and the actual-report replay. These are contract/regression
checks; they do not relabel any fixture as a deployed incident:

```sh
go test -race -short ./diagnostics/... ./cmd/wpdiag/... \
  ./cmd/wpcli/diagnosis/... ./tests/integration/diagnostics/sourceclient
go -C tests/integration/diagnostics/queryclient test -race ./...
python3 -m unittest discover -s tests/integration/diagnostics -p 'test_*.py' -v
WP_SOURCE_ACCEPTANCE_REPORT=/path/to/actual/wp-write.integrated.json \
  go test -race -v ./tests/integration/diagnostics/sourcecheck
```

Specific inspectable counterexamples include:

- Prometheus `TestHealthyIdleAndInsufficientEvidence`,
  `TestConflictingDuplicateSeries`, `TestCounterSourceFactsAndResetCoverage`.
- Jaeger `TestRetainedWindowAndIdentityAreActuallyChecked`,
  `TestDuplicateViewsAndConflicts`, `TestPrivacyAndPartialRealInsertFixture`.
- Loki `TestControlsNoMatchIsNeverHealthy`, `TestInclusivePaginationAndSaturatedBoundary`,
  `TestBackendFailuresAndWrongEvidence`.
- Integrated `TestHistoricalCurrentContextAndStageReuse`,
  `TestActualEventIdentityTimeAndDeduplication`, `TestCancellationDeadlineAndOutputBound`.
- Native `TestNativeStallSemantics`, `TestDuplicateEvidenceDoesNotBecomeCorroboration`,
  `TestReaderExpectedProgressSemantics`; WebUI `TestBudgetErrorsAndHistory`.
- Actual-report `TestDeployedReceiptCorroboration`; Python ownership/ambiguous
  mutation/lost-run restoration and new PID/reset/gap gates.

## Explicit retained limits and closure

The Milvus metrics scenarios used the exact developer ARM64 build
`27bd28d20688ab00cdd74232a20e988fb2162aed`/runtime image ID
`56f66c0b875ae2a49ff54a2b1f43e81441a92d3cecc9d938c68e17434b51dea2`.
WP service scenarios used released v0.1.46/runtime image ID
`4bef58cf01716909e6b2ab2093eb06c98489d8980b9822f4a132ca8e27d2d5f2`.
The receipts preserve all other dependency/helper/browser builds. These results
do not certify every public Milvus version or substitute for #459's image gate.

Native adapter imports remain focused write/read stalls; expanded WP metadata
and quorum/leased-reader paths are not silently imported. External stage
assessors remain Prometheus-backed. No authoritative time-valid Milvus-client
metadata mapping, universal root-cause guarantee, telemetry completeness or
disk/power-loss SLO is added. Registry eviction, lazy metrics, partial spans/log
delivery and retention remain visible limitations.

No agreed required outcome remains deferred within this finite #458 baseline.
The published full-trace release remains #459 under #450. New provider catalogs,
additional fault/version coverage, remote hosting, TUI, operator deployment and
autonomous remediation remain outside this accepted scope, not prerequisites
invented during this audit. Merge this final delivery before closing #493/#458;
do not close #450 automatically.
