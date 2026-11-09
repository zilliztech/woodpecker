# CI ownership and local verification

Three unit workflows have disjoint package ownership. They intentionally share
conservative triggers because `wp` imports native SDK/runtime code and the
coverage aggregate expects three uploads for the same commit. A shared trigger
does not mean the workflows execute each other's unit packages.

| Workflow / rerun command | Responsibility | Local verification |
| --- | --- | --- |
| Core Unit Test — `/rerun-unit-test` | SDK, server, metadata and common runtime packages; CI ownership/controller contracts | `make unit-test-core`; `python3 -m unittest discover -s .github/scripts -p 'test_ci_*.py' -v` |
| WPCLI CI — `/rerun-wpcli` | Native commands including **`wp diagnose`**, context/configuration, candidate inputs and shell | `make unit-test-wpcli`; `python3 cmd/wpcli/tests/shell_terminal_test.py --binary bin/wp` |
| Diagnostics CI — `/rerun-diagnostics` | **`wp diag`**, saved `wp report` results, evidence clients/rules, optional Agent adapters and operations WebUI | `make unit-test-diagnostics`; current CLI/browser and backend fixtures below |
| Component Integration Test — `/rerun-component-test` | SDK integration packages; actual runtime Trace export and protocol propagation | `make integration-test`; `python3 tests/integration/tracing/append_propagation.py --output /tmp/wp-append-trace-evidence` |

`wp diagnose` inspects the current native runtime. `wp diag` queries retained
Metrics/Trace for an incident window. Their command-entry unit tests belong to
WPCLI and Diagnostics respectively, even though both are exposed by `bin/wp`.
`wp webui` startup is wired by WPCLI; its server/interaction implementation is
owned by Diagnostics. No supported CI execution depends on the retired
`bin/wpdiag`; its migration notice still has a unit test.

## Diagnostics jobs

| Check | What it verifies |
| --- | --- |
| Diagnostics Unit | `diagnostics/**` and `cmd/wpdiag/**` unit/race/coverage tests, including reusable historical parser libraries |
| Diagnostics Build | Builds the current `bin/wp` once for the dependent jobs |
| Diagnostics CLI | Atomic checks, scenario/source/window/output contracts against bounded HTTP fixtures |
| Diagnostics Smoke (prometheus) | Current WP Metrics checks against a disposable real Prometheus 3.0.1 |
| Diagnostics Smoke (jaeger) | Current WP Trace checks against disposable real Jaeger 1.53.0, including exact Trace IDs |
| Diagnostics WebUI | All three workspace modules, validation, history and fake-Agent follow-up using Node 22/Chrome and HTTP fixtures |
| Diagnostics Harness | Tests the evidence/workload fixture utilities: nested query-client protocol module, root helper packages and offline Python regressions |
| Stack Examples | Offline deployment-script tests, support-chart lint and generated website consistency |

The backend matrix has **two** entries. Retired Milvus-stage, integrated and Loki
smoke programs under `cmd/wpdiag/tests/legacy/` are historical receipts and are
not executed by the workflow. Keeping a reusable library or offline fixture
regression does not expose a retired diagnostic command.

Stack Examples stays in Diagnostics CI as verification of the supplied
observability/deployment example, rather than adding a fourth workflow here.
It does not deploy a cluster. The harness tests the fixture helpers, not an
operator's running system; it never opts into `--execute` workloads. Browser
and ordinary CLI tests use no model credentials or real model requests.

## Package ownership and coverage

`.github/scripts/ci_packages.py` defines one owner per root-module package:
`cmd/wpcli/**` → WPCLI; `cmd/wpdiag/**` and `diagnostics/**` → Diagnostics;
remaining non-test packages → Core. `verify` enumerates actual packages and
rejects incomplete/overlapping ownership or an unregistered nested module.
Future packages under an owned directory are included automatically. Broken
imports stay in the owning package set and fail its own tests rather than
preventing every suite from starting.

SDK component selection excludes `tests/integration/diagnostics/**` and
`tests/integration/tracing/**`. The nested diagnostic `queryclient` module has
an explicit independent test/tidy owner. Trace request/lifecycle helper tests
belong to Trace Export Integration.

The three unit jobs retain race, short tests, atomic coverage and bounded
execution. They upload `coverage-core.out`, `coverage-wpcli.out` and
`coverage-diagnostics.out` with corresponding flags. Codecov waits for all three
uploads. Current project/patch thresholds remain in force; CI cleanup does not
suppress coverage failures or exclude newly added implementation from coverage.
`make unit-test` remains the local combined suite producing `coverage.out`.

Keep the three primary workflow triggers equivalent until coverage/expected
checks support selective routing. The other runtime integration workflows also
retain conservative Go/dependency triggers; this change does not redesign their
nightly, release, upgrade, fault or performance responsibilities.

## Runtime export versus diagnostic assessment

Trace Export Integration runs actual WP queue/client/executor code and a local
generated-gRPC acknowledgement fixture, exporting real spans into disposable
Jaeger. It checks retained parent identities, remote client/server relationships
and distinct batch links directly. It requires neither a diagnostic binary nor
an application cluster. Diagnostic query/rule behavior belongs to the independent
Jaeger smoke job above. Stream cleanup cancellation alone does not establish an
application failure; the export receipt makes no health/durability verdict.

These fixtures do not verify disk writes/fsync, crash durability, a deployed
Milvus read/write chain, production sampling or lossless export. Previous #450,
#458 and #459 receipts remain historical; CI does not reinstate a cross-application
release or image-matrix gate. Real workloads/repairs are separate explicit actions.

## Artifacts and local prerequisites

Install `.github/scripts/requirements-ci.txt` for CI/stack Python tests.
Terminal tests use Unix PTYs. WebUI installs stable Chrome for Testing and passes
its exact executable through `CHROME_BIN`, preserving bounded startup diagnostics
and owned-process cleanup. Browser artifacts are uploaded on failure; raw Trace
export evidence is uploaded after either outcome, with seven-day retention.
The current `wp` binary is shared only among jobs within one Diagnostics run and
retained for one day.

The stack loader explicitly imports dotted `*.test.py` files, rejects missing or
empty suites, and retains the cache-dependent Grafana-only render skip when its
pinned chart is unavailable. The support chart is linted with optional
observability enabled. No Kubernetes cluster is contacted by these checks.

## Rerun and check aggregation

`/rerun` retries failed jobs in failed runs on the current PR head. A named
`/rerun-*` command reruns its **whole workflow**; for example `/rerun-lint` and
`/rerun-mod-tidy` both rerun Code Checker. The PR command guide lists the supported
mappings, including service corruption/stability and issue-command tests. The
unknown-command help derives its list from the actual dispatch mapping.

`ci-passed` checks all 12 primary job names, including both backend entries.
Matching changes with a missing, pending, failed, skipped or cancelled primary
check cannot get the label. Other PR test workflows participate when their
checks run, including corruption, both PR stability tiers and Issue Command
Tests. K8s stability, Chaos Mesh, release and baseline-priming workflows remain
nightly/manual/tag operations, outside PR gating.

The privileged label controller uses API metadata only, checks previous filenames
for renames and ignores completed runs for an older head. It never executes PR
code or artifacts. The comment/controller workflows load from the default branch,
so mapping and aggregation changes take effect after merge. Repository branch
protection is external configuration and is not changed by this patch.
