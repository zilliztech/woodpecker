# CI ownership and local verification

Three primary workflows report independently. Their first revision deliberately
shares the same conservative change filters, including shared runtime/config,
root dependencies, tools, acceptance helpers and CI files. Splitting ownership
does not assume that a CLI is independent of its SDK/runtime dependencies.

| Workflow | Check / owner | Local entry point |
| --- | --- | --- |
| Core Unit Test (`unit-test.yaml`) | Core runtime, SDK, server, metadata, common libraries and remaining root packages | `make unit-test-core` |
| Core Unit Test | CI Contracts: ownership, migration, coverage and actual label-script counterexamples | `python3 -m unittest discover -s .github/scripts -p 'test_ci_*.py' -v` |
| WPCLI CI (`wpcli-ci.yaml`) | WPCLI Unit, including native WP diagnosis | `make unit-test-wpcli` |
| WPCLI CI | WPCLI Terminal: real pseudo-terminal interaction against local fixtures | `make wpcli` then `python3 cmd/wpcli/tests/shell_terminal_test.py --binary bin/wp` |
| Diagnostics CI (`diagnostics-ci.yaml`) | Diagnostics Unit: external sources, stages, runner, CLI/WebUI and wpcli adapter | `make unit-test-diagnostics` |
| Diagnostics CI | Diagnostics Build / CLI: both real binaries and standalone contracts | `make wpcli wpdiag`; `python3 cmd/wpdiag/tests/smoke.py` |
| Diagnostics CI | Eight independent backend smoke matrix jobs; `fail-fast: false` | `python3 cmd/wpdiag/tests/NAME_smoke.py` |
| Diagnostics CI | Diagnostics WebUI: Node 22, Chrome and bounded HTTP fixtures | `python3 cmd/wpdiag/tests/webui_smoke.py` |
| Diagnostics CI | Diagnostics Harness: nested protocol module, root helper packages and offline Python lifecycle/evidence regressions | Commands below |
| Diagnostics CI | Stack Examples: all dotted Python test files, support-chart lint and generated website consistency | `python3 .github/scripts/stack_tests.py` |
| Component Integration Test (existing workflow) | SDK component tests, excluding diagnostics/tracing helper packages | `make integration-test` |
| Component Integration Test | Trace Export Integration: actual queue/client spans exported into temporary Jaeger, request-helper and lifecycle regressions | `python3 tests/integration/tracing/append_propagation.py --output /tmp/wp-append-trace-evidence` |

Root-package ownership is defined once in `scripts/ci_packages.py`. `core`,
`wpcli` and `diagnostics` are complete, disjoint partitions of the former root
unit set (all packages outside `tests/`). Native diagnosis stays under wpcli;
`diagnostics/wpcli` stays under the external tool and tests the adapter contract.
`python3 .github/scripts/ci_packages.py verify` enumerates actual packages and
rejects incomplete ownership or an unregistered nested diagnostic Go module.
New packages under an owned directory are included automatically.
Enumeration retains packages with import errors (`go list -e`): the owning
suite fails its actual tests, while an unrelated tool's broken import does not
prevent enumeration/startup of every other suite.

The SDK selector retains future SDK integration subpackages but excludes all
`tests/integration/diagnostics/**` and `tests/integration/tracing/**` helpers.
Those helpers have explicit owners above. The nested `queryclient` module is
not included by root `go list`; its independent test/tidy step is intentional.

## Coverage

The three Go unit jobs preserve `-race -short -cover -covermode=atomic -failfast`
and the 20-minute test timeout. They produce `coverage-core.out`,
`coverage-wpcli.out`, `coverage-diagnostics.out`, uploaded with matching flags.
Only the selected packages are instrumented, preserving the previous package
coverage boundary. Codecov merges the reports for the same commit and waits for
three uploads before reporting statuses. Existing project/patch thresholds and
Server/Client/Meta/Common components remain; WPCLI and Diagnostics components
make the new boundaries visible.

Coverage upload follows only its own Go tests. CLI smoke, browser, backend,
terminal, stack and harness failures remain independently visible and do not
prevent another successful unit job from uploading. A failed unit job does not
produce a falsely complete three-report aggregate. `make unit-test` remains the
local aggregate command and produces the legacy combined `coverage.out`.

All three workflows must retain equivalent triggers while this three-upload
strategy is used. A later selective-routing change must also handle coverage
and expected checks; it cannot simply omit one of the three reports.

## Fixtures, deployment and artifacts

Backend smoke uses temporary Prometheus 3.0.1, Jaeger 1.53.0 and Loki 3.5.7
containers. WebUI uses explicit HTTP fixtures; trace export uses actual Go
execution with a generated-gRPC acknowledgement fixture, not real disk storage.
These are not real Kubernetes, Milvus E2E, durability or the #492 browser gate.
The harness job runs only `test_*.py`, never opted-in `--execute` workloads.
Real deployment/fault/performance acceptance remains externally provisioned,
explicitly opted in and separately documented under #458.

The stack loader imports every `*.test.py` explicitly because Python's standard
discovery skips dotted/hyphenated filenames. Empty/missing suites fail. All
99 existing offline cases are retained; the existing cache-dependent Grafana
render case may skip when its pinned chart is not cached. The support chart is
separately linted with optional observability enabled. No cluster is contacted.

Install `.github/scripts/requirements-ci.txt` for CI contract and stack tests.
The stack tests use fake command backends; the terminal tests use Unix PTYs.
Browser evidence is uploaded on failure, trace evidence always, with seven-day
retention. Linux binaries are shared only between jobs in the same Diagnostics
run, retained for one day and made executable after download.

## Rerun and check aggregation

- `/rerun-unit-test`: Core Unit Test and CI Contracts.
- `/rerun-wpcli`: WPCLI Unit and WPCLI Terminal.
- `/rerun-diagnostics`: all Diagnostics CI jobs.
- `/rerun-component-test`: SDK component and trace export integration.
- `/rerun`: failed jobs in failed workflows on the current PR head.

Comment commands and the privileged label controller load from the default
branch, so their updated mappings take effect after merge. This PR's new check
names must be inspected directly before that transition. Repository rulesets or
branch-protection required checks are external configuration; this change does
not edit those settings. Each check name is unique.

`ci-passed` considers all 18 primary job names, including every smoke matrix
entry. Matching changes with a missing, pending, failed, skipped or cancelled
primary check cannot get the label. The controller uses only GitHub API metadata,
checks both names of renamed files, and ignores completed runs for an older PR
head. It never checks out or executes PR code/artifacts in its privileged job.
Other existing workflows retain their established selected-check policy.

## Migration receipt

Base: merged #498 (`4db790c`). All prior Unit Test steps have an owner in the
table above: pure Go packages, standalone CLI, eight smoke scripts, browser,
diagnostic helpers/nested module/Python tests, actual append export and tracing
request/Python contracts. Added coverage includes shell PTY interaction, stack
offline suites and CI migration/label counterexamples. No diagnostic rule,
production behavior, fault threshold or live acceptance result is changed.

Local verification of this migration: actual package sets are 60 core, nine
wpcli and 13 diagnostics packages; all three race/coverage suites pass with
Go 1.25.1. The host's older downloaded Go 1.25.0 fails coverage for no-test
entrypoint packages with `no such tool "covdata"`; CI's Go `1.25` selection uses
the available patch release. Initial core runs also exposed an intermittent
failure of the unchanged metadata reader lease-TTL test, including in isolation.
The final complete core suite passed without production/test edits. Failure
and final logs are retained at `/tmp/wp-ci-core-1251.log`,
`/tmp/wp-ci-meta-isolated.log` and `/tmp/wp-ci-core-final.log`; no assertion was
weakened or test skipped to obtain that pass.

The standalone CLI, nine PTY checks, all eight backend smoke programs, 21 browser
checks, 102 diagnostics harness and ten tracing Python regressions passed.
All 99 stack tests executed (one existing chart-cache skip), the support chart
linted, the ten new CI contract tests passed, and actionlint 1.7.12 accepted
the changed workflows. Codecov's official validation endpoint accepted the new
configuration. This validation does not run the real Kubernetes fault harnesses.
