# CLI admission acceptance: deployed validation

This receipt covers the first CLI subset of #458, not the full validation matrix.

- Run: `20261027T063536Z-ba089bbc`; UTC `2026-10-27T06:35:36.223682+00:00` to `2026-10-27T06:40:34.065242+00:00`.
- Candidate code/binaries: `f14589c` (based on master `232633c`).
- Host: Darwin arm64, Python 3.9.6, Go 1.25.0; Docker Desktop with 8 CPUs / approximately 8 GB.
- Externally provisioned minikube v1.36.0 profile `wp-diag-458`, Kubernetes v1.33.1, 4 CPUs / 6144 MiB.
- Explicit context and opt-in namespace: `wp-diag-458`; default Kubernetes context remained unset.
- Helm: Milvus 5.0.31 (release revision 3), full-stack-support 0.1.0 (revision 3), kube-prometheus-stack 67.4.0 (revision 4).
- Prometheus v3.0.1, operator v0.79.2, 15-second scrape interval. Other observability addons were not selected.

## Fixture

Installed through the existing stack Helm wrapper with the generated explicit baseline,
`--local-arm64`, and `values/acceptance.yaml` (DataNode memory limit 2 GiB).
The local Prometheus override below disables operator TLS together with the admission
webhook and bounds retention. It is a disposable-fixture setting, not a production recommendation.

```yaml
prometheusOperator:
  tls:
    enabled: false
  admissionWebhooks:
    enabled: false
    patch:
      enabled: false
prometheus:
  prometheusSpec:
    retention: 2h
    retentionSize: 256MB
    resources:
      requests: {cpu: 100m, memory: 128Mi}
      limits: {memory: 512Mi}
```

| Component | Image | Runtime image ID |
| --- | --- | --- |
| datanode | `harbor-us-vdc.zilliz.cc/milvusdb/milvus:enhance-woodpecker-v0.1.46-3.0-20261012-27bd28d206-v2-arm64` | `docker://sha256:56f66c0b875ae2a49ff54a2b1f43e81441a92d3cecc9d938c68e17434b51dea2` |
| woodpecker | `milvusdb/woodpecker:v0.1.46` | `docker://sha256:4bef58cf01716909e6b2ab2093eb06c98489d8980b9822f4a132ca8e27d2d5f2` |

Dependencies use the example's pinned etcd v3.5.25 and MinIO RELEASE.2024-12-18T13-15-44Z images.

## Command and observed result

```sh
python3 tests/integration/diagnostics/cli_acceptance.py \
  --context wp-diag-458 --namespace wp-diag-458 --wp bin/wp \
  --duration 30 --settle 15 --scrape-wait 16 \
  --output /tmp/wp-458-live --execute
```

Exit status: **0**. Each phase also has 20 seconds of priming writes. Offered logical
vector load is 1 MiB/s (8 requests/s × 256 rows × 128 float dimensions).

| Measurement phase | Successful inserts | Rows/s | Client insert p95 / p99 (ms) | Strong query p95 (ms) | Admission outcome |
| --- | ---: | ---: | ---: | ---: | --- |
| baseline | 240 | 2047.8 | 105.4 / 118.6 | 194.2 | idle_or_no_completions |
| admission_slowdown | 238 | 2001.5 | 520.9 / 527.2 | 207.3 | suspected_quota_admission_wait |
| recovery | 240 | 2047.4 | 108.5 / 150.4 | 189.6 | idle_or_no_completions |

All writes and concurrent strong reads succeeded. The final exact strong row count was 306,688
(including priming); sampled IDs and vector search passed in each phase.
Selected Pods retained the same identities/image IDs/restart counts and were Ready at the final check.

Each phase independently ran append and query diagnosis. Standalone append findings matched
the integrated child by rule/version/outcome/severity/evidence strength; original finding IDs
and supporting observation references resolved. Proxy completion populations were positively observed.

Negative controls also passed:

| Control | Diagnostic status | Exit | Completed-traffic gate |
| --- | --- | ---: | --- |
| absent_series | completed | 0 | rejected |
| unreachable_backend | failed | 1 | rejected |

## Admission evidence and restoration

The observed quota histogram population recorded 110 completions;
its p95 bucket bounds were 292.402–753.315 ms, above the unchanged
250 ms diagnostic threshold. The rule returned `suspected_quota_admission_wait`.

This metric population is separate from the client latency distribution; their quantiles
were not subtracted or treated as one request. Idle admission outcomes after recovery do not
assert universal health; working traffic is separately established by client checks and completion metrics.

All four initial FileSource values (0.85, 4mb, 256kb, 20m) and original etcd override absence
were restored and read back. The owned collection and namespace acceptance lock were removed.
Re-querying the historical fault interval after recovery alongside a separately labeled native
current snapshot retained the admission suspicion. WP client-metadata authority remained an explicit gap.

## Failure controls encountered during validation

- Builds unable to report implicit-default provenance failed before fault injection; the explicit Helm baseline resolved this.
- The default 500-series source budget was insufficient for the real multi-node profile; the fixture explicitly uses a bounded 2,500-series budget.
- A newly created lazy quota histogram lacked the initial window boundary; the run failed as inconclusive. Priming supplies observations before measurement without relaxing data-quality rules.
- Run `20261027T062728Z-5df6c65a` passed workload/source checks but failed its final stability gate. DataNode was OOMKilled (exit 137) during an index build at the example's 768 MiB limit. Configuration and owned-data cleanup still succeeded. The separate 2 GiB fixture was then applied through Helm; the failing receipt was not rewritten.

The lifecycle suite passed **18 tests** (including ambiguous HTTP commits, exact override/absence
restoration, interruption, unresolved-lock retention, explicit crash recovery, external-change
refusal and bounded restart evidence). Existing integrated CLI smoke checks passed **17 cases**.
Python syntax, `git diff --check`, CLI builds and generated documentation stability also passed.

## Artifact fingerprints

Raw reports are retained locally under `/tmp/wp-458-live/20261027T063536Z-ba089bbc/`.
Connection files are private and are not included in this receipt. Reproduce using the guide;
these hashes identify this observation, not a portable live connection.

| Artifact | SHA256 |
| --- | --- |
| `acceptance.json` | `d026c5a37bffad369c73275a91030e46a05a278e6f48e290746f43484d31f84b` |
| `restoration.json` | `6ea2e9dc5554ad4d5ed3af67b8bd2f57f25656003517d3ad845fc5b706c80b9d` |
| `admission_slowdown.append.json` | `8dca560af27f035c05001306087f7f753bb989f58f4ce1074eb37bab5a18c431` |
| `admission_slowdown-current-context.integrated.json` | `0c2f951d735ba009a3b66285786eef26a9a07b25dbc166c0f47912545781e0fb` |
| `cli_acceptance.py` | `69bd7d3e1c8532bcae9db7d813d5473faf88c9e195922f39342a2f38d7fdbc9e` |
| `perf.py` | `fe8672f19cd2a318c145945017980a31ddfddae5396658c44a95600b059da3fc` |
| `wp` | `7494eb978f3296e9cf1e367520f8eb01309dfe37550e2205cfeeefabd52caef7` |
| `wpdiag` | `68f63e3f2f5c58f67b9534da9681b5d413bb69a9069ec94fedafa08947ec3685` |

WebUI, trace/Loki combinations, other stage/scenario matrices, restart/reset/scrape-gap
acceptance and full trace propagation remain outside this receipt. #458 was subsequently
closed after this first subset, then reopened as the acceptance parent. The
[completion ledger](MATRIX.md) records the accepted subsets and six remaining
children (#488–#493); full trace release acceptance remains #459. This tracking
correction does not change the recorded test results or executed-build hashes.
