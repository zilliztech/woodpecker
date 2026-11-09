---
name: wp-metrics-checks
description: Run source-local Woodpecker WAL Metrics checks with wp diag, select incident windows and interpret structured initial assessments. Use for one metric-backed question or to drill into a scenario finding; excludes Milvus-owned and Loki diagnosis.
---

Use the user's existing `wp` executable and cli.yaml. Inspect `wp diag catalog
--source metrics --output json` for implemented names/IDs, actual metrics,
formulas, criteria and limitations. Configuration lives under
`diagnostics.sources.metrics.connections` and `diagnostics.targets`. The selected
context can supply `diagnostic_target`; `--target` selects it explicitly. A
Metrics check does not require Trace, Admin or an Agent to be available.

Choose one question and run:

```bash
wp diag metrics check <name> --target <target> \
  --from <RFC3339-start> --to <RFC3339-end> --details all --output json
```

For relative lookback use `--window`, optionally with a historical `--to`.
`--from/--to` and the lookback form are alternatives. Use timezone offsets and
preserve the normalized `request.from/to` for subsequent checks. No command here
starts live sampling or generates load.

One check only reads its own required inputs. Buckets/count/sum or disk usage
and matching watermarks can belong to one question. Submission activity is not
silently fetched for a pending check. Cross-check explanations belong in the
scenario/Agent workflow, not another threshold in this skill.

Read `checks[]`: execution, status/data_state, actual value, applied rule and
`inspect`. Distinguish a measured zero from absent series, no completed latency
samples, inadequate sample quality and query failure. Inspect contains metric
meaning, calculation, anomaly intervals and measured curves. Use its actual
criterion; this skill does not define numeric thresholds. `--threshold` only
changes one supported check in its native unit and must be reported as an override.

Compare exact exporter/log/node populations and time windows. Do not subtract
unrelated quantiles, treat loop-iteration time as a whole Reader request, or call
flush time isolated fsync latency. A high filesystem watermark is an observation,
not proof of rejection or the cause of a slow write. Resource metrics may be host
rather than WP-process observations. Retain gaps and unsupported associations.

Save JSON with shell redirection when the user wants a report. `--details all`
retains bounded original evidence for later inspection; compact reports explicitly
omit some details. Normal checks never call a model or require a WebUI. Repairs,
cluster changes and fault injection are not part of this workflow.
