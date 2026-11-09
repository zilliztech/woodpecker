---
name: wp-wal-writes
description: Investigate Woodpecker WAL slow writes, writes not completing or OpenWriter failures through cross-source scenarios and focused atomic follow-up. Excludes Milvus-owned and Loki diagnosis.
---

Resolve the intended WP target and incident interval from cli.yaml. Select one
symptom scenario, preserving the complete structured result for inspection:

```bash
wp diag scenario slow-writes --target <target> --from <start> --to <end> --details all --output json
wp diag scenario stuck-writes --target <target> --from <start> --to <end> --details all --output json
wp diag scenario open-writer --target <target> --from <start> --to <end> --details all --output json
```

Use the command corresponding to the symptom, not all three by default.
`--window` and optional historical `--to` are the relative alternative. A
scenario combines its Metrics/Trace checks without producing a second verdict.
Read configuration/collection coverage first; an optional unconfigured source is
skipped, and a configured unreachable source is a failure. Both absent is a
configuration error.

Follow step results: Writer throughput/errors/timing, append submissions and
pending/progress context, service buffer/flush observations, disk watermarks,
resource evidence and applicable open outcomes. Use catalog IDs and actual rules
rather than assuming a fixed numeric SLO. An anomaly is a check's observed rule
match, not proof that the step caused the reported symptom.

For focused evidence use `wp diag metrics check <name>` or `wp diag trace check
<name>`. Discover definitions with `wp diag catalog`; the source Skills
`wp-metrics-checks` and `wp-trace-checks` describe their execution contracts.
Reuse the same frozen interval before changing scope. Broaden/narrow the window
only with an explicit reason; source-budget failures never justify silent
truncation.

Do not infer idle application demand from zero submissions, write rejection from
high filesystem occupancy, or isolated fsync delay from an inclusive flush timer.
Pending growth and unchanged acknowledgment positions are separate observations.
Unrelated quantiles cannot be subtracted. Relate Metrics and Trace only when
actual target/request identity and time evidence support it; sharing a step or
service is not enough. Preserve counterevidence and missing samples.

Produce a concise observation summary with check IDs, plausible explanations
clearly labeled, and the minimal next evidence needed. Reports include executed
queries, Trace IDs and original references for follow-up. No extra model request
or WebUI is needed for ordinary screening. Stop when evidence answers the user's
question or identify the precise unresolved gap.

`wp diag scenario all` optionally sweeps the agreed catalog for this target.
Milvus-owned diagnostics are not supported. Metadata inspection uses existing
`wp meta` at its observation time. No repairs, fencing, deletion, deployment,
fault injection or performance load are included in this workflow.
