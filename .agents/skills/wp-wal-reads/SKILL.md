---
name: wp-wal-reads
description: Investigate Woodpecker WAL slow reads, reads not advancing or OpenReader failures by running Metrics/Trace scenarios and following specific findings. Focuses on WAL; all Milvus-owned diagnosis is outside this skill.
---

Resolve the affected target and incident interval using the user's cli.yaml.
Keep the selected context separate from client metadata authority. Do not guess
a log-to-node or Metrics-to-Trace association.

Start with the symptom's scenario:

```bash
wp diag scenario slow-reads --target <target> --from <start> --to <end> --details all --output json
wp diag scenario stuck-reads --target <target> --from <start> --to <end> --details all --output json
wp diag scenario open-reader --target <target> --from <start> --to <end> --details all --output json
```

Use one command matching the symptom. `--window` with optional `--to` is the
relative alternative. A scenario runs its agreed Metrics and Trace atoms and
summarizes results by step without cross-source inference. At least one source
must be configured. Summarize an unconfigured optional source, source failures
and no-data populations separately from findings.

Read source summaries first, then step anomalies/gaps. Inspect the applicable
Reader throughput/errors/iteration timing, read branch activity, direct/replica
read observations and service read evidence from the catalog. For non-advancement,
position context without incident-time readable backlog and expected demand is
not proof of a stall. For opens, distinguish recorded open outcomes/timing from
a root cause. These are interpretation boundaries, not extra deterministic rules.

Drill into a specific anomaly with `wp diag metrics check <name>` or `wp diag
trace check <name>`, discovering actual names/formulas in `wp diag catalog`.
Use the same fixed incident interval first. The tool Skills `wp-metrics-checks`
and `wp-trace-checks` provide source-specific calling and result semantics.
Do not duplicate their thresholds or hide missing evidence. Trace service scope
is not a log/request correlation; Reader iteration metrics and full ReadNext
spans measure different intervals, and tail waits may be expected.

State observed facts with check IDs, measurements and criteria; distinguish
suspected explanations from confirmed relationships. Find the smallest missing
fact needed next using source queries, Trace IDs and evidence references in the
report. Do not run every possible follow-up or a model call for an ordinary,
adequately observed result. Stop when the supplied evidence answers the user's
question or explain the specific unresolved gap.

`wp diag scenario all` is an optional agreed-catalog sweep for the selected target,
not all services/metrics. Milvus-owned diagnostics are not supported. Current metadata
inspection uses existing `wp meta` and cannot establish historical state.
No repairs, cluster changes, fault injection or load generation are part of this
workflow. The WebUI is optional; the JSON is the same result contract.
