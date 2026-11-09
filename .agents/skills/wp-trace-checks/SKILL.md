---
name: wp-trace-checks
description: Inspect retained Woodpecker WAL operation durations or error spans using independent wp diag Trace checks. Use for sampled Writer, append, flush, Reader or open-operation evidence; excludes Milvus-owned and Loki diagnosis.
---

Discover implemented operations and boundaries with `wp diag catalog --source
trace --output json`. Use the existing cli.yaml Trace connection, its exact
service bindings and the selected diagnostic target. Metrics and Admin are not
required. A missing/disabled Trace connection is a configuration error; an
unreachable configured connection is a collection failure.

```bash
wp diag trace check <name> --target <target> \
  --from <RFC3339-start> --to <RFC3339-end> --details all --output json
```

`--window` with optional historical `--to` is the alternative time form.
`--trace-id` selects an exact retained Trace, still assessed inside the incident
window. `--request-id` requires a configured `request_id_tag`. Preserve the same
normalized interval when collecting related Metrics. Returned context spans
outside the window are not assessed.

Duration and error checks are separate atomic questions. Read execution,
status/data_state, rule, `inspect.spans`, anomalies, query references and coverage
notes. Trace IDs in source summaries are deduplicated; trace counts, abnormal
span counts and abnormal-check counts are different quantities.

Default evidence is service-scoped. Only a verified target tag mapping or exact
request/Trace relationship can support a more specific association. Do not claim
that a span belongs to the selected Metrics log merely because it shares a
scenario, service, Kubernetes namespace or time range. Returned tags are checked
as well as backend selectors. Missing target tags remain a limitation.

No matching spans do not prove health. Sampling, export delay, retention, filters,
backend warnings and query limits affect coverage. For a budget failure, suggest
an explicitly narrower incident/service/operation/ID selection; never silently
truncate a search and interpret the retained subset as complete.

Timers are inclusive. Parent/child duration subtraction is not an isolated
critical-path measurement. Long ReadNext spans may be ordinary tail waits.
Append RPC cancellation can be normal stream closure; inspect error-semantics
coverage before treating it as a failed write. A duration threshold match alone
is not a confirmed cause. References come from the tool, not duplicated constants
in this skill.

Checks make no model call, change no runtime state and need no WebUI. Preserve
JSON for offline evidence review; Agent-led follow-up is a separate workflow.
