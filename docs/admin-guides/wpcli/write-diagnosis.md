# Diagnose the current WP write path

Use a wpcli binary built from this revision. This is an on-site, read-only check
of current WP service evidence. It works without Prometheus, Jaeger or Loki and
does not alter data, flush writers or repair replicas.

```bash
# Reuse your existing WP service connection and node-admin mappings.
wp --context local diagnose --scenario write --window 30s --interval 1s \
  --max-writers 16 --budget 1m -o json > write-report.json

# Current disk policy/admission only; progress rules remain inconclusive.
wp --context local diagnose node-0 --scenario write --mode snapshot
```

With no node argument, the command inspects all discovered nodes, within
`--max-nodes`. If the writer count exceeds `--max-writers`, it reports partial
coverage. Increase a limit deliberately or narrow the target; a truncated list
cannot establish that omitted writers are healthy. `--window` samples starting
now and does not retrieve incident history.

Read `collection_status` and `coverage_gaps` first, then inspect each finding:

| Finding | What it means | What to inspect next |
| --- | --- | --- |
| Append `suspected_stall` | The same operation persisted without matching-log completions | Its `operation_id`, `ops show`, and matching completion counters |
| Append `conflicting` | The operation stayed pending while same-log work completed | The particular operation may still be slow; aggregate progress cannot clear it |
| Flush `observed_buildup` | Buffer bytes or queued tasks grew while sampled | The same node/log/segment's writer details; incoming load versus downstream capacity |
| Flush `no_durable_progress_observed` | A nonempty queue persisted without durable entry advance | Matching `file.flush` operations and backend-specific evidence; this is not proof of fsync/disk failure |
| Flush `queue_saturated` | The queue was at its own capacity in every sample | Whether durable entries advance despite queue pressure; a busy queue is not automatically a stalled writer |
| `observed_admission_pressure` | Deployed policy and admission state report warn/blocked | Exported watermarks, current configuration and rejected writes/client errors |
| `inconclusive` | A required signal, identity, version or complete live window is missing | The finding's explicit gaps and `next_read_only_checks`; missing is not healthy |

For example, `queue 2→2/8`, `buffer 1024→1024 bytes`, `durable entry 5→9`
shows durable progress. A `file.flush` operation visible throughout that interval
does not establish that the entire writer is stalled. Conversely, a full queue
and durable progress can coexist: pressure is observable even though work still
completes. The command does not convert either case into a p95/p99 latency claim.

## Optional replica comparison

The WP service context selects nodes. A separate, explicitly selected WP-client
metadata namespace supplies a log's segment quorum. For Milvus, use that Milvus
instance's WP-client metadata prefix, not the WP service's own metadata prefix.

```bash
wp --context local diagnose --scenario write --window 30s \
  --log-name YOUR_LOG_NAME --segment 0 \
  --etcd http://127.0.0.1:2379 --meta-prefix YOUR_WP_CLIENT_PREFIX \
  -o json > replica-report.json
```

Existing etcd TLS/auth flags apply. The command reads two exact keys in one
read-only transaction, retains the revision and configured `es/wq/aq`, then
compares the selected segment's loaded writers across discovered replicas.
Writer detail collection is narrowed to that log/segment; append checks remain
node-level. Missing metadata does not fall back to another namespace.

`suspected_replica_lag` means a replica stayed behind while peers advanced, or
its gap grew. Fewer observed loaded writers than `wq`/`aq` means the observation
is incomplete; it does **not** prove writes or reads lost quorum. Sealed/unloaded
segments and idle positions do not establish a live write fault. API snapshots
are non-atomic and cannot establish client acknowledgements or prove a tenant
association merely from matching numeric log IDs.

```bash
# Further read-only quorum position check, with the same metadata selection:
wp --context local logstore lac YOUR_LOG_NAME 0 \
  --etcd http://127.0.0.1:2379 --meta-prefix YOUR_WP_CLIENT_PREFIX
```

Exit 0 means evaluation completed, including inconclusive findings. Add
`--fail-on-findings` for exit 8 on warning/critical findings. Required collection
failures, cancellation and budget exhaustion retain the JSON report and have
their own exit codes. Original focused `stuck-write`/`stuck-read` scenarios stay
compatible with the standalone tool. Expanded source/stage correlation belongs
to that tool; historical metrics, traces and logs are outside this native command.

See the [full rule contract and limits](https://github.com/zilliztech/woodpecker/blob/master/cmd/wpcli/diagnosis/README.md).
