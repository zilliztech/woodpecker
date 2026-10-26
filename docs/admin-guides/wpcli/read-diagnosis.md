# Diagnose current WP reads and reader progress

Use a wpcli binary built from this revision. The read scenario samples current
WP metrics, pending operations, leased reader records and an explicitly selected
segment's readability evidence. It requires no external monitoring system and
does not change data or metadata.

```bash
# Node-level success/EOF and pending read activity, without a tenant metadata join.
wp --context local diagnose --scenario read --window 30s -o json > read-report.json

# Inspect one WP-client log/segment and its readers. Supply the actual namespace.
wp --context local diagnose --scenario read --read-mode catch-up \
  --log-name YOUR_LOG --segment 0 \
  --etcd http://127.0.0.1:2379 --meta-prefix YOUR_WP_CLIENT_PREFIX \
  --window 65s --interval 5s --max-samples 14 --budget 90s \
  --max-readers 16 -o json > reader-report.json
```

The WP service context selects nodes. The separate etcd/prefix flags select the
Milvus or other WP-client metadata namespace. There is no automatic fallback to
the WP service's own namespace. Optional `--reader EXACT_NAME` reads one exact
reader key; otherwise `--max-readers` bounds the selected log's reader list and
lease checks. Omitting a quorum member from selected nodes is partial coverage.

`--read-mode` is an operator expectation: `catch-up`, `tail` or `unknown` (default).
It is not an observed property of a server operation. Reading older WAL data now
is a catch-up use case; it does not fetch historical metrics or an earlier reader
state. `--window` always starts sampling now.

## Read the evidence separately

| Rule | What to look for | Interpretation |
| --- | --- | --- |
| `read_activity` | Success/EOF completion deltas, persistent server ops, resets and registry coverage | `eof_observed` permits normal tail/boundary waiting. `forward_progress_observed` is aggregate matching-log progress, not proof one particular op completed. `pending_read_expectation_unknown` does not assert a catch-up stall. |
| `reader_progress` | Leased reader generation, next position, attempt report times, expected mode and available data throughout the window | `reader_forward_progress_observed` reports an actual position increase. `caught_up_or_awaiting_data` applies to the selected segment's boundary. `suspected_catchup_no_progress` requires renewed fresh attempts, catch-up expectation and a stationary position with available data at every sample. |
| `read_quorum_visibility` | Configured `es/wq/aq`, discovered replica identities and durable entry positions; finalized metadata for Sealed/Completed segments | The Active-segment boundary is the **aq-th highest** sampled durable position. Missing/unloaded replicas give partial/inconclusive evidence, not an outage or corruption verdict. Finalized metadata is not a physical payload verification. |

For example, a fresh reader repeatedly attempts `segment 0, next entry 5`, while
the selected readable boundary remains entry 10. Renewed attempts with no position
advance in catch-up mode are suspicious. A reader attempting entry 11 at that
same boundary can legitimately be waiting for data. Neither case identifies a
disk, network or object-store cause by itself.

Reader timestamps record the latest **read attempt report**, not the last
delivered message. An advancing timestamp alone is not progress. This revision's
client profile reports unchanged positions about every **30 seconds**; an
unchanged position over a shorter window is inconclusive. The selected WP-client
binary/version is not verified merely by querying a WP server, so that profile
assumption is retained as a gap. Freshness uses the profile's two-interval bound;
expired/unverified leases, future timestamps, stale reports and re-created
reader generations never become healthy zero values.

Server ops have no reader name or authoritative tenant namespace. Similar numeric
log IDs do not establish a join. The report therefore keeps server activity and
reader findings distinct, preserves namespace ambiguity, and does not label a
particular server op as the cause of one reader's wait. Reader movement can still
be observed when replica evidence is partial; a no-progress suspicion needs the
stronger expected-data conditions.

## Further read-only checks

Reuse the same context, node mappings and explicit metadata selection:

```bash
wp --context local log readers YOUR_LOG --etcd ENDPOINTS --meta-prefix WP_CLIENT_PREFIX
wp --context local logstore lac YOUR_LOG 0 --etcd ENDPOINTS --meta-prefix WP_CLIENT_PREFIX
wp --context local ops list NODE --type logstore.get_batch_entries

# Optional payload verification: choose explicit depth and time budgets.
wp --context local segment probe YOUR_LOG 0 --from-entry 5 --max-entries 100 \
  --etcd ENDPOINTS --meta-prefix WP_CLIENT_PREFIX
wp --context local segment inspect YOUR_LOG 0 --max-blocks 16 \
  --etcd ENDPOINTS --meta-prefix WP_CLIENT_PREFIX
```

The diagnostic pass does not invoke payload probes, whole-log scans, skip-range,
truncation, fencing or repair. An unreadable replica or metadata disagreement
alone is not corruption. Review existing skip declarations and truncation before
attributing a reader's wait to storage.

Exit 0 means evaluation completed, including inconclusive results. Use
`--fail-on-findings` for exit 8 on warning/critical findings. Required requested
source failures return 1/6, budget exhaustion 5 and cancellation 7, retaining
JSON evidence. Inspect `collection_status`, each finding's `unknown`, actual
observation times and `coverage_gaps` before interpreting an outcome.

The existing focused `stuck-read`/`stuck-write` v1 contracts remain compatible
with the standalone diagnostic tool. Its focused profiles do not yet import the
expanded read scenario; future stage correlation should import native findings
rather than duplicate these rules. Milvus consistency, visibility, query and
historical monitoring belong to the independent tool.
