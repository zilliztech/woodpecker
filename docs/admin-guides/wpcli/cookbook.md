# wp CLI Cookbook

Step-by-step incident-response workflows for the operational surface on current master for v0.1.46.
Command examples use representative names and IDs; replace them with values from your application and cluster.
They are usage examples, not captured production output.

Start with the [configuration reference](configuration.md), then choose a main workflow:

- Node lifecycle and local data: recipes 1, 11–14.
- Write progress and deliberate fencing: recipes 2 and 15.
- Reader diagnosis and whole-log audit: recipe 16.
- Accepting an unreadable range and observing recovery: recipe 17.
- Compaction, marks and retention after corruption: recipes 11 and 18.
- Drift, metrics and evidence collection: recipes 3–10, 13 and 19.

The [website operations guide](../../operations.html) explains how the results fit together.

---

## 1. Decommission a node safely

```bash
# Initiate graceful decommission (blocks until safe)
wp node decommission node-3

# Alternatively, start asynchronously and watch separately
wp node decommission node-3 --async
wp node drain-status node-3 -w

# If you need to abort
wp node cancel-decommission node-3
```

Only terminate when `safe_to_terminate` is true; a timeout is not that signal.
A non-empty idle Active segment now rolls from the **client** auditor under the existing
policy (default 600s plus an auditor tick, default 10s), then still needs compaction and
cleanup. The newly empty segment creates no data file and does not pin local data.
This needs a live application log handle/auditor; the server alone cannot roll it.
If drain remains stuck, compare `instance data`, `logstore segments` and outstanding ops,
then follow recipes 12 and 18. After deployment tooling replaces the node, verify peer
membership (`node list --strict`, `cluster gossip-diff`) and application write/read
progress. Improved readmission after abrupt death is not a guarantee of immediate convergence.

## 2. Investigate a stuck flush

```bash
# Check for long-running ops
wp ops list node-1 --longer-than 30000

# Look at flush queue depth
wp logstore flush-queue node-1

# Check for stall signal (old evictions > 0)
wp ops stats node-1

# Run the automated scenario
wp metrics report node-1 --scenario stuck-flush --window 1m

# Drill into a specific segment
wp logstore segment-show node-1 --log 42 --seg 7
```

## 3. Compare config across nodes

```bash
# Quick drift check
wp config diff --all

# Compare specific nodes
wp config diff node-1 node-2

# Full config dump in JSON
wp config show node-1 -o json | jq .
```

## 4. Check for gossip split-brain

```bash
# Compare memberlist views across all nodes
wp cluster gossip-diff

# If drift is detected, check individual views
wp cluster info
wp node list
```

## 5. Download a CPU profile

```bash
# 30-second CPU profile
wp profile node-1 --type cpu --seconds 30 --output-file cpu.pb.gz

# Heap profile
wp profile node-1 --type heap --output-file heap.pb.gz

# Analyze with go tool pprof
go tool pprof cpu.pb.gz
```

## 6. Change log level for debugging

```bash
# Enable debug logging on a specific node
wp logging set-level node-1 --level debug

# ... reproduce the issue ...

# Check current level
wp logging get-level --all

# Restore to info
wp logging set-level node-1 --level info
```

## 7. Find the busiest node

```bash
# Rank nodes by operation count
wp metrics top --by woodpecker_server_logstore_operations_total

# Rank by active segments
wp metrics top --by woodpecker_server_logstore_active_segments

# Watch a specific metric in real time
wp metrics watch woodpecker_server_logstore_operations_total node-1
```

## 8. Check for under-replication

```bash
# Run automated scenario
wp metrics report --scenario under-replication --window 2m

# Also check
wp metrics report --scenario quorum-degraded --window 2m

# Manual check
wp cluster health
```

## 9. Scale a K8s cluster

```bash
# See what would happen (print mode)
wp k8s scale --replicas 5 --wp-cluster wp-prod -n woodpecker

# Execute
wp k8s scale --replicas 5 --wp-cluster wp-prod -n woodpecker -x

# Verify
wp k8s status --wp-cluster wp-prod -n woodpecker -x
```

## 10. Quick health check

```bash
# Traffic-light health check
wp cluster health

# Cluster topology
wp cluster info

# All segments across a node
wp logstore segments node-1

# Op registry state
wp ops stats node-1
```

Exit code interpretation:
- **0**: all green
- **8**: yellow finding (warning, investigate)
- **9**: red finding (critical, act now)

## 11. Triage stuck compacted-mark distribution (PENDING_MANUAL)

After a segment is compacted, the writer's auditor distributes a "compacted" mark to every
quorum node (durable progress under `root/marking/<logId>/<segId>` in etcd; see
[compacted-file cleanup](../../compacted_file_cleanup.md)). A node that keeps failing for ~30 minutes parks its
record as `NOTIFY_PENDING_MANUAL` — excluded from auto-retry, waiting for an operator.
You'll also see a `compacted-mark distribution parked for manual handling` warning in the
writer's logs.

```bash
# List records waiting for an operator (across all logs)
wp marking list --etcd etcd-a:2379 --meta-prefix by-dev/woodpecker

# Include in-flight / completed records, or narrow to one log
wp marking list --all-states --etcd etcd-a:2379 --meta-prefix by-dev/woodpecker
wp marking list --log 42 --etcd etcd-a:2379 --meta-prefix by-dev/woodpecker

# Investigate the unacked node shown in the record
wp node show <node>

# Node is permanently dead or removed? Confirm (transitions the record to
# OPERATOR_CONFIRMED — settled; it is physically removed at truncate-reap)
wp marking confirm 42 7 --etcd etcd-a:2379 --meta-prefix by-dev/woodpecker
```

Notes:
- These commands read etcd directly. Discovery from a node's `/admin/config` is attempted, but
  **expect to pass `--etcd` and `--meta-prefix` yourself**: the server does not connect to etcd,
  so that whole section is unvalidated. By default the endpoint is loopback, which the command
  refuses rather than dialing your own machine; if etcd really does run on that host, pass
  `--etcd` explicitly to proceed. The meta prefix fails more quietly — a wrong one connects to
  the right etcd and lists an empty keyspace, so `list` prints the prefix it scanned for you to
  check against the client's `etcd.rootPath` (default `by-dev`) plus `woodpecker.meta.prefix`.
  TLS/auth settings come from the same section, overridable with
  `--etcd-cert/-key/-cacert` / `--etcd-username/-password` for a secured etcd (the
  discovered cert paths are server-side paths — valid when running in-pod).
- `confirm` marks the record `OPERATOR_CONFIRMED` — durable across writer restarts and
  hidden from the default list — rather than deleting it; the record is physically reaped
  when the segment is truncated.
- Confirming is low-stakes: a data-holding node self-heals its mark via the server-side
  pull reconcile once it comes back; a node that never held the segment's bytes only loses
  a read optimization.
- Doing nothing is also safe: the record is reaped automatically when the segment is
  truncated. The queue exists for visibility ("a node has been unreachable for 30m — look
  at it"), not because data is at risk.
- `confirm` refuses `IN_PROGRESS`/`COMPLETED` records (they're managed automatically);
  `--force` overrides.

## 12. Find local data left behind by a deleted instance

An instance's node-local WAL data is only reclaimed when the control plane calls
`POST /admin/instance/delete` on every node holding it. That broadcast is lossy: a node
that was down when the delete went out has no marker, so nothing on it will ever reclaim
the data. It shows up later as a PVC that will not drain, or a pod stuck at
`has_local_data: true`.

```bash
# What every node still holds, refusing to answer on a partial view
wp instance data --all --strict

# Is one specific instance gone yet? (both filters required, or neither applies)
wp instance data --all --bucket a-bucket --root in01-abc

# Just one node
wp instance data node-1
```

Reading the output:
- `PROCS` (active processors) and the data's age are what separate a genuine orphan from
  an instance that was merely created a moment ago and has not flushed yet. Treat a row as
  stranded only when `PROCS` is 0, the data is old, and the control plane no longer knows
  the instance.
- `DELETE_STATE` of `instance` or `log` means the delete already landed and the grace
  window is running — do not re-send it, and do not read it as a failure.
- `LIVE` counts only segments not yet durably compacted to object storage, which is the
  same predicate `/admin/node/decommission/progress` uses for `has_local_data`. A row with
  `SEGMENTS` > `LIVE` is why those two can disagree.

Reclaiming one, once you are sure:

```bash
# There is no wp subcommand for the delete — it is deliberately explicit and per-node.
# Send it only to the nodes the query above listed for that instance.
curl -X POST http://<node>:9091/admin/instance/delete \
  -H 'Content-Type: application/json' \
  -d '{"bucketName":"a-bucket","rootPath":"in01-abc"}'

# Then poll until it is absent everywhere
wp instance data --all --strict --bucket a-bucket --root in01-abc
```

Notes:
- **`--strict` is the safety flag, not a formatting one.** Without it an unreachable node
  is merely a row in the table; with it the command exits non-zero. An unreachable node
  still holds intact data, so deleting on a partial view is exactly the accident this
  recipe exists to prevent. Nodes reporting scan errors are flagged for the same reason.
- Reclamation is asynchronous (there is no `sync` option on the instance delete), so the
  data disappears a few seconds after the call, not immediately — poll rather than expect
  the next query to be empty.
- Full endpoint reference, including every field: [admin API reference](../../../common/http/README.md).

## 13. Tell a quiet log from a sick one

`/healthz` decides whether Kubernetes keeps routing to a pod; `/admin/log-health` says how reading
and writing is actually going, per log. They disagree on purpose — a stalled log never pulls a node
out of rotation — so a pod passing its probe while serving nothing is a state you have to look for.

```bash
# The probe readiness acts on. Exits non-zero when the node reports unhealthy.
wp node healthz node-1

# What the data path looks like from that node
wp node log-health node-1

# Narrow to one instance (both filters required, or neither applies)
wp node log-health node-1 --bucket a-bucket --root in01-abc
```

Notes:
- Counts come first: `tracked / healthy / stalled / failed / idle`. A stalled log is one whose
  reads or writes stopped completing — not one nobody is using, which counts as idle.
- Health is derived from real operation outcomes, not synthetic probes, so a log nothing has
  touched reports healthy rather than unknown. Reaching the end of a log is a healthy read.
- From outside the cluster, `wp node log-health 127.0.0.1:9091` works against a `kubectl
  port-forward`: an explicit `host:port` is dialed as given rather than looked up in the
  memberlist, which only knows in-cluster names.

## 14. Bootstrap outside Kubernetes

Forward **each** replica's admin HTTP port to a different local port, in separate terminals:

```bash
kubectl -n woodpecker port-forward pod/wp-0 19091:9091
kubectl -n woodpecker port-forward pod/wp-1 29091:9091
kubectl -n woodpecker port-forward pod/wp-2 39091:9091
```

Use an `external` context with a reachable seed and `node_admin_urls` mappings for the
original advertised service addresses. See the full [configuration example](configuration.md#running-outside-kubernetes).
A one-off override looks like this (replace the key with your cluster's real address):

```bash
wp --context external \
  --node-admin-url 'wp-0.wp-headless.woodpecker.svc.cluster.local:18080=http://127.0.0.1:19091' \
  node show wp-0
wp --context external node list --strict
wp --context external cluster gossip-diff --strict
```

A seed forward alone does not expose the other replicas. Mappings change only admin HTTP
destinations; original identities stay in output and quorum checks. A mapped historical
quorum address absent from memberlist can still be queried. Each endpoint must reach the
intended replica, rather than a load balancer choosing arbitrary pods. Recreate port
forwards after pod replacement. Raw `curl` commands must use the reachable origin
explicitly; the CLI map does not rewrite them.

Metadata access is separate: expose etcd too, and use the **embedding client's** metadata
prefix. The server's unused etcd config is not evidence of the actual deployment's settings.
For recipes 15–18 below, this shell helper makes those choices explicit:

```bash
# Substitute your reachable etcd endpoints, actual prefix and CLI context.
ETCD_ENDPOINTS=127.0.0.1:2379
META_PREFIX=by-dev/woodpecker
WP_CONTEXT=external
wp_meta() {
  wp --context "$WP_CONTEXT" "$@" --etcd "$ETCD_ENDPOINTS" --meta-prefix "$META_PREFIX"
}
```

`wp_meta` is a shell helper defined above, not a new CLI command. Only use it with commands
that accept metadata flags. Add the `--etcd-cert`, `--etcd-key`, `--etcd-cacert`,
`--etcd-username` and `--etcd-password` overrides as required by your etcd deployment.
When both `--etcd` and `--meta-prefix` are set, metadata connection discovery is skipped.
Metadata-only commands (`log readers`, skip-range `list`/`remove`, `marking`) can therefore
run without contacting the seed; quorum commands still need seed discovery and replica access.

## 15. Diagnose writes that stopped advancing

Example: the application knows `my-channel` has log ID 42; segment 7 is suspected.
Names are used by metadata commands; numeric IDs are used by the node's processor/ops queries.

```bash
wp --context external node list --strict
wp --context external ops list node-1 --type logstore.add_entry --log 42 --seg 7
wp --context external ops list node-1 --type logstore.add_entry --longer-than 30000
wp --context external logstore segment-show node-1 --log 42 --seg 7
wp --context external logstore buffer node-1
wp --context external logstore flush-queue node-1
wp_meta logstore lac my-channel 7
# Repeat the readings, and query the other quorum nodes.
wp_meta logstore lac my-channel 7
```

Use two samples to tell progress from a snapshot. Increasing LAC means confirmation
continues; one slow replica is absorbed while `aq` acknowledgements still arrive. For a
non-Active segment the metadata end is authoritative, and no live local writer is expected.
An empty op-registry snapshot means no matching append **on that node at that instant**;
it does not prove the application has no queued work. Check the application and all involved
replicas before declaring it idle.

Buffer growth, flush queue saturation and scheduler waiting/running counts separate
arrival, backpressure and flush execution. See recipe 2 for supporting metrics and profiles.
Metrics live where the work lives: client frontiers belong to the embedding application,
not to the Woodpecker servers. `metrics list` lists names, not values:

```bash
# 19091 is a Woodpecker seed forward; 49091 forwards the application /metrics port.
wp --endpoint http://127.0.0.1:19091 metrics list 127.0.0.1:49091 --filter woodpecker_client
wp --endpoint http://127.0.0.1:19091 metrics snapshot 127.0.0.1:49091 \
  --metric woodpecker_client_write_frontier_segment -o json
wp --endpoint http://127.0.0.1:19091 metrics snapshot 127.0.0.1:49091 \
  --metric woodpecker_client_write_frontier_entry -o json
```

Compare matching labels and the `(segment, entry)` pair, since entry IDs reset per segment.
The write frontier is confirmed LAC, not submitted position. Submitted position/oldest queue
age and auditor-outcome metrics proposed by #371 were not merged; use the server op registry
and the application's auditor summary logs. `metrics watch` aggregates a metric's series on
the process, so it cannot replace a per-log/per-reader position comparison.

The application's `woodpecker.client.segmentAppend.sendTimeout` defaults to **2000ms** for
stream opening and the first buffered response. Durability confirmation, retries, ordered
queues, segment completion and rolling are separate costs. The CLI's `--timeout` does not
change those budgets; do not assert that total append/failover latency must be below 2s.
Active quorum reads default to 3000ms per replica, settled reads to 20000ms, with a settled
retry when the active bound might have coincided with a state transition. These are client
settings under `woodpecker.client.segmentRead`, not a promise about total read latency.

If a deliberate writer interruption is required, fence enough members of the **segment's**
quorum, not an arbitrary node:

```bash
# Preview without touching replicas: expected exit 7 because -y was not supplied.
wp_meta logstore fence-quorum my-channel 7 --reason "incident: stalled writer"
# Execute after reviewing the target and ensuring the application handles reopen.
wp_meta logstore fence-quorum my-channel 7 --reason "incident: stalled writer" -y
# Verify the application reopened/rolled and resumed confirmation.
wp_meta log scan my-channel --mode quick
```

Interrupting this segment needs `wq - aq + 1` fenced nodes. `--nodes node-1,node-2`
selects original quorum members; selecting too few is refused before execution. Nodes
without a live processor refuse fencing and do not count. A partial attempt may already
have fenced some replicas even when the final result is a conflict. Fencing neither stops
future writers permanently nor decommissions a node; the application's ordinary error,
rolling/reopen and quorum-selection path still owns recovery. Per-node `logstore fence`
exists but fencing one of three with `aq=2` cannot reliably interrupt the quorum.

## 16. Diagnose a reader, then audit the suspected range

```bash
wp_meta log readers my-channel
# Repeat after at least a 30s reporting interval; compare CURRENT and LAST_REPORT.
wp_meta logstore lac my-channel 7
# If no segment is suspected, start with the cheap structural sweep.
wp_meta log scan my-channel --mode quick
# Narrow expensive verification to the suspect segment.
wp_meta log scan my-channel --mode raw --from-segment 7 --to-segment 7
wp_meta segment probe my-channel 7 --from-entry 50 --max-entries 100
wp_meta segment inspect my-channel 7 --max-blocks 4096
```

Read the evidence in order:

1. `log readers` reads leased checkpoints directly from etcd. `CURRENT` is the last
   published read position, not necessarily the exact pending entry. `no read since open`
   differs from a live progressing reader; `stale report` means reporting stopped, not that
   data corruption has been established. Sampling faster than 30s can make all readers look frozen.
2. Active `lac` assembles the `aq`-th highest durable node position. Fewer than `aq`
   successful positions establishes no quorum LAC. Completed/Sealed ends come from metadata.
3. `log scan --mode quick` trusts structure; it does not verify payload CRC. `raw` verifies
   local block data and reconciles coverage, but does not verify the authoritative shared
   object-storage copy of a Sealed segment. Scan exit 9 means established damage/inconsistency;
   exit 8 means unknown coverage. `--strict` promotes unknown findings to exit 9.
4. `probe` attempts bounded reads and stops at the first failure. A probe ending at its
   window limit is not evidence that the data ends there. A compacted source is one shared
   object copy, even if several replicas return it.
5. `inspect` walks beyond readable blocks when the chain/index allows it. Compare entry
   ranges across replicas, not block numbers: their flush boundaries differ. `READABLE`
   applies to the entire verified block; `RECOVERABLE` names a prefix a future repair could
   salvage, which normal readers do not serve. `data_incomplete` at an unsealed tail is not
   established corruption. A missing header can leave everything after it unaccounted for.

Unknown/unreachable, bound-limited, shared-object or unaccounted answers cannot prove all
replicas lost a range. Raise `--max-blocks` or page with `--from-block` where appropriate,
restore reachability and repeat. A checksum warning on a still-written Active tail alone
is not a loss diagnosis. No generic replica rebuild or background scrub command exists.

## 17. Recover a stalled reader with an explicit skip declaration

Prerequisite: a settled inspection establishes that every replica has lost entries **50–59**
of segment 7 of `my-channel`. These numbers are an example; use the survey's actual boundary.
If one healthy replica can serve the entries, allow normal failover and do not declare them lost.

```bash
wp_meta log skip-range list my-channel
# Preview the newly lost entries and stop without writing (expected exit 7).
wp_meta log skip-range add my-channel 7 --from-entry 50 --to-entry 59 \
  --reason "incident: every replica damaged; inspection evidence saved"
# Accept the data loss explicitly after reviewing the evidence.
wp_meta log skip-range add my-channel 7 --from-entry 50 --to-entry 59 \
  --reason "incident: every replica damaged; inspection evidence saved" -y
wp_meta log skip-range list my-channel
wp_meta log readers my-channel
```

Both ends are inclusive. `add` inspects the quorum before writing and normally refuses if
any replica can serve any part, if a replica is unreachable, or if the requested range is
not fully accounted for. `--force` bypasses that evidence gate and still requires `-y`;
it should not be used to turn an incomplete survey into a claim of loss. The write uses
compare-and-swap; a revision conflict means reread and review, not blind overwrite.

The declaration gives up entries; it neither repairs bytes nor changes the completed end,
compacts the segment or truncates it. Reader behavior is intentionally conditional:
only a reader stalled at an unchanged position consults declarations on its report tick.
A healthy reader does not unconditionally skip readable entries even under a forced declaration.
A reader opening inside a range is not moved at open; it follows the same stall path.

Allow reporting/cache propagation before expecting progress. Reporting is **30s**;
refresh is asynchronous and a cold cache can require another tick. The client setting
`woodpecker.client.skipRangeRefreshInterval` (default **10s**) controls refresh frequency,
not a guaranteed time from declaration to jump. After applying the example range, the
reader tries entry 60, or proceeds to the next segment if past the completed end. Verify
consumer progress, checkpoints, the application's `reader moved past an operator-declared
skip range` WARN and `woodpecker_client_reader_skip_range_skips_total`.

After data is restored, allow readers to try it again:

```bash
wp_meta log skip-range remove my-channel 7 --from-entry 50 --to-entry 59
# List all declarations, including those whose logs were deleted.
wp_meta log skip-range list
# Withdraw a deleted log's declaration by its original ID instead of name.
wp_meta log skip-range remove --log-id 42 7 --from-entry 50 --to-entry 59
```

Withdrawal neither restores data nor rewinds readers. Reopen/replay through the application
if restored entries need to be consumed. Overlapping additions coalesce, partial removal
can split a range, and adjacent ranges keep separate reasons. Declarations persist until
removed, even after truncation/deletion; a deleted log's ID is not reused.

## 18. Close the compaction and retention loop for a damaged segment

```bash
wp_meta log scan my-channel --mode raw --from-segment 7 --to-segment 7
wp_meta segment inspect my-channel 7 --max-blocks 4096
wp --context external metrics report node-1 --scenario slow-compact --window 1m
wp_meta marking list --log 42
```

A read needs any replica able to serve the entry. Compaction needs one integrity-verified
copy that covers the completed metadata end. One or two damaged replicas are tolerated
when an intact complete copy remains. If every replica is damaged, or the only surviving
copy ends before metadata `LastEntryId`, that segment cannot compact and stays Completed.
A union of readable ranges across replicas is not proof of one complete compactable copy.

The auditor tries Completed segments oldest first. **A segment failure is logged and skipped**,
so later segments can still compact in the same pass. Count/time budgets and cancellation
can defer later work, and a bad early segment can spend that budget. Once the application
marks it Truncated, the next auditor pass excludes it from Completed candidates entirely.
The application logs `segmentsProcessed`, `segmentsCompacted`, `segmentsFailed` and
`segmentsDeferred`; inspect segment states as well as frontiers, since later successful
compaction does not establish that all earlier segments are sealed.

For reader stalls, use recipe 17. Then expire data through the embedding application's
retention policy or SDK `LogHandle.Truncate` when its reader/retention requirements permit.
Completed and Sealed segments are accepted by truncation. **There is no `wp log truncate`.**
Truncation alone is not the recovery operation for a reader already parked on unreadable
bytes. A skip declaration changes reading only and does not make compaction succeed.

Active recovery differs from Completed corruption: restart recovery can shorten a damaged
local prefix; reopening the writer fences/completes against the quorum-derived target.
Inspect the resulting completed end rather than assuming the original end survives.
Server full-scan recovery now reports `segment recovery truncated at ...` warnings with
log/segment, file, offset, current entry and discarded bytes for incomplete/undecodable/
unexpected records. They explain a shortened prefix, not a guarantee that every checksum
fault is caught by writer recovery.

Use recipe 11 when compaction has succeeded but compacted-mark distribution is parked.
That queue is distinct from compaction failure. `marking confirm` settles a notification
record as OPERATOR_CONFIRMED; it does not repair data, repair segment metadata or perform
truncate. Verify using `marking list --all-states`, then let truncation reap the record.

Manual per-node escape hatches, if the complete metadata and backend are understood:

```bash
wp --context external logstore force-flush node-1 --log 42 --seg 7
# 99 must be the exact completed LastEntryId from metadata, not a guessed local tail.
wp --context external logstore compact node-1 --log 42 --seg 7 --expected-last-entry-id 99
```

Flush does not repair checksum failures. Manual compact is a per-node request, not a
transaction repairing coordinator metadata; state advancement and mark distribution belong
to the client auditor. An incorrect expected ID can publish an invalid footer.

## 19. Collect drift and runtime evidence

```bash
wp config diff --all --strict
wp env diff --strict
wp cluster gossip-diff --strict
wp config show node-1 -o json
wp env show node-1 -o json
wp metrics report --list
wp logging get-level node-1
wp logging set-level node-1 --level debug
wp profile node-1 --type cpu --seconds 30 --output-file node1-cpu.pb.gz
# Collect a short reproduction, then restore the previously recorded level (info here).
wp logging set-level node-1 --level info
```

An unreachable node is not an identical node. No config/env respondent fails the command;
a partial comparison only establishes agreement among respondents. Runtime/environment
fields can legitimately differ across nodes, so inspect the changed fields rather than
assuming all drift is a configuration defect. Readiness and per-log health are separate
checks (recipe 13). Kubernetes `status`, `logs` and `scale` print by default; `-x` executes.
`node restart` and `k8s doctor` are placeholders, not incident-recovery commands.

## Script outcomes and capability boundaries

| Exit | Meaning |
| --- | --- |
| 0 | Successful within the command's stated scope; quick scan is not payload verification. |
| 1 / 3 / 12 | Network/runtime error / target absent / configuration error. |
| 2 / 4 | Invalid usage / state or metadata revision conflict. |
| 5 / 6 | Wait timeout / strict partial view; neither authorizes terminating a node. |
| 7 | User abort / acceptance absent; expected for fence and skip-range previews without `-y`. |
| 8 / 9 | Yellow / red finding; inspect the report (scan strict mode promotes unknown to red). |
| 10 / 11 / 13 | Placeholder / resource absent / unmet prerequisite. |
| 100 + N | Executed kubectl exited N. |

There is still no generic `wp meta` editor, log listing, replica resync or truncate command.
Specific marking and skip-range edits do not fill those gaps. See the
[v0.1.46 coverage review](release-0.1.46.md) for the issue-to-document mapping.
