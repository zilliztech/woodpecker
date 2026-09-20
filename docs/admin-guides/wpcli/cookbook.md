# wp CLI Cookbook

12 common incident-response recipes for Woodpecker operators.

---

## 1. Decommission a node safely

```bash
# Initiate graceful decommission (blocks until safe)
wp node decommission node-3

# Or watch progress separately
wp node decommission node-3
wp node drain-status node-3 -w

# If you need to abort
wp node cancel-decommission node-3
```

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
`docs/compacted_file_cleanup.md`). A node that keeps failing for ~30 minutes parks its
record as `NOTIFY_PENDING_MANUAL` — excluded from auto-retry, waiting for an operator.
You'll also see a `compacted-mark distribution parked for manual handling` warning in the
writer's logs.

```bash
# List records waiting for an operator (across all logs)
wp marking list

# Include in-flight / completed records, or narrow to one log
wp marking list --all-states
wp marking list --log 42

# Investigate the unacked node shown in the record
wp node show <node>

# Node is permanently dead or removed? Confirm (transitions the record to
# OPERATOR_CONFIRMED — settled; it is physically removed at truncate-reap)
wp marking confirm 42 7
```

Notes:
- These commands read etcd directly; endpoints, the meta prefix, AND the cluster's etcd
  TLS/auth settings are discovered from any node's `/admin/config` automatically. Override
  with `--etcd` / `--meta-prefix` if the admin plane is unreachable, and
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
- Full endpoint reference, including every field: `common/http/README.md`.
