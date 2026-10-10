# wp — Woodpecker Operational CLI

`wp` is the Woodpecker operational CLI for service-mode clusters.
See [`docs/wip/wpcli/wpcli-design.md`](../../docs/wip/wpcli/wpcli-design.md) for the full design.

## Build

    make wpcli          # current platform
    make wpcli-release  # linux/amd64, linux/arm64, darwin/arm64, windows/amd64
    ./bin/wp version

## Quick start

See [`docs/admin-guides/wpcli/quickstart.md`](../../docs/admin-guides/wpcli/quickstart.md) for a 15-minute onboarding guide.

## In-pod usage (zero-config)

`wp` is bundled in all server images and is on `PATH`, so you can operate a
cluster straight from a server pod with nothing to install:

```bash
kubectl exec -it <woodpecker-pod> -- wp cluster info
kubectl exec -it <woodpecker-pod> -- wp node list
```

This works with no flags because the images set
`WOODPECKER_ENDPOINT=http://localhost:9091` and the admin HTTP API listens on
`9091` inside the container.

### Endpoint precedence

`wp` resolves the admin endpoint in this order (highest priority first):

1. `--endpoint <url>` flag
2. `$WOODPECKER_ENDPOINT`
3. `cli.yaml` active context

Because `$WOODPECKER_ENDPOINT` is baked into the server images, a `cli.yaml`
mounted into a pod is overridden by it; pass `--endpoint` to target a different
cluster from inside a pod.

## Minimum config

Create `~/.woodpecker/cli.yaml`:

```yaml
current-context: local

contexts:
  local:
    endpoint: http://localhost:9091
    admin_port: 9091
```

See [`docs/admin-guides/wpcli/configuration.md`](../../docs/admin-guides/wpcli/configuration.md) for the full reference.

## Running outside Kubernetes

Set `node_admin_urls` in the active CLI context, or repeat
`--node-admin-url 'ADVERTISED_ADDRESS=http://127.0.0.1:FORWARDED_PORT'` to route
peer requests through separate per-node port forwards. This applies to both
memberlist discovery and metadata quorum targets, including mapped historical
nodes absent from memberlist. Original node identities remain unchanged.
See the [external Kubernetes configuration example](../../docs/admin-guides/wpcli/configuration.md#running-outside-kubernetes).

## Commands

### Node lifecycle
- `wp node list` — list all server nodes
- `wp node show <node>` — detailed view of one node
- `wp node decommission <node>` — graceful decommission (default blocks until safe)
- `wp node drain-status <node> [-w]` — watch decommission progress
- `wp node cancel-decommission <node>` — abort an in-progress decommission
- `wp node restart <node>` — intentionally not implemented (exit 10)

### Cluster overview
- `wp cluster info` — cluster summary + topology tree
- `wp cluster health` — red/yellow/green health check
- `wp cluster gossip-diff` — detect membership view divergence

### Config & env
- `wp config show <node>` — show resolved server config
- `wp config diff --all` — compare config across nodes
- `wp env show <node>` — env vars + Go runtime + host + build info
- `wp env diff` — compare env across nodes

### Diagnostics
- `wp profile <node> --type cpu --seconds 30` — download pprof profile

### Log metadata
- `wp log readers <logName>` — where each of a log's readers has read to (etcd only, no node contact)

### Log end to end
- `wp log scan <logName> [--mode quick|raw] [--from-segment N] [--to-segment N]` — sweep every
  segment of a log and reconcile what metadata claims against what the replicas hold. `quick`
  (default) reads structure only — one trailer read per sealed segment — and answers "does the
  shape add up". `raw` reads through the normal path with codec and CRC and answers "how far does
  a reader actually get". Reports one line per segment plus the log-level findings.

  Per-segment verdicts: `ok`, `short` (no replica has some of what metadata claims), `open` (still
  being written), `compacted` (served from one shared object-storage copy, which this sweep does
  not read), `reclaimed` (retention is deleting it), `unknown` (no replica answered, or every
  replica's survey stopped before the end of the segment, so nothing could be established).

  Exit codes follow what the sweep established: `0` only when every segment was established and
  sound, `9` when a segment is short or an open segment has a hole in the middle of what was
  written, and `8` when something could not be established — a sweep that learned nothing neither
  claims loss nor claims the log reads through. `--strict` promotes `8` to `9` for a script that
  wants an unestablished segment to fail the gate.

  A replica that reports no local data is read through the compacted mark, the tombstone cleanup
  writes before dropping a compacted segment's `data.log`: with the mark the copy was reclaimed and
  the object is the authority, without it the replica looked and holds nothing, which is loss. The
  segment's metadata state is a weaker second signal, since cleanup keys off the object-storage
  footer and the metadata update after compaction is only warned about when it fails.

  Log-level findings: ids missing from metadata at or above the truncation point (the segment *at*
  the point is kept by truncation, so its absence is a loss), and the Active segments, reported
  without failing on them because a roll with queued appends legitimately leaves two Active.

### Declaring data unreadable
- `wp log skip-range list [<logName>]` — the declared skip ranges, with each one's age and reason.
  With no log name it lists every log, including ranges whose log no longer exists: log ids are
  never reused, so such a range can never apply again, but it stays until it is removed.
- `wp log skip-range add <logName> <segmentId> --from-entry N --to-entry M --reason "..." [-y] [--force]`
  — declare an entry range unreadable so readers move past it. **This gives up data.** It asks the
  quorum first and refuses in two cases: a replica that can still read part of the range (a read is
  served by any one replica, so such a range is not lost), and a replica that **said nothing** about
  part of it. The second is the one that matters most — a survey reports only what it walked, so an
  unread range is not an empty one, which is what a bounded survey, a broken chain, a compacted
  segment or an unwritten tail all produce. A segment naming no replica refuses too. `--force`
  carries any of those decisions and says so; without `-y` nothing is written.
- `wp log skip-range remove <logName> <segmentId> --from-entry N --to-entry M` — withdraw a
  declaration once it is no longer needed, typically after the WAL has been truncated or compacted
  away and the lost entries can never be read again regardless. Withdrawing part of a range splits
  it, so the boundaries do not have to match the original declaration. It restores nothing, and a
  reader that already moved past the range does not come back for it. Use `--log-id N <segmentId>`
  instead of the name to withdraw a range whose log has been deleted.

All three read and write one record for the whole metadata root, under
`<meta-prefix>/skipranges`, indexed by log id and then segment id. A write refuses if the record
moved since it was read, so two operators cannot drop each other's ranges.

A skip range is a statement of fact: an operator has established that these entries are
permanently unreadable (typically a damaged disk), and any reader reaching them must move past
rather than wait. The declaration is the source of truth; there is no expectation that the data
becomes readable again, and `remove` exists for the later point in the log's life when the WAL has
been truncated or compacted away and the record itself is no longer needed — it does not restore
the entries, and a reader that already moved past them does not come back.

A reader consults that record **only while it is making no progress** — a position unchanged since
its last report — and then moves past a range covering its position, logging a warning and counting
it. A reader that is advancing never consults it, which keeps the per-poll cost of the common,
healthy case at zero.

**The read path never waits for the record.** A reader is answered from what the client already
holds, and an elapsed refresh interval only starts a re-read behind it, one at a time however many
readers ask. So `woodpecker.client.skipRangeRefreshInterval` (default 10s) bounds how often the
record is re-read once a stalled reader starts asking — not the absolute age of the copy: if no
reader has stalled for a long time the copy is correspondingly old. Shortening it never makes a
read slower. How soon a *newly* declared range takes effect is set by the reader's report tick
instead: the ranges are asked for only while the reader is stalled, and a cold cache answers empty
on that ask and only starts the refresh, so a lone reader sees a new range on its following tick.

### Segment across its quorum
- `wp segment probe <logName> <segmentId>` — ask every replica how far it can read the segment; names a damaged replica failover is covering for
- `wp segment inspect <logName> <segmentId>` — walk the blocks on every replica: which entries are damaged where, whether the damage is bounded, and what a skip would cost

### Logstore runtime
- `wp logstore segments <node>` — list active segments
- `wp logstore segment-show <node> --log X --seg Y` — detailed segment view
- `wp logstore buffer <node>` — buffer bytes summary
- `wp logstore flush-queue <node>` — flush queue depth summary
- `wp logstore force-flush <node>` — force sync
- `wp logstore lac <logName> <segmentId>` — quorum view of how far a segment is confirmed readable
- `wp logstore fence <node> --log X --seg Y --reason "..." -y` — fence one node; on a multi-node quorum this alone does not stop a write
- `wp logstore fence-quorum <logName> <segmentId> --reason "..." -y` — fence enough of the quorum (wq-aq+1 nodes) to interrupt a write
- `wp logstore compact <node> --log X --seg Y` — force compaction

### Metrics analysis
- `wp metrics list <node>` — list all metric series
- `wp metrics snapshot [<node>|--all]` — point-in-time snapshot
- `wp metrics top --by <metric>` — cross-node top-N
- `wp metrics watch <metric> <node>` — real-time trend stream
- `wp metrics report --scenario <name>` — scenario-based analysis (12 built-in)

### Ops registry
- `wp ops list <node>` — in-flight operations
- `wp ops show <node> --op-id <id>` — single op detail
- `wp ops stats <node>` — registry utilization + eviction analysis

### Dynamic logging
- `wp logging get-level [<node>|--all]` — read current log level
- `wp logging set-level <node> --level <level>` — change log level

### Kubernetes
- `wp k8s status` — cluster status (print kubectl commands, `-x` to execute)
- `wp k8s scale --replicas N` — scale cluster
- `wp k8s logs <node-or-pod>` — tail pod logs
- `wp k8s doctor` — not yet implemented

### CLI contexts
- `wp ctx list` — list configured contexts
- `wp ctx use <name>` — switch active context
- `wp ctx view` — show resolved active context

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Network/connection error |
| 2 | Usage error |
| 3 | Target not found |
| 4 | State conflict |
| 5 | Wait/Watch timeout |
| 6 | Strict mode partial failure |
| 7 | User abort |
| 8 | Yellow finding |
| 9 | Red finding |
| 10 | Intentionally not implemented |
| 11 | Resource not found |
| 12 | Configuration error |
| 13 | Prerequisite missing |
| 100+ | K8s execute passthrough (100 + kubectl exit code) |

## Incident response cookbook

See [`docs/admin-guides/wpcli/cookbook.md`](../../docs/admin-guides/wpcli/cookbook.md) for incident-response recipes.

## Incident workflows

The [website operations guide](../../docs/operations.html) connects node drain, orphan cleanup,
write progress/quorum fencing, log audits, reader diagnosis, skip-range recovery and retention.
The [CLI cookbook](../../docs/admin-guides/wpcli/cookbook.md) supplies command sequences and
verification steps. The [v0.1.46 coverage review](../../docs/admin-guides/wpcli/release-0.1.46.md)
distinguishes merged capabilities from closed proposals and still-open metadata/timeout work.
