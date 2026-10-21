# wp — Woodpecker Operational CLI

`wp` is the Woodpecker operational CLI for service-mode clusters.
See [`docs/wip/wpcli/wpcli-design.md`](../../docs/wip/wpcli/wpcli-design.md) for the full design.

## Build

    make wpcli          # current platform
    make wpcli-release  # linux/amd64, linux/arm64, darwin/arm64, windows/amd64
    ./bin/wp version

## Quick start

See [`docs/admin-guides/wpcli/quickstart.md`](../../docs/admin-guides/wpcli/quickstart.md) for a 15-minute onboarding guide.

For an interactive session, run `wp shell` to choose a configured service context,
or `wp shell --context <name>` to enter directly. Attach tenant metadata explicitly
with `connect`; `status`, `disconnect`, and session-local `use` keep the selected
targets visible. See the [interactive shell walkthrough](../../docs/admin-guides/wpcli/shell.md).
The native [Interactive shell handbook](../../docs/docs.html#operations-cli-shell)
provides connection parameters, command prerequisites and output interpretation.

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

### Etcd connections

Metadata-consuming commands can read their connection from the selected context:

```yaml
current-context: external
contexts:
  external:
    endpoint: http://127.0.0.1:19091
    etcd:
      endpoints: [127.0.0.1:32379]
      meta_prefix: milvus-prod/woodpecker
```

Command flags override context settings; metadata commands use admin discovery
only for missing endpoints/prefix. Optional context fields include username,
password, tls_enabled, cacert, cert, key and tls_min_version. Certificate paths
refer to files on the machine running wp. See the
[etcd configuration reference](../../docs/admin-guides/wpcli/configuration.md#etcd-connections).

An etcd-only context can omit endpoint and meta_prefix for the raw commands:
`wp etcd list`, `wp etcd get <key>` and `wp etcd cat <key>`. They are read-only,
use literal keys, and never discover a Woodpecker admin endpoint. See the
[raw etcd walkthrough](../../docs/wp-etcd-cli.md).

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
- `wp profile <node> --type trace --seconds 10` — download a Go execution trace

CPU profiles use `/debug/pprof/profile`; `--seconds` controls CPU/trace collection.
Profiles default to `.pb.gz` and traces to `.trace`. Analyze the saved file with
the command printed by the CLI, for example:

```bash
go tool pprof -http=127.0.0.1:8081 ./node-0-cpu-TIMESTAMP.pb.gz
go tool trace -http=127.0.0.1:8081 ./node-0-trace-TIMESTAMP.trace
```

### Log metadata
- `wp log readers <logName>` — where each of a log's readers has read to (etcd only, no node contact)
- `wp log truncate <logName> --segment N --entry M` — preview a truncation-frontier
  advance; apply requires `--apply --expected-revision --reason --backup`.
  The target must be a non-empty Completed or Sealed segment. The frontier
  segment is retained; earlier finalized segments become eligible for auditor
  truncation and retention/reader-protected cleanup. See the
  [truncation walkthrough](../../docs/wp-log-truncate-cli.md).

### Authoritative metadata
- `wp meta log list|show`, `wp meta segment list|show`, `wp meta quorum list|show` — decoded etcd records, including unloaded historical segments, with keys, revisions and leases
- `wp meta check <logName>` — validate metadata constraints and truncation bounds
- `wp meta export --output-file <new-file>` — readable namespace snapshot with original bytes retained
- `wp meta log patch <logName> --patch-file <json>` — preview a restricted log policy correction
- `wp meta segment patch <logName> <segmentId> --patch-file <json>` — preview a correction to size, lifecycle timestamps, lastEntryId or state

These commands accept `--etcd <endpoints>` and `--meta-prefix <complete-prefix>`.
Inspection and checks cover metadata; they do not verify replica or object-storage
contents. Patch defaults to preview. Applying requires `--apply --offline`, the
target's `--expected-revision`, a `--reason` and a new `--backup` file, and refuses
writer locks or concurrent target/log updates. Quorums, identities and log truncation
positions are protected. See the [metadata command guide](../../docs/wp-meta-cli.md)
for connection options, patch formats, output interpretation and executable examples.

### Log end to end
- `wp log scan <logName> [--mode quick|raw] [--from-segment N] [--to-segment N]` — sweep every
  segment of a log and reconcile metadata with the replicas' local coverage. `quick`
  (default) reads structure only; `raw` verifies local blocks with codec and CRC checks.
  Reports metadata state/lastEntryId, observed coverage, and per-replica ranges, survey
  stop reasons and request errors. Neither mode verifies pending writes, compacted objects
  in shared storage, or end-to-end application reads; use `segment probe` for actual reads.

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

`log scan`, `segment inspect` and `segment probe` accept an optional paired
`--bucket` / `--root-path` to select the data storage instance. These are distinct
from `--meta-prefix`, which selects coordinator metadata. Explicit location is
useful for empty or unloaded segments whose instance cannot be discovered:

```bash
wp log scan my-channel --mode raw \
  --bucket milvus-bucket --root-path milvus/wp \
  --etcd 127.0.0.1:32379 --meta-prefix milvus-prod/woodpecker
wp segment probe my-channel 7 --from-entry 50 --max-entries 100 \
  --bucket milvus-bucket --root-path milvus/wp \
  --etcd 127.0.0.1:32379 --meta-prefix milvus-prod/woodpecker
```

Omitting both location flags retains node-side discovery. Providing just one is
a usage error. A 404 or unavailable node establishes no data coverage; the scan
reports the per-replica reason rather than treating it as empty. `local_empty`
confirms no local durable entries, not absence of pending writes. An Active
segment has no final boundary; confirmed local emptiness alone is not a permanent
read stall. Failed raw block checksums must not be labelled as local emptiness.
Sealed/compacted local copies are not authoritative; scan does not verify their
shared object contents, while probe can actually read them.

### Logstore runtime
- `wp logstore segments <node>` — list active segments
- `wp logstore segment-show <node> --log X --seg Y` — details of a loaded node-local writer
- `wp logstore buffer <node>` — buffer bytes summary
- `wp logstore flush-queue <node>` — flush queue depth summary
- `wp logstore force-flush <node>` — force sync
- `wp logstore lac <logName> <segmentId>` — quorum view of how far a segment is confirmed readable
- `wp logstore fence <node> --log X --seg Y --reason "..." -y` — fence one node; on a multi-node quorum this alone does not stop a write
- `wp logstore fence-quorum <logName> <segmentId> --reason "..." -y` — fence enough of the quorum (wq-aq+1 nodes) to interrupt a write
- `wp logstore compact <node> --log X --seg Y` — force compaction

The writer registry is not a persisted-segment inventory. An unloaded writer
returns TargetNotFound; its historical data may still exist. Use `log scan` or
`segment inspect` for historical segments.

### Metrics analysis
- `wp metrics list <node>` — list all metric series
- `wp metrics snapshot [<node>|--all]` — point-in-time snapshot
- `wp metrics top --by <metric>` — cross-node top-N
- `wp metrics watch <metric> <node>` — real-time trend stream
- `wp metrics report --scenario <name>` — scenario-based analysis (12 built-in)

`stuck-read` and `stuck-write` compare relevant completion counters and the
in-flight operation registry over the sampled window. A finding requires the
same matching operation to remain pending with no successful completions for
that log and operation family. An idle node is not a stall; read EOF counts as
progress. Failed evidence collection returns an error. Findings retain their
severity exit code in text, JSON and YAML output.

This is a node-local heuristic: registry entries are bounded and do not describe
every client-side wait. Successful operations on the same log can mask an
individual slow operation; inspect `ops list/show` and application checkpoints
when investigating that case.

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

## Shell development checks

```bash
go test -race ./cmd/wpcli/...
make wpcli
# Linux/macOS terminal behavior, using isolated local HTTP fixtures:
python3 cmd/wpcli/tests/shell_terminal_test.py --binary ./bin/wp
```
