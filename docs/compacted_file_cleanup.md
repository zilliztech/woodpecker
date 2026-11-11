# Compacted Local File Cleanup

How woodpecker promptly reclaims a segment's local staged `data.log` once its data is
durably compacted into object storage — instead of holding it until the truncate GC's
consumer-driven, TTL-gated path (or forever, if truncation never advances). Prompt reclaim
is also what lets node decommission converge: `HasLocalSegmentData()` ignores segments
whose local data has been reclaimed.

Applies to **service (staged) storage mode**: nodes stage writes in a local
`data.log`, and compaction later uploads merged blocks (`m_N.blk`) plus a `footer.blk`
(written **last** — the durability commit point) to object storage.

## The two invariants

Everything below is built around two rules; any change must preserve both.

1. **Never delete a local `data.log` without a confirmed compacted footer.** The one
   place that deletes it requires a `StatObject` HEAD on `footer.blk`: performed immediately
   before the delete for reconcile-discovered segments, or carried from the notify handler's
   own verification seconds earlier in the same process for push-path entries (skipping the
   duplicate probe). Consequently a segment's data is never absent from both local disk and
   object storage at once — every crash/reorder state is read-safe.
2. **The tombstone is removed only by the truncate/delete GC.** After the drop, the empty
   `data.compacted` marker and the segment directory are KEPT. Deleting the tombstone any
   earlier would make a reclaimed segment indistinguishable from one that never had data
   here, degrading every cold read to an object-storage HEAD and breaking the
   "no data → fast `ErrEntryNotFound`, zero object-storage calls" gate.

## Local state encoding (per segment directory)

| state | files present | reads served from |
|---|---|---|
| live staged segment | `data.log` | local |
| compacted, drop pending | `data.log` + `data.compacted` | local |
| tombstone (reclaimed) | `data.compacted` only | object storage (no HEAD needed) |
| never had data here | none | `ErrEntryNotFound` (no object-storage call) |

`data.compacted` is an **empty** file — only its existence matters (mtime records when it
was written). Writing it is one `O_CREATE` plus one parent-dir fsync; durability matters
because the node-side discovery walk keys off `data.log`, so a mark lost after the drop
could never be re-created. A mark is also written on quorum replicas that never held the
segment's bytes: it is true information ("compacted — serve from object storage") that
lets that replica serve reads directly instead of bouncing the client to another node.

## Client / server split

```
──────────── client (the writer's auditor, one 5s cycle per log) ────────────
segment meta list (loaded every cycle anyway)
   ├─ Completed  → Compact(): compact + upload footer + set meta Sealed. Returns
   │              immediately — NO notify inside, not even a bounded wait.
   ├─ Sealed     → SegmentCompactedNotifyManager.EnsureSegmentNotified()   [push]
   └─ Truncated  → SegmentCleanupManager.CleanupSegment()  (+ reap marking records)

──────────── server (each logstore node; unchanged by client cadence) ────────────
NotifySegmentCompacted handler : footer HEAD (verify durably compacted; refuse otherwise)
                                 → write data.compacted + enqueue on the drop queue
maintenance tick (5s)          : drain queue → footer HEAD → rm data.log (keep tombstone)
reconcile walk (~5m, backstop) : re-enqueue marked dirs still holding data.log
                                 (restart recovery); age-gated footer HEAD for
                                 unmarked-but-compacted segments               [pull]
truncate/delete GC             : removes object storage data + data.log + tombstone + dir
```

Three layers close the loop:

- **push** (client): precise, fast (~seconds), with a durable per-node account;
- **pull** (server): independent self-healing for anything the push never reaches —
  crashed/recovered nodes, a log with no open writer, a lost notify;
- **manual queue** (operator): visibility for the tail case of a node that cannot be
  reached at all (see PENDING_MANUAL below).

## Mark distribution: the `marking/` record

Per-node distribution progress is durable in etcd as `SegmentCompactedNotifyStatus`
under **`root/marking/<logId>/<segId>`** — deliberately a *sibling* of the truncate GC's
`root/cleaning/<logId>/<segId>` (`SegmentCleanupStatus`), following the same pattern:
state + per-node bool map + timestamps, resumable at node granularity. The segment state
machine in `root/logs/.../segments/<segId>` is never touched by this feature.

Lifecycle within a segment's life:

```
Active → Completed ──Compact──→ Sealed ──────truncate──────→ Truncated → meta deleted
                                  │                             │
                                  └── marking/ record lives here └── cleaning/ record here
                                      (IN_PROGRESS → COMPLETED,      (truncate GC fanout)
                                       reaped at truncation)
```

- The auditor **lazily creates** the record on first seeing a Sealed segment without one
  (no crash window between Compact and record creation), initialized with every quorum
  node unacked.
- Each cycle re-sends `NotifySegmentCompacted` **only to unacked nodes** (per-node 10s
  bound); acks are persisted. A restarted writer List-seeds its in-memory cache once and
  resumes exactly where it left off — the records are the durable queue; no extra
  persistence exists.
- At most 64 segments do real distribution work per cycle (settled segments are an
  in-memory fast path), so a backlog — e.g. first cycles after upgrading a cluster with
  many pre-existing sealed segments — spreads across cycles instead of swamping one.
- **PENDING_MANUAL**: a node still unacked 30 minutes after the record's `StartTime`
  (durable, hence restart-proof) parks the record. Parked records are excluded from
  auto-retry and surfaced to operators via `wp marking list` / `wp marking confirm`
  (see the wpcli cookbook). Parking is mild by design: a data-holding node self-heals via
  the server-side pull once it returns; a node that never held the bytes only loses the
  serve-from-object-storage read optimization.
- **Reaping** mirrors `cleaning/`: the truncated-branch orphan sweep deletes marking
  records below the oldest pending segment (PENDING_MANUAL included — the need for a mark
  dies with the segment), and the whole-log delete transaction wipes the
  `marking/<logId>/` prefix. Unattended records therefore cannot accumulate forever.

## Server-side drop path

The `compacted-file-cleanup` maintenance task never walks the tree on the frequent path:

- the notify handler enqueues the segment on an in-memory drop queue; every 5s tick
  drains up to 256 segments (`maxDrainPerTick`; the remainder waits for the next tick, so a
  startup backlog cannot monopolize one pass) — the footer confirmation carried from the
  handler (or a fresh HEAD for reconcile entries), then the `data.log` delete, cached
  writer/reader eviction, and the tombstone is kept;
- a full-tree reconcile walk runs only at startup and then every 60th pass (~5m): it
  re-enqueues marked dirs still holding a `data.log` (this is also the restart recovery —
  the on-disk marker *is* the node's persisted queue) and reconciles
  unmarked-but-compacted segments, gated by `reconcileMinDataLogAge` (default 30m,
  `<=0` disables) so segments still in the write→roll→compact pipeline are not HEADed;
- a marked segment whose footer is confirmed **absent** is an anomaly (a mark is only
  ever written after the footer exists): it is logged loudly and left — `data.log` *and*
  mark — for the truncate/delete GC. The task never clears a mark (invariant 2).

## Read side

The staged reader, on local-`data.log`-miss, consults the tombstone: mark present →
serve from object storage (compacted reader; no local re-caching); mark absent →
`ErrEntryNotFound` with zero object-storage calls. The cached reader is evicted on drop
with a single transparent retry, so in-flight reads cross the handoff safely. Resume
states carry provenance (compacted vs local block numbering); a cross-provenance resume
re-resolves by entry id, and a sealed segment that has nothing at/after the requested
point returns end-of-file — never an empty batch — so consumers advance instead of
waiting forever.

## Failure matrix

| failure | recovered by |
|---|---|
| crash after Sealed, before marking record created | next auditor cycle: Sealed + no record → create (lazy creation, no window) |
| crash mid-fanout | per-node acks are durable → next writer resumes exactly, per node |
| node down during distribution | retried each cycle; after 30m → PENDING_MANUAL; if it holds data, server pull self-heals it when back |
| no writer open for the log (no push at all) | server pull reconcile converges independently (~5m walk + 30m age gate) |
| node crash after mark write, before drop | mark is fsynced; node's startup walk re-enqueues → drop proceeds |
| segment truncated while distribution in flight | auditor marks it reaped in-process (distributor skips it even on a stale snapshot); truncated-branch sweep reaps the marking record; truncate GC removes data everywhere; a straggler notify that still lands is refused by the handler footer HEAD (objects already deleted → no footer), so a reaped dir cannot be resurrected |

## rootPath expectations

`minio.rootPath` is operator-set configuration (not request input) and MUST be a clean
relative path ("files", "wp/data") or empty (bucket root). This is enforced at every entry
point: `config.Validate()` (run by `NewConfiguration` for both the config-file and the
all-defaults path) rejects a non-canonical value (leading/trailing/doubled slashes, `.`/`..`
segments), and the server (`NewServerWithConfig`) and client (`NewClient`/`NewEmbedClient`)
constructors re-validate, so a config mutated programmatically after load is caught before
anything consumes it. Every consumer — the staged writer/reader/delete-GC,
the cleanup footer HEAD, the pure object-storage writer/reader, the client's direct reader, and
the local-storage metric labels — then uses the value **verbatim**, with no normalization
anywhere on the chain; fail-fast validation at the entry points is what keeps all of those
sites trivially consistent with each other.

## Config knobs

| key | default | meaning |
|---|---|---|
| `logstore.maintenanceStrategy.compactedFileCleanupInterval` | 5s | node-side tick (queue drain) |
| `logstore.maintenanceStrategy.reconcileMinDataLogAge` | 30m | pull reconcile age gate; `<=0` disables |
| `client.auditor.maxInterval` | 5s in code, 10s in the shipped yaml | auditor cycle driving compaction, marking, truncate cleanup |

The distribution retry budget (30m → PENDING_MANUAL) and the per-cycle budget (64) are
constants (`notifyPendingManualAfter`, `maxCompactedNotifyPerCycle`).
