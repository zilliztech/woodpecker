# Metadata inspection and controlled correction

`wp meta` reads authoritative Woodpecker metadata from etcd. Unlike
`wp logstore segments`, it includes historical segments whose node-local writer
objects have already been unloaded. Metadata inspection does not verify the
contents of replica files; use `wp log scan` and `wp segment inspect` for that.

Pass the externally reachable etcd endpoint and the complete metadata prefix.
For a Helm-installed Milvus cluster, use its configured Woodpecker metadata prefix
and an etcd address reachable from the machine running `wp`. The example assumes
etcd is forwarded to port 2379 and the prefix is `milvus-prod/woodpecker`:

```bash
wp meta log list --etcd 127.0.0.1:2379 --meta-prefix milvus-prod/woodpecker
wp meta log show my-channel --etcd 127.0.0.1:2379 --meta-prefix milvus-prod/woodpecker
wp meta segment list my-channel --etcd 127.0.0.1:2379 --meta-prefix milvus-prod/woodpecker
wp meta segment show my-channel 0 --etcd 127.0.0.1:2379 --meta-prefix milvus-prod/woodpecker
wp meta quorum list --etcd 127.0.0.1:2379 --meta-prefix milvus-prod/woodpecker
wp meta quorum show 1 --etcd 127.0.0.1:2379 --meta-prefix milvus-prod/woodpecker
```

Modern segments contain an embedded quorum. Inspect `meta segment show` to see
that quorum; separately stored quorum records may be absent or belong to legacy
logs. Use `--help` on each command for JSON output and export options.

## Connection and output options

All commands in this group accept the existing metadata connection flags:

| Option | Purpose |
| --- | --- |
| `--etcd` | Reachable etcd endpoints, separated by commas |
| `--meta-prefix` | Complete Woodpecker metadata root, including the configured namespace |
| `--etcd-username`, `--etcd-password` | Etcd authentication |
| `--etcd-cacert` | Local CA certificate file for etcd TLS |
| `--etcd-cert`, `--etcd-key` | Local client certificate and private key for mutual TLS |
| `--etcd-tls-min-version` | Minimum TLS version |
| `--timeout` | Request deadline (for example, `10s`) |
| `-o json`, `-o yaml` | Structured output; this group defaults to JSON |

Specifying both `--etcd` and `--meta-prefix` avoids admin-endpoint discovery.
Otherwise, the existing connection resolver discovers missing settings through
the selected Woodpecker admin endpoint. These operations contact etcd directly
and do not fan out to the quorum nodes.

A `show` result contains `kind`, the exact `key`, its modification `revision`,
`lease`, and decoded `metadata`. Protobuf int64 fields appear as decimal strings;
use the target's `revision` for a later patch, rather than a list/export's
`snapshot_revision`. `list` wraps the same records in a `records` array at a
single snapshot revision. An empty array means that keyspace has no records;
a missing `show` target exits with code 3.

## Export and consistency checks

`wp meta export` exports metadata for backup and review. `wp meta check` checks
metadata invariants. Neither command copies local log files or object-storage
data, proves that replica payloads are readable, nor automatically repairs a
record. Preserve a data backup separately when investigating corruption.

```bash
wp meta export --output-file ./metadata-snapshot.json \
  --etcd 127.0.0.1:2379 --meta-prefix milvus-prod/woodpecker
wp meta check my-channel \
  --etcd 127.0.0.1:2379 --meta-prefix milvus-prod/woodpecker
```

Export includes decoded `metadata` for known protobuf records (logs, segments,
quorums, readers, cleanup/compacted-notify status, version and skip ranges).
Other JSON records are shown as JSON and text records as readable `value`.
Each record also retains its original `value_base64`, lease and revision, in a new file
with mode 0600. Patch backups use the same readable record format.
Unknown binary records remain base64; malformed known records include a
`decode_error` without losing their raw bytes. Existing export files are not
overwritten or converted automatically. `check` validates the
selected log, its segments, embedded quorum structure and truncation bounds;
it does not inspect replica data or verify object-storage contents.

A clean check returns `state: "ok"`, `issues: []`, `segments_checked` and
`snapshot_revision`, with exit code 0. An invalid record returns `state: "invalid"`
and explanations in `issues`, with exit code 9. For example,
`completionTime precedes createTime` identifies inconsistent lifecycle metadata;
`truncation frontier exceeds segment LastEntryID` identifies inconsistent bounds.
A clean result establishes metadata consistency only.

## Patch a log

The initial patch interface accepts a JSON object with protobuf field names.
Only these log policy fields are writable:

| Field | Unit | Constraint |
| --- | --- | --- |
| `max_segment_roll_time_seconds` | Seconds | Nonnegative integer |
| `max_segment_roll_size_bytes` | Bytes | Nonnegative integer |
| `compaction_buffer_size_bytes` | Bytes | Nonnegative integer |
| `max_compaction_file_count` | Files | Nonnegative integer |

These are reserved log metadata fields, not a replacement for client runtime
configuration. Editing them does not configure a running client's rolling policy.
Zero is accepted so legacy/default values can be preserved or restored.

For example, save the following as `log-policy.json`:

```json
{
  "max_segment_roll_time_seconds": 120,
  "max_segment_roll_size_bytes": 268435456
}
```

Preview the correction first. Patch defaults to dry-run:

```bash
wp meta log patch my-channel \
  --patch-file log-policy.json \
  --etcd 127.0.0.1:2379 --meta-prefix milvus-prod/woodpecker
```

The preview shows the original record, proposed record, field changes and source revision.
Stop the clients that can update or cache this log's metadata before applying.
`--offline` is the operator's acknowledgement; it does not stop those clients.

```bash
wp meta log patch my-channel \
  --patch-file log-policy.json \
  --apply --offline --expected-revision <revision-from-preview> \
  --reason 'Correct log rolling policy after configuration review' \
  --backup ./log-before-patch.json \
  --etcd 127.0.0.1:2379 --meta-prefix milvus-prod/woodpecker
```

An apply result reports `applied: true`, `new_revision`, `audit_key` and `backup`.
The before/after records describe the source snapshot and proposed content;
re-run `show` to obtain the committed record. A stale revision or a writer lock
exits with code 4. A reused backup path exits with code 12. If the commit request
times out, re-read the target and audit records before deciding whether to retry.

The expected revision is the target record's etcd modification revision, not an
arbitrary cluster revision. A concurrent update causes the apply to fail rather
than overwrite the newer record. Re-read and review before retrying. Restart
affected clients so cached metadata is refreshed.

## Patch a segment

The writable segment fields are `size`, `createTime`, `completionTime`,
`sealedTime`, `lastEntryId` and `state`. Size is a nonnegative byte count. Times are Unix milliseconds;
creation must be positive, other times nonnegative, and populated lifecycle
times must follow creation/completion order and agree with the segment state.

```json
{
  "size": 1048576
}
```

```bash
wp meta segment patch my-channel 0 \
  --patch-file segment-correction.json \
  --etcd 127.0.0.1:2379 --meta-prefix milvus-prod/woodpecker
```

Apply uses the same required `--apply --offline --expected-revision --reason
--backup` options as log patch. Changing lifecycle timestamps can change cleanup
eligibility: cleanup uses these timestamps to determine retention age. A size
correction changes metadata only; it does not resize or repair any data file.

## Repair lastEntryId and state after a metadata bug

`lastEntryId` accepts an integer or decimal string >= -1. Active segments require
`-1`, meaning their completion boundary is not yet established; this does not
mean their replicas contain no data. For finalized segments, `-1` means empty.
`state` accepts `Active`, `Completed`, `Sealed`, `Truncated`, or the corresponding
numbers 0, 2, 3, 4. State 1 is reserved and rejected. The complete proposed
record must satisfy the lifecycle timestamp rules; include corrected timestamps
in the same patch if necessary. A finalized segment's lastEntryId cannot be
below the log's truncation entry when it is the frontier segment.

If a verified repair requires reverting a segment to `Active`, reset
`lastEntryId` to `-1` and clear both lifecycle timestamps in the same patch:

```json
{
  "state": "Active",
  "lastEntryId": "-1",
  "completionTime": "0",
  "sealedTime": "0"
}
```

Leaving a previous finalized tail in Active metadata would seed recovery with
that old completion boundary. The command rejects this inconsistent record.
All other constraints still apply: for example, a legacy record with
`createTime=0` must also include a verified positive `createTime` in its repair.

For example, after verifying that the segment really has entries through 99
and its compacted files are present, correct an erroneous lastEntryId and state:

```json
{
  "lastEntryId": "99",
  "state": "Sealed",
  "sealedTime": "1791806000000"
}
```

The time above is illustrative; use the verified timestamp for your segment.
Preview using the segment patch command shown above, then stop all relevant
writers/auditors and apply with the target revision, reason and a new backup.
The update preserves other fields, unknown protobuf fields and the etcd lease.
A writer lock or concurrent metadata update blocks the apply.

This is metadata correction only. Setting Sealed does not generate compacted
files, setting Completed does not recover or fence a writer, and changing
lastEntryId does not create or verify entries. Truncated can make the segment
eligible for permanent cleanup. Correct fields to match verified data and the
intended lifecycle. Advance the normal truncation frontier through the dedicated
log truncation workflow; patch does not implement truncation.

## Fields that require specialized workflows

Patch rejects identities (`logId`, `segNo`), quorum identity/topology,
log truncation positions, and log
creation/modification timestamps. It also rejects unknown fields rather than
silently ignoring them. Quorums are read-only through this command group.

Use the existing writer recovery, compaction, client log truncation, node retirement and
skip-range workflows for their respective changes. In particular, setting a
segment to `Sealed` cannot replace generating its compacted object-storage
files, and changing a truncation position is not equivalent to executing
truncation. The apply path preserves the target key's existing lease and keeps
an audit record alongside its compare-and-swap update. The local backup is a
record backup, not an automatic rollback instruction; restoring it must undergo
the same concurrency and lifecycle checks.
