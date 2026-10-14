# Advance a log's truncation frontier

Use `wp log truncate` after verifying the application's checkpoint or accepted
retention boundary. It advances the normal log truncation frontier without
opening a writer or editing segment states. Unlike `wp meta ... patch`, this
command explicitly supports truncation fields and their associated semantics.

The command follows `LogHandle.Truncate`'s metadata-update behavior, with
additional operator guards: the target must be a non-empty Completed or Sealed
segment, its key must match `segNo`, its entry must be within `0..lastEntryId`,
and the frontier cannot move backward. These checks use metadata; they do not
read replica payloads or establish that application checkpoints are correct.

## Connection and parameters

For a Helm-installed Milvus/Woodpecker deployment, connect to its actual etcd
endpoint and use the complete metadata prefix configured by the Milvus client.
For example, forward etcd to `127.0.0.1:32379` and replace
`milvus-prod/woodpecker` and `my-channel` below with your real prefix/log name.
The example requires a source build containing this command; check
`wp log truncate --help`.

```text
wp log truncate <logName> --segment <id> --entry <id> [flags]
```

| Parameter | Use |
| --- | --- |
| `<logName>` | Exact logical log name; not its numeric log ID |
| `--segment`, `--entry` | Required nonnegative target IDs; the segment must be non-empty and finalized |
| `--apply` | Commit the advancement; omitted means preview without writes |
| `--expected-revision` | Required for apply: the log's `before.revision` from preview, not `snapshot_revision` or the target segment revision |
| `--reason` | Required nonblank operator explanation for apply; recorded in backup and audit |
| `--backup` | Required for apply: a new file whose parent directory already exists; existing files are never overwritten |
| `--etcd`, `--meta-prefix` | Reachable etcd endpoints (comma-separated) and complete metadata prefix |
| `--etcd-username`, `--etcd-password` | Optional etcd authentication credentials |
| `--etcd-cacert`, `--etcd-cert`, `--etcd-key`, `--etcd-tls-min-version` | Optional workstation-local TLS settings |
| `--context` | Select a cli.yaml context; its etcd settings can replace repeated connection flags |
| `--timeout` | Request deadline, for example `10s` |
| `-o json`, `-o yaml` | Structured output; default is JSON |

Explicit etcd flags override context settings. Supplying endpoints and the full
prefix avoids admin discovery and needs no node mapping, bucket or root path.
See the [metadata connection guide](wp-meta-cli.md#connection-and-output-options)
for authentication/TLS and discovery details.

## 1. Inspect and preview

First verify the application's durable checkpoint and inspect the target:

```bash
wp meta log show my-channel \
  --etcd 127.0.0.1:32379 --meta-prefix milvus-prod/woodpecker
wp meta segment show my-channel 7 \
  --etcd 127.0.0.1:32379 --meta-prefix milvus-prod/woodpecker
wp log truncate my-channel --segment 7 --entry 49 \
  --etcd 127.0.0.1:32379 --meta-prefix milvus-prod/woodpecker
```

Illustrative abbreviated preview, assuming the frontier was `6:99`:

```json
{
  "applied": false,
  "unchanged": false,
  "before": {
    "revision": 17099,
    "metadata": {"truncated_segment_id": "6", "truncated_entry_id": "99"}
  },
  "after": {
    "revision": 17099,
    "metadata": {"truncated_segment_id": "7", "truncated_entry_id": "49"}
  },
  "snapshot_revision": 18000,
  "target_segment_revision": 174
}
```

`applied: false` confirms no change was committed. `after` is the proposed
content, so its revision still describes the source snapshot. The actual output
also contains record identities, other decoded fields, the reason and cleanup
effect. Use **17099**, the log's modification revision, for the apply below.

## 2. Apply the verified frontier

```bash
wp log truncate my-channel --segment 7 --entry 49 \
  --etcd 127.0.0.1:32379 --meta-prefix milvus-prod/woodpecker \
  --apply --expected-revision 17099 \
  --reason 'Advance WAL retention frontier after checkpoint verification' \
  --backup ./log-before-truncate.json

wp meta log show my-channel \
  --etcd 127.0.0.1:32379 --meta-prefix milvus-prod/woodpecker
```

An apply result includes `applied: true`, `unchanged: false`, `new_revision`,
`audit_key` and `backup`. Re-running `show` confirms the current committed
frontier and revision. If the frontier already equals the requested position,
the result is `unchanged: true`, `applied: false`: no backup, audit or metadata
write is created. Apply guards and expected-revision checks still apply.

The backup contains the original log and target segment, with decoded readable
metadata plus byte-exact `value_base64`, leases and revisions. It uses mode 0600;
both file and parent directory are synced before the write transaction. The
transaction compares the log and target segment revisions, preserves unrelated
log fields, unknown protobuf fields and its lease, and atomically writes the
new frontier with the audit record.

Normal truncation can run alongside a writer, like SDK truncation, so there is
no `--offline` flag and a writer lock is allowed. Coordinate application
checkpoint/retention changes with the owning application; revision checks do
not replace that verification.

## 3. Understand cleanup and failure results

The CLI updates the log frontier and modification timestamp only. Later auditor
rounds mark **earlier finalized segments** Truncated, and cleanup reclaims data
under the existing retention and reader-protection rules. Active segments wait
for finalization. The frontier segment is retained even when its last entry is
the requested position; to release it, advance into a later non-empty finalized
segment. With no client/auditor running, the frontier is saved but cleanup waits.

Truncation can eventually permanently remove data still needed by an
application. Metadata backups cannot restore reclaimed local files or objects.
In Milvus, verify its checkpoint and retention requirements before applying.

| Finding / exit code | Meaning and next step |
| --- | --- |
| `Usage` / 2 | Missing apply guard, negative ID/revision, or entry beyond the target's lastEntryId. Correct the inputs. |
| `TargetNotFound` / 3 | The exact log or target segment is absent; verify context, prefix and IDs. |
| `StateConflict` / 4 | Backward movement, stale revision, empty/Active/Truncated target, identity mismatch or invalid existing frontier. Inspect and resolve the specific cause. |
| `log or target segment changed` / 4 | A concurrent update won the revision comparison. No frontier or audit was written by this attempt; a pre-change backup may already exist. Preview again and use a new backup filename. |
| Backup/configuration failure / 12 | An existing path, missing directory, undecodable record or persistence failure prevented the apply. No truncation transaction was committed. |
| Network failure / 1 | If failure occurred during commit, the outcome may be unknown. Re-read the log and inspect audit records before retrying. |

For example, `entry must be within 0..99` means the target metadata ends at 99;
it does not establish physical readability. A mismatch between key and `segNo`
is rejected rather than silently treated as another segment. The CLI does not
repair replicas, compact data, move readers directly or roll back deleted data.
