# Intermediate segment checkpoints

An intermediate segment checkpoint (`checkpoint.blk`) is a best-effort recovery
hint for an unfinished object-storage segment. Its typed schema is defined in
[`proto/checkpoint.proto`](../../../../proto/checkpoint.proto).
`SegmentCheckpoint` stores the format version, the contiguous block indexes,
last entry ID and total indexed object size. `CheckpointBlockIndex` describes
block number, offset, size and entry range directly; no JSON section table or
embedded WAL index/footer records are used.

## Scope and control

This is an optional recovery accelerator for the direct MinIO-compatible object
storage backend (`woodpecker.storage.type: minio` or `default`) used by embedded
MinIO mode. Local and service/staged writers do not use this checkpoint setting.
The checkpoint is a reusable hint, not a durability requirement or the source
of truth for WAL contents. Segment recovery works without one by following the
existing full scan of the contiguous block prefix.

`woodpecker.logstore.segmentSyncPolicy.checkpointThreshold` controls hint writes:

| Value | Behavior |
| --- | --- |
| Positive (default 10) | Trigger an asynchronous snapshot after this many newly committed blocks. |
| Zero or negative | Stop writing new checkpoints; recovery may still use an existing valid checkpoint. |

Smaller thresholds produce more checkpoint writes and normally leave fewer
block headers to read during recovery. Larger thresholds reduce checkpoint
write frequency but can leave a longer recovery tail. Writes are best effort,
so the threshold is not a strict upper bound on recovery work. Checkpoint write
failure does not fail successful data acknowledgments.

## Framing and integrity

```
["WPCP" (4 bytes)][protobuf payload][CRC32 IEEE of payload (4 bytes, little endian)]
```

The fixed magic identifies the format. The checksum detects payload corruption
and truncation independently of protobuf decoding. The object itself supplies
the length, so there is no offset table or custom metadata-length field.

`Parse` verifies the frame, checksum and protobuf encoding, then checks:

- Supported `format_version` (currently 1).
- Block numbers and offsets form a contiguous prefix starting at zero.
- Nonzero block sizes and valid, contiguous entry ranges.
- `total_size` and `last_entry_id` agree with the block indexes.
- Empty checkpoints explicitly carry `last_entry_id = -1`.

The validated protobuf indexes are converted to existing `codec.IndexRecord`
values in memory. Finalization still writes the existing WAL footer format.

## Schema evolution

Add compatible protobuf fields with new field numbers, retaining the same
format version. Removed field numbers and names must be reserved, never reused.
Unknown protobuf fields are retained by `Parse` and `Serialize`, including
unknown fields in individual block indexes. Converting to the in-memory WAL
indexes keeps only the fields needed for recovery; that conversion is not a
round-trip editor for future checkpoint metadata.

Bump `format_version` only for incompatible recovery semantics that older
readers cannot safely ignore. Unsupported versions cause full-scan fallback.
Generated Go and VT methods use the existing repository `make proto` pipeline.

## Recovery and compatibility

Recovery first tries the finalized `footer.blk`. For unfinished segments it
reads and validates `checkpoint.blk`, scans only subsequent blocks, and stops
at the first missing block or fence. It publishes the recovered indexes only
after the complete scan succeeds, replacing previous state on fence retries.
Fence, finalize and metadata update ordering are unchanged.

Missing, unreadable, corrupt or unsupported checkpoints fall back to the
authoritative full block scan. Earlier unpublished raw-index/footer and
JSON-section checkpoint formats also take this fallback; no legacy decoder is
retained. Existing WAL block/footer files remain compatible, and pre-checkpoint
releases continue to recover from those files without using the hint.

Fallback uses the same authoritative storage path as recovery without a
checkpoint: genuine block/storage errors are still returned. Context cancellation
stops recovery rather than starting another scan.

## Recovery benchmark

With real MinIO available, run:

```sh
go test ./tests/integration -run '^$' -bench '^BenchmarkMinioCheckpointRecovery$' -benchtime=10x -count=1 -timeout=5m
```

The benchmark writes 635 or 909 blocks with 57 KiB payloads, then recovers the
same immutable segment without a checkpoint and with a controlled prefix
checkpoint leaving 5 or 9 tail blocks. It reports median/min/max latency and
ObjectStorage StatObject/GetObject call counts. Only recovery-writer construction
is timed; seeding, Close, fence and finalize are excluded. This isolates recovery
cost rather than measuring asynchronous checkpoint lag or Milvus crash failover.
Each run cleans its own object prefix and asserts no wall-clock speedup.
