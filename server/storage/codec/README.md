# Codec - Format Implementation

## Overview

Codec is a streaming storage format designed for efficient data storage and
retrieval across different backends (local filesystem, S3, MinIO). It implements
a block-based architecture with sparse indexing and block metadata for optimal
performance and recovery.

## Key Features

- **Multi-backend Support**: Works with local filesystem, S3, and MinIO
- **Block-based Architecture**: Organizes data in blocks for efficient access
- **Block Metadata**: Each block starts with a BlockHeaderRecord for efficient recovery
- **Sparse Indexing**: One index record per block instead of per record
- **Object-per-Block**: For S3/MinIO, each block becomes a separate object
- **Streaming Support**: Supports both streaming writes and reads
- **CRC32 Integrity**: Every record includes a CRC32 checksum for data integrity
- **Checkpoint Support**: Periodic intermediate snapshots for fast writer recovery (see `checkpoint/`)

## Record Format

Every record shares the same 9-byte header:

```
[CRC32:4][Type:1][Length:4][Payload:variable]
```

## Record Types

| Type | Name                | Payload Size | Description                                |
|------|---------------------|--------------|--------------------------------------------|
| 1    | HeaderRecord        | 16 bytes     | File metadata: Version(2) + Flags(2) + FirstEntryID(8) + Magic(4) |
| 2    | DataRecord          | variable     | Actual data payload                        |
| 3    | IndexRecord         | 32 bytes     | Block-level index: BlockNumber(4) + StartOffset(8) + BlockSize(4) + FirstEntryID(8) + LastEntryID(8) |
| 4    | FooterRecord        | 36/44 bytes  | File summary: V5 (36B) or V6 with LAC (44B) |
| 5    | BlockHeaderRecord   | 28 bytes     | Block metadata: BlockNumber(4) + FirstEntryID(8) + LastEntryID(8) + BlockLength(4) + BlockCrc(4) |

### Format Versioning

- **Version 5**: Base footer format (36 bytes payload)
- **Version 6** (current): Adds LAC — Last Add Confirmed entry ID (44 bytes payload)

## Object Storage Layout

For S3/MinIO, each block is stored as a separate object. A segment's key space
looks like:

```
{segmentFileKey}/
├── 0.blk              ← block 0
├── 1.blk              ← block 1
├── 2.blk              ← block 2
├── ...
├── checkpoint.blk     ← checkpoint with extensible sections
└── footer.blk         ← final metadata (index records + footer record)
```

### Block Object Layout

Each block object starts with a BlockHeaderRecord, followed by data records.
Block 0 additionally starts with a HeaderRecord before the BlockHeaderRecord.

**Block 0:**
```
[HeaderRecord (25B)] [BlockHeaderRecord (37B)] [DataRecord] [DataRecord] ...
```

**Block N (N > 0):**
```
[BlockHeaderRecord (37B)] [DataRecord] [DataRecord] ...
```

The BlockHeaderRecord at the beginning allows efficient metadata access by
reading only the first few bytes of each object, without loading the full block.

### footer.blk

Written once during `Finalize()`. Contains all index records followed by a
footer record:

```
[IndexRecord 0 (41B)] [IndexRecord 1 (41B)] ... [IndexRecord N (41B)] [FooterRecord (53B)]
```

### checkpoint.blk

Written periodically during normal operation to snapshot the writer's progress.
Uses the self-describing checkpoint format (see `checkpoint/README.md`) which
wraps the same index+footer payload with JSON metadata and a "CKPT" magic
trailer, enabling future extensibility with additional sections (e.g.
asynchronous index building results).

Retained after `Finalize()` so that future asynchronous tasks can continue to
read or append sections to it.

## Recovery

### With Checkpoint (fast path)

1. Read `checkpoint.blk`; detect format via trailing "CKPT" magic.
2. Extract the `block_indexes` section to restore block index records.
3. Scan only blocks written after the checkpoint's max block ID.

### Without Checkpoint (full scan)

1. Sequentially stat block objects starting from `0.blk`.
2. Read the BlockHeaderRecord (first bytes) of each block to recover entry range
   and block metadata.
3. Stop when a block object does not exist (end of continuous sequence).

### From footer.blk (finalized segment)

1. Read `footer.blk` and parse the FooterRecord from the tail.
2. Decode all IndexRecords preceding the footer.

## Query Optimization

1. **Footer First**: Read `footer.blk` from the finalized segment to get full
   metadata.
2. **Block Metadata**: Read the BlockHeaderRecord at the start of each block
   object to quickly identify entry ranges without loading the full block.
3. **Sparse Index**: Use IndexRecords for block-level seeking — one entry per
   block rather than per record.
