# Codec - Format Implementation

## Overview

Codec is a streaming storage format designed for efficient data storage and retrieval across different backends (local
filesystem, S3, MinIO). It implements a block-based architecture with sparse indexing and block metadata for optimal
performance and recovery.

## Key Features

- **Multi-backend Support**: Works with local filesystem, S3, and MinIO
- **Block-based Architecture**: Organizes data in 2MB blocks for efficient access
- **Block Metadata**: Each block ends with metadata for efficient recovery
- **Sparse Indexing**: One index record per block instead of per record
- **Multipart Upload**: For S3/MinIO, each block becomes a separate object
- **Streaming Support**: Supports both streaming writes and reads
- **CRC32 Integrity**: Every record includes CRC32 checksum for data integrity

## Architecture

### Logical File Structure

```
[HeaderRecord] [DataRecord...] [BlockLastRecord] [DataRecord...] [BlockLastRecord] ... [IndexRecord...] [FooterRecord]
```

### Physical Storage

- **Local FS**: Single file containing all data
- **S3/MinIO**: Multi objects where each 2MB block is a separate part

### Record Format

```
[CRC32:4][Type:1][Length:4][Payload:variable]
```

## Record Types

1. **HeaderRecord** (Type 1): File metadata and version info
2. **DataRecord** (Type 2): Actual data payload
3. **IndexRecord** (Type 3): Block-level index information
4. **FooterRecord** (Type 4): File summary and index location
5. **BlockHeaderRecord** (Type 5): Block metadata at the start of each block

## Block Management

- Data is organized in 2MB blocks
- When a record would exceed the 2MB boundary, a new block is started
- Each block ends with a **BlockLastRecord** containing:
    - First and last entry IDs in the block
    - Block number can be inferred from the block sequence (0, 1, 2, ...)
    - Start offset can be calculated as: BlockNumber * 2MB (for traditional storage)
    - Block length can be calculated from the BlockLastRecord position

## Recovery Optimization

### Object Storage (S3/MinIO)

1. **Efficient Block Recovery**: Read only the last few bytes of each object to get BlockLastRecord
2. **No Cross-Object Reads**: Each object is self-contained with its metadata
3. **Parallel Recovery**: Can recover multiple blocks concurrently
4. **Minimal Data Transfer**: Only read metadata, not entire blocks

### Traditional Storage

1. **Footer First**: Read footer from end of file to get metadata
2. **Block Indexing**: Use sparse indexes to locate approximate data position
3. **Sequential Access**: Blocks are accessed sequentially within the file

## Query Optimization

1. **Footer First**: Read footer from end of file to get metadata
2. **Block Metadata**: Use BlockHeaderRecord to quickly identify block boundaries and entry ranges
3. **S3/MinIO Optimization**:
    - List objects to determine number of logical file parts
    - Read last part to get footer
    - Read block metadata from object tails for efficient range queries

## Benefits

- **Efficient Range Queries**: Block-level indexing allows quick seeking
- **Fast Recovery**: Block metadata enables efficient recovery without reading entire blocks
- **Parallel Operations**: S3/MinIO enables concurrent object uploads and recovery
- **Memory Efficient**: Streaming design with bounded memory usage
- **Fast Metadata Access**: Footer and block metadata contain complete statistics
- **Cross-Platform**: Works consistently across different storage systems
- **Self-Contained Blocks**: Each block contains its own metadata for independent processing

## File Structure Example

```
Offset 0:     [HeaderRecord]
Offset 16:    [BlockHeaderRecord]
Offset 42:    [DataRecord 1]
Offset 50:    [DataRecord 2]
...
Offset 2MB-25: [BlockHeaderRecord]
Offset 2MB:    [DataRecord N] (starts new block)
...
Offset 4MB-25: [BlockHeaderRecord]
...
Offset 20MB:   [IndexRecord Block 0]
Offset 20MB+29:[IndexRecord Block 1]
...
Offset 21MB:  [FooterRecord]
```

## Object Storage Layout

For S3/MinIO, each 2MB block becomes a separate object:

```
Object 1: [HeaderRecord] [BlockHeaderRecord][DataRecord][DataRecord][DataRecord]
Object 2: [BlockHeaderRecord][DataRecord][DataRecord][DataRecord][DataRecord][DataRecord]
Object 3: [BlockHeaderRecord][DataRecord][DataRecord][DataRecord]
...
Object N: [IndexRecord...] [FooterRecord]
```

Recovery only needs to read the tail of each object to get BlockLastRecord.
