# Checkpoint Format

The `checkpoint` package defines a self-describing, extensible binary container
used to persist intermediate writer state (e.g. `checkpoint.blk`). It enables
fast recovery by storing a snapshot of progress so that only the delta since the
last checkpoint needs to be replayed.

## Binary Layout

The format is **tail-anchored** — readers start from the last 8 bytes and work
backwards, similar to many file/container formats:

```
+---------------------------+
| Section 1 data            |
+---------------------------+
| Section 2 data            |
+---------------------------+
| ...                       |
+---------------------------+
| JSON metadata  (UTF-8)    |
+---------------------------+
| meta length    (4B LE)    |
+---------------------------+
| magic "CKPT"   (4B)       |
+---------------------------+
```

**Trailer** (last 8 bytes, fixed):

| Field       | Size    | Description                           |
|-------------|---------|---------------------------------------|
| meta length | 4 bytes | Little-endian uint32, JSON byte count |
| magic       | 4 bytes | `0x43 0x4B 0x50 0x54` ("CKPT")       |

## JSON Metadata

```json
{
  "version": 1,
  "sections": [
    { "type": 1, "offset": 0, "length": 2503 }
  ]
}
```

| Field              | Type | Description                                        |
|--------------------|------|----------------------------------------------------|
| `version`          | int  | Metadata format version (currently `1`)            |
| `sections[].type`  | byte | Section type identifier (see table below)          |
| `sections[].offset`| int  | Byte offset from the start of the file             |
| `sections[].length`| int  | Byte length of the section data                    |

## Section Types

| Value | Constant              | Description                                                        |
|-------|-----------------------|--------------------------------------------------------------------|
| 1     | `SectionBlockIndexes` | Block index records + footer record (`serializeFooterAndIndexes`)  |
| 2+    | *(reserved)*          | Future: bloom filters, column stats, WAL position, etc.            |

To add a new section type, define a new `SectionType` constant and use
`SetSection` / `GetSectionData` — no format changes are needed.

## Reading Process

1. Read the last 8 bytes; validate magic = `"CKPT"`, extract `metaLength`.
2. Read `metaLength` bytes immediately before the trailer; parse as JSON.
3. Look up the desired section by `type`; slice `data[offset : offset+length]`.
4. Parse the section payload with its type-specific logic.

## Backward Compatibility

Code that reads checkpoint files should check `IsFormat(data)` first:

- **true** — new format, use `Parse()` then `GetSectionData()`.
- **false** — legacy format (raw index records + footer, identical to
  `footer.blk`); parse the entire blob directly.

This ensures checkpoint files written before the format change remain readable.

## Usage Example

```go
// Writing
cp := checkpoint.New()
cp.SetSection(checkpoint.SectionBlockIndexes, indexAndFooterBytes)
raw := cp.Serialize()
// ... upload raw as checkpoint.blk ...

// Reading
cp, err := checkpoint.Parse(raw)
section, err := cp.GetSectionData(checkpoint.SectionBlockIndexes)
// ... decode index records + footer from section ...
```
