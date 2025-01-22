package storage

import (
	"context"
	"io"
)

// File/Object Format Description:
// ------------------------
//| Section | Field          | Size (bytes) | Description                                                                 |
//|---------|----------------|--------------|-----------------------------------------------------------------------------|
//| Header  | Magic String   | 4            | A unique identifier string to identify the file format (e.g., "FR").        |
//|         | Version        | 4            | The version of the file format being used (e.g., version 1).                |
//| Entry   | Payload Size   | 4            | The size of the payload in bytes, indicating how much data is in the payload.|
//|         | CRC            | 4            | The CRC32 checksum of the payload to verify data integrity.                 |
//|         | Payload        | Variable     | The actual log data. The length is specified by the Payload Size field.     |
//| Footer  | Index Items    | Variable     | A series of `IndexItem` values representing metadata about the entries:     |
//|         |                |              | - High 32 bits: Offset in the file.                                         |
//|         |                |              | - Low 32 bits: Payload size.                                                |
//|         | CRC            | 4            | The CRC32 checksum of the entire index data for integrity verification.     |
//|         | Index Size     | 4            | The total size in bytes of the index (sum of sizes of all index items).     |

// Fragment interface defines Read and Write operations.
type Fragment interface {
	io.Closer
	Read(ctx context.Context, opt ReaderOpt) ([]*LogEntry, error)
	Write(ctx context.Context, data []byte) error
	Flush(ctx context.Context) error
}

// LogEntry represents a single log entry with payload and its metadata
type LogEntry struct {
	Payload     []byte
	SequenceNum uint64
	CRC         uint32
}

// Footer holds the index information (entries and CRC)
type Footer struct {
	EntryOffset []uint32
	CRC         uint32
	IndexSize   uint32
}
