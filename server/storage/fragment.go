package storage

import (
	"context"
)

// Fragment interface defines Read and Write operations.
type Fragment interface {
	GetFragmentId() int64
	GetFragmentKey() string
	Flush(ctx context.Context) error
	Load(ctx context.Context) error
	GetLastEntryId() (int64, error)
	GetFirstEntryIdDirectly() int64
	GetLastEntryIdDirectly() int64
	GetLastModified() int64
	GetEntry(entryId int64) ([]byte, error)
	GetSize() int64
	Release() error
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
