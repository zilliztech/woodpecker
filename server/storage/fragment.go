package storage

import (
	"context"
)

// Fragment interface defines Read and Write operations.
//
//go:generate mockery --dir=./server/storage --name=Fragment --structname=Fragment --output=mocks/mocks_server/mocks_storage --filename=mock_fragment.go --with-expecter=true  --outpkg=mocks_storage
type Fragment interface {
	GetLogId() int64
	GetSegmentId() int64
	GetFragmentId() int64
	GetFragmentKey() string
	Flush(ctx context.Context) error
	Load(ctx context.Context) error
	GetLastEntryId() (int64, error)
	GetFirstEntryId() (int64, error)
	GetLastModified() int64
	GetEntry(entryId int64) ([]byte, error)
	GetSize() int64
	GetRawBufSize() int64
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
