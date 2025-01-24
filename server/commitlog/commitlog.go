package commitlog

import (
	"io"

	"github.com/zilliztech/woodpecker/proto"
)

type CommitLog interface {
	// Append appends a LogEntry asynchronously.
	Append(entry *proto.CommitLogEntry) (int64, error)

	// Sync ensures all buffered data is written to persistent storage.
	Sync() error

	// Truncate removes all entries up to a given offset.
	Truncate(offset int64) (int64, error)

	// LastOffset returns the last appended offset.
	LastOffset() int64

	// NewReader creates a new reader to read log entries.
	NewReader(opt ReaderOpt) (Reader, error)

	// Close closes the WAL and releases all resources, including stopping periodic sync.
	Close() error

	// Purge removes all entries from the log.
	Purge() error

	// startSyncPeriodically starts a background goroutine to sync the WAL periodically.
	startSyncPeriodically() error
}

type ReaderOpt struct {
	// StartOffset is the offset to start reading from.
	StartOffset int64

	// EndOffset is the offset to stop reading at.
	EndOffset int64
}

// Reader is an interface to read log entries sequentially.
type Reader interface {
	io.Closer
	// Read returns the next entry in the log according to the Reader's direction.
	// If a forward/reverse WalReader has passed the end/beginning of the log, it returns [ErrorEntryNotFound].
	// To avoid this error, use HasNext.
	Read() (*proto.CommitLogEntry, error)
	// HasNext returns true if there is an entry to read.
	HasNext() bool
}
