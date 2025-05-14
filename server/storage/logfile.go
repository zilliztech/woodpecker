package storage

import (
	"context"
	"io"

	"github.com/zilliztech/woodpecker/proto"
)

// ReaderOpt represents the options for creating a reader. which read entries in [start,end).
type ReaderOpt struct {
	// StartSequenceNum is the fileLastOffset to start reading from.
	StartSequenceNum int64

	// EndSequenceNum is the fileLastOffset to stop reading at.
	EndSequenceNum int64
}

// Reader is an interface to read log entries sequentially.
type Reader interface {
	io.Closer
	// ReadNext returns the next entry in the log according to the Reader's direction.
	ReadNext() (*proto.LogEntry, error)
	// HasNext returns true if there is an entry to read.
	HasNext() (bool, error)
}

// LogFile represents a log file interface with read and write operations.
//
//go:generate mockery --dir=./server/storage --name=LogFile --structname=LogFile --output=mocks/mocks_server/mocks_storage --filename=mock_logfile.go --with-expecter=true  --outpkg=mocks_storage
type LogFile interface {
	// GetId returns the unique log file id.
	GetId() int64
	// Append adds an entry to the log file synchronously.
	// Returns a future that will receive the result of the append operation.
	// Deprecated: Use AppendAsync instead, entryID is pass by client segmentHandle
	Append(ctx context.Context, data []byte) error
	// AppendAsync adds an entry to the log file asynchronously
	AppendAsync(ctx context.Context, entryId int64, data []byte) (int64, <-chan int64, error)
	// NewReader creates a reader with options for sequential reads.
	NewReader(ctx context.Context, opt ReaderOpt) (Reader, error)
	// LastFragmentId returns the last fragment id of this logFile.
	LastFragmentId() uint64
	// GetLastEntryId returns the last entry id of this logFile.
	GetLastEntryId() (int64, error)
	// Sync ensures all buffered data is written to persistent storage.
	Sync(ctx context.Context) error
	// Merge the log file fragments.
	Merge(ctx context.Context) ([]Fragment, []int32, []int32, error)
	// Load the segment log file fragments info
	Load(ctx context.Context) (int64, Fragment, error)
	// DeleteFragments delete the segment log file fragments.
	DeleteFragments(ctx context.Context, flag int) error
	// Closer closes the log file.
	io.Closer
}
