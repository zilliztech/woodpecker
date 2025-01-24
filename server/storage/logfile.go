package storage

import (
	"context"
	"errors"
	"io"

	"github.com/zilliztech/woodpecker/proto"
)

// Constants for sizes
const (
	MagicString        = "woodpecker01"
	VERSION            = uint32(1)
	FileHeaderSize     = 16
	PayloadSize        = 4
	CrcSize            = 4
	EntryHeaderSize    = PayloadSize + CrcSize
	OffsetSize         = 4
	SequenceNumberSize = 4
	IndexItemSize      = 4
)

// ReaderOpt represents the options for creating a reader.
type ReaderOpt struct {
	// StartSequenceNum is the fileLastOffset to start reading from.
	StartSequenceNum uint64

	// EndSequenceNum is the fileLastOffset to stop reading at.
	EndSequenceNum uint64
}

// Reader is an interface to read log entries sequentially.
type Reader interface {
	io.Closer
	// ReadNext returns the next entry in the log according to the Reader's direction.
	ReadNext() (*proto.LogEntry, error)
	// HasNext returns true if there is an entry to read.
	HasNext() bool
}

// LogFile represents a log file interface with read and write operations.
type LogFile interface {
	// GetId returns the unique log file id.
	GetId() uint64

	// Append adds an entry to the log file synchronously.
	// Returns a future that will receive the result of the append operation.
	Append(ctx context.Context, data []byte) error

	// AppendAsync adds an entry to the log file asynchronously
	AppendAsync(ctx context.Context, data []byte) (int, <-chan int)

	// NewReader creates a reader with options for sequential reads.
	NewReader(ctx context.Context, opt ReaderOpt) (Reader, error)

	// LastOffset returns the fileLastOffset of the last entry.
	LastOffset() uint64

	// Sync ensures all buffered data is written to persistent storage.
	Sync(ctx context.Context) error

	// Closer closes the log file.
	io.Closer
}

// Define custom errors
var (
	ErrInvalidEntryId = errors.New("invalid entry id")
	ErrReadFailure    = errors.New("failed to read from log file")
	ErrWriteFailure   = errors.New("failed to write to log file")
)
