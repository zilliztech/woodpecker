package err

import "github.com/cockroachdb/errors"

const (
	// Ok means no errors
	Ok = iota
	// UnknownError means unknown error happened on broker
	UnknownError
	// InvalidConfiguration means invalid configuration
	InvalidConfiguration
	// TimeoutError means operation timed out
	TimeoutError
	// ConnectError means failed to connect to broker
	ConnectError
	// ReadError means failed to read from socket
	ReadError
	// MetadataError failed in updating metadata
	MetadataError
	// PersistenceError failed to persist entry
	PersistenceError
	// ChecksumError corrupt message checksum failure
	ChecksumError
	// NotConnectedError producer/consumer is not currently connected to broker
	NotConnectedError
	// AlreadyClosedError producer/consumer is already closed and not accepting any operation
	AlreadyClosedError
	// InvalidEntryId entry id is invalid
	InvalidEntryId
	// ReaderNotInitialized reader is not initialized
	ReaderNotInitialized
	// WriterNotInitialized writer is not initialized
	WriterNotInitialized
	// TooManyAppendOpsException too many concurrent AppendOps
	TooManyAppendOpsException
	// InvalidLogName means invalid Log name
	InvalidLogName
	// InvalidURL means Client Initialized with Invalid LogStore Url
	InvalidURL
	// OperationNotSupported operation not supported
	OperationNotSupported
	// WriterBlockedQuotaExceededException writer is getting exception
	WriterBlockedQuotaExceededException
	// WriterQueueIsFull producer queue is full
	WriterQueueIsFull
	// MessageTooBig trying to send a messages exceeding the max size
	MessageTooBig
	// EntryNotFound entry not found
	EntryNotFound
	// UnsupportedVersionError when an older client/version doesn't support a required feature
	UnsupportedVersionError
	// ReaderClosed means reader already been closed
	ReaderClosed
	// WriterClosed means writer already been closed
	WriterClosed
	// InvalidStatus means the component status is not as expected.
	InvalidStatus
	// MemoryBufferIsFull limited buffer is full
	MemoryBufferIsFull
	// SegmentFenced When a segment asks and fail to get exclusive writer access,
	// or loses the exclusive status after a reconnection, the segmentHandle will
	// use this error to indicate that this segment is now permanently
	// fenced.
	SegmentFenced
	// MaxConcurrentOperationsReached indicates that the maximum number of concurrent operations
	// has been reached. This means that no additional operations can be started until some
	// of the current operations complete.
	MaxConcurrentOperationsReached
)

var (
	ErrInvalidEntryId = newWoodpeckerError("Invalid EntryId", InvalidEntryId, false)
	ErrBufferIsEmpty  = newWoodpeckerError("Buffer is empty", MemoryBufferIsFull, true)
	ErrEntryNotFound  = newWoodpeckerError("Entry is not found", EntryNotFound, false)
)

// woodpeckerError is a custom error type that provides richer error information.
type woodpeckerError struct {
	msg       string // msg stores the detailed error message, describing the specific situation of the error.
	errCode   int32  // errCode is an integer error code used to identify specific types of errors.
	retryable bool   // retryable indicates whether the error can be retried. Certain operations can decide whether to retry based on this flag when encountering an error.
}

func newWoodpeckerError(msg string, code int32, retryable bool) woodpeckerError {
	err := woodpeckerError{
		msg:       msg,
		errCode:   code,
		retryable: retryable,
	}
	return err
}

func (e woodpeckerError) Code() int32 {
	return e.errCode
}

func (e woodpeckerError) Error() string {
	return e.msg
}

func (e woodpeckerError) IsRetryable() bool {
	return e.retryable
}

func (e woodpeckerError) Is(err error) bool {
	cause := errors.Cause(err)
	if cause, ok := cause.(woodpeckerError); ok {
		return e.errCode == cause.errCode
	}
	return false
}

func IsRetryableErr(err error) bool {
	if err, ok := err.(woodpeckerError); ok {
		return err.retryable
	}
	return false
}
