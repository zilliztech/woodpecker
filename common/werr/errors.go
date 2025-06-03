// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package werr

import (
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
)

const (
	// Ok means no errors
	Ok = iota
	// UnknownError means unknown error happened on broker
	UnknownError
	// InternalError means internal error
	InternalError
	// InvalidConfiguration means invalid configuration
	InvalidConfiguration
	// TimeoutError means operation timed out
	TimeoutError
	// ConnectError means failed to connect to broker
	ConnectError
	// ClientInitError means failed to initialize client
	ClientInitError
	// ClientClosedError means client is closed
	ClientClosedError
	// ReadError means failed to read from socket
	ReadError
	// MetadataInitError failed to initialize service metadata
	MetadataInitError
	// MetadataEncodeError failed in encode metadata
	MetadataEncodeError
	// MetadataDecodeError failed in decode metadata
	MetadataDecodeError
	// MetadataReadError failed in decode metadata
	MetadataReadError
	// MetadataWriteError failed in write metadata
	MetadataWriteError
	// MetadataCreateLogError failed in create log metadata
	MetadataCreateLogError
	// MetadataCreateLogTxnError failed execute create log metadata txn
	MetadataCreateLogTxnError
	// MetadataCreateSegmentError failed in create segment metadata
	MetadataCreateSegmentError
	// MetadataUpdateSegmentError failed in update segment metadata
	MetadataUpdateSegmentError
	// MetadataUpdateQuorumError failed in update quorum metadata
	MetadataUpdateQuorumError
	// MetadataCreateReaderError failed in create reader temp info
	MetadataCreateReaderError
	// LogAlreadyExists means log already exists
	LogAlreadyExists
	// MetadataSegmentNotFound means segment not found
	MetadataSegmentNotFound
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
	// MemoryBufferIsEmpty buffer is empty
	MemoryBufferIsEmpty
	// SegmentFenced When a segment asks and fail to get exclusive writer access,
	// or loses the exclusive status after a reconnection, the segmentHandle will
	// use this error to indicate that this segment is now permanently
	// fenced.
	SegmentFenced
	// SegmentStateInvalid indicates that the segment is in an invalid state for current Operation.
	SegmentStateInvalid
	// SegmentClosed indicates that the segment is closed.
	SegmentClosed
	// SegmentReadException indicates that an exception occurred while reading a segment.
	SegmentReadException
	// SegmentWriteException indicates that an exception occurred while writing a segment.
	SegmentWriteException
	// SegmentWriteError indicates that an error occurred while writing a segment.
	SegmentWriteError
	// LogFileClosed indicates that the log file is closed.
	LogFileClosed
	// FragmentEmpty indicates that the fragment is empty.
	FragmentEmpty
	// FragmentNotFound indicates that the fragment is not found.
	FragmentNotFound
	// FragmentInfoNotFetched indicates that the fragment info is not fetched.
	FragmentInfoNotFetched
	// FragmentNotLoaded indicates that the fragment is not loaded.
	FragmentNotLoaded
	// FragmentNotUploaded indicates that the fragment is not uploaded.
	FragmentNotUploaded
	// MaxConcurrentOperationsReached indicates that the maximum number of concurrent operations
	// has been reached. This means that no additional operations can be started until some
	// of the current operations complete.
	MaxConcurrentOperationsReached
	// ConfigError indicates that an error occurred while reading the configuration.
	ConfigError
	// DiskFragmentNoSpace indicates that the disk fragment is full.
	DiskFragmentNoSpace
	// TruncateLogError indicates an error occurred during log truncation
	TruncateLogError
	// GetTruncationPointError indicates an error occurred when retrieving truncation point
	GetTruncationPointError
	// WriterLockLost indicates the writer has lost its exclusive lock
	WriterLockLost
)

var (
	// Metadata related
	ErrMetadataInit             = newWoodpeckerError("failed to initialize service metadata", MetadataInitError, true)
	ErrMetadataRead             = newWoodpeckerError("failed to read metadata", MetadataReadError, true)
	ErrMetadataWrite            = newWoodpeckerError("failed to write metadata", MetadataWriteError, true)
	ErrMetadataEncode           = newWoodpeckerError("failed to encode metadata", MetadataEncodeError, false)
	ErrMetadataDecode           = newWoodpeckerError("failed to decode metadata", MetadataDecodeError, false)
	ErrCreateLogMetadata        = newWoodpeckerError("failed to create log metadata", MetadataCreateLogError, true)
	ErrCreateLogMetadataTxn     = newWoodpeckerError("failed execute create log metadata txn", MetadataCreateLogTxnError, true)
	ErrCreateSegmentMetadata    = newWoodpeckerError("failed to create segment metadata", MetadataCreateSegmentError, true)
	ErrUpdateSegmentMetadata    = newWoodpeckerError("failed to update segment metadata", MetadataUpdateSegmentError, true)
	ErrUpdateQuorumInfoMetadata = newWoodpeckerError("failed to update quorum metadata", MetadataUpdateQuorumError, true)

	// Client related
	ErrCreateConnection = newWoodpeckerError("failed to create connection", ConnectError, true)
	ErrInitClient       = newWoodpeckerError("failed to init client", ClientInitError, true)
	ErrClientClosed     = newWoodpeckerError("Client is closed", ClientClosedError, false)

	// log&segment related
	ErrLogAlreadyExists      = newWoodpeckerError("Log already exists", LogAlreadyExists, false)
	ErrSegmentNotFound       = newWoodpeckerError("Segment not found", MetadataSegmentNotFound, false)
	ErrSegmentReadException  = newWoodpeckerError("failed to read segment", SegmentReadException, true)
	ErrSegmentWriteException = newWoodpeckerError("failed to write segment", SegmentWriteException, true)
	ErrSegmentWriteError     = newWoodpeckerError("failed to write segment error", SegmentWriteError, false)
	ErrSegmentClosed         = newWoodpeckerError("Segment is closed", SegmentClosed, true)
	ErrSegmentFenced         = newWoodpeckerError("Segment is fenced", SegmentFenced, false)
	ErrSegmentStateInvalid   = newWoodpeckerError("Segment state is invalid", SegmentStateInvalid, false)

	// LogFile & Fragment related
	ErrLogFileClosed          = newWoodpeckerError("LogFile is closed", LogFileClosed, true)
	ErrFragmentEmpty          = newWoodpeckerError("Fragment is empty", FragmentEmpty, false)
	ErrFragmentNotFound       = newWoodpeckerError("Fragment is not found", FragmentNotFound, false)
	ErrFragmentInfoNotFetched = newWoodpeckerError("Fragment info is not fetched", FragmentInfoNotFetched, false)
	ErrFragmentNotLoaded      = newWoodpeckerError("Fragment is not loaded", FragmentNotLoaded, false)
	ErrFragmentNotUploaded    = newWoodpeckerError("Fragment is not uploaded", FragmentNotUploaded, false)

	// Reader&Writer related
	ErrInvalidEntryId      = newWoodpeckerError("Invalid Entry Id", InvalidEntryId, false)
	ErrBufferIsEmpty       = newWoodpeckerError("Buffer is empty", MemoryBufferIsEmpty, true)
	ErrEntryNotFound       = newWoodpeckerError("Entry is not found", EntryNotFound, false)
	ErrNotSupport          = newWoodpeckerError("Operation not supported", OperationNotSupported, false)
	ErrInternalError       = newWoodpeckerError("internal error", InternalError, true)
	ErrConfigError         = newWoodpeckerError("config error", ConfigError, false)
	ErrReaderTempInfoError = newWoodpeckerError("reader temp info error", MetadataCreateReaderError, true)
	ErrWriterLockLost      = newWoodpeckerError("writer lock has been lost", WriterLockLost, false)

	// Storage Related
	ErrDiskFragmentNoSpace = newWoodpeckerError("disk fragment no space", DiskFragmentNoSpace, false)

	// Truncation related
	ErrTruncateLog        = newWoodpeckerError("failed to truncate log", TruncateLogError, true)
	ErrGetTruncationPoint = newWoodpeckerError("failed to get truncation point", GetTruncationPointError, true)
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

func (e woodpeckerError) WithCauseErr(cause error) error {
	return e.WithCauseErrMsg(cause.Error())
}

func (e woodpeckerError) WithCauseErrMsg(msg string) error {
	return woodpeckerError{
		msg:       msg,
		errCode:   e.errCode,
		retryable: e.retryable,
	}
}

func IsRetryableErr(err error) bool {
	if err, ok := err.(woodpeckerError); ok {
		return err.retryable
	}
	return false
}

type multiErrors struct {
	errs []error
}

func (e *multiErrors) Unwrap() error {
	if len(e.errs) <= 1 {
		return nil
	}
	// To make merr work for multi errors,
	// we need cause of multi errors, which defined as the last error
	if len(e.errs) == 2 {
		return e.errs[1]
	}

	return &multiErrors{
		errs: e.errs[1:],
	}
}

func (e *multiErrors) Error() string {
	final := e.errs[0]
	for i := 1; i < len(e.errs); i++ {
		final = errors.Wrap(e.errs[i], final.Error())
	}
	return final.Error()
}

func (e *multiErrors) Is(err error) bool {
	for _, item := range e.errs {
		if errors.Is(item, err) {
			return true
		}
	}
	return false
}

func Combine(errs ...error) error {
	errs = lo.Filter(errs, func(err error, _ int) bool { return err != nil })
	if len(errs) == 0 {
		return nil
	}
	return &multiErrors{
		errs,
	}
}
