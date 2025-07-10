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

	// ---------------------------------------------
	// Client Layer Error Codes
	// ---------------------------------------------

	// woodpecker_client errors
	WoodpeckerClientConnectionFailed
	WoodpeckerClientInitFailed
	WoodpeckerClientClosed
	WoodpeckerClientConfigInvalid

	// log_handle errors
	LogHandleLogAlreadyExists
	LogHandleLogNotFound
	LogHandleInvalidLogName
	LogHandleWriterLockFailed
	LogHandleInvalidState
	LogHandleTruncateFailed
	LogHandleGetTruncationPointFailed

	// segment_handle errors
	SegmentHandleSegmentFenced
	SegmentHandleSegmentClosed
	SegmentHandleSegmentRolling
	SegmentHandleSegmentStateInvalid
	SegmentHandleReadFailed
	SegmentHandleWriteFailed
	SegmentHandleCompactionFailed
	SegmentHandleRecoveryFailed

	// append_op errors
	AppendOpResultChannelClosed
	AppendOpMaxConcurrentOpsReached
	AppendOpQuorumFailed
	AppendOpTimeout

	// log_writer errors
	LogWriterClosed
	LogWriterFinalized
	LogWriterBufferFull
	LogWriterBufferEmpty
	LogWriterRecordTooLarge
	LogWriterLockLost

	// log_reader errors
	LogReaderClosed
	LogReaderEntryNotFound
	LogReaderBufferEmpty
	LogReaderInvalidEntryId
	LogReaderTempInfoError
	LogReaderReadFailed

	// ---------------------------------------------
	// Server Layer Error Codes
	// ---------------------------------------------

	// logstore errors
	LogStoreNotStarted
	LogStoreAlreadyStarted
	LogStoreShutdown
	LogStoreRegisterFailed
	LogStoreInvalidAddress

	// segment_processor errors
	SegmentProcessorNotFound
	SegmentProcessorAlreadyExists
	SegmentProcessorFenced
	SegmentProcessorClosed
	SegmentProcessorNoWriter
	SegmentProcessorWriteFailed
	SegmentProcessorReadFailed
	SegmentProcessorCompactionFailed
	SegmentProcessorRecoveryFailed
	SegmentProcessorCleanupFailed

	// file writer_impl errors (disk/object storage)
	FileWriterBufferFull
	FileWriterSyncFailed
	FileWriterFinalizeFailed
	FileWriterFenceFailed
	FileWriterRecoveryFailed
	FileWriterCloseFailed
	FileWriterLockFailed
	FileWriterNoSpace
	FileWriterCorrupted
	FileWriterInvalidRecord
	FileWriterCodecNotFound
	FileWriterUnknownRecordType
	FileWriterUnexpectedRecordType
	FileWriterInvalidMagicCode
	FileWriterCRCMismatch
	FileWriterInvalidEntryId
	FileWriterFinalized
	FileWriterAlreadyClosed

	// file reader_impl errors (disk/object storage)
	FileReaderNotFound
	FileReaderCorrupted
	FileReaderInvalidRecord
	FileReaderCodecNotFound
	FileReaderUnknownRecordType
	FileReaderUnexpectedRecordType
	FileReaderInvalidMagicCode
	FileReaderCRCMismatch
	FileReaderCloseFailed
	FileReaderAlreadyClosed
	FileReaderNoBlockFound

	// ---------------------------------------------
	// Metadata Layer Error Codes
	// ---------------------------------------------

	// Metadata operations
	MetadataInitError
	MetadataReadError
	MetadataWriteError
	MetadataEncodeError
	MetadataDecodeError
	MetadataCreateLogError
	MetadataCreateLogTxnError
	MetadataCreateSegmentError
	MetadataUpdateSegmentError
	MetadataUpdateQuorumError
	MetadataCreateReaderError

	// ---------------------------------------------
	// Common/Utility Error Codes
	// ---------------------------------------------

	// General errors
	InternalError
	ConfigError
	TimeoutError
	OperationNotSupported
	InvalidConfiguration
	UnsupportedVersionError
	ChecksumError
	PersistenceError
	EmptyPayload
	EntryNotFound
	StorageNotWritable
	SegmentFenced
	SegmentNotFound
	ObjectAlreadyExists

	// ---------------------------------------------
	// Fragment-related Error Codes (still in use)
	// ---------------------------------------------

	// Fragment errors
	FragmentAlreadyExists

	// LogFile errors (still in use)
	LogFileClosed
)

var (
	// ---------------------------------------------
	// Client Layer Errors
	// ---------------------------------------------

	// woodpecker_client errors
	ErrWoodpeckerClientConnectionFailed = newWoodpeckerError("failed to create connection", WoodpeckerClientConnectionFailed, true)
	ErrWoodpeckerClientInitFailed       = newWoodpeckerError("failed to init client", WoodpeckerClientInitFailed, true)
	ErrWoodpeckerClientClosed           = newWoodpeckerError("client is closed", WoodpeckerClientClosed, false)
	ErrWoodpeckerClientConfigInvalid    = newWoodpeckerError("invalid client configuration", WoodpeckerClientConfigInvalid, false)

	// log_handle errors
	ErrLogHandleLogAlreadyExists         = newWoodpeckerError("log already exists", LogHandleLogAlreadyExists, false)
	ErrLogHandleLogNotFound              = newWoodpeckerError("log not found", LogHandleLogNotFound, false)
	ErrLogHandleInvalidLogName           = newWoodpeckerError("invalid log name", LogHandleInvalidLogName, false)
	ErrLogHandleWriterLockFailed         = newWoodpeckerError("failed to acquire writer lock", LogHandleWriterLockFailed, true)
	ErrLogHandleInvalidState             = newWoodpeckerError("log handle is in invalid state", LogHandleInvalidState, false)
	ErrLogHandleTruncateFailed           = newWoodpeckerError("failed to truncate log", LogHandleTruncateFailed, true)
	ErrLogHandleGetTruncationPointFailed = newWoodpeckerError("failed to get truncation point", LogHandleGetTruncationPointFailed, true)

	// segment_handle errors
	ErrSegmentHandleSegmentClosed       = newWoodpeckerError("segment is closed", SegmentHandleSegmentClosed, true)
	ErrSegmentHandleSegmentRolling      = newWoodpeckerError("segment is rolling", SegmentHandleSegmentRolling, true)
	ErrSegmentHandleSegmentStateInvalid = newWoodpeckerError("segment state is invalid", SegmentHandleSegmentStateInvalid, false)
	ErrSegmentHandleReadFailed          = newWoodpeckerError("failed to read segment", SegmentHandleReadFailed, true)
	ErrSegmentHandleWriteFailed         = newWoodpeckerError("failed to write segment", SegmentHandleWriteFailed, true)
	ErrSegmentHandleCompactionFailed    = newWoodpeckerError("failed to compact segment", SegmentHandleCompactionFailed, true)
	ErrSegmentHandleRecoveryFailed      = newWoodpeckerError("failed to recover segment", SegmentHandleRecoveryFailed, true)

	// append_op errors
	ErrAppendOpResultChannelClosed     = newWoodpeckerError("result channel is closed", AppendOpResultChannelClosed, false)
	ErrAppendOpMaxConcurrentOpsReached = newWoodpeckerError("maximum concurrent operations reached", AppendOpMaxConcurrentOpsReached, true)
	ErrAppendOpQuorumFailed            = newWoodpeckerError("quorum write failed", AppendOpQuorumFailed, true)
	ErrAppendOpTimeout                 = newWoodpeckerError("append operation timeout", AppendOpTimeout, true)

	// log_writer errors
	ErrLogWriterClosed         = newWoodpeckerError("log writer is closed", LogWriterClosed, false)
	ErrLogWriterFinalized      = newWoodpeckerError("log writer is finalized", LogWriterFinalized, false)
	ErrLogWriterBufferFull     = newWoodpeckerError("log writer buffer is full", LogWriterBufferFull, true)
	ErrLogWriterBufferEmpty    = newWoodpeckerError("log writer buffer is empty", LogWriterBufferEmpty, true)
	ErrLogWriterRecordTooLarge = newWoodpeckerError("record is too large", LogWriterRecordTooLarge, false)
	ErrLogWriterLockLost       = newWoodpeckerError("writer lock lost", LogWriterLockLost, false)

	// log_reader errors
	ErrLogReaderClosed         = newWoodpeckerError("log reader is closed", LogReaderClosed, false)
	ErrLogReaderBufferEmpty    = newWoodpeckerError("reader buffer is empty", LogReaderBufferEmpty, true)
	ErrLogReaderInvalidEntryId = newWoodpeckerError("invalid entry id", LogReaderInvalidEntryId, false)
	ErrLogReaderTempInfoError  = newWoodpeckerError("reader temp info error", LogReaderTempInfoError, true)
	ErrLogReaderReadFailed     = newWoodpeckerError("failed to read entry", LogReaderReadFailed, true)

	// ---------------------------------------------
	// Server Layer Errors
	// ---------------------------------------------

	// logstore errors
	ErrLogStoreNotStarted     = newWoodpeckerError("logstore not started", LogStoreNotStarted, false)
	ErrLogStoreAlreadyStarted = newWoodpeckerError("logstore already started", LogStoreAlreadyStarted, false)
	ErrLogStoreShutdown       = newWoodpeckerError("logstore is shutting down", LogStoreShutdown, false)
	ErrLogStoreRegisterFailed = newWoodpeckerError("failed to register logstore", LogStoreRegisterFailed, true)
	ErrLogStoreInvalidAddress = newWoodpeckerError("invalid logstore address", LogStoreInvalidAddress, false)

	// segment_processor errors
	ErrSegmentProcessorNotFound         = newWoodpeckerError("segment processor not found", SegmentProcessorNotFound, false)
	ErrSegmentProcessorAlreadyExists    = newWoodpeckerError("segment processor already exists", SegmentProcessorAlreadyExists, false)
	ErrSegmentProcessorFenced           = newWoodpeckerError("segment processor is fenced", SegmentProcessorFenced, false)
	ErrSegmentProcessorClosed           = newWoodpeckerError("segment processor is closed", SegmentProcessorClosed, false)
	ErrSegmentProcessorNoWriter         = newWoodpeckerError("segment processor no writer", SegmentProcessorNoWriter, false)
	ErrSegmentProcessorWriteFailed      = newWoodpeckerError("segment processor write failed", SegmentProcessorWriteFailed, true)
	ErrSegmentProcessorReadFailed       = newWoodpeckerError("segment processor read failed", SegmentProcessorReadFailed, true)
	ErrSegmentProcessorCompactionFailed = newWoodpeckerError("segment processor compaction failed", SegmentProcessorCompactionFailed, true)
	ErrSegmentProcessorRecoveryFailed   = newWoodpeckerError("segment processor recovery failed", SegmentProcessorRecoveryFailed, true)
	ErrSegmentProcessorCleanupFailed    = newWoodpeckerError("segment processor cleanup failed", SegmentProcessorCleanupFailed, true)

	// file writer errors (disk/object storage)
	ErrFileWriterBufferFull           = newWoodpeckerError("writer buffer is full", FileWriterBufferFull, true)
	ErrFileWriterSyncFailed           = newWoodpeckerError("writer sync failed", FileWriterSyncFailed, true)
	ErrFileWriterFinalizeFailed       = newWoodpeckerError("writer finalize failed", FileWriterFinalizeFailed, true)
	ErrFileWriterFenceFailed          = newWoodpeckerError("writer fence failed", FileWriterFenceFailed, true)
	ErrFileWriterRecoveryFailed       = newWoodpeckerError("writer recovery failed", FileWriterRecoveryFailed, true)
	ErrFileWriterCloseFailed          = newWoodpeckerError("writer close failed", FileWriterCloseFailed, true)
	ErrFileWriterLockFailed           = newWoodpeckerError("writer lock failed", FileWriterLockFailed, true)
	ErrFileWriterNoSpace              = newWoodpeckerError("writer no space available", FileWriterNoSpace, false)
	ErrFileWriterCorrupted            = newWoodpeckerError("writer data corrupted", FileWriterCorrupted, false)
	ErrFileWriterInvalidRecord        = newWoodpeckerError("writer invalid record size", FileWriterInvalidRecord, false)
	ErrFileWriterCodecNotFound        = newWoodpeckerError("writer codec not found", FileWriterCodecNotFound, false)
	ErrFileWriterUnknownRecordType    = newWoodpeckerError("writer unknown record type", FileWriterUnknownRecordType, false)
	ErrFileWriterUnexpectedRecordType = newWoodpeckerError("writer unexpected record type", FileWriterUnexpectedRecordType, false)
	ErrFileWriterInvalidMagicCode     = newWoodpeckerError("writer invalid magic code", FileWriterInvalidMagicCode, false)
	ErrFileWriterCRCMismatch          = newWoodpeckerError("writer crc mismatch", FileWriterCRCMismatch, false)
	ErrFileWriterInvalidEntryId       = newWoodpeckerError("invalid entry id", FileWriterInvalidEntryId, false)
	ErrFileWriterFinalized            = newWoodpeckerError("writer finalized", FileWriterFinalized, false)
	ErrFileWriterAlreadyClosed        = newWoodpeckerError("writer already closed", FileWriterAlreadyClosed, false)

	// file reader errors (disk/object storage)
	ErrFileReaderNotFound             = newWoodpeckerError("reader file not found", FileReaderNotFound, false)
	ErrFileReaderCorrupted            = newWoodpeckerError("reader data corrupted", FileReaderCorrupted, false)
	ErrFileReaderInvalidRecord        = newWoodpeckerError("reader invalid record size", FileReaderInvalidRecord, false)
	ErrFileReaderCodecNotFound        = newWoodpeckerError("reader codec not found", FileReaderCodecNotFound, false)
	ErrFileReaderUnknownRecordType    = newWoodpeckerError("reader unknown record type", FileReaderUnknownRecordType, false)
	ErrFileReaderUnexpectedRecordType = newWoodpeckerError("reader unexpected record type", FileReaderUnexpectedRecordType, false)
	ErrFileReaderInvalidMagicCode     = newWoodpeckerError("reader invalid magic code", FileReaderInvalidMagicCode, false)
	ErrFileReaderCRCMismatch          = newWoodpeckerError("reader crc mismatch", FileReaderCRCMismatch, false)
	ErrFileReaderCloseFailed          = newWoodpeckerError("reader close failed", FileReaderCloseFailed, true)
	ErrFileReaderNoBlockFound         = newWoodpeckerError("reader no block found", FileReaderNoBlockFound, false)
	ErrFileReaderAlreadyClosed        = newWoodpeckerError("reader already closed", FileReaderAlreadyClosed, false)

	// ---------------------------------------------
	// Metadata Layer Errors
	// ---------------------------------------------

	// Metadata operations
	ErrMetadataInit          = newWoodpeckerError("failed to initialize service metadata", MetadataInitError, true)
	ErrMetadataRead          = newWoodpeckerError("failed to read metadata", MetadataReadError, true)
	ErrMetadataWrite         = newWoodpeckerError("failed to write metadata", MetadataWriteError, true)
	ErrMetadataEncode        = newWoodpeckerError("failed to encode metadata", MetadataEncodeError, false)
	ErrMetadataDecode        = newWoodpeckerError("failed to decode metadata", MetadataDecodeError, false)
	ErrMetadataCreateLog     = newWoodpeckerError("failed to create log metadata", MetadataCreateLogError, true)
	ErrMetadataCreateLogTxn  = newWoodpeckerError("failed execute create log metadata txn", MetadataCreateLogTxnError, true)
	ErrMetadataCreateSegment = newWoodpeckerError("failed to create segment metadata", MetadataCreateSegmentError, true)
	ErrMetadataUpdateSegment = newWoodpeckerError("failed to update segment metadata", MetadataUpdateSegmentError, true)
	ErrMetadataUpdateQuorum  = newWoodpeckerError("failed to update quorum metadata", MetadataUpdateQuorumError, true)
	ErrMetadataCreateReader  = newWoodpeckerError("failed to create reader temp info", MetadataCreateReaderError, true)

	// ---------------------------------------------
	// Common/Utility Errors
	// ---------------------------------------------

	// General errors
	ErrInternalError           = newWoodpeckerError("internal error", InternalError, true)
	ErrConfigError             = newWoodpeckerError("config error", ConfigError, false)
	ErrTimeoutError            = newWoodpeckerError("operation timeout", TimeoutError, true)
	ErrOperationNotSupported   = newWoodpeckerError("operation not supported", OperationNotSupported, false)
	ErrInvalidConfiguration    = newWoodpeckerError("invalid configuration", InvalidConfiguration, false)
	ErrUnsupportedVersionError = newWoodpeckerError("unsupported version", UnsupportedVersionError, false)
	ErrChecksumError           = newWoodpeckerError("checksum error", ChecksumError, false)
	ErrPersistenceError        = newWoodpeckerError("persistence error", PersistenceError, true)
	ErrStorageNotWritable      = newWoodpeckerError("storage not writable", StorageNotWritable, false)

	// Data state errors
	ErrEmptyPayload        = newWoodpeckerError("empty payload", EmptyPayload, false)
	ErrEntryNotFound       = newWoodpeckerError("entry not found", EntryNotFound, true)
	ErrSegmentFenced       = newWoodpeckerError("segment fenced", SegmentFenced, false)
	ErrSegmentNotFound     = newWoodpeckerError("segment not found", SegmentNotFound, true)
	ErrObjectAlreadyExists = newWoodpeckerError("object already exists", ObjectAlreadyExists, false)
)

// woodpeckerError is a custom error type that provides richer error information.
type woodpeckerError struct {
	msg       string // msg stores the detailed error message, describing the specific situation of the error.
	errCode   int32  // errCode is an integer error code used to identify specific types of errors.
	retryable bool   // retryable indicates whether the error can be retried. Certain operations can decide whether to retry based on this flag when encountering an error.
	cause     error  // cause stores the underlying error that caused this error, supporting error chaining
}

func newWoodpeckerError(msg string, code int32, retryable bool) woodpeckerError {
	err := woodpeckerError{
		msg:       msg,
		errCode:   code,
		retryable: retryable,
		cause:     nil,
	}
	return err
}

func (e woodpeckerError) Code() int32 {
	return e.errCode
}

func (e woodpeckerError) Error() string {
	if e.cause != nil {
		return e.msg + ": " + e.cause.Error()
	}
	return e.msg
}

func (e woodpeckerError) IsRetryable() bool {
	return e.retryable
}

// Unwrap returns the underlying error, supporting Go 1.13+ error chaining
func (e woodpeckerError) Unwrap() error {
	return e.cause
}

func (e woodpeckerError) Is(err error) bool {
	// First check if the error codes match
	if target, ok := err.(woodpeckerError); ok {
		return e.errCode == target.errCode
	}
	// cause is nil, so no underlying error to check
	if e.cause == nil {
		return false
	}
	// Use the standard library's Is function for error chain traversal
	return errors.Is(e.cause, err)
}

// As supports Go 1.13+ error unwrapping for type assertions
func (e woodpeckerError) As(target interface{}) bool {
	if target, ok := target.(*woodpeckerError); ok {
		*target = e
		return true
	}
	return errors.As(e.cause, target)
}

func (e woodpeckerError) WithCauseErr(cause error) error {
	return woodpeckerError{
		msg:       e.msg,
		errCode:   e.errCode,
		retryable: e.retryable,
		cause:     cause,
	}
}

func (e woodpeckerError) WithCauseErrMsg(msg string) error {
	return woodpeckerError{
		msg:       msg,
		errCode:   e.errCode,
		retryable: e.retryable,
		cause:     nil, // No underlying error for message-only wrapping
	}
}

// WithContext creates a new error with additional context while preserving the error chain
func (e woodpeckerError) WithContext(context string) error {
	return woodpeckerError{
		msg:       context + ": " + e.msg,
		errCode:   e.errCode,
		retryable: e.retryable,
		cause:     e.cause,
	}
}

func IsRetryableErr(err error) bool {
	var wpErr woodpeckerError
	if errors.As(err, &wpErr) {
		return wpErr.retryable
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
