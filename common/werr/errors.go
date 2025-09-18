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

// Error codes are organized by categories using segmented numeric ranges:
// Each component has 100 error codes space for better extensibility
// 0: OK | Success
// 1: errors report by dependencies or other system directly
// 1000-1999: Client Layer errors
// 2000-2999: Server Layer errors
// 3000-3999: Metadata Layer errors
// 4000-7999: Reserved for future system components
// 8000-8999: Common/Utility errors
// 9000-9999: External dependency errors

var (
	// ---------------------------------------------
	// Client Layer Errors
	// ---------------------------------------------

	// woodpecker_client errors (1000-1099)
	ErrWoodpeckerClientConnectionFailed = newWoodpeckerError("failed to create connection", 1000, true)
	ErrWoodpeckerClientInitFailed       = newWoodpeckerError("failed to init client", 1001, true)
	ErrWoodpeckerClientClosed           = newWoodpeckerError("client is closed", 1002, false)
	ErrWoodpeckerClientConfigInvalid    = newWoodpeckerError("invalid client configuration", 1003, false)

	// log_handle errors (1100-1199)
	ErrLogHandleLogAlreadyExists         = newWoodpeckerError("log already exists", 1100, false)
	ErrLogHandleLogNotFound              = newWoodpeckerError("log not found", 1101, false)
	ErrLogHandleInvalidLogName           = newWoodpeckerError("invalid log name", 1102, false)
	ErrLogHandleWriterLockFailed         = newWoodpeckerError("failed to acquire writer lock", 1103, true)
	ErrLogHandleInvalidState             = newWoodpeckerError("log handle is in invalid state", 1104, false)
	ErrLogHandleTruncateFailed           = newWoodpeckerError("failed to truncate log", 1105, true)
	ErrLogHandleGetTruncationPointFailed = newWoodpeckerError("failed to get truncation point", 1106, true)
	ErrLogHandleFenceFailed              = newWoodpeckerError("failed to fence log", 1107, true)

	// segment_handle errors (1200-1299)
	ErrSegmentHandleSegmentClosed       = newWoodpeckerError("segment is closed", 1200, true)
	ErrSegmentHandleSegmentRolling      = newWoodpeckerError("segment is rolling", 1201, true)
	ErrSegmentHandleSegmentStateInvalid = newWoodpeckerError("segment state is invalid", 1202, false)
	ErrSegmentHandleReadFailed          = newWoodpeckerError("failed to read segment", 1203, true)
	ErrSegmentHandleWriteFailed         = newWoodpeckerError("failed to write segment", 1204, true)
	ErrSegmentHandleCompactionFailed    = newWoodpeckerError("failed to compact segment", 1205, true)
	ErrSegmentHandleRecoveryFailed      = newWoodpeckerError("failed to recover segment", 1206, true)

	// append_op errors (1300-1399)
	ErrAppendOpResultChannelClosed     = newWoodpeckerError("result channel is closed", 1300, false)
	ErrAppendOpMaxConcurrentOpsReached = newWoodpeckerError("maximum concurrent operations reached", 1301, true)
	ErrAppendOpQuorumFailed            = newWoodpeckerError("quorum write failed", 1302, true)
	ErrAppendOpTimeout                 = newWoodpeckerError("append operation timeout", 1303, true)

	// log_writer errors (1400-1499)
	ErrLogWriterClosed         = newWoodpeckerError("log writer is closed", 1400, false)
	ErrLogWriterFinalized      = newWoodpeckerError("log writer is finalized", 1401, false)
	ErrLogWriterBufferFull     = newWoodpeckerError("log writer buffer is full", 1402, true)
	ErrLogWriterBufferEmpty    = newWoodpeckerError("log writer buffer is empty", 1403, true)
	ErrLogWriterRecordTooLarge = newWoodpeckerError("record is too large", 1404, false)
	ErrLogWriterLockLost       = newWoodpeckerError("writer lock lost", 1405, false)

	// log_reader errors (1500-1599)
	ErrLogReaderClosed         = newWoodpeckerError("log reader is closed", 1500, false)
	ErrLogReaderBufferEmpty    = newWoodpeckerError("reader buffer is empty", 1501, true)
	ErrLogReaderInvalidEntryId = newWoodpeckerError("invalid entry id", 1502, false)
	ErrLogReaderTempInfoError  = newWoodpeckerError("reader temp info error", 1503, true)
	ErrLogReaderReadFailed     = newWoodpeckerError("failed to read entry", 1504, true)

	// ---------------------------------------------
	// Server Layer Errors
	// ---------------------------------------------

	// logstore errors (2000-2099)
	ErrLogStoreNotStarted     = newWoodpeckerError("logstore not started", 2000, false)
	ErrLogStoreAlreadyStarted = newWoodpeckerError("logstore already started", 2001, false)
	ErrLogStoreShutdown       = newWoodpeckerError("logstore is shutting down", 2002, false)
	ErrLogStoreRegisterFailed = newWoodpeckerError("failed to register logstore", 2003, true)
	ErrLogStoreInvalidAddress = newWoodpeckerError("invalid logstore address", 2004, false)

	// segment_processor errors (2100-2199)
	ErrSegmentProcessorNotFound         = newWoodpeckerError("segment processor not found", 2100, false)
	ErrSegmentProcessorAlreadyExists    = newWoodpeckerError("segment processor already exists", 2101, false)
	ErrSegmentProcessorFenced           = newWoodpeckerError("segment processor is fenced", 2102, false)
	ErrSegmentProcessorClosed           = newWoodpeckerError("segment processor is closed", 2103, false)
	ErrSegmentProcessorNoWriter         = newWoodpeckerError("segment processor no writer", 2104, false)
	ErrSegmentProcessorWriteFailed      = newWoodpeckerError("segment processor write failed", 2105, true)
	ErrSegmentProcessorReadFailed       = newWoodpeckerError("segment processor read failed", 2106, true)
	ErrSegmentProcessorCompactionFailed = newWoodpeckerError("segment processor compaction failed", 2107, true)
	ErrSegmentProcessorRecoveryFailed   = newWoodpeckerError("segment processor recovery failed", 2108, true)
	ErrSegmentProcessorCleanupFailed    = newWoodpeckerError("segment processor cleanup failed", 2109, true)

	// file writer errors (2200-2299)
	ErrFileWriterBufferFull           = newWoodpeckerError("writer buffer is full", 2200, true)
	ErrFileWriterSyncFailed           = newWoodpeckerError("writer sync failed", 2201, true)
	ErrFileWriterFinalizeFailed       = newWoodpeckerError("writer finalize failed", 2202, true)
	ErrFileWriterFenceFailed          = newWoodpeckerError("writer fence failed", 2203, true)
	ErrFileWriterRecoveryFailed       = newWoodpeckerError("writer recovery failed", 2204, true)
	ErrFileWriterCloseFailed          = newWoodpeckerError("writer close failed", 2205, true)
	ErrFileWriterLockFailed           = newWoodpeckerError("writer lock failed", 2206, true)
	ErrFileWriterNoSpace              = newWoodpeckerError("writer no space available", 2207, false)
	ErrFileWriterCorrupted            = newWoodpeckerError("writer data corrupted", 2208, false)
	ErrFileWriterInvalidRecord        = newWoodpeckerError("writer invalid record size", 2209, false)
	ErrFileWriterCodecNotFound        = newWoodpeckerError("writer codec not found", 2210, false)
	ErrFileWriterUnknownRecordType    = newWoodpeckerError("writer unknown record type", 2211, false)
	ErrFileWriterUnexpectedRecordType = newWoodpeckerError("writer unexpected record type", 2212, false)
	ErrFileWriterInvalidMagicCode     = newWoodpeckerError("writer invalid magic code", 2213, false)
	ErrFileWriterCRCMismatch          = newWoodpeckerError("writer crc mismatch", 2214, false)
	ErrFileWriterInvalidEntryId       = newWoodpeckerError("invalid entry id", 2215, false)
	ErrFileWriterFinalized            = newWoodpeckerError("writer finalized", 2216, false)
	ErrFileWriterAlreadyClosed        = newWoodpeckerError("writer already closed", 2217, false)

	// file reader errors (2300-2399)
	ErrFileReaderNotFound             = newWoodpeckerError("reader file not found", 2300, false)
	ErrFileReaderCorrupted            = newWoodpeckerError("reader data corrupted", 2301, false)
	ErrFileReaderInvalidRecord        = newWoodpeckerError("reader invalid record size", 2302, false)
	ErrFileReaderCodecNotFound        = newWoodpeckerError("reader codec not found", 2303, false)
	ErrFileReaderUnknownRecordType    = newWoodpeckerError("reader unknown record type", 2304, false)
	ErrFileReaderUnexpectedRecordType = newWoodpeckerError("reader unexpected record type", 2305, false)
	ErrFileReaderInvalidMagicCode     = newWoodpeckerError("reader invalid magic code", 2306, false)
	ErrFileReaderCRCMismatch          = newWoodpeckerError("reader crc mismatch", 2307, false)
	ErrFileReaderCloseFailed          = newWoodpeckerError("reader close failed", 2308, true)
	ErrFileReaderAlreadyClosed        = newWoodpeckerError("reader already closed", 2309, false)
	ErrFileReaderEndOfFile            = newWoodpeckerError("end of file", 2310, false)
	ErrFileReaderNoBlockFound         = newWoodpeckerError("reader no block found", 2311, false)

	// ---------------------------------------------
	// Metadata Layer Errors
	// ---------------------------------------------

	// Metadata operations (3000-3099)
	ErrMetadataInit            = newWoodpeckerError("failed to initialize service metadata", 3000, true)
	ErrMetadataRead            = newWoodpeckerError("failed to read metadata", 3001, true)
	ErrMetadataWrite           = newWoodpeckerError("failed to write metadata", 3002, true)
	ErrMetadataEncode          = newWoodpeckerError("failed to encode metadata", 3003, false)
	ErrMetadataDecode          = newWoodpeckerError("failed to decode metadata", 3004, false)
	ErrMetadataCreateLog       = newWoodpeckerError("failed to create log metadata", 3005, true)
	ErrMetadataCreateLogTxn    = newWoodpeckerError("failed execute create log metadata txn", 3006, true)
	ErrMetadataCreateSegment   = newWoodpeckerError("failed to create segment metadata", 3007, true)
	ErrMetadataUpdateSegment   = newWoodpeckerError("failed to update segment metadata", 3008, true)
	ErrMetadataUpdateQuorum    = newWoodpeckerError("failed to update quorum metadata", 3009, true)
	ErrMetadataCreateReader    = newWoodpeckerError("failed to create reader temp info", 3010, true)
	ErrMetadataRevisionInvalid = newWoodpeckerError("metadata revision is invalid or outdated", 3011, false)

	// ---------------------------------------------
	// Service Layer Errors
	// ---------------------------------------------

	// Service operations (4000-4099)
	ErrServiceNoFilterFound      = newWoodpeckerError("no filter provided", 4000, false)
	ErrServiceSelectQuorumFailed = newWoodpeckerError("failed to select quorum nodes", 4001, true)
	ErrServiceInsufficientQuorum = newWoodpeckerError("insufficient quorum nodes", 4002, true)

	// ---------------------------------------------
	// Common/Utility Errors
	// ---------------------------------------------

	// General errors (8000-8099)
	ErrInternalError           = newWoodpeckerError("internal error", 8000, true)
	ErrConfigError             = newWoodpeckerError("config error", 8001, false)
	ErrTimeoutError            = newWoodpeckerError("operation timeout", 8002, true)
	ErrCancelError             = newWoodpeckerError("operation cancelled", 8003, false)
	ErrOperationNotSupported   = newWoodpeckerError("operation not supported", 8004, false)
	ErrInvalidConfiguration    = newWoodpeckerError("invalid configuration", 8005, false)
	ErrUnsupportedVersionError = newWoodpeckerError("unsupported version", 8006, false)
	ErrChecksumError           = newWoodpeckerError("checksum error", 8007, false)
	ErrPersistenceError        = newWoodpeckerError("persistence error", 8008, true)

	// Data state errors (8100-8199)
	ErrInvalidMessage      = newWoodpeckerError("invalid message", 8100, false)
	ErrEmptyPayload        = newWoodpeckerError("empty payload", 8101, false)
	ErrEntryNotFound       = newWoodpeckerError("entry not found", 8102, true)
	ErrStorageNotWritable  = newWoodpeckerError("storage not writable", 8103, false)
	ErrSegmentFenced       = newWoodpeckerError("segment fenced", 8104, false)
	ErrSegmentNotFound     = newWoodpeckerError("segment not found", 8105, true)
	ErrObjectAlreadyExists = newWoodpeckerError("object already exists", 8106, false)
	ErrInvalidLACAlignment = newWoodpeckerError("invalid lac alignment", 8107, false)

	// ---------------------------------------------
	// External Errors
	// ---------------------------------------------

	// local filesystem errors ((9000-9099)
	ErrFSOperationErr = newWoodpeckerError("filesystem operation error", 9000, true)

	// minio errors (9100-9199)
	ErrMinioOperationErr = newWoodpeckerError("minio operation error", 9100, true)

	// Default errors report by dependencies or other system directly
	ErrUnknownError = newWoodpeckerError("unknown error", 1, true)
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
	if err == nil {
		return false
	}
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
