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
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/woodpecker/proto"
)

// ---------------------------------------------
// gRPC Status related error
// ---------------------------------------------

func Success(reason ...string) *proto.Status {
	status := &proto.Status{
		Code: 0,
	}
	// NOLINT
	status.Reason = strings.Join(reason, " ")
	return status
}

func Code(err error) int32 {
	if err == nil {
		return 0
	}

	// First check if it's already a woodpeckerError in the chain
	var wpErr woodpeckerError
	if errors.As(err, &wpErr) {
		return wpErr.Code()
	}

	// Then check for specific error types
	if errors.Is(err, context.Canceled) {
		return ErrCancelError.Code()
	} else if errors.Is(err, context.DeadlineExceeded) {
		return ErrTimeoutError.Code()
	} else {
		return ErrUnknownError.Code()
	}
}

// Status returns a status according to the given err,
// returns Success status if err is nil
func Status(err error) *proto.Status {
	if err == nil {
		return Success()
	}

	code := Code(err)

	// Extract the most relevant error message for Reason
	var reason string
	cause := errors.Cause(err)
	if wpErr, ok := cause.(woodpeckerError); ok {
		// For woodpecker errors, use the core message
		reason = wpErr.Error()
	} else {
		// For other errors, use the root cause
		reason = cause.Error()
	}

	status := &proto.Status{
		Code:      code,
		Reason:    reason,
		Retriable: IsRetryableErr(err),
		Detail:    err.Error(), // Full error chain
	}
	return status
}

func Error(status *proto.Status) error {
	// use code first
	code := status.GetCode()
	if code == 0 {
		return nil
	}

	return newWoodpeckerError(status.GetReason(), code, status.GetRetriable())
}

// ---------------------------------------------
// Segment Writable related error
// ---------------------------------------------

func IsSegmentNotWritableErr(err error) bool {
	if err == nil {
		return false
	}
	return ErrSegmentHandleSegmentClosed.Is(err) || ErrSegmentFenced.Is(err) || ErrStorageNotWritable.Is(err) || ErrFileWriterFinalized.Is(err) || ErrWoodpeckerClientClosed.Is(err)
}

// ---------------------------------------------
// Timeout related error
// ---------------------------------------------

// IsTimeoutError Helper function to check if an error is a timeout error (context deadline exceeded or gRPC deadline exceeded)
func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	// Check for standard context errors first
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return true
	}

	// Check for gRPC deadline exceeded
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.DeadlineExceeded || st.Code() == codes.Canceled {
			return true
		}
	}

	// Fallback to string matching for wrapped errors
	errMsg := err.Error()
	return strings.Contains(errMsg, "context deadline exceeded") ||
		strings.Contains(errMsg, "DeadlineExceeded") ||
		strings.Contains(errMsg, "context canceled")
}
