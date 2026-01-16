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
	"testing"

	"github.com/cockroachdb/errors"
)

func TestUtils_Code(t *testing.T) {
	// Test nil error
	if Code(nil) != 0 {
		t.Error("Expected Code(nil) to return 0")
	}

	// Test woodpecker error
	wpErr := ErrLogWriterClosed
	if Code(wpErr) != wpErr.Code() {
		t.Errorf("Expected Code to return %d, got %d", wpErr.Code(), Code(wpErr))
	}

	// Test wrapped woodpecker error
	wrappedErr := errors.Wrap(wpErr, "additional context")
	if Code(wrappedErr) != wpErr.Code() {
		t.Errorf("Expected Code to return %d for wrapped error, got %d", wpErr.Code(), Code(wrappedErr))
	}

	// Test context.Canceled
	if Code(context.Canceled) != ErrCancelError.Code() {
		t.Errorf("Expected Code to return %d for context.Canceled, got %d", ErrCancelError.Code(), Code(context.Canceled))
	}

	// Test context.DeadlineExceeded
	if Code(context.DeadlineExceeded) != ErrTimeoutError.Code() {
		t.Errorf("Expected Code to return %d for context.DeadlineExceeded, got %d", ErrTimeoutError.Code(), Code(context.DeadlineExceeded))
	}

	// Test unknown error
	unknownErr := errors.New("some unknown error")
	if Code(unknownErr) != ErrUnknownError.Code() {
		t.Errorf("Expected Code to return %d for unknown error, got %d", ErrUnknownError.Code(), Code(unknownErr))
	}
}

func TestUtils_StatusAndError(t *testing.T) {
	// Test nil error -> Success status
	status := Status(nil)
	if status.Code != 0 {
		t.Errorf("Expected Success status code 0, got %d", status.Code)
	}
	if err := Error(status); err != nil {
		t.Errorf("Expected Error(Success status) to return nil, got %v", err)
	}

	// Test woodpecker error round-trip
	originalErr := ErrLogWriterClosed.WithContext("during flush")
	status = Status(originalErr)

	expectedCode := ErrLogWriterClosed.Code()
	if status.Code != expectedCode {
		t.Errorf("Expected status code %d, got %d", expectedCode, status.Code)
	}
	if status.Retriable {
		t.Error("Expected status to be non-retriable for ErrLogWriterClosed")
	}
	if status.Detail == "" {
		t.Error("Expected status to have detail")
	}
	if status.Reason == "" {
		t.Error("Expected status to have reason")
	}

	// Round-trip back to error
	reconstructedErr := Error(status)
	if reconstructedErr == nil {
		t.Error("Expected Error to return non-nil error")
	}

	var wpErr woodpeckerError
	if !errors.As(reconstructedErr, &wpErr) {
		t.Error("Expected reconstructed error to be woodpeckerError")
	}
	if wpErr.Code() != expectedCode {
		t.Errorf("Expected reconstructed error code %d, got %d", expectedCode, wpErr.Code())
	}
}

func TestUtils_Success(t *testing.T) {
	// Test Success with no reason
	status := Success()
	if status.Code != 0 {
		t.Errorf("Expected Success code 0, got %d", status.Code)
	}
	if status.Reason != "" {
		t.Errorf("Expected empty reason, got %s", status.Reason)
	}

	// Test Success with reasons
	status = Success("operation", "completed", "successfully")
	if status.Code != 0 {
		t.Errorf("Expected Success code 0, got %d", status.Code)
	}
	expected := "operation completed successfully"
	if status.Reason != expected {
		t.Errorf("Expected reason '%s', got '%s'", expected, status.Reason)
	}
}
