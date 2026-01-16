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
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestWoodpeckerError_ErrorChaining(t *testing.T) {
	// Test basic error creation
	baseErr := ErrWoodpeckerClientConnectionFailed
	if baseErr.Error() != "failed to create connection" {
		t.Errorf("Expected 'failed to create connection', got '%s'", baseErr.Error())
	}

	// Test error chaining with WithCauseErr
	originalErr := errors.New("network timeout")
	wrappedErr := baseErr.WithCauseErr(originalErr)

	// Check that the wrapped error contains both messages
	expectedMsg := "failed to create connection: network timeout"
	if wrappedErr.Error() != expectedMsg {
		t.Errorf("Expected '%s', got '%s'", expectedMsg, wrappedErr.Error())
	}

	// Test error unwrapping
	unwrapped := errors.Unwrap(wrappedErr)
	if unwrapped == nil {
		t.Error("Expected unwrapped error to not be nil")
	}
	if unwrapped.Error() != "network timeout" {
		t.Errorf("Expected 'network timeout', got '%s'", unwrapped.Error())
	}

	// Test errors.Is functionality
	if !errors.Is(wrappedErr, originalErr) {
		t.Error("Expected errors.Is to return true for original error")
	}

	// Test errors.Is with woodpecker error
	if !errors.Is(wrappedErr, baseErr) {
		t.Error("Expected errors.Is to return true for base woodpecker error")
	}

	// Test errors.As functionality
	var wpErr woodpeckerError
	if !errors.As(wrappedErr, &wpErr) {
		t.Error("Expected errors.As to return true for woodpeckerError")
	}
	if wpErr.Code() != 1000 {
		t.Errorf("Expected error code %d, got %d", 1000, wpErr.Code())
	}
}

func TestWoodpeckerError_WithContext(t *testing.T) {
	baseErr := ErrLogWriterClosed
	contextErr := baseErr.WithContext("during flush operation")

	expectedMsg := "during flush operation: log writer is closed"
	if contextErr.Error() != expectedMsg {
		t.Errorf("Expected '%s', got '%s'", expectedMsg, contextErr.Error())
	}

	// Test that the error code is preserved
	var wpErr woodpeckerError
	if !errors.As(contextErr, &wpErr) {
		t.Error("Expected errors.As to return true for woodpeckerError")
	}
	if wpErr.Code() != 1400 {
		t.Errorf("Expected error code %d, got %d", 1400, wpErr.Code())
	}
}

func TestWoodpeckerError_MultiLevelChaining(t *testing.T) {
	// Create a multi-level error chain
	rootErr := errors.New("disk full")
	level1Err := ErrFileWriterNoSpace.WithCauseErr(rootErr)
	level2Err := ErrLogWriterBufferFull.WithCauseErr(level1Err)

	// Test that we can traverse the entire chain
	if !errors.Is(level2Err, rootErr) {
		t.Error("Expected errors.Is to find root error in chain")
	}

	if !errors.Is(level2Err, ErrFileWriterNoSpace) {
		t.Error("Expected errors.Is to find level1 error in chain")
	}

	if !errors.Is(level2Err, ErrLogWriterBufferFull) {
		t.Error("Expected errors.Is to find level2 error in chain")
	}

	// Test error message contains all levels
	expectedSubstrings := []string{
		"log writer buffer is full",
		"writer no space available",
		"disk full",
	}

	errorMsg := level2Err.Error()
	for _, substring := range expectedSubstrings {
		if !contains(errorMsg, substring) {
			t.Errorf("Expected error message to contain '%s', got '%s'", substring, errorMsg)
		}
	}
}

func TestWoodpeckerError_RetryableProperty(t *testing.T) {
	// Test retryable error
	retryableErr := ErrWoodpeckerClientConnectionFailed // This is retryable
	if !IsRetryableErr(retryableErr) {
		t.Error("Expected IsRetryableErr to return true for retryable error")
	}

	// Test non-retryable error
	nonRetryableErr := ErrWoodpeckerClientClosed // This is not retryable
	if IsRetryableErr(nonRetryableErr) {
		t.Error("Expected IsRetryableErr to return false for non-retryable error")
	}

	// Test retryable property is preserved through chaining
	originalErr := errors.New("network issue")
	wrappedErr := retryableErr.WithCauseErr(originalErr)
	if !IsRetryableErr(wrappedErr) {
		t.Error("Expected IsRetryableErr to return true for wrapped retryable error")
	}
}

func TestWoodpeckerError_WithCauseErrMsg(t *testing.T) {
	baseErr := ErrInternalError
	msgErr := baseErr.WithCauseErrMsg("custom error message")

	if msgErr.Error() != "custom error message" {
		t.Errorf("Expected 'custom error message', got '%s'", msgErr.Error())
	}

	// Test that there's no underlying cause for message-only wrapping
	if errors.Unwrap(msgErr) != nil {
		t.Error("Expected no underlying cause for message-only wrapping")
	}

	// Test that the error code is preserved
	var wpErr woodpeckerError
	if !errors.As(msgErr, &wpErr) {
		t.Error("Expected errors.As to return true for woodpeckerError")
	}
	if wpErr.Code() != 8000 {
		t.Errorf("Expected error code %d, got %d", 8000, wpErr.Code())
	}
}

func TestMultiErrors_ErrorChaining(t *testing.T) {
	err1 := ErrLogWriterClosed
	err2 := ErrStorageNotWritable
	err3 := errors.New("custom error")

	multiErr := Combine(err1, err2, err3)

	// Test that errors.Is works with multiErrors
	if !errors.Is(multiErr, err1) {
		t.Error("Expected errors.Is to find err1 in multiErrors")
	}
	if !errors.Is(multiErr, err2) {
		t.Error("Expected errors.Is to find err2 in multiErrors")
	}
	if !errors.Is(multiErr, err3) {
		t.Error("Expected errors.Is to find err3 in multiErrors")
	}

	// Test that the error message contains information about all errors
	errorMsg := multiErr.Error()
	if !contains(errorMsg, "log writer is closed") {
		t.Errorf("Expected error message to contain err1, got '%s'", errorMsg)
	}

	newErr := fmt.Errorf("test")
	assert.True(t, errors.IsAny(multiErr, err2, newErr))
	assert.False(t, errors.IsAny(multiErr, newErr))
	assert.True(t, errors.IsAny(multiErr, err1, err3))

	assert.False(t, ErrSegmentHandleSegmentClosed.Is(nil))
	assert.False(t, ErrSegmentHandleSegmentClosed.Is(fmt.Errorf("test error")))
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			containsSubstring(s, substr))))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
