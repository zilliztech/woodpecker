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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWoodpeckerError tests the custom woodpeckerError type
func TestWoodpeckerError(t *testing.T) {
	// Create a new woodpeckerError instance
	errMsg := "test error"
	errCode := int32(1001)
	retryable := true
	testErr := newWoodpeckerError(errMsg, errCode, retryable)

	// Test Error() method
	assert.Equal(t, errMsg, testErr.Error())

	// Test Code() method
	assert.Equal(t, errCode, testErr.Code())

	// Test IsRetryable() method
	assert.Equal(t, retryable, testErr.IsRetryable())

	// Test Is() method
	sameErr := newWoodpeckerError("test error 2", errCode, retryable)
	assert.True(t, testErr.Is(sameErr))
	differentErr := newWoodpeckerError("different error", int32(2002), retryable)
	assert.False(t, testErr.Is(differentErr))

	// Test IsRetryableErr function
	assert.True(t, IsRetryableErr(testErr))
	assert.False(t, IsRetryableErr(errors.New("standard error")))

	// Test errors.Is
	assert.True(t, errors.Is(testErr, sameErr))
	assert.False(t, errors.Is(testErr, differentErr))
	assert.False(t, errors.Is(testErr, errors.New("standard error")))
}

func TestMultiErrors_Is(t *testing.T) {
	err1 := errors.New("error 1")
	err2 := ErrEntryNotFound
	err3 := ErrSegmentNotFound

	multiErr := Combine(err1, err2, err3)

	assert.True(t, multiErr.(*multiErrors).Is(err1))
	assert.True(t, multiErr.(*multiErrors).Is(err2))
	assert.True(t, multiErr.(*multiErrors).Is(err3))

	assert.Equal(t, fmt.Sprintf("%s: %s: %s", err1.Error(), err2.Error(), err3.Error()), multiErr.Error())
}

// TestMultiErrors_Error 测试 multiErrors 的 Error 方法
func TestMultiErrors_Error(t *testing.T) {
	err1 := errors.New("error 1")
	err2 := errors.New("error 2")
	err3 := errors.New("error 3")

	multiErr := Combine(err1, err2, err3)

	expected := "error 1: error 2: error 3"
	actual := multiErr.Error()
	if actual != expected {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}
