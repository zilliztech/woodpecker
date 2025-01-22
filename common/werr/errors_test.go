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
