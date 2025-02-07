package err

import (
	"errors"
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
	sameErr := newWoodpeckerError(errMsg, errCode, retryable)
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
