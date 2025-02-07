package segment

import (
	"testing"
	"time"
)

func TestDefaultRollingPolicy_ShouldRollover(t *testing.T) {
	policy := NewDefaultRollingPolicy(1000, 100)

	// Test case 1: When the current segment size exceeds the rollover size, ShouldRollover should return true
	lastRolloverTimeMs := time.Now().UnixMilli() - 500
	currentSegmentSize := int64(150)
	if !policy.ShouldRollover(currentSegmentSize, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return true, got false")
	}

	// Test case 2: When the current segment size does not exceed the rollover size, but the last rollover time exceeds the rollover interval, ShouldRollover should return true
	lastRolloverTimeMs = time.Now().UnixMilli() - 1500
	currentSegmentSize = int64(50)
	if !policy.ShouldRollover(currentSegmentSize, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return true, got false")
	}

	// Test case 3: When the current segment size does not exceed the rollover size, and the last rollover time does not exceed the rollover interval, ShouldRollover should return false
	currentSegmentSize = int64(50)
	lastRolloverTimeMs = time.Now().UnixMilli() - 500
	if policy.ShouldRollover(currentSegmentSize, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return false, got true")
	}

	// Test case 4: When the current segment size is 0, and the last rollover time exceeds the rollover interval, ShouldRollover should return false
	currentSegmentSize = int64(0)
	lastRolloverTimeMs = time.Now().UnixMilli() - 1500
	if policy.ShouldRollover(currentSegmentSize, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return false, got true")
	}
}
