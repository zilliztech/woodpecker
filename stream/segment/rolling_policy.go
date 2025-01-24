package segment

import "time"

type RollingPolicy interface {
	// ShouldRollover returns true if the current segment should be rolled over.
	ShouldRollover(currentSegmentSize int64, lastRolloverTimeMs int64) bool
}

func NewDefaultRollingPolicy(rolloverIntervalMs int64, rolloverSizeBytes int64) RollingPolicy {
	return &DefaultRollingPolicy{
		rolloverIntervalMs: rolloverIntervalMs,
		rolloverSizeBytes:  rolloverSizeBytes,
	}
}

var _ RollingPolicy = &DefaultRollingPolicy{}

type DefaultRollingPolicy struct {
	rolloverIntervalMs int64
	rolloverSizeBytes  int64
}

func (p *DefaultRollingPolicy) ShouldRollover(currentSegmentSize int64, lastRolloverTimeMs int64) bool {
	// If the current segment is already larger than the rollover size, or if the last rollover time is more than the rollover interval, roll over.
	if currentSegmentSize >= p.rolloverSizeBytes {
		return true
	}
	// If the current segment is not empty, and the last rollover time is more than the rollover interval, roll over.
	if currentSegmentSize > 0 && (time.Now().UnixNano()-lastRolloverTimeMs) >= p.rolloverIntervalMs {
		return true
	}
	// Otherwise, do not roll over.
	return false
}
