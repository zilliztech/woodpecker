package commitlog

import (
	"time"
)

// SyncPolicy defines the interface for different sync policies.
type SyncPolicy interface {
	ShouldSync() bool
}

// PeriodicSyncPolicy syncs at regular intervals.
type PeriodicSyncPolicy struct {
	interval time.Duration
	lastSync time.Time
}

func NewPeriodicSyncPolicy(interval time.Duration) *PeriodicSyncPolicy {
	return &PeriodicSyncPolicy{
		interval: interval,
		lastSync: time.Now(),
	}
}

func (p *PeriodicSyncPolicy) ShouldSync() bool {
	if time.Since(p.lastSync) >= p.interval {
		p.lastSync = time.Now()
		return true
	}
	return false
}

// CountSyncPolicy syncs after a certain number of appends.
type CountSyncPolicy struct {
	count    int
	maxCount int
}

func NewCountSyncPolicy(maxCount int) *CountSyncPolicy {
	return &CountSyncPolicy{
		maxCount: maxCount,
	}
}

func (c *CountSyncPolicy) ShouldSync() bool {
	c.count++
	if c.count >= c.maxCount {
		c.count = 0
		return true
	}
	return false
}
