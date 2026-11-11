package hardware

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestSystemMetrics_UsedRatio_ZeroTotal(t *testing.T) {
	// When TotalMemoryBytes is 0, UsedRatio should return 1.0
	sm := SystemMetrics{
		UsedMemoryBytes:  0,
		TotalMemoryBytes: 0,
	}
	assert.Equal(t, 1.0, sm.UsedRatio())
}

func TestSystemMetrics_UsedRatio_Normal(t *testing.T) {
	sm := SystemMetrics{
		UsedMemoryBytes:  50,
		TotalMemoryBytes: 100,
	}
	assert.Equal(t, 0.5, sm.UsedRatio())
}

func TestSystemMetrics_String(t *testing.T) {
	sm := SystemMetrics{
		UsedMemoryBytes:  1024 * 1024, // 1MB
		TotalMemoryBytes: 2 * 1024 * 1024,
	}
	s := sm.String()
	assert.Contains(t, s, "used:")
	assert.Contains(t, s, "total:")
}

func TestListener(t *testing.T) {
	w := NewSystemMetricsWatcher(20 * time.Millisecond)
	called := atomic.NewInt32(0)
	conditionCalled := atomic.NewInt32(0)
	l := &SystemMetricsListener{
		Cooldown: 100 * time.Millisecond,
		Condition: func(stats SystemMetrics, listener *SystemMetricsListener) bool {
			assert.NotZero(t, stats.UsedMemoryBytes)
			assert.NotZero(t, stats.TotalMemoryBytes)
			assert.NotZero(t, stats.UsedRatio())
			assert.NotEmpty(t, stats.String())
			conditionCalled.Inc()
			return true
		},
		Callback: func(sm SystemMetrics, listener *SystemMetricsListener) {
			assert.NotZero(t, sm.UsedMemoryBytes)
			assert.NotZero(t, sm.TotalMemoryBytes)
			assert.NotZero(t, sm.UsedRatio())
			assert.NotEmpty(t, sm.String())
			called.Inc()
		},
	}
	w.RegisterListener(l)
	time.Sleep(100 * time.Millisecond)
	assert.Less(t, called.Load(), int32(5))
	assert.Greater(t, called.Load(), int32(0))
	assert.Greater(t, conditionCalled.Load(), int32(0))
	w.UnregisterListener(l)
	w.Close()

	// Test global watcher registration/unregistration (does not panic)
	l2 := &SystemMetricsListener{
		Cooldown:  100 * time.Millisecond,
		Condition: l.Condition,
		Callback:  l.Callback,
	}
	assert.NotPanics(t, func() {
		RegisterSystemMetricsListener(l)
		RegisterSystemMetricsListener(l2)
		RegisterSystemMetricsListener(l2) // duplicate registration should not panic
		UnregisterSystemMetricsListener(l)
		UnregisterSystemMetricsListener(l2)
	})
}
