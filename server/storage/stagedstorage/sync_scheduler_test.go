package stagedstorage

import (
	"container/heap"
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testSyncSchedulerTarget struct {
	resetCount   atomic.Int64
	enqueueCount atomic.Int64
	fired        chan time.Time
}

func (t *testSyncSchedulerTarget) resetSyncScheduled() {
	t.resetCount.Add(1)
}

func (t *testSyncSchedulerTarget) enqueueScheduledSyncJob() {
	t.enqueueCount.Add(1)
	if t.fired != nil {
		select {
		case t.fired <- time.Now():
		default:
		}
	}
}

func TestSyncScheduleHeapOrdersByDueTime(t *testing.T) {
	now := time.Now()
	first := &testSyncSchedulerTarget{}
	second := &testSyncSchedulerTarget{}
	third := &testSyncSchedulerTarget{}

	h := &syncScheduleHeap{}
	heap.Init(h)
	heap.Push(h, &scheduledSync{target: third, due: now.Add(30 * time.Millisecond)})
	heap.Push(h, &scheduledSync{target: first, due: now.Add(10 * time.Millisecond)})
	heap.Push(h, &scheduledSync{target: second, due: now.Add(20 * time.Millisecond)})

	require.Same(t, first, heap.Pop(h).(*scheduledSync).target)
	require.Same(t, second, heap.Pop(h).(*scheduledSync).target)
	require.Same(t, third, heap.Pop(h).(*scheduledSync).target)
}

func TestSyncSchedulerTryEnqueueJobExecutesJobs(t *testing.T) {
	scheduler := NewSyncScheduler(2)
	defer scheduler.Close()

	const totalJobs = 32
	var wg sync.WaitGroup
	wg.Add(totalJobs)
	for i := 0; i < totalJobs; i++ {
		require.True(t, scheduler.tryEnqueueJob(func(context.Context) {
			wg.Done()
		}))
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for scheduler jobs")
	}
	require.Eventually(t, func() bool {
		return scheduler.Running() == 0 && scheduler.Waiting() == 0
	}, time.Second, time.Millisecond)
}

func TestSyncSchedulerScheduleSyncCheckAfterFiresAfterDelay(t *testing.T) {
	scheduler := NewSyncScheduler(1)
	defer scheduler.Close()

	target := &testSyncSchedulerTarget{fired: make(chan time.Time, 1)}
	delay := 20 * time.Millisecond
	start := time.Now()
	require.True(t, scheduler.scheduleSyncCheckAfter(target, delay))

	select {
	case firedAt := <-target.fired:
		t.Fatalf("sync check fired too early after %s", firedAt.Sub(start))
	case <-time.After(delay / 2):
	}

	select {
	case firedAt := <-target.fired:
		require.GreaterOrEqual(t, firedAt.Sub(start), delay/2)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delayed sync check")
	}
	require.Equal(t, int64(1), target.resetCount.Load())
	require.Equal(t, int64(1), target.enqueueCount.Load())
}

func TestSyncSchedulerCloseRejectsNewWork(t *testing.T) {
	scheduler := NewSyncScheduler(1)
	scheduler.Close()

	target := &testSyncSchedulerTarget{}
	require.False(t, scheduler.scheduleSyncCheckAfter(target, time.Millisecond))
	require.False(t, scheduler.tryEnqueueJob(func(context.Context) {}))
}

type benchmarkSyncSchedulerTarget struct {
	dueUnixNano int64
	fired       chan time.Duration
}

func (t *benchmarkSyncSchedulerTarget) resetSyncScheduled() {}

func (t *benchmarkSyncSchedulerTarget) enqueueScheduledSyncJob() {
	t.fired <- time.Since(time.Unix(0, t.dueUnixNano))
}

func BenchmarkSyncSchedulerJobDispatchLatency(b *testing.B) {
	for _, jobs := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("jobs_%d", jobs), func(b *testing.B) {
			b.ReportAllocs()
			scheduler := NewSyncScheduler(runtime.NumCPU())
			defer scheduler.Close()

			latencies := make(chan time.Duration, jobs)
			var totalJobs int64
			var totalLatencyNs int64
			var maxLatencyNs int64

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(jobs)
				for j := 0; j < jobs; j++ {
					enqueuedAt := time.Now()
					if !scheduler.tryEnqueueJob(func(context.Context) {
						latencies <- time.Since(enqueuedAt)
						wg.Done()
					}) {
						b.Fatal("failed to enqueue scheduler job")
					}
				}
				wg.Wait()

				for j := 0; j < jobs; j++ {
					latencyNs := (<-latencies).Nanoseconds()
					totalLatencyNs += latencyNs
					if latencyNs > maxLatencyNs {
						maxLatencyNs = latencyNs
					}
				}
				totalJobs += int64(jobs)
			}
			b.StopTimer()

			b.ReportMetric(float64(totalLatencyNs)/float64(totalJobs)/1e3, "avg_us/job")
			b.ReportMetric(float64(maxLatencyNs)/1e3, "max_us/job")
		})
	}
}

func BenchmarkSyncSchedulerDelayedCheckLatency(b *testing.B) {
	for _, checks := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("checks_%d", checks), func(b *testing.B) {
			b.ReportAllocs()
			scheduler := NewSyncScheduler(runtime.NumCPU())
			defer scheduler.Close()

			delay := 10 * time.Millisecond
			targets := make([]benchmarkSyncSchedulerTarget, checks)
			latencies := make(chan time.Duration, checks)
			var totalChecks int64
			var totalLateNs int64
			var maxLateNs int64

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < checks; j++ {
					due := time.Now().Add(delay)
					targets[j].dueUnixNano = due.UnixNano()
					targets[j].fired = latencies
					if !scheduler.scheduleSyncCheckAfter(&targets[j], delay) {
						b.Fatal("failed to schedule sync check")
					}
				}
				for j := 0; j < checks; j++ {
					lateNs := (<-latencies).Nanoseconds()
					totalLateNs += lateNs
					if lateNs > maxLateNs {
						maxLateNs = lateNs
					}
				}
				totalChecks += int64(checks)
			}
			b.StopTimer()

			b.ReportMetric(float64(totalLateNs)/float64(totalChecks)/1e3, "avg_late_us/check")
			b.ReportMetric(float64(maxLateNs)/1e3, "max_late_us/check")
		})
	}
}
