package stagedstorage

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
			scheduler := NewSyncScheduler(defaultSyncSchedulerWorkers)
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
			scheduler := NewSyncScheduler(defaultSyncSchedulerWorkers)
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

// TestNewSyncScheduler_WorkerCountIsExplicit verifies the pool size comes from the caller, and
// that a non-positive value falls back to the documented default rather than to the host's core
// count. Deriving it from runtime.NumCPU() read the machine's cores rather than the pod's cgroup
// limit, so identically specced pods ended up with pools that differed by a factor of eight
// depending on where they were scheduled.
func TestNewSyncScheduler_WorkerCountIsExplicit(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   int
		want int
	}{
		{"explicit count is honoured", 5, 5},
		{"one is honoured", 1, 1},
		{"zero falls back to the default", 0, defaultSyncSchedulerWorkers},
		{"negative falls back to the default", -3, defaultSyncSchedulerWorkers},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSyncScheduler(tc.in)
			defer s.Close()
			assert.Equal(t, tc.want, s.Capacity())
		})
	}
}

// TestNewSyncScheduler_QueueDepthIsIndependentOfWorkers verifies the channel depth no longer
// scales with the pool size. The queues hold pending work, which is bounded by the number of
// writers rather than by how many are being serviced, so tying a multi-megabyte preallocation to
// the worker count sized it by the wrong quantity.
func TestNewSyncScheduler_QueueDepthIsIndependentOfWorkers(t *testing.T) {
	small := NewSyncScheduler(1)
	defer small.Close()
	large := NewSyncScheduler(64)
	defer large.Close()

	assert.Equal(t, cap(small.scheduleCh), cap(large.scheduleCh))
	assert.Equal(t, cap(small.jobCh), cap(large.jobCh))
	assert.Equal(t, syncSchedulerQueueSize, cap(small.scheduleCh))
	assert.Equal(t, syncSchedulerQueueSize, cap(small.jobCh))
}

// blockWorker holds one worker until the test explicitly releases it.
func blockWorker(t *testing.T, scheduler *SyncScheduler, queue chan syncJob) func() {
	t.Helper()
	started, gate := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	require.True(t, scheduler.enqueue(queue, func(ctx context.Context) {
		close(started)
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}))
	<-started
	return release
}

func TestClosingFlushWithNormalPoolSaturated(t *testing.T) {
	for _, operation := range []string{"empty fence", "fence", "finalize", "close"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			s := NewSyncSchedulerWithClosingWorkers(1, 1)
			t.Cleanup(s.Close)
			release := blockWorker(t, s, s.jobCh)
			w, err := NewStagedFileWriter(ctx, "bucket", "root", t.TempDir(), 1, 0, nil, newTestConfig(t), s)
			require.NoError(t, err)
			t.Cleanup(func() { release(); _ = w.Close(context.Background()) })

			blocks := 3
			if operation == "empty fence" {
				blocks = 0
			}
			for i := 0; i < blocks; i++ {
				_, err := w.WriteDataAsync(ctx, int64(i), []byte("entry"), nil)
				require.NoError(t, err)
				require.NoError(t, w.Sync(ctx))
			}
			require.Len(t, w.flushTaskChan, blocks)
			switch operation {
			case "close":
				err = w.Close(ctx)
			case "finalize":
				_, err = w.Finalize(ctx, int64(blocks-1))
			default:
				_, err = w.Fence(ctx)
			}
			require.NoError(t, err)
			require.Equal(t, 1, s.Running())
			// Run the stale normal job before checking the persisted entries.
			finished := make(chan struct{})
			require.True(t, s.tryEnqueueJob(func(context.Context) { close(finished) }))
			release()
			<-finished
			require.Len(t, w.blockIndexes, blocks)
			for i, idx := range w.blockIndexes {
				require.EqualValues(t, i, idx.BlockNumber)
				require.EqualValues(t, i, idx.FirstEntryID)
				require.EqualValues(t, i, idx.LastEntryID)
			}
		})
	}
}

func TestClosingFlushCancelledFinalizeCanRetryAfterAnotherAppend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := NewSyncSchedulerWithClosingWorkers(1, 1)
	t.Cleanup(s.Close)
	releaseNormal := blockWorker(t, s, s.jobCh)
	releaseClosing := blockWorker(t, s, s.closingJobCh)
	w, err := NewStagedFileWriter(ctx, "bucket", "root", t.TempDir(), 1, 0, nil, newTestConfig(t), s)
	require.NoError(t, err)
	t.Cleanup(func() {
		releaseClosing()
		releaseNormal()
		_ = w.Close(context.Background())
	})
	_, err = w.WriteDataAsync(ctx, 0, []byte("first"), nil)
	require.NoError(t, err)
	require.NoError(t, w.Sync(ctx))

	attempt, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { _, err := w.Finalize(attempt, 0); done <- err }()
	require.Eventually(t, func() bool {
		running, waiting, capacity := s.ClosingStats()
		return running == 1 && waiting == 1 && capacity == 1 && len(w.flushTaskChan) == 2
	}, time.Second, time.Millisecond)
	stop()
	require.ErrorIs(t, <-done, context.Canceled)
	// Let the cancelled barrier complete before appending. Its acknowledgement
	// must not allow a retry to skip the new data waiting in the normal pool.
	releaseClosing()
	releaseNormal()
	require.Eventually(t, func() bool { return w.flushDrainState.Load() == flushDrainIdle }, time.Second, time.Millisecond)
	blockWorker(t, s, s.jobCh)
	_, err = w.WriteDataAsync(ctx, 1, []byte("second"), nil)
	require.NoError(t, err)
	last, err := w.Finalize(ctx, 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, last)
	require.Len(t, w.blockIndexes, 2)
}

func TestClosingFlushHandsBackWriterAfterCancelledFinalize(t *testing.T) {
	for _, tc := range []struct {
		name    string
		running bool
	}{
		{"queued before barrier", false},
		{"running with barrier", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s := NewSyncSchedulerWithClosingWorkers(1, 1)
			t.Cleanup(s.Close)
			releaseNormal := blockWorker(t, s, s.jobCh)
			releaseClosing := blockWorker(t, s, s.closingJobCh)
			w, err := NewStagedFileWriter(ctx, "bucket", "root", t.TempDir(), 1, 0, nil, newTestConfig(t), s)
			require.NoError(t, err)
			w.flushMu.Lock()
			unlock := sync.OnceFunc(w.flushMu.Unlock)
			t.Cleanup(func() {
				unlock()
				releaseNormal()
				releaseClosing()
				_ = w.Close(context.Background())
			})
			_, err = w.WriteDataAsync(ctx, 0, []byte("first"), nil)
			require.NoError(t, err)
			require.NoError(t, w.Sync(ctx))

			attempt, stop := context.WithCancel(ctx)
			defer stop()
			if tc.running {
				done := make(chan error, 1)
				go func() { _, err := w.Finalize(attempt, 0); done <- err }()
				require.Eventually(t, func() bool { return len(w.flushTaskChan) == 2 }, time.Second, time.Millisecond)
				releaseClosing()
				// The worker has taken the first block and is held in its flush;
				// only the Finalize barrier remains in the writer queue.
				require.Eventually(t, func() bool {
					return w.flushDrainState.Load() == flushDrainRunning && len(w.flushTaskChan) == 1
				}, time.Second, time.Millisecond)
				stop()
				require.ErrorIs(t, <-done, context.Canceled)
			} else {
				// Finalize promotes the queued drain before checking cancellation.
				stop()
				_, err := w.Finalize(attempt, 0)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, flushDrainClosingQueued, w.flushDrainState.Load())
				require.Len(t, w.flushTaskChan, 1)
			}

			_, err = w.WriteDataAsync(ctx, 1, []byte("after cancellation"), nil)
			require.NoError(t, err)
			require.NoError(t, w.Sync(ctx))
			unlock()
			releaseClosing()

			other, err := NewStagedFileWriter(ctx, "bucket", "root", t.TempDir(), 1, 1, nil, newTestConfig(t), s)
			require.NoError(t, err)
			t.Cleanup(func() { _ = other.Close(context.Background()) })
			_, err = other.Fence(ctx)
			require.NoError(t, err)
			require.Equal(t, flushDrainNormalQueued, w.flushDrainState.Load())
			require.Less(t, w.GetLastEntryId(ctx), int64(1), "new append must wait for an ordinary worker")

			releaseNormal()
			require.Eventually(t, func() bool { return w.GetLastEntryId(ctx) == 1 }, time.Second, time.Millisecond)
			last, err := w.Finalize(ctx, 1)
			require.NoError(t, err)
			require.EqualValues(t, 1, last)
			require.Len(t, w.blockIndexes, 2)
			for i, block := range w.blockIndexes {
				require.EqualValues(t, i, block.FirstEntryID)
				require.EqualValues(t, i, block.LastEntryID)
			}
		})
	}
}

func TestClosingFlushWaitsForRunningNormalDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := NewSyncSchedulerWithClosingWorkers(1, 1)
	t.Cleanup(s.Close)
	w, err := NewStagedFileWriter(ctx, "bucket", "root", t.TempDir(), 1, 0, nil, newTestConfig(t), s)
	require.NoError(t, err)
	w.flushMu.Lock()
	unlock := sync.OnceFunc(w.flushMu.Unlock)
	t.Cleanup(func() { unlock(); _ = w.Close(context.Background()) })
	_, err = w.WriteDataAsync(ctx, 0, []byte("active"), nil)
	require.NoError(t, err)
	require.NoError(t, w.Sync(ctx))
	require.Eventually(t, func() bool {
		return w.flushDrainState.Load() == flushDrainRunning && len(w.flushTaskChan) == 0
	}, time.Second, time.Millisecond)

	done := make(chan error, 2)
	go func() { _, err := w.Fence(ctx); done <- err }()
	require.Eventually(t, func() bool { return len(w.flushTaskChan) == 1 }, time.Second, time.Millisecond)
	go func() { _, err := w.Finalize(ctx, 0); done <- err }()
	running, waiting, _ := s.ClosingStats()
	require.Zero(t, running)
	require.Zero(t, waiting)
	unlock()
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	require.Len(t, w.blockIndexes, 1)
	require.True(t, w.finalized.Load())
}

func TestClosingFlushPromotesFullWriterQueueBeforeAcquiringWriterLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := NewSyncSchedulerWithClosingWorkers(1, 1)
	t.Cleanup(s.Close)
	release := blockWorker(t, s, s.jobCh)
	cfg := newTestConfig(t)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries = 1
	w, err := NewStagedFileWriter(ctx, "bucket", "root", t.TempDir(), 1, 0, nil, cfg, s)
	require.NoError(t, err)
	t.Cleanup(func() { release(); _ = w.Close(context.Background()) })
	w.flushTaskChan = make(chan *blockFlushTask, 1)

	_, err = w.WriteDataAsync(ctx, 0, []byte("fill queue"), nil)
	require.NoError(t, err)
	require.Len(t, w.flushTaskChan, 1)
	done := make(chan error, 1)
	go func() {
		_, err := w.WriteDataAsync(ctx, 1, []byte("wait for queue space"), nil)
		done <- err
	}()
	require.Eventually(t, func() bool {
		if w.mu.TryLock() {
			w.mu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
	last, err := w.Fence(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, last)
	require.NoError(t, <-done)
	require.Len(t, w.blockIndexes, 2)
}
