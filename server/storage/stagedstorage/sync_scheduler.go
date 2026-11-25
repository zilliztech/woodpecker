package stagedstorage

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zilliztech/woodpecker/common/config"
)

type syncJob func(context.Context)

type syncSchedulerTarget interface {
	resetSyncScheduled()
	enqueueScheduledSyncJob()
}

type syncScheduleRequest struct {
	target syncSchedulerTarget
	delay  time.Duration
}

type scheduledSync struct {
	target syncSchedulerTarget
	due    time.Time
}

type syncScheduleHeap []*scheduledSync

func (h syncScheduleHeap) Len() int {
	return len(h)
}

func (h syncScheduleHeap) Less(i, j int) bool {
	return h[i].due.Before(h[j].due)
}

func (h syncScheduleHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *syncScheduleHeap) Push(x any) {
	*h = append(*h, x.(*scheduledSync))
}

func (h *syncScheduleHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// SyncScheduler owns the shared staged-writer timer and two bounded worker pools.
// It replaces per-writer run goroutines without changing each writer's flush
// queue processing semantics.
type SyncScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	scheduleCh   chan syncScheduleRequest
	jobCh        chan syncJob
	closingJobCh chan syncJob

	workerCount        int
	closingWorkerCount int
	wg                 sync.WaitGroup
	closeOnce          sync.Once

	running        atomic.Int64
	closingRunning atomic.Int64
	delayed        atomic.Int64
	closed         atomic.Bool
}

// syncSchedulerQueueSize is the depth of each scheduler channel.
//
// It is deliberately independent of the worker count: the queues hold pending work, and the
// amount of pending work is bounded by the number of writers, not by how many of them are being
// serviced at once. Writers coalesce their timer requests, sync jobs and queue drains.
//
// The depth matters because a full scheduleCh blocks its caller, and the caller is the append
// path: WriteDataAsync schedules the next sync check before it returns.
const syncSchedulerQueueSize = 65536

var (
	defaultSyncSchedulerOnce sync.Once
	defaultSyncScheduler     *SyncScheduler
)

func DefaultSyncScheduler() *SyncScheduler {
	defaultSyncSchedulerOnce.Do(func() {
		defaultSyncScheduler = NewSyncScheduler(config.DefaultMaxWorkers)
	})
	return defaultSyncScheduler
}

func NewSyncScheduler(workerCount int) *SyncScheduler {
	return NewSyncSchedulerWithClosingWorkers(workerCount, config.DefaultClosingWorkers)
}

func NewSyncSchedulerWithClosingWorkers(workerCount, closingWorkers int) *SyncScheduler {
	if closingWorkers <= 0 {
		closingWorkers = config.DefaultClosingWorkers
	}
	if workerCount <= 0 {
		workerCount = config.DefaultMaxWorkers
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &SyncScheduler{
		ctx:                ctx,
		cancel:             cancel,
		scheduleCh:         make(chan syncScheduleRequest, syncSchedulerQueueSize),
		jobCh:              make(chan syncJob, syncSchedulerQueueSize),
		workerCount:        workerCount,
		closingWorkerCount: closingWorkers,
		closingJobCh:       make(chan syncJob, syncSchedulerQueueSize),
	}
	s.wg.Add(1)
	go s.runDelayLoop()
	for i := 0; i < workerCount; i++ {
		s.wg.Add(1)
		go s.runWorkerLoop(s.jobCh, &s.running)
	}
	for i := 0; i < closingWorkers; i++ {
		s.wg.Add(1)
		go s.runWorkerLoop(s.closingJobCh, &s.closingRunning)
	}
	return s
}

func (s *SyncScheduler) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		s.wg.Wait()
	})
}

func (s *SyncScheduler) ScheduleSyncCheckAfter(writer *StagedFileWriter, delay time.Duration) bool {
	if s == nil || writer == nil {
		return false
	}
	return s.scheduleSyncCheckAfter(writer, delay)
}

func (s *SyncScheduler) scheduleSyncCheckAfter(target syncSchedulerTarget, delay time.Duration) bool {
	if s == nil || target == nil || s.closed.Load() {
		return false
	}
	if delay <= 0 {
		delay = time.Millisecond
	}
	select {
	case s.scheduleCh <- syncScheduleRequest{target: target, delay: delay}:
		return true
	case <-s.ctx.Done():
		return false
	}
}

func (s *SyncScheduler) tryEnqueueJob(job syncJob) bool {
	return s != nil && s.enqueue(s.jobCh, job)
}

func (s *SyncScheduler) enqueue(queue chan<- syncJob, job syncJob) bool {
	if job == nil || s.closed.Load() {
		return false
	}
	select {
	case queue <- job:
		return true
	case <-s.ctx.Done():
		return false
	}
}

func (s *SyncScheduler) Running() int {
	if s == nil {
		return 0
	}
	return int(s.running.Load())
}

func (s *SyncScheduler) Waiting() int {
	if s == nil {
		return 0
	}
	return len(s.jobCh)
}

// ClosingStats reports the running workers, queued jobs and capacity of the closing pool.
func (s *SyncScheduler) ClosingStats() (running, waiting, capacity int) {
	if s == nil {
		return 0, 0, 0
	}
	return int(s.closingRunning.Load()), len(s.closingJobCh), s.closingWorkerCount
}

func (s *SyncScheduler) Scheduled() int {
	if s == nil {
		return 0
	}
	return int(s.delayed.Load())
}

func (s *SyncScheduler) Capacity() int {
	if s == nil {
		return 0
	}
	return s.workerCount
}

func (s *SyncScheduler) runDelayLoop() {
	defer s.wg.Done()
	h := &syncScheduleHeap{}
	heap.Init(h)
	for {
		if h.Len() == 0 {
			select {
			case <-s.ctx.Done():
				return
			case req := <-s.scheduleCh:
				heap.Push(h, &scheduledSync{
					target: req.target,
					due:    time.Now().Add(req.delay),
				})
				s.delayed.Add(1)
			}
			continue
		}

		next := (*h)[0]
		delay := time.Until(next.due)
		if delay <= 0 {
			item := heap.Pop(h).(*scheduledSync)
			s.delayed.Add(-1)
			item.target.resetSyncScheduled()
			item.target.enqueueScheduledSyncJob()
			continue
		}

		timer := time.NewTimer(delay)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case req := <-s.scheduleCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			heap.Push(h, &scheduledSync{
				target: req.target,
				due:    time.Now().Add(req.delay),
			})
			s.delayed.Add(1)
		}
	}
}

func (s *SyncScheduler) runWorkerLoop(queue <-chan syncJob, running *atomic.Int64) {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case job := <-queue:
			if job == nil {
				continue
			}
			running.Add(1)
			func() {
				defer running.Add(-1)
				job(s.ctx)
			}()
		}
	}
}
