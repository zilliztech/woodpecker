package stagedstorage

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"
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

// SyncScheduler owns the shared staged-writer timer and bounded worker pool.
// It replaces per-writer run goroutines without changing each writer's flush
// queue processing semantics.
type SyncScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	scheduleCh chan syncScheduleRequest
	jobCh      chan syncJob

	workerCount int
	wg          sync.WaitGroup
	closeOnce   sync.Once

	running atomic.Int64
	delayed atomic.Int64
	closed  atomic.Bool
}

// defaultSyncSchedulerWorkers is the pool size used when no configured value is available.
// It mirrors logstore.syncScheduler.maxWorkers so the two cannot drift.
const defaultSyncSchedulerWorkers = 32

// syncSchedulerQueueSize is the depth of both scheduler channels.
//
// It is deliberately independent of the worker count: the queues hold pending work, and the
// amount of pending work is bounded by the number of writers, not by how many of them are being
// serviced at once. Each writer can have at most one schedule request and one submitted sync job
// outstanding (the syncScheduled and syncTaskSubmitted CAS gates), so one slot per writer is the
// real ceiling. Sizing it as workerCount x 4096 tied a multi-megabyte preallocation to a quantity
// that has nothing to do with it.
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
		defaultSyncScheduler = NewSyncScheduler(defaultSyncSchedulerWorkers)
	})
	return defaultSyncScheduler
}

func NewSyncScheduler(workerCount int) *SyncScheduler {
	if workerCount <= 0 {
		workerCount = defaultSyncSchedulerWorkers
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &SyncScheduler{
		ctx:         ctx,
		cancel:      cancel,
		scheduleCh:  make(chan syncScheduleRequest, syncSchedulerQueueSize),
		jobCh:       make(chan syncJob, syncSchedulerQueueSize),
		workerCount: workerCount,
	}
	s.wg.Add(1)
	go s.runDelayLoop()
	for i := 0; i < workerCount; i++ {
		s.wg.Add(1)
		go s.runWorkerLoop()
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
	if s == nil || job == nil || s.closed.Load() {
		return false
	}
	select {
	case s.jobCh <- job:
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

func (s *SyncScheduler) runWorkerLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case job := <-s.jobCh:
			if job == nil {
				continue
			}
			s.running.Add(1)
			func() {
				defer s.running.Add(-1)
				job(s.ctx)
			}()
		}
	}
}
