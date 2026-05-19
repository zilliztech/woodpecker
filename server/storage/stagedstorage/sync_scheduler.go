package stagedstorage

import (
	"container/heap"
	"context"
	"runtime"
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

var (
	defaultSyncSchedulerOnce sync.Once
	defaultSyncScheduler     *SyncScheduler
)

func DefaultSyncScheduler() *SyncScheduler {
	defaultSyncSchedulerOnce.Do(func() {
		defaultSyncScheduler = NewSyncScheduler(runtime.NumCPU() * 2)
	})
	return defaultSyncScheduler
}

func NewSyncScheduler(workerCount int) *SyncScheduler {
	if workerCount <= 0 {
		workerCount = runtime.NumCPU() * 2
	}
	if workerCount <= 0 {
		workerCount = 1
	}
	queueSize := workerCount * 4096
	if queueSize < 1024 {
		queueSize = 1024
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &SyncScheduler{
		ctx:         ctx,
		cancel:      cancel,
		scheduleCh:  make(chan syncScheduleRequest, queueSize),
		jobCh:       make(chan syncJob, queueSize),
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
