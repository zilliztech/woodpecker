package logger

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"

	"github.com/zilliztech/woodpecker/common/config"
)

type outputPolicy struct {
	capacity          int
	droppedTimeout    time.Duration
	nonDroppableLevel zapcore.Level
	stopTimeout       time.Duration
}

func resolveOutputPolicy(cfg config.AsyncWriteConfig) outputPolicy {
	defaults := config.DefaultAsyncWriteConfig()
	if cfg.PendingLength <= 0 {
		cfg.PendingLength = defaults.PendingLength
	}
	if cfg.DroppedTimeout == nil || *cfg.DroppedTimeout < 0 {
		cfg.DroppedTimeout = defaults.DroppedTimeout
	}
	if cfg.StopTimeout <= 0 {
		cfg.StopTimeout = defaults.StopTimeout
	}
	level, err := zapcore.ParseLevel(cfg.NonDroppableLevel)
	if cfg.NonDroppableLevel == "" || err != nil {
		level = zapcore.ErrorLevel
	}
	return outputPolicy{
		capacity:          cfg.PendingLength,
		droppedTimeout:    *cfg.DroppedTimeout,
		nonDroppableLevel: level,
		stopTimeout:       cfg.StopTimeout,
	}
}

func (p outputPolicy) enqueueWait(level zapcore.Level) time.Duration {
	if level >= p.nonDroppableLevel {
		return max(p.droppedTimeout, p.stopTimeout)
	}
	return p.droppedTimeout
}

// The encoder runs on the caller so mutable fields are captured at log time.
// All child loggers share the same bounded output and worker.
type asyncCore struct {
	zapcore.LevelEnabler
	encoder zapcore.Encoder
	output  *asyncOutput
}

func (c *asyncCore) With(fields []zapcore.Field) zapcore.Core {
	child := *c
	child.encoder = c.encoder.Clone()
	for _, field := range fields {
		field.AddTo(child.encoder)
	}
	return &child
}

func (c *asyncCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *asyncCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	q := c.output
	policy := q.policy.Load()
	wait := policy.enqueueWait(ent.Level)
	// With immediate drop configured, skip encoding when the queue is full.
	if wait == 0 && q.depth() >= policy.capacity {
		q.dropped.Add(1)
		return nil
	}
	buf, err := c.encoder.EncodeEntry(ent, fields)
	if err != nil {
		return err
	}
	item := outputItem{buf: buf}
	queued := q.offer(item, nil)
	if !queued && wait > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		queued = q.offer(item, ctx.Done())
		cancel()
	}
	if !queued {
		buf.Free()
		q.dropped.Add(1)
	} else if ent.Level > zapcore.ErrorLevel {
		// Zap may panic or exit immediately after this call.
		_ = c.Sync()
	}
	return nil
}

func (c *asyncCore) Sync() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.output.policy.Load().stopTimeout)
	defer cancel()
	result := make(chan error, 1)
	if !c.output.offer(outputItem{flush: result}, ctx.Done()) {
		// Shutdown drains every item admitted before it closed the queue.
		select {
		case <-c.output.done:
			return c.output.syncErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type outputItem struct {
	buf   *buffer.Buffer
	flush chan error // FIFO barrier for explicit Sync
}

type asyncOutput struct {
	sink   zapcore.WriteSyncer
	policy atomic.Pointer[outputPolicy]
	done   chan struct{}

	mu          sync.Mutex
	ready       *sync.Cond    // wakes the worker when a record arrives or output closes
	space       chan struct{} // allocated only while producers wait for space
	closed      bool
	slots       []outputItem // FIFO ring, shared by startup and configured loggers
	head, count int

	dropped     atomic.Uint64
	writeErrors atomic.Uint64
	syncErr     error // published by closing done
}

func newAsyncOutput(sink zapcore.WriteSyncer, policy outputPolicy) *asyncOutput {
	q := &asyncOutput{sink: sink, done: make(chan struct{}), slots: make([]outputItem, policy.capacity)}
	q.ready = sync.NewCond(&q.mu)
	q.policy.Store(&policy)
	go q.run()
	return q
}

// InitLogger may run after child loggers and queued records already exist.
// Keep their FIFO order on resize. When shrinking below the current depth,
// resume admission only after the backlog falls below the configured limit.
func (q *asyncOutput) configure(policy outputPolicy) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || *q.policy.Load() == policy {
		return
	}
	if policy.capacity != len(q.slots) {
		slots := make([]outputItem, max(policy.capacity, q.count))
		for i := range q.count {
			slots[i] = q.slots[(q.head+i)%len(q.slots)]
		}
		q.slots, q.head = slots, 0
	}
	q.policy.Store(&policy)
	if q.count < policy.capacity {
		q.notifySpaceLocked()
	}
}

func (q *asyncOutput) depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

func (q *asyncOutput) notifySpaceLocked() {
	if q.space != nil {
		close(q.space)
		q.space = nil
	}
}

func (q *asyncOutput) offer(item outputItem, deadline <-chan struct{}) bool {
	q.mu.Lock()
	for !q.closed && q.count >= q.policy.Load().capacity {
		if deadline == nil {
			q.mu.Unlock()
			return false
		}
		if q.space == nil {
			q.space = make(chan struct{})
		}
		space := q.space
		q.mu.Unlock()
		select {
		case <-space:
		case <-deadline:
			return false
		}
		q.mu.Lock()
	}
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.slots[(q.head+q.count)%len(q.slots)] = item
	q.count++
	if q.count == 1 {
		q.ready.Signal()
	}
	return true
}

func (q *asyncOutput) take() (outputItem, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.count == 0 && !q.closed {
		q.ready.Wait()
	}
	if q.count == 0 {
		return outputItem{}, false
	}
	item := q.slots[q.head]
	q.slots[q.head] = outputItem{}
	q.head = (q.head + 1) % len(q.slots)
	q.count--
	if q.count < q.policy.Load().capacity {
		q.notifySpaceLocked()
	}
	return item, true
}

func (q *asyncOutput) run() {
	defer close(q.done)
	for item, ok := q.take(); ok; item, ok = q.take() {
		if item.flush != nil {
			item.flush <- q.sink.Sync()
			continue
		}
		n, err := q.sink.Write(item.buf.Bytes())
		if err != nil || n != item.buf.Len() {
			q.writeErrors.Add(1)
		}
		item.buf.Free()
	}
	q.syncErr = q.sink.Sync()
}

func (q *asyncOutput) stop() {
	deadline := time.After(q.policy.Load().stopTimeout)
	q.mu.Lock()
	q.closed = true
	q.notifySpaceLocked()
	q.ready.Signal()
	q.mu.Unlock()
	select {
	case <-q.done:
	case <-deadline:
	}
}

// Shutdown drains previously accepted diagnostic logs up to the configured stopTimeout.
// The logger belongs to the process. An individual embedded client must not
// shut it down while other clients still use it. A blocked write can finish
// draining on the existing worker if the sink later recovers.
func Shutdown() { stdoutOutput.stop() }

// OutputStats describes the process-wide diagnostic log queue at scrape time.
type OutputStats struct {
	QueueDepth  int
	Dropped     uint64
	WriteErrors uint64
}

func GetOutputStats() OutputStats {
	return OutputStats{
		QueueDepth:  stdoutOutput.depth(),
		Dropped:     stdoutOutput.dropped.Load(),
		WriteErrors: stdoutOutput.writeErrors.Load(),
	}
}
