package logger

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

const (
	priorityWait = 100 * time.Millisecond
	drainTimeout = time.Second
)

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
	// Skip encoding on a saturated DEBUG/INFO path. Admission below also
	// handles another producer filling the last slot while we encode.
	if ent.Level < zapcore.WarnLevel && len(q.pending) == cap(q.pending) {
		q.dropped.Add(1)
		return nil
	}
	buf, err := c.encoder.EncodeEntry(ent, fields)
	if err != nil {
		return err
	}
	item := outputItem{buf: buf}
	queued := q.offer(item, nil)
	if !queued && ent.Level >= zapcore.WarnLevel {
		ctx, cancel := context.WithTimeout(context.Background(), priorityWait)
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
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
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
	sink        zapcore.WriteSyncer
	pending     chan outputItem
	closing     chan struct{}
	done        chan struct{}
	stopOnce    sync.Once
	admission   sync.RWMutex // protects sends against close(pending); never held across sink I/O
	dropped     atomic.Uint64
	writeErrors atomic.Uint64
	syncErr     error // published by closing done
}

func newAsyncOutput(sink zapcore.WriteSyncer, capacity int) *asyncOutput {
	q := &asyncOutput{
		sink: sink, pending: make(chan outputItem, capacity),
		closing: make(chan struct{}), done: make(chan struct{}),
	}
	go q.run()
	return q
}

func (q *asyncOutput) offer(item outputItem, deadline <-chan struct{}) bool {
	q.admission.RLock()
	defer q.admission.RUnlock()
	select {
	case <-q.closing:
		return false
	default:
	}
	if deadline == nil {
		select {
		case q.pending <- item:
			return true
		default:
			return false
		}
	}
	select {
	case q.pending <- item:
		return true
	case <-q.closing:
	case <-deadline:
	}
	return false
}

func (q *asyncOutput) run() {
	defer close(q.done)
	for item := range q.pending {
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
	deadline := time.After(drainTimeout)
	q.stopOnce.Do(func() {
		close(q.closing) // wake producers waiting for space
		q.admission.Lock()
		close(q.pending)
		q.admission.Unlock()
	})
	select {
	case <-q.done:
	case <-deadline:
	}
}

// Shutdown drains previously accepted diagnostic logs for up to one second.
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
		QueueDepth:  len(stdoutOutput.pending),
		Dropped:     stdoutOutput.dropped.Load(),
		WriteErrors: stdoutOutput.writeErrors.Load(),
	}
}
