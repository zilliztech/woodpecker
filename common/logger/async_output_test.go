package logger

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SaturateDiagnosticOutputForTest installs the production logger with a blocked
// output and a full queue for the external WAL regression test.
func SaturateDiagnosticOutputForTest(t *testing.T) {
	t.Helper()
	prev := GetLevel()
	require.NoError(t, SetLevel("debug"))
	t.Cleanup(func() { _ = SetLevel(prev) })

	c, sink, _ := gatedCore(t, 1)
	l := newLogger("text", c.output)
	t.Cleanup(ReplaceGlobals(l))
	l.Debug("in flight")
	<-sink.entered
	l.Debug("fill the queue")
	l.Debug("queue is full")
	require.EqualValues(t, 1, c.output.dropped.Load())
}

type gatedLogSink struct {
	entered      chan struct{}
	release      chan struct{}
	once         sync.Once
	bytes.Buffer // accessed by the test only after Sync or worker exit
}

func (s *gatedLogSink) Write(p []byte) (int, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.Buffer.Write(p)
}

func gatedCore(t *testing.T, capacity int) (*asyncCore, *gatedLogSink, func()) {
	t.Helper()
	s := &gatedLogSink{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(s.release) })
	q := newAsyncOutput(zapcore.AddSync(s), capacity)
	t.Cleanup(func() { release(); q.stop() })
	return &asyncCore{LevelEnabler: zap.DebugLevel, encoder: NewTextEncoderByConfig("text"), output: q}, s, release
}

func TestAsyncOutputBackpressure(t *testing.T) {
	c, sink, release := gatedCore(t, 1)
	app := zap.New(c).With(zap.String("traceID", "trace-before-stall"))
	app.Info("in flight")
	<-sink.entered
	event := time.Date(2026, 11, 16, 14, 9, 27, 0, time.UTC)
	child := c.With([]zap.Field{zap.String("traceID", "queued-trace")})
	payload := []byte("before")
	require.NoError(t, child.Write(zapcore.Entry{Time: event, Level: zap.InfoLevel, Message: "queued"}, []zap.Field{zap.ByteString("value", payload)}))
	copy(payload, "after!")

	app.Debug("dropped debug")
	app.Info("dropped info")
	require.EqualValues(t, 2, c.output.dropped.Load())
	start := time.Now()
	app.Warn("timed out warning")
	require.GreaterOrEqual(t, time.Since(start), priorityWait)
	require.Less(t, time.Since(start), time.Second)
	require.EqualValues(t, 3, c.output.dropped.Load())
	require.Len(t, c.output.pending, 1)

	release()
	require.NoError(t, app.Sync())
	out := sink.String()
	require.Contains(t, out, "trace-before-stall")
	require.Contains(t, out, "2026/11/16 14:09:27.000")
	require.Contains(t, out, "queued-trace")
	require.Contains(t, out, "before")
	require.NotContains(t, out, "after!")
	require.NotContains(t, out, "dropped")
	require.NotContains(t, out, "timed out warning")
}

func TestAsyncOutputWaitingProducers(t *testing.T) {
	for _, scenario := range []string{"sink recovers", "logger stops"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, sink, release := gatedCore(t, 1)
				l := zap.New(c)
				l.Info("in flight")
				<-sink.entered
				l.Info("queued")
				var producers sync.WaitGroup
				for range 8 {
					producers.Go(func() { l.Error("waiting error") })
				}
				synctest.Wait() // all eight producers are waiting on the full queue
				require.Zero(t, c.output.dropped.Load())
				require.Len(t, c.output.pending, 1)
				if scenario == "logger stops" {
					go c.output.stop()
					producers.Wait()
					require.EqualValues(t, 8, c.output.dropped.Load())
				}
				release()
				producers.Wait()
				c.output.stop()
				if scenario == "sink recovers" {
					require.Zero(t, c.output.dropped.Load())
					require.Equal(t, 8, strings.Count(sink.String(), "waiting error"))
				}
			})
		})
	}
}

func TestAsyncOutputBlockedShutdown(t *testing.T) {
	c, sink, release := gatedCore(t, 2)
	app := zap.New(c)
	app.Info("in flight")
	<-sink.entered
	app.Info("drain after recovery")
	syncDone := make(chan error, 1)
	go func() { syncDone <- app.Sync() }()
	require.Eventually(t, func() bool { return len(c.output.pending) == 2 }, time.Second, time.Millisecond)

	stopped := make(chan struct{})
	go func() { c.output.stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * drainTimeout):
		t.Fatal("shutdown waited for the sink")
	}
	require.ErrorIs(t, <-syncDone, context.DeadlineExceeded)
	app.Error("after shutdown")
	require.EqualValues(t, 1, c.output.dropped.Load())
	release()
	select {
	case <-c.output.done:
	case <-time.After(time.Second):
		t.Fatal("worker did not drain and exit after sink recovered")
	}
	require.Contains(t, sink.String(), "drain after recovery")
	require.NotContains(t, sink.String(), "after shutdown")
}

type failingSink struct{ short bool }

func (s failingSink) Write(p []byte) (int, error) {
	if s.short {
		return len(p) - 1, nil
	}
	return 0, errors.New("unavailable")
}

func TestAsyncOutputWriteFailure(t *testing.T) {
	for _, short := range []bool{false, true} {
		q := newAsyncOutput(zapcore.AddSync(failingSink{short}), 4)
		l := newLogger("text", q)
		l.Info("sink failure")
		require.NoError(t, l.Sync())
		require.EqualValues(t, 1, q.writeErrors.Load())
		q.stop()
	}
}

func TestAsyncOutputSyncOnFullAndClosedQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, sink, release := gatedCore(t, 1)
		l := zap.New(c)
		l.Info("in flight")
		<-sink.entered
		l.Info("queued")
		require.ErrorIs(t, l.Sync(), context.DeadlineExceeded)
		c.output.stop()
		require.ErrorIs(t, l.Sync(), context.DeadlineExceeded)
		release()
		<-c.output.done
		require.NoError(t, l.Sync())
	})
}

func TestAsyncOutputFlushesBeforePanic(t *testing.T) {
	c, sink, release := gatedCore(t, 2)
	release()
	l := zap.New(c)
	require.Panics(t, func() { l.Panic("terminal diagnostic") })
	require.Contains(t, sink.String(), "terminal diagnostic")
}
