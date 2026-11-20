package logger

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/zilliztech/woodpecker/common/config"
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
	q := newAsyncOutput(zapcore.AddSync(s), resolveOutputPolicy(config.AsyncWriteConfig{PendingLength: capacity}))
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
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
	require.Less(t, time.Since(start), time.Second)
	require.EqualValues(t, 3, c.output.dropped.Load())
	require.Equal(t, 1, c.output.depth())

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
				require.Equal(t, 1, c.output.depth())
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
	require.Eventually(t, func() bool { return c.output.depth() == 2 }, time.Second, time.Millisecond)

	stopped := make(chan struct{})
	go func() { c.output.stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
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
		q := newAsyncOutput(zapcore.AddSync(failingSink{short}), resolveOutputPolicy(config.AsyncWriteConfig{PendingLength: 4}))
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

func TestAsyncOutputAdmissionDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, sink, release := gatedCore(t, 1)
		l := zap.New(c)
		l.Info("in flight")
		<-sink.entered
		l.Info("queued")
		checkWait := func(level zapcore.Level, want time.Duration) {
			t.Helper()
			start := time.Now()
			before := c.output.dropped.Load()
			require.NoError(t, c.Write(zapcore.Entry{Level: level, Message: "overflow"}, nil))
			require.Equal(t, want, time.Since(start))
			require.Equal(t, before+1, c.output.dropped.Load())
		}
		for _, level := range []zapcore.Level{zap.DebugLevel, zap.InfoLevel, zap.WarnLevel} {
			checkWait(level, 100*time.Millisecond)
		}
		for _, level := range []zapcore.Level{zap.ErrorLevel, zap.DPanicLevel, zap.PanicLevel, zap.FatalLevel} {
			checkWait(level, time.Second)
		}
		zero := time.Duration(0)
		cfg := config.AsyncWriteConfig{PendingLength: 1, NonDroppableLevel: "warn", DroppedTimeout: &zero, StopTimeout: 30 * time.Millisecond}
		c.output.configure(resolveOutputPolicy(cfg))
		checkWait(zap.InfoLevel, 0)
		checkWait(zap.WarnLevel, 30*time.Millisecond)
		longer := 60 * time.Millisecond
		cfg.DroppedTimeout = &longer
		c.output.configure(resolveOutputPolicy(cfg))
		checkWait(zap.ErrorLevel, longer)
		release()
	})
}

func TestAsyncOutputPolicyDefaults(t *testing.T) {
	negative := -time.Second
	fallback := resolveOutputPolicy(config.AsyncWriteConfig{
		PendingLength: -1, DroppedTimeout: &negative, NonDroppableLevel: "unknown", StopTimeout: negative,
	})
	require.Equal(t, resolveOutputPolicy(config.AsyncWriteConfig{}), fallback)
	require.Equal(t, 1024, fallback.capacity)
	require.Equal(t, zapcore.ErrorLevel, fallback.nonDroppableLevel)
}

func TestAsyncOutputResizeWhileBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, sink, release := gatedCore(t, 3)
		l := zap.New(c).With(zap.String("scope", "startup"))
		l.Info("in flight")
		<-sink.entered
		for _, msg := range []string{"first", "second", "third"} {
			l.Info(msg)
		}
		zero := time.Duration(0)
		cfg := config.AsyncWriteConfig{PendingLength: 1, DroppedTimeout: &zero}
		c.output.configure(resolveOutputPolicy(cfg))
		l.Info("drop on shrink")
		require.Equal(t, 3, c.output.depth())
		require.EqualValues(t, 1, c.output.dropped.Load())
		waiting := make(chan struct{})
		go func() { l.Error("fourth"); close(waiting) }()
		synctest.Wait()
		cfg.PendingLength = 4
		c.output.configure(resolveOutputPolicy(cfg))
		<-waiting
		require.Equal(t, 4, c.output.depth())
		release()
		require.NoError(t, l.Sync())
		lines := strings.Split(strings.TrimSpace(sink.String()), "\n")
		want := []string{"in flight", "first", "second", "third", "fourth"}
		require.Len(t, lines, len(want))
		for i, msg := range want {
			require.Contains(t, lines[i], msg)
			require.Contains(t, lines[i], "scope=startup")
		}
	})
}

func TestInitLoggerAsyncWriteFromYAML(t *testing.T) {
	// InitLogger is process-wide. Start with the real package defaults and
	// capture a derived logger before the first configuration is applied.
	const key = "WP_TEST_ASYNC_LOG_CONFIG"
	path := os.Getenv(key)
	if path == "" {
		path = filepath.Join(t.TempDir(), "woodpecker.yaml")
		require.NoError(t, os.WriteFile(path, []byte("log:\n  asyncWrite:\n    pendingLength: 7\n    droppedTimeout: 12ms\n    nonDroppableLevel: warn\n    stopTimeout: 250ms\n"), 0o600))
		exe, err := os.Executable()
		require.NoError(t, err)
		cmd := exec.Command(exe, "-test.run=^TestInitLoggerAsyncWriteFromYAML$")
		cmd.Env = append(os.Environ(), key+"="+path)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		return
	}
	c := Ctx(WithFields(context.Background(), zap.String("scope", "early"))).Core().(*asyncCore)
	cfg, err := config.NewConfiguration(path)
	require.NoError(t, err)
	InitLogger(cfg)
	// A second embedded client cannot replace the process's established policy.
	InitLogger(&config.Configuration{})
	p := c.output.policy.Load()
	require.Equal(t, 7, p.capacity)
	require.Equal(t, 12*time.Millisecond, p.enqueueWait(zap.InfoLevel))
	require.Equal(t, 250*time.Millisecond, p.enqueueWait(zap.WarnLevel))
}
