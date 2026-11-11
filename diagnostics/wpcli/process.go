package wpcli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"
)

type execution struct {
	Data       []byte
	ExitCode   int
	Overflow   bool
	StartError bool
}

type cappedWriter struct {
	buffer   bytes.Buffer
	limit    int64
	overflow *atomic.Bool
	cancel   context.CancelFunc
}

func (w *cappedWriter) Write(p []byte) (int, error) {
	n := len(p)
	remaining := w.limit - int64(w.buffer.Len())
	if int64(n) > remaining {
		p = p[:remaining]
		w.overflow.Store(true)
		w.cancel()
	}
	_, _ = w.buffer.Write(p)
	return n, nil
}

func execute(ctx context.Context, s Settings, args []string, limit int64) execution {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Executable is an explicit local trust/configuration choice; argv is built
	// from validated options without shell interpretation.
	cmd := exec.CommandContext(ctx, s.Executable, args...) // #nosec G204
	cmd.Env = []string{}
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		// Prevent ambient endpoint selection from overriding the named context.
		if key != "WOODPECKER_ENDPOINT" && key != "WOODPECKER_CLI_CONFIG" {
			cmd.Env = append(cmd.Env, value)
		}
	}
	if s.CLIConfig != "" {
		cmd.Env = append(cmd.Env, "WOODPECKER_CLI_CONFIG="+s.CLIConfig)
	}
	var overflow atomic.Bool
	out := &cappedWriter{limit: limit, overflow: &overflow, cancel: cancel}
	errout := &cappedWriter{limit: 64 << 10, overflow: &overflow, cancel: cancel}
	cmd.Stdout, cmd.Stderr = out, errout
	cmd.Cancel = func() error {
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
	cmd.WaitDelay = 250 * time.Millisecond
	err := cmd.Run()
	code := 0
	startError := false
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			code = exit.ExitCode()
		} else {
			code = -1
			startError = cmd.Process == nil
		}
	}
	return execution{Data: out.buffer.Bytes(), ExitCode: code, Overflow: overflow.Load(), StartError: startError}
}
