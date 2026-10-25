package wpcli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProcessHelper(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "--" {
		return
	}
	switch os.Args[len(os.Args)-1] {
	case "environment":
		fmt.Print(os.Getenv("WOODPECKER_ENDPOINT") + "|" + os.Getenv("WOODPECKER_CLI_CONFIG"))
		os.Exit(0)
	case "overflow":
		fmt.Print(strings.Repeat("x", 100000))
		os.Exit(0)
	case "stderr":
		fmt.Fprint(os.Stderr, strings.Repeat("s", 100000))
		os.Exit(0)
	case "wait":
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "partial":
		fmt.Print(`{"retained":true}`)
		os.Exit(6)
	}
}

func TestSubprocessBoundsEnvironmentAndCancellation(t *testing.T) {
	t.Setenv("WOODPECKER_ENDPOINT", "http://sentinel-secret")
	t.Setenv("WOODPECKER_CLI_CONFIG", "ambient-secret")
	s := Settings{Executable: os.Args[0], CLIConfig: "explicit-cli.yaml"}
	run := func(ctx context.Context, mode string) execution {
		return execute(ctx, s, []string{"-test.run=^TestProcessHelper$", "--", mode}, 4096)
	}
	result := run(context.Background(), "environment")
	require.Equal(t, 0, result.ExitCode)
	require.Equal(t, "|explicit-cli.yaml", string(result.Data))
	for _, mode := range []string{"overflow", "stderr"} {
		result = run(context.Background(), mode)
		require.True(t, result.Overflow)
		require.LessOrEqual(t, len(result.Data), 4096)
	}
	result = run(context.Background(), "partial")
	require.Equal(t, 6, result.ExitCode)
	require.JSONEq(t, `{"retained":true}`, string(result.Data))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	result = run(ctx, "wait")
	require.Less(t, time.Since(start), 2*time.Second)
	require.NotEqual(t, 0, result.ExitCode)
	result = execute(context.Background(), Settings{Executable: "/does/not/exist"}, nil, 1024)
	require.True(t, result.StartError)
	require.Empty(t, result.Data)
}
