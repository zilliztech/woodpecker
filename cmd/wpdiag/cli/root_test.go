package cli

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetiredCommandsDoNotExecute(t *testing.T) {
	for _, args := range [][]string{{"milvus", "append"}, {"prometheus", "diagnose", "--scope", "milvus"}, {"trace", "diagnose"}, {"loki", "diagnose"}, {"run"}, {"serve"}, {"report", "analyze", "/does-not-exist"}} {
		var out, stderr bytes.Buffer
		require.Equal(t, 2, Execute(context.Background(), args, &out, &stderr))
		require.Empty(t, out.String())
		require.Contains(t, stderr.String(), "retired")
		require.Contains(t, stderr.String(), "wp diag")
	}
	var out, stderr bytes.Buffer
	require.Zero(t, Execute(context.Background(), []string{"--help"}, &out, &stderr))
	require.Contains(t, out.String(), "wp webui")
}
