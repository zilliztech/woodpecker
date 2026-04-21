package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func TestNodeRestart_StubExit10(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "restart", "node-1"})

	err := root.Execute()
	require.Error(t, err)
	require.Equal(t, 10, wperrors.ExitCodeFor(err))
	require.Contains(t, buf.String(), "intentionally not implemented")
}
