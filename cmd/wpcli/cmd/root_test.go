package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRootCommand_Flags(t *testing.T) {
	root := NewRootCommand()

	// Every global flag the spec §3.3 declares must be present.
	expected := []string{
		"context", "endpoint", "admin-port", "timeout",
		"concurrency", "strict", "output", "no-color", "verbose",
	}
	for _, name := range expected {
		flag := root.PersistentFlags().Lookup(name)
		require.NotNilf(t, flag, "expected persistent flag %q", name)
	}
}

func TestRootCommand_DefaultOutput(t *testing.T) {
	root := NewRootCommand()
	out, err := root.PersistentFlags().GetString("output")
	require.NoError(t, err)
	require.Equal(t, "table", out)
}

func TestRootCommand_Help(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"--help"})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "wp")
}
