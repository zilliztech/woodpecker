package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionCommand_Default(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"version"})

	require.NoError(t, root.Execute())
	// Default text output contains the version (whatever it is via ldflags, at least "dev").
	require.Contains(t, buf.String(), "wp version")
}

func TestVersionCommand_JSON(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"version", "-o", "json"})

	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), `"version"`)
	require.Contains(t, buf.String(), `"go_version"`)
}
