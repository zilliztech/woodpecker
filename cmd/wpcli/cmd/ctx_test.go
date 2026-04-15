package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeTempConfig creates a cli.yaml in a temp dir and returns its path.
func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "cli.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

const goodYAML = `current-context: prod
contexts:
  prod:
    endpoint: http://prod.wp.svc:9091
  dev:
    endpoint: http://localhost:9091
defaults:
  output: table
`

func TestCtxList(t *testing.T) {
	p := writeTempConfig(t, goodYAML)
	t.Setenv("WOODPECKER_CLI_CONFIG", p)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"ctx", "list"})

	require.NoError(t, root.Execute())
	out := buf.String()
	require.Contains(t, out, "prod")
	require.Contains(t, out, "dev")
	require.Contains(t, out, "*") // active marker on prod
}

func TestCtxView(t *testing.T) {
	p := writeTempConfig(t, goodYAML)
	t.Setenv("WOODPECKER_CLI_CONFIG", p)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"ctx", "view"})

	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "http://prod.wp.svc:9091")
}

func TestCtxUse(t *testing.T) {
	p := writeTempConfig(t, goodYAML)
	t.Setenv("WOODPECKER_CLI_CONFIG", p)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"ctx", "use", "dev"})

	require.NoError(t, root.Execute())

	// File should now have current-context: dev
	data, _ := os.ReadFile(p)
	require.Contains(t, string(data), "current-context: dev")
}

func TestCtxUse_NonexistentContext(t *testing.T) {
	p := writeTempConfig(t, goodYAML)
	t.Setenv("WOODPECKER_CLI_CONFIG", p)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"ctx", "use", "nosuch"})

	err := root.Execute()
	require.Error(t, err)
}
