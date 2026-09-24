package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoad_GoodFile(t *testing.T) {
	cfg, err := Load("testdata/good.yaml")
	require.NoError(t, err)
	require.Equal(t, "prod", cfg.CurrentContext)
	require.Len(t, cfg.Contexts, 2)
	require.Equal(t, "http://prod.wp.svc:9091", cfg.Contexts["prod"].Endpoint)
	require.Equal(t, "table", cfg.Defaults.Output)
}

func TestLoad_BadYAML(t *testing.T) {
	_, err := Load("testdata/bad-yaml.yaml")
	require.Error(t, err)
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("testdata/does-not-exist.yaml")
	require.Error(t, err)
}

func TestResolveContext_Explicit(t *testing.T) {
	cfg, _ := Load("testdata/good.yaml")
	ctx, err := cfg.ResolveContext("dev")
	require.NoError(t, err)
	require.Equal(t, "http://localhost:9091", ctx.Endpoint)
}

func TestResolveContext_Current(t *testing.T) {
	cfg, _ := Load("testdata/good.yaml")
	ctx, err := cfg.ResolveContext("")
	require.NoError(t, err)
	require.Equal(t, "http://prod.wp.svc:9091", ctx.Endpoint)
}

func TestResolveContext_MissingName(t *testing.T) {
	cfg, _ := Load("testdata/missing-context.yaml")
	_, err := cfg.ResolveContext("")
	require.Error(t, err)
}

func TestResolveContext_Defaults(t *testing.T) {
	cfg, _ := Load("testdata/good.yaml")
	ctx, _ := cfg.ResolveContext("dev")
	require.Equal(t, 9091, ctx.AdminPort)
	require.Equal(t, 30*time.Second, ctx.Timeout)
}

// TestDefaultConfigPaths_IncludesTheBinaryDirectory covers the arrangement a fixed cluster wants:
// a directory holding wp and its cli.yaml, with endpoint, admin port and strict already set, that
// hands over to a colleague as one folder. Today the config has to live in the invoking user's
// home instead.
//
// It is deliberately the binary's directory and not the working directory. Adopting a cli.yaml
// because of where the shell happens to be would let any directory someone else wrote point this
// tool — which can fence segments and delete instance data — at a cluster the operator did not
// choose.
func TestDefaultConfigPaths_IncludesTheBinaryDirectory(t *testing.T) {
	t.Setenv("WOODPECKER_CLI_CONFIG", "")

	exe, err := os.Executable()
	require.NoError(t, err)
	// Symlinks are resolved on purpose: the server images ship /usr/local/bin/wp as a link to
	// /woodpecker/bin/wp, and the config belongs beside the real binary.
	if resolved, linkErr := filepath.EvalSymlinks(exe); linkErr == nil {
		exe = resolved
	}
	wantDir := filepath.Dir(exe)

	paths := DefaultConfigPaths()

	var found int
	for _, p := range paths {
		if filepath.Dir(p) == wantDir && filepath.Base(p) == "cli.yaml" {
			found++
		}
	}
	require.Equal(t, 1, found, "the binary's own directory should be searched, got %v", paths)

	// Ahead of the personal locations: running that copy of wp is a more specific statement of
	// intent than a global config.
	idxBinary, idxHome := -1, -1
	home, _ := os.UserHomeDir()
	for i, p := range paths {
		if filepath.Dir(p) == wantDir && idxBinary < 0 {
			idxBinary = i
		}
		if home != "" && strings.HasPrefix(p, home) && idxHome < 0 {
			idxHome = i
		}
	}
	if idxHome >= 0 {
		require.Less(t, idxBinary, idxHome, "the binary's directory should win over the home config")
	}
}
