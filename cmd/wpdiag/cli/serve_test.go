package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServeCommandConfigurationAndLocalBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wpdiag.yaml")
	require.NoError(t, os.WriteFile(path, []byte("schema_version: wpdiag.config/v1\nsources: {}\n"), 0o600))
	for _, tc := range []struct {
		args []string
		code int
		want string
	}{
		{[]string{"serve", "--help"}, 0, "--listen"},
		{[]string{"serve", "--config", path + "-missing"}, 2, "Cannot load wpdiag configuration"},
		{[]string{"serve", "--config", path, "--listen", "0.0.0.0:8787"}, 1, "loopback"},
		{[]string{"serve", "--config", path, "--listen", "127.0.0.1:0"}, 0, "WP diagnosis WebUI: http://127.0.0.1:"},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var out, errout bytes.Buffer
		code := Execute(ctx, tc.args, &out, &errout)
		require.Equal(t, tc.code, code)
		require.Contains(t, out.String()+errout.String(), tc.want)
	}
}
