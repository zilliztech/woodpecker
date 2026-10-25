package diagnostics_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func TestLoadConfigLimitsAndStrictEnvelope(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wpdiag.yaml")
	body := "schema_version: wpdiag.config/v1\nsources:\n  wpcli:\n    connections:\n      local:\n        settings: {context: fixture, executable: wp}\n"
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	c, err := d.LoadConfig(path)
	require.NoError(t, err)
	require.Equal(t, dir, c.Sources["wpcli"].Connections["local"].BaseDir)
	for _, content := range []string{
		strings.Repeat("x", (1<<20)+1),
		strings.Replace(body, "wpcli:", "unknown:", 1),
		strings.Replace(body, "local:", "'':", 1),
		strings.Replace(body, "local:", strings.Repeat("a", 129)+":", 1),
		strings.Replace(body, "{context: fixture, executable: wp}", "sentinel-secret", 1),
		body + "unknown_field: sentinel-secret\n",
	} {
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		c, err = d.LoadConfig(path)
		require.Error(t, err)
		require.Empty(t, c.Sources)
		require.NotContains(t, err.Error(), "sentinel-secret")
	}
	_, err = d.LoadConfig(dir)
	require.Error(t, err)
}
