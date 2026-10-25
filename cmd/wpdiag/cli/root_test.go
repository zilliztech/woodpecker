package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func TestCLIIndependentCapabilitiesAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		exit     int
		contains string
	}{
		{"help", []string{"--help"}, 0, "capabilities"},
		{"capabilities", []string{"capabilities", "-o", "json"}, 0, d.CatalogSchema},
		{"planned source", []string{"prometheus", "diagnose", "--scope", "e2e", "--flow", "read", "--stage", "query", "-o", "json"}, 3, "module_not_implemented"},
		{"invalid stage", []string{"wpcli", "diagnose", "--flow", "write", "--stage", "wal_read", "-o", "json"}, 2, "invalid_selection"},
		{"historical is unsupported", []string{"wpcli", "diagnose", "--flow", "write", "--mode", "historical", "--from", "2026-10-01T00:00:00Z", "--to", "2026-10-01T01:00:00Z", "-o", "json"}, 3, "profile_not_supported"},
		{"missing config", []string{"wpcli", "diagnose", "--flow", "write", "--mode", "snapshot", "--connection", "local", "--config", filepath.Join(t.TempDir(), "missing.yaml"), "-o", "json"}, 2, "configuration_unavailable"},
		{"missing flow", []string{"wpcli", "diagnose", "--mode", "snapshot", "-o", "json"}, 2, "invalid_selection"},
		{"bad duration", []string{"wpcli", "diagnose", "--flow", "read", "--window", "0s", "-o", "json"}, 2, "invalid_selection"},
		{"unknown option", []string{"--sentinel-secret"}, 2, "Invalid command"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, errout bytes.Buffer
			code := Execute(context.Background(), tc.args, &out, &errout)
			require.Equal(t, tc.exit, code)
			require.Contains(t, out.String()+errout.String(), tc.contains)
			require.NotContains(t, out.String()+errout.String(), "sentinel-secret")
			if json.Valid(out.Bytes()) && tc.name != "capabilities" {
				var report d.Report
				require.NoError(t, json.Unmarshal(out.Bytes(), &report))
				require.Equal(t, d.ReportSchema, report.SchemaVersion)
				require.NotEqual(t, "completed", report.Status)
			}
		})
	}
}
