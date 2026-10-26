package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
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
		{"text capabilities", []string{"capabilities"}, 0, "wp.write_stall"},
		{"bad catalog output", []string{"capabilities", "-o", "yaml"}, 2, "Invalid command"},
		{"bad report output", []string{"wpcli", "diagnose", "-o", "yaml"}, 2, "Invalid command"},
		{"bad timestamp", []string{"wpcli", "diagnose", "--from", "invalid"}, 2, "Invalid command"},
		{"submillisecond duration", []string{"wpcli", "diagnose", "--window", "1ns"}, 2, "Invalid command"},
		{"text unavailable source", []string{"loki", "diagnose", "--flow", "write"}, 3, "module_not_implemented"},
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

func TestCLIDisabledConnectionTextReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wpdiag.yaml")
	require.NoError(t, os.WriteFile(path, []byte("schema_version: wpdiag.config/v1\nsources:\n  wpcli:\n    connections:\n      local:\n        enabled: false\n        settings: {}\n"), 0o600))
	var out, errout bytes.Buffer
	code := Execute(context.Background(), []string{"wpcli", "diagnose", "--flow", "read", "--mode", "snapshot", "--connection", "local", "--config", path}, &out, &errout)
	require.Equal(t, 3, code)
	require.Contains(t, out.String(), "connection_disabled")
	require.Contains(t, out.String(), "stage=wal_read")
	require.Empty(t, errout.String())
}
