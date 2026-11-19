package main

import (
	"bytes"
	"flag"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Startup errors must return control to main so it can drain the diagnostic
// logger before exiting with the original non-zero status.
func TestServerStartupErrorsReturnToMain(t *testing.T) {
	originalArgs, originalFlags, originalOutput := os.Args, flag.CommandLine, log.Writer()
	t.Cleanup(func() {
		os.Args, flag.CommandLine = originalArgs, originalFlags
		log.SetOutput(originalOutput)
	})
	badConfig := filepath.Join(t.TempDir(), "bad.yaml")
	require.NoError(t, os.WriteFile(badConfig, []byte("bad: ["), 0o600))
	for _, tc := range []struct {
		args    []string
		message string
	}{
		{nil, "Usage:"},
		{[]string{"unknown"}, "Unknown command"},
		{[]string{"server", "-config", badConfig}, "Failed to load configuration"},
		{[]string{"server", "-config", "../config/woodpecker.yaml", "-external-user-config", badConfig}, "Failed to load external user configuration"},
		{[]string{"server", "-config", "../config/woodpecker.yaml", "-advertise-gossip-addr", "missing-port"}, "Failed to parse advertise-gossip-addr"},
		{[]string{"server", "-config", "../config/woodpecker.yaml", "-advertise-service-addr", "missing-port"}, "Failed to parse advertise-service-addr"},
	} {
		t.Run(tc.message, func(t *testing.T) {
			flag.CommandLine = flag.NewFlagSet("server", flag.ContinueOnError)
			os.Args = append([]string{"woodpecker"}, tc.args...)
			var output bytes.Buffer
			log.SetOutput(&output)
			require.Equal(t, 1, run())
			require.Contains(t, output.String(), tc.message)
		})
	}
}
