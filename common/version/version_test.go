package version

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInfo_DefaultValues(t *testing.T) {
	info := Info()
	// Default values (no ldflags) should still be non-empty / sensible
	require.NotEmpty(t, info.Version)
	require.NotEmpty(t, info.GoVersion)
}

func TestString_Format(t *testing.T) {
	Version = "v0.1.26-test"
	Commit = "abc1234"
	BuildTime = "2026-04-09T12:00:00Z"
	defer func() {
		Version = "dev"
		Commit = "unknown"
		BuildTime = "unknown"
	}()

	info := Info()
	s := info.String()
	require.Contains(t, s, "v0.1.26-test")
	require.Contains(t, s, "abc1234")
	require.Contains(t, s, "2026-04-09T12:00:00Z")
}
