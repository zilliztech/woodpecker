// Package version exposes build info populated via -ldflags at build time.
package version

import (
	"fmt"
	"runtime"
)

// These are set via linker -X flags at build time. See Makefile.
var (
	Version   = "dev"     // semantic version, e.g. "v0.1.26"
	Commit    = "unknown" // git commit short sha
	BuildTime = "unknown" // RFC3339 timestamp
)

// BuildInfo carries the build information exposed by this package.
type BuildInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
	GoVersion string `json:"go_version"`
}

// Info returns a populated BuildInfo using the current package-level vars.
func Info() BuildInfo {
	return BuildInfo{
		Version:   Version,
		Commit:    Commit,
		BuildTime: BuildTime,
		GoVersion: runtime.Version(),
	}
}

// String renders BuildInfo in a single human-readable line.
func (b BuildInfo) String() string {
	return fmt.Sprintf("%s (commit=%s, built=%s, %s)",
		b.Version, b.Commit, b.BuildTime, b.GoVersion)
}
