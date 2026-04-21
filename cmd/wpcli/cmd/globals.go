package cmd

import "time"

// GlobalFlags holds the values of wp's persistent flags after parsing.
// Populated by the root PersistentPreRun hook; consumed by sub-commands.
type GlobalFlags struct {
	Context     string
	Endpoint    string
	AdminPort   int
	Timeout     time.Duration
	Concurrency int
	Strict      bool
	Output      string
	NoColor     bool
	Verbose     int
}

// Globals is the singleton populated per-invocation. Sub-commands read from it.
var Globals GlobalFlags
