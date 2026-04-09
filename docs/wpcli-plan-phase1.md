# wp CLI Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship Phase 1 of the wp CLI — a usable `wp` Go binary covering 14 feature commands (node lifecycle, cluster overview, config/env inspection, pprof) backed by 3 new admin endpoints + 2 existing endpoint enhancements. Zero business code metric migration, zero opregistry, zero k8s integration (those are Phase 2/3).

**Architecture:** Independent Go binary under `cmd/wpcli/` using `cobra`. Loads contexts from `~/.woodpecker/cli.yaml`, talks to server admin HTTP endpoints (default `:9091`), discovery via `/admin/memberlist`, fan-out for multi-node commands with strict / non-strict modes. No changes to `server/storage/`, no changes to metric call sites.

**Tech Stack:**
- Go 1.24.2 (existing project version)
- `github.com/spf13/cobra` (new dep — must add)
- `gopkg.in/yaml.v3 v3.0.1` (already in go.mod)
- `github.com/stretchr/testify v1.11.1` (already in go.mod)
- `text/tabwriter` (stdlib)
- `encoding/json`, `encoding/yaml` (stdlib + yaml.v3)

**Reference documents:**
- Design spec: [`docs/wpcli-design.md`](./wpcli-design.md) — every task below references concrete sections of the spec
- Existing pattern for integration tests: `tests/docker/monitor/` (not yet mirrored in Phase 1; that waits for Phase 2)

**Phase 1 scope locked (from spec §5.3):**
- 14 feature commands + 4 CLI infrastructure commands (`wp ctx` × 3, `wp version`)
- 3 new endpoints: `POST /admin/node/decommission/cancel`, `GET /admin/config`, `GET /admin/env`
- 2 existing endpoint enhancements: `/admin/memberlist` JSON mode, `NodeStatus` field augmentation
- 1 new server method: `NodeLifecycleManager.CancelDecommission()` strict version
- `common/version/` new package + Makefile ldflags
- `cmd/wpcli/` complete skeleton

**Out of scope for Phase 1 (explicit):**
- opregistry package, metrics op abstraction, gRPC interceptor
- Any business code metric migration (~12 call sites → Phase 2)
- `tests/docker/wpcli/` docker-compose E2E suite → Phase 2
- C / D / G families → Phase 2
- F family (k8s hybrid) → Phase 3
- `logging set/get-level` → Phase 2

---

## Phase 1 Task Inventory

33 tasks, grouped by dependency layer. Each task is self-contained and ends with a commit.

| # | Task | Layer | Depends on |
|---|---|---|---|
| 1 | `common/version/` package + ldflags | Foundation | — |
| 2 | `cmd/wpcli/` scaffold + `make wpcli` target | Foundation | 1 |
| 3 | cobra root command + global flags | Foundation | 2 |
| 4 | Exit code / error system | Foundation | 2 |
| 5 | CLI config loading (cli.yaml) | Foundation | 3, 4 |
| 6 | `wp version` command | Foundation | 3 |
| 7 | `wp ctx list / use / view` commands | Foundation | 5 |
| 8 | Output renderer: JSON + YAML | Output | 2 |
| 9 | Output renderer: table (row + sectioned) | Output | 8 |
| 10 | Output renderer: tree | Output | 9 |
| 11 | Admin HTTP client + memberlist JSON parser | HTTP client | 2 |
| 12 | Fan-out client (strict / non-strict) | HTTP client | 11 |
| 13 | Server: `GetMemberlistJSON` method | Server — enhance | — |
| 14 | Server: `/admin/memberlist` JSON mode | Server — enhance | 13 |
| 15 | Server: `NodeStatus` field augmentation | Server — enhance | — |
| 16 | Server: `NodeLifecycleManager.CancelDecommission()` | Server — new | — |
| 17 | Server: `POST /admin/node/decommission/cancel` handler | Server — new | 16 |
| 18 | Server: `GET /admin/config` handler | Server — new | — |
| 19 | Server: `GET /admin/env` handler | Server — new | — |
| 20 | `wp node list` | Commands A | 9, 12, 14, 15 |
| 21 | `wp node show` | Commands A | 9, 11, 15 |
| 22 | `wp node decommission` (with wait + heartbeat) | Commands A | 11 |
| 23 | `wp node drain-status` (with --watch) | Commands A | 11 |
| 24 | `wp node cancel-decommission` | Commands A | 11, 17 |
| 25 | `wp node restart` (stub) | Commands A | 3 |
| 26 | `wp cluster info` (overview + tree) | Commands B | 10, 12, 14 |
| 27 | `wp cluster health` (replica-aware) | Commands B | 12 |
| 28 | `wp cluster gossip-diff` | Commands B | 12 |
| 29 | `wp config show` + `config diff` | Commands E | 11, 12, 18 |
| 30 | `wp env show` + `env diff` | Commands E | 11, 12, 19 |
| 31 | `wp profile` | Commands E | 11 |
| 32 | `cmd/wpcli/README.md` (Phase 1 quickstart) | Docs | 20-31 |
| 33 | Phase 1 verification sweep | Verification | all |

**Estimated commits:** ~33 (one per task) + occasional intermediate refactor commits.

---

## Part A — Foundation (Tasks 1-7)

### Task 1: `common/version/` package + ldflags

**Goal:** Give both server and `wp` binary a single source of truth for version info (version string, git commit hash, build time), populated via linker `-ldflags` at build time.

**Files:**
- Create: `common/version/version.go`
- Create: `common/version/version_test.go`
- Modify: `Makefile` — add `VERSION_PKG` variable + ldflags to existing server build target

- [ ] **Step 1: Write the test**

Create `common/version/version_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./common/version/...
```

Expected: FAIL — `common/version` package does not exist.

- [ ] **Step 3: Create `common/version/version.go`**

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
go test ./common/version/... -v
```

Expected: PASS on both tests.

- [ ] **Step 5: Add Makefile ldflags variable**

Modify `Makefile` — at the top, near other variables, add:

```makefile
VERSION_PKG := github.com/zilliztech/woodpecker/common/version
VERSION     := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT      := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME  := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     := -ldflags "-s -w \
                -X '$(VERSION_PKG).Version=$(VERSION)' \
                -X '$(VERSION_PKG).Commit=$(COMMIT)' \
                -X '$(VERSION_PKG).BuildTime=$(BUILD_TIME)'"
```

Leave existing server build target for now (we'll wire ldflags into wpcli in Task 2). If the existing server target uses its own flags, do NOT modify them in this task — scope is just introducing the variables.

- [ ] **Step 6: Verify Makefile parses**

```bash
make -n 2>&1 | head -5    # dry-run, no errors expected
```

- [ ] **Step 7: Commit**

```bash
git add common/version/ Makefile
git commit -m "feat(version): add common/version package with build info ldflags"
```

---

### Task 2: `cmd/wpcli/` scaffold + `make wpcli` target

**Goal:** Create the minimal compilable `cmd/wpcli/` skeleton so subsequent tasks have something to land on, and add `make wpcli` target.

**Files:**
- Create: `cmd/wpcli/README.md` (1-line placeholder)
- Create: `cmd/wpcli/main.go`
- Create: `cmd/wpcli/cmd/root.go` (minimal stub; full implementation in Task 3)
- Modify: `Makefile` — add `wpcli` target
- Modify: `go.mod` + `go.sum` (via `go get github.com/spf13/cobra`)

- [ ] **Step 1: Add cobra dependency**

```bash
go get github.com/spf13/cobra@latest
go mod tidy
```

Expected: `go.mod` picks up `github.com/spf13/cobra` and `github.com/spf13/pflag`.

- [ ] **Step 2: Create `cmd/wpcli/README.md`** (placeholder, Task 32 expands it)

```markdown
# wp — Woodpecker CLI

WIP. See [`docs/wpcli-design.md`](../../docs/wpcli-design.md) for the full design.

Build: `make wpcli` (outputs `bin/wp`).
```

- [ ] **Step 3: Create minimal `cmd/wpcli/cmd/root.go`**

```go
// Package cmd holds the cobra commands for the wp CLI.
package cmd

import "github.com/spf13/cobra"

// NewRootCommand creates the root cobra command for wp.
// Full flag wiring and sub-commands are added in later tasks.
func NewRootCommand() *cobra.Command {
	return &cobra.Command{
		Use:          "wp",
		Short:        "Woodpecker operational CLI",
		SilenceUsage: true,
	}
}
```

- [ ] **Step 4: Create `cmd/wpcli/main.go`**

```go
package main

import (
	"fmt"
	"os"

	"github.com/zilliztech/woodpecker/cmd/wpcli/cmd"
)

func main() {
	root := cmd.NewRootCommand()
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "wp:", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 5: Add `make wpcli` target**

Modify `Makefile`, add:

```makefile
.PHONY: wpcli
wpcli:
	@mkdir -p bin
	CGO_ENABLED=0 go build $(LDFLAGS) -o bin/wp ./cmd/wpcli
	@echo "Built bin/wp"
```

- [ ] **Step 6: Build and run**

```bash
make wpcli
./bin/wp --help
```

Expected output: cobra's default help for `wp`. Exit code 0.

- [ ] **Step 7: Commit**

```bash
git add cmd/wpcli/ Makefile go.mod go.sum
git commit -m "feat(wpcli): scaffold wp binary with cobra + make wpcli target"
```

---

### Task 3: cobra root command + global flags

**Goal:** Add all global flags from spec §3.3 to the root command. Flags are parsed but not yet consumed — consumption happens in later tasks (config resolution in Task 5, HTTP client in Task 11, etc.).

**Files:**
- Modify: `cmd/wpcli/cmd/root.go`
- Create: `cmd/wpcli/cmd/root_test.go`
- Create: `cmd/wpcli/cmd/globals.go`

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/root_test.go`:

```go
package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRootCommand_Flags(t *testing.T) {
	root := NewRootCommand()

	// Every global flag the spec §3.3 declares must be present.
	expected := []string{
		"context", "endpoint", "admin-port", "timeout",
		"concurrency", "strict", "output", "no-color", "verbose",
	}
	for _, name := range expected {
		flag := root.PersistentFlags().Lookup(name)
		require.NotNilf(t, flag, "expected persistent flag %q", name)
	}
}

func TestRootCommand_DefaultOutput(t *testing.T) {
	root := NewRootCommand()
	out, err := root.PersistentFlags().GetString("output")
	require.NoError(t, err)
	require.Equal(t, "table", out)
}

func TestRootCommand_Help(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"--help"})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "wp")
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/cmd/... -run TestRootCommand -v
```

Expected: FAIL (flags not yet defined).

- [ ] **Step 3: Create `cmd/wpcli/cmd/globals.go`**

```go
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
```

- [ ] **Step 4: Update `cmd/wpcli/cmd/root.go`** to define all global flags

```go
// Package cmd holds the cobra commands for the wp CLI.
package cmd

import (
	"time"

	"github.com/spf13/cobra"
)

// NewRootCommand creates the root cobra command for wp with all global flags.
func NewRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:          "wp",
		Short:        "Woodpecker operational CLI",
		Long:         "wp is the Woodpecker operational CLI for service-mode clusters.",
		SilenceUsage: true,
	}

	pf := root.PersistentFlags()
	pf.StringVar(&Globals.Context, "context", "", "CLI context name (overrides current-context in cli.yaml)")
	pf.StringVar(&Globals.Endpoint, "endpoint", "", "Admin HTTP seed endpoint (e.g. http://node:9091)")
	pf.IntVar(&Globals.AdminPort, "admin-port", 9091, "Admin port for fan-out peer discovery")
	pf.DurationVar(&Globals.Timeout, "timeout", 30*time.Second, "Per-request timeout")
	pf.IntVar(&Globals.Concurrency, "concurrency", 8, "Fan-out concurrency")
	pf.BoolVar(&Globals.Strict, "strict", false, "Treat partial fan-out failures as errors")
	pf.StringVarP(&Globals.Output, "output", "o", "table", "Output format: table|wide|json|yaml")
	pf.BoolVar(&Globals.NoColor, "no-color", false, "Disable color in output")
	pf.CountVarP(&Globals.Verbose, "verbose", "v", "Increase verbosity (-v, -vv, -vvv)")

	return root
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./cmd/wpcli/cmd/... -run TestRootCommand -v
```

Expected: PASS on all three.

- [ ] **Step 6: Build to ensure no regressions**

```bash
make wpcli && ./bin/wp --help
```

Expected: help output includes the 9 global flags.

- [ ] **Step 7: Commit**

```bash
git add cmd/wpcli/cmd/root.go cmd/wpcli/cmd/root_test.go cmd/wpcli/cmd/globals.go
git commit -m "feat(wpcli): add global flags on cobra root command"
```

---

### Task 4: Exit code / error system

**Goal:** Centralize exit code mapping per spec §3.5 (14 codes + 130 SIGINT + 100-200 kubectl passthrough). Every sub-command will return typed errors that the root converts to exit codes.

**Files:**
- Create: `cmd/wpcli/internal/errors/types.go`
- Create: `cmd/wpcli/internal/errors/exit.go`
- Create: `cmd/wpcli/internal/errors/exit_test.go`
- Modify: `cmd/wpcli/main.go` — wire the exit mapping

- [ ] **Step 1: Write the test for the exit-code table**

Create `cmd/wpcli/internal/errors/exit_test.go`:

```go
package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExitCodeFor_AllTypes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, 0},
		{"usage", NewUsageError("bad flag"), 2},
		{"network", NewNetworkError("seed unreachable"), 1},
		{"target_not_found", NewTargetNotFoundError("node-99"), 3},
		{"state_conflict", NewStateConflictError("already decommissioned"), 4},
		{"wait_timeout", NewWaitTimeoutError("decommission", 30), 5},
		{"strict_partial", NewStrictPartialFailureError(1, 5), 6},
		{"user_abort", NewUserAbortError(), 7},
		{"yellow", NewYellowFindingError("health warning"), 8},
		{"red", NewRedFindingError("health critical"), 9},
		{"not_impl", NewNotImplementedError("wp node restart"), 10},
		{"resource_not_found", NewResourceNotFoundError("op-id foo"), 11},
		{"config", NewConfigError("bad yaml"), 12},
		{"prereq", NewPrerequisiteError("kubectl not in PATH"), 13},
		{"kubectl_passthrough_2", NewKubectlPassthroughError(2), 102},
		{"kubectl_passthrough_127", NewKubectlPassthroughError(127), 227},
		{"unknown", errors.New("random error"), 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, ExitCodeFor(c.err))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/internal/errors/... -v
```

Expected: FAIL — package does not exist.

- [ ] **Step 3: Create `cmd/wpcli/internal/errors/types.go`**

```go
// Package errors centralizes wp CLI's error types and exit code mapping.
package errors

import "fmt"

// Type identifies the semantic class of a CLI error.
type Type string

const (
	TypeUsage             Type = "Usage"
	TypeNetwork           Type = "Network"
	TypeTargetNotFound    Type = "TargetNotFound"
	TypeStateConflict     Type = "StateConflict"
	TypeWaitTimeout       Type = "WaitTimeout"
	TypeStrictPartial     Type = "StrictPartial"
	TypeUserAbort         Type = "UserAbort"
	TypeYellowFinding     Type = "YellowFinding"
	TypeRedFinding        Type = "RedFinding"
	TypeNotImplemented    Type = "NotImplemented"
	TypeResourceNotFound  Type = "ResourceNotFound"
	TypeConfig            Type = "Config"
	TypePrerequisite      Type = "Prerequisite"
	TypeKubectlPassthrough Type = "KubectlPassthrough"
)

// CLIError is the structured error type wp commands return.
// The root command converts this to an exit code via ExitCodeFor.
type CLIError struct {
	Type    Type
	Message string
	Detail  string
	Hint    string
	// KubectlExitCode is only meaningful when Type == TypeKubectlPassthrough.
	KubectlExitCode int
	Cause           error
}

func (e *CLIError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Type, e.Message, e.Detail)
	}
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

func (e *CLIError) Unwrap() error { return e.Cause }

// Constructors — one per type. Keep signatures minimal; detail/hint added via chaining if needed.

func NewUsageError(msg string) *CLIError {
	return &CLIError{Type: TypeUsage, Message: msg}
}

func NewNetworkError(msg string) *CLIError {
	return &CLIError{Type: TypeNetwork, Message: msg}
}

func NewTargetNotFoundError(target string) *CLIError {
	return &CLIError{Type: TypeTargetNotFound, Message: fmt.Sprintf("target not found: %s", target)}
}

func NewStateConflictError(msg string) *CLIError {
	return &CLIError{Type: TypeStateConflict, Message: msg}
}

func NewWaitTimeoutError(what string, seconds int) *CLIError {
	return &CLIError{Type: TypeWaitTimeout, Message: fmt.Sprintf("%s wait timed out after %ds", what, seconds)}
}

func NewStrictPartialFailureError(unreachable, total int) *CLIError {
	return &CLIError{
		Type:    TypeStrictPartial,
		Message: fmt.Sprintf("%d/%d nodes unreachable in strict mode", unreachable, total),
	}
}

func NewUserAbortError() *CLIError {
	return &CLIError{Type: TypeUserAbort, Message: "user aborted"}
}

func NewYellowFindingError(msg string) *CLIError {
	return &CLIError{Type: TypeYellowFinding, Message: msg}
}

func NewRedFindingError(msg string) *CLIError {
	return &CLIError{Type: TypeRedFinding, Message: msg}
}

func NewNotImplementedError(what string) *CLIError {
	return &CLIError{
		Type:    TypeNotImplemented,
		Message: fmt.Sprintf("%s is intentionally not implemented by this CLI", what),
	}
}

func NewResourceNotFoundError(resource string) *CLIError {
	return &CLIError{Type: TypeResourceNotFound, Message: fmt.Sprintf("resource not found: %s", resource)}
}

func NewConfigError(msg string) *CLIError {
	return &CLIError{Type: TypeConfig, Message: msg}
}

func NewPrerequisiteError(missing string) *CLIError {
	return &CLIError{Type: TypePrerequisite, Message: fmt.Sprintf("prerequisite missing: %s", missing)}
}

func NewKubectlPassthroughError(code int) *CLIError {
	return &CLIError{
		Type:            TypeKubectlPassthrough,
		Message:         fmt.Sprintf("kubectl exited with code %d", code),
		KubectlExitCode: code,
	}
}
```

- [ ] **Step 4: Create `cmd/wpcli/internal/errors/exit.go`**

```go
package errors

import "errors"

// ExitCodeFor maps an error to the wp CLI exit code per spec §3.5.
// nil → 0. Unknown errors → 1 (treated as generic network/runtime failure).
func ExitCodeFor(err error) int {
	if err == nil {
		return 0
	}
	var cli *CLIError
	if !errors.As(err, &cli) {
		return 1
	}
	switch cli.Type {
	case TypeNetwork:
		return 1
	case TypeUsage:
		return 2
	case TypeTargetNotFound:
		return 3
	case TypeStateConflict:
		return 4
	case TypeWaitTimeout:
		return 5
	case TypeStrictPartial:
		return 6
	case TypeUserAbort:
		return 7
	case TypeYellowFinding:
		return 8
	case TypeRedFinding:
		return 9
	case TypeNotImplemented:
		return 10
	case TypeResourceNotFound:
		return 11
	case TypeConfig:
		return 12
	case TypePrerequisite:
		return 13
	case TypeKubectlPassthrough:
		return 100 + cli.KubectlExitCode
	default:
		return 1
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./cmd/wpcli/internal/errors/... -v
```

Expected: PASS on all sub-tests.

- [ ] **Step 6: Wire exit mapping into `cmd/wpcli/main.go`**

Replace `cmd/wpcli/main.go` with:

```go
package main

import (
	"fmt"
	"os"

	"github.com/zilliztech/woodpecker/cmd/wpcli/cmd"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func main() {
	root := cmd.NewRootCommand()
	err := root.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "wp:", err)
	}
	os.Exit(wperrors.ExitCodeFor(err))
}
```

- [ ] **Step 7: Verify build still works**

```bash
make wpcli && ./bin/wp --help && echo "exit=$?"
```

Expected: help output, `exit=0`.

- [ ] **Step 8: Commit**

```bash
git add cmd/wpcli/internal/errors/ cmd/wpcli/main.go
git commit -m "feat(wpcli): add structured error types and exit code mapping"
```

---

### Task 5: CLI config loading (`cli.yaml`)

**Goal:** Load `~/.woodpecker/cli.yaml` per spec §3.1 / §3.2, produce a resolved effective config (flag > env > context > defaults). This is the plumbing layer — no command uses it yet.

**Files:**
- Create: `cmd/wpcli/config/cli_config.go`
- Create: `cmd/wpcli/config/cli_config_test.go`
- Create: `cmd/wpcli/config/testdata/good.yaml`
- Create: `cmd/wpcli/config/testdata/bad-yaml.yaml`
- Create: `cmd/wpcli/config/testdata/missing-context.yaml`

- [ ] **Step 1: Create test fixtures**

`cmd/wpcli/config/testdata/good.yaml`:

```yaml
current-context: prod

contexts:
  prod:
    endpoint:    http://prod.wp.svc:9091
    admin_port:  9091
    timeout:     30s
    concurrency: 8
    strict:      false
  dev:
    endpoint: http://localhost:9091

defaults:
  output:    table
  no_color:  false
```

`cmd/wpcli/config/testdata/bad-yaml.yaml`:

```yaml
current-context: prod
contexts:
  prod:
    endpoint: [not a string
```

`cmd/wpcli/config/testdata/missing-context.yaml`:

```yaml
current-context: nonexistent

contexts:
  prod:
    endpoint: http://prod.wp.svc:9091
```

- [ ] **Step 2: Write tests**

Create `cmd/wpcli/config/cli_config_test.go`:

```go
package config

import (
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
	// A context with no timeout set should pick up defaults.
	cfg, _ := Load("testdata/good.yaml")
	ctx, _ := cfg.ResolveContext("dev")
	// dev has no admin_port → falls back to 9091 hardcoded default
	require.Equal(t, 9091, ctx.AdminPort)
	// dev has no timeout → falls back to 30s hardcoded default
	require.Equal(t, 30*time.Second, ctx.Timeout)
}
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
go test ./cmd/wpcli/config/... -v
```

Expected: FAIL — package does not exist.

- [ ] **Step 4: Create `cmd/wpcli/config/cli_config.go`**

```go
// Package config loads and resolves the wp CLI configuration (cli.yaml).
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Context is one named cluster context in cli.yaml.
type Context struct {
	Endpoint    string        `yaml:"endpoint"`
	AdminPort   int           `yaml:"admin_port"`
	Timeout     time.Duration `yaml:"timeout"`
	Concurrency int           `yaml:"concurrency"`
	Strict      bool          `yaml:"strict"`
	// K8s is populated in Phase 3; Phase 1 loader just ignores it safely.
	K8s map[string]any `yaml:"k8s,omitempty"`
}

// Defaults carries the `defaults:` section of cli.yaml.
type Defaults struct {
	Output   string `yaml:"output"`
	NoColor  bool   `yaml:"no_color"`
	PageSize int    `yaml:"page_size"`
}

// File is the parsed cli.yaml document.
type File struct {
	CurrentContext string             `yaml:"current-context"`
	Contexts       map[string]Context `yaml:"contexts"`
	Defaults       Defaults           `yaml:"defaults"`
}

// Load parses a cli.yaml file.
func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cli.yaml: %w", err)
	}
	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse cli.yaml: %w", err)
	}
	return &f, nil
}

// ResolveContext returns the effective context, applying defaults for unset fields.
// If name is empty, uses f.CurrentContext.
func (f *File) ResolveContext(name string) (Context, error) {
	if name == "" {
		name = f.CurrentContext
	}
	if name == "" {
		return Context{}, fmt.Errorf("no context name provided and no current-context set")
	}
	ctx, ok := f.Contexts[name]
	if !ok {
		return Context{}, fmt.Errorf("context %q not found in cli.yaml", name)
	}
	// Apply hardcoded fallback defaults (spec §3.2 bottom layer).
	if ctx.AdminPort == 0 {
		ctx.AdminPort = 9091
	}
	if ctx.Timeout == 0 {
		ctx.Timeout = 30 * time.Second
	}
	if ctx.Concurrency == 0 {
		ctx.Concurrency = 8
	}
	return ctx, nil
}

// DefaultConfigPaths returns the list of probe paths for cli.yaml, in priority order.
func DefaultConfigPaths() []string {
	var paths []string
	if p := os.Getenv("WOODPECKER_CLI_CONFIG"); p != "" {
		paths = append(paths, p)
	}
	if p := os.Getenv("XDG_CONFIG_HOME"); p != "" {
		paths = append(paths, p+"/woodpecker/cli.yaml")
	}
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, home+"/.woodpecker/cli.yaml")
	}
	return paths
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./cmd/wpcli/config/... -v
```

Expected: PASS on all tests.

- [ ] **Step 6: Commit**

```bash
git add cmd/wpcli/config/
git commit -m "feat(wpcli): add cli.yaml loader with context overlay"
```

---

### Task 6: `wp version` command

**Goal:** Smallest real sub-command. Prints build info from `common/version`. Exercises the Globals flag, cobra wiring, and the `-o json` path early.

**Files:**
- Create: `cmd/wpcli/cmd/version.go`
- Create: `cmd/wpcli/cmd/version_test.go`
- Modify: `cmd/wpcli/cmd/root.go` — register the sub-command

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/version_test.go`:

```go
package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionCommand_Default(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"version"})

	require.NoError(t, root.Execute())
	// Default text output contains the version (whatever it is via ldflags, at least "dev").
	require.Contains(t, buf.String(), "wp version")
}

func TestVersionCommand_JSON(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"version", "-o", "json"})

	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), `"version"`)
	require.Contains(t, buf.String(), `"go_version"`)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/cmd/... -run TestVersionCommand -v
```

Expected: FAIL — `wp version` sub-command does not exist.

- [ ] **Step 3: Create `cmd/wpcli/cmd/version.go`**

```go
package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/common/version"
)

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show wp CLI version and build info",
		RunE: func(cmd *cobra.Command, args []string) error {
			info := version.Info()
			out := cmd.OutOrStdout()
			switch Globals.Output {
			case "json":
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				return enc.Encode(info)
			case "yaml":
				fmt.Fprintf(out, "version: %s\ncommit: %s\nbuild_time: %s\ngo_version: %s\n",
					info.Version, info.Commit, info.BuildTime, info.GoVersion)
				return nil
			default:
				fmt.Fprintf(out, "wp version %s\n", info.String())
				return nil
			}
		},
	}
}
```

- [ ] **Step 4: Register the sub-command in `cmd/wpcli/cmd/root.go`**

Modify `NewRootCommand` to add at the end before `return root`:

```go
	root.AddCommand(newVersionCommand())
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./cmd/wpcli/cmd/... -v
```

Expected: PASS on all tests.

- [ ] **Step 6: Build and exercise manually**

```bash
make wpcli
./bin/wp version
./bin/wp version -o json
```

Expected: text output on first call, JSON on second.

- [ ] **Step 7: Commit**

```bash
git add cmd/wpcli/cmd/version.go cmd/wpcli/cmd/version_test.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add version command"
```

---

### Task 7: `wp ctx list / use / view` commands

**Goal:** CLI-side context management (spec §3.1). Three sub-commands grouped under `wp ctx`. Exercises the config loader end-to-end.

**Files:**
- Create: `cmd/wpcli/cmd/ctx.go`
- Create: `cmd/wpcli/cmd/ctx_test.go`
- Modify: `cmd/wpcli/cmd/root.go` — register the sub-command

- [ ] **Step 1: Write tests**

Create `cmd/wpcli/cmd/ctx_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./cmd/wpcli/cmd/... -run TestCtx -v
```

Expected: FAIL — `ctx` sub-command does not exist.

- [ ] **Step 3: Create `cmd/wpcli/cmd/ctx.go`**

```go
package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newCtxCommand() *cobra.Command {
	ctx := &cobra.Command{
		Use:   "ctx",
		Short: "Manage CLI contexts (cli.yaml)",
	}
	ctx.AddCommand(newCtxListCommand(), newCtxUseCommand(), newCtxViewCommand())
	return ctx
}

// loadFileFromEnvOrDefault resolves which cli.yaml to operate on.
func loadFileFromEnvOrDefault() (*config.File, string, error) {
	// Explicit flag / env first
	for _, p := range config.DefaultConfigPaths() {
		if _, err := os.Stat(p); err == nil {
			f, err := config.Load(p)
			if err != nil {
				return nil, p, wperrors.NewConfigError(err.Error())
			}
			return f, p, nil
		}
	}
	return nil, "", wperrors.NewConfigError("no cli.yaml found in any known location")
}

func newCtxListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all contexts defined in cli.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			f, _, err := loadFileFromEnvOrDefault()
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tENDPOINT\tACTIVE")
			for name, c := range f.Contexts {
				active := ""
				if name == f.CurrentContext {
					active = "*"
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\n", name, c.Endpoint, active)
			}
			return tw.Flush()
		},
	}
}

func newCtxUseCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "use <name>",
		Short: "Switch current-context to the given name and persist to cli.yaml",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			f, path, err := loadFileFromEnvOrDefault()
			if err != nil {
				return err
			}
			name := args[0]
			if _, ok := f.Contexts[name]; !ok {
				return wperrors.NewConfigError(fmt.Sprintf("context %q does not exist", name))
			}
			f.CurrentContext = name
			data, err := yaml.Marshal(f)
			if err != nil {
				return wperrors.NewConfigError(fmt.Sprintf("marshal cli.yaml: %v", err))
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				return wperrors.NewConfigError(fmt.Sprintf("write cli.yaml: %v", err))
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Switched to context %q\n", name)
			return nil
		},
	}
}

func newCtxViewCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "view",
		Short: "Print the resolved fields of the active context",
		RunE: func(cmd *cobra.Command, args []string) error {
			f, _, err := loadFileFromEnvOrDefault()
			if err != nil {
				return err
			}
			active, err := f.ResolveContext(Globals.Context)
			if err != nil {
				return wperrors.NewConfigError(err.Error())
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Active context:\n")
			fmt.Fprintf(out, "  endpoint:     %s\n", active.Endpoint)
			fmt.Fprintf(out, "  admin_port:   %d\n", active.AdminPort)
			fmt.Fprintf(out, "  timeout:      %s\n", active.Timeout)
			fmt.Fprintf(out, "  concurrency:  %d\n", active.Concurrency)
			fmt.Fprintf(out, "  strict:       %v\n", active.Strict)
			return nil
		},
	}
}
```

- [ ] **Step 4: Register `ctx` in `cmd/wpcli/cmd/root.go`**

Add to `NewRootCommand` just before `return root`:

```go
	root.AddCommand(newCtxCommand())
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./cmd/wpcli/cmd/... -run TestCtx -v
```

Expected: PASS on all `TestCtx*`.

- [ ] **Step 6: Commit**

```bash
git add cmd/wpcli/cmd/ctx.go cmd/wpcli/cmd/ctx_test.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add ctx list/use/view commands"
```

---

## Part B — Output Renderers + HTTP Client (Tasks 8-12)

### Task 8: Output renderers — JSON + YAML

**Goal:** Minimal structured renderers. Every command will use these for `-o json` / `-o yaml` outputs.

**Files:**
- Create: `cmd/wpcli/output/output.go`
- Create: `cmd/wpcli/output/json.go`
- Create: `cmd/wpcli/output/yaml.go`
- Create: `cmd/wpcli/output/json_test.go`

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/output/json_test.go`:

```go
package output

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

type sample struct {
	Name string `json:"name" yaml:"name"`
	Age  int    `json:"age" yaml:"age"`
}

func TestRenderJSON(t *testing.T) {
	buf := new(bytes.Buffer)
	err := RenderJSON(buf, sample{Name: "alice", Age: 30})
	require.NoError(t, err)
	require.Contains(t, buf.String(), `"name": "alice"`)
	require.Contains(t, buf.String(), `"age": 30`)
}

func TestRenderYAML(t *testing.T) {
	buf := new(bytes.Buffer)
	err := RenderYAML(buf, sample{Name: "alice", Age: 30})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "name: alice")
	require.Contains(t, buf.String(), "age: 30")
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/output/... -v
```

Expected: FAIL — package does not exist.

- [ ] **Step 3: Create `cmd/wpcli/output/output.go`**

```go
// Package output holds wp CLI output renderers.
package output

import "io"

// Format is the enum of supported output formats.
type Format string

const (
	FormatTable Format = "table"
	FormatWide  Format = "wide"
	FormatJSON  Format = "json"
	FormatYAML  Format = "yaml"
	FormatTree  Format = "tree"
	FormatRaw   Format = "raw"
)

// Renderer is anything that can render an object to a writer.
type Renderer interface {
	Render(w io.Writer, data any) error
}
```

- [ ] **Step 4: Create `cmd/wpcli/output/json.go`**

```go
package output

import (
	"encoding/json"
	"io"
)

// RenderJSON writes data as indented JSON.
func RenderJSON(w io.Writer, data any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}
```

- [ ] **Step 5: Create `cmd/wpcli/output/yaml.go`**

```go
package output

import (
	"io"

	"gopkg.in/yaml.v3"
)

// RenderYAML writes data as YAML.
func RenderYAML(w io.Writer, data any) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	defer enc.Close()
	return enc.Encode(data)
}
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
go test ./cmd/wpcli/output/... -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add cmd/wpcli/output/
git commit -m "feat(wpcli): add JSON and YAML output renderers"
```

---

### Task 9: Output renderer — table (row + sectioned)

**Goal:** Two table renderers per spec §3.4:
- **Row-per-entity** for list commands (`wp node list`, etc.): rows and columns
- **Sectioned key-value** for show/stats commands: section headers + indented key/value pairs

Both use `text/tabwriter`.

**Files:**
- Create: `cmd/wpcli/output/table.go`
- Create: `cmd/wpcli/output/table_test.go`

- [ ] **Step 1: Write tests**

Create `cmd/wpcli/output/table_test.go`:

```go
package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowTable_Basic(t *testing.T) {
	buf := new(bytes.Buffer)
	rows := [][]string{
		{"node-1", "10.0.1.1", "active"},
		{"node-2", "10.0.1.2", "decommissioning"},
	}
	err := RenderRowTable(buf, []string{"NAME", "ADDR", "STATE"}, rows)
	require.NoError(t, err)
	lines := strings.Split(buf.String(), "\n")
	// header, node-1, node-2, trailing empty
	require.GreaterOrEqual(t, len(lines), 3)
	require.Contains(t, lines[0], "NAME")
	require.Contains(t, lines[0], "STATE")
	require.Contains(t, lines[1], "node-1")
	require.Contains(t, lines[2], "node-2")
}

func TestRowTable_EmptyRows(t *testing.T) {
	buf := new(bytes.Buffer)
	err := RenderRowTable(buf, []string{"NAME", "ADDR"}, nil)
	require.NoError(t, err)
	// Header still printed.
	require.Contains(t, buf.String(), "NAME")
}

func TestSectionedTable_Basic(t *testing.T) {
	buf := new(bytes.Buffer)
	sections := []Section{
		{
			Title: "Identity",
			Pairs: [][2]string{
				{"node_id", "node-2"},
				{"gossip_addr", "10.0.1.2:17946"},
			},
		},
		{
			Title: "Lifecycle",
			Pairs: [][2]string{
				{"state", "decommissioning"},
				{"since", "18m42s ago"},
			},
		},
	}
	err := RenderSectionedTable(buf, sections)
	require.NoError(t, err)
	out := buf.String()
	require.Contains(t, out, "Identity")
	require.Contains(t, out, "node_id")
	require.Contains(t, out, "node-2")
	require.Contains(t, out, "Lifecycle")
	require.Contains(t, out, "decommissioning")
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./cmd/wpcli/output/... -run "TestRowTable|TestSectionedTable" -v
```

Expected: FAIL — functions do not exist.

- [ ] **Step 3: Create `cmd/wpcli/output/table.go`**

```go
package output

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// RenderRowTable renders a row-per-entity table for list commands.
// headers is the column headers; rows is [][]string where len(row) == len(headers).
func RenderRowTable(w io.Writer, headers []string, rows [][]string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, strings.Join(headers, "\t")); err != nil {
		return err
	}
	for _, r := range rows {
		if _, err := fmt.Fprintln(tw, strings.Join(r, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// Section is one titled block of key-value pairs for sectioned table output.
type Section struct {
	Title string
	Pairs [][2]string // [key, value]
}

// RenderSectionedTable writes sections with a title line followed by indented "key: value" pairs.
// Used for show/stats commands where a single entity has multiple logical groups.
func RenderSectionedTable(w io.Writer, sections []Section) error {
	for i, s := range sections {
		if i > 0 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w, s.Title); err != nil {
			return err
		}
		// Use tabwriter within each section so key column aligns.
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, kv := range s.Pairs {
			if _, err := fmt.Fprintf(tw, "  %s:\t%s\n", kv[0], kv[1]); err != nil {
				return err
			}
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./cmd/wpcli/output/... -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/output/table.go cmd/wpcli/output/table_test.go
git commit -m "feat(wpcli): add row and sectioned table renderers"
```

---

### Task 10: Output renderer — tree

**Goal:** ASCII tree rendering for `wp cluster info` topology mode.

**Files:**
- Create: `cmd/wpcli/output/tree.go`
- Create: `cmd/wpcli/output/tree_test.go`

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/output/tree_test.go`:

```go
package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderTree_Simple(t *testing.T) {
	root := &TreeNode{
		Label: "Cluster",
		Children: []*TreeNode{
			{
				Label: "us-east-1a",
				Children: []*TreeNode{
					{Label: "node-1"},
					{Label: "node-2"},
				},
			},
			{
				Label: "us-east-1b",
				Children: []*TreeNode{
					{Label: "node-3"},
				},
			},
		},
	}
	buf := new(bytes.Buffer)
	require.NoError(t, RenderTree(buf, root))
	out := buf.String()

	require.Contains(t, out, "Cluster")
	require.Contains(t, out, "us-east-1a")
	require.Contains(t, out, "node-1")
	require.Contains(t, out, "node-3")
	// Uses box-drawing characters.
	require.True(t,
		strings.Contains(out, "├──") || strings.Contains(out, "└──"),
		"expected box-drawing chars, got:\n%s", out)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/output/... -run TestRenderTree -v
```

Expected: FAIL.

- [ ] **Step 3: Create `cmd/wpcli/output/tree.go`**

```go
package output

import (
	"fmt"
	"io"
)

// TreeNode is one node in the tree structure RenderTree renders.
type TreeNode struct {
	Label    string
	Children []*TreeNode
}

// RenderTree writes an ASCII tree to w starting from root.
// Example output:
//
//	Cluster
//	├── us-east-1a
//	│   ├── node-1
//	│   └── node-2
//	└── us-east-1b
//	    └── node-3
func RenderTree(w io.Writer, root *TreeNode) error {
	if root == nil {
		return nil
	}
	if _, err := fmt.Fprintln(w, root.Label); err != nil {
		return err
	}
	return renderChildren(w, root.Children, "")
}

func renderChildren(w io.Writer, children []*TreeNode, prefix string) error {
	for i, c := range children {
		last := i == len(children)-1
		branch := "├── "
		nextPrefix := prefix + "│   "
		if last {
			branch = "└── "
			nextPrefix = prefix + "    "
		}
		if _, err := fmt.Fprintf(w, "%s%s%s\n", prefix, branch, c.Label); err != nil {
			return err
		}
		if err := renderChildren(w, c.Children, nextPrefix); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./cmd/wpcli/output/... -run TestRenderTree -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/output/tree.go cmd/wpcli/output/tree_test.go
git commit -m "feat(wpcli): add ASCII tree renderer"
```

---

### Task 11: Admin HTTP client + memberlist JSON parser

**Goal:** HTTP client that knows how to talk to the admin port, parse the (future) JSON memberlist response, and resolve node-id → admin URL.

This task defines the **expected JSON response schema** for `/admin/memberlist` (Task 13/14 makes the server produce it).

**Files:**
- Create: `cmd/wpcli/client/client.go`
- Create: `cmd/wpcli/client/memberlist.go`
- Create: `cmd/wpcli/client/memberlist_test.go`

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/client/memberlist_test.go`:

```go
package client

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

const memberlistJSONBody = `{
  "members": [
    {
      "id": "node-1",
      "gossip_addr": "10.0.1.1:17946",
      "service_addr": "10.0.1.1:18080",
      "az": "us-east-1a",
      "rg": "default",
      "state": 0,
      "incarnation": 42,
      "last_seen_ms": 1712553600000,
      "tags": {"role": "logstore"}
    },
    {
      "id": "node-2",
      "gossip_addr": "10.0.1.2:17946",
      "service_addr": "10.0.1.2:18080",
      "az": "us-east-1b",
      "rg": "default",
      "state": 0,
      "incarnation": 17,
      "last_seen_ms": 1712553601000,
      "tags": {"role": "logstore"}
    }
  ]
}`

func TestMemberlistJSON_Parse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/admin/memberlist", r.URL.Path)
		require.Equal(t, "application/json", r.Header.Get("Accept"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(memberlistJSONBody))
	}))
	defer srv.Close()

	c := New(srv.URL, ClientOpts{})
	ml, err := c.GetMemberlist()
	require.NoError(t, err)
	require.Len(t, ml.Members, 2)
	require.Equal(t, "node-1", ml.Members[0].ID)
	require.Equal(t, "us-east-1a", ml.Members[0].AZ)
}

func TestResolveNode_ByID(t *testing.T) {
	ml := &Memberlist{
		Members: []Member{
			{ID: "node-1", GossipAddr: "10.0.1.1:17946"},
			{ID: "node-2", GossipAddr: "10.0.1.2:17946"},
		},
	}
	m, ok := ml.Resolve("node-2")
	require.True(t, ok)
	require.Equal(t, "node-2", m.ID)
}

func TestResolveNode_ByHostPrefix(t *testing.T) {
	ml := &Memberlist{
		Members: []Member{
			{ID: "node-1", GossipAddr: "10.0.1.1:17946"},
			{ID: "node-2", GossipAddr: "10.0.1.2:17946"},
		},
	}
	m, ok := ml.Resolve("10.0.1.2")
	require.True(t, ok)
	require.Equal(t, "node-2", m.ID)
}

func TestResolveNode_NotFound(t *testing.T) {
	ml := &Memberlist{Members: []Member{{ID: "node-1"}}}
	_, ok := ml.Resolve("node-99")
	require.False(t, ok)
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./cmd/wpcli/client/... -v
```

Expected: FAIL — package does not exist.

- [ ] **Step 3: Create `cmd/wpcli/client/client.go`**

```go
// Package client is the wp CLI's admin HTTP client.
package client

import (
	"net/http"
	"time"
)

// ClientOpts carries client-level options (timeout, admin port override, etc.).
type ClientOpts struct {
	Timeout   time.Duration
	AdminPort int // port to use when constructing peer URLs from gossip hosts; 0 means extract from seed
}

// Client talks to a Woodpecker server admin endpoint.
type Client struct {
	baseURL string
	http    *http.Client
	opts    ClientOpts
}

// New creates a new Client pinned to a single admin URL (the seed).
func New(baseURL string, opts ClientOpts) *Client {
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		baseURL: baseURL,
		http: &http.Client{
			Timeout: timeout,
		},
		opts: opts,
	}
}
```

- [ ] **Step 4: Create `cmd/wpcli/client/memberlist.go`**

```go
package client

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Member is one entry in the admin memberlist JSON response.
type Member struct {
	ID          string            `json:"id"`
	GossipAddr  string            `json:"gossip_addr"`
	ServiceAddr string            `json:"service_addr"`
	AZ          string            `json:"az"`
	RG          string            `json:"rg"`
	State       int               `json:"state"`
	Incarnation uint32            `json:"incarnation"`
	LastSeenMS  int64             `json:"last_seen_ms"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// Memberlist is the top-level response of GET /admin/memberlist (JSON mode).
type Memberlist struct {
	Members []Member `json:"members"`
}

// Resolve finds a member by node_id, by `host:port`, or by IP prefix (gossip host).
// Returns the matching member and true on a unique match.
func (m *Memberlist) Resolve(identifier string) (Member, bool) {
	// 1. Exact node_id
	for _, mem := range m.Members {
		if mem.ID == identifier {
			return mem, true
		}
	}
	// 2. Exact host:port match on gossip or service addr
	for _, mem := range m.Members {
		if mem.GossipAddr == identifier || mem.ServiceAddr == identifier {
			return mem, true
		}
	}
	// 3. IP prefix (host portion of gossip_addr)
	var matches []Member
	for _, mem := range m.Members {
		host := mem.GossipAddr
		if i := strings.LastIndex(host, ":"); i >= 0 {
			host = host[:i]
		}
		if strings.HasPrefix(host, identifier) {
			matches = append(matches, mem)
		}
	}
	if len(matches) == 1 {
		return matches[0], true
	}
	return Member{}, false
}

// GetMemberlist fetches and parses /admin/memberlist from the client's seed URL.
func (c *Client) GetMemberlist() (*Memberlist, error) {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+"/admin/memberlist", nil)
	if err != nil {
		return nil, fmt.Errorf("build memberlist request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get memberlist: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("memberlist returned status %d", resp.StatusCode)
	}
	var ml Memberlist
	if err := json.NewDecoder(resp.Body).Decode(&ml); err != nil {
		return nil, fmt.Errorf("decode memberlist: %w", err)
	}
	return &ml, nil
}

// PeerAdminURL returns the admin HTTP URL for a given member, using the configured admin port.
func (c *Client) PeerAdminURL(m Member) string {
	host := m.GossipAddr
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	port := c.opts.AdminPort
	if port == 0 {
		port = 9091
	}
	return fmt.Sprintf("http://%s:%d", host, port)
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./cmd/wpcli/client/... -v
```

Expected: PASS on all four sub-tests.

- [ ] **Step 6: Commit**

```bash
git add cmd/wpcli/client/
git commit -m "feat(wpcli): add admin HTTP client + memberlist JSON parser"
```

---

### Task 12: Fan-out client (strict / non-strict modes)

**Goal:** Concurrent fan-out to multiple peer URLs with bounded concurrency, respecting the strict / non-strict partial failure semantics from spec §3.3.

**Files:**
- Create: `cmd/wpcli/client/fanout.go`
- Create: `cmd/wpcli/client/fanout_test.go`

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/client/fanout_test.go`:

```go
package client

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newEchoServer(code int, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
}

func TestFanout_AllSuccess(t *testing.T) {
	s1 := newEchoServer(200, `{"ok":1}`)
	defer s1.Close()
	s2 := newEchoServer(200, `{"ok":2}`)
	defer s2.Close()

	f := NewFanout(FanoutOpts{Concurrency: 4, Timeout: 5 * time.Second})
	res := f.Get([]string{s1.URL, s2.URL}, "/admin/node/status", "node-id-placeholder-n/a")
	require.Equal(t, 2, res.Reachable)
	require.Equal(t, 0, res.Unreachable)
	require.Len(t, res.Results, 2)
}

func TestFanout_PartialFailure_NotStrict(t *testing.T) {
	s1 := newEchoServer(200, `{"ok":1}`)
	defer s1.Close()
	s2 := newEchoServer(500, `err`)
	defer s2.Close()

	f := NewFanout(FanoutOpts{Concurrency: 4, Timeout: 5 * time.Second, Strict: false})
	res := f.Get([]string{s1.URL, s2.URL}, "/admin/node/status", "")
	require.Equal(t, 1, res.Reachable)
	require.Equal(t, 1, res.Unreachable)
	require.False(t, res.StrictFailure())
}

func TestFanout_PartialFailure_Strict(t *testing.T) {
	s1 := newEchoServer(200, `{"ok":1}`)
	defer s1.Close()
	s2 := newEchoServer(500, `err`)
	defer s2.Close()

	f := NewFanout(FanoutOpts{Concurrency: 4, Timeout: 5 * time.Second, Strict: true})
	res := f.Get([]string{s1.URL, s2.URL}, "/admin/node/status", "")
	require.Equal(t, 1, res.Reachable)
	require.Equal(t, 1, res.Unreachable)
	require.True(t, res.StrictFailure())
}

func TestFanout_ConcurrencyBound(t *testing.T) {
	// All servers sleep; verify no more than `concurrency` are in flight at once.
	// This is a looser assertion: total time should be ≥ ceil(n/concurrency) × sleep.
	var servers []*httptest.Server
	for i := 0; i < 8; i++ {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(200 * time.Millisecond)
			w.WriteHeader(200)
		}))
		defer s.Close()
		servers = append(servers, s)
	}
	var urls []string
	for _, s := range servers {
		urls = append(urls, s.URL)
	}

	f := NewFanout(FanoutOpts{Concurrency: 2, Timeout: 5 * time.Second})
	start := time.Now()
	_ = f.Get(urls, "/admin/node/status", "")
	elapsed := time.Since(start)
	// With 8 requests at 200ms each and concurrency 2, expect roughly ≥ 800ms.
	require.GreaterOrEqual(t, elapsed, 700*time.Millisecond, "concurrency bound should serialize batches")
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./cmd/wpcli/client/... -run TestFanout -v
```

Expected: FAIL — Fanout does not exist.

- [ ] **Step 3: Create `cmd/wpcli/client/fanout.go`**

```go
package client

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// FanoutOpts configures fan-out behavior.
type FanoutOpts struct {
	Concurrency int
	Timeout     time.Duration
	Strict      bool
}

// Fanout executes concurrent HTTP requests across a set of peer URLs.
type Fanout struct {
	opts FanoutOpts
	http *http.Client
}

// NewFanout constructs a Fanout client from opts.
func NewFanout(opts FanoutOpts) *Fanout {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 8
	}
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	return &Fanout{
		opts: opts,
		http: &http.Client{Timeout: opts.Timeout},
	}
}

// NodeResult is the per-node outcome of a fan-out request.
type NodeResult struct {
	URL    string
	NodeID string // optional context (for the output layer)
	OK     bool
	Status int
	Body   []byte
	Err    error
}

// FanoutResult is the aggregate across all peers.
type FanoutResult struct {
	Results     []NodeResult
	Reachable   int
	Unreachable int
	strict      bool
}

// StrictFailure returns true when strict mode is on AND at least one peer failed.
func (r *FanoutResult) StrictFailure() bool {
	return r.strict && r.Unreachable > 0
}

// Get issues a concurrent GET to each base URL at the given path.
// nodeIDHint is attached to all results to help downstream labeling (pass "" to skip).
func (f *Fanout) Get(urls []string, path string, nodeIDHint string) *FanoutResult {
	sem := make(chan struct{}, f.opts.Concurrency)
	results := make([]NodeResult, len(urls))
	var wg sync.WaitGroup

	for i, u := range urls {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, base string) {
			defer wg.Done()
			defer func() { <-sem }()

			r := NodeResult{URL: base, NodeID: nodeIDHint}
			req, err := http.NewRequest(http.MethodGet, base+path, nil)
			if err != nil {
				r.Err = err
				results[idx] = r
				return
			}
			req.Header.Set("Accept", "application/json")
			resp, err := f.http.Do(req)
			if err != nil {
				r.Err = err
				results[idx] = r
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			r.Status = resp.StatusCode
			r.Body = body
			r.OK = resp.StatusCode >= 200 && resp.StatusCode < 300
			if !r.OK {
				r.Err = fmt.Errorf("status %d", resp.StatusCode)
			}
			results[idx] = r
		}(i, u)
	}
	wg.Wait()

	agg := &FanoutResult{Results: results, strict: f.opts.Strict}
	for _, r := range results {
		if r.OK {
			agg.Reachable++
		} else {
			agg.Unreachable++
		}
	}
	return agg
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./cmd/wpcli/client/... -run TestFanout -v
```

Expected: PASS on all four.

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/client/fanout.go cmd/wpcli/client/fanout_test.go
git commit -m "feat(wpcli): add fan-out HTTP client with strict/non-strict modes"
```

---

## Part C — Server-side Changes (Tasks 13-19)

These are the only Phase 1 changes to existing Woodpecker server code. Kept tightly scoped: no metric migration, no opregistry, no interceptors.

### Task 13: Server — `GetMemberlistJSON` method

**Goal:** Add a structured JSON accessor to `ServerNode` alongside the existing text `GetMemberlistStatus()`. Shape must match what the CLI client parses in Task 11.

**Files:**
- Modify: `common/membership/server_node.go` — add new method
- Create: `common/membership/server_node_json_test.go` (or extend existing test file)

- [ ] **Step 1: Inspect the existing `GetMemberlistStatus` implementation**

```bash
grep -n "GetMemberlistStatus" common/membership/server_node.go
```

Verify the existing method (around line 194) so the new JSON method can source from the same `n.memberlist.Members()` call.

- [ ] **Step 2: Write the test**

Create or extend `common/membership/server_node_json_test.go`:

```go
package membership

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetMemberlistJSON_Shape(t *testing.T) {
	// Bring up a single-node memberlist.
	cfg := &ServerConfig{
		NodeID:        "node-test",
		BindPort:      0, // let OS pick
		ServicePort:   18080,
		ResourceGroup: "default",
		AZ:            "us-east-1a",
		Tags:          map[string]string{"role": "logstore"},
	}
	node, err := NewServerNode(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, node.Prepare())
	defer node.Stop()

	data := node.GetMemberlistJSON()
	require.NotEmpty(t, data)

	// Verify it's valid JSON with the expected top-level shape.
	var parsed struct {
		Members []struct {
			ID          string            `json:"id"`
			GossipAddr  string            `json:"gossip_addr"`
			ServiceAddr string            `json:"service_addr"`
			AZ          string            `json:"az"`
			RG          string            `json:"rg"`
			State       int               `json:"state"`
			Incarnation uint32            `json:"incarnation"`
			LastSeenMS  int64             `json:"last_seen_ms"`
			Tags        map[string]string `json:"tags"`
		} `json:"members"`
	}
	require.NoError(t, json.Unmarshal(data, &parsed))
	require.GreaterOrEqual(t, len(parsed.Members), 1)
	found := false
	for _, m := range parsed.Members {
		if m.ID == "node-test" {
			found = true
			require.Equal(t, "us-east-1a", m.AZ)
			require.Equal(t, "default", m.RG)
			break
		}
	}
	require.True(t, found, "expected node-test in memberlist")
}
```

> **Note for implementer:** Inspect `NewServerNode` / `Prepare` signatures in `common/membership/server_node.go` before writing this test — constructor arg shape may differ from the draft above. Adjust to match the existing test pattern in `common/membership/cluster_test.go`.

- [ ] **Step 3: Run test to verify it fails**

```bash
go test ./common/membership/... -run TestGetMemberlistJSON -v
```

Expected: FAIL — method does not exist.

- [ ] **Step 4: Implement `GetMemberlistJSON` in `common/membership/server_node.go`**

Add below the existing `GetMemberlistStatus`:

```go
// GetMemberlistJSON returns the memberlist as a JSON blob matching the
// schema consumed by the wp CLI's client/memberlist.go. This is the
// structured counterpart of GetMemberlistStatus (which returns text).
func (n *ServerNode) GetMemberlistJSON() []byte {
	type memberJSON struct {
		ID          string            `json:"id"`
		GossipAddr  string            `json:"gossip_addr"`
		ServiceAddr string            `json:"service_addr"`
		AZ          string            `json:"az"`
		RG          string            `json:"rg"`
		State       int               `json:"state"`
		Incarnation uint32            `json:"incarnation"`
		LastSeenMS  int64             `json:"last_seen_ms"`
		Tags        map[string]string `json:"tags,omitempty"`
	}
	type listJSON struct {
		Members []memberJSON `json:"members"`
	}

	members := n.memberlist.Members()
	out := listJSON{Members: make([]memberJSON, 0, len(members))}
	for _, m := range members {
		mj := memberJSON{
			ID:          m.Name,
			GossipAddr:  fmt.Sprintf("%s:%d", m.Addr.String(), m.Port),
			State:       int(m.State),
			Incarnation: m.Incarnation,
			LastSeenMS:  time.Now().UnixMilli(),
		}
		// Enrich from our NodeMeta map (indexed by nodeID) when available.
		if meta := n.serviceDiscovery.GetByNodeID(m.Name); meta != nil {
			mj.AZ = meta.Az
			mj.RG = meta.ResourceGroup
			mj.ServiceAddr = fmt.Sprintf("%s:%d", meta.ServiceHost, meta.ServicePort)
			mj.Tags = meta.Tags
		}
		out.Members = append(out.Members, mj)
	}
	b, err := json.Marshal(out)
	if err != nil {
		// Should never happen for this shape; log and fall back to empty list.
		logger.Ctx(context.Background()).Error("marshal memberlist JSON failed", zap.Error(err))
		return []byte(`{"members":[]}`)
	}
	return b
}
```

> **Implementation guardrail:** Verify the actual `NodeMeta` field names (`Az`, `ResourceGroup`, `ServiceHost`, `ServicePort`, `Tags`) against `common/membership/service_discovery.go` and `proto/meta.proto` before compiling. If `serviceDiscovery.GetByNodeID` is not the correct accessor, search `service_discovery.go` for the equivalent method and substitute. **Do not guess** — grep for it.

- [ ] **Step 5: Add required imports**

Ensure `common/membership/server_node.go` imports:

```go
import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "go.uber.org/zap"
    "github.com/zilliztech/woodpecker/common/logger"
    // ... existing imports
)
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
go test ./common/membership/... -run TestGetMemberlistJSON -v
```

Expected: PASS. If the test fails due to `NodeMeta` field mismatches, fix by consulting the actual proto / discovery code.

- [ ] **Step 7: Commit**

```bash
git add common/membership/server_node.go common/membership/server_node_json_test.go
git commit -m "feat(membership): add GetMemberlistJSON for structured admin queries"
```

---

### Task 14: Server — `/admin/memberlist` JSON mode

**Goal:** Extend the existing `/admin/memberlist` handler to return JSON when `Accept: application/json` is set, while keeping the text format for backward compat.

**Files:**
- Modify: `common/http/server.go` — the `AdminCallbacks` + `Start` function
- Modify: `cmd/main.go` — pass the new `GetMemberlistJSON` callback
- Create: `common/http/server_test.go` — if not present, add test for memberlist content negotiation

- [ ] **Step 1: Write the test**

Create or extend `common/http/server_test.go`:

```go
package http

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemberlistHandler_ContentNegotiation(t *testing.T) {
	callbacks := AdminCallbacks{
		GetMemberlistStatus: func() string { return "Total Members: 1\n" },
		GetMemberlistJSON:   func() []byte { return []byte(`{"members":[{"id":"node-1"}]}`) },
	}

	// Build a mux manually that wires in the memberlist handler; mirror what Start does.
	mux := http.NewServeMux()
	mux.HandleFunc(AdminMemberlistPath, newMemberlistHandler(callbacks))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Default (no Accept) → text
	resp, err := http.Get(srv.URL + AdminMemberlistPath)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Contains(t, string(body), "Total Members")

	// Accept: application/json → JSON
	req, _ := http.NewRequest("GET", srv.URL+AdminMemberlistPath, nil)
	req.Header.Set("Accept", "application/json")
	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	require.Contains(t, string(body2), `"members"`)
	require.Equal(t, "application/json", resp2.Header.Get("Content-Type"))
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./common/http/... -run TestMemberlistHandler -v
```

Expected: FAIL — `GetMemberlistJSON` field and `newMemberlistHandler` helper don't exist yet.

- [ ] **Step 3: Add the callback field and extract the handler**

In `common/http/server.go`, modify the `AdminCallbacks` struct:

```go
type AdminCallbacks struct {
    GetMemberlistStatus     func() string   // text format (existing)
    GetMemberlistJSON       func() []byte   // JSON format (new)
    GetNodeStatus           func() any
    Decommission            func() error
    GetDecommissionProgress func() any
}
```

Add a helper function in the same file:

```go
// newMemberlistHandler serves /admin/memberlist with content negotiation.
// Returns JSON when Accept: application/json, plain text otherwise.
func newMemberlistHandler(callbacks AdminCallbacks) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accept := r.Header.Get("Accept")
		if accept == "application/json" && callbacks.GetMemberlistJSON != nil {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(callbacks.GetMemberlistJSON())
			return
		}
		if callbacks.GetMemberlistStatus != nil {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte(callbacks.GetMemberlistStatus()))
		}
	}
}
```

- [ ] **Step 4: Update `Start()` to use the new helper**

Replace the existing inline memberlist registration with:

```go
// Register admin handler for memberlist status (with content negotiation)
Register(&Handler{
    Path:        AdminMemberlistPath,
    HandlerFunc: newMemberlistHandler(callbacks),
})
```

- [ ] **Step 5: Wire the new callback in `cmd/main.go`**

In `cmd/main.go`, find the existing `commonhttp.Start(cfg, commonhttp.AdminCallbacks{ ... })` call and add:

```go
GetMemberlistJSON: srv.GetServerNodeMemberlistJSON,
```

And in `server/service.go` (or wherever `GetServerNodeMemberlistStatus` lives), add the sibling:

```go
// GetServerNodeMemberlistJSON delegates to the underlying ServerNode.
func (s *Server) GetServerNodeMemberlistJSON() []byte {
    if s.node == nil {
        return []byte(`{"members":[]}`)
    }
    return s.node.GetMemberlistJSON()
}
```

> **Guardrail:** Verify the exact name of the existing `GetServerNodeMemberlistStatus` wrapper in `server/service.go` before adding the new one — mirror its access pattern for `s.node`.

- [ ] **Step 6: Run tests to verify they pass**

```bash
go test ./common/http/... -run TestMemberlistHandler -v
go build ./...
```

Expected: PASS + clean build.

- [ ] **Step 7: Commit**

```bash
git add common/http/server.go common/http/server_test.go cmd/main.go server/service.go
git commit -m "feat(http): memberlist endpoint content negotiation (JSON support)"
```

---

### Task 15: Server — `NodeStatus` field augmentation

**Goal:** Add `StartedAt`, `Version`, `LastHealthCheck` to the `NodeStatus` struct per spec §2.A.2. These are needed by `wp node show` / `wp node list -o wide`.

**Files:**
- Modify: `server/service.go` — `NodeStatus` struct + `GetNodeStatus()` method
- Modify: `server/service_test.go` — assert new fields are populated
- Modify: `server/lifecycle.go` (read-only) — if there's a `StartedAt` field on the server struct, reference it; otherwise add it

- [ ] **Step 1: Write or extend a test**

Add to `server/service_test.go` (near existing `TestGetNodeStatus` lines around 1520-1535):

```go
func TestGetNodeStatus_AugmentedFields(t *testing.T) {
	// Setup your standard test Server (mirror the pattern used by the
	// existing TestGetNodeStatus at line 1525).
	srv := newTestServer(t)
	defer srv.Stop()

	status := srv.GetNodeStatus()

	// New Phase-1 fields must be populated and sensible.
	require.NotZero(t, status.StartedAt, "StartedAt must be set")
	require.NotEmpty(t, status.Version, "Version must be set (ldflags or 'dev')")
	require.NotZero(t, status.LastHealthCheck, "LastHealthCheck must be set")
}
```

> **Guardrail:** The exact constructor helper (e.g., `newTestServer`) may differ in the real test file. Inspect `server/service_test.go:1525` to see how the existing `TestGetNodeStatus` sets up its Server; mirror that pattern.

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./server/... -run TestGetNodeStatus_AugmentedFields -v
```

Expected: FAIL — fields do not exist.

- [ ] **Step 3: Extend `NodeStatus` struct in `server/service.go`**

Replace the existing struct (lines 63-73) with:

```go
// NodeStatus represents the current status of this node for external management systems.
type NodeStatus struct {
	NodeID            string            `json:"node_id"`
	State             string            `json:"state"`
	IsDecommissioning bool              `json:"is_decommissioning"`
	MemberCount       int               `json:"member_count"`
	Address           string            `json:"address"`
	ResourceGroup     string            `json:"resource_group"`
	AZ                string            `json:"az"`
	Tags              map[string]string `json:"tags"`

	// Phase 1 additions — consumed by `wp node show` / `wp node list -o wide`.
	StartedAt       int64  `json:"started_at_ms"`        // unix milliseconds, when this process came up
	Version         string `json:"version"`              // from common/version.Info().Version
	LastHealthCheck int64  `json:"last_health_check_ms"` // unix milliseconds, time of last /healthz hit
}
```

- [ ] **Step 4: Track `StartedAt` on the `Server` struct**

Find the `Server` struct definition in `server/service.go` and add a field:

```go
type Server struct {
    // ... existing fields
    startedAtMS atomic.Int64
}
```

In the constructor (`NewServerWithConfig`), set it:

```go
s.startedAtMS.Store(time.Now().UnixMilli())
```

- [ ] **Step 5: Track `LastHealthCheck`**

Option A (preferred if a health check handler exists): update `lastHealthCheckMS` in the healthz handler.
Option B (simpler, Phase 1 OK): return `time.Now().UnixMilli()` at call time — this is approximate but non-zero.

For Phase 1, use Option B. Add a field:

```go
type Server struct {
    // ... existing
    lastHealthCheckMS atomic.Int64
}
```

And wherever `healthz` handler is registered (check `common/http/health/health_handler.go`), call `s.lastHealthCheckMS.Store(time.Now().UnixMilli())`. If that requires a callback wiring change, simpler: populate it on demand in `GetNodeStatus()` with `time.Now().UnixMilli()` and leave the accurate tracking to Phase 2.

- [ ] **Step 6: Update `GetNodeStatus()` to populate the new fields**

Find the existing method (around line 562) and extend the returned struct:

```go
func (s *Server) GetNodeStatus() NodeStatus {
	return NodeStatus{
		// ... existing fields ...
		StartedAt:       s.startedAtMS.Load(),
		Version:         version.Info().Version,
		LastHealthCheck: time.Now().UnixMilli(),
	}
}
```

Add the import:

```go
"github.com/zilliztech/woodpecker/common/version"
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
go test ./server/... -run TestGetNodeStatus -v
go build ./...
```

Expected: PASS + clean build.

- [ ] **Step 8: Commit**

```bash
git add server/service.go server/service_test.go
git commit -m "feat(server): augment NodeStatus with started_at, version, last_health_check"
```

---

### Task 16: Server — `NodeLifecycleManager.CancelDecommission()` strict method

**Goal:** Add the strict cancel method per spec §2.A.5: only allows `decommissioning → active`. Rejects both `active` (idempotent? NO — spec said reject) and `decommissioned` (hard error).

**Wait, let me re-check the spec:** §2.A.5 says "`active`（幂等返回）；`decommissioned` 报错". So active IS idempotent (returns nil without changing state). decommissioned returns error. Only decommissioning actually transitions.

**Files:**
- Modify: `server/lifecycle.go`
- Modify: `server/lifecycle_test.go`

- [ ] **Step 1: Write the test**

Add to `server/lifecycle_test.go`:

```go
func TestCancelDecommission_FromDecommissioning(t *testing.T) {
	m := NewNodeLifecycleManager()
	require.NoError(t, m.StartDecommission())
	require.Equal(t, NodeStateDecommissioning, m.GetState())

	require.NoError(t, m.CancelDecommission())
	require.Equal(t, NodeStateActive, m.GetState())
}

func TestCancelDecommission_FromActive_Idempotent(t *testing.T) {
	m := NewNodeLifecycleManager()
	require.Equal(t, NodeStateActive, m.GetState())

	require.NoError(t, m.CancelDecommission())
	require.Equal(t, NodeStateActive, m.GetState())
}

func TestCancelDecommission_FromDecommissioned_Errors(t *testing.T) {
	m := NewNodeLifecycleManager()
	require.NoError(t, m.StartDecommission())
	require.NoError(t, m.MarkDecommissioned())
	require.Equal(t, NodeStateDecommissioned, m.GetState())

	err := m.CancelDecommission()
	require.Error(t, err)
	require.Equal(t, NodeStateDecommissioned, m.GetState(), "state must not change on error")
}

func TestCancelDecommission_StatePersisted(t *testing.T) {
	dir := t.TempDir()
	m, err := NewNodeLifecycleManagerWithPersistence(dir)
	require.NoError(t, err)

	require.NoError(t, m.StartDecommission())
	require.NoError(t, m.CancelDecommission())

	// Reload from disk; should be active.
	m2, err := NewNodeLifecycleManagerWithPersistence(dir)
	require.NoError(t, err)
	require.Equal(t, NodeStateActive, m2.GetState())
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./server/... -run TestCancelDecommission -v
```

Expected: FAIL — method does not exist.

- [ ] **Step 3: Implement `CancelDecommission()` in `server/lifecycle.go`**

Add after the existing `MarkDecommissioned` method:

```go
// CancelDecommission transitions the node from decommissioning back to active.
// Semantics:
//   - decommissioning → active: transition, persist state
//   - active → active: idempotent, returns nil
//   - decommissioned → error: cannot un-retire a fully decommissioned node
//
// Used by the POST /admin/node/decommission/cancel admin endpoint (Phase 1).
func (m *NodeLifecycleManager) CancelDecommission() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.state {
	case NodeStateActive:
		return nil // idempotent
	case NodeStateDecommissioning:
		m.state = NodeStateActive
		return m.persistStateLocked()
	case NodeStateDecommissioned:
		return fmt.Errorf("cannot cancel: node already decommissioned")
	}
	return fmt.Errorf("unknown state: %s", m.state)
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./server/... -run TestCancelDecommission -v
```

Expected: PASS on all four.

- [ ] **Step 5: Commit**

```bash
git add server/lifecycle.go server/lifecycle_test.go
git commit -m "feat(lifecycle): add NodeLifecycleManager.CancelDecommission strict method"
```

---

### Task 17: Server — `POST /admin/node/decommission/cancel` handler

**Goal:** Expose the new lifecycle method as an HTTP endpoint per spec §2.A.5.

**Files:**
- Modify: `common/http/router.go` — add path constant
- Modify: `common/http/management/node_handler.go` — add handler
- Modify: `common/http/management/node_handler_test.go` — add handler test
- Modify: `common/http/server.go` — extend `AdminCallbacks` + register handler
- Modify: `cmd/main.go` — wire the callback
- Modify: `server/service.go` — add `Server.CancelDecommission()` wrapper

- [ ] **Step 1: Write the handler test**

Add to `common/http/management/node_handler_test.go`:

```go
func TestNewNodeCancelDecommissionHandler_Success(t *testing.T) {
	called := false
	handler := NewNodeCancelDecommissionHandler(func() error {
		called = true
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/admin/node/decommission/cancel", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	require.True(t, called)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	require.Contains(t, w.Body.String(), "cancelled")
}

func TestNewNodeCancelDecommissionHandler_WrongMethod(t *testing.T) {
	handler := NewNodeCancelDecommissionHandler(func() error { return nil })
	req := httptest.NewRequest(http.MethodGet, "/admin/node/decommission/cancel", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestNewNodeCancelDecommissionHandler_Conflict(t *testing.T) {
	handler := NewNodeCancelDecommissionHandler(func() error {
		return errors.New("cannot cancel: node already decommissioned")
	})
	req := httptest.NewRequest(http.MethodPost, "/admin/node/decommission/cancel", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Contains(t, w.Body.String(), "already decommissioned")
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./common/http/management/... -run TestNewNodeCancelDecommissionHandler -v
```

Expected: FAIL.

- [ ] **Step 3: Add the handler**

In `common/http/management/node_handler.go`, add at the bottom:

```go
// NewNodeCancelDecommissionHandler returns an http.HandlerFunc for POST /admin/node/decommission/cancel.
// The cancel callback performs the strict decommissioning → active transition.
func NewNodeCancelDecommissionHandler(cancel func() error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := cancel(); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "decommission cancelled"})
	}
}
```

- [ ] **Step 4: Add the path constant**

In `common/http/router.go`, add:

```go
// AdminNodeDecommissionCancelPath is path for POST /admin/node/decommission/cancel.
const AdminNodeDecommissionCancelPath = "/admin/node/decommission/cancel"
```

- [ ] **Step 5: Extend `AdminCallbacks` and register in `Start()`**

In `common/http/server.go`, add to `AdminCallbacks`:

```go
CancelDecommission func() error
```

In `Start()`, register after the existing decommission progress handler:

```go
if callbacks.CancelDecommission != nil {
    Register(&Handler{
        Path:        AdminNodeDecommissionCancelPath,
        HandlerFunc: management.NewNodeCancelDecommissionHandler(callbacks.CancelDecommission),
    })
}
```

- [ ] **Step 6: Add `Server.CancelDecommission()` wrapper**

In `server/service.go`, add near the existing `Decommission()` method:

```go
// CancelDecommission cancels an in-progress decommission and returns the node to active.
// Used by POST /admin/node/decommission/cancel.
func (s *Server) CancelDecommission() error {
    if s.lifecycle == nil {
        return fmt.Errorf("lifecycle manager not initialized")
    }
    // After cancel, re-enable write acceptance if it was blocked during decommission.
    if err := s.lifecycle.CancelDecommission(); err != nil {
        return err
    }
    // TODO (Phase 2+): if Decommission() disables gRPC write acceptance, re-enable here.
    return nil
}
```

- [ ] **Step 7: Wire the callback in `cmd/main.go`**

Add to the `commonhttp.AdminCallbacks{}` initialization:

```go
CancelDecommission: func() error {
    return srv.CancelDecommission()
},
```

- [ ] **Step 8: Run tests + build**

```bash
go test ./common/http/management/... -run TestNewNodeCancelDecommissionHandler -v
go build ./...
```

Expected: PASS + clean build.

- [ ] **Step 9: Commit**

```bash
git add common/http/ server/service.go cmd/main.go
git commit -m "feat(server): add POST /admin/node/decommission/cancel endpoint"
```

---

### Task 18: Server — `GET /admin/config` handler

**Goal:** Expose the loaded `*config.Configuration` as JSON per spec §2.E. **No redaction** (admin tool per §3.7 security model).

**Files:**
- Create: `common/http/management/config_handler.go`
- Create: `common/http/management/config_handler_test.go`
- Modify: `common/http/router.go` — add path constant
- Modify: `common/http/server.go` — extend `AdminCallbacks` + register
- Modify: `cmd/main.go` — wire the callback

- [ ] **Step 1: Write the handler test**

Create `common/http/management/config_handler_test.go`:

```go
package management

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewConfigHandler_Success(t *testing.T) {
	handler := NewConfigHandler(func() any {
		return map[string]any{
			"etcd":  map[string]any{"endpoints": []string{"etcd:2379"}},
			"minio": map[string]any{"bucketName": "woodpecker"},
		}
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/config", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	require.Contains(t, w.Body.String(), "etcd")
	require.Contains(t, w.Body.String(), "bucketName")
}

func TestNewConfigHandler_WrongMethod(t *testing.T) {
	handler := NewConfigHandler(func() any { return nil })
	req := httptest.NewRequest(http.MethodPost, "/admin/config", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./common/http/management/... -run TestNewConfigHandler -v
```

Expected: FAIL.

- [ ] **Step 3: Create the handler**

Create `common/http/management/config_handler.go`:

```go
// Package management — admin endpoint handlers.
package management

import (
	"encoding/json"
	"net/http"
)

// NewConfigHandler returns a handler for GET /admin/config.
// getConfig must return a JSON-serializable snapshot of the loaded configuration.
// Per spec §2.E.2 there is NO redaction — this CLI is an admin tool and the admin
// port is assumed to be on an internal network (spec §3.7 security model).
func NewConfigHandler(getConfig func() any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(getConfig())
	}
}
```

- [ ] **Step 4: Add the path constant**

In `common/http/router.go`:

```go
// AdminConfigPath is path for GET /admin/config.
const AdminConfigPath = "/admin/config"
```

- [ ] **Step 5: Extend `AdminCallbacks` and register**

In `common/http/server.go`, add to `AdminCallbacks`:

```go
GetConfig func() any
```

In `Start()`, after existing registrations:

```go
if callbacks.GetConfig != nil {
    Register(&Handler{
        Path:        AdminConfigPath,
        HandlerFunc: management.NewConfigHandler(callbacks.GetConfig),
    })
}
```

- [ ] **Step 6: Wire the callback in `cmd/main.go`**

Add to the `commonhttp.AdminCallbacks{}` initialization:

```go
GetConfig: func() any {
    return cfg // the *config.Configuration loaded earlier
},
```

- [ ] **Step 7: Run tests + build**

```bash
go test ./common/http/management/... -run TestNewConfigHandler -v
go build ./...
```

- [ ] **Step 8: Commit**

```bash
git add common/http/ cmd/main.go
git commit -m "feat(server): add GET /admin/config endpoint"
```

---

### Task 19: Server — `GET /admin/env` handler

**Goal:** Expose env vars + Go runtime + host info + build info per spec §2.E.6.

**Files:**
- Create: `common/http/management/env_handler.go`
- Create: `common/http/management/env_handler_test.go`
- Modify: `common/http/router.go` — add path constant
- Modify: `common/http/server.go` — extend `AdminCallbacks` + register
- Modify: `cmd/main.go` — wire the callback

- [ ] **Step 1: Write the handler test**

Create `common/http/management/env_handler_test.go`:

```go
package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewEnvHandler_Shape(t *testing.T) {
	handler := NewEnvHandler()

	req := httptest.NewRequest(http.MethodGet, "/admin/env", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var got struct {
		Env     map[string]string `json:"env"`
		Runtime struct {
			GoVersion    string `json:"go_version"`
			GoMaxProcs   int    `json:"gomaxprocs"`
			NumCPU       int    `json:"num_cpu"`
			NumGoroutine int    `json:"num_goroutine"`
		} `json:"runtime"`
		Host struct {
			Hostname string `json:"hostname"`
			OS       string `json:"os"`
			Arch     string `json:"arch"`
		} `json:"host"`
		Build struct {
			Version   string `json:"version"`
			Commit    string `json:"commit"`
			BuildTime string `json:"build_time"`
			GoVersion string `json:"go_version"`
		} `json:"build"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.NotEmpty(t, got.Runtime.GoVersion)
	require.NotEmpty(t, got.Host.OS)
	require.NotEmpty(t, got.Build.Version)
	// env PATH is almost always set on a test runner.
	_, hasPath := got.Env["PATH"]
	require.True(t, hasPath, "expected PATH in env section")
}

func TestNewEnvHandler_WrongMethod(t *testing.T) {
	handler := NewEnvHandler()
	req := httptest.NewRequest(http.MethodPost, "/admin/env", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./common/http/management/... -run TestNewEnvHandler -v
```

Expected: FAIL.

- [ ] **Step 3: Create the handler**

Create `common/http/management/env_handler.go`:

```go
package management

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"strings"

	"github.com/zilliztech/woodpecker/common/version"
)

type envResponse struct {
	Env     map[string]string `json:"env"`
	Runtime envRuntime        `json:"runtime"`
	Host    envHost           `json:"host"`
	Build   version.BuildInfo `json:"build"`
}

type envRuntime struct {
	GoVersion    string `json:"go_version"`
	GoMaxProcs   int    `json:"gomaxprocs"`
	GOGC         string `json:"gogc"`
	NumCPU       int    `json:"num_cpu"`
	NumGoroutine int    `json:"num_goroutine"`
	AllocBytes   uint64 `json:"memstats_alloc_bytes"`
	SysBytes     uint64 `json:"memstats_sys_bytes"`
}

type envHost struct {
	Hostname string `json:"hostname"`
	OS       string `json:"os"`
	Arch     string `json:"arch"`
}

// NewEnvHandler returns a handler for GET /admin/env.
// No arguments: the handler collects env vars, Go runtime info, host info,
// and build info at request time.
func NewEnvHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		envMap := make(map[string]string, 32)
		for _, kv := range os.Environ() {
			i := strings.IndexByte(kv, '=')
			if i < 0 {
				continue
			}
			envMap[kv[:i]] = kv[i+1:]
		}

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		hostname, _ := os.Hostname()

		resp := envResponse{
			Env: envMap,
			Runtime: envRuntime{
				GoVersion:    runtime.Version(),
				GoMaxProcs:   runtime.GOMAXPROCS(0),
				GOGC:         os.Getenv("GOGC"),
				NumCPU:       runtime.NumCPU(),
				NumGoroutine: runtime.NumGoroutine(),
				AllocBytes:   ms.Alloc,
				SysBytes:     ms.Sys,
			},
			Host: envHost{
				Hostname: hostname,
				OS:       runtime.GOOS,
				Arch:     runtime.GOARCH,
			},
			Build: version.Info(),
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(resp)
	}
}
```

- [ ] **Step 4: Add the path constant**

In `common/http/router.go`:

```go
// AdminEnvPath is path for GET /admin/env.
const AdminEnvPath = "/admin/env"
```

- [ ] **Step 5: Register in `Start()`**

In `common/http/server.go`, after other registrations:

```go
// /admin/env has no callback - it's self-contained.
Register(&Handler{
    Path:        AdminEnvPath,
    HandlerFunc: management.NewEnvHandler(),
})
```

> **Note:** This endpoint is unconditionally registered (no nil-check on a callback) because `NewEnvHandler()` takes no arguments — it reads `os.Environ()` etc. at request time.

- [ ] **Step 6: Run tests + build**

```bash
go test ./common/http/management/... -run TestNewEnvHandler -v
go build ./...
```

Expected: PASS + clean build.

- [ ] **Step 7: Commit**

```bash
git add common/http/
git commit -m "feat(server): add GET /admin/env endpoint"
```

---

## Part D — A Family: Node Lifecycle Commands (Tasks 20-25)

All A-family commands share a "resolver" helper: `(ContextSettings, *client.Client, *client.Memberlist, error) = resolveAndDiscover()`. Task 20 creates this helper; subsequent tasks reuse it.

### Task 20: `wp node list`

**Goal:** Fully wire the command surface: context → seed endpoint → memberlist → fan-out → table rendering. This is the first "real" command and establishes the pattern all subsequent commands follow.

**Files:**
- Create: `cmd/wpcli/cmd/resolver.go` (shared helper: resolves context, builds client, fetches memberlist)
- Create: `cmd/wpcli/cmd/node.go`
- Create: `cmd/wpcli/cmd/node_list.go`
- Create: `cmd/wpcli/cmd/node_list_test.go`
- Modify: `cmd/wpcli/cmd/root.go` — register `node` sub-command

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/node_list_test.go`:

```go
package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// spinTestServer mounts handlers for /admin/memberlist and /admin/node/status
// on a single httptest server. Used by multiple command tests in this package.
func spinTestServer(t *testing.T, memberlist string, nodeStatus map[string]string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(memberlist))
	})
	for id, body := range nodeStatus {
		id := id
		body := body
		mux.HandleFunc("/admin/node/status-for/"+id, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		})
	}
	mux.HandleFunc("/admin/node/status", func(w http.ResponseWriter, r *http.Request) {
		// Returns the status associated with whichever test context is active.
		// For node list tests we route per-id via Host header is complicated —
		// instead, we spin ONE server per node below.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"node_id":"node-default","state":"active","member_count":1}`))
	})
	return httptest.NewServer(mux)
}

// withCliYAML writes a cli.yaml pointing current-context at `prod` with the given
// endpoint and sets WOODPECKER_CLI_CONFIG to that file for this test.
func withCliYAML(t *testing.T, endpoint string) {
	t.Helper()
	dir := t.TempDir()
	cfg := `current-context: prod
contexts:
  prod:
    endpoint: ` + endpoint + `
`
	p := filepath.Join(dir, "cli.yaml")
	require.NoError(t, os.WriteFile(p, []byte(cfg), 0o600))
	t.Setenv("WOODPECKER_CLI_CONFIG", p)
}

func TestNodeList_HappyPath(t *testing.T) {
	// One fake server backs the memberlist AND both node/status responses,
	// because fanout will reuse the same Host:Port when the memberlist reports
	// the seed's own host.
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946","service_addr":"127.0.0.1:18080","az":"us-east-1a","rg":"default","state":0,"incarnation":1,"last_seen_ms":1}
	]}`
	srv := spinTestServer(t, ml, nil)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	// Make fanout dial the test server's port for admin calls.
	// We override --admin-port from the global default 9091 to the ephemeral port.
	adminPort := extractPort(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "list", "--admin-port", adminPort, "-o", "json"})

	require.NoError(t, root.Execute())

	var rows []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rows))
	require.Len(t, rows, 1)
	require.Equal(t, "node-1", rows[0]["name"])
}
```

> **Guardrail:** `extractPort` is a tiny test helper that parses `httptest.Server.URL` and returns the port as a string. Add it to `node_list_test.go` (or a shared `helpers_test.go` in the same package). Implementation:
>
> ```go
> func extractPort(t *testing.T, rawURL string) string {
> 	t.Helper()
> 	u, err := url.Parse(rawURL)
> 	require.NoError(t, err)
> 	return u.Port()
> }
> ```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/cmd/... -run TestNodeList -v
```

Expected: FAIL — `node list` command does not exist.

- [ ] **Step 3: Create the shared resolver**

Create `cmd/wpcli/cmd/resolver.go`:

```go
package cmd

import (
	"fmt"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

// resolved carries everything a command needs after context resolution.
type resolved struct {
	Context   config.Context
	Client    *client.Client
	Members   *client.Memberlist
}

// resolveAndDiscover loads cli.yaml, applies flag/env overrides, builds the
// admin HTTP client, and fetches the memberlist. This is called at the start
// of nearly every command that touches cluster state.
func resolveAndDiscover() (*resolved, error) {
	// 1. Load cli.yaml (if present).
	var ctx config.Context
	for _, p := range config.DefaultConfigPaths() {
		f, err := config.Load(p)
		if err == nil {
			c, err := f.ResolveContext(Globals.Context)
			if err != nil {
				return nil, wperrors.NewConfigError(err.Error())
			}
			ctx = c
			break
		}
	}

	// 2. Flag / env overrides on top of context.
	if Globals.Endpoint != "" {
		ctx.Endpoint = Globals.Endpoint
	}
	if Globals.AdminPort != 0 {
		ctx.AdminPort = Globals.AdminPort
	}
	if Globals.Timeout != 0 {
		ctx.Timeout = Globals.Timeout
	}
	if Globals.Concurrency != 0 {
		ctx.Concurrency = Globals.Concurrency
	}
	if Globals.Strict {
		ctx.Strict = true
	}

	// 3. Validate.
	if ctx.Endpoint == "" {
		return nil, wperrors.NewUsageError("no endpoint configured (set --endpoint, $WOODPECKER_ENDPOINT, or cli.yaml context)")
	}

	// 4. Build the seed client and fetch memberlist.
	c := client.New(ctx.Endpoint, client.ClientOpts{
		Timeout:   ctx.Timeout,
		AdminPort: ctx.AdminPort,
	})
	ml, err := c.GetMemberlist()
	if err != nil {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("fetch memberlist from %s: %v", ctx.Endpoint, err))
	}

	return &resolved{Context: ctx, Client: c, Members: ml}, nil
}
```

- [ ] **Step 4: Create the `node` sub-command group**

Create `cmd/wpcli/cmd/node.go`:

```go
package cmd

import "github.com/spf13/cobra"

func newNodeCommand() *cobra.Command {
	n := &cobra.Command{
		Use:   "node",
		Short: "Manage and inspect server nodes",
	}
	n.AddCommand(
		newNodeListCommand(),
		// subsequent tasks will add more here:
		// newNodeShowCommand(),
		// newNodeDecommissionCommand(),
		// newNodeDrainStatusCommand(),
		// newNodeCancelDecommissionCommand(),
		// newNodeRestartCommand(),
	)
	return n
}
```

- [ ] **Step 5: Create `node list` command**

Create `cmd/wpcli/cmd/node_list.go`:

```go
package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

type nodeListRow struct {
	Name   string `json:"name"`
	Addr   string `json:"addr"`
	State  string `json:"state"`
	AZ     string `json:"az"`
	RG     string `json:"rg"`
	Health string `json:"health"`
}

func newNodeListCommand() *cobra.Command {
	var (
		filterAZ    string
		filterRG    string
		filterState string
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all server nodes in the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			// Fan out to every peer and hit /admin/node/status
			var urls []string
			for _, m := range r.Members.Members {
				urls = append(urls, r.Client.PeerAdminURL(m))
			}
			f := client.NewFanout(client.FanoutOpts{
				Concurrency: r.Context.Concurrency,
				Timeout:     r.Context.Timeout,
				Strict:      r.Context.Strict,
			})
			res := f.Get(urls, "/admin/node/status", "")
			if res.StrictFailure() {
				return wperrors.NewStrictPartialFailureError(res.Unreachable, len(urls))
			}

			// Build rows by pairing memberlist entries with fanout results (index-aligned).
			rows := make([]nodeListRow, 0, len(r.Members.Members))
			for i, m := range r.Members.Members {
				row := nodeListRow{
					Name: m.ID,
					Addr: m.ServiceAddr,
					AZ:   m.AZ,
					RG:   m.RG,
				}
				nr := res.Results[i]
				if nr.OK {
					var s struct {
						State             string `json:"state"`
						IsDecommissioning bool   `json:"is_decommissioning"`
					}
					_ = json.Unmarshal(nr.Body, &s)
					if s.State == "" {
						s.State = "active"
					}
					row.State = s.State
					row.Health = "OK"
				} else {
					row.State = "UNREACHABLE"
					row.Health = "FAIL"
				}
				// Apply filters.
				if filterAZ != "" && row.AZ != filterAZ {
					continue
				}
				if filterRG != "" && row.RG != filterRG {
					continue
				}
				if filterState != "" && filterState != "all" && row.State != filterState {
					continue
				}
				rows = append(rows, row)
			}

			return renderNodeList(cmd, rows)
		},
	}
	cmd.Flags().StringVar(&filterAZ, "az", "", "filter by availability zone")
	cmd.Flags().StringVar(&filterRG, "rg", "", "filter by resource group")
	cmd.Flags().StringVar(&filterState, "state", "all", "filter by state: active|decommissioning|decommissioned|unreachable|all")
	return cmd
}

func renderNodeList(cmd *cobra.Command, rows []nodeListRow) error {
	w := cmd.OutOrStdout()
	switch Globals.Output {
	case "json":
		return output.RenderJSON(w, rows)
	case "yaml":
		return output.RenderYAML(w, rows)
	case "wide":
		fallthrough
	default:
		headers := []string{"NAME", "ADDR", "STATE", "AZ", "RG", "HEALTH"}
		table := make([][]string, len(rows))
		for i, r := range rows {
			table[i] = []string{r.Name, r.Addr, r.State, r.AZ, r.RG, r.Health}
		}
		return output.RenderRowTable(w, headers, table)
	}
}

// Suppress unused import warning; `fmt` is kept for future additions.
var _ = fmt.Sprintf
```

- [ ] **Step 6: Register `node` in root**

In `cmd/wpcli/cmd/root.go`, add to `NewRootCommand`:

```go
	root.AddCommand(newNodeCommand())
```

- [ ] **Step 7: Run tests + build**

```bash
go test ./cmd/wpcli/cmd/... -run TestNodeList -v
make wpcli
```

Expected: PASS + clean build.

- [ ] **Step 8: Commit**

```bash
git add cmd/wpcli/cmd/resolver.go cmd/wpcli/cmd/node.go cmd/wpcli/cmd/node_list.go cmd/wpcli/cmd/node_list_test.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add node list command with fan-out + resolver"
```

---

### Task 21: `wp node show`

**Goal:** Show detailed status for one node. Uses sectioned table renderer.

**Files:**
- Create: `cmd/wpcli/cmd/node_show.go`
- Create: `cmd/wpcli/cmd/node_show_test.go`
- Modify: `cmd/wpcli/cmd/node.go` — register sub-command

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/node_show_test.go`:

```go
package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeShow_HappyPath(t *testing.T) {
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946","service_addr":"127.0.0.1:18080","az":"us-east-1a","rg":"default","state":0}
	]}`
	status := `{
		"node_id":"node-1","state":"active","is_decommissioning":false,
		"member_count":1,"address":"127.0.0.1:18080","resource_group":"default","az":"us-east-1a",
		"tags":{"role":"logstore"},
		"started_at_ms":1712500000000,"version":"v0.1.26-test","last_health_check_ms":1712553600000
	}`

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(ml))
	})
	mux.HandleFunc("/admin/node/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(status))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "show", "node-1", "--admin-port", extractPort(t, srv.URL)})

	require.NoError(t, root.Execute())
	out := buf.String()
	require.Contains(t, out, "node-1")
	require.Contains(t, out, "Identity")
	require.Contains(t, out, "Lifecycle")
	require.Contains(t, out, "v0.1.26-test")
}

func TestNodeShow_NotInMemberlist(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(ml))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "show", "node-99", "--admin-port", extractPort(t, srv.URL)})

	err := root.Execute()
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/cmd/... -run TestNodeShow -v
```

Expected: FAIL.

- [ ] **Step 3: Create `cmd/wpcli/cmd/node_show.go`**

```go
package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

// nodeStatusDTO mirrors server.NodeStatus JSON on the wire.
type nodeStatusDTO struct {
	NodeID            string            `json:"node_id"`
	State             string            `json:"state"`
	IsDecommissioning bool              `json:"is_decommissioning"`
	MemberCount       int               `json:"member_count"`
	Address           string            `json:"address"`
	ResourceGroup     string            `json:"resource_group"`
	AZ                string            `json:"az"`
	Tags              map[string]string `json:"tags"`
	StartedAtMS       int64             `json:"started_at_ms"`
	Version           string            `json:"version"`
	LastHealthCheckMS int64             `json:"last_health_check_ms"`
}

func newNodeShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <node>",
		Short: "Show detailed status for a single node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			target, ok := r.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}

			peerURL := r.Client.PeerAdminURL(target)
			resp, err := http.Get(peerURL + "/admin/node/status")
			if err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("fetch node status: %v", err))
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return wperrors.NewNetworkError(fmt.Sprintf("node status returned %d", resp.StatusCode))
			}
			var dto nodeStatusDTO
			if err := json.NewDecoder(resp.Body).Decode(&dto); err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("decode node status: %v", err))
			}

			return renderNodeShow(cmd, target.ID, dto)
		},
	}
}

func renderNodeShow(cmd *cobra.Command, nodeID string, dto nodeStatusDTO) error {
	w := cmd.OutOrStdout()
	switch Globals.Output {
	case "json":
		return output.RenderJSON(w, dto)
	case "yaml":
		return output.RenderYAML(w, dto)
	default:
		startedAt := time.UnixMilli(dto.StartedAtMS).UTC().Format(time.RFC3339)
		uptime := time.Since(time.UnixMilli(dto.StartedAtMS)).Round(time.Second).String()
		sections := []output.Section{
			{
				Title: "Identity",
				Pairs: [][2]string{
					{"node_id", dto.NodeID},
					{"address", dto.Address},
					{"version", dto.Version},
				},
			},
			{
				Title: "Placement",
				Pairs: [][2]string{
					{"az", dto.AZ},
					{"rg", dto.ResourceGroup},
				},
			},
			{
				Title: "Lifecycle",
				Pairs: [][2]string{
					{"state", dto.State},
					{"is_decommissioning", fmt.Sprintf("%v", dto.IsDecommissioning)},
					{"started_at", startedAt},
					{"uptime", uptime},
				},
			},
			{
				Title: "Health",
				Pairs: [][2]string{
					{"member_count", fmt.Sprintf("%d", dto.MemberCount)},
					{"last_health_check", time.UnixMilli(dto.LastHealthCheckMS).UTC().Format(time.RFC3339)},
				},
			},
		}
		return output.RenderSectionedTable(w, sections)
	}
}
```

- [ ] **Step 4: Register in `node.go`**

Modify `cmd/wpcli/cmd/node.go` — uncomment / add `newNodeShowCommand()` in the `AddCommand` call.

- [ ] **Step 5: Run tests + build**

```bash
go test ./cmd/wpcli/cmd/... -run TestNodeShow -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/wpcli/cmd/node_show.go cmd/wpcli/cmd/node_show_test.go cmd/wpcli/cmd/node.go
git commit -m "feat(wpcli): add node show command with sectioned output"
```

---

### Task 22: `wp node decommission` (with wait + heartbeat)

**Goal:** Trigger decommission and (by default) wait for `safe_to_terminate=true`, printing progress + heartbeat lines every 15s when nothing changes.

**Files:**
- Create: `cmd/wpcli/cmd/node_decommission.go`
- Create: `cmd/wpcli/cmd/node_decommission_test.go`
- Modify: `cmd/wpcli/cmd/node.go` — register

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/node_decommission_test.go`:

```go
package cmd

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeDecommission_AsyncMode(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946","service_addr":"127.0.0.1:18080"}]}`
	var decommissionCalled int32

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/decommission", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&decommissionCalled, 1)
		_, _ = w.Write([]byte(`{"status":"decommission started"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{
		"node", "decommission", "node-1",
		"--async", "-y",
		"--admin-port", extractPort(t, srv.URL),
	})
	require.NoError(t, root.Execute())
	require.Equal(t, int32(1), atomic.LoadInt32(&decommissionCalled))
	require.Contains(t, buf.String(), "decommission started")
}

func TestNodeDecommission_WaitUntilSafe(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946","service_addr":"127.0.0.1:18080"}]}`
	var progressCalls int32

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/decommission", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"started"}`))
	})
	mux.HandleFunc("/admin/node/decommission/progress", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&progressCalls, 1)
		safe := "false"
		if n >= 3 {
			safe = "true"
		}
		_, _ = fmt.Fprintf(w, `{"state":"decommissioning","remaining_processors":%d,"has_local_data":false,"safe_to_terminate":%s}`, 4-n, safe)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{
		"node", "decommission", "node-1", "-y",
		"--interval", "10ms",
		"--heartbeat-interval", "100ms",
		"--timeout", "5s",
		"--admin-port", extractPort(t, srv.URL),
	})
	require.NoError(t, root.Execute())
	require.GreaterOrEqual(t, atomic.LoadInt32(&progressCalls), int32(3))
	require.Contains(t, buf.String(), "Safe to terminate")
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/cmd/... -run TestNodeDecommission -v
```

Expected: FAIL.

- [ ] **Step 3: Create `cmd/wpcli/cmd/node_decommission.go`**

```go
package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

type decommissionProgress struct {
	State               string `json:"state"`
	RemainingProcessors int    `json:"remaining_processors"`
	HasLocalData        bool   `json:"has_local_data"`
	SafeToTerminate     bool   `json:"safe_to_terminate"`
}

func newNodeDecommissionCommand() *cobra.Command {
	var (
		async             bool
		timeout           time.Duration
		interval          time.Duration
		heartbeatInterval time.Duration
		yes               bool
	)
	cmd := &cobra.Command{
		Use:   "decommission <node>",
		Short: "Trigger graceful node decommission (blocks until safe by default)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			target, ok := r.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}

			if !yes && !promptConfirm(cmd, fmt.Sprintf("decommission node %q", target.ID)) {
				return wperrors.NewUserAbortError()
			}

			peerURL := r.Client.PeerAdminURL(target)
			// POST /admin/node/decommission
			resp, err := http.Post(peerURL+"/admin/node/decommission", "application/json", bytes.NewReader(nil))
			if err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("POST decommission: %v", err))
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusConflict {
				return wperrors.NewStateConflictError("node is not in active state")
			}
			if resp.StatusCode != http.StatusOK {
				return wperrors.NewNetworkError(fmt.Sprintf("decommission returned status %d", resp.StatusCode))
			}
			fmt.Fprintf(cmd.OutOrStdout(), "decommission started on %s\n", target.ID)

			if async {
				return nil
			}

			// Blocking wait loop with heartbeat.
			return waitForSafeTermination(cmd, peerURL, timeout, interval, heartbeatInterval)
		},
	}
	cmd.Flags().BoolVar(&async, "async", false, "return immediately instead of waiting for safe_to_terminate")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Minute, "overall timeout for the wait")
	cmd.Flags().DurationVar(&interval, "interval", 3*time.Second, "progress poll interval")
	cmd.Flags().DurationVar(&heartbeatInterval, "heartbeat-interval", 15*time.Second, "heartbeat line interval when no progress change")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip interactive confirmation")
	return cmd
}

func waitForSafeTermination(cmd *cobra.Command, peerURL string, timeout, interval, heartbeat time.Duration) error {
	w := cmd.OutOrStdout()
	deadline := time.Now().Add(timeout)
	var last decommissionProgress
	lastPrint := time.Now()
	first := true

	for {
		if time.Now().After(deadline) {
			return wperrors.NewWaitTimeoutError("decommission", int(timeout.Seconds()))
		}
		resp, err := http.Get(peerURL + "/admin/node/decommission/progress")
		if err != nil {
			return wperrors.NewNetworkError(fmt.Sprintf("GET progress: %v", err))
		}
		var p decommissionProgress
		_ = json.NewDecoder(resp.Body).Decode(&p)
		resp.Body.Close()

		changed := first || p != last
		heartbeatDue := !first && time.Since(lastPrint) >= heartbeat
		if changed || heartbeatDue {
			suffix := ""
			if !changed && heartbeatDue {
				suffix = "   (heartbeat: no change; see `wp ops list <node>` for in-flight work)"
			}
			fmt.Fprintf(w, "[%s] state=%s remaining_processors=%d has_local_data=%v safe=%v%s\n",
				time.Now().UTC().Format("15:04:05"),
				p.State, p.RemainingProcessors, p.HasLocalData, p.SafeToTerminate, suffix)
			lastPrint = time.Now()
			last = p
			first = false
		}
		if p.SafeToTerminate {
			fmt.Fprintln(w, "Safe to terminate.")
			return nil
		}
		time.Sleep(interval)
	}
}

// promptConfirm reads stdin for y/N. Returns true on "y"/"yes", false otherwise.
// In non-tty mode the caller should pass --yes; this helper does NOT check tty,
// it just reads from the command's InOrStdin.
func promptConfirm(cmd *cobra.Command, action string) bool {
	fmt.Fprintf(cmd.ErrOrStderr(), "About to %s. Proceed? [y/N]: ", action)
	buf := make([]byte, 8)
	n, _ := cmd.InOrStdin().Read(buf)
	if n == 0 {
		return false
	}
	first := buf[0]
	return first == 'y' || first == 'Y'
}
```

- [ ] **Step 4: Register + run tests**

Update `cmd/wpcli/cmd/node.go` to add `newNodeDecommissionCommand()` in `AddCommand`.

```bash
go test ./cmd/wpcli/cmd/... -run TestNodeDecommission -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/cmd/node_decommission.go cmd/wpcli/cmd/node_decommission_test.go cmd/wpcli/cmd/node.go
git commit -m "feat(wpcli): add node decommission command with wait and heartbeat"
```

---

### Task 23: `wp node drain-status` (with `--watch`)

**Goal:** Read-only observation of decommission progress. Supports `-w/--watch` for continuous polling.

**Files:**
- Create: `cmd/wpcli/cmd/node_drain_status.go`
- Create: `cmd/wpcli/cmd/node_drain_status_test.go`
- Modify: `cmd/wpcli/cmd/node.go` — register

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/node_drain_status_test.go`:

```go
package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeDrainStatus_OneShot(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	progress := `{"state":"decommissioning","remaining_processors":3,"has_local_data":true,"safe_to_terminate":false}`

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/decommission/progress", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(progress))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "drain-status", "node-1", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "decommissioning")
	require.Contains(t, buf.String(), "remaining_processors")
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/cmd/... -run TestNodeDrainStatus -v
```

- [ ] **Step 3: Create `cmd/wpcli/cmd/node_drain_status.go`**

```go
package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newNodeDrainStatusCommand() *cobra.Command {
	var (
		watch    bool
		interval time.Duration
		timeout  time.Duration
	)
	cmd := &cobra.Command{
		Use:   "drain-status <node>",
		Short: "Show (or watch) decommission progress for a node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			target, ok := r.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}
			peerURL := r.Client.PeerAdminURL(target)

			fetch := func() (*decommissionProgress, error) {
				resp, err := http.Get(peerURL + "/admin/node/decommission/progress")
				if err != nil {
					return nil, wperrors.NewNetworkError(err.Error())
				}
				defer resp.Body.Close()
				var p decommissionProgress
				if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
					return nil, wperrors.NewNetworkError(fmt.Sprintf("decode progress: %v", err))
				}
				return &p, nil
			}

			render := func(p *decommissionProgress) error {
				return output.RenderSectionedTable(cmd.OutOrStdout(), []output.Section{{
					Title: fmt.Sprintf("Drain status for %s", target.ID),
					Pairs: [][2]string{
						{"state", p.State},
						{"remaining_processors", fmt.Sprintf("%d", p.RemainingProcessors)},
						{"has_local_data", fmt.Sprintf("%v", p.HasLocalData)},
						{"safe_to_terminate", fmt.Sprintf("%v", p.SafeToTerminate)},
					},
				}})
			}

			if !watch {
				p, err := fetch()
				if err != nil {
					return err
				}
				return render(p)
			}

			deadline := time.Now().Add(timeout)
			for {
				if time.Now().After(deadline) {
					return wperrors.NewWaitTimeoutError("drain-status watch", int(timeout.Seconds()))
				}
				p, err := fetch()
				if err != nil {
					return err
				}
				_ = render(p)
				if p.SafeToTerminate {
					return nil
				}
				time.Sleep(interval)
			}
		},
	}
	cmd.Flags().BoolVarP(&watch, "watch", "w", false, "continuously watch until safe_to_terminate")
	cmd.Flags().DurationVar(&interval, "interval", 3*time.Second, "watch poll interval")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Minute, "watch total timeout")
	return cmd
}
```

- [ ] **Step 4: Register + tests**

Update `cmd/wpcli/cmd/node.go`. Run:

```bash
go test ./cmd/wpcli/cmd/... -run TestNodeDrainStatus -v
```

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/cmd/node_drain_status.go cmd/wpcli/cmd/node_drain_status_test.go cmd/wpcli/cmd/node.go
git commit -m "feat(wpcli): add node drain-status command with watch mode"
```

---

### Task 24: `wp node cancel-decommission`

**Goal:** Call the new `POST /admin/node/decommission/cancel` endpoint (Task 17).

**Files:**
- Create: `cmd/wpcli/cmd/node_cancel_decommission.go`
- Create: `cmd/wpcli/cmd/node_cancel_decommission_test.go`
- Modify: `cmd/wpcli/cmd/node.go`

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/node_cancel_decommission_test.go`:

```go
package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeCancelDecommission_HappyPath(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	var called int32
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/decommission/cancel", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&called, 1)
		require.Equal(t, http.MethodPost, r.Method)
		_, _ = w.Write([]byte(`{"status":"decommission cancelled"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "cancel-decommission", "node-1", "-y", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Equal(t, int32(1), atomic.LoadInt32(&called))
	require.Contains(t, buf.String(), "cancelled")
}

func TestNodeCancelDecommission_Conflict(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/decommission/cancel", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"already decommissioned"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "cancel-decommission", "node-1", "-y", "--admin-port", extractPort(t, srv.URL)})
	err := root.Execute()
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Create `cmd/wpcli/cmd/node_cancel_decommission.go`**

```go
package cmd

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newNodeCancelDecommissionCommand() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "cancel-decommission <node>",
		Short: "Cancel an in-progress decommission (decommissioning → active)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			target, ok := r.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}
			if !yes && !promptConfirm(cmd, fmt.Sprintf("cancel decommission of %q", target.ID)) {
				return wperrors.NewUserAbortError()
			}
			peerURL := r.Client.PeerAdminURL(target)
			resp, err := http.Post(peerURL+"/admin/node/decommission/cancel", "application/json", bytes.NewReader(nil))
			if err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("POST cancel: %v", err))
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusConflict {
				return wperrors.NewStateConflictError("cannot cancel: node is not decommissioning (may already be decommissioned)")
			}
			if resp.StatusCode != http.StatusOK {
				return wperrors.NewNetworkError(fmt.Sprintf("cancel returned status %d", resp.StatusCode))
			}
			fmt.Fprintf(cmd.OutOrStdout(), "decommission cancelled on %s; returning to active\n", target.ID)
			return nil
		},
	}
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip interactive confirmation")
	return cmd
}
```

- [ ] **Step 4: Register + tests**

Update `cmd/wpcli/cmd/node.go`. Run:

```bash
go test ./cmd/wpcli/cmd/... -run TestNodeCancelDecommission -v
```

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/cmd/node_cancel_decommission.go cmd/wpcli/cmd/node_cancel_decommission_test.go cmd/wpcli/cmd/node.go
git commit -m "feat(wpcli): add node cancel-decommission command"
```

---

### Task 25: `wp node restart` (stub)

**Goal:** Intentionally-not-implemented stub per spec §2.A.6. Prints guidance and exits `10`.

**Files:**
- Create: `cmd/wpcli/cmd/node_restart.go`
- Create: `cmd/wpcli/cmd/node_restart_test.go`
- Modify: `cmd/wpcli/cmd/node.go`

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/node_restart_test.go`:

```go
package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func TestNodeRestart_StubExit10(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "restart", "node-1"})

	err := root.Execute()
	require.Error(t, err)
	require.Equal(t, 10, wperrors.ExitCodeFor(err))
	require.Contains(t, buf.String(), "intentionally not implemented")
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Create the stub**

```go
// cmd/wpcli/cmd/node_restart.go
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newNodeRestartCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "restart <node>",
		Short: "(Not implemented) Restart a node — use your orchestrator instead",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), `wp node restart is intentionally not implemented by this CLI.

On Kubernetes (managed by Woodpecker Operator):
  kubectl rollout restart sts/<cluster-name>-server

On bare metal / systemd:
  ssh to the node and run: sudo systemctl restart woodpecker

Why: the wp CLI does not assume a specific supervisor for process restart.
Restart responsibility belongs to the orchestrator.`)
			return wperrors.NewNotImplementedError("wp node restart")
		},
	}
}
```

- [ ] **Step 4: Register + test**

Update `cmd/wpcli/cmd/node.go`. Run:

```bash
go test ./cmd/wpcli/cmd/... -run TestNodeRestart -v
```

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/cmd/node_restart.go cmd/wpcli/cmd/node_restart_test.go cmd/wpcli/cmd/node.go
git commit -m "feat(wpcli): add node restart stub with exit code 10"
```

---

## Part E — B Family: Cluster Overview Commands (Tasks 26-28)

### Task 26: `wp cluster info` (overview + topology tree)

**Goal:** Single-page cluster snapshot with overview block + ASCII tree. Uses `tree` renderer from Task 10.

**Files:**
- Create: `cmd/wpcli/cmd/cluster.go`
- Create: `cmd/wpcli/cmd/cluster_info.go`
- Create: `cmd/wpcli/cmd/cluster_info_test.go`
- Modify: `cmd/wpcli/cmd/root.go` — register `cluster` sub-command

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/cluster_info_test.go`:

```go
package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterInfo_HappyPath(t *testing.T) {
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946","az":"us-east-1a","rg":"default"},
		{"id":"node-2","gossip_addr":"127.0.0.1:17946","az":"us-east-1a","rg":"default"},
		{"id":"node-3","gossip_addr":"127.0.0.1:17946","az":"us-east-1b","rg":"default"}
	]}`

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(ml))
	})
	mux.HandleFunc("/admin/node/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"node_id":"x","state":"active","member_count":3}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"cluster", "info", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	out := buf.String()
	require.Contains(t, out, "Total Nodes")
	require.Contains(t, out, "us-east-1a")
	require.Contains(t, out, "node-1")
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/cmd/... -run TestClusterInfo -v
```

- [ ] **Step 3: Create the `cluster` sub-command group**

Create `cmd/wpcli/cmd/cluster.go`:

```go
package cmd

import "github.com/spf13/cobra"

func newClusterCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster-level overview and health checks",
	}
	c.AddCommand(
		newClusterInfoCommand(),
		// Tasks 27 and 28 add:
		// newClusterHealthCommand(),
		// newClusterGossipDiffCommand(),
	)
	return c
}
```

- [ ] **Step 4: Create `cluster info` command**

Create `cmd/wpcli/cmd/cluster_info.go`:

```go
package cmd

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newClusterInfoCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Show cluster overview (nodes, state, topology)",
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			// Fan out to each peer's /admin/node/status to learn each node's state.
			var urls []string
			for _, m := range r.Members.Members {
				urls = append(urls, r.Client.PeerAdminURL(m))
			}
			f := client.NewFanout(client.FanoutOpts{
				Concurrency: r.Context.Concurrency,
				Timeout:     r.Context.Timeout,
				Strict:      r.Context.Strict,
			})
			res := f.Get(urls, "/admin/node/status", "")
			if res.StrictFailure() {
				return wperrors.NewStrictPartialFailureError(res.Unreachable, len(urls))
			}

			// Compute state counts and group by AZ/RG.
			type nodeInfo struct {
				member client.Member
				state  string
			}
			byAZRG := make(map[string]map[string][]nodeInfo) // az → rg → nodes
			stateCounts := map[string]int{}
			for i, m := range r.Members.Members {
				state := "UNREACHABLE"
				if res.Results[i].OK {
					var s struct {
						State string `json:"state"`
					}
					_ = json.Unmarshal(res.Results[i].Body, &s)
					if s.State != "" {
						state = s.State
					} else {
						state = "active"
					}
				}
				stateCounts[state]++
				if byAZRG[m.AZ] == nil {
					byAZRG[m.AZ] = make(map[string][]nodeInfo)
				}
				byAZRG[m.AZ][m.RG] = append(byAZRG[m.AZ][m.RG], nodeInfo{member: m, state: state})
			}

			return renderClusterInfo(cmd, r, stateCounts, byAZRG)
		},
	}
}

func renderClusterInfo(cmd *cobra.Command, r *resolved, states map[string]int, byAZRG map[string]map[string][]struct {
	member client.Member
	state  string
}) error {
	w := cmd.OutOrStdout()

	// Build overview block
	fmt.Fprintln(w, "Cluster Overview")
	fmt.Fprintf(w, "  Endpoint:   %s\n", r.Context.Endpoint)
	fmt.Fprintf(w, "  Total Nodes: %d\n", len(r.Members.Members))
	fmt.Fprintln(w)
	fmt.Fprintln(w, "By State")
	for _, k := range sortedKeys(states) {
		fmt.Fprintf(w, "  %s: %d\n", k, states[k])
	}
	fmt.Fprintln(w)

	// Build topology tree
	root := &output.TreeNode{Label: "Topology"}
	for _, az := range sortedKeys2D(byAZRG) {
		azNode := &output.TreeNode{Label: az}
		for _, rg := range sortedKeysInner(byAZRG[az]) {
			rgNode := &output.TreeNode{Label: rg}
			for _, ni := range byAZRG[az][rg] {
				rgNode.Children = append(rgNode.Children, &output.TreeNode{
					Label: fmt.Sprintf("%s  %s", ni.member.ID, ni.state),
				})
			}
			azNode.Children = append(azNode.Children, rgNode)
		}
		root.Children = append(root.Children, azNode)
	}
	return output.RenderTree(w, root)
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeys2D(m map[string]map[string][]struct {
	member client.Member
	state  string
}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeysInner(m map[string][]struct {
	member client.Member
	state  string
}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
```

> **Implementation note:** Go doesn't let you pass anonymous-struct map types around easily. If the compile complains about the anonymous-struct-in-map-value, promote `nodeInfo` to a package-level type and use it in both `newClusterInfoCommand` and the sort helpers. Keep it local to this file.

- [ ] **Step 5: Register + test**

Add `newClusterCommand()` to `NewRootCommand()` in `cmd/wpcli/cmd/root.go`. Run:

```bash
go test ./cmd/wpcli/cmd/... -run TestClusterInfo -v
```

- [ ] **Step 6: Commit**

```bash
git add cmd/wpcli/cmd/cluster.go cmd/wpcli/cmd/cluster_info.go cmd/wpcli/cmd/cluster_info_test.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add cluster info command with topology tree"
```

---

### Task 27: `wp cluster health` (replica-aware)

**Goal:** Red/yellow/green health check per spec §2.B (now the locked "B.2"). Phase 1 uses the **fallback path only** (default `N=3`, no metric-based log replica count); the metric-based path is Phase 2 (requires Task 28+ infrastructure).

**Files:**
- Create: `cmd/wpcli/cmd/cluster_health.go`
- Create: `cmd/wpcli/cmd/cluster_health_test.go`
- Modify: `cmd/wpcli/cmd/cluster.go`

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/cluster_health_test.go`:

```go
package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func TestClusterHealth_Green(t *testing.T) {
	// 3 nodes, all reachable, all active
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946","az":"us-east-1a"},
		{"id":"node-2","gossip_addr":"127.0.0.1:17946","az":"us-east-1b"},
		{"id":"node-3","gossip_addr":"127.0.0.1:17946","az":"us-east-1c"}
	]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"node_id":"x","state":"active","version":"v0.1.26"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"cluster", "health", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "GREEN")
}

func TestClusterHealth_Red_OneUnreachable(t *testing.T) {
	// 3 members but one server is dead → strict mode off still reports red because
	// active_nodes_total = 2 < N=3.
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946","az":"us-east-1a"},
		{"id":"node-2","gossip_addr":"127.0.0.1:17946","az":"us-east-1b"},
		{"id":"node-3","gossip_addr":"10.255.255.255:17946","az":"us-east-1c"}
	]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"node_id":"x","state":"active","version":"v0.1.26"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"cluster", "health", "--admin-port", extractPort(t, srv.URL)})
	err := root.Execute()
	require.Error(t, err)
	require.Equal(t, 9, wperrors.ExitCodeFor(err))
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/cmd/... -run TestClusterHealth -v
```

- [ ] **Step 3: Create `cmd/wpcli/cmd/cluster_health.go`**

```go
package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newClusterHealthCommand() *cobra.Command {
	var expectedReplicas int
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Red/yellow/green health check for the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			// Phase 1: use cluster-level fallback only. Per-log replica health
			// is Phase 2 (requires scraping /metrics with the new scenario engine).
			N := expectedReplicas

			var urls []string
			for _, m := range r.Members.Members {
				urls = append(urls, r.Client.PeerAdminURL(m))
			}
			f := client.NewFanout(client.FanoutOpts{
				Concurrency: r.Context.Concurrency,
				Timeout:     r.Context.Timeout,
			})
			res := f.Get(urls, "/admin/node/status", "")

			activeCount := 0
			decommissioningCount := 0
			unreachableCount := res.Unreachable
			versions := make(map[string]int)
			for i, m := range r.Members.Members {
				_ = m
				if !res.Results[i].OK {
					continue
				}
				var s struct {
					State             string `json:"state"`
					Version           string `json:"version"`
					IsDecommissioning bool   `json:"is_decommissioning"`
				}
				_ = json.Unmarshal(res.Results[i].Body, &s)
				if s.IsDecommissioning || s.State == "decommissioning" {
					decommissioningCount++
				} else if s.State == "active" || s.State == "" {
					activeCount++
				}
				if s.Version != "" {
					versions[s.Version]++
				}
			}

			verdict := "GREEN"
			var reasons []string
			// RED conditions
			if activeCount < N {
				verdict = "RED"
				reasons = append(reasons, fmt.Sprintf("active nodes %d < expected replicas %d", activeCount, N))
			}
			if unreachableCount > 0 {
				verdict = "RED"
				reasons = append(reasons, fmt.Sprintf("%d nodes unreachable", unreachableCount))
			}
			// YELLOW conditions (only if not already RED)
			if verdict != "RED" {
				if decommissioningCount > 0 {
					verdict = "YELLOW"
					reasons = append(reasons, fmt.Sprintf("%d nodes decommissioning", decommissioningCount))
				}
				if len(versions) > 1 {
					verdict = "YELLOW"
					reasons = append(reasons, fmt.Sprintf("version skew: %d distinct versions", len(versions)))
				}
				if activeCount == N {
					verdict = "YELLOW"
					reasons = append(reasons, "zero redundancy (active == N)")
				}
			}

			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "Cluster Health: %s\n", verdict)
			fmt.Fprintf(w, "  expected_replicas: %d (fallback; metric-based check is Phase 2)\n", N)
			fmt.Fprintf(w, "  active: %d  decommissioning: %d  unreachable: %d\n", activeCount, decommissioningCount, unreachableCount)
			for _, rsn := range reasons {
				fmt.Fprintf(w, "  - %s\n", rsn)
			}

			switch verdict {
			case "RED":
				return wperrors.NewRedFindingError("cluster health RED")
			case "YELLOW":
				return wperrors.NewYellowFindingError("cluster health YELLOW")
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&expectedReplicas, "expected-replicas", 3, "expected replica count baseline (default 3)")
	return cmd
}
```

- [ ] **Step 4: Register + tests**

Update `cmd/wpcli/cmd/cluster.go` to add `newClusterHealthCommand()`. Run:

```bash
go test ./cmd/wpcli/cmd/... -run TestClusterHealth -v
```

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/cmd/cluster_health.go cmd/wpcli/cmd/cluster_health_test.go cmd/wpcli/cmd/cluster.go
git commit -m "feat(wpcli): add cluster health command (fallback mode, replica-aware Phase 2)"
```

---

### Task 28: `wp cluster gossip-diff`

**Goal:** Ask each node for its view of the memberlist, compare, report any divergence. Phase 1 does **set-level comparison only** (which members does each node see); per-member incarnation/state comparison is a nice-to-have that stays in Phase 1 if time permits.

**Files:**
- Create: `cmd/wpcli/cmd/cluster_gossip_diff.go`
- Create: `cmd/wpcli/cmd/cluster_gossip_diff_test.go`
- Modify: `cmd/wpcli/cmd/cluster.go`

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/cluster_gossip_diff_test.go`:

```go
package cmd

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func TestClusterGossipDiff_Consistent(t *testing.T) {
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946"},
		{"id":"node-2","gossip_addr":"127.0.0.1:17946"}
	]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(ml))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"cluster", "gossip-diff", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "CONSISTENT")
}

func TestClusterGossipDiff_Inconsistent(t *testing.T) {
	// node-1's view has both nodes; node-2 (a different server) has only itself.
	// For simplicity in test, we use a single http server that returns different
	// memberlist bodies based on a query param smuggled via path.
	//
	// Actually — since the fanout uses PeerAdminURL which will resolve to the SAME
	// httptest server (because memberlist returns 127.0.0.1:17946 twice), we need to
	// simulate inconsistency by distinguishing request counts.
	var callCount int
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		callCount++
		// First call (seed discovery) returns both members.
		if callCount == 1 {
			_, _ = fmt.Fprint(w, `{"members":[
				{"id":"node-1","gossip_addr":"127.0.0.1:17946"},
				{"id":"node-2","gossip_addr":"127.0.0.1:17946"}
			]}`)
			return
		}
		// Subsequent fanout calls alternate between two views.
		if callCount%2 == 0 {
			_, _ = fmt.Fprint(w, `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`)
		} else {
			_, _ = fmt.Fprint(w, `{"members":[
				{"id":"node-1","gossip_addr":"127.0.0.1:17946"},
				{"id":"node-2","gossip_addr":"127.0.0.1:17946"}
			]}`)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"cluster", "gossip-diff", "--admin-port", extractPort(t, srv.URL)})

	err := root.Execute()
	require.Error(t, err)
	require.Equal(t, 9, wperrors.ExitCodeFor(err))
	require.Contains(t, buf.String(), "INCONSISTENT")
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./cmd/wpcli/cmd/... -run TestClusterGossipDiff -v
```

- [ ] **Step 3: Create `cmd/wpcli/cmd/cluster_gossip_diff.go`**

```go
package cmd

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newClusterGossipDiffCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "gossip-diff",
		Short: "Compare memberlist views across nodes to detect partitions",
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			// Fan out: each peer is asked for its /admin/memberlist view.
			var urls []string
			for _, m := range r.Members.Members {
				urls = append(urls, r.Client.PeerAdminURL(m))
			}
			f := client.NewFanout(client.FanoutOpts{
				Concurrency: r.Context.Concurrency,
				Timeout:     r.Context.Timeout,
			})
			res := f.Get(urls, "/admin/memberlist", "")

			type viewRow struct {
				NodeID  string
				Members []string
			}
			var rows []viewRow
			for i, m := range r.Members.Members {
				nr := res.Results[i]
				if !nr.OK {
					rows = append(rows, viewRow{NodeID: m.ID, Members: []string{"<unreachable>"}})
					continue
				}
				var ml client.Memberlist
				if err := json.Unmarshal(nr.Body, &ml); err != nil {
					rows = append(rows, viewRow{NodeID: m.ID, Members: []string{"<parse-error>"}})
					continue
				}
				names := make([]string, 0, len(ml.Members))
				for _, sub := range ml.Members {
					names = append(names, sub.ID)
				}
				sort.Strings(names)
				rows = append(rows, viewRow{NodeID: m.ID, Members: names})
			}

			// Compare: consistent iff all row.Members slices are identical.
			w := cmd.OutOrStdout()
			consistent := true
			var reference []string
			for i, row := range rows {
				if i == 0 {
					reference = row.Members
					continue
				}
				if !stringSlicesEqual(reference, row.Members) {
					consistent = false
					break
				}
			}

			if consistent {
				fmt.Fprintln(w, "Gossip View Consistency: CONSISTENT")
				fmt.Fprintf(w, "All %d nodes agree on membership (%d members each).\n",
					len(rows), len(reference))
				return nil
			}

			fmt.Fprintln(w, "Gossip View Consistency: INCONSISTENT")
			fmt.Fprintln(w)
			fmt.Fprintln(w, "Membership Set Agreement")
			for _, row := range rows {
				fmt.Fprintf(w, "  %-12s %d members [%s]\n",
					row.NodeID, len(row.Members), strings.Join(row.Members, ", "))
			}
			return wperrors.NewRedFindingError("gossip views inconsistent")
		},
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
```

- [ ] **Step 4: Register + tests**

Update `cmd/wpcli/cmd/cluster.go`. Run:

```bash
go test ./cmd/wpcli/cmd/... -run TestClusterGossipDiff -v
```

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/cmd/cluster_gossip_diff.go cmd/wpcli/cmd/cluster_gossip_diff_test.go cmd/wpcli/cmd/cluster.go
git commit -m "feat(wpcli): add cluster gossip-diff command"
```

---

## Part F — E Family: Config / Env / Profile Commands (Tasks 29-31)

### Task 29: `wp config show` + `wp config diff`

**Goal:** Fetch `/admin/config` from one node (show) or compare across multiple nodes (diff).

**Files:**
- Create: `cmd/wpcli/cmd/config.go` (cobra group)
- Create: `cmd/wpcli/cmd/config_show.go`
- Create: `cmd/wpcli/cmd/config_diff.go`
- Create: `cmd/wpcli/cmd/config_test.go`
- Modify: `cmd/wpcli/cmd/root.go` — register `config` sub-command

- [ ] **Step 1: Write the tests**

Create `cmd/wpcli/cmd/config_test.go`:

```go
package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigShow_HappyPath(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	cfg := `{"etcd":{"endpoints":["etcd:2379"]},"minio":{"bucketName":"woodpecker"}}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/config", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(cfg)) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"config", "show", "node-1", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "etcd")
	require.Contains(t, buf.String(), "bucketName")
}

func TestConfigDiff_NoDrift(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	cfg := `{"foo":"bar"}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/config", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(cfg)) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"config", "diff", "--all", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "identical")
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Create `cmd/wpcli/cmd/config.go`**

```go
package cmd

import "github.com/spf13/cobra"

func newConfigCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "config",
		Short: "Inspect and compare server configurations",
	}
	c.AddCommand(newConfigShowCommand(), newConfigDiffCommand())
	return c
}
```

- [ ] **Step 4: Create `cmd/wpcli/cmd/config_show.go`**

```go
package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

// fetchConfig fetches /admin/config from the given peer URL and returns
// the raw JSON bytes.
func fetchConfig(peerURL string) ([]byte, error) {
	resp, err := http.Get(peerURL + "/admin/config")
	if err != nil {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("GET /admin/config: %v", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("/admin/config returned status %d", resp.StatusCode))
	}
	return io.ReadAll(resp.Body)
}

func newConfigShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <node>",
		Short: "Show the resolved configuration of a single node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			target, ok := r.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}
			body, err := fetchConfig(r.Client.PeerAdminURL(target))
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			switch Globals.Output {
			case "json":
				_, _ = w.Write(body)
				return nil
			case "yaml":
				var generic any
				if err := json.Unmarshal(body, &generic); err != nil {
					return wperrors.NewNetworkError(fmt.Sprintf("decode config: %v", err))
				}
				enc := yaml.NewEncoder(w)
				enc.SetIndent(2)
				defer enc.Close()
				return enc.Encode(generic)
			default: // table — render as indented YAML which reads well
				var generic any
				_ = json.Unmarshal(body, &generic)
				enc := yaml.NewEncoder(w)
				enc.SetIndent(2)
				defer enc.Close()
				return enc.Encode(generic)
			}
		},
	}
}
```

- [ ] **Step 5: Create `cmd/wpcli/cmd/config_diff.go`**

```go
package cmd

import (
	"bytes"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newConfigDiffCommand() *cobra.Command {
	var all bool
	var reference string
	cmd := &cobra.Command{
		Use:   "diff [node-a] [node-b]",
		Short: "Compare configurations across nodes",
		Args:  cobra.RangeArgs(0, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			// Decide targets
			var targets []client.Member
			if len(args) == 2 {
				a, okA := r.Members.Resolve(args[0])
				b, okB := r.Members.Resolve(args[1])
				if !okA {
					return wperrors.NewTargetNotFoundError(args[0])
				}
				if !okB {
					return wperrors.NewTargetNotFoundError(args[1])
				}
				targets = []client.Member{a, b}
			} else if all || len(args) == 0 {
				targets = r.Members.Members
			} else {
				return wperrors.NewUsageError("pass two node names, or --all")
			}

			// Fetch each target's config
			configs := make(map[string][]byte)
			for _, t := range targets {
				b, err := fetchConfig(r.Client.PeerAdminURL(t))
				if err != nil {
					configs[t.ID] = []byte("<unreachable>")
					continue
				}
				configs[t.ID] = b
			}

			// Reference: first target, or explicit --reference
			refID := targets[0].ID
			if reference != "" {
				refID = reference
			}
			refBytes, ok := configs[refID]
			if !ok {
				return wperrors.NewTargetNotFoundError(refID)
			}

			w := cmd.OutOrStdout()
			anyDrift := false
			for _, t := range targets {
				if t.ID == refID {
					continue
				}
				if bytes.Equal(configs[t.ID], refBytes) {
					fmt.Fprintf(w, "%s: identical\n", t.ID)
				} else {
					anyDrift = true
					fmt.Fprintf(w, "%s: DRIFT vs %s\n", t.ID, refID)
				}
			}
			if anyDrift {
				return wperrors.NewYellowFindingError("config drift detected")
			}
			if len(targets) == 1 {
				fmt.Fprintln(w, "only one node — nothing to diff")
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "diff all nodes in the cluster against the first reachable node")
	cmd.Flags().StringVar(&reference, "reference", "", "explicit reference node name")
	return cmd
}
```

- [ ] **Step 6: Register + tests**

Add `newConfigCommand()` to `NewRootCommand()`. Run:

```bash
go test ./cmd/wpcli/cmd/... -run TestConfig -v
```

- [ ] **Step 7: Commit**

```bash
git add cmd/wpcli/cmd/config.go cmd/wpcli/cmd/config_show.go cmd/wpcli/cmd/config_diff.go cmd/wpcli/cmd/config_test.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add config show and config diff commands"
```

---

### Task 30: `wp env show` + `wp env diff`

**Goal:** Same shape as Task 29 but hitting `/admin/env`. Share the fanout + diff logic via a small helper.

**Files:**
- Create: `cmd/wpcli/cmd/env.go`
- Create: `cmd/wpcli/cmd/env_show.go`
- Create: `cmd/wpcli/cmd/env_diff.go`
- Create: `cmd/wpcli/cmd/env_test.go`
- Modify: `cmd/wpcli/cmd/root.go`

- [ ] **Step 1: Write a happy-path test**

Create `cmd/wpcli/cmd/env_test.go`:

```go
package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvShow_Build(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	env := `{
		"env":{"PATH":"/usr/bin"},
		"runtime":{"go_version":"go1.24.2"},
		"host":{"hostname":"node-1","os":"linux","arch":"amd64"},
		"build":{"version":"v0.1.26","commit":"abc1234","build_time":"2026-04-09T00:00:00Z","go_version":"go1.24.2"}
	}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/env", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(env)) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"env", "show", "node-1", "--section", "build", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "v0.1.26")
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Create the sub-command group + two sub-commands**

Create `cmd/wpcli/cmd/env.go`:

```go
package cmd

import "github.com/spf13/cobra"

func newEnvCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "env",
		Short: "Inspect process environment, Go runtime, host, and build info",
	}
	c.AddCommand(newEnvShowCommand(), newEnvDiffCommand())
	return c
}
```

Create `cmd/wpcli/cmd/env_show.go`:

```go
package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func fetchEnv(peerURL string) ([]byte, error) {
	resp, err := http.Get(peerURL + "/admin/env")
	if err != nil {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("GET /admin/env: %v", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("/admin/env returned status %d", resp.StatusCode))
	}
	return io.ReadAll(resp.Body)
}

func newEnvShowCommand() *cobra.Command {
	var section string
	cmd := &cobra.Command{
		Use:   "show <node>",
		Short: "Show env vars + runtime + host + build for a single node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			target, ok := r.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}
			body, err := fetchEnv(r.Client.PeerAdminURL(target))
			if err != nil {
				return err
			}

			var full map[string]any
			if err := json.Unmarshal(body, &full); err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("decode env: %v", err))
			}

			// Filter by --section
			var out any = full
			if section != "" && section != "all" {
				sect, ok := full[section]
				if !ok {
					return wperrors.NewUsageError(fmt.Sprintf("unknown section %q (valid: env, runtime, host, build, all)", section))
				}
				out = sect
			}

			w := cmd.OutOrStdout()
			if Globals.Output == "json" {
				enc := json.NewEncoder(w)
				enc.SetIndent("", "  ")
				return enc.Encode(out)
			}
			enc := yaml.NewEncoder(w)
			enc.SetIndent(2)
			defer enc.Close()
			return enc.Encode(out)
		},
	}
	cmd.Flags().StringVar(&section, "section", "all", "section filter: env|runtime|host|build|all")
	return cmd
}
```

Create `cmd/wpcli/cmd/env_diff.go`:

```go
package cmd

import (
	"bytes"
	"fmt"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newEnvDiffCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "diff",
		Short: "Compare env / runtime / host / build across all nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			envs := make(map[string][]byte)
			for _, m := range r.Members.Members {
				b, err := fetchEnv(r.Client.PeerAdminURL(m))
				if err != nil {
					envs[m.ID] = []byte("<unreachable>")
					continue
				}
				envs[m.ID] = b
			}
			w := cmd.OutOrStdout()
			// Simple byte-compare. A real implementation would filter noise env keys
			// (HOSTNAME, PWD, etc.) per spec §2.E.7 — this is a Phase 1 TODO noted
			// inline but not yet applied to avoid shipping incomplete noise-filter
			// logic. A future Phase 1.5 enhancement.
			var reference []byte
			var refID string
			for id, b := range envs {
				reference = b
				refID = id
				break
			}
			anyDrift := false
			for id, b := range envs {
				if id == refID {
					continue
				}
				if bytes.Equal(b, reference) {
					fmt.Fprintf(w, "%s: identical\n", id)
				} else {
					anyDrift = true
					fmt.Fprintf(w, "%s: DIFFERS from %s\n", id, refID)
				}
			}
			if anyDrift {
				return wperrors.NewYellowFindingError("env drift detected")
			}
			return nil
		},
	}
}
```

> **Phase 1 known limitation:** `env diff` does a raw byte comparison. A proper implementation filters noise keys (HOSTNAME, PWD, SHLVL, KUBERNETES_*_PORT_*) per spec §2.E.7. Deferred to a Phase 1.5 micro-task to keep Phase 1 scope bounded. Noted inline so reviewers know it's intentional.

- [ ] **Step 4: Register + tests**

Add `newEnvCommand()` to `NewRootCommand()`. Run:

```bash
go test ./cmd/wpcli/cmd/... -run TestEnv -v
```

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/cmd/env.go cmd/wpcli/cmd/env_show.go cmd/wpcli/cmd/env_diff.go cmd/wpcli/cmd/env_test.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add env show and env diff commands"
```

---

### Task 31: `wp profile`

**Goal:** Download Go pprof profile from a node's existing `/debug/pprof/*` endpoint. Zero server changes.

**Files:**
- Create: `cmd/wpcli/cmd/profile.go`
- Create: `cmd/wpcli/cmd/profile_test.go`
- Modify: `cmd/wpcli/cmd/root.go`

- [ ] **Step 1: Write the test**

Create `cmd/wpcli/cmd/profile_test.go`:

```go
package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfile_DownloadsFile(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	// Fake pprof bytes — not a real gzipped profile but the test just checks save.
	pprofBody := []byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00}

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(ml))
	})
	mux.HandleFunc("/debug/pprof/heap", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(pprofBody)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	outFile := filepath.Join(t.TempDir(), "heap.pb.gz")

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{
		"profile", "node-1",
		"--type", "heap",
		"--output-file", outFile,
		"--admin-port", extractPort(t, srv.URL),
	})
	require.NoError(t, root.Execute())

	data, err := os.ReadFile(outFile)
	require.NoError(t, err)
	require.Equal(t, pprofBody, data)
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Create `cmd/wpcli/cmd/profile.go`**

```go
package cmd

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newProfileCommand() *cobra.Command {
	var (
		profileType string
		seconds     int
		outputFile  string
	)
	cmd := &cobra.Command{
		Use:   "profile <node>",
		Short: "Download a Go pprof profile from a node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			target, ok := r.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}
			peerURL := r.Client.PeerAdminURL(target)

			url := fmt.Sprintf("%s/debug/pprof/%s", peerURL, profileType)
			if profileType == "cpu" || profileType == "trace" {
				url = fmt.Sprintf("%s?seconds=%d", url, seconds)
			}

			if outputFile == "" {
				outputFile = fmt.Sprintf("./%s-%s-%s.pb.gz",
					target.ID, profileType, time.Now().UTC().Format("20060102-150405"))
			}

			fmt.Fprintf(cmd.ErrOrStderr(), "Collecting %s profile from %s ...\n", profileType, target.ID)
			client := &http.Client{Timeout: time.Duration(seconds+30) * time.Second}
			resp, err := client.Get(url)
			if err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("GET pprof: %v", err))
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return wperrors.NewNetworkError(fmt.Sprintf("pprof returned status %d", resp.StatusCode))
			}

			f, err := os.Create(outputFile)
			if err != nil {
				return wperrors.NewConfigError(fmt.Sprintf("create output file: %v", err))
			}
			defer f.Close()
			n, err := io.Copy(f, resp.Body)
			if err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("write pprof: %v", err))
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Saved: %s (%d bytes)\n", outputFile, n)
			fmt.Fprintf(cmd.OutOrStdout(), "To analyze: go tool pprof -http=:0 %s\n", outputFile)
			return nil
		},
	}
	cmd.Flags().StringVar(&profileType, "type", "cpu", "profile type: cpu|heap|goroutine|allocs|mutex|block|threadcreate|trace")
	cmd.Flags().IntVar(&seconds, "seconds", 30, "duration for cpu/trace profiles")
	cmd.Flags().StringVar(&outputFile, "output-file", "", "output file path (default: ./<node>-<type>-<timestamp>.pb.gz)")
	return cmd
}
```

- [ ] **Step 4: Register + tests**

Add `newProfileCommand()` to `NewRootCommand()`. Run:

```bash
go test ./cmd/wpcli/cmd/... -run TestProfile -v
```

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/cmd/profile.go cmd/wpcli/cmd/profile_test.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add profile command for pprof download"
```

---

## Part G — Docs + Final Verification (Tasks 32-33)

### Task 32: `cmd/wpcli/README.md` (Phase 1 quickstart)

**Goal:** Give a new user 60 seconds to understand what `wp` is and how to use it.

**Files:**
- Modify: `cmd/wpcli/README.md` (expand from the stub created in Task 2)

- [ ] **Step 1: Replace `cmd/wpcli/README.md` with a real quickstart**

```markdown
# wp — Woodpecker Operational CLI

`wp` is the Woodpecker operational CLI for service-mode clusters.
See [`docs/wpcli-design.md`](../../docs/wpcli-design.md) for the full design.

## Build

    make wpcli
    ./bin/wp --version

## Minimum config

Create `~/.woodpecker/cli.yaml`:

```yaml
current-context: local

contexts:
  local:
    endpoint: http://localhost:9091
    admin_port: 9091
```

## Phase 1 commands

### Node lifecycle
- `wp node list` — list all server nodes
- `wp node show <node>` — detailed view of one node
- `wp node decommission <node>` — graceful decommission (default blocks until safe)
- `wp node drain-status <node> [-w]` — watch decommission progress
- `wp node cancel-decommission <node>` — abort an in-progress decommission
- `wp node restart <node>` — intentionally not implemented (exit 10)

### Cluster overview
- `wp cluster info` — cluster summary + topology tree
- `wp cluster health` — red/yellow/green health check
- `wp cluster gossip-diff` — detect membership view divergence

### Config & env
- `wp config show <node>` — show resolved server config
- `wp config diff --all` — compare config across nodes
- `wp env show <node>` — env vars + Go runtime + host + build info
- `wp env diff` — compare env across nodes

### Diagnostics
- `wp profile <node> --type cpu --seconds 30` — download pprof profile

### CLI contexts
- `wp ctx list` — list configured contexts
- `wp ctx use <name>` — switch active context
- `wp ctx view` — show resolved active context

## Exit codes

See [`docs/wpcli-design.md` §3.5](../../docs/wpcli-design.md#35-退出码总表).

## Phase 1 NOT yet implemented

The following command families are planned for Phase 2 and Phase 3:
- `wp logstore *`, `wp metrics *`, `wp ops *`, `wp logging *` — Phase 2
- `wp k8s *` — Phase 3
```

- [ ] **Step 2: Commit**

```bash
git add cmd/wpcli/README.md
git commit -m "docs(wpcli): add Phase 1 quickstart README"
```

---

### Task 33: Phase 1 verification sweep

**Goal:** Final sanity check — run everything, confirm nothing regressed, produce a clean `bin/wp`.

**Files:** None (verification only)

- [ ] **Step 1: Run the full test suite**

```bash
go test ./... -race -count=1
```

Expected: PASS. If any existing test fails due to Phase 1 changes (e.g., `cmd/main.go` rewiring), fix the breakage in that test's commit rather than suppressing.

- [ ] **Step 2: Build the wp binary**

```bash
make wpcli
./bin/wp --version
./bin/wp --help
```

Expected: version shows a git-derived value (not `dev` if you're on a committed tree), help lists all Phase 1 sub-commands.

- [ ] **Step 3: Smoke test against a local docker-compose cluster**

(Optional but recommended — `tests/docker/wpcli/` infrastructure doesn't exist yet in Phase 1, so this is manual.)

```bash
cd deployments
docker compose up -d
sleep 10
cd ..

# Create a minimal cli.yaml
mkdir -p ~/.woodpecker
cat > ~/.woodpecker/cli.yaml <<EOF
current-context: local
contexts:
  local:
    endpoint: http://localhost:9091
    admin_port: 9091
EOF

./bin/wp cluster info
./bin/wp node list
./bin/wp env show $(./bin/wp node list -o json | jq -r '.[0].name') --section build
```

Expected: All three commands produce output without error. Record any problems found for hotfix in a follow-up commit.

- [ ] **Step 4: Spec coverage check**

Open `docs/wpcli-design.md` §5.3 (Phase 1 Scope) and verify every command and every server change listed there has a corresponding completed task above. Any gap is a plan failure — add a task and re-execute.

- [ ] **Step 5: Commit verification log (optional)**

If you kept notes during verification, commit them as a release-notes stub:

```bash
mkdir -p docs/wpcli
cat > docs/wpcli/phase1-verification.md <<EOF
# Phase 1 Verification Results

Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)

## Test suite
- \`go test ./... -race\`: PASS

## Build
- \`make wpcli\`: bin/wp $(./bin/wp --version)

## Smoke test (docker-compose)
- \`wp cluster info\`: OK
- \`wp node list\`: OK
- \`wp env show\`: OK

## Known issues for Phase 2
- (list any discovered limitations here)
EOF
git add docs/wpcli/phase1-verification.md
git commit -m "docs(wpcli): add Phase 1 verification results"
```

---

## Phase 2 Outline (dependent on Phase 1 verified completion)

**⚠️ Do NOT start Phase 2 until all Phase 1 tasks are committed, all tests pass, and the binary has been manually verified on a docker-compose cluster.**

Phase 2 adds the **observability & op registry** layer (spec §5.4). It's the largest single phase: the opregistry package, the metrics observer chain, business code metric migration, C + D + G command families, E族 logging commands, and the `tests/docker/wpcli/` E2E setup.

### Phase 2 dependency on Phase 1

- Phase 1 builds the CLI framework (cobra, context, output, fanout, error system). Phase 2 commands reuse all of it.
- Phase 1 establishes the admin endpoint conventions. Phase 2 adds more endpoints following the same patterns.
- Phase 1's `NodeLifecycleManager.CancelDecommission()` + `AdminCallbacks` extension pattern is the template for Phase 2's new callbacks.

### Phase 2 preliminary task list (to be expanded in `docs/wpcli-plan-phase2.md` when Phase 1 is done)

1. **Create `common/runtime/opregistry/` package** — types, Registry, eviction policy, observer interface (~500 lines)
2. **Create `common/metrics/op.go` + `op_observer.go`** — StartOp/End abstraction + observer chain (~200 lines)
3. **Add clockwork v0.2.2 dependency** + wire it into opregistry for time mocking
4. **Add `Runtime.OpRegistry.{Capacity, WarnAge}` config fields** in `common/config/configuration.go`
5. **Define `WriterRegistry` interface** in `server/storage/writer_registry.go` + implement in `objectstorage` and `stagedstorage` backends
6. **Extend `storage.Writer` interface** with `Snapshot()` / `SnapshotDetailed()` methods + implement in all 3 backends (objectstorage/stagedstorage/disk); regenerate mocks
7. **Migrate ~12 op-shaped histogram call sites** from `start := time.Now() / Observe(elapsed)` to `op := metrics.StartOp / defer op.End(status)` — each with regression test verifying the original histogram behavior is preserved. The 12 sites are:
   - `server/logstore.go` × 9 methods (AddEntry, GetBatchEntries, Complete, Fence, GetSegmentLac, GetSegmentBlockCount, CompactSegment, CleanSegment, UpdateLac)
   - `server/storage/objectstorage/writer_impl.go` flush entry
   - `server/storage/stagedstorage/writer_impl.go` flush entry
   - `server/storage/disk/writer_impl.go:617` flush observe
8. **Convert `common/logger/logger.go`** to `zap.AtomicLevel` and expose `GetLevel()` / `SetLevel()`
9. **gRPC interceptor** — create `common/runtime/opregistry/interceptor.go`, wire it via `grpc.NewServer(grpc.UnaryInterceptor(...))` in `cmd/main.go`
10. **Instantiate OpRegistry in `cmd/main.go`** + `metrics.RegisterOpObserver(opReg)` + wire ops admin callbacks
11. **Create `common/http/management/logstore_handler.go`** (5 endpoints: segments, segment-show, flush, fence, compact)
12. **Create `common/http/management/ops_handler.go`** (3 endpoints: list, get, stats)
13. **Create `common/http/management/logging_handler.go`** (补实 GET + POST /log/level)
14. **Add router path constants** + AdminCallbacks sub-structs (Logstore, Ops, LogLevel)
15. **C family CLI commands** (7): `logstore segments/segment-show/buffer/flush-queue/force-flush/fence/compact`
16. **D family CLI commands** (5): `metrics list/snapshot/top/watch/report` + YAML scenario engine + 12 built-in scenarios
17. **G family CLI commands** (3): `ops list/show/stats` + Health Interpretation block
18. **E.3/E.4**: `logging get-level` + `logging set-level`
19. **Create `tests/docker/wpcli/`** mirroring `tests/docker/monitor/` — docker-compose.wpcli.yaml, wpcli_cluster.go, wpcli_test.go, run_wpcli_tests.sh, scenarios/ subdirectories
20. **Phase 2 acceptance:** On the docker-compose cluster, construct a stuck flush scenario and verify:
    - `wp ops list --longer-than 30s` shows the stuck flush op
    - `wp ops stats` shows `evicted_old > 0`
    - `wp metrics report --scenario stuck-flush --window 1m` produces a critical finding + exit 9
    - All 12 metric migration regression tests pass
    - `go test ./... -race` passes (no new races from interceptor or opregistry)

**Phase 2 estimated task count in full plan:** ~35-45 tasks. Will be materialized in `docs/wpcli-plan-phase2.md` once Phase 1 is merged.

---

## Phase 3 Outline (dependent on Phase 2 verified completion)

**⚠️ Do NOT start Phase 3 until all Phase 2 tasks are committed, all tests pass (including the Phase 2 `tests/docker/wpcli/` E2E suite), and the opregistry eviction signal has been manually verified on a stall scenario.**

Phase 3 adds K8s hybrid mode and release polish (spec §5.5). Smallest phase in scope; pure CLI-side work.

### Phase 3 dependency on Phase 2

- Phase 3 does NOT touch any server code.
- Phase 3 adds `cmd/wpcli/internal/k8s/` for shell-out to kubectl. Reuses Phase 1's config / context / error system.
- F family commands use the Phase 1 `resolveAndDiscover` pattern but with an added kubectl executor step.

### Phase 3 preliminary task list (to be expanded in `docs/wpcli-plan-phase3.md` when Phase 2 is done)

1. **Create `cmd/wpcli/internal/k8s/executor.go`** — wraps `os/exec.Command` for kubectl, honoring `--kubectl` / `--kubeconfig` / `--kube-context` / `--namespace`
2. **Create `cmd/wpcli/internal/k8s/builder.go`** — constructs kubectl command strings for each F族 use case (get, patch, logs)
3. **Extend `cmd/wpcli/config/cli_config.go`** to include the `k8s:` sub-block in context (namespace, cluster, kube_context, kubeconfig, kubectl path)
4. **F.1 `wp k8s status`** — hybrid print/execute for `kubectl get woodpeckercluster + describe + get pods`
5. **F.2 `wp k8s scale --replicas N`** — hybrid for `kubectl patch woodpeckercluster`
6. **F.3 `wp k8s logs <node-or-pod>`** — hybrid for `kubectl logs`, including `-f` passthrough and node-id → pod-name resolution (`<cluster>-server-<ordinal>` convention)
7. **F.4 `wp k8s doctor`** — permanent stub (spec §2.F.3), exit 10 with explanatory text
8. **Cross-platform release build** — extend Makefile with `wpcli-release` target producing linux/amd64, linux/arm64, darwin/arm64, windows/amd64 binaries
9. **Embed `wp` in server Docker image** — modify `build/Dockerfile` to `COPY bin/wp /usr/local/bin/wp`
10. **GitHub Release workflow** — trigger cross-platform build on tag, upload binaries to GitHub Releases
11. **Documentation: `cmd/wpcli/README.md` full version** (expand Phase 1 quickstart with Phase 2 + 3 commands)
12. **Documentation: `docs/wpcli/quickstart.md`** — 15-minute onboarding for a new SRE
13. **Documentation: `docs/wpcli/cookbook.md`** — 10 common incident-response recipes
14. **Documentation: `docs/wpcli/configuration.md`** — full cli.yaml reference
15. **Manual K8s / Operator E2E verification (kind)** — per spec §4.4, run `wp k8s status/scale/logs -x` against a kind cluster with the operator, record results in release notes
16. **Spec finalize** — remove WIP marker from `docs/wpcli-design.md` header, bump status to "Implemented"

**Phase 3 estimated task count in full plan:** ~15-20 tasks. Will be materialized in `docs/wpcli-plan-phase3.md` once Phase 2 is merged.

---

## Self-Review Checklist (for plan author)

Run these checks after finishing Phase 1 execution:

- [ ] **Spec coverage:** every command in spec §2.A / §2.B / §5.3 E-subset / §2.E.5 has a completed task here
- [ ] **Server endpoint coverage:** all 3 new endpoints + 2 enhancements from §6.3 Phase 1 rows have a completed task
- [ ] **Exit code coverage:** every exit code a Phase 1 command can emit has at least one unit test (spec §3.5)
- [ ] **No placeholders:** grep this plan for TODO / TBD / 待填 — none should remain (the `(待填)` author field in the design doc header is intentional and not in this plan)
- [ ] **Type consistency:** function / method / struct names are identical across tasks (e.g., `CancelDecommission` not `cancelDecommission` in some tasks)
- [ ] **Commit granularity:** each task ends in exactly one commit with a clear conventional-commits subject
- [ ] **Test-first:** every non-trivial task writes its failing test before the implementation

---

## Execution Handoff

Plan complete and saved to `docs/wpcli-plan-phase1.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration
2. **Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
