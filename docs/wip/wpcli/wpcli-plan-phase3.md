# wp CLI Phase 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Ship Phase 3 of the wp CLI — K8s hybrid commands (print/execute dual mode), cross-platform release build, Docker image integration, and full documentation suite. This is the smallest phase in scope: pure CLI-side work, zero server code changes.

**Architecture:** F family commands use a new `cmd/wpcli/internal/k8s/` package that wraps `os/exec.Command` for kubectl. Default mode prints the kubectl commands without executing (zero side effects); `-x` / `--execute` mode runs them and streams output. Node-id → pod-name resolution follows the `<cluster>-server-<ordinal>` convention. All commands reuse Phase 1's config/context/error system. The `k8s:` sub-block in `cli.yaml` contexts provides namespace, cluster name, and kubectl path.

**Tech Stack (additions to Phase 1/2):**
- No new Go dependencies
- `os/exec` (stdlib) for kubectl shell-out
- `embed` (stdlib) for version embedding in release build

**Reference documents:**
- Design spec: [`docs/wpcli-design.md`](./wpcli-design.md) — §2.F, §5.5
- Phase 1 plan: [`docs/wpcli-plan-phase1.md`](./wpcli-plan-phase1.md)
- Phase 2 plan: [`docs/wpcli-plan-phase2.md`](./wpcli-plan-phase2.md)
- Existing pattern for CLI commands: `cmd/wpcli/cmd/node_list.go`

**Phase 3 scope locked (from spec §5.5):**
- 4 feature commands: F family (k8s status/scale/logs/doctor)
- `cmd/wpcli/internal/k8s/` package (~400 lines)
- cli.yaml `k8s:` sub-block extension
- Cross-platform release build (Makefile target)
- Docker image wp embedding
- GitHub Release workflow
- Full documentation suite: README update, quickstart, cookbook, configuration reference
- Spec finalization

**Prerequisite:** All Phase 2 tasks committed, tests pass (including E2E suite), opregistry verified on docker-compose.

**Out of scope for Phase 3 (explicitly deferred):**
- `wp k8s doctor` is a permanent stub (exit 10) — actual implementation deferred to future release
- Operator-managed rolling restart via K8s — deferred
- Windows-specific testing (build only, no E2E)

---

## Phase 3 Task Inventory

18 tasks, grouped by dependency layer. Each task is self-contained and ends with a commit.

| # | Task | Layer | Depends on |
|---|---|---|---|
| 1 | cli.yaml `k8s:` sub-block extension | Config | — |
| 2 | kubectl executor (`internal/k8s/executor.go`) | K8s Infrastructure | 1 |
| 3 | kubectl command builder (`internal/k8s/builder.go`) | K8s Infrastructure | 2 |
| 4 | K8s shared flags | K8s Infrastructure | 1 |
| 5 | `wp k8s` parent command | CLI — F family | 4 |
| 6 | `wp k8s status` | CLI — F family | 3, 5 |
| 7 | `wp k8s scale` | CLI — F family | 3, 5 |
| 8 | `wp k8s logs` | CLI — F family | 3, 5 |
| 9 | `wp k8s doctor` (stub) | CLI — F family | 5 |
| 10 | Cross-platform release build (`make wpcli-release`) | Release | — |
| 11 | Embed `wp` in server Docker image | Release | 10 |
| 12 | GitHub Release workflow | Release | 10 |
| 13 | `cmd/wpcli/README.md` full version | Docs | all commands |
| 14 | `docs/wpcli/quickstart.md` | Docs | 13 |
| 15 | `docs/wpcli/cookbook.md` | Docs | 14 |
| 16 | `docs/wpcli/configuration.md` | Docs | 14 |
| 17 | Manual K8s E2E verification (kind) | Verification | 6, 7, 8 |
| 18 | Spec finalize + Phase 3 verification sweep | Verification | all |

**Estimated commits:** ~18 (one per task).

---

## Part A — K8s Config + Infrastructure (Tasks 1-4)

### Task 1: cli.yaml `k8s:` sub-block extension

**Goal:** Extend the CLI config to support K8s-specific settings per context: namespace, cluster name, kubeconfig path, kubectl binary path, and kube-context.

**Files:**
- Modify: `cmd/wpcli/config/cli_config.go`
- Modify: `cmd/wpcli/config/cli_config_test.go`

**Spec reference:** §2.F.2 — shared K8s flags, cli.yaml k8s sub-block.

- [x] **Step 1: Add K8sConfig struct**

```go
// K8sConfig holds Kubernetes-specific settings for a CLI context.
type K8sConfig struct {
	Namespace   string `yaml:"namespace,omitempty"`    // K8s namespace (overridden by --namespace)
	Cluster     string `yaml:"cluster,omitempty"`      // WoodpeckerCluster CR name (overridden by --wp-cluster)
	KubeContext string `yaml:"kube_context,omitempty"` // kubectl context (overridden by --kube-context)
	Kubeconfig  string `yaml:"kubeconfig,omitempty"`   // kubeconfig path (overridden by --kubeconfig)
	Kubectl     string `yaml:"kubectl,omitempty"`      // kubectl binary path (default: $(which kubectl))
}
```

- [x] **Step 2: Add to Context struct**

```go
type Context struct {
	Endpoint    string        `yaml:"endpoint"`
	AdminPort   int           `yaml:"admin_port"`
	Timeout     time.Duration `yaml:"timeout"`
	Concurrency int           `yaml:"concurrency"`
	Strict      bool          `yaml:"strict"`
	K8s         K8sConfig     `yaml:"k8s,omitempty"`
}
```

- [x] **Step 3: Write tests**

Test that `cli.yaml` with a `k8s:` block parses correctly:
```yaml
contexts:
  prod:
    endpoint: http://wp-prod:9091
    k8s:
      namespace: woodpecker
      cluster: wp-prod
      kubeconfig: /etc/kube/config
```

- [x] **Step 4: Commit**

```bash
git add cmd/wpcli/config/cli_config.go cmd/wpcli/config/cli_config_test.go
git commit -m "feat(wpcli): add k8s sub-block to cli.yaml context config"
```

---

### Task 2: kubectl executor

**Goal:** Create a thin wrapper around `os/exec.Command` that constructs and optionally executes kubectl commands. Handles `--kubectl`, `--kubeconfig`, `--kube-context`, `--namespace` passthrough, and transparent stdout/stderr/exit code forwarding.

**Files:**
- Create: `cmd/wpcli/internal/k8s/executor.go`
- Create: `cmd/wpcli/internal/k8s/executor_test.go`

**Spec reference:** §2.F.1 — print vs execute mode, kubectl missing auto-downgrade.

- [x] **Step 1: Implement executor**

```go
package k8s

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// ExecutorOpts configures kubectl execution.
type ExecutorOpts struct {
	Kubectl     string // path to kubectl binary (default: "kubectl")
	Kubeconfig  string // --kubeconfig passthrough
	KubeContext string // --context passthrough (kubectl's --context, not wp's)
	Namespace   string // -n passthrough
}

// Executor wraps kubectl command construction and execution.
type Executor struct {
	opts ExecutorOpts
}

// New creates a new Executor. Resolves kubectl path if not provided.
func New(opts ExecutorOpts) *Executor

// KubectlAvailable returns true if kubectl is found in PATH or at the configured path.
func (e *Executor) KubectlAvailable() bool

// BuildCommand constructs a kubectl command string with all configured flags.
// Returns the full command as a string (for print mode) and as exec.Cmd (for execute mode).
func (e *Executor) BuildCommand(args ...string) (printable string, cmd *exec.Cmd)

// Run executes the kubectl command, streaming stdout/stderr to the given writers.
// Returns the exit code.
func (e *Executor) Run(stdout, stderr io.Writer, args ...string) (exitCode int, err error)

// PrintCommand prints the kubectl command to stdout without executing.
func (e *Executor) PrintCommand(w io.Writer, args ...string)
```

- [x] **Step 2: Write tests**

Test `BuildCommand` output with various flag combinations. Test `KubectlAvailable` with a non-existent path. Test `Run` with a simple command (e.g., `kubectl version --client`).

- [x] **Step 3: Commit**

```bash
git add cmd/wpcli/internal/k8s/executor.go cmd/wpcli/internal/k8s/executor_test.go
git commit -m "feat(wpcli): add kubectl executor with print/execute dual mode"
```

---

### Task 3: kubectl command builder

**Goal:** Provide typed helper functions that construct kubectl command arguments for each F family use case, so command implementations don't build raw strings.

**Files:**
- Create: `cmd/wpcli/internal/k8s/builder.go`
- Create: `cmd/wpcli/internal/k8s/builder_test.go`

- [x] **Step 1: Implement builder functions**

```go
package k8s

// StatusCommands returns the kubectl args for `wp k8s status`.
// Returns 3 command arg slices:
//   1. kubectl get woodpeckercluster <cluster> -o wide
//   2. kubectl describe woodpeckercluster <cluster>
//   3. kubectl get pods -l app.kubernetes.io/instance=<cluster> -o wide
func StatusCommands(cluster string) [][]string

// ScaleCommand returns the kubectl args for `wp k8s scale --replicas N`.
//   kubectl patch woodpeckercluster <cluster> --type merge -p '{"spec":{"replicas":<N>}}'
func ScaleCommand(cluster string, replicas int) []string

// LogsCommand returns the kubectl args for `wp k8s logs <target>`.
//   kubectl logs <pod-or-node-resolved> [-f] [--tail N] [--since T]
// If target looks like a node ID (not a pod name), resolves via <cluster>-server-<ordinal>.
func LogsCommand(cluster, target string, follow bool, tail int, since string) []string

// ResolvePodName converts a node identifier to a pod name.
// If the target already looks like a pod name, returns it unchanged.
// Otherwise applies the <cluster>-server-<ordinal> convention.
func ResolvePodName(cluster, nodeIdentifier string) string
```

- [x] **Step 2: Write tests**

Test each builder function with various inputs. Verify correct flag ordering and quoting.

- [x] **Step 3: Commit**

```bash
git add cmd/wpcli/internal/k8s/builder.go cmd/wpcli/internal/k8s/builder_test.go
git commit -m "feat(wpcli): add kubectl command builder for F family commands"
```

---

### Task 4: K8s shared flags

**Goal:** Define the shared K8s flags (`-x`, `--kubectl`, `--namespace`, `--wp-cluster`, `--kube-context`, `--kubeconfig`) as a reusable flag set that all F family commands attach.

**Files:**
- Create: `cmd/wpcli/cmd/k8s_flags.go`

- [x] **Step 1: Implement shared flags**

```go
package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/k8s"
)

// K8sFlags holds the shared flags for F family commands.
type K8sFlags struct {
	Execute     bool   // -x / --execute
	Kubectl     string // --kubectl
	Namespace   string // -n / --namespace
	WpCluster   string // --wp-cluster
	KubeContext string // --kube-context
	Kubeconfig  string // --kubeconfig
}

// AddK8sFlags registers the shared K8s flags on the given command.
func AddK8sFlags(cmd *cobra.Command, flags *K8sFlags) {
	cmd.Flags().BoolVarP(&flags.Execute, "execute", "x", false, "Execute kubectl commands (default: print only)")
	cmd.Flags().StringVar(&flags.Kubectl, "kubectl", "", "Path to kubectl binary")
	cmd.Flags().StringVarP(&flags.Namespace, "namespace", "n", "", "Kubernetes namespace")
	cmd.Flags().StringVar(&flags.WpCluster, "wp-cluster", "", "WoodpeckerCluster CR name")
	cmd.Flags().StringVar(&flags.KubeContext, "kube-context", "", "kubectl context name")
	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
}

// ResolveExecutor builds a k8s.Executor from the flags, merging with cli.yaml k8s config.
func (f *K8sFlags) ResolveExecutor(cliK8s config.K8sConfig) *k8s.Executor
```

- [x] **Step 2: Commit**

```bash
git add cmd/wpcli/cmd/k8s_flags.go
git commit -m "feat(wpcli): add shared K8s flags for F family commands"
```

---

## Part B — F Family CLI: K8s Commands (Tasks 5-9)

### Task 5: `wp k8s` parent command

**Goal:** Create the `k8s` command group and register it in the root.

**Files:**
- Create: `cmd/wpcli/cmd/k8s.go`
- Modify: `cmd/wpcli/cmd/root.go`

- [x] **Step 1: Create parent**

```go
func newK8sCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "k8s",
		Short: "Kubernetes integration commands (print or execute kubectl)",
		Long:  "Commands for managing Woodpecker on Kubernetes via the operator.\n" +
			"Default mode prints kubectl commands without executing.\n" +
			"Use -x / --execute to actually run them.",
	}
	cmd.AddCommand(
		newK8sStatusCommand(),
		newK8sScaleCommand(),
		newK8sLogsCommand(),
		newK8sDoctorCommand(),
	)
	return cmd
}
```

- [x] **Step 2: Register in root.go**

- [x] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/k8s.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add k8s parent command"
```

---

### Task 6: `wp k8s status`

**Goal:** Show cluster status by printing (or executing) 3 kubectl commands: get woodpeckercluster, describe, get pods.

**Files:**
- Create: `cmd/wpcli/cmd/k8s_status.go`
- Create: `cmd/wpcli/cmd/k8s_test.go`

**Spec reference:** §2.F.1 — k8s status, print/execute dual mode.

- [x] **Step 1: Implement**

Print mode (default):
```
# To see WoodpeckerCluster status, run:
kubectl get woodpeckercluster wp-prod -n woodpecker -o wide
kubectl describe woodpeckercluster wp-prod -n woodpecker
kubectl get pods -l app.kubernetes.io/instance=wp-prod -n woodpecker -o wide

# Tip: add -x to execute these commands directly
```

Execute mode (`-x`):
- Run all 3 commands sequentially
- Stream stdout/stderr
- If kubectl not found: downgrade to print mode + warning
- Exit code: `100 + kubectl_exit_code` if kubectl fails

- [x] **Step 2: Write tests**

Test print mode output format. Test execute mode with a mock executor.

- [x] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/k8s_status.go cmd/wpcli/cmd/k8s_test.go
git commit -m "feat(wpcli): add k8s status command with print/execute dual mode"
```

---

### Task 7: `wp k8s scale`

**Goal:** Scale the cluster by printing (or executing) a kubectl patch command.

**Files:**
- Create: `cmd/wpcli/cmd/k8s_scale.go`
- Modify: `cmd/wpcli/cmd/k8s_test.go`

**Spec reference:** §2.F.2 — k8s scale.

- [x] **Step 1: Implement**

- Required: `--replicas N`
- Confirmation prompt in execute mode (scaling is impactful)
- Print mode: show the kubectl patch command
- Execute mode: run `kubectl patch woodpeckercluster <cluster> --type merge -p '{"spec":{"replicas":N}}'`

- [x] **Step 2: Write tests**

- [x] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/k8s_scale.go cmd/wpcli/cmd/k8s_test.go
git commit -m "feat(wpcli): add k8s scale command"
```

---

### Task 8: `wp k8s logs`

**Goal:** Tail logs from a Woodpecker pod by printing (or executing) kubectl logs.

**Files:**
- Create: `cmd/wpcli/cmd/k8s_logs.go`
- Modify: `cmd/wpcli/cmd/k8s_test.go`

**Spec reference:** §2.F.3 — k8s logs.

- [x] **Step 1: Implement**

- Positional arg: `<node-or-pod>` — if it looks like a node ID, resolve to pod name via `<cluster>-server-<ordinal>`
- Flags: `-f` / `--follow`, `--tail N` (default 100), `--since` (e.g., "1h")
- Print mode: show the kubectl logs command
- Execute mode: run and stream output, `-f` passthrough

- [x] **Step 2: Write tests**

Test pod name resolution. Test flag passthrough.

- [x] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/k8s_logs.go cmd/wpcli/cmd/k8s_test.go
git commit -m "feat(wpcli): add k8s logs command with node-to-pod resolution"
```

---

### Task 9: `wp k8s doctor` (stub)

**Goal:** Permanent stub — explains that k8s doctor is planned for a future release. Exit 10.

**Files:**
- Create: `cmd/wpcli/cmd/k8s_doctor.go`
- Modify: `cmd/wpcli/cmd/k8s_test.go`

**Spec reference:** §2.F.4 — k8s doctor, permanent stub.

- [x] **Step 1: Implement**

```go
func newK8sDoctorCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Diagnose K8s cluster issues (not yet implemented)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return wperrors.NewNotImplementedError("k8s doctor")
		},
	}
}
```

- [x] **Step 2: Write test**

Verify exit code 10, verify message mentions "not yet implemented".

- [x] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/k8s_doctor.go cmd/wpcli/cmd/k8s_test.go
git commit -m "feat(wpcli): add k8s doctor stub (exit 10, future release)"
```

---

## Part C — Release Build + Distribution (Tasks 10-12)

### Task 10: Cross-platform release build

**Goal:** Add a `make wpcli-release` Makefile target that produces `wp` binaries for linux/amd64, linux/arm64, darwin/arm64, and windows/amd64.

**Files:**
- Modify: `Makefile`

- [x] **Step 1: Add target**

```makefile
WPCLI_PLATFORMS ?= linux/amd64 linux/arm64 darwin/arm64 windows/amd64

.PHONY: wpcli-release
wpcli-release: ## Build wp CLI for all release platforms
	@for platform in $(WPCLI_PLATFORMS); do \
		GOOS=$${platform%%/*} GOARCH=$${platform##*/} \
		CGO_ENABLED=0 go build $(LDFLAGS) \
			-o $(BIN_DIR)/wp-$${platform%%/*}-$${platform##*/}$$([ "$${platform%%/*}" = "windows" ] && echo ".exe") \
			./cmd/wpcli; \
		echo "Built $(BIN_DIR)/wp-$${platform%%/*}-$${platform##*/}"; \
	done
```

- [x] **Step 2: Verify**

```bash
make wpcli-release
ls -la bin/wp-*
```

Expected: 4 binaries (wp-linux-amd64, wp-linux-arm64, wp-darwin-arm64, wp-windows-amd64.exe).

- [x] **Step 3: Commit**

```bash
git add Makefile
git commit -m "build: add make wpcli-release for cross-platform wp binaries"
```

---

### Task 11: Embed `wp` in server Docker image

**Goal:** Modify the Dockerfile to include `wp` in the server image so operators can use it from inside running containers.

**Files:**
- Modify: `build/Dockerfile` (or whichever Dockerfile builds the server image)

- [x] **Step 1: Add build stage + COPY**

Add after the server binary build stage:

```dockerfile
# Build wp CLI
RUN CGO_ENABLED=0 go build ${LDFLAGS} -o /wp ./cmd/wpcli

# In the final stage:
COPY --from=builder /wp /usr/local/bin/wp
```

- [x] **Step 2: Verify**

```bash
docker build -f build/Dockerfile -t woodpecker-test .
docker run --rm woodpecker-test wp version
```

- [x] **Step 3: Commit**

```bash
git add build/Dockerfile
git commit -m "build: embed wp CLI in server Docker image"
```

---

### Task 12: GitHub Release workflow

**Goal:** Create a GitHub Actions workflow that triggers on tag push, runs `make wpcli-release`, and uploads binaries to GitHub Releases.

**Files:**
- Create: `.github/workflows/release-wpcli.yaml`

- [x] **Step 1: Create workflow**

```yaml
name: Release wp CLI

on:
  push:
    tags:
      - 'v*'

jobs:
  release:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - name: Build release binaries
        run: make wpcli-release
      - name: Upload to GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          files: bin/wp-*
```

- [x] **Step 2: Commit**

```bash
git add .github/workflows/release-wpcli.yaml
git commit -m "ci: add GitHub Actions workflow for wp CLI release"
```

---

## Part D — Documentation (Tasks 13-16)

### Task 13: `cmd/wpcli/README.md` full version

**Goal:** Expand the Phase 1 quickstart README with all Phase 2 and Phase 3 commands.

**Files:**
- Modify: `cmd/wpcli/README.md`

- [x] **Step 1: Add Phase 2 + 3 command sections**

Add after the Phase 1 commands section:

```markdown
### Logstore runtime (Phase 2)
- `wp logstore segments <node>` — list active segments
- `wp logstore segment-show <node> --log X --seg Y` — detailed segment view
- `wp logstore buffer <node>` — buffer bytes summary
- `wp logstore flush-queue <node>` — flush queue depth summary
- `wp logstore force-flush <node>` — force sync
- `wp logstore fence <node> --log X --seg Y --reason "..."` — force fence (high-risk)
- `wp logstore compact <node> --log X --seg Y` — force compaction

### Metrics analysis (Phase 2)
- `wp metrics list <node>` — list all metric series
- `wp metrics snapshot [<node>|--all]` — point-in-time snapshot
- `wp metrics top --by <metric>` — cross-node top-N
- `wp metrics watch <metric>` — real-time trend stream
- `wp metrics report --scenario <name>` — scenario-based analysis

### Ops registry (Phase 2)
- `wp ops list <node>` — in-flight operations
- `wp ops show <node> --op-id <id>` — single op detail
- `wp ops stats <node>` — registry utilization

### Dynamic logging (Phase 2)
- `wp logging get-level <node>` — read current log level
- `wp logging set-level <node> --level <level>` — change log level

### Kubernetes (Phase 3)
- `wp k8s status` — cluster status (print kubectl commands, `-x` to execute)
- `wp k8s scale --replicas N` — scale cluster
- `wp k8s logs <node-or-pod>` — tail pod logs
- `wp k8s doctor` — not yet implemented
```

Remove the "Phase 1 NOT yet implemented" section.

- [x] **Step 2: Commit**

```bash
git add cmd/wpcli/README.md
git commit -m "docs(wpcli): expand README with Phase 2 + Phase 3 commands"
```

---

### Task 14: `docs/wpcli/quickstart.md`

**Goal:** 15-minute onboarding guide for a new SRE. Covers install, config, first commands, and common workflows.

**Files:**
- Create: `docs/wpcli/quickstart.md`

- [x] **Step 1: Write quickstart**

Sections:
1. **Install** — `make wpcli` or download from GitHub Releases
2. **Configure** — create `~/.woodpecker/cli.yaml` with examples
3. **First commands** — `wp cluster info`, `wp node list`, `wp cluster health`
4. **Multi-context setup** — `wp ctx list`, `wp ctx use prod`
5. **Common workflows** — decommission a node, check stuck flushes, download a pprof profile
6. **K8s integration** — configure k8s sub-block, `wp k8s status`, `wp k8s logs`

- [x] **Step 2: Commit**

```bash
git add docs/wpcli/quickstart.md
git commit -m "docs(wpcli): add 15-minute quickstart guide"
```

---

### Task 15: `docs/wpcli/cookbook.md`

**Goal:** 10 common incident-response recipes that SREs can follow step-by-step.

**Files:**
- Create: `docs/wpcli/cookbook.md`

- [x] **Step 1: Write cookbook**

Recipes:
1. **Decommission a node safely** — `wp node decommission`, `wp node drain-status -w`
2. **Investigate a stuck flush** — `wp ops list --longer-than 30s`, `wp logstore flush-queue`, `wp logstore segment-show`, `wp metrics report --scenario stuck-flush`
3. **Compare config across nodes** — `wp config diff --all`
4. **Check for gossip split-brain** — `wp cluster gossip-diff`
5. **Download a CPU profile** — `wp profile <node> --type cpu --seconds 30`
6. **Change log level for debugging** — `wp logging set-level <node> --level debug`
7. **Find the busiest node** — `wp metrics top --by woodpecker_server_logstore_operation_latency`
8. **Check for under-replication** — `wp metrics report --scenario under-replication`
9. **Scale a K8s cluster** — `wp k8s scale --replicas 5 -x`
10. **Quick health check** — `wp cluster health`, interpret red/yellow/green

- [x] **Step 2: Commit**

```bash
git add docs/wpcli/cookbook.md
git commit -m "docs(wpcli): add 10-recipe incident response cookbook"
```

---

### Task 16: `docs/wpcli/configuration.md`

**Goal:** Full reference documentation for `cli.yaml` — every field, every default, every environment variable override.

**Files:**
- Create: `docs/wpcli/configuration.md`

- [x] **Step 1: Write reference**

Sections:
1. **File location** — `$WOODPECKER_CLI_CONFIG`, `$XDG_CONFIG_HOME/woodpecker/cli.yaml`, `~/.woodpecker/cli.yaml`
2. **Structure** — annotated YAML with all fields
3. **Context fields** — endpoint, admin_port, timeout, concurrency, strict, k8s sub-block
4. **Defaults section** — output format, no_color, page_size
5. **K8s sub-block** — namespace, cluster, kube_context, kubeconfig, kubectl
6. **Global flag overrides** — table of flag → config field → env var mapping
7. **Precedence** — flag > env var > cli.yaml > default
8. **Examples** — single-node dev, multi-context prod/staging, K8s-enabled

- [x] **Step 2: Commit**

```bash
git add docs/wpcli/configuration.md
git commit -m "docs(wpcli): add full cli.yaml configuration reference"
```

---

## Part E — Verification (Tasks 17-18)

### Task 17: Manual K8s E2E verification (kind)

**Goal:** Run the F family commands against a kind cluster with the Woodpecker operator and record results.

**Files:** None (verification only, results recorded in Task 18's verification log)

**Spec reference:** §4.4 — kind cluster verification.

- [x] **Step 1: Set up kind cluster**

```bash
kind create cluster --name wp-test
# Install Woodpecker operator (if available) or mock the CRD
kubectl apply -f deploy/operator/crd.yaml  # or similar
```

- [x] **Step 2: Test F family commands**

```bash
./bin/wp k8s status                    # print mode
./bin/wp k8s status -x                 # execute mode
./bin/wp k8s scale --replicas 3        # print mode
./bin/wp k8s logs wp-server-0 --tail 5 # print mode
./bin/wp k8s doctor                    # stub, exit 10
```

Expected: print mode produces correct kubectl commands; execute mode runs them (may fail if no real cluster, which is OK for verification — the point is that the commands are constructed correctly and kubectl is invoked).

- [x] **Step 3: Record results**

Note any issues found for hotfix.

- [x] **Step 4: Clean up**

```bash
kind delete cluster --name wp-test
```

---

### Task 18: Spec finalize + Phase 3 verification sweep

**Goal:** Final verification: run all tests, build all binaries, verify spec coverage is complete, remove WIP markers from design doc.

**Files:**
- Modify: `docs/wpcli-design.md` — remove WIP header, set status to "Implemented"
- Create: `docs/wpcli/phase3-verification.md` (optional verification log)

- [x] **Step 1: Run full test suite**

```bash
go test ./... -race -count=1
```

Expected: PASS.

- [x] **Step 2: Build and verify**

```bash
make wpcli
./bin/wp version
./bin/wp help
```

Expected: help lists all 10 command groups (node, cluster, config, env, profile, ctx, logging, logstore, ops, metrics, k8s). Version shows clean git commit.

- [x] **Step 3: Command count verification**

Total 35 commands (14 Phase 1 + 17 Phase 2 + 4 Phase 3).

- [x] **Step 4: Spec coverage check**

Open `docs/wpcli-design.md` §5.3, §5.4, §5.5. Verify every listed command, endpoint, and package has been implemented across the three phases.

- [x] **Step 5: Update design doc**

Remove the WIP marker from the header. Set status to "Implemented — Phase 1/2/3 complete."

- [x] **Step 6: Commit**

```bash
git add docs/wpcli-design.md
git commit -m "docs(wpcli): finalize spec — mark all phases as Implemented"
```

---

## Self-Review Checklist

- [x] **Spec coverage:** every command in §2.F / §5.5 has a completed task
- [x] **No server changes:** Phase 3 touches zero files in `server/`, `common/http/`, or `common/metrics/`
- [x] **Exit code coverage:** k8s passthrough exit code (100+) has a test
- [x] **Print/execute duality:** every F family command works in both modes
- [x] **kubectl missing handling:** auto-downgrade to print mode when kubectl not in PATH
- [x] **Cross-platform build:** 4 binaries produced by `make wpcli-release`
- [x] **Documentation completeness:** README, quickstart, cookbook, configuration reference all present

---

## Execution Handoff

Plan complete and saved to `docs/wpcli-plan-phase3.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task
2. **Inline Execution** — execute tasks in this session

Which approach?
