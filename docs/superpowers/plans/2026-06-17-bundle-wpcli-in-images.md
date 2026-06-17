# Bundle `wp` into server images — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. (This plan is being executed via the user-requested Workflow orchestration; the task boundaries below map 1:1 to the workflow's parallel stages.)

**Goal:** Make `wp` a first-class, zero-config in-pod tool across all four Woodpecker server images, and implement the `WOODPECKER_ENDPOINT` env var.

**Architecture:** Three independent, disjoint-file deliverables — (1) a Go env-fallback in `cmd/wpcli/cmd/resolver.go` with a pure, unit-tested helper; (2) consistent Dockerfile edits across all four server images (COPY `wp`, symlink onto `PATH`, set `ENV`); (3) doc reconciliation. No build-pipeline change (`bin/wp` already produced).

**Tech Stack:** Go (cobra CLI, `testify/require`), Docker (multi-stage server images), Markdown docs.

## Global Constraints

- **Endpoint precedence (verbatim):** `--endpoint flag > $WOODPECKER_ENDPOINT > cli.yaml context > defaults`. Env is **above** cli.yaml. This diverges from issue #187 (which proposed env lowest) per maintainer decision.
- **In-pod default endpoint:** `http://localhost:9091` (admin HTTP API; `METRICS_PORT` defaults to 9091, unset in images).
- **Scope:** Only `WOODPECKER_ENDPOINT` is implemented at the value level. Do **not** wire other env vars.
- **Four server images only:** `build/docker/{ubuntu20.04,ubuntu22.04,amazonlinux2023,rockylinux8}/Dockerfile`. Do **not** touch `deployments/operator/Dockerfile` or `tests/upgrade/Dockerfile.writer`.
- **Commit style:** conventional commits; end messages with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

---

### Task 1: Implement `WOODPECKER_ENDPOINT` env fallback (Go)

**Files:**
- Modify: `cmd/wpcli/cmd/resolver.go` (imports; step 2 of `resolveAndDiscover`; add helper)
- Test: `cmd/wpcli/cmd/resolver_test.go` (create)

**Interfaces:**
- Produces: `func resolveEndpoint(flagEndpoint, envEndpoint, ctxEndpoint string) string` — returns the first non-empty of (flag, env, ctx) in that order; `""` if all empty.

- [ ] **Step 1: Write the failing test** — create `cmd/wpcli/cmd/resolver_test.go`:

```go
package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveEndpoint(t *testing.T) {
	const flag, env, ctx = "http://flag:9091", "http://env:9091", "http://ctx:9091"
	tests := []struct {
		name             string
		flag, env, ctx   string
		want             string
	}{
		{"flag beats env and ctx", flag, env, ctx, flag},
		{"flag beats env (no ctx)", flag, env, "", flag},
		{"flag beats ctx (no env)", flag, "", ctx, flag},
		{"flag only", flag, "", "", flag},
		{"env beats ctx (no flag)", "", env, ctx, env},
		{"env only", "", env, "", env},
		{"ctx only", "", "", ctx, ctx},
		{"all empty -> empty", "", "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, resolveEndpoint(tt.flag, tt.env, tt.ctx))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/wpcli/cmd/ -run TestResolveEndpoint`
Expected: FAIL — `undefined: resolveEndpoint`.

- [ ] **Step 3: Add the `"os"` import** to `cmd/wpcli/cmd/resolver.go`:

```go
import (
	"fmt"
	"os"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)
```

- [ ] **Step 4: Replace the endpoint flag-override block.** In `resolveAndDiscover`, replace:

```go
	// 2. Flag / env overrides on top of context.
	if Globals.Endpoint != "" {
		ctx.Endpoint = Globals.Endpoint
	}
```

with:

```go
	// 2. Flag / env overrides on top of context.
	// Endpoint precedence: --endpoint flag > $WOODPECKER_ENDPOINT > cli.yaml context.
	ctx.Endpoint = resolveEndpoint(Globals.Endpoint, os.Getenv("WOODPECKER_ENDPOINT"), ctx.Endpoint)
```

- [ ] **Step 5: Add the helper** at the end of `cmd/wpcli/cmd/resolver.go`:

```go
// resolveEndpoint applies the wp endpoint precedence:
// --endpoint flag > $WOODPECKER_ENDPOINT env > cli.yaml context.
// It returns the first non-empty value in that order, or "" if all are empty
// (which the caller reports as a usage error). Env is intentionally above
// cli.yaml so the server images' baked-in WOODPECKER_ENDPOINT gives zero-config
// in-pod ops; pass --endpoint to override from inside a pod.
func resolveEndpoint(flagEndpoint, envEndpoint, ctxEndpoint string) string {
	switch {
	case flagEndpoint != "":
		return flagEndpoint
	case envEndpoint != "":
		return envEndpoint
	default:
		return ctxEndpoint
	}
}
```

- [ ] **Step 6: Run tests + build to verify pass**

Run: `go test ./cmd/wpcli/... && go build ./...`
Expected: PASS; build clean.

- [ ] **Step 7: Lint changed file**

Run: `golangci-lint run cmd/wpcli/cmd/...` (skip gracefully if not installed)
Expected: no new findings.

- [ ] **Step 8: Commit**

```bash
git add cmd/wpcli/cmd/resolver.go cmd/wpcli/cmd/resolver_test.go
git commit -m "feat(wpcli): read WOODPECKER_ENDPOINT as endpoint fallback (#187)"
```

---

### Task 2: Bundle `wp` in all four server images

**Files (all Modify):**
- `build/docker/ubuntu20.04/Dockerfile`
- `build/docker/ubuntu22.04/Dockerfile` (already has the COPY — add symlink + ENV only)
- `build/docker/amazonlinux2023/Dockerfile`
- `build/docker/rockylinux8/Dockerfile`

**Interfaces:** none (image config). Deliverable: every image contains the `wp` COPY, the `/usr/local/bin/wp` symlink, and `ENV WOODPECKER_ENDPOINT=http://localhost:9091`.

- [ ] **Step 1: ubuntu20.04 / amazonlinux2023 / rockylinux8 — add COPY + symlink.** After the line `COPY --chmod=755 ./bin/woodpecker /woodpecker/bin/woodpecker`, insert:

```dockerfile

# Copy wp CLI binary (operational CLI; bundled for zero-config in-pod ops)
COPY --chmod=755 ./bin/wp /woodpecker/bin/wp

# Put wp on PATH so `wp ...` works from any directory in the pod
RUN ln -sf /woodpecker/bin/wp /usr/local/bin/wp
```

- [ ] **Step 2: ubuntu22.04 — add symlink only** (COPY already present at the `# Copy wp CLI binary` block). Immediately after `COPY --chmod=755 ./bin/wp /woodpecker/bin/wp`, insert:

```dockerfile

# Put wp on PATH so `wp ...` works from any directory in the pod
RUN ln -sf /woodpecker/bin/wp /usr/local/bin/wp
```

- [ ] **Step 3: All four — add the ENV.** In each `ENV` block, change the final line `    SERVICE_SEEDS=""` to:

```dockerfile
    SERVICE_SEEDS="" \
    WOODPECKER_ENDPOINT="http://localhost:9091"
```

- [ ] **Step 4: Consistency check**

Run:
```bash
for d in ubuntu20.04 ubuntu22.04 amazonlinux2023 rockylinux8; do
  f=build/docker/$d/Dockerfile
  echo "== $d =="
  grep -c 'COPY --chmod=755 ./bin/wp /woodpecker/bin/wp' "$f"
  grep -c 'ln -sf /woodpecker/bin/wp /usr/local/bin/wp' "$f"
  grep -c 'WOODPECKER_ENDPOINT="http://localhost:9091"' "$f"
done
```
Expected: each count is exactly `1` for all four images.

- [ ] **Step 5: Commit**

```bash
git add build/docker/*/Dockerfile
git commit -m "enhance(docker): bundle wp in all server images, on PATH, zero-config endpoint (#187)"
```

---

### Task 3: Reconcile docs

**Files (all Modify):**
- `cmd/wpcli/README.md`
- `docs/admin-guides/wpcli/configuration.md`
- `docs/wip/wpcli/wpcli-design.md`

- [ ] **Step 1: `cmd/wpcli/README.md` — add an in-pod section** after the `## Quick start` block (after the line linking quickstart.md):

```markdown

## In-pod usage (zero-config)

`wp` is bundled in all server images and is on `PATH`, so you can operate a
cluster straight from a server pod with nothing to install:

```bash
kubectl exec -it <woodpecker-pod> -- wp cluster info
kubectl exec -it <woodpecker-pod> -- wp node list
```

This works with no flags because the images set
`WOODPECKER_ENDPOINT=http://localhost:9091` and the admin HTTP API listens on
`9091` inside the container.

### Endpoint precedence

`wp` resolves the admin endpoint in this order (highest priority first):

1. `--endpoint <url>` flag
2. `$WOODPECKER_ENDPOINT`
3. `cli.yaml` active context

Because `$WOODPECKER_ENDPOINT` is baked into the server images, a `cli.yaml`
mounted into a pod is overridden by it; pass `--endpoint` to target a different
cluster from inside a pod.
```

- [ ] **Step 2: `docs/admin-guides/wpcli/configuration.md` — update §Precedence.** Replace item 2 line `2. **Environment variables** (`$WOODPECKER_CLI_CONFIG`)` with:

```markdown
2. **Environment variables** — `$WOODPECKER_ENDPOINT` (the admin endpoint;
   overrides the cli.yaml context); `$WOODPECKER_CLI_CONFIG` (selects which
   `cli.yaml` to load)
```

Then add, immediately after the precedence list (after the `4. **Hardcoded defaults**` line):

```markdown

> Server images set `WOODPECKER_ENDPOINT=http://localhost:9091`, so `wp` runs
> zero-config inside a pod. Because env beats the cli.yaml context, this also
> overrides a `cli.yaml` mounted into the pod — pass `--endpoint` to target a
> different cluster from in-pod.
```

- [ ] **Step 3: `docs/wip/wpcli/wpcli-design.md` — annotate §3.2.** Immediately after the flag/env/context mapping table (after the `-v, --verbose` row), add:

```markdown

> **Note (#187):** `WOODPECKER_ENDPOINT` is implemented in
> `cmd/wpcli/cmd/resolver.go` and is the zero-config in-pod default — all server
> images set `WOODPECKER_ENDPOINT=http://localhost:9091`. Per the chain above it
> sits above the cli.yaml context, so it overrides a pod-mounted `cli.yaml`.
```

- [ ] **Step 4: Sanity check** the three edits land:

Run:
```bash
grep -c 'In-pod usage' cmd/wpcli/README.md
grep -c 'WOODPECKER_ENDPOINT' docs/admin-guides/wpcli/configuration.md
grep -c 'Note (#187)' docs/wip/wpcli/wpcli-design.md
```
Expected: `1` each.

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/README.md docs/admin-guides/wpcli/configuration.md docs/wip/wpcli/wpcli-design.md
git commit -m "docs(wpcli): document in-pod wp usage and WOODPECKER_ENDPOINT precedence (#187)"
```

---

### Task 4: Final verification (cross-cutting)

**Files:** none (verification only).

- [ ] **Step 1: Build + test + vet**

Run: `go build ./... && go test ./cmd/wpcli/... && go vet ./cmd/wpcli/...`
Expected: all clean/PASS.

- [ ] **Step 2: Lint**

Run: `golangci-lint run cmd/wpcli/cmd/...` (skip gracefully if not installed)
Expected: no new findings.

- [ ] **Step 3: Dockerfile consistency** — rerun Task 2 Step 4; expect `1` for all four.

- [ ] **Step 4: Optional real image build** (only if Docker available + `bin/wp` built):

Run: `make build-linux && docker build -f build/docker/ubuntu22.04/Dockerfile -t wp-check:local . && docker run --rm --entrypoint wp wp-check:local version`
Expected: `wp` resolves on PATH and prints its version. (Skip if Docker unavailable.)

## Self-Review

- **Spec coverage:** Dockerfile COPY+PATH+ENV → Task 2; `WOODPECKER_ENDPOINT` Go + precedence + unit test → Task 1; docs (README + configuration.md + design §3.2) → Task 3; verification → Task 4. All spec sections covered.
- **Placeholder scan:** none — every step has concrete code/commands.
- **Type consistency:** `resolveEndpoint(flagEndpoint, envEndpoint, ctxEndpoint string) string` is defined in Task 1 Step 5 and called identically in Task 1 Step 4 and tested in Task 1 Step 1. Argument order (flag, env, ctx) matches the precedence everywhere.
