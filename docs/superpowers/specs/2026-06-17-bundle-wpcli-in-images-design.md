# Bundle `wp` (wpcli) into server images for zero-config in-pod ops

- **Issue:** [zilliztech/woodpecker#187](https://github.com/zilliztech/woodpecker/issues/187)
- **Date:** 2026-06-17
- **Status:** Approved design, ready for implementation plan

## Problem

`wp` (the Woodpecker operational CLI, `cmd/wpcli/`) is how we operate service-mode
clusters (`wp node`, `wp cluster`, `wp logstore`, `wp metrics`, …). Today it ships
only as standalone release binaries, so an operator must download/install the right
arch binary and point it at the cluster. The in-image plumbing is half-done:

1. **Inconsistent packaging.** `make build-linux` / `build-linux-arm64`
   (`Makefile:171-178`) already build `bin/wp` (static, `CGO_ENABLED=0`, with version
   ldflags) for both arches, and `build/build_bin.sh:102-107` asserts it exists — so
   `bin/wp` is always present at COPY time. Yet only `build/docker/ubuntu22.04/Dockerfile`
   copies it in; `ubuntu20.04`, `amazonlinux2023`, and `rockylinux8` do not.
2. **Not on `PATH`.** Where present it lives at `/woodpecker/bin/wp` (WORKDIR
   `/woodpecker`), so an operator must type `bin/wp …` or a full path.
3. **Not zero-config in-pod.** `wp` resolves its endpoint from `--endpoint`, a
   `cli.yaml` context, or — per the error string at `cmd/wpcli/cmd/resolver.go:55` —
   `$WOODPECKER_ENDPOINT`. But `WOODPECKER_ENDPOINT` is **advertised yet never read**
   anywhere in `cmd/wpcli/`, so every in-pod command needs `--endpoint http://localhost:9091`.

The natural in-pod endpoint is `http://localhost:9091`: the admin HTTP API is served by
`commonhttp.Start` on `METRICS_PORT`, which defaults to `9091`
(`common/http/server.go:70-71`); the server images don't set `METRICS_PORT`, so the
server listens on 9091 inside the container.

## Goals

- `kubectl exec -it <woodpecker-pod> -- wp cluster info` (and `wp node list`, …) work
  with nothing to install and no flags, across **all four** server images.
- External/binary usage (`wp` on a laptop against a remote cluster) is unaffected and
  still errors loudly when no target is configured.
- All four server Dockerfiles are consistent.
- Docs accurately describe the in-pod experience and the endpoint precedence.

## Non-Goals

- **No build-pipeline change.** `bin/wp` is already produced for both arches.
- **Standalone `release-wpcli.yaml` flow stays as-is.**
- **Operator image** (`deployments/operator/Dockerfile`) and the upgrade-test writer
  image (`tests/upgrade/Dockerfile.writer`) do not get `wp` — different components.
- **Only `WOODPECKER_ENDPOINT`** is implemented at the value level. Other env vars in
  the design-doc table (`WOODPECKER_CONTEXT`, `WOODPECKER_ADMIN_PORT`, …) are out of
  scope and remain unimplemented.

## Key decision — endpoint precedence (diverges from the issue)

The issue proposes `--endpoint > cli.yaml > $WOODPECKER_ENDPOINT` (env **lowest**).
Per maintainer decision we instead implement:

```
--endpoint flag  >  $WOODPECKER_ENDPOINT  >  cli.yaml context  >  defaults
```

i.e. **env above cli.yaml**, matching the precedence already documented in
`docs/wip/wpcli/wpcli-design.md` §3.2 (`flag > env > context > defaults`) and
`docs/admin-guides/wpcli/configuration.md` §Precedence. Choosing this keeps the
existing docs consistent rather than introducing a per-variable special case.

**Consequence (must be documented):** the image-baked `ENV
WOODPECKER_ENDPOINT=http://localhost:9091` will override a `cli.yaml` *mounted into the
pod*. To target a different endpoint in-pod, use `--endpoint` (highest priority). This
is an intentional divergence from the issue text and will be called out in the PR.

**Why this still satisfies all goals:**
- In-pod, zero config: no flag, no user env → image `ENV` provides `localhost:9091`. ✓
- On a laptop: the image `ENV` is not present, so behavior is unchanged; a user who
  sets `WOODPECKER_ENDPOINT` themselves gets the documented override. ✓
- Downstream tolerates the otherwise-empty context: `client.New` defaults `Timeout`→30s
  and treats `AdminPort==0` as "extract from seed"; `cmd/wpcli/client/fanout.go:26-29`
  defaults `Concurrency`→8 and `Timeout`→30s. So endpoint is the only wiring needed.

## Design

### A. Dockerfiles — make all four consistent
For `ubuntu20.04`, `ubuntu22.04`, `amazonlinux2023`, `rockylinux8`:
1. `COPY --chmod=755 ./bin/wp /woodpecker/bin/wp` — add to the three missing it
   (ubuntu22.04 already has it), right after the `woodpecker` COPY.
2. `RUN ln -sf /woodpecker/bin/wp /usr/local/bin/wp` — add to all four, after the COPY,
   so `wp` is on `PATH` from any directory.
3. Append `WOODPECKER_ENDPOINT="http://localhost:9091"` to the existing `ENV` block in
   all four (matching the quoted style already used).

### B. Go — `cmd/wpcli/cmd/resolver.go`
- Add a pure helper:
  ```go
  // resolveEndpoint applies endpoint precedence:
  // --endpoint flag > $WOODPECKER_ENDPOINT > cli.yaml context.
  func resolveEndpoint(flagEndpoint, envEndpoint, ctxEndpoint string) string
  ```
- In `resolveAndDiscover`, replace the endpoint flag-override block with
  `ctx.Endpoint = resolveEndpoint(Globals.Endpoint, os.Getenv("WOODPECKER_ENDPOINT"), ctx.Endpoint)`
  (where `ctx.Endpoint` is the cli.yaml-loaded value). Add the `"os"` import. Leave the
  `AdminPort`/`Timeout`/`Concurrency`/`Strict` overrides unchanged.

### C. Test — `cmd/wpcli/cmd/resolver_test.go` (new)
- Table-driven unit test of `resolveEndpoint` proving precedence across every
  flag/env/ctx combination (flag wins; else env; else ctx; else empty), `testify/require`
  style consistent with `cmd/wpcli/cmd/*_test.go`.

### D. Docs (all three)
- `cmd/wpcli/README.md`: add an "In-pod usage" note (`kubectl exec … -- wp cluster info`,
  zero-config, bundled in all server images) and the `WOODPECKER_ENDPOINT` precedence.
- `docs/admin-guides/wpcli/configuration.md`: extend §Precedence item 2 to name
  `WOODPECKER_ENDPOINT` (value-level, overrides cli.yaml) and document the in-pod default.
- `docs/wip/wpcli/wpcli-design.md` §3.2: note `WOODPECKER_ENDPOINT` is now implemented
  and is the in-pod default via the image `ENV` (the chain already reads `flag > env >
  context`).

## Verification

- `go build ./...` clean.
- `go test ./cmd/wpcli/...` passes (incl. the new `resolveEndpoint` test).
- `golangci-lint run` clean on changed Go files.
- Cross-Dockerfile consistency: all four contain the `wp` COPY, the `ln` to
  `/usr/local/bin/wp`, and the `WOODPECKER_ENDPOINT` ENV.
- Docs accurately state `flag > env > cli.yaml`.

## Execution

- Branch: `enhance/issue-187-bundle-wpcli-in-images`.
- Implement via a workflow: parallel edits of independent file-groups (Go+test / four
  Dockerfiles / docs) → verify phase (build, test, lint, Dockerfile-consistency,
  docs-accuracy) → adversarial review of the precedence logic.
- Then code review → PR (explicitly noting the deliberate precedence divergence from #187).
