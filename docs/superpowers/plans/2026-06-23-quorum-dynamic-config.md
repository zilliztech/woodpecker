# Dynamic Quorum Client Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the embedding application change client-side quorum policy (`Replicas`, `Strategy`, `AffinityMode`) at runtime without restarting Woodpecker, via a generic `Dynamic[T]` field wrapper.

**Architecture:** A small generic `Dynamic[T]` type wraps a leaf scalar config field: it parses from YAML like a bare `T`, optionally consults a runtime `source func() (T, bool)` bound by the app, and is read via `.Get()`. Only the three quorum scalars are wrapped. `quorumDiscovery` stops caching derived state at construction and recomputes per `SelectQuorum` call (which runs once per new segment), so changes take effect at the next segment boundary and never disturb in-flight segments.

**Tech Stack:** Go 1.25 (generics), `gopkg.in/yaml.v3`, `testify` (assert/mock/require), `go.uber.org/zap`.

**Design doc:** `docs/superpowers/specs/2026-06-23-quorum-dynamic-config-design.md`

## Global Constraints

- Go module path: `github.com/zilliztech/woodpecker`. Go version: 1.25.
- License header (Apache 2.0, the LF AI & Data foundation block) MUST be the first thing in every new `.go` file — copy it verbatim from the top of `common/config/configuration.go`.
- No new third-party dependencies.
- `go build ./...` must stay green at every commit; the type-change commit (Task 3) is necessarily atomic across production + test code.
- Backward compatibility: with no source bound, behavior and the `woodpecker.yaml` format are byte-for-byte unchanged.
- Only `Replicas`, `Strategy`, `AffinityMode` become `Dynamic[T]`. `BufferPools` and `CustomPlacement` stay plain values.
- Concurrency contract: source funcs may be called concurrently; the app guarantees they are concurrency-safe. Woodpecker adds no locking on the read path.
- Verification commands: build = `go build ./...`; unit tests = `go test ./common/config/... ./woodpecker/quorum/... ./woodpecker/... ./cmd/...`; full = `go test ./...` (integration tests under `tests/integration/` have no build tags, so they must at least compile).

---

## File Structure

- `common/config/dynamic.go` (**create**) — the generic `Dynamic[T]` type + YAML integration. One responsibility: the value wrapper.
- `common/config/dynamic_test.go` (**create**) — unit tests for `Dynamic[T]`.
- `common/config/configuration.go` (**modify**) — wrap the three quorum scalar field types; route `GetEnsembleSize` and validation reads through `.Get()`; wrap defaults with `NewDynamic`.
- `common/config/configuration_test.go` (**modify**) — migrate literals/reads of the three fields.
- `woodpecker/quorum/discovery.go` (**modify**) — drop construction-time cache; extract pure helpers; recompute per call; strict construction validation, tolerant runtime.
- `woodpecker/quorum/discovery_test.go` (**modify**) — migrate `QuorumSelectStrategy{}` literals; add new dynamic-behavior tests.
- `cmd/external/user_config.go` (**modify**) — merge-guard comparisons use `.Get()`.
- `woodpecker/woodpecker_client_test.go`, `tests/integration/quorum_discovery_test.go`, `tests/integration/e2e_service_failover_test.go` (**modify**) — migrate literals/reads/assignments.

---

## Task 1: The `Dynamic[T]` primitive

**Files:**
- Create: `common/config/dynamic.go`
- Test: `common/config/dynamic_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type Dynamic[T any] struct { ... }`
  - `func NewDynamic[T any](v T) Dynamic[T]`
  - `func (d Dynamic[T]) Get() T`
  - `func (d *Dynamic[T]) WithSource(source func() (T, bool))`
  - `func (d *Dynamic[T]) Set(v T)`
  - `func (d *Dynamic[T]) UnmarshalYAML(node *yaml.Node) error`
  - `func (d Dynamic[T]) MarshalYAML() (interface{}, error)`

- [ ] **Step 1: Write the failing test**

Create `common/config/dynamic_test.go` (license header first, then):

```go
package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDynamic_GetReturnsStaticWhenUnbound(t *testing.T) {
	d := NewDynamic(3)
	assert.Equal(t, 3, d.Get())
}

func TestDynamic_SourceWinsWhenBoundAndPresent(t *testing.T) {
	d := NewDynamic("random")
	d.WithSource(func() (string, bool) { return "cross-region", true })
	assert.Equal(t, "cross-region", d.Get())
}

func TestDynamic_FallsBackWhenSourceReturnsFalse(t *testing.T) {
	d := NewDynamic("random")
	d.WithSource(func() (string, bool) { return "ignored", false })
	assert.Equal(t, "random", d.Get())
}

func TestDynamic_NilSourceClears(t *testing.T) {
	d := NewDynamic(5)
	d.WithSource(func() (int, bool) { return 7, true })
	assert.Equal(t, 7, d.Get())
	d.WithSource(nil)
	assert.Equal(t, 5, d.Get())
}

func TestDynamic_SetOverwritesStatic(t *testing.T) {
	d := NewDynamic(3)
	d.Set(5)
	assert.Equal(t, 5, d.Get())
}

func TestDynamic_YAMLRoundTripLikeBareT(t *testing.T) {
	type holder struct {
		Replicas Dynamic[int]    `yaml:"replicas"`
		Strategy Dynamic[string] `yaml:"strategy"`
	}
	var h holder
	require.NoError(t, yaml.Unmarshal([]byte("replicas: 5\nstrategy: custom\n"), &h))
	assert.Equal(t, 5, h.Replicas.Get())
	assert.Equal(t, "custom", h.Strategy.Get())

	out, err := yaml.Marshal(h)
	require.NoError(t, err)
	assert.Contains(t, string(out), "replicas: 5")
	assert.Contains(t, string(out), "strategy: custom")
}

func TestDynamic_YAMLAbsentKeyKeepsDefault(t *testing.T) {
	type holder struct {
		Replicas Dynamic[int] `yaml:"replicas"`
	}
	h := holder{Replicas: NewDynamic(3)}
	require.NoError(t, yaml.Unmarshal([]byte("other: 1\n"), &h))
	assert.Equal(t, 3, h.Replicas.Get())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./common/config/ -run TestDynamic -v`
Expected: FAIL — `undefined: NewDynamic` / `undefined: Dynamic`.

- [ ] **Step 3: Write minimal implementation**

Create `common/config/dynamic.go` (license header first, then):

```go
package config

import "gopkg.in/yaml.v3"

// Dynamic wraps a single config value so it can be read either from its static
// (YAML-parsed) value or from an optional runtime source supplied by the
// embedding application. With no source bound, Get() returns the static value,
// so existing behavior is preserved byte-for-byte.
type Dynamic[T any] struct {
	value  T                // static value from YAML / defaults
	source func() (T, bool) // optional runtime override
}

// NewDynamic builds a Dynamic with a static default (used by the defaults builder).
func NewDynamic[T any](v T) Dynamic[T] { return Dynamic[T]{value: v} }

// Get returns the dynamic value when a source is bound and has an opinion,
// otherwise the static value.
func (d Dynamic[T]) Get() T {
	if d.source != nil {
		if v, ok := d.source(); ok {
			return v
		}
	}
	return d.value
}

// WithSource binds (or, with nil, clears) the dynamic source. Bind at startup,
// before the client is used concurrently.
func (d *Dynamic[T]) WithSource(source func() (T, bool)) { d.source = source }

// Set overwrites the static value (defaults builder, config merge, tests).
func (d *Dynamic[T]) Set(v T) { d.value = v }

// UnmarshalYAML decodes a bare T into the static value, so a Dynamic[T] field
// parses exactly like a plain T on disk.
func (d *Dynamic[T]) UnmarshalYAML(node *yaml.Node) error { return node.Decode(&d.value) }

// MarshalYAML emits the static value, so a Dynamic[T] field serializes like T.
func (d Dynamic[T]) MarshalYAML() (interface{}, error) { return d.value, nil }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./common/config/ -run TestDynamic -v`
Expected: PASS (all 7 tests).

- [ ] **Step 5: Commit**

```bash
git add common/config/dynamic.go common/config/dynamic_test.go
git commit -m "feat(config): add generic Dynamic[T] value wrapper (#195)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Refactor `discovery.go` to recompute per call (no type change yet)

This task removes the construction-time cache and extracts pure helpers, **while
the config fields are still plain** (`string`/`int`). All existing quorum tests
must stay green. The type change happens in Task 3.

**Files:**
- Modify: `woodpecker/quorum/discovery.go`
- Test: `woodpecker/quorum/discovery_test.go` (run existing tests; no edits here)

**Interfaces:**
- Consumes: `config.QuorumConfig` with plain `SelectStrategy.{Replicas int, Strategy string, AffinityMode string}` (unchanged at this point); `GetEnsembleSize/GetWriteQuorumSize/GetAckQuorumSize`.
- Produces (within package `quorum`):
  - struct `quorumDiscovery { cfg *config.QuorumConfig; clientPool client.LogStoreClientPool }`
  - pure funcs: `parseAffinity(mode string) (proto.AffinityMode, error)`, `parseStrategy(strategy string) proto.StrategyType`, `buildFilters(ctx context.Context, strategy string, es int32, custom []config.CustomPlacement, pools []config.QuorumBufferPool) ([]*proto.NodeFilter, error)`, plus `buildSingleFilter(es int32) []*proto.NodeFilter`, `buildCrossRegionFilters(ctx context.Context, es int32, pools []config.QuorumBufferPool) ([]*proto.NodeFilter, error)`, `buildCustomFilters(ctx context.Context, es int32, custom []config.CustomPlacement) ([]*proto.NodeFilter, error)`.
  - helper methods: `(d *quorumDiscovery) es()/wq()/aq() int32`, `(d *quorumDiscovery) affinityMode() proto.AffinityMode`, `(d *quorumDiscovery) strategyType() proto.StrategyType`.
  - `select*` methods now take `filters []*proto.NodeFilter`; `requestNodesFromPool/requestNodesFromSeed` and `newQuorumInfo` read derived values via the helper methods.

- [ ] **Step 1: Run the existing tests first (baseline must be green)**

Run: `go test ./woodpecker/quorum/ -count=1`
Expected: PASS (records the green baseline before refactor).

- [ ] **Step 2: Replace the struct and constructor**

In `woodpecker/quorum/discovery.go`, replace the `quorumDiscovery` struct (lines 28-39) and `NewQuorumDiscovery` (lines 41-79) with:

```go
type quorumDiscovery struct {
	cfg        *config.QuorumConfig
	clientPool client.LogStoreClientPool
}

func NewQuorumDiscovery(ctx context.Context, cfg *config.QuorumConfig, clientPool client.LogStoreClientPool) (QuorumDiscovery, error) {
	logger.Ctx(ctx).Info("Initializing active quorum discovery")

	d := &quorumDiscovery{cfg: cfg, clientPool: clientPool}

	// Fail-fast validation of the STATIC config (preserves current behavior and
	// the construction-error tests). The per-call read path (SelectQuorum) is
	// tolerant so a bad dynamic override cannot wedge writes.
	if _, err := parseAffinity(cfg.SelectStrategy.AffinityMode); err != nil {
		logger.Ctx(ctx).Warn("Invalid affinity mode", zap.Error(err))
		return nil, err
	}
	if _, err := buildFilters(ctx, cfg.SelectStrategy.Strategy, int32(cfg.GetEnsembleSize()), cfg.SelectStrategy.CustomPlacement, cfg.BufferPools); err != nil {
		logger.Ctx(ctx).Warn("Failed to build filters", zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Info("Successfully initialized quorum discovery",
		zap.Int32("ensembleSize", int32(cfg.GetEnsembleSize())),
		zap.String("strategy", cfg.SelectStrategy.Strategy),
		zap.String("affinityMode", cfg.SelectStrategy.AffinityMode))
	return d, nil
}

// --- derived values, recomputed fresh from cfg on each read ---

func (d *quorumDiscovery) es() int32 { return int32(d.cfg.GetEnsembleSize()) }
func (d *quorumDiscovery) wq() int32 { return int32(d.cfg.GetWriteQuorumSize()) }
func (d *quorumDiscovery) aq() int32 { return int32(d.cfg.GetAckQuorumSize()) }

func (d *quorumDiscovery) strategyType() proto.StrategyType {
	return parseStrategy(d.cfg.SelectStrategy.Strategy)
}

// affinityMode is tolerant at runtime: an unknown value degrades to SOFT with a
// warning instead of erroring (the static value is validated at construction).
func (d *quorumDiscovery) affinityMode() proto.AffinityMode {
	mode, err := parseAffinity(d.cfg.SelectStrategy.AffinityMode)
	if err != nil {
		logger.Ctx(context.Background()).Warn("Unknown affinity mode at runtime, defaulting to SOFT", zap.Error(err))
		return proto.AffinityMode_SOFT
	}
	return mode
}
```

- [ ] **Step 3: Convert the parsers and filter builders to pure functions**

Replace `parseAffinityMode` (82-92), `parseStrategyType` (95-118), `buildFilters` (121-133), `buildCustomFilters` (136-165), `buildCrossRegionFilters` (168-183), `buildSingleFilter` (186-193) with these pure functions:

```go
// parseAffinity converts a string affinity mode to the proto enum. Empty or
// "soft" => SOFT, "hard" => HARD, anything else => error.
func parseAffinity(mode string) (proto.AffinityMode, error) {
	switch mode {
	case "hard":
		return proto.AffinityMode_HARD, nil
	case "soft", "":
		return proto.AffinityMode_SOFT, nil
	default:
		return proto.AffinityMode_SOFT, fmt.Errorf("invalid affinity mode: %s, must be 'hard' or 'soft'", mode)
	}
}

// parseStrategy converts a string strategy to the proto enum. Unknown/empty
// values default to RANDOM for backward compatibility.
func parseStrategy(strategy string) proto.StrategyType {
	switch strategy {
	case "single-az-single-rg":
		return proto.StrategyType_SINGLE_AZ_SINGLE_RG
	case "single-az-multi-rg":
		return proto.StrategyType_SINGLE_AZ_MULTI_RG
	case "multi-az-single-rg":
		return proto.StrategyType_MULTI_AZ_SINGLE_RG
	case "multi-az-multi-rg":
		return proto.StrategyType_MULTI_AZ_MULTI_RG
	case "cross-region":
		return proto.StrategyType_CROSS_REGION
	case "custom":
		return proto.StrategyType_CUSTOM
	case "random-group":
		return proto.StrategyType_RANDOM_GROUP
	default: // "random", "", and any unknown value
		return proto.StrategyType_RANDOM
	}
}

// buildFilters creates node selection filters for the given strategy.
func buildFilters(ctx context.Context, strategy string, es int32, custom []config.CustomPlacement, pools []config.QuorumBufferPool) ([]*proto.NodeFilter, error) {
	switch strategy {
	case "custom":
		return buildCustomFilters(ctx, es, custom)
	case "cross-region":
		return buildCrossRegionFilters(ctx, es, pools)
	default:
		return buildSingleFilter(es), nil
	}
}

func buildCustomFilters(ctx context.Context, es int32, custom []config.CustomPlacement) ([]*proto.NodeFilter, error) {
	requiredNodes := int(es)
	if len(custom) == 0 {
		return nil, fmt.Errorf("custom strategy requires CustomPlacement configuration")
	}
	if len(custom) != requiredNodes {
		return nil, fmt.Errorf("custom placement rules count (%d) must equal required nodes count (%d)", len(custom), requiredNodes)
	}
	filters := make([]*proto.NodeFilter, len(custom))
	for i, placement := range custom {
		filters[i] = &proto.NodeFilter{
			Limit:         1,
			Az:            placement.Az,
			ResourceGroup: placement.ResourceGroup,
		}
		logger.Ctx(ctx).Debug("Built custom filter",
			zap.Int("index", i),
			zap.String("region", placement.Region),
			zap.String("az", placement.Az),
			zap.String("resourceGroup", placement.ResourceGroup))
	}
	return filters, nil
}

func buildCrossRegionFilters(ctx context.Context, es int32, pools []config.QuorumBufferPool) ([]*proto.NodeFilter, error) {
	if len(pools) < 2 {
		return nil, fmt.Errorf("cross-region strategy requires at least two buffer pools")
	}
	logger.Ctx(ctx).Debug("Built cross-region filter", zap.Int("poolsCount", len(pools)))
	return []*proto.NodeFilter{{Limit: es}}, nil
}

func buildSingleFilter(es int32) []*proto.NodeFilter {
	return []*proto.NodeFilter{{Limit: es}}
}
```

- [ ] **Step 4: Rewrite `SelectQuorum` to build filters once and thread them**

Replace `SelectQuorum` (195-227) with:

```go
func (d *quorumDiscovery) SelectQuorum(ctx context.Context) (*proto.QuorumInfo, error) {
	logger.Ctx(ctx).Info("Active discovery: Starting quorum node selection",
		zap.String("strategy", d.cfg.SelectStrategy.Strategy),
		zap.String("affinityMode", d.cfg.SelectStrategy.AffinityMode),
		zap.Int32("ensembleSize", d.es()))

	if len(d.cfg.BufferPools) == 0 {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg("no buffer pools configured")
	}

	// Build filters once (before the retry loop) so a genuine misconfiguration
	// fails fast instead of being retried forever.
	filters, err := buildFilters(ctx, d.cfg.SelectStrategy.Strategy, d.es(), d.cfg.SelectStrategy.CustomPlacement, d.cfg.BufferPools)
	if err != nil {
		return nil, werr.ErrServiceSelectQuorumFailed.WithCauseErr(err)
	}

	var result *proto.QuorumInfo
	err = retry.Do(ctx, func() error {
		var selErr error
		switch d.cfg.SelectStrategy.Strategy {
		case "cross-region":
			result, selErr = d.selectCrossRegionQuorum(ctx, filters)
		case "custom":
			result, selErr = d.selectCustomPlacementQuorum(ctx, filters)
		default:
			result, selErr = d.selectSingleRegionQuorum(ctx, filters)
		}
		return selErr
	}, retry.AttemptAlways(), retry.Sleep(200*time.Millisecond), retry.MaxSleepTime(2*time.Second))
	if err != nil {
		return nil, werr.ErrServiceSelectQuorumFailed.WithCauseErr(err)
	}
	return result, nil
}
```

- [ ] **Step 5: Update the `select*`, `requestNodes*`, `newQuorumInfo`, `fill*` methods**

Apply these exact replacements in `woodpecker/quorum/discovery.go`:

1. `selectSingleRegionQuorum`: change signature to `func (d *quorumDiscovery) selectSingleRegionQuorum(ctx context.Context, filters []*proto.NodeFilter) (*proto.QuorumInfo, error)`. Inside, replace `d.cfg.BufferPools` reads as-is (still valid), replace `d.filters` → `filters`, and `int(d.wq)` → `int(d.wq())`.
2. `newQuorumInfo`: replace `Aq: d.aq` → `Aq: d.aq()`, `Wq: d.wq` → `Wq: d.wq()`, `Es: d.es` → `Es: d.es()`.
3. `selectCrossRegionQuorum`: change signature to `(ctx context.Context, filters []*proto.NodeFilter)`. Replace `requiredNodes := int(d.es)` → `int(d.es())`; `d.filters` → `filters`; `d.affinityMode` → `d.affinityMode()`; the recursive call `d.fillRemainingNodesWithReplicas(ctx, ...)` is unchanged in shape (see item 6).
4. `selectCustomPlacementQuorum`: change signature to `(ctx context.Context, filters []*proto.NodeFilter)`. Replace `requiredNodes := int(d.es)` → `int(d.es())`; `d.filters` → `filters`; `len(d.filters)` → `len(filters)`; `d.filters[i].Az` / `d.filters[i].ResourceGroup` → `filters[i].Az` / `filters[i].ResourceGroup`; `d.es` (as `Limit: d.es`) → `d.es()`. `d.cfg.SelectStrategy.CustomPlacement` reads stay as-is.
5. `requestNodesFromSeed`: replace `d.strategyType` → `d.strategyType()` (both the `SelectNodes` arg and the zap log) and `d.affinityMode` → `d.affinityMode()` (zap log + `.String()`).
6. `fillRemainingNodes` / `fillRemainingNodesWithReplicas`: replace `requiredNodes := int(d.es)` → `int(d.es())`.
7. Remove any now-unused `d.es`/`d.wq`/`d.aq`/`d.affinityMode`/`d.strategyType`/`d.filters` field references that remain in zap logs (e.g. the `SelectQuorum`/`requestNodesFromSeed` logs) — replace with the method-call equivalents shown above.

- [ ] **Step 6: Build and run the quorum tests**

Run: `go build ./woodpecker/quorum/ && go test ./woodpecker/quorum/ -count=1`
Expected: PASS — same set of tests green as the Step 1 baseline (including `TestQuorumDiscovery_InvalidAffinityMode` and the custom-placement construction-error tests, which still fail fast via the Step 2 validation).

- [ ] **Step 7: Commit**

```bash
git add woodpecker/quorum/discovery.go
git commit -m "refactor(quorum): recompute selection state per call, drop ctor cache (#195)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Wrap the three quorum fields in `Dynamic[T]` (atomic breaking change)

This is the single atomic commit that flips the field types and fixes every
call site (production + tests) so the module compiles.

**Files:**
- Modify: `common/config/configuration.go`, `cmd/external/user_config.go`, `woodpecker/quorum/discovery.go`, `common/config/configuration_test.go`, `woodpecker/quorum/discovery_test.go`, `woodpecker/woodpecker_client_test.go`, `tests/integration/quorum_discovery_test.go`, `tests/integration/e2e_service_failover_test.go`

**Interfaces:**
- Consumes: `Dynamic[T]`, `NewDynamic` (Task 1).
- Produces: `QuorumSelectStrategy.{Replicas Dynamic[int], Strategy Dynamic[string], AffinityMode Dynamic[string]}`; `GetEnsembleSize/Write/Ack` now read via `.Get()`.

- [ ] **Step 1: Change the field types in `configuration.go`**

Replace `QuorumSelectStrategy` (lines 90-95) with:

```go
// QuorumSelectStrategy stores the quorum selection strategy configuration.
type QuorumSelectStrategy struct {
	AffinityMode    Dynamic[string]   `yaml:"affinityMode"`
	Replicas        Dynamic[int]      `yaml:"replicas"`
	Strategy        Dynamic[string]   `yaml:"strategy"`
	CustomPlacement []CustomPlacement `yaml:"customPlacement"`
}
```

- [ ] **Step 2: Route the size getter through `.Get()`**

Replace `GetEnsembleSize` (lines 104-112) with:

```go
// GetEnsembleSize returns the ensemble size.
func (q *QuorumConfig) GetEnsembleSize() int {
	switch q.SelectStrategy.Replicas.Get() {
	case 3:
		return 3
	case 5:
		return 5
	default:
		return 3
	}
}
```

(`GetWriteQuorumSize`/`GetAckQuorumSize` are unchanged — they call `GetEnsembleSize`.)

- [ ] **Step 3: Update validation reads in `configuration.go`**

In `validateQuorumConfig`, add `.Get()` to the three wrapped reads:
- line 561: `validAffinityModes[q.SelectStrategy.AffinityMode]` → `validAffinityModes[q.SelectStrategy.AffinityMode.Get()]`
- line 562: `q.SelectStrategy.AffinityMode` → `q.SelectStrategy.AffinityMode.Get()`
- line 577: `validStrategies[q.SelectStrategy.Strategy]` → `validStrategies[q.SelectStrategy.Strategy.Get()]`
- line 579: `q.SelectStrategy.Strategy` → `q.SelectStrategy.Strategy.Get()`
- line 602: `q.SelectStrategy.Strategy == "custom"` → `q.SelectStrategy.Strategy.Get() == "custom"`
- line 638: `q.SelectStrategy.Strategy == "cross-region"` → `q.SelectStrategy.Strategy.Get() == "cross-region"`

- [ ] **Step 4: Wrap the defaults builder**

In `getDefaultWoodpeckerConfig` replace the quorum `SelectStrategy` literal (lines 757-762) with:

```go
				SelectStrategy: QuorumSelectStrategy{
					AffinityMode:    NewDynamic("soft"),
					Replicas:        NewDynamic(3),
					Strategy:        NewDynamic("random"),
					CustomPlacement: []CustomPlacement{},
				},
```

- [ ] **Step 5: Update the config-merge guards**

In `cmd/external/user_config.go` (lines 297-304) replace the three guards (keep the assignments as-is — `Dynamic[string] = Dynamic[string]` is a valid struct copy):

```go
	if src.Client.Quorum.SelectStrategy.AffinityMode.Get() != "" {
		dst.Client.Quorum.SelectStrategy.AffinityMode = src.Client.Quorum.SelectStrategy.AffinityMode
	}
	if src.Client.Quorum.SelectStrategy.Replicas.Get() > 0 {
		dst.Client.Quorum.SelectStrategy.Replicas = src.Client.Quorum.SelectStrategy.Replicas
	}
	if src.Client.Quorum.SelectStrategy.Strategy.Get() != "" {
		dst.Client.Quorum.SelectStrategy.Strategy = src.Client.Quorum.SelectStrategy.Strategy
	}
```

- [ ] **Step 6: Add `.Get()` to the discovery reads of the wrapped fields**

In `woodpecker/quorum/discovery.go`, the only direct reads of the three wrapped fields are of `Strategy` and `AffinityMode` (Replicas is read only via `GetEnsembleSize`). Add `.Get()` to each:
- `NewQuorumDiscovery`: `parseAffinity(cfg.SelectStrategy.AffinityMode)` → `...AffinityMode.Get()`; `buildFilters(ctx, cfg.SelectStrategy.Strategy, ...)` → `...Strategy.Get()`; the two zap logs `cfg.SelectStrategy.Strategy` / `cfg.SelectStrategy.AffinityMode` → add `.Get()`.
- `strategyType()`: `parseStrategy(d.cfg.SelectStrategy.Strategy)` → `...Strategy.Get()`.
- `affinityMode()`: `parseAffinity(d.cfg.SelectStrategy.AffinityMode)` → `...AffinityMode.Get()`.
- `SelectQuorum`: the two zap logs and the `switch d.cfg.SelectStrategy.Strategy` and the `buildFilters(ctx, d.cfg.SelectStrategy.Strategy, ...)` → add `.Get()`.
- `requestNodesFromSeed` zap log `d.cfg.SelectStrategy.Strategy` → `.Get()`.

(`d.cfg.SelectStrategy.CustomPlacement` and `d.cfg.BufferPools` stay plain — no `.Get()`.)

- [ ] **Step 7: Migrate `common/config/configuration_test.go`**

Apply uniformly:
- Reads of the three fields → add `.Get()`: lines 49, 50, 51, 146, 147, 148, 307, 308, 309 (e.g. `...SelectStrategy.AffinityMode` → `...SelectStrategy.AffinityMode.Get()`).
- In every `QuorumSelectStrategy{...}` literal (lines 342, 353, 364, 377, 393, 822, 832, 842, 852, 862, 875, 890, 905, 920, 935, 947, 958, 969, 982), wrap the `AffinityMode:`/`Replicas:`/`Strategy:` values with `NewDynamic(...)` (package-local, no `config.` prefix). Leave `CustomPlacement:` plain. Example transform:

```go
// before
SelectStrategy: QuorumSelectStrategy{Strategy: "random", AffinityMode: "soft", Replicas: 3},
// after
SelectStrategy: QuorumSelectStrategy{Strategy: NewDynamic("random"), AffinityMode: NewDynamic("soft"), Replicas: NewDynamic(3)},
```

(The `GetEnsembleSize()/GetWriteQuorumSize()/GetAckQuorumSize()` assertion sites — 60-62, 157-159, 324-326, 952-965, 974-976, 1087-1089 — need no change; those methods still return `int`.)

- [ ] **Step 8: Migrate `woodpecker/quorum/discovery_test.go`**

These tests only construct `config.QuorumSelectStrategy{...}` literals (they do not read the three fields). In every literal (lines 29, 83, 136, 159, 189, 237, 278, 316, 383, 439, 497, 562, 635, 684, 727, 791, 840, 882, 942, 962, 983, 1004, 1024, 1058, 1079, 1113, 1152, 1404, 1428, 1451, 1482, 1518, 1588), wrap the `Strategy:`/`AffinityMode:`/`Replicas:` values with `config.NewDynamic(...)`. Example:

```go
// before
SelectStrategy: config.QuorumSelectStrategy{Strategy: "custom", AffinityMode: "hard", Replicas: 3, CustomPlacement: [...]},
// after
SelectStrategy: config.QuorumSelectStrategy{Strategy: config.NewDynamic("custom"), AffinityMode: config.NewDynamic("hard"), Replicas: config.NewDynamic(3), CustomPlacement: [...]},
```

The line-936 `AffinityMode: "invalid-mode"` becomes `AffinityMode: config.NewDynamic("invalid-mode")`; that test still asserts `NewQuorumDiscovery` errors (Task 2 construction validation handles it).

- [ ] **Step 9: Migrate the remaining literals**

- `woodpecker/woodpecker_client_test.go` (lines 614, 642, 708, 744, 814, 849): wrap the three fields in each `config.QuorumSelectStrategy{...}` with `config.NewDynamic(...)`.
- `tests/integration/quorum_discovery_test.go` (lines 58, 129, 201, 295, 375, 423): same wrapping.

- [ ] **Step 10: Migrate `tests/integration/e2e_service_failover_test.go`**

This file reads/assigns `Replicas`:
- line 760: `originalReplica := cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas` — keep as-is (now captures a `Dynamic[int]`).
- line 763: `cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas = 5` → `cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas.Set(5)`
- line 765 (printf arg) `...Replicas` → `...Replicas.Get()`
- line 769: `cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas = originalReplica` — keep as-is (assigning `Dynamic[int]`).
- line 771 (printf arg) `...Replicas` → `...Replicas.Get()`
- line 803 (printf arg) `...Replicas` → `...Replicas.Get()`
- line 904: `expectedEnsembleSize := cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas` → append `.Get()`
- line 1195 (printf/arg) `...Replicas` → `...Replicas.Get()`

- [ ] **Step 11: Build the whole module**

Run: `go build ./... && go vet ./...`
Expected: no errors. (If `go vet` flags anything in the migrated files, fix the specific site and re-run.)

- [ ] **Step 12: Run unit tests**

Run: `go test ./common/config/... ./woodpecker/quorum/... ./woodpecker/... ./cmd/... -count=1`
Expected: PASS.

- [ ] **Step 13: Commit**

```bash
git add common/config/configuration.go common/config/configuration_test.go \
  cmd/external/user_config.go woodpecker/quorum/discovery.go \
  woodpecker/quorum/discovery_test.go woodpecker/woodpecker_client_test.go \
  tests/integration/quorum_discovery_test.go tests/integration/e2e_service_failover_test.go
git commit -m "feat(quorum): make Replicas/Strategy/AffinityMode Dynamic[T] (#195)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Dynamic-behavior tests

**Files:**
- Test: `woodpecker/quorum/discovery_test.go` (append), `common/config/configuration_test.go` (append)

**Interfaces:**
- Consumes: `Dynamic[T].WithSource`, `NewQuorumDiscovery`, `SelectQuorum`, `GetEnsembleSize`.

- [ ] **Step 1: Write the config-level dynamic test**

Append to `common/config/configuration_test.go`:

```go
func TestQuorumConfig_DynamicReplicasAffectsEnsembleSize(t *testing.T) {
	q := QuorumConfig{
		SelectStrategy: QuorumSelectStrategy{Replicas: NewDynamic(3)},
	}
	assert.Equal(t, 3, q.GetEnsembleSize())

	current := 5
	q.SelectStrategy.Replicas.WithSource(func() (int, bool) { return current, true })
	assert.Equal(t, 5, q.GetEnsembleSize())

	current = 4 // invalid -> clamps to 3
	assert.Equal(t, 3, q.GetEnsembleSize())
}
```

- [ ] **Step 2: Write the discovery-level dynamic test**

Append to `woodpecker/quorum/discovery_test.go` (mirror the setup of `TestQuorumDiscovery_SelectQuorumNodes_SingleRegion_Success`, but bind a dynamic replicas source and assert the second selection uses the new ensemble size):

```go
func TestQuorumDiscovery_DynamicReplicas_NextSelectionUsesNewSize(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{{Name: "region-a", Seeds: []string{"localhost:8080"}}},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     config.NewDynamic("random"),
			AffinityMode: config.NewDynamic("soft"),
			Replicas:     config.NewDynamic(3),
		},
	}
	replicas := 3
	cfg.SelectStrategy.Replicas.WithSource(func() (int, bool) { return replicas, true })

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClientPool.EXPECT().GetLogStoreClient(ctx, "localhost:8080").Return(mockClient, nil)

	five := []*proto.NodeMeta{
		{Endpoint: "n1:8080"}, {Endpoint: "n2:8080"}, {Endpoint: "n3:8080"},
		{Endpoint: "n4:8080"}, {Endpoint: "n5:8080"},
	}
	mockClient.EXPECT().SelectNodes(ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return(five, nil).Maybe()

	d, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	require.NoError(t, err)

	replicas = 5 // flip at runtime, no restart
	result, err := d.SelectQuorum(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(5), result.Es)
	assert.Equal(t, int32(5), result.Wq)
	assert.Equal(t, int32(3), result.Aq) // (5/2)+1
}
```

- [ ] **Step 3: Write the relaxed-runtime-affinity test**

Append to `woodpecker/quorum/discovery_test.go`:

```go
func TestQuorumDiscovery_DynamicAffinity_UnknownDegradesToSoft(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{{Name: "region-a", Seeds: []string{"localhost:8080"}}},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     config.NewDynamic("random"),
			AffinityMode: config.NewDynamic("soft"), // valid static -> ctor passes
			Replicas:     config.NewDynamic(3),
		},
	}
	// runtime override returns a typo; must degrade to SOFT, not error
	cfg.SelectStrategy.AffinityMode.WithSource(func() (string, bool) { return "nonsense", true })

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClientPool.EXPECT().GetLogStoreClient(ctx, "localhost:8080").Return(mockClient, nil)
	mockClient.EXPECT().SelectNodes(ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "n1:8080"}, {Endpoint: "n2:8080"}, {Endpoint: "n3:8080"},
	}, nil)

	d, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	require.NoError(t, err)
	result, err := d.SelectQuorum(ctx)
	require.NoError(t, err)
	assert.Len(t, result.Nodes, 3)
}
```

- [ ] **Step 4: Run the new tests**

Run: `go test ./common/config/ -run TestQuorumConfig_DynamicReplicas -v && go test ./woodpecker/quorum/ -run 'TestQuorumDiscovery_Dynamic' -v`
Expected: PASS.

- [ ] **Step 5: Full unit test sweep**

Run: `go test ./common/config/... ./woodpecker/quorum/... -count=1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add common/config/configuration_test.go woodpecker/quorum/discovery_test.go
git commit -m "test(quorum): cover dynamic replicas/affinity overrides (#195)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Docs

**Files:**
- Modify: `config/woodpecker.yaml`, `docs/superpowers/specs/2026-06-23-quorum-dynamic-config-design.md`

- [ ] **Step 1: Document the dynamic capability in the sample config**

In `config/woodpecker.yaml`, under the `quorumSelectStrategy:` block (around line 53), add a comment noting these three knobs can be sourced dynamically by the embedding application (no format change):

```yaml
      # affinityMode, replicas, and strategy below can be overridden at runtime
      # by the embedding application (via the client's Dynamic[T] source hooks);
      # such changes take effect on the next segment and require no restart.
      quorumSelectStrategy:
```

- [ ] **Step 2: Mark the design doc implemented**

In `docs/superpowers/specs/2026-06-23-quorum-dynamic-config-design.md`, change the status line to `Status: Implemented (#195)`.

- [ ] **Step 3: Commit**

```bash
git add config/woodpecker.yaml docs/superpowers/specs/2026-06-23-quorum-dynamic-config-design.md
git commit -m "docs(quorum): note runtime-dynamic quorum knobs (#195)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification

- [ ] **Build + vet:** `go build ./... && go vet ./...` → clean.
- [ ] **Unit tests:** `go test ./common/config/... ./woodpecker/quorum/... ./woodpecker/... ./cmd/... -count=1` → PASS.
- [ ] **Compile integration tests:** `go test ./tests/integration/... -run xxxNoSuchTest -count=1` → builds (0 tests run is fine; this only proves compilation).
- [ ] **Lint (if available):** `golangci-lint run common/config/... woodpecker/quorum/... cmd/external/...`.

## Self-review notes (coverage vs spec)

- Spec §4 primitive → Task 1.
- Spec §5 config field/getter/defaults/validation changes → Task 3 Steps 1-4.
- Spec §5.1 call-site migration (user_config + tests) → Task 3 Steps 5, 7-10.
- Spec §6 discovery refactor (pure helpers, recompute, strict-ctor/tolerant-runtime, thread filters) → Task 2 + Task 3 Step 6.
- Spec §7 semantics/clamping/back-compat → verified by Task 4 Step 1 (clamp) + Task 2 baseline (back-compat).
- Spec §8 concurrency contract → documented; sources bound before use in tests.
- Spec §11 testing → Tasks 1, 4.
