# Dynamic quorum client config via a `Dynamic[T]` field wrapper

- Date: 2026-06-23
- Status: Proposed (design approved in brainstorming; pending spec review)
- Scope: client-side **quorum** config only

## 1. Context & problem

Quorum selection is client-side policy (replica count, selection strategy,
affinity mode). Operationally it should be switchable without a restart, but
today it cannot be:

- Config is loaded once at startup from local YAML (`config.NewConfiguration`,
  `cmd/main.go:105`) and passed around purely by pointer. There is no file
  watch, SIGHUP handler, or etcd watch on config — nothing re-reads it.
- `quorumDiscovery` freezes derived state at construction: `es/wq/aq`, the
  parsed `affinityMode`/`strategyType`, and the `filters` array
  (`woodpecker/quorum/discovery.go:44-68`). Even though `SelectQuorum` reads
  `Strategy`/`BufferPools`/`CustomPlacement` fresh per call, the replica count
  and filters are stuck at their startup values.

One thing already works in our favor: `SelectQuorum` runs **once per new
segment** (`woodpecker/log/log_handle.go:599`), and the selected nodes are
frozen into that segment's `QuorumInfo` metadata. So a clean, safe semantic is
available for free: *a config change applies to the next segment and never
disturbs an in-flight one.*

## 2. Goals / non-goals

Goals:
- Let the embedding application make quorum `Replicas`, `Strategy`, and
  `AffinityMode` change at runtime without a Woodpecker restart.
- Keep the "where does the value come from" decision in the application (its
  own Paramtable / etcd watch / atomic / feature flag / test stub). Woodpecker
  only provides the seam.
- Zero behavior change when no dynamic source is bound.

Non-goals (this iteration):
- Server / logstore config dynamism — explicitly out of scope.
- Making `BufferPools` / `CustomPlacement` (topology) dynamic. They stay plain
  values; can be wrapped later with the same primitive if needed.
- Woodpecker owning any watcher (fsnotify / etcd / SIGHUP). The application
  owns the source.

## 3. Design overview

Introduce one small generic wrapper, `Dynamic[T]`, and apply it **selectively
to leaf scalar fields only** — the specific quorum knobs we want tunable. A
`Dynamic[T]` field:

- parses from YAML exactly like a bare `T` (so the config file is unchanged),
- holds an optional dynamic `source func() (T, bool)` bound by the application,
- is read with `.Get()`: the source wins when bound and it has an opinion,
  otherwise the static (YAML/defaults) value is returned.

Then `quorumDiscovery` stops caching derived state at construction and computes
it per `SelectQuorum` call from `.Get()` reads. Because `SelectQuorum` only runs
at segment roll, this recompute cost is negligible.

## 4. The primitive (`common/config/dynamic.go`)

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

// NewDynamic builds a Dynamic with a static default (used in the defaults builder).
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

// --- YAML integration: behave exactly like a bare T on disk ---

func (d *Dynamic[T]) UnmarshalYAML(node *yaml.Node) error { return node.Decode(&d.value) }
func (d Dynamic[T]) MarshalYAML() (interface{}, error)    { return d.value, nil }
```

`node.Decode(&d.value)` composes with the existing custom scalar types, so a
hypothetical `Dynamic[DurationSeconds]` or `Dynamic[ByteSize]` would also work —
but this iteration only uses `Dynamic[string]` and `Dynamic[int]`.

## 5. `configuration.go` changes (quorum only)

Only three leaf scalars change type; the composite fields stay plain.

```go
type QuorumSelectStrategy struct {
	AffinityMode    Dynamic[string]   `yaml:"affinityMode"`    // wrapped
	Replicas        Dynamic[int]      `yaml:"replicas"`        // wrapped
	Strategy        Dynamic[string]   `yaml:"strategy"`        // wrapped
	CustomPlacement []CustomPlacement `yaml:"customPlacement"` // unchanged (plain)
}

type QuorumConfig struct {
	BufferPools    []QuorumBufferPool   `yaml:"quorumBufferPools"` // unchanged (plain)
	SelectStrategy QuorumSelectStrategy `yaml:"quorumSelectStrategy"`
}
```

Derived getters read through `.Get()` and keep the existing runtime clamping
(so a bad dynamic value can never break selection):

```go
func (q *QuorumConfig) GetEnsembleSize() int {
	switch q.SelectStrategy.Replicas.Get() { // was q.SelectStrategy.Replicas
	case 3, 5:
		return q.SelectStrategy.Replicas.Get()
	default:
		return 3
	}
}
func (q *QuorumConfig) GetWriteQuorumSize() int { return q.GetEnsembleSize() }
func (q *QuorumConfig) GetAckQuorumSize() int   { return (q.GetWriteQuorumSize() / 2) + 1 }
```

Defaults wrap the three scalars with `NewDynamic`; the rest is unchanged:

```go
SelectStrategy: QuorumSelectStrategy{
	AffinityMode:    NewDynamic("soft"),
	Replicas:        NewDynamic(3),
	Strategy:        NewDynamic("random"),
	CustomPlacement: []CustomPlacement{},
},
```

Validation (`validateQuorumConfig`) switches its reads of the three wrapped
fields to `.Get()` (e.g. `q.SelectStrategy.Strategy.Get()`,
`q.SelectStrategy.AffinityMode.Get()`); `BufferPools` / `CustomPlacement` reads
are unchanged. Note: `NewConfiguration` runs `Validate()` before the application
has a chance to bind sources, so validation always checks the **static
baseline** — dynamic overrides intentionally bypass startup validation and rely
on the runtime clamping described in §7.

## 6. Consumer changes (`woodpecker/quorum/discovery.go`)

Drop the construction-time cache; resolve a fresh, self-consistent snapshot at
the top of each `SelectQuorum`.

Before — cached, never refreshed:

```go
type quorumDiscovery struct {
	cfg          *config.QuorumConfig
	clientPool   client.LogStoreClientPool
	es, wq, aq   int32
	affinityMode proto.AffinityMode
	strategyType proto.StrategyType
	filters      []*proto.NodeFilter
}
```

After — only stable dependencies remain on the struct:

```go
type quorumDiscovery struct {
	cfg        *config.QuorumConfig
	clientPool client.LogStoreClientPool
}

func (d *quorumDiscovery) SelectQuorum(ctx context.Context) (*proto.QuorumInfo, error) {
	es := int32(d.cfg.GetEnsembleSize())
	wq := int32(d.cfg.GetWriteQuorumSize())
	aq := int32(d.cfg.GetAckQuorumSize())
	strategy := d.cfg.SelectStrategy.Strategy.Get()
	affinity := parseAffinity(d.cfg.SelectStrategy.AffinityMode.Get())
	stype := parseStrategy(strategy)
	pools := d.cfg.BufferPools                       // plain, unchanged
	custom := d.cfg.SelectStrategy.CustomPlacement   // plain, unchanged
	filters, err := buildFilters(strategy, es, custom)
	if err != nil {
		return nil, err
	}
	// ...existing selection logic, using these locals instead of d.es / d.filters...
}
```

`parseAffinityMode` / `parseStrategyType` / `buildFilters` become pure helpers
(`parseAffinity` / `parseStrategy` / `buildFilters`) taking their inputs as
arguments instead of reading `d` and mutating it. The threaded `es/wq/aq` and
`affinityMode`/`strategyType` values are passed down to
`newQuorumInfo`/`requestNodesFromSeed` rather than read from struct fields.

Behavior parity for the parsers:
- `parseStrategy`: unknown / empty → `RANDOM` (same as today).
- `parseAffinity`: **relaxed** — unknown → `SOFT` with a warning, instead of
  erroring. Rationale: it is now evaluated per call at runtime, so a typo in a
  dynamic override must not turn into a hard failure of segment creation. The
  static baseline is still strictly validated at startup (§5).

## 7. Semantics, safety, backward compatibility

- **Segment-boundary application.** The selected nodes are frozen into the
  segment's `QuorumInfo` (`woodpecker/segment/segment_handle.go`). A change to
  `Replicas`/`Strategy`/`AffinityMode` therefore takes effect on the **next
  segment** and never alters an in-flight one.
- **Runtime clamping.** `GetEnsembleSize` coerces anything but 3/5 → 3; unknown
  strategy → random; unknown affinity → soft. So a malformed dynamic value
  degrades safely rather than breaking writes.
- **Backward compatible.** With no source bound, `Get()` returns the static
  value and the YAML/file format is unchanged. Existing deployments behave
  identically.

## 8. Concurrency contract

`WithSource` is called at startup, before the client is used. After that, the
`source` func may be invoked concurrently (selection can run from multiple
goroutines). Contract: **the application's source func must be safe for
concurrent calls** (an `atomic.*` read or Milvus Paramtable lookup already is).
Woodpecker adds no locking on the read path. Runtime mutation of values happens
inside the source func (e.g. the func reads an `atomic.Int64`), not by
re-binding.

## 9. Edge cases / caveats

- **Dynamic strategy vs static topology.** `Strategy` is dynamic but
  `CustomPlacement` / `BufferPools` are static. Switching the dynamic strategy
  to `custom` or `cross-region` only works if the static topology already
  satisfies that strategy (`buildFilters` validates this and will error
  otherwise; `SelectQuorum`'s `retry.AttemptAlways` would then retry). Guidance:
  only switch to topology-dependent strategies when the matching static
  topology is present. If this becomes a real need, wrap the topology fields in
  `Dynamic[T]` too (same primitive).
- **Replicas change mid-life.** Existing segments keep their captured ensemble;
  only new segments use the new replica count. This is the intended, safe
  behavior, not a bug.

## 10. Usage example (embedding application)

```go
cfg, _ := config.NewConfiguration("woodpecker.yaml")

var replicas atomic.Int64
replicas.Store(3)

// Make replicas dynamic; leave strategy/affinity static (or bind them too).
cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas.
	WithSource(func() (int, bool) { return int(replicas.Load()), true })

cfg.Woodpecker.Client.Quorum.SelectStrategy.Strategy.
	WithSource(func() (string, bool) {
		if s := pt.Get("woodpecker.quorum.strategy"); s != "" {
			return s, true   // dynamic wins
		}
		return "", false     // no opinion -> fall back to YAML
	})

client, _ := woodpecker.NewClient(ctx, cfg, etcdCli, true)

// later, no restart:
replicas.Store(5) // the next segment rolls as a 5-node ensemble
```

## 11. Testing

- `Dynamic[T]` unit tests: YAML round-trip (unmarshal/marshal) equals a bare
  `T`; `Get()` returns static when unbound; source wins when bound; source
  returning `(_, false)` falls back to static.
- Config tests: defaults build via `NewDynamic`; `Validate()` passes on the
  static baseline; `GetEnsembleSize` clamps a dynamic `4 → 3`.
- Discovery tests: with a bound `Replicas` source flipping `3 → 5`, a second
  `SelectQuorum` produces a 5-node ensemble while the first call's result is
  unchanged; unknown dynamic affinity degrades to SOFT with a warning rather
  than erroring; unknown dynamic strategy degrades to random.
- Regression: with no sources bound, discovery behavior is identical to today.

## 12. Out of scope / future extension

The `Dynamic[T]` primitive is reusable. Later, other client knobs
(`segmentAppend.maxRetries`, `segmentRollingPolicy.*`, `directRead.*`) can each
be made dynamic by flipping their field type to `Dynamic[T]` and updating their
read sites — one field at a time, no new mechanism. Server/logstore config
remains static until a separate decision.
