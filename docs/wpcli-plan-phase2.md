# wp CLI Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship Phase 2 of the wp CLI — the observability & op registry layer. Adds the `common/runtime/opregistry/` package, `metrics.StartOp/End` abstraction, gRPC interceptor, ~12 business code metric migrations, 17 new CLI commands (C/D/E/G families), 10 new admin endpoints, dynamic log level control, and docker-compose E2E tests.

**Architecture:** Builds on Phase 1's cobra framework, `resolveAndDiscover()` pattern, fan-out client, output renderers, and error/exit-code system. New server-side packages (`opregistry`, `op.go`) are wired through `AdminCallbacks` extension — same pattern as Phase 1. D family commands parse Prometheus `/metrics` endpoint directly (no new server endpoints needed). The op registry is a bounded pool with drop-oldest eviction; eviction of old ops is the stall detection signal.

**Tech Stack (additions to Phase 1):**
- `github.com/jonboulle/clockwork v0.2.2` (new dep — time mocking for opregistry tests)
- `github.com/prometheus/common/expfmt` (already in go.mod via client_golang — Prometheus text format parser)
- `gopkg.in/yaml.v3 v3.0.1` (already in go.mod — scenario YAML parsing)

**Reference documents:**
- Design spec: [`docs/wpcli-design.md`](./wpcli-design.md) — §2.C, §2.D, §2.E.3-E.4, §2.G, §5.4, §6.3
- Phase 1 plan: [`docs/wpcli-plan-phase1.md`](./wpcli-plan-phase1.md) — for pattern reference
- Existing pattern for admin handlers: `common/http/management/node_handler.go`
- Existing pattern for CLI commands: `cmd/wpcli/cmd/node_list.go`

**Phase 2 scope locked (from spec §5.4):**
- 17 feature commands: C family ×7, D family ×5, E family ×2, G family ×3
- 10 new endpoints: C family ×5, E family ×2 (stub replacement), G family ×3
- `common/runtime/opregistry/` new package (~500 lines)
- `common/metrics/op.go` + `op_observer.go` (~200 lines)
- `cmd/wpcli/internal/scenarios/` scenario YAML engine + 12 built-in scenarios
- `common/http/management/logstore_handler.go`, `ops_handler.go`, `logging_handler.go` (3 new handler files)
- ~12 business code metric migration sites (logstore + storage backends)
- `common/logger/logger.go` refactor to `zap.AtomicLevel` for dynamic level control
- gRPC `UnaryServerInterceptor` for automatic op registration
- `tests/docker/wpcli/` docker-compose E2E suite
- Writer `Snapshot()` / `SnapshotDetailed()` interface extension + `WriterRegistry`

**Prerequisite:** All Phase 1 tasks committed, tests pass, `bin/wp` verified.

**Out of scope for Phase 2 (explicit):**
- F family (k8s hybrid commands) → Phase 3
- Cross-platform release build → Phase 3
- Docker image embedding → Phase 3
- Cookbook / configuration reference docs → Phase 3

---

## Phase 2 Task Inventory

45 tasks, grouped by dependency layer. Each task is self-contained and ends with a commit.

| # | Task | Layer | Depends on |
|---|---|---|---|
| 1 | Add clockwork dependency | Foundation | — |
| 2 | OpRegistry types | Op Infrastructure | 1 |
| 3 | OpRegistry bounded pool + drop-oldest | Op Infrastructure | 2 |
| 4 | OpRegistry eviction + Prometheus self-metrics | Op Infrastructure | 3 |
| 5 | OpRegistry unit tests (clockwork time-mocking) | Op Infrastructure | 4 |
| 6 | OpObserver interface | Op Abstraction | 2 |
| 7 | Op bridge API: `StartOp` / `End` | Op Abstraction | 6 |
| 8 | OpRegistry as OpObserver | Op Abstraction | 5, 7 |
| 9 | OpRegistry config fields | Server Config | — |
| 10 | gRPC `UnaryServerInterceptor` | Server Config | 7 |
| 11 | OpRegistry instantiation + wiring | Server Wiring | 8, 9, 10 |
| 12 | Logger `zap.AtomicLevel` + `GetLevel` / `SetLevel` | Logger | — |
| 13 | Logging handler: `GET` + `POST /log/level` | Server Handler | 12 |
| 14 | `Writer.Snapshot()` / `SnapshotDetailed()` interface | Writer | — |
| 15 | Snapshot impl: objectstorage backend | Writer | 14 |
| 16 | Snapshot impl: stagedstorage + disk backends | Writer | 14 |
| 17 | `WriterRegistry` interface + logstore integration | Writer | 15, 16 |
| 18 | Router path constants for C/G families | Server Handler | — |
| 19 | `AdminCallbacks` extension: Logstore, Ops, LogLevel sub-structs | Server Handler | 18 |
| 20 | Logstore handler: `GET /admin/logstore/segments` + `segments/{log}/{seg}` | Server Handler | 17, 19 |
| 21 | Logstore handler: `POST /admin/logstore/flush` + `/fence` + `/compact` | Server Handler | 17, 19 |
| 22 | Ops handler: `GET /admin/runtime/ops` + `/{op_id}` + `/stats` | Server Handler | 11, 19 |
| 23 | Logstore + Ops admin callbacks wiring in `cmd/main.go` | Server Wiring | 20, 21, 22 |
| 24 | Migrate logstore.go × 9 methods to op pattern | Metric Migration | 7 |
| 25 | Migrate storage flush sites (objectstorage + stagedstorage + disk) | Metric Migration | 7 |
| 26 | Metric migration regression tests | Metric Migration | 24, 25 |
| 27 | `wp logging` parent + `get-level` | CLI — E family | 13 |
| 28 | `wp logging set-level` | CLI — E family | 27 |
| 29 | `wp logstore` parent command | CLI — C family | — |
| 30 | `wp logstore segments` | CLI — C family | 20, 29 |
| 31 | `wp logstore segment-show` | CLI — C family | 20, 29 |
| 32 | `wp logstore buffer` | CLI — C family | 20, 29 |
| 33 | `wp logstore flush-queue` | CLI — C family | 20, 29 |
| 34 | `wp logstore force-flush` | CLI — C family | 21, 29 |
| 35 | `wp logstore fence` + `wp logstore compact` | CLI — C family | 21, 29 |
| 36 | `wp ops` parent + `ops list` | CLI — G family | 22 |
| 37 | `wp ops show` | CLI — G family | 22, 36 |
| 38 | `wp ops stats` | CLI — G family | 22, 36 |
| 39 | Prometheus scrape parser | CLI — D infra | — |
| 40 | `wp metrics` parent + `metrics list` | CLI — D family | 39 |
| 41 | `wp metrics snapshot` | CLI — D family | 39 |
| 42 | `wp metrics top` | CLI — D family | 39 |
| 43 | `wp metrics watch` | CLI — D family | 39 |
| 44 | Scenario YAML engine + `wp metrics report` + 12 built-in scenarios | CLI — D family | 39, 43 |
| 45 | Phase 2 verification sweep | Verification | all |

**Estimated commits:** ~45 (one per task).

---

## Part A — Op Registry Core (Tasks 1-5)

### Task 1: Add clockwork dependency

**Goal:** Add `github.com/jonboulle/clockwork` for deterministic time control in opregistry tests.

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Add dependency**

```bash
go get github.com/jonboulle/clockwork@v0.2.2
go mod tidy
```

- [ ] **Step 2: Verify**

```bash
go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add clockwork v0.2.2 for opregistry time mocking"
```

---

### Task 2: OpRegistry types

**Goal:** Define the immutable data types used throughout the op registry: `OpType`, `OpRecord`, `Filter`, `Stats`.

**Files:**
- Create: `common/runtime/opregistry/types.go`
- Create: `common/runtime/opregistry/types_test.go`

**Spec reference:** §2.G.3 — OpRecord fields, OpType enum, Filter/Stats structures.

- [ ] **Step 1: Write tests**

Create `common/runtime/opregistry/types_test.go`:

```go
package opregistry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpType_String(t *testing.T) {
	assert.Equal(t, "logstore.add_entry", string(OpTypeLogstoreAddEntry))
	assert.Equal(t, "file.flush", string(OpTypeFileFlush))
	assert.Equal(t, "file.compact", string(OpTypeFileCompact))
}

func TestOpRecord_Elapsed(t *testing.T) {
	r := OpRecord{
		StartedAt: time.Now().Add(-5 * time.Second),
	}
	elapsed := r.Elapsed()
	assert.InDelta(t, 5.0, elapsed.Seconds(), 0.5)
}

func TestFilter_Matches(t *testing.T) {
	r := OpRecord{
		OpType:    OpTypeFileFlush,
		LogID:     42,
		SegmentID: 7,
		StartedAt: time.Now().Add(-2 * time.Minute),
	}
	// Empty filter matches everything
	require.True(t, Filter{}.Matches(r, time.Now()))

	// Type filter
	require.True(t, Filter{Types: []OpType{OpTypeFileFlush}}.Matches(r, time.Now()))
	require.False(t, Filter{Types: []OpType{OpTypeFileCompact}}.Matches(r, time.Now()))

	// LogID filter
	require.True(t, Filter{LogID: ptr(int64(42))}.Matches(r, time.Now()))
	require.False(t, Filter{LogID: ptr(int64(99))}.Matches(r, time.Now()))

	// LongerThan filter
	require.True(t, Filter{LongerThan: 30 * time.Second}.Matches(r, time.Now()))
	require.False(t, Filter{LongerThan: 5 * time.Minute}.Matches(r, time.Now()))
}

func ptr[T any](v T) *T { return &v }
```

- [ ] **Step 2: Implement types**

Create `common/runtime/opregistry/types.go`:

```go
package opregistry

import "time"

// OpType identifies the category of an in-flight operation.
type OpType string

const (
	OpTypeLogstoreAddEntry       OpType = "logstore.add_entry"
	OpTypeLogstoreGetBatchEntries OpType = "logstore.get_batch_entries"
	OpTypeLogstoreComplete       OpType = "logstore.complete"
	OpTypeLogstoreFence          OpType = "logstore.fence"
	OpTypeLogstoreGetSegmentLac  OpType = "logstore.get_segment_lac"
	OpTypeLogstoreGetBlockCount  OpType = "logstore.get_segment_block_count"
	OpTypeLogstoreCompact        OpType = "logstore.compact_segment"
	OpTypeLogstoreClean          OpType = "logstore.clean_segment"
	OpTypeLogstoreUpdateLac      OpType = "logstore.update_lac"
	OpTypeFileFlush              OpType = "file.flush"
	OpTypeFileSync               OpType = "file.sync"
	OpTypeFileCompact            OpType = "file.compact"
	OpTypeFileFinalize           OpType = "file.finalize"
	OpTypeFileFence              OpType = "file.fence"
	OpTypeFileRecover            OpType = "file.recover"
	OpTypeGRPC                   OpType = "grpc"
)

// OpRecord is the immutable, 7-field record stored in the registry.
// Fields are set at registration time and never mutated.
type OpRecord struct {
	OpID      string    // e.g. "fl-7d2f4a9-001"
	OpType    OpType    // e.g. OpTypeFileFlush
	TraceID   string    // OTel hex; empty = no span
	SpanID    string    // OTel hex
	StartedAt time.Time
	LogID     int64     // 0 = N/A
	SegmentID int64     // 0 = N/A
}

// Elapsed returns duration since the op started.
func (r OpRecord) Elapsed() time.Duration {
	return time.Since(r.StartedAt)
}

// ElapsedAt returns duration since the op started, relative to the given time.
func (r OpRecord) ElapsedAt(now time.Time) time.Duration {
	return now.Sub(r.StartedAt)
}

// Filter specifies criteria for listing ops.
type Filter struct {
	Types      []OpType       // match any of these types (empty = all)
	LogID      *int64         // match exact log ID (nil = any)
	SegmentID  *int64         // match exact segment ID (nil = any)
	LongerThan time.Duration  // only ops running longer than this (0 = no filter)
	Limit      int            // max results (0 = default 100)
	SortBy     string         // "elapsed" (default) or "type"
}

// Matches returns true if the record passes all filter criteria.
func (f Filter) Matches(r OpRecord, now time.Time) bool {
	if len(f.Types) > 0 {
		found := false
		for _, t := range f.Types {
			if r.OpType == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if f.LogID != nil && r.LogID != *f.LogID {
		return false
	}
	if f.SegmentID != nil && r.SegmentID != *f.SegmentID {
		return false
	}
	if f.LongerThan > 0 && now.Sub(r.StartedAt) < f.LongerThan {
		return false
	}
	return true
}

// Stats holds registry utilization and eviction statistics.
type Stats struct {
	Capacity     int           `json:"capacity"`
	InUse        int           `json:"in_use"`
	WarnAge      time.Duration `json:"warn_age"`
	EvictedTotal int64         `json:"evicted_total"`
	EvictedYoung int64         `json:"evicted_young"` // age < WarnAge
	EvictedOld   int64         `json:"evicted_old"`   // age >= WarnAge — STALL SIGNAL
}
```

- [ ] **Step 3: Verify tests pass**

```bash
go test ./common/runtime/opregistry/... -count=1 -v
```

- [ ] **Step 4: Commit**

```bash
git add common/runtime/opregistry/types.go common/runtime/opregistry/types_test.go
git commit -m "feat(opregistry): add OpType, OpRecord, Filter, Stats types"
```

---

### Task 3: OpRegistry bounded pool + drop-oldest

**Goal:** Implement the core registry: a fixed-capacity pool where `Register()` inserts and `Deregister()` removes; when full, the oldest entry is evicted (drop-oldest FIFO).

**Files:**
- Create: `common/runtime/opregistry/registry.go`

**Spec reference:** §2.G.3 — bounded pool, drop-oldest eviction.

- [ ] **Step 1: Implement Registry**

Create `common/runtime/opregistry/registry.go`:

Core API:
```go
package opregistry

import (
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

// Registry is a bounded, thread-safe pool of in-flight OpRecords.
// When capacity is reached, the oldest entry is evicted (drop-oldest FIFO).
type Registry struct {
	mu       sync.RWMutex
	capacity int
	warnAge  time.Duration
	clock    clockwork.Clock

	// pool is the ordered list of active ops (oldest first)
	pool []*entry
	// index maps OpID → entry for O(1) lookup/deregister
	index map[string]*entry

	// Eviction counters
	evictedYoung int64
	evictedOld   int64

	// onEvict is called when an entry is evicted (for Prometheus metrics)
	onEvict func(age time.Duration, isOld bool)
}

type entry struct {
	record OpRecord
	handle uint64
}

// New creates a Registry with the given capacity and warn age threshold.
func New(capacity int, warnAge time.Duration) *Registry

// NewWithClock creates a Registry with a custom clock (for testing).
func NewWithClock(capacity int, warnAge time.Duration, clock clockwork.Clock) *Registry

// Register adds an OpRecord to the pool. Returns a handle for Deregister.
// If the pool is full, the oldest entry is evicted first.
func (r *Registry) Register(rec OpRecord) uint64

// Deregister removes an op by handle. Returns the elapsed duration.
func (r *Registry) Deregister(handle uint64) time.Duration

// List returns all ops matching the filter, sorted by elapsed desc.
func (r *Registry) List(f Filter) []OpRecord

// Get returns a single op by OpID, or nil if not found.
func (r *Registry) Get(opID string) *OpRecord

// Stats returns current registry utilization and eviction counters.
func (r *Registry) Stats() Stats

// SetOnEvict sets the eviction callback (called under lock — must be fast).
func (r *Registry) SetOnEvict(fn func(age time.Duration, isOld bool))
```

Key invariants:
- `pool` maintains insertion order (oldest at index 0)
- `index` maps `OpID` → `*entry` for O(1) deregister
- `Register()` evicts `pool[0]` when `len(pool) >= capacity`
- `Deregister()` removes from both `pool` and `index`
- All methods are goroutine-safe (protected by `mu`)

- [ ] **Step 2: Verify it compiles**

```bash
go build ./common/runtime/opregistry/...
```

- [ ] **Step 3: Commit**

```bash
git add common/runtime/opregistry/registry.go
git commit -m "feat(opregistry): bounded pool with drop-oldest eviction"
```

---

### Task 4: OpRegistry eviction signaling + Prometheus self-metrics

**Goal:** Add Prometheus self-metrics for the op registry: eviction histogram, eviction counter with young/old signal labels. Wire them into the eviction callback.

**Files:**
- Create: `common/runtime/opregistry/metrics.go`
- Modify: `common/runtime/opregistry/registry.go` — wire `SetOnEvict` in constructor
- Modify: `common/metrics/service_metrics.go` — register new metrics

**Spec reference:** §2.G.3 — eviction-as-signal, Prometheus self-metrics.

- [ ] **Step 1: Define Prometheus metrics**

Create `common/runtime/opregistry/metrics.go`:

```go
package opregistry

import "github.com/prometheus/client_golang/prometheus"

var (
	OpRegistryEvictedAgeSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "woodpecker",
		Subsystem: "server",
		Name:      "op_registry_evicted_age_seconds",
		Help:      "Age in seconds of evicted ops — distribution reveals stall vs throughput",
		Buckets:   []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300},
	})

	OpRegistryEvictedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "woodpecker",
		Subsystem: "server",
		Name:      "op_registry_evicted_total",
		Help:      "Total evictions by signal type: young (normal throughput) or old (stall indicator)",
	}, []string{"signal"})

	OpRegistryPoolUsage = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "woodpecker",
		Subsystem: "server",
		Name:      "op_registry_pool_usage",
		Help:      "Current number of in-flight ops in the registry",
	})
)

func RegisterMetrics(registerer prometheus.Registerer) {
	registerer.MustRegister(OpRegistryEvictedAgeSeconds)
	registerer.MustRegister(OpRegistryEvictedTotal)
	registerer.MustRegister(OpRegistryPoolUsage)
}
```

- [ ] **Step 2: Wire eviction callback in `New()`**

In `registry.go`, the `New()` constructor should call `r.SetOnEvict(defaultEvictCallback)` where `defaultEvictCallback` observes the Prometheus metrics:

```go
func defaultEvictCallback(age time.Duration, isOld bool) {
	OpRegistryEvictedAgeSeconds.Observe(age.Seconds())
	if isOld {
		OpRegistryEvictedTotal.WithLabelValues("old").Inc()
	} else {
		OpRegistryEvictedTotal.WithLabelValues("young").Inc()
	}
}
```

Also update `Register()` and `Deregister()` to update `OpRegistryPoolUsage.Set(float64(len(r.pool)))`.

- [ ] **Step 3: Register in `service_metrics.go`**

Add `opregistry.RegisterMetrics(registerer)` call in `RegisterServerMetricsWithRegisterer()`.

- [ ] **Step 4: Commit**

```bash
git add common/runtime/opregistry/metrics.go common/runtime/opregistry/registry.go common/metrics/service_metrics.go
git commit -m "feat(opregistry): eviction signaling with Prometheus young/old counters"
```

---

### Task 5: OpRegistry unit tests

**Goal:** Comprehensive unit tests for the registry using `clockwork.FakeClock` for deterministic time control. Cover: basic register/deregister, capacity eviction, young vs old eviction, filter matching, stats accuracy, concurrent access.

**Files:**
- Create: `common/runtime/opregistry/registry_test.go`

- [ ] **Step 1: Write tests**

Test cases (minimum):

```go
func TestRegistry_RegisterDeregister(t *testing.T)
// Register 3 ops, verify List returns 3, Deregister one, verify List returns 2.

func TestRegistry_CapacityEviction(t *testing.T)
// Create registry with capacity=3. Register 4 ops. Verify oldest evicted.
// Use FakeClock to control StartedAt so ages are deterministic.

func TestRegistry_EvictionYoungSignal(t *testing.T)
// Register ops that are < warnAge. Fill pool, force eviction.
// Verify evictedYoung incremented, evictedOld stays 0.

func TestRegistry_EvictionOldSignal(t *testing.T)
// Register op, advance FakeClock past warnAge, fill pool to force eviction.
// Verify evictedOld incremented — this is the stall signal.

func TestRegistry_Get(t *testing.T)
// Register op with known OpID, verify Get returns it.
// Verify Get with unknown ID returns nil.

func TestRegistry_ListFilter(t *testing.T)
// Register mixed op types. Filter by type, logID, longerThan.
// Verify only matching ops returned, sorted by elapsed desc.

func TestRegistry_Stats(t *testing.T)
// Register some ops, evict some, verify Stats() returns correct counts.

func TestRegistry_ConcurrentAccess(t *testing.T)
// Run 100 goroutines doing Register+Deregister+List concurrently with -race.
```

- [ ] **Step 2: Verify all pass**

```bash
go test ./common/runtime/opregistry/... -race -count=1 -v
```

- [ ] **Step 3: Commit**

```bash
git add common/runtime/opregistry/registry_test.go
git commit -m "test(opregistry): comprehensive unit tests with clockwork time mocking"
```

---

## Part B — Op Abstraction + Observer Chain (Tasks 6-8)

### Task 6: OpObserver interface

**Goal:** Define the `OpObserver` interface that decouples business code metrics from the op registry. Business code calls `metrics.StartOp()` which notifies all registered observers. The opregistry is one observer; future observers (tracing, logging) can be added without changing call sites.

**Files:**
- Create: `common/metrics/op_observer.go`

**Spec reference:** §2.G.3 — OpObserver interface, RegisterOpObserver.

- [ ] **Step 1: Create interface**

Create `common/metrics/op_observer.go`:

```go
package metrics

import "time"

// OpObserver receives notifications when ops start and end.
// Implementations must be goroutine-safe and fast (called in hot path).
type OpObserver interface {
	// OnOpStart is called when a new op begins. Returns a handle used in OnOpEnd.
	OnOpStart(op *Op) uint64
	// OnOpEnd is called when the op completes.
	OnOpEnd(op *Op, handle uint64, elapsed time.Duration, status string)
}

// observers is the global chain of registered observers.
var observers []OpObserver

// RegisterOpObserver adds an observer to the global chain.
// Must be called during init, before any ops are created.
func RegisterOpObserver(o OpObserver) {
	observers = append(observers, o)
}
```

- [ ] **Step 2: Commit**

```bash
git add common/metrics/op_observer.go
git commit -m "feat(metrics): add OpObserver interface for op lifecycle notifications"
```

---

### Task 7: Op bridge API — `StartOp` / `End`

**Goal:** Implement the `metrics.Op` struct and `StartOp()` function that business code will use. The `Op` wraps the existing histogram observation pattern (`start := time.Now()` → `Observe(elapsed)`) and additionally notifies all registered `OpObserver`s.

**Files:**
- Create: `common/metrics/op.go`
- Create: `common/metrics/op_test.go`

**Spec reference:** §2.G.3 — `StartOp` / `End` API, migration pattern.

- [ ] **Step 1: Write tests**

Create `common/metrics/op_test.go`:

```go
func TestOp_EndObservesHistogram(t *testing.T)
// Create a test histogram, StartOp with it, call End.
// Verify the histogram was observed with the elapsed duration.

func TestOp_EndNotifiesObservers(t *testing.T)
// Register a mock observer, StartOp, End.
// Verify OnOpStart and OnOpEnd were called with correct args.

func TestOp_EndCalledOnce(t *testing.T)
// Call End twice. Second call should be no-op (no double observation).

func TestOp_WithNilHistogram(t *testing.T)
// StartOp with nil histogram (e.g. gRPC interceptor ops with no pre-existing metric).
// Verify End doesn't panic.
```

- [ ] **Step 2: Implement Op**

Create `common/metrics/op.go`:

```go
package metrics

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Op represents an in-flight operation being tracked.
type Op struct {
	OpType    string
	Labels    prometheus.Labels
	histo     prometheus.Observer // may be nil
	start     time.Time
	ended     atomic.Bool
	handles   []uint64 // one per observer
	LogID     int64
	SegmentID int64
	TraceID   string
	SpanID    string
}

// StartOp begins tracking an operation. The histogram observer is optional.
func StartOp(opType string, hist prometheus.Observer, labels prometheus.Labels) *Op {
	op := &Op{
		OpType: opType,
		Labels: labels,
		histo:  hist,
		start:  time.Now(),
	}
	op.handles = make([]uint64, len(observers))
	for i, obs := range observers {
		op.handles[i] = obs.OnOpStart(op)
	}
	return op
}

// End completes the operation. Records the histogram observation and notifies observers.
// Safe to call multiple times — only the first call takes effect.
func (o *Op) End(status string) {
	if !o.ended.CompareAndSwap(false, true) {
		return
	}
	elapsed := time.Since(o.start)
	if o.histo != nil {
		o.histo.Observe(float64(elapsed.Milliseconds()))
	}
	for i, obs := range observers {
		obs.OnOpEnd(o, o.handles[i], elapsed, status)
	}
}

// StartedAt returns when the op started.
func (o *Op) StartedAt() time.Time {
	return o.start
}
```

- [ ] **Step 3: Verify**

```bash
go test ./common/metrics/... -race -count=1 -v
```

- [ ] **Step 4: Commit**

```bash
git add common/metrics/op.go common/metrics/op_test.go
git commit -m "feat(metrics): add StartOp/End bridge API with observer chain"
```

---

### Task 8: OpRegistry as OpObserver

**Goal:** Implement the `OpObserver` interface on the `Registry`, so that `metrics.StartOp()` automatically registers ops in the registry and `End()` deregisters them.

**Files:**
- Create: `common/runtime/opregistry/observer.go`
- Create: `common/runtime/opregistry/observer_test.go`

**Spec reference:** §2.G.3 — observer implementation, zero business code import of opregistry.

- [ ] **Step 1: Implement observer**

Create `common/runtime/opregistry/observer.go`:

```go
package opregistry

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zilliztech/woodpecker/common/metrics"
)

var _ metrics.OpObserver = (*Registry)(nil)

var opCounter atomic.Uint64

// OnOpStart registers the op in the pool and returns the handle.
func (r *Registry) OnOpStart(op *metrics.Op) uint64 {
	rec := OpRecord{
		OpID:      generateOpID(op.OpType),
		OpType:    OpType(op.OpType),
		TraceID:   op.TraceID,
		SpanID:    op.SpanID,
		StartedAt: op.StartedAt(),
		LogID:     op.LogID,
		SegmentID: op.SegmentID,
	}
	return r.Register(rec)
}

// OnOpEnd deregisters the op from the pool.
func (r *Registry) OnOpEnd(op *metrics.Op, handle uint64, elapsed time.Duration, status string) {
	r.Deregister(handle)
}

func generateOpID(opType string) string {
	seq := opCounter.Add(1)
	return fmt.Sprintf("%s-%06d", opType, seq)
}
```

- [ ] **Step 2: Write test**

Verify that `Registry` satisfies `metrics.OpObserver` and that `StartOp` → `End` results in the op being registered then deregistered from the pool:

```go
func TestRegistry_AsOpObserver(t *testing.T) {
	reg := NewWithClock(100, 30*time.Second, clockwork.NewFakeClock())
	metrics.RegisterOpObserver(reg)
	defer func() { /* reset global observers */ }()

	op := metrics.StartOp("file.flush", nil, nil)
	require.Len(t, reg.List(Filter{}), 1)

	op.End("success")
	require.Len(t, reg.List(Filter{}), 0)
}
```

- [ ] **Step 3: Commit**

```bash
git add common/runtime/opregistry/observer.go common/runtime/opregistry/observer_test.go
git commit -m "feat(opregistry): implement OpObserver interface for automatic op tracking"
```

---

## Part C — Server Config + gRPC Interceptor (Tasks 9-11)

### Task 9: OpRegistry config fields

**Goal:** Add `Runtime.OpRegistry.{Capacity, WarnAge}` configuration fields so operators can tune the registry size and stall detection threshold.

**Files:**
- Modify: `common/config/configuration.go` — add `RuntimeConfig` and `OpRegistryConfig` structs, add `Runtime` field to `WoodpeckerConfig`

**Spec reference:** §5.4 — config additions.

- [ ] **Step 1: Add config types**

In `common/config/configuration.go`, add after the `StorageConfig` types:

```go
// OpRegistryConfig controls the in-flight operation registry.
type OpRegistryConfig struct {
	Capacity int             `yaml:"capacity"` // default 1024
	WarnAge  DurationSeconds `yaml:"warnAge"`  // default 30s
}

// RuntimeConfig holds runtime subsystem configuration.
type RuntimeConfig struct {
	OpRegistry OpRegistryConfig `yaml:"opRegistry"`
}
```

Extend `WoodpeckerConfig`:
```go
type WoodpeckerConfig struct {
	Meta     MetaConfig     `yaml:"meta"`
	Client   ClientConfig   `yaml:"client"`
	Logstore LogstoreConfig `yaml:"logstore"`
	Storage  StorageConfig  `yaml:"storage"`
	Runtime  RuntimeConfig  `yaml:"runtime"`
}
```

Add defaults in `getDefaultWoodpeckerConfig()`:
```go
Runtime: RuntimeConfig{
	OpRegistry: OpRegistryConfig{
		Capacity: 1024,
		WarnAge:  NewDurationSecondsFromInt(30),
	},
},
```

- [ ] **Step 2: Verify build**

```bash
go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add common/config/configuration.go
git commit -m "feat(config): add Runtime.OpRegistry.{Capacity, WarnAge} fields"
```

---

### Task 10: gRPC `UnaryServerInterceptor`

**Goal:** Create a gRPC unary interceptor that automatically wraps each RPC call in a `metrics.StartOp` / `End` cycle, so all gRPC operations appear in the op registry without manual instrumentation.

**Files:**
- Create: `common/runtime/opregistry/interceptor.go`
- Create: `common/runtime/opregistry/interceptor_test.go`

**Spec reference:** §2.G.3 — gRPC interceptor.

- [ ] **Step 1: Implement interceptor**

Create `common/runtime/opregistry/interceptor.go`:

```go
package opregistry

import (
	"context"

	"google.golang.org/grpc"

	"github.com/zilliztech/woodpecker/common/metrics"
)

// UnaryInterceptor returns a gRPC unary server interceptor that registers
// each RPC as an op in the registry via the metrics.StartOp bridge.
func UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		op := metrics.StartOp(string(OpTypeGRPC), nil, nil)
		resp, err := handler(ctx, req)
		status := "success"
		if err != nil {
			status = "error"
		}
		op.End(status)
		return resp, err
	}
}
```

- [ ] **Step 2: Write test**

Test that the interceptor calls StartOp and End:

```go
func TestUnaryInterceptor(t *testing.T) {
	interceptor := UnaryInterceptor()
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}
	resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test/Method"}, handler)
	require.NoError(t, err)
	require.Equal(t, "ok", resp)
}

func TestUnaryInterceptor_Error(t *testing.T) {
	interceptor := UnaryInterceptor()
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, fmt.Errorf("boom")
	}
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test/Method"}, handler)
	require.Error(t, err)
}
```

- [ ] **Step 3: Commit**

```bash
git add common/runtime/opregistry/interceptor.go common/runtime/opregistry/interceptor_test.go
git commit -m "feat(opregistry): add gRPC UnaryServerInterceptor for automatic op tracking"
```

---

### Task 11: OpRegistry instantiation + wiring

**Goal:** Wire the op registry into the server startup path: instantiate from config, register as `OpObserver`, add interceptor to gRPC chain, pass ops callbacks to `AdminCallbacks`.

**Files:**
- Modify: `cmd/main.go` — instantiate opregistry, call `metrics.RegisterOpObserver`, add to admin callbacks
- Modify: `server/service.go` — accept opregistry interceptor in gRPC chain

**Important:** This task depends on Tasks 8, 9, 10, and Task 22 (ops handler). If the handler isn't ready yet, wire the callbacks as nil initially and fill them in Task 23.

- [ ] **Step 1: Modify `server/service.go`**

Add an `OpRegistryInterceptor` field to the Server or accept it as a gRPC option parameter. Add it to the `grpc.ChainUnaryInterceptor` list at `service.go:176`:

```go
grpc.ChainUnaryInterceptor(
	s.shutdownUnaryInterceptor(),
	otelgrpc.UnaryServerInterceptor(),
	s.opRegistryInterceptor, // NEW — may be nil (no-op if nil)
),
```

- [ ] **Step 2: Modify `cmd/main.go`**

Before `commonhttp.Start()` (around line 190):

```go
opReg := opregistry.New(
	cfg.Woodpecker.Runtime.OpRegistry.Capacity,
	cfg.Woodpecker.Runtime.OpRegistry.WarnAge.ToDuration(),
)
metrics.RegisterOpObserver(opReg)
```

Pass `opregistry.UnaryInterceptor()` to the server constructor or set it on the server struct.

- [ ] **Step 3: Build and verify**

```bash
go build ./...
go test ./cmd/... -count=1
```

- [ ] **Step 4: Commit**

```bash
git add cmd/main.go server/service.go
git commit -m "feat(server): wire OpRegistry into startup — interceptor, observer, config"
```

---

## Part D — Logger Dynamic Level (Tasks 12-13)

### Task 12: Logger `zap.AtomicLevel` + `GetLevel` / `SetLevel`

**Goal:** Refactor `common/logger/logger.go` to use a shared `zap.AtomicLevel` instead of pre-built per-level loggers, enabling runtime level changes without restarting.

**Files:**
- Modify: `common/logger/logger.go`
- Create: `common/logger/level_test.go`

**Current state:** `logger.go` creates 4 separate loggers (debug/info/warn/error) at init time, stored in `_globalLevelLogger sync.Map`. `InitLogger()` picks one based on config and stores it in `_globalLogger atomic.Value`. Level cannot be changed after init.

**Target state:** A single `zap.AtomicLevel` controls the active logger. `SetLevel()` atomically changes the level. `GetLevel()` reads it. The `Ctx()` function continues to work as before (context-level override still supported).

- [ ] **Step 1: Write tests**

Create `common/logger/level_test.go`:

```go
func TestGetLevel_DefaultInfo(t *testing.T) {
	// After InitLogger with default config, level should be "info"
	level := GetLevel()
	assert.Equal(t, "info", level)
}

func TestSetLevel_ChangesLevel(t *testing.T) {
	SetLevel("debug")
	assert.Equal(t, "debug", GetLevel())
	SetLevel("warn")
	assert.Equal(t, "warn", GetLevel())
	// Reset
	SetLevel("info")
}

func TestSetLevel_InvalidLevel(t *testing.T) {
	err := SetLevel("invalid")
	assert.Error(t, err)
}
```

- [ ] **Step 2: Refactor logger.go**

Key changes:
1. Add a package-level `var globalLevel zap.AtomicLevel` initialized in `init()`
2. `InitLogger()` sets the global level from config via `globalLevel.SetLevel()`
3. `GetLevel() string` — returns `globalLevel.Level().String()`
4. `SetLevel(level string) error` — parses level string, calls `globalLevel.SetLevel()`
5. The `Ctx()` function behavior is preserved: context-level loggers still work, but the default logger respects the dynamic level

- [ ] **Step 3: Verify no regressions**

```bash
go test ./common/logger/... -race -count=1 -v
go test ./... -race -count=1 -short  # broader check
```

- [ ] **Step 4: Commit**

```bash
git add common/logger/logger.go common/logger/level_test.go
git commit -m "feat(logger): refactor to zap.AtomicLevel for runtime GetLevel/SetLevel"
```

---

### Task 13: Logging handler — `GET` + `POST /log/level`

**Goal:** Replace the TODO stub in `common/http/server.go` with a real handler that reads and sets the log level via the `logger.GetLevel()` / `SetLevel()` API.

**Files:**
- Create: `common/http/management/logging_handler.go`
- Create: `common/http/management/logging_handler_test.go`
- Modify: `common/http/server.go` — replace TODO stub with real handler

- [ ] **Step 1: Create handler**

Create `common/http/management/logging_handler.go`:

```go
package management

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/zilliztech/woodpecker/common/logger"
)

// NewLogLevelHandler returns a handler for GET + POST /log/level.
// GET returns {"level": "info"}.
// POST expects {"level": "debug"} and sets the log level.
func NewLogLevelHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"level": logger.GetLevel()})
		case http.MethodPost:
			var body struct {
				Level string `json:"level"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
				return
			}
			if err := logger.SetLevel(body.Level); err != nil {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"level": logger.GetLevel()})
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	}
}
```

- [ ] **Step 2: Write tests**

Test GET returns current level, POST changes it, POST with bad level returns 400.

- [ ] **Step 3: Wire in `server.go`**

Replace the TODO block at `server.go:107-113`:
```go
Register(&Handler{
	Path:        LogLevelRouterPath,
	HandlerFunc: management.NewLogLevelHandler(),
})
```

- [ ] **Step 4: Verify**

```bash
go test ./common/http/... -race -count=1 -v
```

- [ ] **Step 5: Commit**

```bash
git add common/http/management/logging_handler.go common/http/management/logging_handler_test.go common/http/server.go
git commit -m "feat(server): implement GET + POST /log/level handler (replace stub)"
```

---

## Part E — Writer Snapshot + WriterRegistry (Tasks 14-17)

### Task 14: `Writer.Snapshot()` / `SnapshotDetailed()` interface extension

**Goal:** Extend the `storage.Writer` interface with two snapshot methods that expose writer state for the C family logstore commands. `Snapshot()` returns a lightweight summary; `SnapshotDetailed()` returns full state including buffer/flush info.

**Files:**
- Modify: `server/storage/writer.go` — add 2 methods to interface
- Modify: mock generation — regenerate Writer mock

**Spec reference:** §2.C.3 — Writer snapshot methods.

- [ ] **Step 1: Define snapshot types**

Add to `server/storage/writer.go` or a new `server/storage/types.go`:

```go
// WriterSnapshot is the lightweight state returned by Snapshot().
type WriterSnapshot struct {
	LogID       int64  `json:"log_id"`
	SegmentID   int64  `json:"segment_id"`
	Backend     string `json:"backend"` // "objectstorage" | "stagedstorage" | "disk"
	State       string `json:"state"`   // "active" | "finalized" | "fenced"
	Writable    bool   `json:"writable"`
	Fenced      bool   `json:"fenced"`
	Finalized   bool   `json:"finalized"`
	SizeBytes   int64  `json:"size_bytes"`
	EntryCount  int64  `json:"entry_count"`
	FirstEntry  int64  `json:"first_entry_id"`
	LastEntry   int64  `json:"last_entry_id"`
	CreatedAtMS int64  `json:"created_at_ms"`
}

// WriterSnapshotDetailed extends WriterSnapshot with buffer and flush queue info.
type WriterSnapshotDetailed struct {
	WriterSnapshot
	BufferBytes                   int64 `json:"buffer_bytes"`
	BufferEntries                 int64 `json:"buffer_entries"`
	FlushQueueDepth               int   `json:"flush_queue_depth"`
	FlushQueueCapacity            int   `json:"flush_queue_capacity"`
	WrittenBytes                  int64 `json:"written_bytes"`
	LastSubmittedFlushingEntryID  int64 `json:"last_submitted_flushing_entry_id"`
	LastSubmittedFlushingBlockID  int64 `json:"last_submitted_flushing_block_id"`
	LastModifiedMS                int64 `json:"last_modified_ms"`
	Recovered                     bool  `json:"recovered"`
}
```

- [ ] **Step 2: Add methods to Writer interface**

```go
type Writer interface {
	// ... existing 9 methods ...

	// Snapshot returns a lightweight state summary of this writer.
	Snapshot(ctx context.Context) WriterSnapshot

	// SnapshotDetailed returns full state including buffer and flush queue info.
	SnapshotDetailed(ctx context.Context) WriterSnapshotDetailed
}
```

- [ ] **Step 3: Regenerate mock**

```bash
~/go/bin/mockery --dir=./server/storage --name=Writer --structname=Writer --output=mocks/mocks_server/mocks_storage --filename=mock_writer.go --with-expecter=true --outpkg=mocks_storage
```

- [ ] **Step 4: Commit**

```bash
git add server/storage/writer.go server/storage/types.go mocks/mocks_server/mocks_storage/mock_writer.go
git commit -m "feat(storage): add Snapshot/SnapshotDetailed to Writer interface"
```

---

### Task 15: Snapshot impl — objectstorage backend

**Goal:** Implement `Snapshot()` and `SnapshotDetailed()` on `MinioFileWriter`.

**Files:**
- Modify: `server/storage/objectstorage/writer_impl.go`
- Create: `server/storage/objectstorage/snapshot_test.go`

- [ ] **Step 1: Implement Snapshot methods**

Map existing `MinioFileWriter` fields to `WriterSnapshot` / `WriterSnapshotDetailed`:
- `Backend`: `"objectstorage"`
- `LogID`/`SegmentID`: from writer fields
- `Writable`/`Fenced`/`Finalized`: from writer state flags
- `SizeBytes`: from `currentBlockSize` or accumulated bytes
- `EntryCount`: from `lastEntryId - firstEntryId + 1`
- `BufferBytes`: from buffer pointer
- `FlushQueueDepth`/`FlushQueueCapacity`: from flush channel len/cap

- [ ] **Step 2: Write test**

Create a test that initializes a `MinioFileWriter` (or uses the existing test setup), writes some entries, and verifies `Snapshot()` returns correct field values.

- [ ] **Step 3: Commit**

```bash
git add server/storage/objectstorage/writer_impl.go server/storage/objectstorage/snapshot_test.go
git commit -m "feat(objectstorage): implement Writer.Snapshot/SnapshotDetailed"
```

---

### Task 16: Snapshot impl — stagedstorage + disk backends

**Goal:** Implement `Snapshot()` and `SnapshotDetailed()` on `StagedFileWriter` and `DiskFileWriter`.

**Files:**
- Modify: `server/storage/stagedstorage/writer_impl.go`
- Modify: `server/storage/disk/writer_impl.go`
- Create: snapshot tests for each

- [ ] **Step 1: Implement on StagedFileWriter**

Map fields similarly to Task 15. `Backend`: `"stagedstorage"`.

- [ ] **Step 2: Implement on DiskFileWriter**

Map fields similarly. `Backend`: `"disk"`.

- [ ] **Step 3: Write tests for both**

- [ ] **Step 4: Commit**

```bash
git add server/storage/stagedstorage/writer_impl.go server/storage/disk/writer_impl.go \
    server/storage/stagedstorage/snapshot_test.go server/storage/disk/snapshot_test.go
git commit -m "feat(storage): implement Snapshot/SnapshotDetailed on stagedstorage + disk backends"
```

---

### Task 17: WriterRegistry interface + logstore integration

**Goal:** Create a `WriterRegistry` interface that exposes all active writers across all logs/segments, enabling the C family logstore commands to list and query segment state.

**Files:**
- Create: `server/storage/writer_registry.go`
- Modify: `server/logstore.go` — implement `WriterRegistry` on `LogStoreImpl` (or expose via method)

**Spec reference:** §2.C.3 — WriterRegistry abstraction.

- [ ] **Step 1: Define interface**

Create `server/storage/writer_registry.go`:

```go
package storage

import "context"

// WriterRegistry provides access to all active Writers.
type WriterRegistry interface {
	// ListWriterSnapshots returns snapshots of all active writers,
	// optionally filtered by logID, state, writable flag.
	ListWriterSnapshots(ctx context.Context, filter WriterFilter) []WriterSnapshot

	// GetWriterSnapshotDetailed returns the detailed snapshot for a specific writer.
	GetWriterSnapshotDetailed(ctx context.Context, logID, segmentID int64) (*WriterSnapshotDetailed, error)

	// ForceFlush forces a sync on the specified writer (or all if logID=0, segmentID=0).
	ForceFlush(ctx context.Context, logID, segmentID int64) error

	// ForceFence forces a fence on the specified writer.
	ForceFence(ctx context.Context, logID, segmentID int64, reason string) error

	// ForceCompact forces compaction on the specified writer.
	ForceCompact(ctx context.Context, logID, segmentID int64) error
}

// WriterFilter specifies criteria for listing writers.
type WriterFilter struct {
	LogID    *int64  // nil = all
	State    string  // empty = all
	Writable *bool   // nil = all
}
```

- [ ] **Step 2: Implement on LogStoreImpl**

The `LogStoreImpl` already maintains a map of segment processors, each holding a writer. Implement `WriterRegistry` by iterating over all processors and collecting snapshots.

- [ ] **Step 3: Write tests**

- [ ] **Step 4: Commit**

```bash
git add server/storage/writer_registry.go server/logstore.go
git commit -m "feat(storage): add WriterRegistry interface with logstore implementation"
```

---

## Part F — Server Handlers + Routing (Tasks 18-23)

### Task 18: Router path constants for C/G families

**Goal:** Add path constants for all Phase 2 admin endpoints to `common/http/router.go`.

**Files:**
- Modify: `common/http/router.go`

- [ ] **Step 1: Add constants**

```go
// C family — logstore
const AdminLogstoreSegmentsPath = "/admin/logstore/segments"
const AdminLogstoreFlushPath = "/admin/logstore/flush"
const AdminLogstoreFencePath = "/admin/logstore/fence"
const AdminLogstoreCompactPath = "/admin/logstore/compact"

// G family — ops
const AdminRuntimeOpsPath = "/admin/runtime/ops"
const AdminRuntimeOpsStatsPath = "/admin/runtime/ops/stats"
```

Note: `/admin/logstore/segments/{log}/{seg}` and `/admin/runtime/ops/{op_id}` use query parameters or path parsing within the handler, not separate path constants.

- [ ] **Step 2: Commit**

```bash
git add common/http/router.go
git commit -m "feat(http): add router path constants for C/G family endpoints"
```

---

### Task 19: `AdminCallbacks` extension

**Goal:** Extend the `AdminCallbacks` struct with sub-structs for Logstore, Ops, and LogLevel callback groups.

**Files:**
- Modify: `common/http/server.go`

- [ ] **Step 1: Add sub-structs**

```go
// LogstoreCallbacks holds callbacks for logstore admin endpoints.
type LogstoreCallbacks struct {
	ListSegments    func(filter storage.WriterFilter) []storage.WriterSnapshot
	GetSegment      func(logID, segmentID int64) (*storage.WriterSnapshotDetailed, error)
	ForceFlush      func(logID, segmentID int64) error
	ForceFence      func(logID, segmentID int64, reason string) error
	ForceCompact    func(logID, segmentID int64) error
}

// OpsCallbacks holds callbacks for ops admin endpoints.
type OpsCallbacks struct {
	List  func(filter opregistry.Filter) []opregistry.OpRecord
	Get   func(opID string) *opregistry.OpRecord
	Stats func() opregistry.Stats
}

type AdminCallbacks struct {
	// Phase 1 callbacks (unchanged)
	GetMemberlistStatus     func() string
	GetMemberlistJSON       func() []byte
	GetNodeStatus           func() any
	Decommission            func() error
	GetDecommissionProgress func() any
	CancelDecommission      func() error
	GetConfig               func() any

	// Phase 2 callbacks
	Logstore LogstoreCallbacks
	Ops      OpsCallbacks
}
```

Note: LogLevel doesn't need a callback — the `management.NewLogLevelHandler()` is self-contained (calls `logger.GetLevel/SetLevel` directly).

- [ ] **Step 2: Commit**

```bash
git add common/http/server.go
git commit -m "feat(http): extend AdminCallbacks with Logstore and Ops sub-structs"
```

---

### Task 20: Logstore handler — segments list + segment show

**Goal:** Implement the GET endpoints for listing and showing segment state via the `WriterRegistry`.

**Files:**
- Create: `common/http/management/logstore_handler.go`
- Create: `common/http/management/logstore_handler_test.go`

**Spec reference:** §2.C — C.1 segments list response structure, C.2 segment show.

- [ ] **Step 1: Implement handlers**

```go
// NewLogstoreSegmentsHandler handles GET /admin/logstore/segments
// Query params: ?log_id=42&state=active&writable=true
func NewLogstoreSegmentsHandler(list func(filter storage.WriterFilter) []storage.WriterSnapshot) http.HandlerFunc

// Segments detail uses the same path with ?log_id=X&segment_id=Y&detailed=true
// or a separate path. Implementation returns WriterSnapshotDetailed.
func NewLogstoreSegmentShowHandler(get func(logID, segmentID int64) (*storage.WriterSnapshotDetailed, error)) http.HandlerFunc
```

Response format for list:
```json
{"segments": [{ "log_id": 42, "segment_id": 7, "backend": "staged", ... }]}
```

- [ ] **Step 2: Write tests**

Test with mock callbacks. Verify query param parsing, JSON response shape, 404 on unknown segment.

- [ ] **Step 3: Commit**

```bash
git add common/http/management/logstore_handler.go common/http/management/logstore_handler_test.go
git commit -m "feat(server): add GET /admin/logstore/segments list and show handlers"
```

---

### Task 21: Logstore handler — flush + fence + compact

**Goal:** Implement the POST endpoints for force-flush, force-fence, and force-compact.

**Files:**
- Modify: `common/http/management/logstore_handler.go`
- Modify: `common/http/management/logstore_handler_test.go`

**Spec reference:** §2.C — C.5 force-flush, C.6 fence, C.7 compact.

- [ ] **Step 1: Implement handlers**

```go
// NewLogstoreFlushHandler handles POST /admin/logstore/flush
// Body: {"log_id": 42, "segment_id": 7} or {} for all
func NewLogstoreFlushHandler(flush func(logID, segmentID int64) error) http.HandlerFunc

// NewLogstoreFenceHandler handles POST /admin/logstore/fence
// Body: {"log_id": 42, "segment_id": 7, "reason": "stuck flush investigation"}
func NewLogstoreFenceHandler(fence func(logID, segmentID int64, reason string) error) http.HandlerFunc

// NewLogstoreCompactHandler handles POST /admin/logstore/compact
// Body: {"log_id": 42, "segment_id": 7}
func NewLogstoreCompactHandler(compact func(logID, segmentID int64) error) http.HandlerFunc
```

Error responses:
- 409 Conflict: segment not in valid state for the operation
- 404: segment not found
- 400: invalid request body

- [ ] **Step 2: Write tests**

- [ ] **Step 3: Commit**

```bash
git add common/http/management/logstore_handler.go common/http/management/logstore_handler_test.go
git commit -m "feat(server): add POST /admin/logstore/flush, fence, compact handlers"
```

---

### Task 22: Ops handler — list + get + stats

**Goal:** Implement the 3 ops endpoints that expose the in-flight op registry state.

**Files:**
- Create: `common/http/management/ops_handler.go`
- Create: `common/http/management/ops_handler_test.go`

**Spec reference:** §2.G — G.1 list, G.2 show, G.3 stats.

- [ ] **Step 1: Implement handlers**

```go
// NewOpsListHandler handles GET /admin/runtime/ops
// Query params: ?type=file.flush&log_id=42&segment_id=7&longer_than_ms=30000&limit=100
func NewOpsListHandler(list func(filter opregistry.Filter) []opregistry.OpRecord) http.HandlerFunc

// NewOpsGetHandler handles GET /admin/runtime/ops?op_id=fl-000001
func NewOpsGetHandler(get func(opID string) *opregistry.OpRecord) http.HandlerFunc

// NewOpsStatsHandler handles GET /admin/runtime/ops/stats
func NewOpsStatsHandler(stats func() opregistry.Stats) http.HandlerFunc
```

- [ ] **Step 2: Write tests**

Test with mock callbacks. Verify query param parsing for filters, 404 on unknown op_id, JSON response shape.

- [ ] **Step 3: Commit**

```bash
git add common/http/management/ops_handler.go common/http/management/ops_handler_test.go
git commit -m "feat(server): add ops list/get/stats handlers for op registry"
```

---

### Task 23: Logstore + Ops admin callbacks wiring in `cmd/main.go`

**Goal:** Wire the logstore (WriterRegistry) and ops (Registry) callbacks into `AdminCallbacks` in `cmd/main.go`, and register all new handlers in `server.go`.

**Files:**
- Modify: `cmd/main.go` — fill `Logstore` and `Ops` callback fields
- Modify: `common/http/server.go` — register new handlers in `Start()`

- [ ] **Step 1: Wire callbacks in `cmd/main.go`**

```go
Logstore: commonhttp.LogstoreCallbacks{
	ListSegments: func(filter storage.WriterFilter) []storage.WriterSnapshot {
		return srv.ListWriterSnapshots(context.Background(), filter)
	},
	GetSegment: func(logID, segmentID int64) (*storage.WriterSnapshotDetailed, error) {
		return srv.GetWriterSnapshotDetailed(context.Background(), logID, segmentID)
	},
	ForceFlush: func(logID, segmentID int64) error {
		return srv.ForceFlush(context.Background(), logID, segmentID)
	},
	ForceFence: func(logID, segmentID int64, reason string) error {
		return srv.ForceFence(context.Background(), logID, segmentID, reason)
	},
	ForceCompact: func(logID, segmentID int64) error {
		return srv.ForceCompact(context.Background(), logID, segmentID)
	},
},
Ops: commonhttp.OpsCallbacks{
	List:  opReg.List,
	Get:   opReg.Get,
	Stats: opReg.Stats,
},
```

- [ ] **Step 2: Register handlers in `server.go` Start()**

Add conditional registration for all new endpoints (same pattern as Phase 1):

```go
if callbacks.Logstore.ListSegments != nil {
	Register(&Handler{Path: AdminLogstoreSegmentsPath, HandlerFunc: management.NewLogstoreSegmentsHandler(callbacks.Logstore.ListSegments)})
	// ... etc for each endpoint
}
if callbacks.Ops.List != nil {
	Register(&Handler{Path: AdminRuntimeOpsPath, HandlerFunc: management.NewOpsListHandler(callbacks.Ops.List)})
	Register(&Handler{Path: AdminRuntimeOpsStatsPath, HandlerFunc: management.NewOpsStatsHandler(callbacks.Ops.Stats)})
}
```

- [ ] **Step 3: Build and verify**

```bash
go build ./...
go test ./common/http/... -count=1 -v
```

- [ ] **Step 4: Commit**

```bash
git add cmd/main.go common/http/server.go
git commit -m "feat(server): wire logstore + ops callbacks into AdminCallbacks"
```

---

## Part G — Metric Migration (Tasks 24-26)

### Task 24: Migrate `logstore.go` × 9 methods to op pattern

**Goal:** Replace the manual `start := time.Now()` + `Observe(elapsed)` pattern in all 9 logstore methods with `metrics.StartOp()` / `defer op.End()`. This preserves the existing histogram behavior (same metric, same labels) while adding automatic op registration.

**Files:**
- Modify: `server/logstore.go`

**Spec reference:** §2.G.3 — mechanical migration, ~5 lines per site.

**Migration pattern:**

Before (current):
```go
func (l *LogStoreImpl) AddEntry(ctx context.Context, ...) (int64, error) {
	start := time.Now()
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath

	segmentProcessor, err := l.getOrCreateSegmentProcessor(...)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		return -1, err
	}
	entryId, err := segmentProcessor.AddEntry(ctx, entry, syncedResultCh)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", "error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, err
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", "success").Observe(float64(time.Since(start).Milliseconds()))
	return entryId, nil
}
```

After:
```go
func (l *LogStoreImpl) AddEntry(ctx context.Context, ...) (int64, error) {
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath
	op := metrics.StartOp("logstore.add_entry",
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", "pending"),
		nil)
	op.LogID = logId
	op.SegmentID = entry.SegId
	status := "success"
	defer func() {
		// Re-observe with correct status label (overwrite the "pending" observation)
		op.End(status)
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", status).Inc()
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(...)
	if err != nil {
		status = "error_get_processor"
		return -1, err
	}
	entryId, err := segmentProcessor.AddEntry(ctx, entry, syncedResultCh)
	if err != nil {
		status = "error"
		return -1, err
	}
	return entryId, nil
}
```

**Important note on histogram labels:** The existing metric uses a `status` label. The op pattern uses `op.End(status)` which observes the histogram. We need to handle the fact that the histogram label set includes `status` — the `StartOp` creates the `Op` but the histogram observation needs the final status. The simplest approach: pass a placeholder observer to `StartOp` and do the histogram observation in the deferred function where `status` is known. Or modify `Op.End` to accept label overrides.

**Design decision:** Keep `Op.End()` simple. Instead, don't pass the histogram to `StartOp` — observe it manually in the defer, and `StartOp` only serves as the op registration mechanism:

```go
op := metrics.StartOp("logstore.add_entry", nil, nil) // nil histogram
op.LogID = logId
status := "success"
defer func() {
	op.End(status)
	elapsed := float64(time.Since(op.StartedAt()).Milliseconds())
	metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", status).Observe(elapsed)
	metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", status).Inc()
}()
```

This preserves exact backward compatibility of the histogram label values.

- [ ] **Step 1: Apply the pattern to all 9 methods**

Methods to migrate (all in `server/logstore.go`):
1. `AddEntry` (~line 185)
2. `GetBatchEntriesAdv` (~line 280)
3. `CompleteSegment` (~line 320)
4. `FenceSegment` (~line 340)
5. `GetSegmentLastAddConfirmed` (~line 370)
6. `GetSegmentBlockCount` (~line 400)
7. `CompactSegment` (~line 435)
8. `CleanSegment` (~line 465)
9. `UpdateLastAddConfirmed` (~line 495)

- [ ] **Step 2: Build and verify existing tests pass**

```bash
go build ./...
go test ./server/... -race -count=1
```

- [ ] **Step 3: Commit**

```bash
git add server/logstore.go
git commit -m "refactor(logstore): migrate 9 methods to metrics.StartOp/End pattern"
```

---

### Task 25: Migrate storage flush sites to op pattern

**Goal:** Apply the same `StartOp` / `End` pattern to the 3 storage backend flush metric sites.

**Files:**
- Modify: `server/storage/objectstorage/writer_impl.go` — `Sync()` method (~line 984)
- Modify: `server/storage/stagedstorage/writer_impl.go` — `processFlushTask()` (~line 631)
- Modify: `server/storage/disk/writer_impl.go` — `processFlushTask()` (~line 617)

**Spec reference:** §2.G.3 — mechanical migration.

- [ ] **Step 1: Migrate objectstorage**

In `Sync()`, replace the manual `startTime := time.Now()` / `Observe(elapsed)` pattern with `StartOp("file.sync", nil, nil)` + deferred `End` + manual histogram observation.

- [ ] **Step 2: Migrate stagedstorage**

In `processFlushTask()`, same pattern with `StartOp("file.flush", nil, nil)`.

- [ ] **Step 3: Migrate disk**

In `processFlushTask()`, same pattern.

- [ ] **Step 4: Build and verify**

```bash
go build ./...
go test ./server/storage/... -race -count=1
```

- [ ] **Step 5: Commit**

```bash
git add server/storage/objectstorage/writer_impl.go \
    server/storage/stagedstorage/writer_impl.go \
    server/storage/disk/writer_impl.go
git commit -m "refactor(storage): migrate flush sites to metrics.StartOp/End pattern"
```

---

### Task 26: Metric migration regression tests

**Goal:** Verify that all 12 migrated sites still produce the same Prometheus metrics with the same label values and bucket distribution. Each test calls the migrated function and checks the histogram/counter was observed.

**Files:**
- Create: `server/logstore_op_test.go`
- Create: `server/storage/objectstorage/op_test.go` (or extend existing test files)

- [ ] **Step 1: Write regression tests**

For each migration site, write a test that:
1. Resets the relevant Prometheus metric (or uses a fresh registry)
2. Calls the migrated function
3. Asserts the histogram was observed (count > 0)
4. Asserts the counter was incremented with the expected labels

Example for `AddEntry`:
```go
func TestAddEntry_MetricMigration(t *testing.T) {
	// Setup mock segment processor, call AddEntry
	// Verify WpLogStoreOperationLatency was observed with labels [..., "add_entry", "success"]
	// Verify WpLogStoreOperationsTotal was incremented with same labels
}
```

- [ ] **Step 2: Run all**

```bash
go test ./server/... -race -count=1 -v -run "MetricMigration"
```

- [ ] **Step 3: Commit**

```bash
git add server/logstore_op_test.go server/storage/objectstorage/op_test.go
git commit -m "test(metrics): regression tests for 12 op-pattern metric migration sites"
```

---

## Part H — E Family CLI: Logging Commands (Tasks 27-28)

### Task 27: `wp logging` parent + `get-level`

**Goal:** Add the `logging` command family and implement `wp logging get-level <node>`.

**Files:**
- Create: `cmd/wpcli/cmd/logging.go`
- Create: `cmd/wpcli/cmd/logging_get_level.go`
- Create: `cmd/wpcli/cmd/logging_test.go`
- Modify: `cmd/wpcli/cmd/root.go` — register `logging` command

**Spec reference:** §2.E.3 — logging get-level.

- [ ] **Step 1: Create parent command**

`logging.go`: Create `newLoggingCommand()` that adds `get-level` and `set-level` subcommands.

- [ ] **Step 2: Implement `get-level`**

`logging_get_level.go`:
- Resolve node target (single node via `resolveAndDiscover`)
- `GET /log/level` on the target node
- Parse JSON response `{"level": "info"}`
- Table output: `NODE  LEVEL`
- JSON/YAML: passthrough

With `--all` flag: fan-out to all nodes, show level per node.

- [ ] **Step 3: Write tests**

Test with httptest server returning `{"level": "debug"}`. Verify table and JSON output.

- [ ] **Step 4: Register in root.go**

- [ ] **Step 5: Commit**

```bash
git add cmd/wpcli/cmd/logging.go cmd/wpcli/cmd/logging_get_level.go cmd/wpcli/cmd/logging_test.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add logging get-level command"
```

---

### Task 28: `wp logging set-level`

**Goal:** Implement `wp logging set-level <node> --level <level>` to change a node's log level at runtime.

**Files:**
- Create: `cmd/wpcli/cmd/logging_set_level.go`
- Modify: `cmd/wpcli/cmd/logging_test.go`

**Spec reference:** §2.E.4 — logging set-level.

- [ ] **Step 1: Implement**

- Required flag: `--level` (debug/info/warn/error)
- `POST /log/level` with body `{"level": "debug"}`
- Parse response, print confirmation
- With `--all`: fan-out POST to all nodes

- [ ] **Step 2: Write tests**

Test POST body construction, response parsing, error on invalid level.

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/logging_set_level.go cmd/wpcli/cmd/logging_test.go
git commit -m "feat(wpcli): add logging set-level command"
```

---

## Part I — C Family CLI: Logstore Commands (Tasks 29-35)

### Task 29: `wp logstore` parent command

**Goal:** Create the `logstore` command group and register it in the root.

**Files:**
- Create: `cmd/wpcli/cmd/logstore.go`
- Modify: `cmd/wpcli/cmd/root.go`

- [ ] **Step 1: Create parent**

```go
func newLogstoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logstore",
		Short: "Inspect and manage logstore segments",
	}
	cmd.AddCommand(
		newLogstoreSegmentsCommand(),
		newLogstoreSegmentShowCommand(),
		newLogstoreBufferCommand(),
		newLogstoreFlushQueueCommand(),
		newLogstoreForceFlushCommand(),
		newLogstoreFenceCommand(),
		newLogstoreCompactCommand(),
	)
	return cmd
}
```

- [ ] **Step 2: Register in root.go**

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/logstore.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add logstore parent command"
```

---

### Task 30: `wp logstore segments`

**Goal:** List active segments across a node with filtering by log_id, state, writable.

**Files:**
- Create: `cmd/wpcli/cmd/logstore_segments.go`
- Create: `cmd/wpcli/cmd/logstore_test.go`

**Spec reference:** §2.C.1 — segments list.

- [ ] **Step 1: Implement**

- Fetch `GET /admin/logstore/segments?log_id=X&state=active&writable=true`
- Parse response into `[]WriterSnapshot`
- Table columns: `LOG_ID  SEG_ID  BACKEND  STATE  WRITABLE  SIZE  ENTRIES  FLUSH_Q`
- Flags: `--log`, `--state`, `--writable`, `--all` (fan-out)
- JSON/YAML: full response passthrough

- [ ] **Step 2: Write tests**

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/logstore_segments.go cmd/wpcli/cmd/logstore_test.go
git commit -m "feat(wpcli): add logstore segments command"
```

---

### Task 31: `wp logstore segment-show`

**Goal:** Show detailed state of a single segment (full dump with buffer/flush info).

**Files:**
- Create: `cmd/wpcli/cmd/logstore_segment_show.go`
- Modify: `cmd/wpcli/cmd/logstore_test.go`

**Spec reference:** §2.C.2 — segment show.

- [ ] **Step 1: Implement**

- Required flags: `--log` and `--seg`
- Optional: `--full` (includes all detail fields)
- Fetch `GET /admin/logstore/segments?log_id=X&segment_id=Y&detailed=true`
- Sectioned output (like `wp node show`): Identity, State, Buffer, Flush Queue, Timing

- [ ] **Step 2: Write tests**

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/logstore_segment_show.go cmd/wpcli/cmd/logstore_test.go
git commit -m "feat(wpcli): add logstore segment-show command"
```

---

### Task 32: `wp logstore buffer`

**Goal:** Show buffer bytes summary across all writers on a node, sorted by buffer size descending.

**Files:**
- Create: `cmd/wpcli/cmd/logstore_buffer.go`
- Modify: `cmd/wpcli/cmd/logstore_test.go`

**Spec reference:** §2.C.3 — buffer summary.

- [ ] **Step 1: Implement**

- Reuses `GET /admin/logstore/segments` endpoint (same data, different view)
- Client-side aggregation: sort by `buffer_bytes` descending, show top-N
- Table columns: `LOG:SEG  BUFFER_BYTES  BUFFER_ENTRIES  PCT_OF_TOTAL`
- Flags: `--top N` (default 20), `--all` (fan-out)

- [ ] **Step 2: Write tests**

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/logstore_buffer.go cmd/wpcli/cmd/logstore_test.go
git commit -m "feat(wpcli): add logstore buffer command"
```

---

### Task 33: `wp logstore flush-queue`

**Goal:** Show flush queue depth summary across all writers on a node.

**Files:**
- Create: `cmd/wpcli/cmd/logstore_flush_queue.go`
- Modify: `cmd/wpcli/cmd/logstore_test.go`

**Spec reference:** §2.C.4 — flush queue summary.

- [ ] **Step 1: Implement**

- Reuses `GET /admin/logstore/segments` endpoint
- Client-side aggregation: sort by `flush_queue_depth` descending
- Table columns: `LOG:SEG  QUEUE_DEPTH  QUEUE_CAP  UTILIZATION`
- Flags: `--top N` (default 20), `--all` (fan-out)

- [ ] **Step 2: Write tests**

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/logstore_flush_queue.go cmd/wpcli/cmd/logstore_test.go
git commit -m "feat(wpcli): add logstore flush-queue command"
```

---

### Task 34: `wp logstore force-flush`

**Goal:** Force sync on a specific segment or all segments on a node.

**Files:**
- Create: `cmd/wpcli/cmd/logstore_force_flush.go`
- Modify: `cmd/wpcli/cmd/logstore_test.go`

**Spec reference:** §2.C.5 — force flush.

- [ ] **Step 1: Implement**

- `POST /admin/logstore/flush` with body `{"log_id": X, "segment_id": Y}` or `{}`
- Flags: `--log`, `--seg` (both 0 = flush all)
- No confirmation prompt (flush is safe)
- Print result: "Flushed segment 42:7" or "Flushed all segments"

- [ ] **Step 2: Write tests**

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/logstore_force_flush.go cmd/wpcli/cmd/logstore_test.go
git commit -m "feat(wpcli): add logstore force-flush command"
```

---

### Task 35: `wp logstore fence` + `wp logstore compact`

**Goal:** Implement the two remaining intervene commands. Fence is high-risk and requires `--reason` and confirmation.

**Files:**
- Create: `cmd/wpcli/cmd/logstore_fence.go`
- Create: `cmd/wpcli/cmd/logstore_compact.go`
- Modify: `cmd/wpcli/cmd/logstore_test.go`

**Spec reference:** §2.C.6 (fence), §2.C.7 (compact).

- [ ] **Step 1: Implement fence**

- `POST /admin/logstore/fence` with body `{"log_id": X, "segment_id": Y, "reason": "..."}`
- Required: `--log`, `--seg`, `--reason`
- Confirmation prompt (unless `-y` / `--yes`)
- Exit 7 on user abort

- [ ] **Step 2: Implement compact**

- `POST /admin/logstore/compact` with body `{"log_id": X, "segment_id": Y}`
- Required: `--log`, `--seg`
- No confirmation (compact is safe; server validates preconditions and returns 409 if not ready)

- [ ] **Step 3: Write tests for both**

- [ ] **Step 4: Commit**

```bash
git add cmd/wpcli/cmd/logstore_fence.go cmd/wpcli/cmd/logstore_compact.go cmd/wpcli/cmd/logstore_test.go
git commit -m "feat(wpcli): add logstore fence and compact commands"
```

---

## Part J — G Family CLI: Ops Commands (Tasks 36-38)

### Task 36: `wp ops` parent + `ops list`

**Goal:** Create the `ops` command family and implement the list command with rich filtering.

**Files:**
- Create: `cmd/wpcli/cmd/ops.go`
- Create: `cmd/wpcli/cmd/ops_list.go`
- Create: `cmd/wpcli/cmd/ops_test.go`
- Modify: `cmd/wpcli/cmd/root.go`

**Spec reference:** §2.G.1 — ops list.

- [ ] **Step 1: Create parent + register**

- [ ] **Step 2: Implement `ops list`**

- Fetch `GET /admin/runtime/ops?type=...&log_id=...&segment_id=...&longer_than_ms=...&limit=...`
- Table columns: `OP_TYPE  OP_ID  TRACE_ID  ELAPSED  LOG:SEG`
- Color coding: elapsed > warnAge → yellow indicator
- Flags: `--type` (multi-value), `--log`, `--seg`, `--longer-than`, `--limit`, `--sort-by`, `--all` (fan-out)
- Pool utilization header: `Pool: 487/1024 used (47.6%)  Evicted: total=12341 young=12287 old=54 ⚠`

- [ ] **Step 3: Write tests**

- [ ] **Step 4: Commit**

```bash
git add cmd/wpcli/cmd/ops.go cmd/wpcli/cmd/ops_list.go cmd/wpcli/cmd/ops_test.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add ops list command with filtering"
```

---

### Task 37: `wp ops show`

**Goal:** Show detailed view of a single in-flight operation with cross-reference hints.

**Files:**
- Create: `cmd/wpcli/cmd/ops_show.go`
- Modify: `cmd/wpcli/cmd/ops_test.go`

**Spec reference:** §2.G.2 — ops show.

- [ ] **Step 1: Implement**

- Required: `--op-id`
- Fetch `GET /admin/runtime/ops?op_id=<id>` from the resolved node
- Sectioned output:
  - **Identity:** op_type, op_id, trace_id, span_id
  - **Timing:** started_at, elapsed, age status (color)
  - **Context:** log_id, segment_id
  - **Trace:** "Use this trace_id with your trace backend: <trace_id>"
  - **Cross-references:** suggested follow-up commands
- Exit 11 (ResourceNotFound) if op_id not found

- [ ] **Step 2: Write tests**

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/ops_show.go cmd/wpcli/cmd/ops_test.go
git commit -m "feat(wpcli): add ops show command with cross-reference hints"
```

---

### Task 38: `wp ops stats`

**Goal:** Show registry utilization and eviction statistics.

**Files:**
- Create: `cmd/wpcli/cmd/ops_stats.go`
- Modify: `cmd/wpcli/cmd/ops_test.go`

**Spec reference:** §2.G.3 — ops stats.

- [ ] **Step 1: Implement**

- Fetch `GET /admin/runtime/ops/stats`
- Sectioned output:
  - **Capacity:** capacity, in_use, utilization %
  - **Eviction Totals:** total, young (%), old (%) — old > 0 gets ⚠ marker
- Flags: `--all` (fan-out, show stats per node)
- JSON/YAML: full response passthrough

- [ ] **Step 2: Write tests**

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/ops_stats.go cmd/wpcli/cmd/ops_test.go
git commit -m "feat(wpcli): add ops stats command with eviction analysis"
```

---

## Part K — D Family CLI: Metrics Commands (Tasks 39-44)

### Task 39: Prometheus scrape parser

**Goal:** Create a reusable Prometheus text format parser for the D family commands. Parses the output of `GET /metrics` into structured metric families.

**Files:**
- Create: `cmd/wpcli/internal/prom/parser.go`
- Create: `cmd/wpcli/internal/prom/parser_test.go`

- [ ] **Step 1: Implement parser**

Use `github.com/prometheus/common/expfmt` to parse the Prometheus text exposition format:

```go
package prom

import (
	"io"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// MetricFamily wraps the parsed Prometheus metric family.
type MetricFamily = dto.MetricFamily

// Parse reads Prometheus text format from r and returns metric families.
func Parse(r io.Reader) (map[string]*MetricFamily, error) {
	parser := expfmt.TextParser{}
	return parser.TextToMetricFamilies(r)
}

// ScrapeNode fetches /metrics from the given URL and parses it.
func ScrapeNode(url string, timeout time.Duration) (map[string]*MetricFamily, error)

// GetGaugeValue extracts the gauge value from a metric with matching labels.
func GetGaugeValue(mf *MetricFamily, labels map[string]string) (float64, bool)

// GetHistogramQuantile extracts a quantile from a histogram metric.
func GetHistogramQuantile(mf *MetricFamily, labels map[string]string, quantile float64) (float64, bool)

// GetCounterValue extracts the counter value from a metric with matching labels.
func GetCounterValue(mf *MetricFamily, labels map[string]string) (float64, bool)
```

- [ ] **Step 2: Write tests**

Test with sample Prometheus text output. Verify gauge, counter, histogram extraction.

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/internal/prom/parser.go cmd/wpcli/internal/prom/parser_test.go
git commit -m "feat(wpcli): add Prometheus text format scrape parser"
```

---

### Task 40: `wp metrics` parent + `metrics list`

**Goal:** Create the `metrics` command family and implement `wp metrics list <node>` which lists all metric series exposed by a node.

**Files:**
- Create: `cmd/wpcli/cmd/metrics.go`
- Create: `cmd/wpcli/cmd/metrics_list.go`
- Create: `cmd/wpcli/cmd/metrics_test.go`
- Modify: `cmd/wpcli/cmd/root.go`

**Spec reference:** §2.D.1 — metrics list.

- [ ] **Step 1: Create parent + register**

- [ ] **Step 2: Implement `metrics list`**

- Scrape `GET /metrics` from target node
- Parse with prom parser
- Table columns: `NAME  TYPE  HELP  SERIES_COUNT`
- Flags: `--filter` (substring match on metric name)
- JSON/YAML: structured metric family list

- [ ] **Step 3: Write tests**

- [ ] **Step 4: Commit**

```bash
git add cmd/wpcli/cmd/metrics.go cmd/wpcli/cmd/metrics_list.go cmd/wpcli/cmd/metrics_test.go cmd/wpcli/cmd/root.go
git commit -m "feat(wpcli): add metrics list command"
```

---

### Task 41: `wp metrics snapshot`

**Goal:** Point-in-time snapshot of metrics from one or all nodes.

**Files:**
- Create: `cmd/wpcli/cmd/metrics_snapshot.go`
- Modify: `cmd/wpcli/cmd/metrics_test.go`

**Spec reference:** §2.D.2 — metrics snapshot.

- [ ] **Step 1: Implement**

- Scrape `GET /metrics` (single node or `--all` fan-out)
- Parse and display selected metrics with current values
- Flags: `--metric` (specific metric name, can repeat), `--all` (fan-out)
- Table columns: `NODE  METRIC  LABELS  VALUE`
- JSON: full parsed metric families

- [ ] **Step 2: Write tests**

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/metrics_snapshot.go cmd/wpcli/cmd/metrics_test.go
git commit -m "feat(wpcli): add metrics snapshot command"
```

---

### Task 42: `wp metrics top`

**Goal:** Cross-node comparison of a specific metric — show top-N nodes by metric value.

**Files:**
- Create: `cmd/wpcli/cmd/metrics_top.go`
- Modify: `cmd/wpcli/cmd/metrics_test.go`

**Spec reference:** §2.D.3 — metrics top.

- [ ] **Step 1: Implement**

- Required: `--by <metric_name>`
- Fan-out scrape to all nodes
- Client-side: extract metric value per node, sort descending
- Table columns: `RANK  NODE  VALUE  LABELS`
- Flags: `--top N` (default 10), `--labels` (label filter)

- [ ] **Step 2: Write tests**

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/metrics_top.go cmd/wpcli/cmd/metrics_test.go
git commit -m "feat(wpcli): add metrics top command for cross-node comparison"
```

---

### Task 43: `wp metrics watch`

**Goal:** Real-time streaming display of a metric with periodic scrape and trend arrows.

**Files:**
- Create: `cmd/wpcli/cmd/metrics_watch.go`
- Modify: `cmd/wpcli/cmd/metrics_test.go`

**Spec reference:** §2.D.4 — metrics watch.

- [ ] **Step 1: Implement**

- Required: positional arg `<metric_name>`
- Periodic scrape of `GET /metrics` (default interval 1s, configurable with `--interval`)
- Print one line per scrape: `TIMESTAMP  NODE  VALUE  DELTA  TREND`
- Trend arrows: `↑` (increasing), `↓` (decreasing), `→` (stable)
- Ctrl+C to exit (exit 130)
- Flags: `--interval`, `--node` (single node, default first discovered), `--labels`

- [ ] **Step 2: Write tests**

Test the metric comparison logic (delta, trend arrow). Use a cancellable context for the watch loop test.

- [ ] **Step 3: Commit**

```bash
git add cmd/wpcli/cmd/metrics_watch.go cmd/wpcli/cmd/metrics_test.go
git commit -m "feat(wpcli): add metrics watch command with trend arrows"
```

---

### Task 44: Scenario YAML engine + `wp metrics report` + 12 built-in scenarios

**Goal:** Implement the scenario-based metrics analysis engine and the `wp metrics report` command. This is the most complex D family command — it scrapes metrics over a window period, applies YAML-defined rules, and produces findings with severity levels and recommended follow-up commands.

**Files:**
- Create: `cmd/wpcli/internal/scenarios/engine.go`
- Create: `cmd/wpcli/internal/scenarios/engine_test.go`
- Create: `cmd/wpcli/internal/scenarios/types.go`
- Create: 12 embedded YAML scenario files in `cmd/wpcli/internal/scenarios/builtin/`
- Create: `cmd/wpcli/cmd/metrics_report.go`
- Modify: `cmd/wpcli/cmd/metrics_test.go`

**Spec reference:** §2.D.5 — metrics report, scenario YAML structure.

- [ ] **Step 1: Define scenario types**

Create `cmd/wpcli/internal/scenarios/types.go`:

```go
package scenarios

// Scenario defines a YAML-based diagnostic scenario.
type Scenario struct {
	Name        string       `yaml:"name"`
	Description string       `yaml:"description"`
	Metrics     []MetricRef  `yaml:"metrics"`
	Rules       []Rule       `yaml:"rules"`
}

type MetricRef struct {
	Name     string  `yaml:"name"`
	Type     string  `yaml:"type"` // "gauge" | "histogram"
	Quantile float64 `yaml:"quantile,omitempty"`
}

type Rule struct {
	ID        string   `yaml:"id"`
	Severity  string   `yaml:"severity"` // "red" | "yellow"
	Condition string   `yaml:"condition"`
	Hints     []string `yaml:"hints"`
}

// Finding is the result of evaluating a rule against collected data.
type Finding struct {
	RuleID   string
	Severity string // "red" | "yellow"
	Message  string
	Hints    []string
}
```

- [ ] **Step 2: Implement scenario engine**

`engine.go`:
1. Load scenario from embedded YAML (via `embed.FS`)
2. Collect metrics over a time window (default 1 minute) with periodic scrapes
3. Evaluate rules against the collected samples
4. Produce `[]Finding` sorted by severity (red first)

The condition DSL is kept simple for Phase 2:
- Threshold comparisons: `metric > value`
- Sustained conditions: `for at least N% of window samples`
- Rate comparisons: `rate > value`

- [ ] **Step 3: Create 12 built-in scenario YAMLs**

Embed via `//go:embed builtin/*.yaml`:

1. `hot-write.yaml` — cross-node append rate imbalance
2. `slow-write.yaml` — append p99 deviation
3. `stuck-write.yaml` — append throughput near zero
4. `slow-slot.yaml` — quorum member latency drag
5. `stuck-flush.yaml` — flush queue high + p99 spike
6. `slow-compact.yaml` — compact p99 deviation
7. `fencing.yaml` — fence events exceed threshold
8. `slow-read.yaml` — read p99 deviation
9. `stuck-read.yaml` — read throughput near zero
10. `read-amplification.yaml` — block/entry read ratio anomalous
11. `quorum-degraded.yaml` — active replicas < ensemble
12. `under-replication.yaml` — sustained under-replication

- [ ] **Step 4: Implement `wp metrics report`**

`metrics_report.go`:
- Required: `--scenario <name>`
- Flags: `--window` (default 1m), `--interval` (default 1s), `--node` or `--all`
- Execution: scrape → collect → evaluate → print findings
- Output: severity-sorted findings with hints
- Exit codes: 0 (all green), 8 (yellow finding), 9 (red finding)

- [ ] **Step 5: Write tests**

Test engine with a mock scraper and a simple scenario YAML. Verify findings are produced correctly.

- [ ] **Step 6: Commit**

```bash
git add cmd/wpcli/internal/scenarios/ cmd/wpcli/cmd/metrics_report.go cmd/wpcli/cmd/metrics_test.go
git commit -m "feat(wpcli): add metrics report command with scenario YAML engine and 12 built-in scenarios"
```

---

## Part L — Verification (Task 45)

### Task 45: Phase 2 verification sweep

**Goal:** Final sanity check — run everything, confirm nothing regressed, produce a clean `bin/wp` with all Phase 2 commands.

**Files:** None (verification only)

- [ ] **Step 1: Run the full test suite**

```bash
go test ./... -race -count=1
```

Expected: PASS. All existing tests + all Phase 2 tests green.

- [ ] **Step 2: Build and verify**

```bash
make wpcli
./bin/wp version
./bin/wp help
```

Expected: help lists all Phase 1 + Phase 2 subcommands: `node`, `cluster`, `config`, `env`, `profile`, `ctx`, `logging`, `logstore`, `ops`, `metrics`.

- [ ] **Step 3: Command count verification**

Count all subcommands — expected 31 total (14 Phase 1 + 17 Phase 2):

Phase 1 (14): node list/show/decommission/drain-status/cancel-decommission/restart, cluster info/health/gossip-diff, config show/diff, env show/diff, profile

Phase 2 (17): logging get-level/set-level, logstore segments/segment-show/buffer/flush-queue/force-flush/fence/compact, ops list/show/stats, metrics list/snapshot/top/watch/report

- [ ] **Step 4: Spec coverage check**

Open `docs/wpcli-design.md` §5.4 and verify every command, every server endpoint, and every package listed there has a corresponding completed task above. Any gap is a plan failure — add a task and re-execute.

- [ ] **Step 5: Op registry verification (optional, docker-compose)**

On a docker-compose cluster:
```bash
./bin/wp ops list <node>
./bin/wp ops stats <node>
./bin/wp logstore segments <node>
./bin/wp logging get-level <node>
./bin/wp metrics list <node>
```

Expected: All commands produce output without error.

---

## Self-Review Checklist

- [ ] **Spec coverage:** every command in §2.C / §2.D / §2.E.3-E.4 / §2.G has a completed task
- [ ] **Server endpoint coverage:** all 10 new endpoints from §6.3 Phase 2 rows have a completed task
- [ ] **Exit code coverage:** every exit code a Phase 2 command can emit has at least one unit test
- [ ] **No placeholders:** grep this plan for TODO / TBD — none should remain
- [ ] **Type consistency:** function / method / struct names are identical across tasks
- [ ] **Commit granularity:** each task ends in exactly one commit
- [ ] **Metric regression:** all 12 migration sites have regression tests proving histogram behavior is preserved
- [ ] **opregistry coverage:** unit tests ≥ 90% line coverage on `common/runtime/opregistry/`

---

## Execution Handoff

Plan complete and saved to `docs/wpcli-plan-phase2.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks
2. **Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
