# Node Lifecycle & Rolling Upgrade APIs Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose HTTP admin APIs for node status, decommission, and decommission progress to enable automated K8s lifecycle management (rolling upgrades, scale-up/down).

**Architecture:** Add three new HTTP admin endpoints to the existing `common/http` server (port 9091). Introduce a `NodeLifecycleManager` component in `server/` that tracks decommission state and active segment processors. The manager is owned by the `Server` struct and queried by HTTP handlers via a callback pattern (same as the existing `GetServerNodeMemberlistStatus` callback). Go 1.22+ `http.ServeMux` supports path parameters natively, but since these endpoints target the *local* node (the one receiving the HTTP request), we simplify to `/admin/node/status`, `/admin/node/decommission`, and `/admin/node/decommission/progress` — no `{node_id}` path param needed (the external orchestrator calls each pod's admin port directly).

**Tech Stack:** Go 1.24, standard `net/http`, Hashicorp Memberlist (existing), stretchr/testify (existing)

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `server/lifecycle.go` | `NodeLifecycleManager` — decommission state machine, progress tracking |
| Create | `server/lifecycle_test.go` | Unit tests for lifecycle manager |
| Create | `common/http/management/node_handler.go` | HTTP handlers for the 3 new admin endpoints |
| Create | `common/http/management/node_handler_test.go` | Unit tests for HTTP handlers |
| Modify | `common/http/router.go` | Add route path constants for new endpoints |
| Modify | `common/http/server.go` | Register new admin handlers on startup |
| Modify | `server/service.go` | Integrate `NodeLifecycleManager` into `Server`, wire decommission into `Stop()` |
| Modify | `server/logstore.go` | Add `GetActiveProcessorCount()` and `RejectNewWrites()` methods to `LogStore` interface (uses separate `rejectWrites` flag, not `stopped`) |

---

## Chunk 1: Node Lifecycle Manager (Core Logic)

### Task 1: Define NodeLifecycleManager and its interface

**Files:**
- Create: `server/lifecycle.go`
- Create: `server/lifecycle_test.go`

- [x] **Step 1: Write the failing test for lifecycle manager creation and initial state**

```go
// server/lifecycle_test.go
package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewNodeLifecycleManager(t *testing.T) {
	m := NewNodeLifecycleManager()
	assert.NotNil(t, m)
	assert.Equal(t, NodeStateActive, m.GetState())
	assert.False(t, m.IsDecommissioning())
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./server/ -run TestNewNodeLifecycleManager -v -count=1`
Expected: FAIL — `NewNodeLifecycleManager` not defined

- [x] **Step 3: Write minimal implementation**

```go
// server/lifecycle.go
package server

import "sync"

type NodeState string

const (
	NodeStateActive          NodeState = "active"
	NodeStateDecommissioning NodeState = "decommissioning"
	NodeStateDecommissioned  NodeState = "decommissioned"
)

type NodeLifecycleManager struct {
	mu    sync.RWMutex
	state NodeState
}

func NewNodeLifecycleManager() *NodeLifecycleManager {
	return &NodeLifecycleManager{
		state: NodeStateActive,
	}
}

func (m *NodeLifecycleManager) GetState() NodeState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

func (m *NodeLifecycleManager) IsDecommissioning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state == NodeStateDecommissioning
}
```

- [x] **Step 4: Run test to verify it passes**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./server/ -run TestNewNodeLifecycleManager -v -count=1`
Expected: PASS

- [x] **Step 5: Commit**

```bash
git add server/lifecycle.go server/lifecycle_test.go
git commit -m "feat(lifecycle): add NodeLifecycleManager with initial state tracking"
```

---

### Task 2: Add Decommission and Progress methods

**Files:**
- Modify: `server/lifecycle.go`
- Modify: `server/lifecycle_test.go`

- [x] **Step 1: Write the failing test for decommission transitions**

```go
// Append to server/lifecycle_test.go

func TestNodeLifecycleManager_Decommission(t *testing.T) {
	m := NewNodeLifecycleManager()

	// First decommission should succeed
	err := m.StartDecommission()
	assert.NoError(t, err)
	assert.Equal(t, NodeStateDecommissioning, m.GetState())
	assert.True(t, m.IsDecommissioning())

	// Second decommission should be idempotent (no error)
	err = m.StartDecommission()
	assert.NoError(t, err)
}

func TestNodeLifecycleManager_DecommissionAlreadyDone(t *testing.T) {
	m := NewNodeLifecycleManager()
	m.StartDecommission()
	m.MarkDecommissioned()
	assert.Equal(t, NodeStateDecommissioned, m.GetState())

	// Cannot decommission an already decommissioned node
	err := m.StartDecommission()
	assert.Error(t, err)
}

func TestNodeLifecycleManager_Progress(t *testing.T) {
	m := NewNodeLifecycleManager()
	m.StartDecommission()

	progress := m.GetProgress(10)
	assert.Equal(t, NodeStateDecommissioning, NodeState(progress.State))
	assert.Equal(t, 10, progress.RemainingProcessors)
	assert.False(t, progress.SafeToTerminate)

	progress = m.GetProgress(0)
	assert.Equal(t, 0, progress.RemainingProcessors)
	assert.True(t, progress.SafeToTerminate)
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./server/ -run "TestNodeLifecycleManager_Decommission|TestNodeLifecycleManager_Progress" -v -count=1`
Expected: FAIL — `StartDecommission`, `MarkDecommissioned`, `GetProgress` not defined

- [x] **Step 3: Write minimal implementation**

Add to `server/lifecycle.go`:

```go
import (
	"fmt"
	"sync"
)

type DecommissionProgress struct {
	State               string `json:"state"`
	RemainingProcessors int    `json:"remaining_processors"`
	SafeToTerminate     bool   `json:"safe_to_terminate"`
}

func (m *NodeLifecycleManager) StartDecommission() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.state {
	case NodeStateActive:
		m.state = NodeStateDecommissioning
		return nil
	case NodeStateDecommissioning:
		return nil // idempotent
	case NodeStateDecommissioned:
		return fmt.Errorf("node already decommissioned")
	}
	return fmt.Errorf("unknown state: %s", m.state)
}

func (m *NodeLifecycleManager) MarkDecommissioned() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = NodeStateDecommissioned
}

func (m *NodeLifecycleManager) GetProgress(remainingProcessors int) DecommissionProgress {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return DecommissionProgress{
		State:               string(m.state),
		RemainingProcessors: remainingProcessors,
		SafeToTerminate:     remainingProcessors == 0,
	}
}
```

- [x] **Step 4: Run test to verify it passes**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./server/ -run "TestNodeLifecycleManager_Decommission|TestNodeLifecycleManager_Progress" -v -count=1`
Expected: PASS

- [x] **Step 5: Commit**

```bash
git add server/lifecycle.go server/lifecycle_test.go
git commit -m "feat(lifecycle): add decommission state transitions and progress tracking"
```

---

## Chunk 2: LogStore Interface Extension + Server Integration

### Task 3: Add GetActiveProcessorCount and RejectNewWrites to LogStore

> **IMPORTANT:** `RejectNewWrites()` must NOT reuse the existing `stopped` atomic.Bool field. The `stopped` flag is checked by ALL operations (reads, fence, complete, compact, etc.). Setting it would block reads and segment completions, preventing task draining during decommission. Instead, add a separate `rejectWrites atomic.Bool` field that is only checked in `AddEntry()`.

**Files:**
- Modify: `server/logstore.go` — add `rejectWrites` field, `GetActiveProcessorCount()` and `RejectNewWrites()` methods

- [x] **Step 1: Write the failing tests for GetActiveProcessorCount and RejectNewWrites**

```go
// Append to server/logstore_test.go

func TestLogStore_GetActiveProcessorCount(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)
	count := ls.GetActiveProcessorCount()
	assert.Equal(t, 0, count)
}

func TestLogStore_RejectNewWrites(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)
	concrete := ls.(*logStore)

	// Before reject: rejectWrites should be false
	assert.False(t, concrete.rejectWrites.Load())

	ls.RejectNewWrites()

	// After reject: rejectWrites should be true, but stopped should still be true
	// (stopped is true by default until Start() is called)
	assert.True(t, concrete.rejectWrites.Load())
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./server/ -run "TestLogStore_GetActiveProcessorCount|TestLogStore_RejectNewWrites" -v -count=1`
Expected: FAIL — `GetActiveProcessorCount`, `RejectNewWrites`, `rejectWrites` not defined

- [x] **Step 3: Write minimal implementation**

Add `rejectWrites` field to `logStore` struct in `server/logstore.go`:

```go
type logStore struct {
	// ... existing fields ...
	stopped      atomic.Bool
	rejectWrites atomic.Bool // separate from stopped: only blocks new writes, not reads
}
```

Add to `LogStore` interface:

```go
GetActiveProcessorCount() int
RejectNewWrites()
```

Add implementations:

```go
func (l *logStore) GetActiveProcessorCount() int {
	l.spMu.RLock()
	defer l.spMu.RUnlock()
	return l.getTotalProcessorCountUnsafe()
}

func (l *logStore) RejectNewWrites() {
	l.rejectWrites.Store(true)
}
```

Update `AddEntry()` to also check `rejectWrites`:

```go
func (l *logStore) AddEntry(...) (int64, error) {
	if l.stopped.Load() || l.rejectWrites.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	// ... rest unchanged ...
}
```

- [x] **Step 4: Run test to verify it passes**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./server/ -run "TestLogStore_GetActiveProcessorCount|TestLogStore_RejectNewWrites" -v -count=1`
Expected: PASS

- [x] **Step 5: Verify existing tests still pass**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./server/ -v -count=1 -timeout 120s`
Expected: All existing tests PASS.

- [x] **Step 6: Regenerate mock implementations (REQUIRED)**

The mock at `mocks/mocks_server/mock_logstore.go` will fail to compile with the new interface methods. Regenerate:

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go generate ./server/...`

If `go generate` doesn't work, manually add the two methods to `mocks/mocks_server/mock_logstore.go`. Verify with:

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go build ./...`

- [x] **Step 7: Commit**

```bash
git add server/logstore.go server/logstore_test.go mocks/
git commit -m "feat(logstore): add GetActiveProcessorCount and RejectNewWrites with separate flag"
```

---

### Task 4: Integrate NodeLifecycleManager into Server

> **Depends on:** Task 3 must be fully completed (LogStore interface updated, mocks regenerated) before starting this task.

**Files:**
- Modify: `server/service.go` — add `lifecycle *NodeLifecycleManager` field, expose query methods

- [x] **Step 1: Write the failing test for server lifecycle integration**

```go
// Append to server/service_test.go

func TestServer_NodeLifecycle(t *testing.T) {
	ctx := context.Background()
	serverConfig := &membership.ServerConfig{
		NodeID:        "test-lifecycle",
		BindPort:      0,
		ServicePort:   0,
		ResourceGroup: "default",
		AZ:            "default",
		Tags:          map[string]string{"role": "logstore"},
	}
	srv := createTestServer(ctx, serverConfig)

	// Server should have lifecycle manager
	status := srv.GetNodeStatus()
	assert.Equal(t, "active", status.State)
	assert.False(t, status.IsDecommissioning)

	// Decommission
	err := srv.Decommission()
	assert.NoError(t, err)

	status = srv.GetNodeStatus()
	assert.Equal(t, "decommissioning", status.State)
	assert.True(t, status.IsDecommissioning)

	// Get progress
	progress := srv.GetDecommissionProgress()
	assert.Equal(t, "decommissioning", progress.State)
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./server/ -run TestServer_NodeLifecycle -v -count=1`
Expected: FAIL

- [x] **Step 3: Write implementation — add lifecycle to Server struct**

In `server/service.go`, add to `Server` struct:

```go
lifecycle *NodeLifecycleManager
```

In `NewServerWithConfig()`, initialize it:

```go
s.lifecycle = NewNodeLifecycleManager()
```

Also in `createTestServer()` in `service_test.go`, add:

```go
lifecycle: NewNodeLifecycleManager(),
```

Add the following exported methods to `Server`:

```go
type NodeStatus struct {
	NodeID            string            `json:"node_id"`
	State             string            `json:"state"`
	IsDecommissioning bool              `json:"is_decommissioning"`
	MemberCount       int               `json:"member_count"`
	Address           string            `json:"address"`
	ResourceGroup     string            `json:"resource_group"`
	AZ                string            `json:"az"`
	Tags              map[string]string `json:"tags"`
}

func (s *Server) GetNodeStatus() NodeStatus {
	return NodeStatus{
		NodeID:            s.serverConfig.NodeID,
		State:             string(s.lifecycle.GetState()),
		IsDecommissioning: s.lifecycle.IsDecommissioning(),
		MemberCount:       s.GetMemberCount(),
		Address:           s.logStore.GetAddress(),
		ResourceGroup:     s.serverConfig.ResourceGroup,
		AZ:                s.serverConfig.AZ,
		Tags:              s.serverConfig.Tags,
	}
}

func (s *Server) Decommission() error {
	if err := s.lifecycle.StartDecommission(); err != nil {
		return err
	}
	// Stop accepting new writes
	s.logStore.RejectNewWrites()
	return nil
}

func (s *Server) GetDecommissionProgress() DecommissionProgress {
	remaining := s.logStore.GetActiveProcessorCount()
	return s.lifecycle.GetProgress(remaining)
}
```

- [x] **Step 4: Run test to verify it passes**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./server/ -run TestServer_NodeLifecycle -v -count=1`
Expected: PASS

- [x] **Step 5: Commit**

```bash
git add server/service.go server/service_test.go server/lifecycle.go
git commit -m "feat(server): integrate NodeLifecycleManager into Server"
```

---

## Chunk 3: HTTP Admin Handlers

### Task 5: Add route constants for new admin endpoints

**Files:**
- Modify: `common/http/router.go`

- [x] **Step 1: Add the three new route constants**

Append to `common/http/router.go`:

```go
// AdminNodeStatusPath is path for node status API.
const AdminNodeStatusPath = "/admin/node/status"

// AdminNodeDecommissionPath is path for node decommission API.
const AdminNodeDecommissionPath = "/admin/node/decommission"

// AdminNodeDecommissionProgressPath is path for node decommission progress API.
const AdminNodeDecommissionProgressPath = "/admin/node/decommission/progress"
```

- [x] **Step 2: Verify compilation**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go build ./common/http/...`
Expected: Build succeeds

- [x] **Step 3: Commit**

```bash
git add common/http/router.go
git commit -m "feat(http): add route constants for node lifecycle admin endpoints"
```

---

### Task 6: Implement HTTP handlers for node lifecycle endpoints

**Files:**
- Create: `common/http/management/node_handler.go`
- Create: `common/http/management/node_handler_test.go`

- [x] **Step 1: Write the failing tests for all three handlers**

```go
// common/http/management/node_handler_test.go
package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Minimal stubs matching the callback signatures
type mockNodeStatus struct {
	NodeID string `json:"node_id"`
	State  string `json:"state"`
}

func TestNodeStatusHandler(t *testing.T) {
	handler := NewNodeStatusHandler(func() interface{} {
		return mockNodeStatus{NodeID: "node-1", State: "active"}
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/node/status", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")

	var result mockNodeStatus
	err := json.Unmarshal(w.Body.Bytes(), &result)
	require.NoError(t, err)
	assert.Equal(t, "node-1", result.NodeID)
	assert.Equal(t, "active", result.State)
}

func TestNodeStatusHandler_MethodNotAllowed(t *testing.T) {
	handler := NewNodeStatusHandler(func() interface{} {
		return mockNodeStatus{}
	})

	req := httptest.NewRequest(http.MethodPost, "/admin/node/status", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestNodeDecommissionHandler(t *testing.T) {
	called := false
	handler := NewNodeDecommissionHandler(func() error {
		called = true
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/admin/node/decommission", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.True(t, called)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestNodeDecommissionHandler_Error(t *testing.T) {
	handler := NewNodeDecommissionHandler(func() error {
		return assert.AnError
	})

	req := httptest.NewRequest(http.MethodPost, "/admin/node/decommission", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestNodeDecommissionHandler_MethodNotAllowed(t *testing.T) {
	handler := NewNodeDecommissionHandler(func() error { return nil })

	req := httptest.NewRequest(http.MethodGet, "/admin/node/decommission", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

type mockProgress struct {
	State           string `json:"state"`
	SafeToTerminate bool   `json:"safe_to_terminate"`
}

func TestNodeDecommissionProgressHandler(t *testing.T) {
	handler := NewNodeDecommissionProgressHandler(func() interface{} {
		return mockProgress{State: "decommissioning", SafeToTerminate: false}
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/node/decommission/progress", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var result mockProgress
	err := json.Unmarshal(w.Body.Bytes(), &result)
	require.NoError(t, err)
	assert.Equal(t, "decommissioning", result.State)
	assert.False(t, result.SafeToTerminate)
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./common/http/management/ -v -count=1`
Expected: FAIL — handler functions not defined

- [x] **Step 3: Write the handler implementations**

```go
// common/http/management/node_handler.go
package management

import (
	"encoding/json"
	"net/http"
)

// NewNodeStatusHandler returns an http.HandlerFunc for GET /admin/node/status.
// getStatus is a callback that returns the current node status as a JSON-serializable value.
func NewNodeStatusHandler(getStatus func() interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(getStatus())
	}
}

// NewNodeDecommissionHandler returns an http.HandlerFunc for POST /admin/node/decommission.
// decommission is a callback that triggers the decommission process.
func NewNodeDecommissionHandler(decommission func() error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := decommission(); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "decommission started"})
	}
}

// NewNodeDecommissionProgressHandler returns an http.HandlerFunc for GET /admin/node/decommission/progress.
// getProgress is a callback that returns the decommission progress as a JSON-serializable value.
func NewNodeDecommissionProgressHandler(getProgress func() interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(getProgress())
	}
}
```

- [x] **Step 4: Run test to verify it passes**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./common/http/management/ -v -count=1`
Expected: PASS

- [x] **Step 5: Commit**

```bash
git add common/http/management/node_handler.go common/http/management/node_handler_test.go
git commit -m "feat(admin): add HTTP handlers for node status, decommission, and progress"
```

---

## Chunk 4: Wiring — Register Handlers and End-to-End Integration

### Task 7: Update HTTP server to accept and register node lifecycle handlers

**Files:**
- Modify: `common/http/server.go` — extend `Start()` signature to accept lifecycle callbacks
- Modify: `common/http/server_test.go` — update existing tests for new signature

- [x] **Step 0: Verify all callers of `Start()` before changing signature**

Run: `grep -rn 'commonhttp\.Start\|http\.Start(' --include='*.go' /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker/`

Identify all call sites. Expected: only `cmd/main.go` and `common/http/server_test.go`. If other callers exist, update them in this task too.

- [x] **Step 1: Update `Start()` function signature**

Change `common/http/server.go` `Start()` to accept a struct of admin callbacks:

```go
// AdminCallbacks holds callbacks for admin HTTP endpoints.
type AdminCallbacks struct {
	GetMemberlistStatus         func() string
	GetNodeStatus               func() interface{}
	Decommission                func() error
	GetDecommissionProgress     func() interface{}
}

func Start(cfg *config.Configuration, callbacks AdminCallbacks) error {
```

Replace the existing memberlist handler registration with:

```go
	// Register admin handler for memberlist status
	Register(&Handler{
		Path: AdminMemberlistPath,
		HandlerFunc: func(writer http.ResponseWriter, request *http.Request) {
			fmt.Fprint(writer, callbacks.GetMemberlistStatus())
		},
	})

	// Register node lifecycle admin handlers (if callbacks provided)
	if callbacks.GetNodeStatus != nil {
		Register(&Handler{
			Path:        AdminNodeStatusPath,
			HandlerFunc: management.NewNodeStatusHandler(callbacks.GetNodeStatus),
		})
	}
	if callbacks.Decommission != nil {
		Register(&Handler{
			Path:        AdminNodeDecommissionPath,
			HandlerFunc: management.NewNodeDecommissionHandler(callbacks.Decommission),
		})
	}
	if callbacks.GetDecommissionProgress != nil {
		Register(&Handler{
			Path:        AdminNodeDecommissionProgressPath,
			HandlerFunc: management.NewNodeDecommissionProgressHandler(callbacks.GetDecommissionProgress),
		})
	}
```

- [x] **Step 2: Update existing tests in `common/http/server_test.go`**

Change the `TestStartAndStop` test to use new signature:

```go
	err := Start(cfg, AdminCallbacks{
		GetMemberlistStatus: func() string { return "memberlist status" },
	})
```

- [x] **Step 3: Verify tests pass**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./common/http/ -v -count=1`
Expected: PASS

- [x] **Step 4: Commit**

```bash
git add common/http/server.go common/http/server_test.go
git commit -m "feat(http): extend Start() to register node lifecycle admin handlers"
```

---

### Task 8: Wire everything together in cmd/main.go

**Files:**
- Modify: `cmd/main.go` — pass lifecycle callbacks to `commonhttp.Start()`

- [x] **Step 1: Update the HTTP Start call in main.go**

Change the `commonhttp.Start()` call to:

```go
	if err := commonhttp.Start(cfg, commonhttp.AdminCallbacks{
		GetMemberlistStatus: srv.GetServerNodeMemberlistStatus,
		GetNodeStatus: func() interface{} {
			return srv.GetNodeStatus()
		},
		Decommission: func() error {
			return srv.Decommission()
		},
		GetDecommissionProgress: func() interface{} {
			return srv.GetDecommissionProgress()
		},
	}); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
```

- [x] **Step 2: Verify compilation**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go build ./cmd/...`
Expected: Build succeeds

- [x] **Step 3: Run all tests to confirm no regressions**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./server/ ./common/http/ ./common/http/management/ -v -count=1 -timeout 120s`
Expected: All PASS

- [x] **Step 4: Commit**

```bash
git add cmd/main.go
git commit -m "feat(cmd): wire node lifecycle admin APIs into HTTP server startup"
```

---

### Task 9: Add integration test for end-to-end HTTP lifecycle flow

> **Note:** This test uses port 19092 to avoid conflict with existing test on 19091. Both tests share package-level globals (`metricsServer`, `server`) reset via `resetGlobals()`, so they must NOT run in parallel. Go runs tests within a package sequentially by default, so this is safe.

**Files:**
- Modify: `common/http/server_test.go` — add integration test

- [x] **Step 1: Write integration test**

```go
func TestStartAndStop_WithLifecycleEndpoints(t *testing.T) {
	resetGlobals()
	t.Setenv(PprofEnableEnvKey, "false")
	t.Setenv(ListenPortEnvKey, "19092")

	cfg, _ := config.NewConfiguration()

	decommissioned := false
	err := Start(cfg, AdminCallbacks{
		GetMemberlistStatus: func() string { return "ok" },
		GetNodeStatus: func() interface{} {
			return map[string]string{"state": "active"}
		},
		Decommission: func() error {
			decommissioned = true
			return nil
		},
		GetDecommissionProgress: func() interface{} {
			return map[string]interface{}{"safe_to_terminate": decommissioned}
		},
	})
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Test GET /admin/node/status
	resp, httpErr := http.Get("http://127.0.0.1:19092" + AdminNodeStatusPath)
	if httpErr == nil {
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}

	// Test POST /admin/node/decommission
	resp, httpErr = http.Post("http://127.0.0.1:19092"+AdminNodeDecommissionPath, "", nil)
	if httpErr == nil {
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}
	assert.True(t, decommissioned)

	// Test GET /admin/node/decommission/progress
	resp, httpErr = http.Get("http://127.0.0.1:19092" + AdminNodeDecommissionProgressPath)
	if httpErr == nil {
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}

	err = Stop()
	assert.NoError(t, err)
}
```

- [x] **Step 2: Run test to verify it passes**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./common/http/ -run TestStartAndStop_WithLifecycleEndpoints -v -count=1`
Expected: PASS

- [x] **Step 3: Final full test suite run**

Run: `cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/tinswzy_repo/work1/woodpecker && go test ./... -count=1 -timeout 300s 2>&1 | tail -50`
Expected: All relevant packages PASS (some integration tests may be skipped if they require external services like etcd/minio)

- [x] **Step 4: Commit**

```bash
git add common/http/server_test.go
git commit -m "test(http): add integration test for node lifecycle admin endpoints"
```

---

## API Summary

After implementation, the following endpoints will be available on the admin HTTP port (default 9091):

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| GET | `/admin/node/status` | Node membership and state info | `{"node_id":"...","state":"active\|decommissioning\|decommissioned","member_count":4,"address":"...","resource_group":"...","az":"...","tags":{...}}` |
| POST | `/admin/node/decommission` | Start node decommission (stops accepting new writes) | `{"status":"decommission started"}` or `409 {"error":"..."}` |
| GET | `/admin/node/decommission/progress` | Check decommission progress | `{"state":"decommissioning","remaining_processors":5,"safe_to_terminate":false}` |

**K8s Integration Example (PreStop hook):**

```yaml
lifecycle:
  preStop:
    exec:
      command:
        - /bin/sh
        - -c
        - |
          curl -X POST http://localhost:9091/admin/node/decommission
          while true; do
            SAFE=$(curl -s http://localhost:9091/admin/node/decommission/progress | jq .safe_to_terminate)
            if [ "$SAFE" = "true" ]; then break; fi
            sleep 2
          done
```
