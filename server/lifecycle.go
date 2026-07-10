// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

const nodeStateFileName = "node_state.json"

// NodeState represents the lifecycle state of a node.
type NodeState string

const (
	NodeStateActive          NodeState = "active"
	NodeStateDecommissioning NodeState = "decommissioning"
	NodeStateDecommissioned  NodeState = "decommissioned"
)

// DecommissionProgress reports the current decommission status.
type DecommissionProgress struct {
	State               string `json:"state"`
	RemainingProcessors int    `json:"remaining_processors"`
	HasLocalData        bool   `json:"has_local_data"`
	SafeToTerminate     bool   `json:"safe_to_terminate"`
}

// nodeStatePersisted is the JSON structure written to node_state.json.
type nodeStatePersisted struct {
	State     NodeState `json:"state"`
	Timestamp int64     `json:"timestamp"`
}

// NodeLifecycleManager tracks the lifecycle state of a node for decommission operations.
// State is persisted to a local file so it survives node restarts.
type NodeLifecycleManager struct {
	mu            sync.RWMutex
	state         NodeState
	stateFilePath string // empty means no persistence (for testing)
}

// NewNodeLifecycleManager creates a new lifecycle manager in the active state
// without file persistence (for testing or when no data dir is available).
func NewNodeLifecycleManager() *NodeLifecycleManager {
	return &NodeLifecycleManager{
		state: NodeStateActive,
	}
}

// NewNodeLifecycleManagerWithPersistence creates a lifecycle manager that persists
// state to <dataDir>/node_state.json. On creation it loads any previously persisted
// state, so a node that was decommissioning before a restart will resume in that state.
func NewNodeLifecycleManagerWithPersistence(dataDir string) (*NodeLifecycleManager, error) {
	m := &NodeLifecycleManager{
		state:         NodeStateActive,
		stateFilePath: filepath.Join(dataDir, nodeStateFileName),
	}
	if err := m.loadState(); err != nil {
		return nil, fmt.Errorf("failed to load node lifecycle state: %w", err)
	}
	return m, nil
}

// GetState returns the current node state.
func (m *NodeLifecycleManager) GetState() NodeState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// IsDecommissioning returns true if the node is in the decommissioning state.
func (m *NodeLifecycleManager) IsDecommissioning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state == NodeStateDecommissioning
}

// StartDecommission transitions the node from active to decommissioning.
// Idempotent if already decommissioning. Returns error if already decommissioned.
func (m *NodeLifecycleManager) StartDecommission() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.state {
	case NodeStateActive:
		m.state = NodeStateDecommissioning
		err := m.persistStateLocked()
		m.logTransitionLocked("start decommission", NodeStateActive, err)
		return err
	case NodeStateDecommissioning:
		logger.Ctx(context.Background()).Info("start decommission: already decommissioning, no-op")
		return nil // idempotent
	case NodeStateDecommissioned:
		logger.Ctx(context.Background()).Warn("start decommission rejected: node already decommissioned")
		return fmt.Errorf("node already decommissioned")
	}
	return fmt.Errorf("unknown state: %s", m.state)
}

// ErrNotDecommissioning is returned by MarkDecommissioned when the node is not
// in the decommissioning state — typically because the decommission was
// cancelled after the monitor's last state check.
var ErrNotDecommissioning = errors.New("node is not decommissioning")

// MarkDecommissioned transitions the node from decommissioning to decommissioned.
// Only the decommissioning → decommissioned transition is allowed: a cancelled
// (active) node must never be moved to the terminal decommissioned state by a
// stale monitor. Idempotent if already decommissioned. Returns an error if
// persisting the state fails (the in-memory state is rolled back so the caller
// can retry).
func (m *NodeLifecycleManager) MarkDecommissioned() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.state {
	case NodeStateDecommissioning:
		m.state = NodeStateDecommissioned
		err := m.persistStateLocked()
		m.logTransitionLocked("mark decommissioned", NodeStateDecommissioning, err)
		if err != nil {
			// Roll back so the monitor retries the transition (and its persistence).
			m.state = NodeStateDecommissioning
		}
		return err
	case NodeStateDecommissioned:
		return nil // idempotent
	default:
		logger.Ctx(context.Background()).Warn("mark decommissioned rejected: node is not decommissioning",
			zap.String("state", string(m.state)))
		return ErrNotDecommissioning
	}
}

// CancelDecommission transitions the node from decommissioning back to active.
// Semantics:
//   - decommissioning → active: transition, persist state
//   - active → active: idempotent, returns nil
//   - decommissioned → error: cannot un-retire a fully decommissioned node
func (m *NodeLifecycleManager) CancelDecommission() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.state {
	case NodeStateActive:
		logger.Ctx(context.Background()).Info("cancel decommission: already active, no-op")
		return nil // idempotent
	case NodeStateDecommissioning:
		m.state = NodeStateActive
		err := m.persistStateLocked()
		m.logTransitionLocked("cancel decommission", NodeStateDecommissioning, err)
		return err
	case NodeStateDecommissioned:
		logger.Ctx(context.Background()).Warn("cancel decommission rejected: node already decommissioned")
		return fmt.Errorf("cannot cancel: node already decommissioned")
	}
	return fmt.Errorf("unknown state: %s", m.state)
}

// ClearState resets the node to active and removes the persisted state file.
// Used when a decommissioned node is re-commissioned or the state should be cleared.
func (m *NodeLifecycleManager) ClearState() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	from := m.state
	m.state = NodeStateActive
	if m.stateFilePath == "" {
		return nil
	}
	err := os.Remove(m.stateFilePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove state file: %w", err)
	}
	logger.Ctx(context.Background()).Info("node lifecycle state cleared",
		zap.String("from", string(from)),
		zap.String("stateFile", m.stateFilePath))
	return nil
}

// GetProgress returns the current decommission progress. The safe_to_terminate
// decision is based solely on whether local segment data files remain on disk.
// Processor count is reported for observability but does not affect the decision —
// processors are in-memory objects that disappear on shutdown.
func (m *NodeLifecycleManager) GetProgress(remainingProcessors int, hasLocalData bool) DecommissionProgress {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return DecommissionProgress{
		State:               string(m.state),
		RemainingProcessors: remainingProcessors,
		HasLocalData:        hasLocalData,
		SafeToTerminate:     !hasLocalData,
	}
}

// loadState reads persisted state from the state file. If the file does not
// exist, the manager stays in the active state (fresh node / first boot).
func (m *NodeLifecycleManager) loadState() error {
	if m.stateFilePath == "" {
		return nil
	}
	data, err := os.ReadFile(m.stateFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no persisted state, start as active
		}
		return err
	}
	var persisted nodeStatePersisted
	if err := json.Unmarshal(data, &persisted); err != nil {
		return fmt.Errorf("corrupt state file %s: %w", m.stateFilePath, err)
	}
	switch persisted.State {
	case NodeStateActive, NodeStateDecommissioning, NodeStateDecommissioned:
		m.state = persisted.State
	default:
		return fmt.Errorf("unknown state %q in %s", persisted.State, m.stateFilePath)
	}
	if m.state != NodeStateActive {
		logger.Ctx(context.Background()).Info("restored persisted node lifecycle state",
			zap.String("state", string(m.state)),
			zap.Int64("persistedAtMs", persisted.Timestamp),
			zap.String("stateFile", m.stateFilePath))
	}
	return nil
}

// logTransitionLocked emits the audit line for a real state transition.
// Must be called while holding m.mu, after persistStateLocked.
func (m *NodeLifecycleManager) logTransitionLocked(action string, from NodeState, persistErr error) {
	fields := []zap.Field{
		zap.String("from", string(from)),
		zap.String("to", string(m.state)),
	}
	if persistErr != nil {
		logger.Ctx(context.Background()).Warn("node lifecycle transition failed to persist: "+action,
			append(fields, zap.Error(persistErr))...)
		return
	}
	logger.Ctx(context.Background()).Info("node lifecycle transition: "+action, fields...)
}

// persistStateLocked writes the current state to the state file.
// Must be called while holding m.mu.
func (m *NodeLifecycleManager) persistStateLocked() error {
	if m.stateFilePath == "" {
		return nil
	}
	persisted := nodeStatePersisted{
		State:     m.state,
		Timestamp: time.Now().UnixMilli(),
	}
	data, err := json.Marshal(persisted)
	if err != nil {
		return err
	}
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(m.stateFilePath), 0o755); err != nil {
		return fmt.Errorf("failed to create state file directory: %w", err)
	}
	// Write to temp file then rename for atomicity
	tmpPath := m.stateFilePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}
	if err := os.Rename(tmpPath, m.stateFilePath); err != nil {
		return fmt.Errorf("failed to rename state file: %w", err)
	}
	return nil
}
