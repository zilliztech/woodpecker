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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
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
		return m.persistStateLocked()
	case NodeStateDecommissioning:
		return nil // idempotent
	case NodeStateDecommissioned:
		return fmt.Errorf("node already decommissioned")
	}
	return fmt.Errorf("unknown state: %s", m.state)
}

// MarkDecommissioned transitions the node to the decommissioned state.
func (m *NodeLifecycleManager) MarkDecommissioned() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = NodeStateDecommissioned
	_ = m.persistStateLocked()
}

// ClearState resets the node to active and removes the persisted state file.
// Used when a decommissioned node is re-commissioned or the state should be cleared.
func (m *NodeLifecycleManager) ClearState() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = NodeStateActive
	if m.stateFilePath == "" {
		return nil
	}
	err := os.Remove(m.stateFilePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove state file: %w", err)
	}
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
	return nil
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
