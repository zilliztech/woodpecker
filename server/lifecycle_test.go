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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewNodeLifecycleManager(t *testing.T) {
	m := NewNodeLifecycleManager()
	assert.NotNil(t, m)
	assert.Equal(t, NodeStateActive, m.GetState())
	assert.False(t, m.IsDecommissioning())
}

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
	err := m.MarkDecommissioned()
	assert.NoError(t, err)
	assert.Equal(t, NodeStateDecommissioned, m.GetState())

	// Cannot decommission an already decommissioned node
	err = m.StartDecommission()
	assert.Error(t, err)
}

func TestNodeLifecycleManager_MarkDecommissionedRequiresDecommissioning(t *testing.T) {
	m := NewNodeLifecycleManager()

	// From active: rejected — a cancelled node must never be moved to the
	// terminal decommissioned state by a stale monitor (issue #220).
	err := m.MarkDecommissioned()
	assert.ErrorIs(t, err, ErrNotDecommissioning)
	assert.Equal(t, NodeStateActive, m.GetState())

	// From decommissioning: allowed
	assert.NoError(t, m.StartDecommission())
	assert.NoError(t, m.MarkDecommissioned())
	assert.Equal(t, NodeStateDecommissioned, m.GetState())

	// From decommissioned: idempotent no-op
	assert.NoError(t, m.MarkDecommissioned())
	assert.Equal(t, NodeStateDecommissioned, m.GetState())
}

func TestNodeLifecycleManager_CancelThenMarkRejected(t *testing.T) {
	m := NewNodeLifecycleManager()
	require.NoError(t, m.StartDecommission())
	require.NoError(t, m.CancelDecommission())
	assert.Equal(t, NodeStateActive, m.GetState())

	// A monitor racing with the cancel must not be able to mark the node.
	err := m.MarkDecommissioned()
	assert.ErrorIs(t, err, ErrNotDecommissioning)
	assert.Equal(t, NodeStateActive, m.GetState())
}

func TestNodeLifecycleManager_Progress(t *testing.T) {
	m := NewNodeLifecycleManager()
	m.StartDecommission()

	// Has local data → not safe (regardless of processor count)
	progress := m.GetProgress(10, true)
	assert.Equal(t, NodeStateDecommissioning, NodeState(progress.State))
	assert.Equal(t, 10, progress.RemainingProcessors)
	assert.True(t, progress.HasLocalData)
	assert.False(t, progress.SafeToTerminate)

	// Still has local data, even with 0 processors → not safe
	progress = m.GetProgress(0, true)
	assert.False(t, progress.SafeToTerminate)

	// No local data → safe (processor count is informational only)
	progress = m.GetProgress(5, false)
	assert.True(t, progress.SafeToTerminate)

	progress = m.GetProgress(0, false)
	assert.False(t, progress.HasLocalData)
	assert.True(t, progress.SafeToTerminate)
}

// --- File persistence tests ---

func TestNodeLifecycleManager_Persistence_FreshStart(t *testing.T) {
	dir := t.TempDir()
	m, err := NewNodeLifecycleManagerWithPersistence(dir)
	require.NoError(t, err)
	assert.Equal(t, NodeStateActive, m.GetState())

	// No file should exist yet (active is the default, no need to persist)
	_, err = os.Stat(filepath.Join(dir, nodeStateFileName))
	assert.True(t, os.IsNotExist(err))
}

func TestNodeLifecycleManager_Persistence_DecommissionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// First boot: start decommission
	m1, err := NewNodeLifecycleManagerWithPersistence(dir)
	require.NoError(t, err)
	err = m1.StartDecommission()
	require.NoError(t, err)

	// Verify file was written
	data, err := os.ReadFile(filepath.Join(dir, nodeStateFileName))
	require.NoError(t, err)
	var persisted nodeStatePersisted
	require.NoError(t, json.Unmarshal(data, &persisted))
	assert.Equal(t, NodeStateDecommissioning, persisted.State)
	assert.Greater(t, persisted.Timestamp, int64(0))

	// Simulate restart: create new manager from same dir
	m2, err := NewNodeLifecycleManagerWithPersistence(dir)
	require.NoError(t, err)
	assert.Equal(t, NodeStateDecommissioning, m2.GetState())
	assert.True(t, m2.IsDecommissioning())
}

func TestNodeLifecycleManager_Persistence_DecommissionedSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	m1, err := NewNodeLifecycleManagerWithPersistence(dir)
	require.NoError(t, err)
	m1.StartDecommission()
	err = m1.MarkDecommissioned()
	require.NoError(t, err)

	// Simulate restart
	m2, err := NewNodeLifecycleManagerWithPersistence(dir)
	require.NoError(t, err)
	assert.Equal(t, NodeStateDecommissioned, m2.GetState())
}

func TestNodeLifecycleManager_Persistence_ClearState(t *testing.T) {
	dir := t.TempDir()

	m, err := NewNodeLifecycleManagerWithPersistence(dir)
	require.NoError(t, err)
	m.StartDecommission()

	// File exists
	_, err = os.Stat(filepath.Join(dir, nodeStateFileName))
	require.NoError(t, err)

	// Clear state
	err = m.ClearState()
	require.NoError(t, err)
	assert.Equal(t, NodeStateActive, m.GetState())

	// File should be removed
	_, err = os.Stat(filepath.Join(dir, nodeStateFileName))
	assert.True(t, os.IsNotExist(err))

	// New manager from same dir should start as active
	m2, err := NewNodeLifecycleManagerWithPersistence(dir)
	require.NoError(t, err)
	assert.Equal(t, NodeStateActive, m2.GetState())
}

func TestNodeLifecycleManager_Persistence_CorruptFile(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, nodeStateFileName), []byte("not json"), 0o644)
	require.NoError(t, err)

	_, err = NewNodeLifecycleManagerWithPersistence(dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "corrupt state file")
}

func TestNodeLifecycleManager_Persistence_UnknownState(t *testing.T) {
	dir := t.TempDir()
	data, _ := json.Marshal(nodeStatePersisted{State: "bogus", Timestamp: 1})
	err := os.WriteFile(filepath.Join(dir, nodeStateFileName), data, 0o644)
	require.NoError(t, err)

	_, err = NewNodeLifecycleManagerWithPersistence(dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown state")
}

func TestNodeLifecycleManager_Persistence_IdempotentNoExtraWrite(t *testing.T) {
	dir := t.TempDir()

	m, err := NewNodeLifecycleManagerWithPersistence(dir)
	require.NoError(t, err)
	err = m.StartDecommission()
	require.NoError(t, err)

	// Get file mod time
	info1, _ := os.Stat(filepath.Join(dir, nodeStateFileName))

	// Idempotent call should not error and not rewrite file
	err = m.StartDecommission()
	require.NoError(t, err)
	info2, _ := os.Stat(filepath.Join(dir, nodeStateFileName))
	assert.Equal(t, info1.ModTime(), info2.ModTime())
}

// --- CancelDecommission tests ---

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
	require.Contains(t, err.Error(), "already decommissioned")
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
