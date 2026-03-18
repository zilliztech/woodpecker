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

package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockNodeStatus struct {
	NodeID string `json:"node_id"`
	State  string `json:"state"`
}

func TestNodeStatusHandler(t *testing.T) {
	handler := NewNodeStatusHandler(func() any {
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
	handler := NewNodeStatusHandler(func() any {
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
	handler := NewNodeDecommissionProgressHandler(func() any {
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
