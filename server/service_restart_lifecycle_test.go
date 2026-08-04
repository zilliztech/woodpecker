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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The monitor is the only place the live decommissioning -> decommissioned transition happens,
// and it returns immediately afterwards. If it does not retire the log store on the way out,
// nothing ever will: a later fence/complete from a peer with a stale gossip view re-creates a
// staged file and flips has_local_data back to true permanently (#257).
func TestServer_DecommissionMonitor_RetiresLogStoreOnTransition(t *testing.T) {
	setShortDecommissionInterval(t, 10*time.Millisecond)

	fake := &fakeLogStore{} // HasLocalSegmentData() is false: already drained
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()
	s.lifecycle.StartDecommission()

	s.startDecommissionMonitor()
	s.decommWG.Wait() // the monitor returns once it has marked the node decommissioned

	assert.True(t, s.lifecycle.IsDecommissioned(), "monitor should have completed the transition")
	assert.True(t, fake.retired, "monitor must retire the log store before exiting")
}

// Restart must re-apply whatever the persisted lifecycle state implies. The previous code keyed
// off IsDecommissioning() alone, which matches only the draining state, so a node persisted as
// decommissioned restarted with no write rejection, no retirement and no gossip tag — back in
// the cluster indistinguishable from an active node (#257).
func TestServer_RestoreLifecycleEnforcement(t *testing.T) {
	for _, tc := range []struct {
		name              string
		setup             func(m *NodeLifecycleManager)
		wantWritesReject  bool
		wantRetired       bool
		wantMonitorResume bool
	}{
		{
			name:  "active restarts unconstrained",
			setup: func(m *NodeLifecycleManager) {},
		},
		{
			// Still draining: writes stay refused, but fence/complete/compact must keep working
			// so its own segments can close, and the monitor resumes to finish the transition.
			name:              "decommissioning re-applies write rejection and resumes monitor",
			setup:             func(m *NodeLifecycleManager) { m.StartDecommission() },
			wantWritesReject:  true,
			wantMonitorResume: true,
		},
		{
			// Terminal: nothing left to drain or monitor, so it also stops participating.
			name: "decommissioned also retires",
			setup: func(m *NodeLifecycleManager) {
				m.StartDecommission()
				require.NoError(t, m.MarkDecommissioned())
			},
			wantWritesReject: true,
			wantRetired:      true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeLogStore{}
			s := createTestServerWithFakeLogStore(fake)
			defer s.cancel()
			tc.setup(s.lifecycle)

			s.restoreLifecycleEnforcement()

			assert.Equal(t, tc.wantWritesReject, fake.writesRejected, "writes rejected")
			assert.Equal(t, tc.wantRetired, fake.retired, "retired")

			s.decommMu.Lock()
			running := s.decommRunning
			s.decommMu.Unlock()
			assert.Equal(t, tc.wantMonitorResume, running, "decommission monitor running")
		})
	}
}

// The monitor must not be resumed for a terminally decommissioned node: there is nothing left to
// drain, and it would be the only thing that could later clear the gate.
func TestServer_RestoreLifecycleEnforcement_DecommissionedDoesNotResumeMonitor(t *testing.T) {
	fake := &fakeLogStore{}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()
	s.lifecycle.StartDecommission()
	require.NoError(t, s.lifecycle.MarkDecommissioned())

	s.restoreLifecycleEnforcement()

	s.decommMu.Lock()
	defer s.decommMu.Unlock()
	assert.False(t, s.decommRunning)
	assert.True(t, fake.retired)
}
