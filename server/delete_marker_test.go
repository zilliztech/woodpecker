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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteMarker_WriteScanRoundtrip(t *testing.T) {
	root := t.TempDir()

	// Write 3 markers: two log markers and one instance marker
	require.NoError(t, writeDeleteMarker(context.Background(), root, deleteMarker{Bucket: "b", RootPath: "r", LogId: 7, DeletedAt: 111}))
	require.NoError(t, writeDeleteMarker(context.Background(), root, deleteMarker{Bucket: "b", RootPath: "r", LogId: 8, DeletedAt: 222}))
	require.NoError(t, writeDeleteMarker(context.Background(), root, deleteMarker{Bucket: "b", RootPath: "r2", Instance: true, DeletedAt: 333}))

	markers, err := scanDeleteMarkers(context.Background(), root)
	require.NoError(t, err)
	assert.Len(t, markers, 3)

	// Check log markers have LogIds {7, 8}
	var logIds []int64
	var instanceMarkers []deleteMarker
	for _, m := range markers {
		if m.Instance {
			instanceMarkers = append(instanceMarkers, m)
		} else {
			logIds = append(logIds, m.LogId)
		}
	}
	assert.ElementsMatch(t, []int64{7, 8}, logIds)

	// Exactly 1 instance marker with RootPath "r2" (identity from content)
	require.Len(t, instanceMarkers, 1)
	assert.Equal(t, "r2", instanceMarkers[0].RootPath)
}

func TestDeleteMarker_CreateIfAbsentKeepsDeletedAt(t *testing.T) {
	root := t.TempDir()

	// Write initial marker
	require.NoError(t, writeDeleteMarker(context.Background(), root, deleteMarker{Bucket: "b", RootPath: "r", LogId: 7, DeletedAt: 111}))
	// Attempt to overwrite with a different DeletedAt — should be a no-op
	require.NoError(t, writeDeleteMarker(context.Background(), root, deleteMarker{Bucket: "b", RootPath: "r", LogId: 7, DeletedAt: 999}))

	markers, err := scanDeleteMarkers(context.Background(), root)
	require.NoError(t, err)
	require.Len(t, markers, 1)
	// Original DeletedAt must be preserved
	assert.Equal(t, int64(111), markers[0].DeletedAt)
}

func TestScanDeleteMarkers_EmptyRoot(t *testing.T) {
	root := t.TempDir()
	markers, err := scanDeleteMarkers(context.Background(), root)
	require.NoError(t, err)
	assert.Empty(t, markers)
}
