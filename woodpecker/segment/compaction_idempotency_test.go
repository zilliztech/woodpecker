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

package segment

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

func TestCompactAlreadySealedIsIdempotent(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	metadata := mocks_meta.NewMetadataProvider(t)
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	sealed := &meta.SegmentMeta{Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Sealed, LastEntryId: 10}, Revision: 2}
	metadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(sealed, nil).Once()
	handle := NewSegmentHandle(t.Context(), 1, "testLog", sealed, metadata, pool, cfg, false, nil)
	// No compaction RPC or metadata update is permitted by the strict mocks.
	require.NoError(t, handle.Compact(t.Context()))
}

func TestCompactMetadataConflictOnlyAcceptsSameSealedRange(t *testing.T) {
	unrelated := errors.New("unrelated metadata failure")
	refreshFailure := errors.New("metadata unavailable")
	for _, tc := range []struct {
		name       string
		state      proto.SegmentState
		lac        int64
		updateErr  error
		refreshErr error
		wantOK     bool
	}{
		{name: "same sealed range", state: proto.SegmentState_Sealed, lac: 10, updateErr: werr.ErrMetadataRevisionInvalid, wantOK: true},
		{name: "different range", state: proto.SegmentState_Sealed, lac: 9, updateErr: werr.ErrMetadataRevisionInvalid},
		{name: "truncated", state: proto.SegmentState_Truncated, lac: 10, updateErr: werr.ErrMetadataRevisionInvalid},
		{name: "still completed", state: proto.SegmentState_Completed, lac: 10, updateErr: werr.ErrMetadataRevisionInvalid},
		{name: "refresh failed", state: proto.SegmentState_Sealed, lac: 10, updateErr: werr.ErrMetadataRevisionInvalid, refreshErr: refreshFailure},
		{name: "deleted", state: proto.SegmentState_Sealed, lac: 10, updateErr: werr.ErrMetadataRevisionInvalid, refreshErr: werr.ErrSegmentNotFound},
		{name: "unrelated error", state: proto.SegmentState_Sealed, lac: 10, updateErr: unrelated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration()
			require.NoError(t, err)
			metadata := mocks_meta.NewMetadataProvider(t)
			pool := mocks_logstore_client.NewLogStoreClientPool(t)
			client := mocks_logstore_client.NewLogStoreClient(t)
			completed := &meta.SegmentMeta{Metadata: &proto.SegmentMetadata{
				SegNo: 1, State: proto.SegmentState_Completed, LastEntryId: 10,
				Quorum: &proto.QuorumInfo{Nodes: []string{"node0"}},
			}, Revision: 1}
			latest := &meta.SegmentMeta{Metadata: &proto.SegmentMetadata{SegNo: 1, State: tc.state, LastEntryId: tc.lac}, Revision: 2}
			metadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(completed, nil).Once()
			metadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(latest, tc.refreshErr).Once()
			metadata.EXPECT().UpdateSegmentMetadata(mock.Anything, "testLog", int64(1), mock.Anything, proto.SegmentState_Completed).Return(tc.updateErr).Once()
			pool.EXPECT().GetLogStoreClient(mock.Anything, "node0").Return(client, nil).Once()
			client.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(10)).
				Return(&proto.SegmentMetadata{LastEntryId: 10, Size: 100}, nil).Once()
			handle := NewSegmentHandle(context.Background(), 1, "testLog", completed, metadata, pool, cfg, false, nil)
			err = handle.Compact(t.Context())
			if tc.wantOK {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.updateErr)
			}
		})
	}
}
