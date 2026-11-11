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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_server/mocks_segment"
	"github.com/zilliztech/woodpecker/server/processor"
)

// A retired node must refuse to participate in segment lifecycle. Without this, a peer whose
// gossip view is still stale places a new segment here and fence/complete materialise a staged
// file for a segment this node holds no data for, flipping has_local_data back to true forever
// (#257).
func TestLogStore_Retired_RefusesSegmentLifecycleOps(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	store.MarkRetired()

	ctx := context.Background()
	const segId = int64(29)

	t.Run("FenceSegment", func(t *testing.T) {
		_, err := store.FenceSegment(ctx, testBucketName, testRootPath, testLogId, segId)
		require.ErrorIs(t, err, werr.ErrLogStoreRetired)
	})
	t.Run("CompleteSegment", func(t *testing.T) {
		_, err := store.CompleteSegment(ctx, testBucketName, testRootPath, testLogId, segId, 0)
		require.ErrorIs(t, err, werr.ErrLogStoreRetired)
	})
	t.Run("CompactSegment", func(t *testing.T) {
		_, err := store.CompactSegment(ctx, testBucketName, testRootPath, testLogId, segId, 0)
		require.ErrorIs(t, err, werr.ErrLogStoreRetired)
	})
	t.Run("UpdateLastAddConfirmed", func(t *testing.T) {
		err := store.UpdateLastAddConfirmed(ctx, testBucketName, testRootPath, testLogId, segId, 0)
		require.ErrorIs(t, err, werr.ErrLogStoreRetired)
	})
}

// Retiring implies write rejection: the terminal state is strictly stricter than decommissioning.
func TestLogStore_Retired_AlsoRejectsWrites(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	require.False(t, store.rejectWrites.Load(), "precondition: writes accepted before retiring")

	store.MarkRetired()

	assert.True(t, store.rejectWrites.Load())
	_, err := store.AddEntry(context.Background(), testBucketName, testRootPath, testLogId, nil, nil)
	require.ErrorIs(t, err, werr.ErrLogStoreShutdown)
}

// Cleanup and reads stay open on a retired node: they remove data or create nothing, and
// blocking them would strand whatever the node still holds locally. A mock processor is
// pre-seeded so these reach the processor instead of building one against absent storage.
func TestLogStore_Retired_AllowsCleanupAndReads(t *testing.T) {
	ctx := context.Background()
	const segId = int64(29)

	newStoreWithProcessor := func(t *testing.T) (*logStore, *mocks_segment.SegmentProcessor) {
		store := createTestLogStore()
		store.stopped.Store(false)
		store.MarkRetired()
		mp := mocks_segment.NewSegmentProcessor(t)
		store.segmentProcessors[GetLogKey(testBucketName, testRootPath, testLogId)] = map[int64]processor.SegmentProcessor{segId: mp}
		return store, mp
	}

	t.Run("CleanSegment reaches the processor", func(t *testing.T) {
		store, mp := newStoreWithProcessor(t)
		mp.EXPECT().Clean(mock.Anything, 0).Return(nil).Once()
		require.NoError(t, store.CleanSegment(ctx, testBucketName, testRootPath, testLogId, segId, 0))
	})

	t.Run("reads reach the processor", func(t *testing.T) {
		store, mp := newStoreWithProcessor(t)
		mp.EXPECT().GetSegmentLastAddConfirmed(mock.Anything).Return(int64(7), nil).Once()
		lac, err := store.GetSegmentLastAddConfirmed(ctx, testBucketName, testRootPath, testLogId, segId)
		require.NoError(t, err)
		assert.Equal(t, int64(7), lac)
	})
}

// A node still draining must keep serving fence/complete/compact — that is how its own segments
// get closed and uploaded so it can reach the terminal state at all.
func TestLogStore_Decommissioning_StillServesSegmentLifecycleOps(t *testing.T) {
	ctx := context.Background()
	const segId = int64(29)

	store := createTestLogStore()
	store.stopped.Store(false)
	store.RejectNewWrites() // decommissioning, not retired

	mp := mocks_segment.NewSegmentProcessor(t)
	mp.EXPECT().Fence(mock.Anything).Return(int64(4), nil).Once()
	mp.EXPECT().Complete(mock.Anything, int64(4)).Return(int64(4), nil).Once()
	store.segmentProcessors[GetLogKey(testBucketName, testRootPath, testLogId)] = map[int64]processor.SegmentProcessor{segId: mp}

	lastEntryId, err := store.FenceSegment(ctx, testBucketName, testRootPath, testLogId, segId)
	require.NoError(t, err)
	assert.Equal(t, int64(4), lastEntryId)

	_, err = store.CompleteSegment(ctx, testBucketName, testRootPath, testLogId, segId, 4)
	require.NoError(t, err)

	// ...while appends are already refused.
	_, addErr := store.AddEntry(ctx, testBucketName, testRootPath, testLogId, nil, nil)
	require.ErrorIs(t, addErr, werr.ErrLogStoreShutdown)
}

// Retirement is a one-way latch: decommissioned is terminal and CancelDecommission refuses it,
// so AllowNewWrites (the cancel path) must not resurrect a retired node. Both halves of the
// invariant are checked — segment ops stay refused *and* writes stay refused. The write half is
// the one that was actually breakable: AllowNewWrites used to clear rejectWrites unconditionally,
// so a retired store would admit AddEntry again and could materialise a fresh data.log.
func TestLogStore_Retired_NotClearedByAllowNewWrites(t *testing.T) {
	ctx := context.Background()
	store := createTestLogStore()
	store.stopped.Store(false)
	store.MarkRetired()

	store.AllowNewWrites()

	assert.True(t, store.retired.Load(), "retirement must survive AllowNewWrites")
	assert.True(t, store.rejectWrites.Load(), "retired => rejectWrites must survive AllowNewWrites")

	_, addErr := store.AddEntry(ctx, testBucketName, testRootPath, testLogId, nil, nil)
	require.ErrorIs(t, addErr, werr.ErrLogStoreShutdown, "a retired store must not admit appends again")

	_, fenceErr := store.FenceSegment(ctx, testBucketName, testRootPath, testLogId, 29)
	require.ErrorIs(t, fenceErr, werr.ErrLogStoreRetired)
}

// ErrLogStoreRetired must be non-retryable: retrying against a terminally retired node can never
// succeed, so the caller should drop it from the quorum and re-discover instead of backing off.
func TestErrLogStoreRetired_NotRetryable(t *testing.T) {
	assert.False(t, werr.IsRetryableErr(werr.ErrLogStoreRetired))
}
