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

package stagedstorage

// Finalize scenario matrix covered in this file:
//
//	Initial call:
//	  local tail = -1, < target, = target, > target -> finalize and return local tail
//	  target LAC < -1                           -> reject without finalizing
//	Retry/recovery:
//	  same target (exact/partial/empty/restart)  -> idempotent success
//	  different lower or higher target           -> reject, preserve durable footer
//	Invalid recovered state:
//	  finalized flag without recovered footer    -> reject
//	Concurrency:
//	  same target                                -> both succeed
//	  different targets                          -> exactly one durable winner
//	Read integration:
//	  partial finalized replica                  -> preserve global LAC and signal
//	                                                missing suffix for failover

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
)

func writeFinalizeEntriesThrough(t *testing.T, writer *StagedFileWriter, localLastEntryID int64) {
	t.Helper()
	for entryID := int64(0); entryID <= localLastEntryID; entryID++ {
		_, err := writer.WriteDataAsync(context.Background(), entryID, []byte("test data"), nil)
		require.NoError(t, err)
	}
}

// TestStagedFileWriter_FinalizeInitialMatrix documents the first-Finalize
// decision matrix. Finalize freezes the local file and records the global target
// LAC; it intentionally does not require the local replica to cover that target.
// The returned local LastEntryId lets the coordinator decide whether this replica
// is qualified for the Aq completion proof.
func TestStagedFileWriter_FinalizeInitialMatrix(t *testing.T) {
	tests := []struct {
		name             string
		localLastEntryID int64
		targetLAC        int64
		wantErr          bool
	}{
		{name: "empty replica empty target", localLastEntryID: -1, targetLAC: -1},
		{name: "empty replica partial for entry zero", localLastEntryID: -1, targetLAC: 0},
		{name: "partial replica", localLastEntryID: 1, targetLAC: 2},
		{name: "exact replica", localLastEntryID: 2, targetLAC: 2},
		{name: "ahead replica", localLastEntryID: 3, targetLAC: 2},
		{name: "invalid target below empty sentinel", localLastEntryID: -1, targetLAC: -2, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := newTestConfig(t)
			writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
			require.NoError(t, err)
			defer writer.Close(context.Background())
			writeFinalizeEntriesThrough(t, writer, tt.localLastEntryID)

			lastEntryID, err := writer.Finalize(context.Background(), tt.targetLAC)
			assert.Equal(t, tt.localLastEntryID, lastEntryID)
			if tt.wantErr {
				assert.Error(t, err)
				assert.True(t, werr.ErrInvalidLACAlignment.Is(err), "expected invalid LAC, got %v", err)
				assert.False(t, writer.finalized.Load())
				return
			}
			require.NoError(t, err)
			assert.True(t, writer.finalized.Load())
			require.NotNil(t, writer.recoveredFooter)
			assert.Equal(t, tt.targetLAC, writer.recoveredFooter.LAC)
		})
	}
}

// TestStagedFileWriter_FinalizeIdempotencyMatrix covers retries after a local
// footer already exists. Same-target retries are idempotent even for empty or
// partial replicas; any different target is rejected so a retry cannot silently
// change the durable completion boundary.
func TestStagedFileWriter_FinalizeIdempotencyMatrix(t *testing.T) {
	tests := []struct {
		name             string
		localLastEntryID int64
		initialLAC       int64
		retryLAC         int64
		wantErr          bool
	}{
		{name: "exact same target", localLastEntryID: 2, initialLAC: 2, retryLAC: 2},
		{name: "partial same target", localLastEntryID: 1, initialLAC: 2, retryLAC: 2},
		{name: "empty same target", localLastEntryID: -1, initialLAC: -1, retryLAC: -1},
		{name: "different lower target", localLastEntryID: 2, initialLAC: 2, retryLAC: 1, wantErr: true},
		{name: "different higher target", localLastEntryID: 2, initialLAC: 2, retryLAC: 3, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := newTestConfig(t)
			writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
			require.NoError(t, err)
			defer writer.Close(context.Background())
			writeFinalizeEntriesThrough(t, writer, tt.localLastEntryID)

			lastEntryID, err := writer.Finalize(context.Background(), tt.initialLAC)
			require.NoError(t, err)
			assert.Equal(t, tt.localLastEntryID, lastEntryID)

			lastEntryID, err = writer.Finalize(context.Background(), tt.retryLAC)
			assert.Equal(t, tt.localLastEntryID, lastEntryID)
			if tt.wantErr {
				assert.Error(t, err)
				assert.True(t, werr.ErrInvalidLACAlignment.Is(err), "expected LAC mismatch, got %v", err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.initialLAC, writer.recoveredFooter.LAC, "retry must not change the durable target")
		})
	}
}

func TestStagedFileWriter_FinalizeAlreadyFinalizedWithoutFooterFails(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())
	writer.finalized.Store(true)

	lastEntryID, err := writer.Finalize(context.Background(), 0)
	assert.Equal(t, int64(-1), lastEntryID)
	assert.Error(t, err)
	assert.True(t, werr.ErrInvalidLACAlignment.Is(err))
}

func TestStagedFileWriter_FinalizePartialReplicaRestartIdempotency(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	writeFinalizeEntriesThrough(t, writer, 1)

	lastEntryID, err := writer.Finalize(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, int64(1), lastEntryID)
	require.NoError(t, writer.Close(context.Background()))

	recovered, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer recovered.Close(context.Background())
	assert.True(t, recovered.finalized.Load())

	lastEntryID, err = recovered.Finalize(context.Background(), 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), lastEntryID)
	_, err = recovered.Finalize(context.Background(), 3)
	assert.Error(t, err)
	assert.True(t, werr.ErrInvalidLACAlignment.Is(err))
}

func TestStagedFileWriter_FinalizeConcurrentSameTarget(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())
	writeFinalizeEntriesThrough(t, writer, 2)

	type result struct {
		lastEntryID int64
		err         error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lastEntryID, finalizeErr := writer.Finalize(context.Background(), 2)
			results <- result{lastEntryID: lastEntryID, err: finalizeErr}
		}()
	}
	wg.Wait()
	close(results)

	for result := range results {
		assert.NoError(t, result.err)
		assert.Equal(t, int64(2), result.lastEntryID)
	}
	assert.Equal(t, int64(2), writer.recoveredFooter.LAC)
}

func TestStagedFileWriter_FinalizeConcurrentDifferentTargets(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())
	writeFinalizeEntriesThrough(t, writer, 2)

	type result struct {
		targetLAC   int64
		lastEntryID int64
		err         error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, targetLAC := range []int64{1, 2} {
		targetLAC := targetLAC
		wg.Add(1)
		go func() {
			defer wg.Done()
			lastEntryID, finalizeErr := writer.Finalize(context.Background(), targetLAC)
			results <- result{targetLAC: targetLAC, lastEntryID: lastEntryID, err: finalizeErr}
		}()
	}
	wg.Wait()
	close(results)

	successes := 0
	failures := 0
	var successfulTarget int64
	for result := range results {
		assert.Equal(t, int64(2), result.lastEntryID)
		if result.err == nil {
			successes++
			successfulTarget = result.targetLAC
			continue
		}
		failures++
		assert.True(t, werr.ErrInvalidLACAlignment.Is(result.err))
	}
	assert.Equal(t, 1, successes)
	assert.Equal(t, 1, failures)
	assert.Equal(t, successfulTarget, writer.recoveredFooter.LAC)
}

func TestStagedFileWriter_FinalizePartialReplicaKeepsGlobalLACForReadFailover(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	writeFinalizeEntriesThrough(t, writer, 1)

	lastEntryID, err := writer.Finalize(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, int64(1), lastEntryID)
	assert.Equal(t, int64(2), writer.recoveredFooter.LAC)
	require.NoError(t, writer.Close(context.Background()))

	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	result, err := reader.ReadNextBatchAdv(context.Background(), storage.ReaderOpt{
		StartEntryID:    2,
		MaxBatchEntries: 1,
	}, nil)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err), "missing suffix should fail over to another replica, got %v", err)
}
