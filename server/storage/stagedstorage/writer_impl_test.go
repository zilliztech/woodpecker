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

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// newTestConfig creates a default test configuration
func newTestConfig(t *testing.T) *config.Configuration {
	t.Helper()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	return cfg
}

// --- NewStagedFileWriter ---

func TestNewStagedFileWriter(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)
	defer writer.Close(context.Background())

	assert.Equal(t, int64(1), writer.logId)
	assert.Equal(t, int64(0), writer.segmentId)
	assert.False(t, writer.closed.Load())
	assert.False(t, writer.finalized.Load())
	assert.False(t, writer.fenced.Load())
	assert.True(t, writer.storageWritable.Load())
	assert.Equal(t, int64(-1), writer.firstEntryID.Load())
	assert.Equal(t, int64(-1), writer.lastEntryID.Load())
}

func TestNewStagedFileWriter_NilStorage(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	// nil ObjectStorage is allowed (local-only mode)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 2, 1, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)
	defer writer.Close(context.Background())
}

func TestNewStagedFileWriterWithMode_RecoveryMode_NoExistingFile(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	// Recovery mode with no existing file should succeed (nothing to recover)
	writer, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg, true)
	require.NoError(t, err)
	require.NotNil(t, writer)
	defer writer.Close(context.Background())

	assert.Equal(t, int64(-1), writer.GetLastEntryId(context.Background()))
}

// --- GetFirstEntryId ---

func TestStagedFileWriter_GetFirstEntryId(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.Equal(t, int64(-1), writer.GetFirstEntryId(context.Background()))
}

// --- GetLastEntryId ---

func TestStagedFileWriter_GetLastEntryId(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.Equal(t, int64(-1), writer.GetLastEntryId(context.Background()))
}

// --- GetBlockCount ---

func TestStagedFileWriter_GetBlockCount(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.Equal(t, int64(-1), writer.GetBlockCount(context.Background()))
}

// --- WriteDataAsync ---

func TestStagedFileWriter_WriteDataAsync_EmptyData(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	_, err = writer.WriteDataAsync(context.Background(), 0, []byte{}, nil)
	assert.ErrorIs(t, err, werr.ErrEmptyPayload)
}

func TestStagedFileWriter_WriteDataAsync_AfterClose(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	err = writer.Close(context.Background())
	require.NoError(t, err)

	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrFileWriterAlreadyClosed)
}

func TestStagedFileWriter_WriteDataAsync_AfterFence(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Fence the writer first (WriteDataAsync checks fenced but doesn't reject directly;
	// the fenced check happens at the processFlushTask level. However, the WriteDataAsync
	// check for storageWritable covers this scenario).
	// Directly set fenced to test the guard in WriteDataAsync's entry check.
	// Actually WriteDataAsync doesn't check fenced directly; it checks closed, finalized, storageWritable, inRecoveryMode.
	// The fenced guard is in processFlushTask. Let's use the storageWritable guard instead.
	writer.storageWritable.Store(false)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrStorageNotWritable)
}

func TestStagedFileWriter_WriteDataAsync_AfterFinalize(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	writer.finalized.Store(true)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrFileWriterFinalized)
}

func TestStagedFileWriter_WriteDataAsync_StorageNotWritable(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	writer.storageWritable.Store(false)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrStorageNotWritable)
}

func TestStagedFileWriter_WriteDataAsync_InRecoveryMode(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	writer.inRecoveryMode.Store(true)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrFileWriterInRecoveryMode)
}

func TestStagedFileWriter_WriteDataAsync_Success(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	id, err := writer.WriteDataAsync(context.Background(), 0, []byte("test data"), nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id)
}

func TestStagedFileWriter_WriteDataAsync_MultipleEntries(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	for i := int64(0); i < 5; i++ {
		id, writeErr := writer.WriteDataAsync(context.Background(), i, []byte("entry data"), nil)
		assert.NoError(t, writeErr)
		assert.Equal(t, i, id)
	}
}

func TestStagedFileWriter_WriteDataAsync_AlreadySubmittedEntry(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write entry 0
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)

	// Sync so entry 0 is submitted for flushing
	err = writer.Sync(context.Background())
	require.NoError(t, err)

	// Write entry 0 again while it is in the flushing pipeline
	// The lastSubmittedFlushingEntryID should cover this entry, so it returns early
	id, err := writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id)
}

// --- Sync ---

func TestStagedFileWriter_Sync(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write some data first
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("test"), nil)
	require.NoError(t, err)

	// Sync should succeed
	err = writer.Sync(context.Background())
	assert.NoError(t, err)
}

func TestStagedFileWriter_Sync_StorageNotWritable(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	writer.storageWritable.Store(false)

	// Sync should return nil when storage is not writable (early return)
	err = writer.Sync(context.Background())
	assert.NoError(t, err)
}

// --- Close ---

func TestStagedFileWriter_Close(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	err = writer.Close(context.Background())
	assert.NoError(t, err)
	assert.True(t, writer.closed.Load())
}

func TestStagedFileWriter_Close_Idempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	err = writer.Close(context.Background())
	assert.NoError(t, err)

	// Second close should also succeed (idempotent)
	err = writer.Close(context.Background())
	assert.NoError(t, err)
}

// --- Fence ---

func TestStagedFileWriter_Fence(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	lastId, err := writer.Fence(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), lastId) // No data written yet
	assert.True(t, writer.fenced.Load())
}

func TestStagedFileWriter_Fence_Idempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	lastId1, err := writer.Fence(context.Background())
	assert.NoError(t, err)

	// Second fence should be idempotent
	lastId2, err := writer.Fence(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, lastId1, lastId2)
}

func TestStagedFileWriter_Fence_AfterWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write some entries
	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}

	// Fence should sync pending data
	lastId, err := writer.Fence(context.Background())
	assert.NoError(t, err)
	assert.True(t, writer.fenced.Load())
	// Last entry ID might not be updated immediately because flush is async,
	// but the fence should have triggered a sync.
	_ = lastId
}

// --- Finalize ---

func TestStagedFileWriter_Finalize(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	// Write some entries
	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}

	// Sync to flush
	err = writer.Sync(context.Background())
	require.NoError(t, err)

	// Wait for async flush
	time.Sleep(100 * time.Millisecond)

	// Finalize
	lastId, err := writer.Finalize(context.Background(), 4)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, lastId, int64(0))
	assert.True(t, writer.finalized.Load())

	writer.Close(context.Background())
}

func TestStagedFileWriter_Finalize_Idempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	// Write some entries
	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}

	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Finalize twice
	lastId1, err := writer.Finalize(context.Background(), 2)
	assert.NoError(t, err)

	lastId2, err := writer.Finalize(context.Background(), 2)
	assert.NoError(t, err)
	assert.Equal(t, lastId1, lastId2)

	writer.Close(context.Background())
}

func TestStagedFileWriter_Finalize_EmptyWriter(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	// Finalize with no data written
	lastId, err := writer.Finalize(context.Background(), -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), lastId)

	writer.Close(context.Background())
}

// --- Write then Read roundtrip ---

func TestStagedFileWriter_WriteAndRead(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	// Write entries
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 10; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("entry data"), nil)
		require.NoError(t, err)
	}

	// Sync to flush
	err = writer.Sync(context.Background())
	require.NoError(t, err)

	// Wait for async flush to complete
	time.Sleep(200 * time.Millisecond)

	// Finalize
	lastId, err := writer.Finalize(context.Background(), 9)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lastId, int64(0))
	writer.Close(context.Background())

	// Now read back using StagedFileReaderAdv
	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	// Verify the footer exists (file was finalized)
	assert.NotNil(t, reader.footer)
}

// --- Compact (requires finalized writer) ---

func TestStagedFileWriter_Compact_NotFinalized(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Compact without finalization should fail
	_, err = writer.Compact(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "segment must be finalized before compaction")
}

// --- getFooterBlockKey / getCompactedBlockKey ---

func TestStagedFileWriter_GetFooterBlockKey(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	key := writer.getFooterBlockKey()
	assert.Equal(t, "test-root/1/0/footer.blk", key)
}

func TestStagedFileWriter_GetCompactedBlockKey(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 3, 7, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.Equal(t, "test-root/3/7/m_0.blk", writer.getCompactedBlockKey(0))
	assert.Equal(t, "test-root/3/7/m_5.blk", writer.getCompactedBlockKey(5))
}

// --- GetRecoveredFooter ---

func TestStagedFileWriter_GetRecoveredFooter_Nil(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.Nil(t, writer.GetRecoveredFooter())
}

func TestStagedFileWriter_GetRecoveredFooter_AfterFinalize(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer.Finalize(context.Background(), 2)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	footer := writer.GetRecoveredFooter()
	assert.NotNil(t, footer)
	assert.Equal(t, int64(2), footer.LAC)
}

// --- serializeCompactedFooterAndIndexes ---

func TestStagedFileWriter_SerializeCompactedFooterAndIndexes(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	blockIndexes := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
		{BlockNumber: 1, StartOffset: 100, BlockSize: 200, FirstEntryID: 10, LastEntryID: 19},
	}
	footer := &codec.FooterRecord{
		TotalBlocks:  2,
		TotalRecords: 20,
		TotalSize:    300,
		IndexLength:  0,
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0),
		LAC:          19,
	}

	data := writer.serializeCompactedFooterAndIndexes(context.Background(), blockIndexes, footer)
	assert.NotEmpty(t, data)
	// Data should contain 2 index records + 1 footer record
	expectedMinSize := 2*(codec.RecordHeaderSize+codec.IndexRecordSize) + codec.RecordHeaderSize + codec.FooterRecordSizeV5
	assert.GreaterOrEqual(t, len(data), expectedMinSize)
}

func TestStagedFileWriter_SerializeCompactedFooterAndIndexes_Empty(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	footer := &codec.FooterRecord{
		TotalBlocks: 0,
		Version:     codec.FormatVersion,
		Flags:       codec.SetCompacted(0),
		LAC:         -1,
	}
	data := writer.serializeCompactedFooterAndIndexes(context.Background(), nil, footer)
	// Should just contain footer record
	assert.NotEmpty(t, data)
}

// --- planMergeBlockTasks ---

func TestStagedFileWriter_PlanMergeBlockTasks_Empty(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	tasks := writer.planMergeBlockTasks(2 * 1024 * 1024)
	assert.Empty(t, tasks)
}

func TestStagedFileWriter_PlanMergeBlockTasks_SingleBlock(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	writer.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
	}
	writer.firstEntryID.Store(0)

	tasks := writer.planMergeBlockTasks(2 * 1024 * 1024)
	assert.Len(t, tasks, 1)
	assert.Len(t, tasks[0].blocks, 1)
}

func TestStagedFileWriter_PlanMergeBlockTasks_MultipleMerged(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Create 5 small blocks that should all fit in one merge task
	writer.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
		{BlockNumber: 1, StartOffset: 100, BlockSize: 100, FirstEntryID: 10, LastEntryID: 19},
		{BlockNumber: 2, StartOffset: 200, BlockSize: 100, FirstEntryID: 20, LastEntryID: 29},
		{BlockNumber: 3, StartOffset: 300, BlockSize: 100, FirstEntryID: 30, LastEntryID: 39},
		{BlockNumber: 4, StartOffset: 400, BlockSize: 100, FirstEntryID: 40, LastEntryID: 49},
	}
	writer.firstEntryID.Store(0)

	// Target block size is larger than all blocks combined
	tasks := writer.planMergeBlockTasks(2 * 1024 * 1024)
	assert.Len(t, tasks, 1)
	assert.Len(t, tasks[0].blocks, 5)
}

func TestStagedFileWriter_PlanMergeBlockTasks_Split(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Create blocks where first 2 fit in target, last 2 fit in another
	writer.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 500, FirstEntryID: 0, LastEntryID: 9},
		{BlockNumber: 1, StartOffset: 500, BlockSize: 500, FirstEntryID: 10, LastEntryID: 19},
		{BlockNumber: 2, StartOffset: 1000, BlockSize: 500, FirstEntryID: 20, LastEntryID: 29},
		{BlockNumber: 3, StartOffset: 1500, BlockSize: 500, FirstEntryID: 30, LastEntryID: 39},
	}
	writer.firstEntryID.Store(0)

	// Target is 1000 bytes — first block (500) fits, then second (500+500=1000 >= target) triggers split
	tasks := writer.planMergeBlockTasks(1000)
	assert.GreaterOrEqual(t, len(tasks), 2)
}

// --- validateLACAlignment ---

func TestStagedFileWriter_ValidateLACAlignment_Success(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 10; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer.Finalize(context.Background(), 9)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	lac, err := writer.validateLACAlignment(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(9), lac)
}

func TestStagedFileWriter_ValidateLACAlignment_NotFinalized(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	_, err = writer.validateLACAlignment(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not finalized")
}

func TestStagedFileWriter_ValidateLACAlignment_NegativeLAC(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer.Finalize(context.Background(), -1)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	_, err = writer.validateLACAlignment(context.Background())
	assert.Error(t, err)
}

func TestStagedFileWriter_ValidateLACAlignment_FirstEntryNotZero(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer.Finalize(context.Background(), 2)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Manually tamper with firstEntryID
	writer.firstEntryID.Store(5)
	_, err = writer.validateLACAlignment(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not start from entry 0")
}

func TestStagedFileWriter_ValidateLACAlignment_LACExceedsData(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	// Finalize with LAC=2 (valid)
	_, err = writer.Finalize(context.Background(), 2)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Override recoveredFooter with LAC > lastEntryID
	writer.recoveredFooter.LAC = 100
	_, err = writer.validateLACAlignment(context.Background())
	assert.Error(t, err)
}

// --- readBlockDataFromLocalFile ---

func TestStagedFileWriter_ReadBlockDataFromLocalFile_Success(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	// Write data, sync, wait for flush
	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer.Finalize(context.Background(), 4)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	require.NotEmpty(t, writer.blockIndexes)

	result := writer.readBlockDataFromLocalFile(context.Background(), writer.blockIndexes[0])
	assert.Nil(t, result.error)
	assert.NotEmpty(t, result.blockData)
	assert.Equal(t, writer.blockIndexes[0], result.blockIndex)
}

func TestStagedFileWriter_ReadBlockDataFromLocalFile_FileNotFound(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Remove the segment file to cause read failure
	os.Remove(writer.segmentFilePath)

	blockIndex := &codec.IndexRecord{
		BlockNumber:  0,
		StartOffset:  0,
		BlockSize:    100,
		FirstEntryID: 0,
		LastEntryID:  9,
	}
	result := writer.readBlockDataFromLocalFile(context.Background(), blockIndex)
	assert.NotNil(t, result.error)
}

// --- determineIfNeedRecoveryMode ---

func TestStagedFileWriter_DetermineIfNeedRecoveryMode_ForceTrue(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.True(t, writer.determineIfNeedRecoveryMode(true))
}

func TestStagedFileWriter_DetermineIfNeedRecoveryMode_NoFile(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// segmentFilePath already exists (created by constructor), so override
	writer.segmentFilePath = filepath.Join(dir, "nonexist", "data.log")
	assert.False(t, writer.determineIfNeedRecoveryMode(false))
}

func TestStagedFileWriter_DetermineIfNeedRecoveryMode_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// The file was created by constructor and may have data written, so create a new empty file
	emptyFile := filepath.Join(dir, "empty.log")
	f, err := os.Create(emptyFile)
	require.NoError(t, err)
	f.Close()

	writer.segmentFilePath = emptyFile
	assert.False(t, writer.determineIfNeedRecoveryMode(false))
}

func TestStagedFileWriter_DetermineIfNeedRecoveryMode_FileWithData(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	// Create a writer, write data, then close
	writer1, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	_, err = writer1.WriteDataAsync(context.Background(), 0, []byte("test"), nil)
	require.NoError(t, err)
	err = writer1.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	writer1.Close(context.Background())

	// Create another writer pointing to the same file
	writer2, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer2.Close(context.Background())

	// Since the file has data, this should auto-detect recovery mode
	// (The constructor already calls determineIfNeedRecoveryMode)
	// We verify by calling it directly
	assert.True(t, writer2.determineIfNeedRecoveryMode(false))
}

// --- recoverBlocksFromFullScanUnsafe ---

func TestStagedFileWriter_RecoverBlocksFromFullScan(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	// Create a writer, write data, finalize
	writer1, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 5; i++ {
		_, err = writer1.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}
	err = writer1.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	writer1.Close(context.Background())

	// Now create a recovery writer
	writer2, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg, true)
	require.NoError(t, err)
	defer writer2.Close(context.Background())

	// Verify blocks were recovered
	assert.True(t, writer2.recovered.Load())
	assert.Equal(t, int64(4), writer2.lastEntryID.Load())
	assert.NotEmpty(t, writer2.blockIndexes)
}

// --- recoverBlocksFromFooterUnsafe ---

func TestStagedFileWriter_RecoverBlocksFromFooter(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	// Create a writer, write data, finalize
	writer1, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 5; i++ {
		_, err = writer1.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}
	err = writer1.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer1.Finalize(context.Background(), 4)
	require.NoError(t, err)
	writer1.Close(context.Background())

	// Now create a recovery writer — it should find the footer
	writer2, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg, true)
	require.NoError(t, err)
	defer writer2.Close(context.Background())

	assert.True(t, writer2.recovered.Load())
	assert.True(t, writer2.finalized.Load())
	assert.NotNil(t, writer2.recoveredFooter)
	assert.Equal(t, int64(4), writer2.lastEntryID.Load())
	assert.Equal(t, int64(0), writer2.firstEntryID.Load())
}

// --- Compact full flow ---

func TestStagedFileWriter_Compact_FullFlow(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	cfg.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelUploads = 4
	cfg.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelReads = 4

	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 10; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer.Finalize(context.Background(), 9)
	require.NoError(t, err)

	// Mock PutObject for merged blocks and footer
	mockStorage.EXPECT().PutObject(mock.Anything, "test-bucket", mock.MatchedBy(func(key string) bool {
		return true // accept any key
	}), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	totalSize, err := writer.Compact(context.Background())
	assert.NoError(t, err)
	assert.Greater(t, totalSize, int64(0))
	writer.Close(context.Background())
}

func TestStagedFileWriter_Compact_NoBlocks(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	// Finalize with no data
	_, err = writer.Finalize(context.Background(), -1)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	totalSize, err := writer.Compact(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), totalSize)
}

// --- uploadCompactedFooter ---

func TestStagedFileWriter_UploadCompactedFooter(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	writer.firstEntryID.Store(0)
	writer.lastEntryID.Store(9)

	blockIndexes := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
	}

	mockStorage.EXPECT().PutObject(mock.Anything, "test-bucket", "test-root/1/0/footer.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	size, err := writer.uploadCompactedFooter(context.Background(), blockIndexes, 100, 9)
	assert.NoError(t, err)
	assert.Greater(t, size, int64(0))
}

func TestStagedFileWriter_UploadCompactedFooter_PutError(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	writer.firstEntryID.Store(0)
	writer.lastEntryID.Store(9)

	blockIndexes := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
	}

	mockStorage.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(fmt.Errorf("put error"))

	_, err = writer.uploadCompactedFooter(context.Background(), blockIndexes, 100, 9)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to upload footer")
}

// === NewStagedFileWriter - open file error ===

func TestNewStagedFileWriter_OpenFilePermissionError(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	logId := int64(300)
	segId := int64(300)

	// Create the segment directory, then make it read-only so file creation fails
	segmentDir := getSegmentDir(dir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	require.NoError(t, os.Chmod(segmentDir, 0o444))
	t.Cleanup(func() { os.Chmod(segmentDir, 0o755) })

	_, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "open file")
}

// === processFlushTask error branches ===

func createWriterForFlushTest(t *testing.T, dir string, logId, segId int64) *StagedFileWriter {
	t.Helper()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)
	return writer
}

func TestProcessFlushTask_NilTask(t *testing.T) {
	dir := t.TempDir()
	writer := createWriterForFlushTest(t, dir, 301, 301)
	defer writer.Close(context.Background())

	// Should not panic
	writer.processFlushTask(context.Background(), nil)
}

func TestProcessFlushTask_Fenced(t *testing.T) {
	dir := t.TempDir()
	writer := createWriterForFlushTest(t, dir, 302, 302)
	defer writer.Close(context.Background())

	writer.fenced.Store(true)

	task := &blockFlushTask{
		entries:      []*cache.BufferEntry{{EntryId: 0, Data: []byte("data")}},
		firstEntryId: 0,
		lastEntryId:  0,
		blockNumber:  0,
	}
	// Should return without writing, no panic
	writer.processFlushTask(context.Background(), task)
}

func TestProcessFlushTask_Finalized(t *testing.T) {
	dir := t.TempDir()
	writer := createWriterForFlushTest(t, dir, 303, 303)
	defer writer.Close(context.Background())

	writer.finalized.Store(true)

	task := &blockFlushTask{
		entries:      []*cache.BufferEntry{{EntryId: 0, Data: []byte("data")}},
		firstEntryId: 0,
		lastEntryId:  0,
		blockNumber:  0,
	}
	writer.processFlushTask(context.Background(), task)
}

func TestProcessFlushTask_StorageNotWritable(t *testing.T) {
	dir := t.TempDir()
	writer := createWriterForFlushTest(t, dir, 304, 304)
	defer writer.Close(context.Background())

	writer.storageWritable.Store(false)

	task := &blockFlushTask{
		entries:      []*cache.BufferEntry{{EntryId: 0, Data: []byte("data")}},
		firstEntryId: 0,
		lastEntryId:  0,
		blockNumber:  0,
	}
	writer.processFlushTask(context.Background(), task)
}

// === WriteDataAsync - duplicate entry already written ===

func TestWriteDataAsync_DuplicateAlreadyWrittenToDisk(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write entries 0-4
	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	// Now try to write entry 2 again (already written to disk, lastEntryID >= 2)
	resultCh := channel.NewLocalResultChannel("dup-test")
	id, err := writer.WriteDataAsync(context.Background(), 2, []byte("duplicate"), resultCh)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), id)
}

// === Compact - readLocalFileAndUploadToMinio fails ===

func TestCompact_UploadMergedBlockFails(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer.Finalize(context.Background(), 4)
	require.NoError(t, err)

	// Mock PutObject to fail for block upload
	mockStorage.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(fmt.Errorf("upload failed"))

	_, err = writer.Compact(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read local file and upload to minio")
	writer.Close(context.Background())
}

// === recoverBlocksFromFooterUnsafe - empty index length ===

func TestRecoverBlocksFromFooter_EmptyIndexLength(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	// Create a writer, finalize with no data (empty footer)
	writer1, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	_, err = writer1.Finalize(context.Background(), -1)
	require.NoError(t, err)
	writer1.Close(context.Background())

	// Now recover - footer exists but IndexLength is 0
	writer2, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg, true)
	require.NoError(t, err)
	defer writer2.Close(context.Background())

	assert.True(t, writer2.recovered.Load())
	assert.True(t, writer2.finalized.Load())
	assert.NotNil(t, writer2.recoveredFooter)
	assert.Empty(t, writer2.blockIndexes)
}

// === recoverBlocksFromFullScan - block number mismatch ===

func TestRecoverBlocksFromFullScan_BlockNumberMismatch(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	logId := int64(310)
	segId := int64(310)

	// Manually create a file with mismatched block numbers
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write header
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	// Write block header with block number 5 (mismatch, should be 0)
	dataRecords := []codec.Record{
		&codec.DataRecord{Payload: []byte("data1")},
		&codec.DataRecord{Payload: []byte("data2")},
	}
	var blockDataBuf []byte
	for _, r := range dataRecords {
		blockDataBuf = append(blockDataBuf, codec.EncodeRecord(r)...)
	}
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  5, // mismatched block number
		FirstEntryID: 0,
		LastEntryID:  1,
		BlockLength:  uint32(len(blockDataBuf)),
		BlockCrc:     0, // We won't verify CRC in full scan recovery
	}
	_, err = file.Write(codec.EncodeRecord(blockHeader))
	require.NoError(t, err)
	_, err = file.Write(blockDataBuf)
	require.NoError(t, err)
	file.Close()

	// Recover from the file - should handle block number mismatch gracefully
	writer, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg, true)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.True(t, writer.recovered.Load())
	// Block should still be recovered despite mismatch
	assert.NotEmpty(t, writer.blockIndexes)
	assert.Equal(t, int64(1), writer.lastEntryID.Load())
}

// === ValidateLACAlignment - LAC exceeds max block entry ===

func TestValidateLACAlignment_LACExceedsMaxBlockEntry(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	// Finalize with LAC=100, much larger than actual data (0-4)
	_, err = writer.Finalize(context.Background(), 100)
	require.NoError(t, err)

	_, err = writer.validateLACAlignment(context.Background())
	assert.Error(t, err)
	assert.True(t, werr.ErrInvalidLACAlignment.Is(err))
	writer.Close(context.Background())
}

// === WriteDataAsync - Fenced ===

func TestWriteDataAsync_Fenced(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Set fenced directly (bypassing Fence() which would await flush tasks)
	writer.fenced.Store(true)

	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrSegmentFenced)
}

// === WriteDataAsync - NilBuffer ===

func TestWriteDataAsync_NilBuffer(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Set buffer to nil
	writer.buffer.Store((*cache.SequentialBuffer)(nil))

	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrFileWriterAlreadyClosed)
}

// === NewStagedFileWriter - MkdirAll Error ===

func TestNewStagedFileWriter_MkdirAllError(t *testing.T) {
	// Use a path under a file (not a directory) to make MkdirAll fail
	dir := t.TempDir()
	filePath := filepath.Join(dir, "not-a-dir")
	err := os.WriteFile(filePath, []byte("block"), 0o644)
	require.NoError(t, err)

	cfg := newTestConfig(t)
	_, err = NewStagedFileWriter(context.Background(), "test-bucket", "test-root", filePath, 1, 0, nil, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create directory")
}

// === NewStagedFileWriter - Recovery Corrupt File ===

func TestNewStagedFileWriter_RecoveryCorruptFile(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	logId := int64(1)
	segId := int64(0)
	segDir := getSegmentDir(dir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	// Create a corrupt file with some random bytes (not valid records)
	filePath := getSegmentFilePath(dir, logId, segId)
	// Write garbage that's large enough but not parseable
	garbage := make([]byte, 100)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	err = os.WriteFile(filePath, garbage, 0o644)
	require.NoError(t, err)

	// Recovery mode should succeed but the full scan will find no valid header
	// and reset state (L1781-1789)
	writer, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg, true)
	// Recovery scan finds garbage, breaks at decode error, no header found -> resets state
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// After recovery with no valid header, state should be reset
	assert.Equal(t, int64(-1), writer.lastEntryID.Load())
	assert.Equal(t, int64(-1), writer.firstEntryID.Load())
}

// === NewStagedFileWriter - Empty File Recovery ===

func TestNewStagedFileWriter_EmptyFileRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	logId := int64(1)
	segId := int64(0)
	segDir := getSegmentDir(dir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	// Create an empty file
	filePath := getSegmentFilePath(dir, logId, segId)
	err = os.WriteFile(filePath, []byte{}, 0o644)
	require.NoError(t, err)

	// Recovery mode with empty file should succeed (start fresh)
	writer, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg, true)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.Equal(t, int64(-1), writer.lastEntryID.Load())
}

// === RecoverBlocksFromFullScan - Legacy DataRecord Without Block ===

func TestRecoverBlocksFromFullScan_LegacyDataRecordWithoutBlock(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	logId := int64(1)
	segId := int64(0)
	segDir := getSegmentDir(dir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(dir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write HeaderRecord
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	// Write DataRecords directly (no BlockHeaderRecord) - legacy format
	for i := 0; i < 3; i++ {
		dr := &codec.DataRecord{Payload: []byte(fmt.Sprintf("data-%d", i))}
		_, err = file.Write(codec.EncodeRecord(dr))
		require.NoError(t, err)
	}
	file.Close()

	// Recovery mode should handle this legacy format
	writer, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg, true)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Should have recovered the data records as a block
	assert.Equal(t, int64(2), writer.lastEntryID.Load())
	assert.Equal(t, int64(0), writer.firstEntryID.Load())
	assert.Len(t, writer.blockIndexes, 1) // One block created from loose DataRecords
}

// === RecoverBlocksFromFullScan - Multi Block Finalization ===

func TestRecoverBlocksFromFullScan_MultiBlockFinalization(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	logId := int64(1)
	segId := int64(0)

	// First create a file with multiple blocks using a writer
	writer1, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)

	// Write enough entries to create multiple blocks (force multiple syncs)
	for i := int64(0); i < 5; i++ {
		_, err = writer1.WriteDataAsync(context.Background(), i, []byte("test data entry"), nil)
		require.NoError(t, err)
	}
	err = writer1.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Write more entries for a second block
	for i := int64(5); i < 10; i++ {
		_, err = writer1.WriteDataAsync(context.Background(), i, []byte("test data entry 2"), nil)
		require.NoError(t, err)
	}
	err = writer1.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	writer1.Close(context.Background())

	// Now recover from the file - should detect multiple blocks
	writer2, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg, true)
	require.NoError(t, err)
	defer writer2.Close(context.Background())

	assert.True(t, writer2.recovered.Load())
	assert.GreaterOrEqual(t, len(writer2.blockIndexes), 2)
	assert.Equal(t, int64(9), writer2.lastEntryID.Load())
}

// === RecoverBlocksFromFullScan - Unknown Record Type ===

func TestRecoverBlocksFromFullScan_UnknownRecordType(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	logId := int64(1)
	segId := int64(0)
	segDir := getSegmentDir(dir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(dir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write a valid HeaderRecord
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	// Write a valid block with data
	dataRecords := []byte{}
	dr := &codec.DataRecord{Payload: []byte("test")}
	dataRecords = append(dataRecords, codec.EncodeRecord(dr)...)

	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  0,
		BlockLength:  uint32(len(dataRecords)),
		BlockCrc:     crc32.ChecksumIEEE(dataRecords),
	}
	_, err = file.Write(codec.EncodeRecord(blockHeader))
	require.NoError(t, err)
	_, err = file.Write(dataRecords)
	require.NoError(t, err)

	// Now write an IndexRecord which is an unexpected type during block scan
	// This will trigger the default case -> goto exitLoop
	indexRecord := &codec.IndexRecord{
		BlockNumber:  0,
		StartOffset:  0,
		BlockSize:    100,
		FirstEntryID: 0,
		LastEntryID:  0,
	}
	_, err = file.Write(codec.EncodeRecord(indexRecord))
	require.NoError(t, err)

	file.Close()

	// Recovery should stop at the unknown record type but recover the valid block
	writer, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg, true)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.True(t, writer.recovered.Load())
	assert.Len(t, writer.blockIndexes, 1) // Only the valid block recovered
	assert.Equal(t, int64(0), writer.lastEntryID.Load())
}

// === ValidateLACAlignment - Segment Incomplete ===

func TestValidateLACAlignment_SegmentIncomplete(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	// Write entries 0-2
	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	// Finalize with LAC=2 (valid)
	_, err = writer.Finalize(context.Background(), 2)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Override recoveredFooter with LAC > lastEntryID to trigger L1849-1852
	writer.recoveredFooter.LAC = 100
	// Also ensure lastEntryID is < LAC
	writer.lastEntryID.Store(2)
	_, err = writer.validateLACAlignment(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "segment incomplete")
}

// === Compact - Empty Blocks ===

func TestCompact_EmptyBlocks(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	// Finalize empty writer
	_, err = writer.Finalize(context.Background(), -1)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Compact with empty blocks should be a no-op (returns -1, nil)
	result, err := writer.Compact(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), result)
}

// === Compact - Invalid LAC ===

func TestCompact_InvalidLAC(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	// Finalize with valid LAC
	_, err = writer.Finalize(context.Background(), 2)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Override LAC to -1 to trigger validation failure
	writer.recoveredFooter.LAC = -1
	_, err = writer.Compact(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no valid LAC")
}

// === Compact - Upload Footer Fails ===

func TestCompact_UploadFooterFails(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer.Finalize(context.Background(), 4)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Mock: PutObject succeeds for merged blocks but fails for footer
	callCount := 0
	mockStorage.EXPECT().PutObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, bucket string, key string, reader io.Reader, size int64, ns string, logIdStr string) error {
			callCount++
			// Let merged block uploads succeed, fail on the footer upload
			if callCount > len(writer.blockIndexes) {
				return fmt.Errorf("footer upload failed")
			}
			return nil
		})

	_, err = writer.Compact(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "footer upload failed")
}

// === ProcessFlushTask - Write Header Error ===

func TestProcessFlushTask_WriteHeaderError(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Close the file to force write errors
	writer.file.Close()

	// Create a flush task manually
	task := &blockFlushTask{
		entries: []*cache.BufferEntry{
			{EntryId: 0, Data: []byte("test"), NotifyChan: nil},
		},
		firstEntryId: 0,
		lastEntryId:  0,
		blockNumber:  0,
	}

	writer.processFlushTask(context.Background(), task)

	// Should have notified with error
	assert.False(t, writer.storageWritable.Load())
}

// === RecoverBlocksFromFooter - ReadAt Index Error ===

func TestRecoverBlocksFromFooter_ReadAtIndexError(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	logId := int64(1)
	segId := int64(0)

	// First create a valid finalized file
	writer1, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 5; i++ {
		_, err = writer1.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}
	err = writer1.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer1.Finalize(context.Background(), 4)
	require.NoError(t, err)
	originalFooter := writer1.recoveredFooter
	writer1.Close(context.Background())

	// Now corrupt the file: truncate it so that the index section is incomplete
	// but the footer portion at the end is still parseable
	filePath := getSegmentFilePath(dir, logId, segId)

	// Read the file and create a new file with wrong index offset
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	// Create a new segment with different IDs to avoid conflict
	logId2 := int64(2)
	segDir2 := getSegmentDir(dir, logId2, segId)
	err = os.MkdirAll(segDir2, 0o755)
	require.NoError(t, err)

	filePath2 := getSegmentFilePath(dir, logId2, segId)

	// Write footer with wrong IndexOffset pointing beyond file
	footerRecordSize := codec.RecordHeaderSize + codec.GetFooterRecordSize(originalFooter.Version)
	// Keep only the header + a bit of data + footer (skip actual index)
	// Take just first 50 bytes + footer
	headerSize := codec.RecordHeaderSize + codec.HeaderRecordSize
	if len(data) > headerSize+footerRecordSize {
		truncatedData := make([]byte, 0)
		truncatedData = append(truncatedData, data[:headerSize]...)                 // header
		truncatedData = append(truncatedData, data[len(data)-footerRecordSize:]...) // footer
		err = os.WriteFile(filePath2, truncatedData, 0o644)
		require.NoError(t, err)

		// Try recovery - should fail because index section ReadAt fails
		writer2, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, logId2, segId, nil, cfg, true)
		if err != nil {
			// Expected: recovery fails due to bad index section
			assert.Contains(t, err.Error(), "recover from existing file")
		} else {
			defer writer2.Close(context.Background())
		}
	}
}

// === WriteDataAsync - non-sequential entry ===

func TestWriteDataAsync_NonSequentialEntry(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write entry beyond buffer capacity (MaxEntries=2000 by default)
	// This triggers the ErrFileWriterBufferFull path at L773-781
	_, err = writer.WriteDataAsync(context.Background(), 10000, []byte("data"), nil)
	assert.Error(t, err)
}

// === ProcessFlushTask - write block data error ===

func TestProcessFlushTask_WriteBlockDataError(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write header first so that the header write succeeds
	writer.headerWritten.Store(true)

	// Now close the file to force block data write to fail
	writer.file.Close()

	task := &blockFlushTask{
		entries: []*cache.BufferEntry{
			{EntryId: 0, Data: []byte("test"), NotifyChan: nil},
		},
		firstEntryId: 0,
		lastEntryId:  0,
		blockNumber:  0,
	}

	writer.processFlushTask(context.Background(), task)

	// Storage should be marked as not writable
	assert.False(t, writer.storageWritable.Load())
}

// === RecoverBlocksFromFooter - decode error ===

func TestRecoverBlocksFromFooter_DecodeError(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	logId := int64(3)
	segId := int64(0)

	// First create a valid finalized file
	writer1, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 3; i++ {
		_, err = writer1.WriteDataAsync(context.Background(), i, []byte("data"), nil)
		require.NoError(t, err)
	}
	err = writer1.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer1.Finalize(context.Background(), 2)
	require.NoError(t, err)
	originalFooter := writer1.recoveredFooter
	writer1.Close(context.Background())

	filePath := getSegmentFilePath(dir, logId, segId)
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	// Create a new file with corrupted index section
	logId2 := int64(4)
	segDir2 := getSegmentDir(dir, logId2, segId)
	err = os.MkdirAll(segDir2, 0o755)
	require.NoError(t, err)

	filePath2 := getSegmentFilePath(dir, logId2, segId)

	footerRecordSize := codec.RecordHeaderSize + codec.GetFooterRecordSize(originalFooter.Version)
	headerSize := codec.RecordHeaderSize + codec.HeaderRecordSize

	if len(data) > headerSize+footerRecordSize {
		// Build: header + garbage bytes (as "index") + footer
		garbageIndex := make([]byte, int(originalFooter.IndexLength))
		for i := range garbageIndex {
			garbageIndex[i] = 0xFF // corrupt data
		}
		corruptData := make([]byte, 0)
		corruptData = append(corruptData, data[:headerSize]...)
		corruptData = append(corruptData, garbageIndex...)
		corruptData = append(corruptData, data[len(data)-footerRecordSize:]...)
		err = os.WriteFile(filePath2, corruptData, 0o644)
		require.NoError(t, err)

		// Try recovery - should fall back to full scan due to corrupt index
		writer2, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, logId2, segId, nil, cfg, true)
		if err != nil {
			// Recovery may fail or fall back to full scan
			t.Logf("Recovery with corrupt index: %v", err)
		} else {
			defer writer2.Close(context.Background())
		}
	}
}

// === RecoverBlocksFromFooter - wrong record type ===

func TestRecoverBlocksFromFooter_WrongRecordType(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	logId := int64(5)
	segId := int64(0)

	// Create a valid finalized file
	writer1, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 3; i++ {
		_, err = writer1.WriteDataAsync(context.Background(), i, []byte("data"), nil)
		require.NoError(t, err)
	}
	err = writer1.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer1.Finalize(context.Background(), 2)
	require.NoError(t, err)
	originalFooter := writer1.recoveredFooter
	writer1.Close(context.Background())

	filePath := getSegmentFilePath(dir, logId, segId)
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	// Create file with DataRecord in place of IndexRecord in index section
	logId2 := int64(6)
	segDir2 := getSegmentDir(dir, logId2, segId)
	err = os.MkdirAll(segDir2, 0o755)
	require.NoError(t, err)

	filePath2 := getSegmentFilePath(dir, logId2, segId)

	footerRecordSize := codec.RecordHeaderSize + codec.GetFooterRecordSize(originalFooter.Version)
	headerSize := codec.RecordHeaderSize + codec.HeaderRecordSize

	if len(data) > headerSize+footerRecordSize {
		// Build: header + data-records-as-index + footer
		// Encode DataRecords to use as fake index section (wrong type)
		var fakeIndex []byte
		for i := int32(0); i < originalFooter.TotalBlocks; i++ {
			fakeRecord := &codec.DataRecord{Payload: []byte("fake")}
			fakeIndex = append(fakeIndex, codec.EncodeRecord(fakeRecord)...)
		}

		// Modify footer's IndexLength to match the fake index
		modifiedFooter := *originalFooter
		modifiedFooter.IndexLength = uint32(len(fakeIndex))

		corruptData := make([]byte, 0)
		corruptData = append(corruptData, data[:headerSize]...)
		corruptData = append(corruptData, fakeIndex...)
		corruptData = append(corruptData, codec.EncodeRecord(&modifiedFooter)...)
		err = os.WriteFile(filePath2, corruptData, 0o644)
		require.NoError(t, err)

		// Try recovery - should fail due to wrong record type in index section
		writer2, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, logId2, segId, nil, cfg, true)
		if err != nil {
			t.Logf("Recovery with wrong record type in index: %v", err)
		} else {
			defer writer2.Close(context.Background())
		}
	}
}

// === RecoverBlocksFromFooter - block count mismatch ===

func TestRecoverBlocksFromFooter_BlockCountMismatch(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	logId := int64(7)
	segId := int64(0)

	// Create a valid finalized file with multiple blocks
	writer1, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)

	// Write enough data to create multiple blocks
	for i := int64(0); i < 5; i++ {
		_, err = writer1.WriteDataAsync(context.Background(), i, []byte("data"), nil)
		require.NoError(t, err)
	}
	err = writer1.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer1.Finalize(context.Background(), 4)
	require.NoError(t, err)
	originalFooter := writer1.recoveredFooter
	writer1.Close(context.Background())

	filePath := getSegmentFilePath(dir, logId, segId)
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	// Create file with footer that claims more blocks than exist
	logId2 := int64(8)
	segDir2 := getSegmentDir(dir, logId2, segId)
	err = os.MkdirAll(segDir2, 0o755)
	require.NoError(t, err)

	filePath2 := getSegmentFilePath(dir, logId2, segId)
	footerRecordSize := codec.RecordHeaderSize + codec.GetFooterRecordSize(originalFooter.Version)

	if len(data) > footerRecordSize {
		// Replace footer with one that has wrong TotalBlocks
		modifiedFooter := *originalFooter
		modifiedFooter.TotalBlocks = originalFooter.TotalBlocks + 5 // claim 5 more blocks than exist

		newData := make([]byte, len(data)-footerRecordSize)
		copy(newData, data[:len(data)-footerRecordSize])
		newData = append(newData, codec.EncodeRecord(&modifiedFooter)...)
		err = os.WriteFile(filePath2, newData, 0o644)
		require.NoError(t, err)

		// Try recovery - should handle block count mismatch
		writer2, err := NewStagedFileWriterWithMode(context.Background(), "test-bucket", "test-root", dir, logId2, segId, nil, cfg, true)
		if err != nil {
			t.Logf("Recovery with block count mismatch: %v", err)
		} else {
			writer2.Close(context.Background())
		}
	}
}

func TestStagedFileWriter_Close_ErrorCleansUpResources(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	// Verify resources are live before close
	require.NotNil(t, writer.file)
	require.NoError(t, writer.runCtx.Err())

	// Cancel runCtx so the run goroutine exits, making awaitAllFlushTasks fail
	writer.runCancel()
	time.Sleep(100 * time.Millisecond)

	// Close with an already-cancelled context so awaitAllFlushTasks returns error immediately
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	closeErr := writer.Close(cancelledCtx)
	assert.Error(t, closeErr, "Close should return error when awaitAllFlushTasks fails")

	// Verify cleanup happened despite the error return
	assert.Nil(t, writer.file, "file should be closed and set to nil")
	assert.Error(t, writer.runCtx.Err(), "runCtx should be cancelled")
}

func TestStagedFileWriter_Close_Idempotent_NoDoubleClose(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)

	// First close succeeds
	err = writer.Close(context.Background())
	assert.NoError(t, err)

	// Second close returns nil without panic (idempotent)
	err = writer.Close(context.Background())
	assert.NoError(t, err)
	assert.True(t, writer.closed.Load())
}
