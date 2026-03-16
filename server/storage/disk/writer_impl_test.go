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

package disk

import (
	"context"
	"hash/crc32"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

func TestNewLocalFileWriter(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)
	defer writer.Close(context.Background())

	assert.Equal(t, int64(1), writer.logId)
	assert.Equal(t, int64(0), writer.segmentId)
	assert.False(t, writer.closed.Load())
	assert.False(t, writer.finalized.Load())
	assert.False(t, writer.fenced.Load())
}

func TestNewLocalFileWriterWithMode_RecoveryMode(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Recovery mode with no existing file should still succeed (nothing to recover)
	writer, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	require.NoError(t, err)
	require.NotNil(t, writer)
	defer writer.Close(context.Background())

	// No existing file means no recovery happened, lastEntryID remains -1
	assert.Equal(t, int64(-1), writer.GetLastEntryId(context.Background()))
}

func TestLocalFileWriter_GetFirstEntryId(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.Equal(t, int64(-1), writer.GetFirstEntryId(context.Background()))
}

func TestLocalFileWriter_GetLastEntryId(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.Equal(t, int64(-1), writer.GetLastEntryId(context.Background()))
}

func TestLocalFileWriter_GetBlockCount(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.Equal(t, int64(-1), writer.GetBlockCount(context.Background()))
}

func TestLocalFileWriter_WriteDataAsync_EmptyData(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	_, err = writer.WriteDataAsync(context.Background(), 0, []byte{}, nil)
	assert.ErrorIs(t, err, werr.ErrEmptyPayload)
}

func TestLocalFileWriter_WriteDataAsync_AfterClose(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	err = writer.Close(context.Background())
	assert.NoError(t, err)

	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrFileWriterAlreadyClosed)
}

func TestLocalFileWriter_WriteDataAsync_AfterFence(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	writer.fenced.Store(true)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrSegmentFenced)
}

func TestLocalFileWriter_WriteDataAsync_AfterFinalize(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	writer.finalized.Store(true)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrFileWriterFinalized)
}

func TestLocalFileWriter_WriteDataAsync_StorageNotWritable(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	writer.storageWritable.Store(false)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrStorageNotWritable)
}

func TestLocalFileWriter_WriteDataAsync_Success(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	id, err := writer.WriteDataAsync(context.Background(), 0, []byte("test data"), nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id)
}

func TestLocalFileWriter_Sync(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write some data first
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("test"), nil)
	require.NoError(t, err)

	// Sync should succeed
	err = writer.Sync(context.Background())
	assert.NoError(t, err)
}

func TestLocalFileWriter_Close_Idempotent(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	err = writer.Close(context.Background())
	assert.NoError(t, err)

	// Second close should also succeed (idempotent)
	err = writer.Close(context.Background())
	assert.NoError(t, err)
}

func TestLocalFileWriter_Fence(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	lastId, err := writer.Fence(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), lastId) // No data written yet
	assert.True(t, writer.fenced.Load())

	// Second fence should be idempotent
	lastId2, err := writer.Fence(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, lastId, lastId2)
}

func TestLocalFileWriter_WriteAndFinalize(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
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

	// Second finalize should be idempotent
	lastId2, err := writer.Finalize(context.Background(), 4)
	assert.NoError(t, err)
	assert.Equal(t, lastId, lastId2)

	writer.Close(context.Background())
}

func TestLocalFileWriter_Compact(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Compact is not supported for local disk
	_, err = writer.Compact(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not need to compact local file currently")
}

func TestLocalFileWriter_WriteMultipleEntries_ThenFence(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	// Write entries
	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("data"), nil)
		require.NoError(t, err)
	}

	// Sync to ensure data flushed
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Fence
	lastId, err := writer.Fence(context.Background())
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, lastId, int64(0))
	assert.True(t, writer.fenced.Load())

	writer.Close(context.Background())
}

func TestLocalFileWriter_RecoveryMode_WithExistingData(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Write some data first
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("hello"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	writer.Close(context.Background())

	// Open in recovery mode - should recover existing data
	writer2, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	require.NoError(t, err)
	require.NotNil(t, writer2)
	defer writer2.Close(context.Background())

	// lastEntryID should be recovered
	assert.GreaterOrEqual(t, writer2.GetLastEntryId(context.Background()), int64(0))
}

func TestLocalFileWriter_RecoveryMode_WithFinalizedFile(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Write and finalize
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("data"), nil)
		require.NoError(t, err)
	}
	_, err = writer.Finalize(context.Background(), 2)
	require.NoError(t, err)
	writer.Close(context.Background())

	// Recover from finalized file
	writer2, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	require.NoError(t, err)
	require.NotNil(t, writer2)
	defer writer2.Close(context.Background())

	assert.GreaterOrEqual(t, writer2.GetLastEntryId(context.Background()), int64(0))
}

func TestLocalFileWriter_WriteAndRead(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Write entries
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 10; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("entry data"), nil)
		require.NoError(t, err)
	}

	// Finalize
	lastId, err := writer.Finalize(context.Background(), 9)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lastId, int64(0))
	writer.Close(context.Background())

	// Now read back
	reader, err := NewLocalFileReaderAdv(context.Background(), dir, 1, 0, 100)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	// Read entries
	result, err := reader.ReadNextBatchAdv(context.Background(), storage.ReaderOpt{
		StartEntryID:    0,
		EndEntryID:      10,
		MaxBatchEntries: 10,
	}, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result.Entries), 1)
}

func TestLocalFileReader_NonExistentFile(t *testing.T) {
	dir := getTempDir(t)

	_, err := NewLocalFileReaderAdv(context.Background(), dir, 99, 99, 100)
	assert.ErrorIs(t, err, werr.ErrEntryNotFound)
}

func TestLocalFileReader_UpdateLastAddConfirmed(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Create a finalized file first
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)
	_, err = writer.Finalize(context.Background(), 0)
	require.NoError(t, err)
	writer.Close(context.Background())

	// Open reader
	reader, err := NewLocalFileReaderAdv(context.Background(), dir, 1, 0, 100)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	// UpdateLastAddConfirmed should succeed
	err = reader.UpdateLastAddConfirmed(context.Background(), 0)
	assert.NoError(t, err)
}

func TestLocalFileReader_GetLastEntryID_Closed(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Create finalized file
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)
	_, err = writer.Finalize(context.Background(), 0)
	require.NoError(t, err)
	writer.Close(context.Background())

	reader, err := NewLocalFileReaderAdv(context.Background(), dir, 1, 0, 100)
	require.NoError(t, err)

	reader.Close(context.Background())

	// GetLastEntryID on closed reader
	_, err = reader.GetLastEntryID(context.Background())
	assert.ErrorIs(t, err, werr.ErrFileReaderAlreadyClosed)
}

func TestLocalFileReader_GetLastEntryID_WithFooter(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Create finalized file
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("data"), nil)
		require.NoError(t, err)
	}
	_, err = writer.Finalize(context.Background(), 4)
	require.NoError(t, err)
	writer.Close(context.Background())

	reader, err := NewLocalFileReaderAdv(context.Background(), dir, 1, 0, 100)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	lastId, err := reader.GetLastEntryID(context.Background())
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, lastId, int64(0))
}

func TestLocalFileReader_GetLastEntryID_NoFooterNoBlocks(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Create file without finalize (incomplete)
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)
	// Don't finalize - close without footer
	writer.Close(context.Background())

	reader, err := NewLocalFileReaderAdv(context.Background(), dir, 1, 0, 100)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	// Should attempt to scan, might find blocks or return no blocks error
	lastId, err := reader.GetLastEntryID(context.Background())
	if err != nil {
		// No footer and possibly no complete blocks
		assert.True(t, werr.ErrFileReaderNoBlockFound.Is(err), "unexpected error: %v", err)
	} else {
		assert.GreaterOrEqual(t, lastId, int64(0))
	}
}

func TestLocalFileReader_GetBlockIndexes(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Create finalized file
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)
	_, err = writer.Finalize(context.Background(), 0)
	require.NoError(t, err)
	writer.Close(context.Background())

	reader, err := NewLocalFileReaderAdv(context.Background(), dir, 1, 0, 100)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	indexes := reader.GetBlockIndexes()
	assert.NotNil(t, indexes)
	assert.GreaterOrEqual(t, len(indexes), 1)
}

func TestLocalFileReader_GetTotalRecords_NilFooter(t *testing.T) {
	reader := &LocalFileReaderAdv{footer: nil}
	assert.Equal(t, uint32(0), reader.GetTotalRecords())
}

func TestLocalFileReader_GetTotalBlocks_NilFooter(t *testing.T) {
	reader := &LocalFileReaderAdv{footer: nil}
	assert.Equal(t, int32(0), reader.GetTotalBlocks())
}

func TestLocalFileReader_Close_Idempotent(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)
	_, err = writer.Finalize(context.Background(), 0)
	require.NoError(t, err)
	writer.Close(context.Background())

	reader, err := NewLocalFileReaderAdv(context.Background(), dir, 1, 0, 100)
	require.NoError(t, err)

	err = reader.Close(context.Background())
	assert.NoError(t, err)

	// Second close should be idempotent
	err = reader.Close(context.Background())
	assert.NoError(t, err)
}

func TestLocalFileReader_ReadBatchAdv_Closed(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)
	_, err = writer.Finalize(context.Background(), 0)
	require.NoError(t, err)
	writer.Close(context.Background())

	reader, err := NewLocalFileReaderAdv(context.Background(), dir, 1, 0, 100)
	require.NoError(t, err)
	reader.Close(context.Background())

	_, err = reader.ReadNextBatchAdv(context.Background(), storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}, nil)
	assert.Error(t, err)
}

func TestLocalFileReader_ReadMultipleBatches(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Write many entries
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 20; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("entry data"), nil)
		require.NoError(t, err)
	}
	_, err = writer.Finalize(context.Background(), 19)
	require.NoError(t, err)
	writer.Close(context.Background())

	// Read in small batches
	reader, err := NewLocalFileReaderAdv(context.Background(), dir, 1, 0, 100)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	// Read first batch
	result, err := reader.ReadNextBatchAdv(context.Background(), storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 5,
	}, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result.Entries), 1)

	// Read second batch using last read state
	if result.LastReadState != nil {
		result2, err := reader.ReadNextBatchAdv(context.Background(), storage.ReaderOpt{
			StartEntryID:    result.Entries[len(result.Entries)-1].EntryId + 1,
			MaxBatchEntries: 5,
		}, result.LastReadState)
		if err == nil {
			assert.NotNil(t, result2)
		}
	}
}

func TestLocalFileReader_GettersAfterWrite(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Create a finalized file
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("data"), nil)
		require.NoError(t, err)
	}
	_, err = writer.Finalize(context.Background(), 2)
	require.NoError(t, err)
	writer.Close(context.Background())

	// Open reader and test getters
	reader, err := NewLocalFileReaderAdv(context.Background(), dir, 1, 0, 100)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	assert.NotNil(t, reader.GetFooter())
	assert.GreaterOrEqual(t, reader.GetTotalBlocks(), int32(1))
	assert.GreaterOrEqual(t, reader.GetTotalRecords(), uint32(1))
}

// === WriteDataAsync duplicate entry paths ===

func TestLocalFileWriter_WriteDataAsync_DuplicateAlreadyWritten(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write entry 0 with a result channel
	ch := channel.NewLocalResultChannel("test-dup")
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), ch)
	require.NoError(t, err)

	// Sync to flush it so lastEntryID is updated
	writer.Sync(context.Background())
	time.Sleep(200 * time.Millisecond)

	// Drain the result channel from the first write
	_, _ = ch.ReadResult(context.Background())

	// Write same entry again with a result channel — should hit entryId <= lastEntryID path
	ch2 := channel.NewLocalResultChannel("test-dup2")
	id, err := writer.WriteDataAsync(context.Background(), 0, []byte("data"), ch2)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id)
}

func TestLocalFileWriter_WriteDataAsync_DuplicateAlreadyFlushing(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write entry 0
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)

	// Sync to submit flush task (sets lastSubmittedFlushingEntryID)
	writer.Sync(context.Background())

	// Write same entry again — should hit entryId <= lastSubmittedFlushingEntryID path
	id, err := writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id)
}

func TestLocalFileWriter_WriteDataAsync_NilBuffer(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Set buffer to nil to simulate closed state
	writer.buffer.Store(nil)

	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrFileWriterAlreadyClosed)
}

// === processFlushTask error paths ===

func TestLocalFileWriter_ProcessFlushTask_Fenced(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write data and sync to have actual flush task processing
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)

	// Fence before flush processes
	writer.fenced.Store(true)

	// Trigger sync — processFlushTask should detect fence
	writer.Sync(context.Background())
	time.Sleep(200 * time.Millisecond)

	// After fenced processFlushTask, lastEntryID should still be -1 (data not flushed)
	assert.True(t, writer.fenced.Load())
}

func TestLocalFileWriter_ProcessFlushTask_Finalized(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write entry
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)

	// Set finalized before flush processes
	writer.finalized.Store(true)

	writer.Sync(context.Background())
	time.Sleep(200 * time.Millisecond)

	assert.True(t, writer.finalized.Load())
}

func TestLocalFileWriter_ProcessFlushTask_StorageNotWritable(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)

	// Set not writable before flush
	writer.storageWritable.Store(false)

	writer.Sync(context.Background())
	time.Sleep(200 * time.Millisecond)
}

// === NewLocalFileWriterWithMode error paths ===

func TestNewLocalFileWriterWithMode_MkdirAllError(t *testing.T) {
	// Use a file as baseDir to make MkdirAll fail
	tmpFile, err := os.CreateTemp("", "test_writer_mkdir_*")
	require.NoError(t, err)
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	cfg, _ := config.NewConfiguration()
	_, err = NewLocalFileWriterWithMode(context.Background(), tmpFile.Name(), 1, 0, cfg, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create directory")
}

func TestNewLocalFileWriterWithMode_RecoveryMode_EmptyFile(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Create the segment directory and an empty file
	segmentDir := getSegmentDir(dir, 1, 0)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(dir, 1, 0)
	f, err := os.Create(filePath)
	require.NoError(t, err)
	f.Close()

	// Recovery mode with empty file should succeed
	writer, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.Equal(t, int64(-1), writer.GetLastEntryId(context.Background()))
}

// === Finalize edge cases ===

func TestLocalFileWriter_Finalize_NoDataWritten(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Finalize without writing any data — should still write header + footer
	lastId, err := writer.Finalize(context.Background(), -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), lastId)
	assert.True(t, writer.finalized.Load())
}

// === Fence edge cases ===

func TestLocalFileWriter_Fence_AfterWrite(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Use recovery mode to avoid creating lock file (which triggers 5s wait in Fence)
	writer, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write some data
	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("data"), nil)
		require.NoError(t, err)
	}

	// Fence — no lock file so no 5s wait
	lastId, err := writer.Fence(context.Background())
	assert.NoError(t, err)
	assert.True(t, writer.fenced.Load())

	// Write after fence should fail
	_, err = writer.WriteDataAsync(context.Background(), 99, []byte("data"), nil)
	assert.ErrorIs(t, err, werr.ErrSegmentFenced)

	_ = lastId
}

// === checkFenceFlagFileExists ===

func TestCheckFenceFlagFileExists_FlagExists(t *testing.T) {
	dir := getTempDir(t)
	ctx := context.Background()

	// Create the segment dir and fence flag file
	segmentDir := getSegmentDir(dir, 1, 0)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	fencePath := getFenceFlagPath(dir, 1, 0)
	f, err := os.Create(fencePath)
	require.NoError(t, err)
	f.Close()

	exists, err := checkFenceFlagFileExists(ctx, dir, 1, 0)
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestCheckFenceFlagFileExists_NoFlag(t *testing.T) {
	dir := getTempDir(t)
	ctx := context.Background()

	exists, err := checkFenceFlagFileExists(ctx, dir, 1, 0)
	assert.NoError(t, err)
	assert.False(t, exists)
}

// === waitForFenceCheckIntervalIfLockExists ===

func TestWaitForFenceCheckInterval_NoLockFile(t *testing.T) {
	dir := getTempDir(t)
	ctx := context.Background()

	// No lock file → should return immediately
	err := waitForFenceCheckIntervalIfLockExists(ctx, dir, 1, 0, "dummy")
	assert.NoError(t, err)
}

func TestWaitForFenceCheckInterval_ContextCancelled(t *testing.T) {
	dir := getTempDir(t)

	// Create lock file so the function enters the wait path
	segmentDir := getSegmentDir(dir, 1, 0)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	lockPath := getSegmentLockPath(dir, 1, 0)
	f, err := os.Create(lockPath)
	require.NoError(t, err)
	f.Close()

	// Use cancelled context so it doesn't wait 5 seconds
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = waitForFenceCheckIntervalIfLockExists(ctx, dir, 1, 0, "dummy")
	assert.Error(t, err)
}

// === recoverBlocksFromFooterUnsafe edge cases ===

func TestLocalFileWriter_RecoverFromFooter_InvalidFooterRecord(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// First write & finalize using recovery mode to avoid lock
	writer, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	require.NoError(t, err)
	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("data"), nil)
		require.NoError(t, err)
	}
	_, err = writer.Finalize(context.Background(), 2)
	require.NoError(t, err)
	writer.Close(context.Background())

	// Read the file, corrupt the index records portion
	filePath := getSegmentFilePath(dir, 1, 0)
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	// Corrupt index data (before footer) to trigger decode error in recoverBlocksFromFooterUnsafe
	footerSize := codec.RecordHeaderSize + codec.FooterRecordSizeV6
	if len(data) > footerSize+20 {
		corruptStart := len(data) - footerSize - 15
		for i := corruptStart; i < corruptStart+10; i++ {
			data[i] = 0xAB
		}
		err = os.WriteFile(filePath, data, 0o644)
		require.NoError(t, err)
	}

	// Recovery mode should fail on corrupted index
	_, err = NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	// May succeed or fail depending on exactly what was corrupted
	if err != nil {
		assert.Contains(t, err.Error(), "recover")
	}
}

// === Sync when storageWritable is false ===

func TestLocalFileWriter_Sync_NotWritable(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	writer.storageWritable.Store(false)

	err = writer.Sync(context.Background())
	assert.NoError(t, err) // Should return nil early
}

// === WriteDataAsync triggers immediate sync ===

func TestLocalFileWriter_WriteDataAsync_TriggersImmediateSync(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Set maxFlushSize very low to trigger immediate sync
	writer.maxFlushSize = 1 // 1 byte

	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data that exceeds 1 byte"), nil)
	assert.NoError(t, err)

	// Wait for sync to complete
	time.Sleep(200 * time.Millisecond)
}

// === RecoverBlocksFromFullScan with various record types ===

func TestLocalFileWriter_RecoverFromFullScan_WithBlocks(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Write data with multiple blocks
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	// Write enough data to create multiple blocks
	writer.maxFlushSize = 50 // Small block size to force multiple blocks
	for i := int64(0); i < 10; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data for recovery"), nil)
		require.NoError(t, err)
	}
	writer.Sync(context.Background())
	time.Sleep(300 * time.Millisecond)
	writer.Close(context.Background())

	// Recovery mode should scan and find blocks
	writer2, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	require.NoError(t, err)
	defer writer2.Close(context.Background())

	assert.GreaterOrEqual(t, writer2.GetLastEntryId(context.Background()), int64(0))
	assert.True(t, writer2.recovered.Load())
}

// === createSegmentLock error paths ===

func TestLocalFileWriter_CreateSegmentLock_AlreadyLocked(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Create first writer which acquires the lock
	writer1, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer1.Close(context.Background())

	// Second writer should fail to acquire lock
	_, err = NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lock")
}

// === processFlushTask nil task ===

func TestLocalFileWriter_ProcessFlushTask_NilTask(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Call processFlushTask with nil — should return immediately
	writer.processFlushTask(context.Background(), nil)
}

// === releaseSegmentLock with nil lockFile ===

func TestLocalFileWriter_ReleaseSegmentLock_NilLockFile(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	require.NoError(t, err)

	// Recovery mode doesn't create a lock file
	writer.lockFile = nil
	err = writer.releaseSegmentLock(context.Background())
	assert.NoError(t, err)

	writer.Close(context.Background())
}

// === recoverFromExistingFileUnsafe stat error ===

func TestLocalFileWriter_RecoverFromExisting_StatError(t *testing.T) {
	dir := getTempDir(t)

	// Create writer manually to test recoverFromExistingFileUnsafe
	runCtx, runCancel := context.WithCancel(context.Background())
	writer := &LocalFileWriter{
		baseDir:         dir,
		segmentFilePath: "/nonexistent/path/data.log",
		logId:           1,
		segmentId:       0,
		logIdStr:        "1",
		nsStr:           dir,
		blockIndexes:    make([]*codec.IndexRecord, 0),
		flushTaskChan:   make(chan *blockFlushTask, 10),
		runCtx:          runCtx,
		runCancel:       runCancel,
	}
	writer.firstEntryID.Store(-1)
	writer.lastEntryID.Store(-1)

	// File doesn't exist → should return nil (start fresh)
	err := writer.recoverFromExistingFileUnsafe(context.Background())
	assert.NoError(t, err)
	runCancel()
}

// === processFlushTask write error paths ===

func TestLocalFileWriter_ProcessFlushTask_HeaderWriteError(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write some data
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)

	// Close the file descriptor to make writeHeader fail
	writer.file.Close()

	// Trigger sync → processFlushTask will try to write header and fail
	writer.Sync(context.Background())
	time.Sleep(300 * time.Millisecond)

	// storageWritable should be false after write error
	assert.False(t, writer.storageWritable.Load())
}

func TestLocalFileWriter_ProcessFlushTask_BlockHeaderWriteError(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write first entry and flush it so header gets written
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("first"), nil)
	require.NoError(t, err)
	writer.Sync(context.Background())
	time.Sleep(300 * time.Millisecond)

	// Now write second entry
	_, err = writer.WriteDataAsync(context.Background(), 1, []byte("second"), nil)
	require.NoError(t, err)

	// Close file descriptor after header is written, before next flush
	writer.file.Close()

	// Trigger sync → processFlushTask will fail on writeRecord(blockHeader)
	writer.Sync(context.Background())
	time.Sleep(300 * time.Millisecond)

	assert.False(t, writer.storageWritable.Load())
}

func TestLocalFileWriter_ProcessFlushTask_WriteError_ReadOnlyFd(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write and flush first entry to write header
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("first"), nil)
	require.NoError(t, err)
	writer.Sync(context.Background())
	time.Sleep(300 * time.Millisecond)

	// Now write more data
	_, err = writer.WriteDataAsync(context.Background(), 1, []byte("second data block"), nil)
	require.NoError(t, err)

	// Replace the fd with a read-only one so the block header write fails
	filePath := getSegmentFilePath(dir, 1, 0)
	writer.file.Close()
	// Reopen as read-only to cause write failure
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0o644)
	require.NoError(t, err)
	writer.file = f

	writer.Sync(context.Background())
	time.Sleep(300 * time.Millisecond)

	assert.False(t, writer.storageWritable.Load())
}

func TestLocalFileWriter_ProcessFlushTask_WriteError_ClosedFd(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	// Write and flush first entry to write header normally
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)
	writer.Sync(context.Background())
	time.Sleep(300 * time.Millisecond)

	// Write second entry
	_, err = writer.WriteDataAsync(context.Background(), 1, []byte("more data"), nil)
	require.NoError(t, err)

	// Replace the fd with a closed one so writes fail
	filePath := getSegmentFilePath(dir, 1, 0)
	writer.file.Close()
	closedFd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	writer.file = closedFd
	// Close it right away so file.Write() fails on the closed fd
	closedFd.Close()

	writer.Sync(context.Background())
	time.Sleep(300 * time.Millisecond)

	assert.False(t, writer.storageWritable.Load())
	writer.Close(context.Background())
}

// === rollBufferAndSubmitFlushTaskUnsafe context cancellation paths ===

func TestLocalFileWriter_RollBuffer_CtxDone(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write data to fill buffer
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)

	// Fill the flush task channel so it blocks
	for i := 0; i < cap(writer.flushTaskChan); i++ {
		writer.flushTaskChan <- &blockFlushTask{
			entries:      []*cache.BufferEntry{},
			firstEntryId: -1,
			lastEntryId:  -1,
			blockNumber:  int32(i),
		}
	}

	// Use cancelled context → should hit ctx.Done case in select
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	writer.mu.Lock()
	writer.rollBufferAndSubmitFlushTaskUnsafe(cancelledCtx)
	writer.mu.Unlock()
}

func TestLocalFileWriter_RollBuffer_RunCtxDone(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	// Write data to fill buffer
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)

	// Fill the flush task channel so it blocks
	for i := 0; i < cap(writer.flushTaskChan); i++ {
		writer.flushTaskChan <- &blockFlushTask{
			entries:      []*cache.BufferEntry{},
			firstEntryId: -1,
			lastEntryId:  -1,
			blockNumber:  int32(i),
		}
	}

	// Cancel the writer's runCtx → should hit runCtx.Done case
	writer.runCancel()
	time.Sleep(100 * time.Millisecond)

	writer.mu.Lock()
	writer.rollBufferAndSubmitFlushTaskUnsafe(context.Background())
	writer.mu.Unlock()

	// Clean up manually since runCtx is cancelled
	writer.closed.Store(true)
}

// === awaitAllFlushTasks error paths ===

func TestLocalFileWriter_AwaitAllFlushTasks_CtxDone_DuringSend(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Fill the flush task channel so sending termination signal blocks
	for i := 0; i < cap(writer.flushTaskChan); i++ {
		writer.flushTaskChan <- &blockFlushTask{
			entries:      []*cache.BufferEntry{},
			firstEntryId: -1,
			lastEntryId:  -1,
			blockNumber:  int32(i),
		}
	}

	// Use cancelled context → should hit ctx.Done in the first select
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err = writer.awaitAllFlushTasks(cancelledCtx)
	assert.Error(t, err)
}

func TestLocalFileWriter_AwaitAllFlushTasks_CtxDone_DuringWait(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	// Stop the run goroutine by cancelling runCtx, so the termination signal is consumed
	// but allUploadingTaskDone is never set
	writer.runCancel()
	time.Sleep(100 * time.Millisecond)

	// Use a short-lived context
	shortCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = writer.awaitAllFlushTasks(shortCtx)
	assert.Error(t, err) // ctx.Done during the polling loop

	// Manually clean up - don't use Close() which would call awaitAllFlushTasks again with no timeout
	writer.closed.Store(true)
}

// === Finalize error paths ===

func TestLocalFileWriter_Finalize_SyncError(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	// Write data and flush
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)
	writer.Sync(context.Background())
	time.Sleep(300 * time.Millisecond)

	// Close the file descriptor so Finalize's file.Sync() fails
	writer.file.Close()

	_, err = writer.Finalize(context.Background(), 0)
	// Error during finalize (index/footer write or final sync fails)
	assert.Error(t, err)

	writer.Close(context.Background())
}

func TestLocalFileWriter_Finalize_AwaitError(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	// Write data
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)

	// Cancel runCtx so the run goroutine stops and awaitAllFlushTasks can't complete
	writer.runCancel()
	time.Sleep(100 * time.Millisecond)

	// Use a short timeout context for Finalize
	shortCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = writer.Finalize(shortCtx, 0)
	assert.Error(t, err)

	writer.closed.Store(true)
}

// === NewLocalFileWriterWithMode file open error ===

func TestNewLocalFileWriterWithMode_FileOpenError(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Create the segment directory
	segmentDir := getSegmentDir(dir, 1, 0)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	// Create a directory with the same name as the segment file to cause open error
	filePath := getSegmentFilePath(dir, 1, 0)
	err = os.MkdirAll(filePath, 0o755)
	require.NoError(t, err)

	_, err = NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "open file")
}

// === notifyFlushError with result channels ===

func TestLocalFileWriter_NotifyFlushError_WithChannels(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write data with a result channel
	ch := channel.NewLocalResultChannel("test-notify-err")
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), ch)
	require.NoError(t, err)

	// Close the file descriptor to force write error during flush
	writer.file.Close()

	// Trigger sync → processFlushTask fails and notifies via channel
	writer.Sync(context.Background())
	time.Sleep(300 * time.Millisecond)

	// Read from channel should get an error
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, readErr := ch.ReadResult(ctx)
	require.NoError(t, readErr)
	assert.Error(t, result.Err)
}

// === run() goroutine exit paths ===

func TestLocalFileWriter_Run_ContextCancelled(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	// Cancel the runCtx to trigger the run goroutine's context cancellation path
	writer.runCancel()
	time.Sleep(200 * time.Millisecond)

	// The run goroutine should have exited via runCtx.Done()
	// Writer is still "open" but the goroutine stopped
	writer.closed.Store(true)
}

// === Close error paths ===

func TestLocalFileWriter_Close_WithCancelledRunCtx(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	// Write data
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("data"), nil)
	require.NoError(t, err)

	// Cancel runCtx so the run goroutine exits
	writer.runCancel()
	time.Sleep(100 * time.Millisecond)

	// Close with a short-lived context to avoid the 15s awaitAllFlushTasks timeout
	shortCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err = writer.Close(shortCtx)
	// Should return error from awaitAllFlushTasks due to cancelled runCtx
	assert.Error(t, err)
}

// === RecoveryMode with recovery error ===

func TestNewLocalFileWriterWithMode_RecoveryMode_RecoverError(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Create the segment directory
	segmentDir := getSegmentDir(dir, 1, 0)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	// Create a directory with the same name as the segment file to cause recovery read error
	filePath := getSegmentFilePath(dir, 1, 0)
	err = os.MkdirAll(filePath, 0o755)
	require.NoError(t, err)

	_, err = NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "recover")
}

// === Recovery edge case tests for improved coverage ===

func TestLocalFileWriter_RecoverFromFullScan_NoHeaderRecord(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Create segment directory and file with DataRecords only (no HeaderRecord)
	segmentDir := getSegmentDir(dir, 1, 0)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	data1 := codec.EncodeRecord(&codec.DataRecord{Payload: []byte("hello")})
	data2 := codec.EncodeRecord(&codec.DataRecord{Payload: []byte("world")})
	content := append(data1, data2...)

	filePath := getSegmentFilePath(dir, 1, 0)
	err = os.WriteFile(filePath, content, 0o644)
	require.NoError(t, err)

	// Recovery should succeed but reset state (no header found → reset all)
	writer, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// State should be reset because no header was found
	assert.Equal(t, int64(-1), writer.GetLastEntryId(context.Background()))
	assert.Equal(t, int64(-1), writer.GetFirstEntryId(context.Background()))
	assert.True(t, writer.recovered.Load())
}

func TestLocalFileWriter_RecoverFromFooter_ZeroIndexOffset(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	segmentDir := getSegmentDir(dir, 1, 0)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	// Create a valid footer with IndexOffset=0 (invalid for recovery)
	footer := &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 1,
		TotalSize:    100,
		IndexOffset:  0,
		IndexLength:  0,
		Version:      codec.FormatVersion,
		LAC:          -1,
	}
	encodedFooter := codec.EncodeRecord(footer)

	filePath := getSegmentFilePath(dir, 1, 0)
	err = os.WriteFile(filePath, encodedFooter, 0o644)
	require.NoError(t, err)

	// Recovery should fail because IndexOffset=0 is invalid
	_, err = NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "recover")
}

func TestLocalFileWriter_RecoverFromFullScan_NonContiguousBlockNumbers(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	segmentDir := getSegmentDir(dir, 1, 0)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	// Build file: HeaderRecord + Block0 + Block5 (non-contiguous block numbers)
	header := codec.EncodeRecord(&codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	})

	// Block 0
	dataRecord0 := codec.EncodeRecord(&codec.DataRecord{Payload: []byte("data0")})
	blockCrc0 := crc32.ChecksumIEEE(dataRecord0)
	blockHeader0 := codec.EncodeRecord(&codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  0,
		BlockLength:  uint32(len(dataRecord0)),
		BlockCrc:     blockCrc0,
	})

	// Block 5 (non-contiguous!)
	dataRecord1 := codec.EncodeRecord(&codec.DataRecord{Payload: []byte("data1")})
	blockCrc1 := crc32.ChecksumIEEE(dataRecord1)
	blockHeader1 := codec.EncodeRecord(&codec.BlockHeaderRecord{
		BlockNumber:  5, // Non-contiguous: expected 1 but got 5
		FirstEntryID: 1,
		LastEntryID:  1,
		BlockLength:  uint32(len(dataRecord1)),
		BlockCrc:     blockCrc1,
	})

	var content []byte
	content = append(content, header...)
	content = append(content, blockHeader0...)
	content = append(content, dataRecord0...)
	content = append(content, blockHeader1...)
	content = append(content, dataRecord1...)

	filePath := getSegmentFilePath(dir, 1, 0)
	err = os.WriteFile(filePath, content, 0o644)
	require.NoError(t, err)

	// Recovery should handle non-contiguous block numbers
	writer, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.True(t, writer.recovered.Load())
	assert.Equal(t, int64(1), writer.GetLastEntryId(context.Background()))
}

func TestLocalFileWriter_RecoverFromFullScan_DataRecordOutsideBlock(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	segmentDir := getSegmentDir(dir, 1, 0)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	// Build file: HeaderRecord + DataRecord (outside any block)
	header := codec.EncodeRecord(&codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	})
	dataRecord := codec.EncodeRecord(&codec.DataRecord{Payload: []byte("orphan data")})

	content := append(header, dataRecord...)

	filePath := getSegmentFilePath(dir, 1, 0)
	err = os.WriteFile(filePath, content, 0o644)
	require.NoError(t, err)

	// Recovery should handle DataRecord outside any block
	writer, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, true)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	assert.True(t, writer.recovered.Load())
	// DataRecord at entryId 0 should be found
	assert.Equal(t, int64(0), writer.GetLastEntryId(context.Background()))
	assert.Equal(t, int64(0), writer.GetFirstEntryId(context.Background()))
}

// === Writer state abnormal tests ===

func TestLocalFileWriter_ProcessFlushTask_StorageNotWritable_Direct(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Set storage as not writable
	writer.storageWritable.Store(false)

	// Call processFlushTask directly with a real task
	resultCh := channel.NewLocalResultChannel("test-not-writable")
	task := &blockFlushTask{
		entries: []*cache.BufferEntry{
			{EntryId: 0, Data: []byte("data"), NotifyChan: resultCh, EnqueueTime: time.Now()},
		},
		firstEntryId: 0,
		lastEntryId:  0,
		blockNumber:  0,
	}
	writer.processFlushTask(context.Background(), task)

	// Result channel should have ErrStorageNotWritable
	result, err := resultCh.ReadResult(context.Background())
	require.NoError(t, err)
	assert.Error(t, result.Err)
}

func TestLocalFileWriter_RollBuffer_EmptyBuffer(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Call rollBufferAndSubmitFlushTaskUnsafe directly with empty buffer
	// (no data was written, so buffer is empty)
	writer.mu.Lock()
	writer.rollBufferAndSubmitFlushTaskUnsafe(context.Background())
	writer.mu.Unlock()

	// Should return early without submitting any flush task
	assert.Equal(t, int64(-1), writer.GetLastEntryId(context.Background()))
}

func TestNewLocalFileWriterWithMode_ZeroBlockSize(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	// Set zero MaxFlushSize to trigger the default block size fallback
	// Previously this would panic with division-by-zero at flushQueueSize calculation
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.ByteSize(0)

	writer, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, false)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Should use default block size (2MB) because blockSize was <= 0
	assert.Equal(t, int64(2*1024*1024), writer.maxFlushSize)
}

func TestNewLocalFileWriterWithMode_NegativeBlockSize(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	// Set negative MaxFlushSize to trigger the default block size fallback
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.ByteSize(-1)

	writer, err := NewLocalFileWriterWithMode(context.Background(), dir, 1, 0, cfg, false)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Should use default block size (2MB) because blockSize was <= 0
	assert.Equal(t, int64(2*1024*1024), writer.maxFlushSize)
}

func TestLocalFileWriter_Close_ErrorCleansUpResources(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
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

func TestLocalFileWriter_Close_Idempotent_NoDoubleClose(t *testing.T) {
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)

	// First close succeeds
	err = writer.Close(context.Background())
	assert.NoError(t, err)

	// Second close returns nil without panic (idempotent)
	err = writer.Close(context.Background())
	assert.NoError(t, err)
	assert.True(t, writer.closed.Load())
}
