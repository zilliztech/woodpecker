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

package processor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/mocks/mocks_server/mocks_storage"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
)

func TestNewSegmentProcessor(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	sp := NewSegmentProcessor(context.Background(), cfg, "bucket", "root", 1, 2, nil)
	assert.NotNil(t, sp)

	impl := sp.(*segmentProcessor)
	assert.Equal(t, int64(1), impl.logId)
	assert.Equal(t, int64(2), impl.segId)
	assert.Equal(t, "bucket", impl.bucketName)
	assert.Equal(t, "root", impl.rootPath)
	assert.Nil(t, impl.storageClient)
}

func TestSegmentProcessor_GetLogId(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	sp := NewSegmentProcessor(context.Background(), cfg, "b", "r", 42, 7, nil)
	assert.Equal(t, int64(42), sp.GetLogId())
}

func TestSegmentProcessor_GetSegmentId(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	sp := NewSegmentProcessor(context.Background(), cfg, "b", "r", 42, 7, nil)
	assert.Equal(t, int64(7), sp.GetSegmentId())
}

func TestSegmentProcessor_GetLastAccessTime(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	before := time.Now().UnixMilli()
	sp := NewSegmentProcessor(context.Background(), cfg, "b", "r", 1, 1, nil)
	after := time.Now().UnixMilli()

	accessTime := sp.GetLastAccessTime()
	assert.GreaterOrEqual(t, accessTime, before)
	assert.LessOrEqual(t, accessTime, after)
}

func TestSegmentProcessor_UpdateAccessTime(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	sp := NewSegmentProcessor(context.Background(), cfg, "b", "r", 1, 1, nil)
	impl := sp.(*segmentProcessor)

	oldTime := impl.GetLastAccessTime()
	time.Sleep(2 * time.Millisecond)
	impl.updateAccessTime()
	newTime := impl.GetLastAccessTime()

	assert.Greater(t, newTime, oldTime)
}

func TestSegmentProcessor_GetInstanceBucket(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	sp := NewSegmentProcessor(context.Background(), cfg, "my-bucket", "r", 1, 1, nil)
	impl := sp.(*segmentProcessor)
	assert.Equal(t, "my-bucket", impl.getInstanceBucket())
}

func TestSegmentProcessor_GetLogBaseDir(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	sp := NewSegmentProcessor(context.Background(), cfg, "b", "my-root", 1, 1, nil)
	impl := sp.(*segmentProcessor)
	assert.Equal(t, "my-root", impl.getLogBaseDir())
}

func TestSegmentProcessor_Close_NilWriterReader(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	sp := NewSegmentProcessor(context.Background(), cfg, "b", "r", 1, 1, nil)

	// Close with nil writer and reader should succeed
	err := sp.Close(context.Background())
	assert.NoError(t, err)
}

func TestSegmentProcessor_Close_Idempotent(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	sp := NewSegmentProcessor(context.Background(), cfg, "b", "r", 1, 1, nil)

	// Double close should be fine
	assert.NoError(t, sp.Close(context.Background()))
	assert.NoError(t, sp.Close(context.Background()))
}

// helper to create a segmentProcessor with injected mocks
func newTestProcessor(t *testing.T) *segmentProcessor {
	cfg, _ := config.NewConfiguration()
	sp := NewSegmentProcessor(context.Background(), cfg, "bucket", "root", 1, 2, nil)
	return sp.(*segmentProcessor)
}

// === Fence Tests ===

func TestSegmentProcessor_Fence_Success(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockWriter.EXPECT().Fence(mock.Anything).Return(int64(42), nil)
	sp.currentSegmentWriter = mockWriter

	lac, err := sp.Fence(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(42), lac)
}

func TestSegmentProcessor_Fence_WriterFenceError(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockWriter.EXPECT().Fence(mock.Anything).Return(int64(-1), fmt.Errorf("fence failed"))
	sp.currentSegmentWriter = mockWriter

	lac, err := sp.Fence(context.Background())
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lac)
}

// === Complete Tests ===

func TestSegmentProcessor_Complete_Success(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockWriter.EXPECT().Finalize(mock.Anything, int64(10)).Return(int64(10), nil)
	sp.currentSegmentWriter = mockWriter

	lac, err := sp.Complete(context.Background(), 10)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), lac)
}

func TestSegmentProcessor_Complete_NoWriter(t *testing.T) {
	sp := newTestProcessor(t)
	// No writer set

	lac, err := sp.Complete(context.Background(), 10)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lac)
	assert.True(t, werr.ErrSegmentProcessorNoWriter.Is(err))
}

func TestSegmentProcessor_Complete_FinalizeError(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockWriter.EXPECT().Finalize(mock.Anything, int64(5)).Return(int64(-1), fmt.Errorf("finalize err"))
	sp.currentSegmentWriter = mockWriter

	lac, err := sp.Complete(context.Background(), 5)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lac)
}

// === AddEntry Tests ===

func TestSegmentProcessor_AddEntry_Success(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockWriter.EXPECT().WriteDataAsync(mock.Anything, int64(1), []byte("data"), nil).Return(int64(100), nil)
	sp.currentSegmentWriter = mockWriter

	entry := &proto.LogEntry{EntryId: 1, Values: []byte("data")}
	seqNo, err := sp.AddEntry(context.Background(), entry, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), seqNo)
}

func TestSegmentProcessor_AddEntry_WriteError(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockWriter.EXPECT().WriteDataAsync(mock.Anything, int64(1), []byte("data"), nil).Return(int64(-1), fmt.Errorf("write err"))
	sp.currentSegmentWriter = mockWriter

	entry := &proto.LogEntry{EntryId: 1, Values: []byte("data")}
	seqNo, err := sp.AddEntry(context.Background(), entry, nil)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), seqNo)
}

func TestSegmentProcessor_AddEntry_ReturnsMinusOne(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockWriter.EXPECT().WriteDataAsync(mock.Anything, int64(1), []byte("data"), nil).Return(int64(-1), nil)
	sp.currentSegmentWriter = mockWriter

	entry := &proto.LogEntry{EntryId: 1, Values: []byte("data")}
	seqNo, err := sp.AddEntry(context.Background(), entry, nil)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), seqNo)
	assert.Contains(t, err.Error(), "failed to append to log file")
}

// === ReadBatchEntriesAdv Tests ===

func TestSegmentProcessor_ReadBatchEntriesAdv_Success(t *testing.T) {
	sp := newTestProcessor(t)
	mockReader := mocks_storage.NewReader(t)
	expectedBatch := &proto.BatchReadResult{
		Entries: []*proto.LogEntry{{EntryId: 1, Values: []byte("data")}},
	}
	mockReader.EXPECT().ReadNextBatchAdv(mock.Anything, mock.Anything, mock.Anything).Return(expectedBatch, nil)
	sp.currentSegmentReader = mockReader

	batch, err := sp.ReadBatchEntriesAdv(context.Background(), 0, 100, nil)
	assert.NoError(t, err)
	assert.Equal(t, expectedBatch, batch)
}

func TestSegmentProcessor_ReadBatchEntriesAdv_WithMatchingLastReadState(t *testing.T) {
	sp := newTestProcessor(t)
	mockReader := mocks_storage.NewReader(t)
	expectedBatch := &proto.BatchReadResult{}
	// When lastReadState.SegmentId matches sp.segId (2), it should be passed through
	lastState := &proto.LastReadState{SegmentId: 2}
	mockReader.EXPECT().ReadNextBatchAdv(mock.Anything, mock.MatchedBy(func(opt storage.ReaderOpt) bool {
		return opt.StartEntryID == 5 && opt.MaxBatchEntries == 50
	}), lastState).Return(expectedBatch, nil)
	sp.currentSegmentReader = mockReader

	batch, err := sp.ReadBatchEntriesAdv(context.Background(), 5, 50, lastState)
	assert.NoError(t, err)
	assert.Equal(t, expectedBatch, batch)
}

func TestSegmentProcessor_ReadBatchEntriesAdv_WithNonMatchingLastReadState(t *testing.T) {
	sp := newTestProcessor(t)
	mockReader := mocks_storage.NewReader(t)
	expectedBatch := &proto.BatchReadResult{}
	// When lastReadState.SegmentId doesn't match sp.segId (2), nil should be passed
	lastState := &proto.LastReadState{SegmentId: 999}
	mockReader.EXPECT().ReadNextBatchAdv(mock.Anything, mock.Anything, (*proto.LastReadState)(nil)).Return(expectedBatch, nil)
	sp.currentSegmentReader = mockReader

	batch, err := sp.ReadBatchEntriesAdv(context.Background(), 0, 100, lastState)
	assert.NoError(t, err)
	assert.Equal(t, expectedBatch, batch)
}

func TestSegmentProcessor_ReadBatchEntriesAdv_EntryNotFoundError(t *testing.T) {
	sp := newTestProcessor(t)
	mockReader := mocks_storage.NewReader(t)
	mockReader.EXPECT().ReadNextBatchAdv(mock.Anything, mock.Anything, mock.Anything).Return(nil, werr.ErrEntryNotFound)
	sp.currentSegmentReader = mockReader

	batch, err := sp.ReadBatchEntriesAdv(context.Background(), 0, 100, nil)
	assert.Error(t, err)
	assert.Nil(t, batch)
	assert.True(t, werr.ErrEntryNotFound.Is(err))
}

func TestSegmentProcessor_ReadBatchEntriesAdv_EOFError(t *testing.T) {
	sp := newTestProcessor(t)
	mockReader := mocks_storage.NewReader(t)
	mockReader.EXPECT().ReadNextBatchAdv(mock.Anything, mock.Anything, mock.Anything).Return(nil, werr.ErrFileReaderEndOfFile)
	sp.currentSegmentReader = mockReader

	batch, err := sp.ReadBatchEntriesAdv(context.Background(), 0, 100, nil)
	assert.Error(t, err)
	assert.Nil(t, batch)
}

func TestSegmentProcessor_ReadBatchEntriesAdv_OtherError(t *testing.T) {
	sp := newTestProcessor(t)
	mockReader := mocks_storage.NewReader(t)
	mockReader.EXPECT().ReadNextBatchAdv(mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("some error"))
	sp.currentSegmentReader = mockReader

	batch, err := sp.ReadBatchEntriesAdv(context.Background(), 0, 100, nil)
	assert.Error(t, err)
	assert.Nil(t, batch)
}

// === GetSegmentLastAddConfirmed Tests ===

func TestSegmentProcessor_GetSegmentLastAddConfirmed_Success(t *testing.T) {
	sp := newTestProcessor(t)
	mockReader := mocks_storage.NewReader(t)
	mockReader.EXPECT().GetLastEntryID(mock.Anything).Return(int64(99), nil)
	sp.currentSegmentReader = mockReader

	lac, err := sp.GetSegmentLastAddConfirmed(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(99), lac)
}

func TestSegmentProcessor_GetSegmentLastAddConfirmed_Error(t *testing.T) {
	sp := newTestProcessor(t)
	mockReader := mocks_storage.NewReader(t)
	mockReader.EXPECT().GetLastEntryID(mock.Anything).Return(int64(-1), fmt.Errorf("read error"))
	sp.currentSegmentReader = mockReader

	lac, err := sp.GetSegmentLastAddConfirmed(context.Background())
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lac)
}

// === GetBlocksCount Tests ===

func TestSegmentProcessor_GetBlocksCount_Success(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockWriter.EXPECT().GetBlockCount(mock.Anything).Return(int64(5))
	sp.currentSegmentWriter = mockWriter

	count, err := sp.GetBlocksCount(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)
}

func TestSegmentProcessor_GetBlocksCount_NoWriter(t *testing.T) {
	sp := newTestProcessor(t)
	// No writer set

	count, err := sp.GetBlocksCount(context.Background())
	assert.Error(t, err)
	assert.Equal(t, int64(-1), count)
	assert.True(t, werr.ErrSegmentProcessorNoWriter.Is(err))
}

// === Compact Tests ===

func TestSegmentProcessor_Compact_Success(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockWriter.EXPECT().Compact(mock.Anything).Return(int64(1024), nil)
	sp.currentSegmentWriter = mockWriter

	meta, err := sp.Compact(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, meta)
	assert.Equal(t, proto.SegmentState_Sealed, meta.State)
	assert.Equal(t, int64(1024), meta.Size)
	assert.Greater(t, meta.SealedTime, int64(0))
}

func TestSegmentProcessor_Compact_MergeError(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockWriter.EXPECT().Compact(mock.Anything).Return(int64(0), fmt.Errorf("compact failed"))
	sp.currentSegmentWriter = mockWriter

	meta, err := sp.Compact(context.Background())
	assert.Error(t, err)
	assert.Nil(t, meta)
}

// === Clean Tests ===

func TestSegmentProcessor_Clean_Success(t *testing.T) {
	sp := newTestProcessor(t)
	mockSegment := mocks_storage.NewSegment(t)
	mockSegment.EXPECT().DeleteFileData(mock.Anything, 1).Return(3, nil)
	sp.currentSegmentImpl = mockSegment

	err := sp.Clean(context.Background(), 1)
	assert.NoError(t, err)
}

func TestSegmentProcessor_Clean_DeleteError(t *testing.T) {
	sp := newTestProcessor(t)
	mockSegment := mocks_storage.NewSegment(t)
	mockSegment.EXPECT().DeleteFileData(mock.Anything, 1).Return(0, fmt.Errorf("delete failed"))
	sp.currentSegmentImpl = mockSegment

	err := sp.Clean(context.Background(), 1)
	assert.Error(t, err)
}

// === UpdateSegmentLastAddConfirmed Tests ===

func TestSegmentProcessor_UpdateSegmentLastAddConfirmed_Success(t *testing.T) {
	sp := newTestProcessor(t)
	mockReader := mocks_storage.NewReader(t)
	mockReader.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(50)).Return(nil)
	sp.currentSegmentReader = mockReader

	err := sp.UpdateSegmentLastAddConfirmed(context.Background(), 50)
	assert.NoError(t, err)
}

func TestSegmentProcessor_UpdateSegmentLastAddConfirmed_Error(t *testing.T) {
	sp := newTestProcessor(t)
	mockReader := mocks_storage.NewReader(t)
	mockReader.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(50)).Return(fmt.Errorf("update failed"))
	sp.currentSegmentReader = mockReader

	err := sp.UpdateSegmentLastAddConfirmed(context.Background(), 50)
	assert.Error(t, err)
}

// === Close with mock writer/reader Tests ===

func TestSegmentProcessor_Close_WithWriterAndReader_Success(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockReader := mocks_storage.NewReader(t)
	mockWriter.EXPECT().Close(mock.Anything).Return(nil)
	mockReader.EXPECT().Close(mock.Anything).Return(nil)
	sp.currentSegmentWriter = mockWriter
	sp.currentSegmentReader = mockReader

	err := sp.Close(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, sp.currentSegmentWriter)
	assert.Nil(t, sp.currentSegmentReader)
}

func TestSegmentProcessor_Close_WriterCloseError(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockReader := mocks_storage.NewReader(t)
	mockWriter.EXPECT().Close(mock.Anything).Return(fmt.Errorf("writer close err"))
	mockReader.EXPECT().Close(mock.Anything).Return(nil)
	sp.currentSegmentWriter = mockWriter
	sp.currentSegmentReader = mockReader

	err := sp.Close(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "writer close err")
}

func TestSegmentProcessor_Close_ReaderCloseError(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	mockReader := mocks_storage.NewReader(t)
	mockWriter.EXPECT().Close(mock.Anything).Return(nil)
	mockReader.EXPECT().Close(mock.Anything).Return(fmt.Errorf("reader close err"))
	sp.currentSegmentWriter = mockWriter
	sp.currentSegmentReader = mockReader

	// Writer succeeded, so Close returns nil (only writerErr is returned)
	err := sp.Close(context.Background())
	assert.NoError(t, err)
}

// === getSegmentWriter Tests ===

func TestSegmentProcessor_GetSegmentWriter_NoWriter(t *testing.T) {
	sp := newTestProcessor(t)

	_, err := sp.getSegmentWriter(context.Background())
	assert.Error(t, err)
	assert.True(t, werr.ErrSegmentProcessorNoWriter.Is(err))
}

func TestSegmentProcessor_GetSegmentWriter_WithWriter(t *testing.T) {
	sp := newTestProcessor(t)
	mockWriter := mocks_storage.NewWriter(t)
	sp.currentSegmentWriter = mockWriter

	w, err := sp.getSegmentWriter(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, mockWriter, w)
}

// === Tests using local storage mode to cover getOrCreate* creation branches ===

func newLocalStorageProcessor(t *testing.T) *segmentProcessor {
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Storage.Type = "local"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()
	sp := NewSegmentProcessor(context.Background(), cfg, "bucket", "root", 1, 0, nil)
	return sp.(*segmentProcessor)
}

func TestSegmentProcessor_GetOrCreateSegmentImpl_Local(t *testing.T) {
	sp := newLocalStorageProcessor(t)

	impl, err := sp.getOrCreateSegmentImpl(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, impl)

	// Second call should return cached impl (double-check path)
	impl2, err := sp.getOrCreateSegmentImpl(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, impl, impl2)
}

func TestSegmentProcessor_GetOrCreateSegmentWriter_Local(t *testing.T) {
	sp := newLocalStorageProcessor(t)

	writer, err := sp.getOrCreateSegmentWriter(context.Background(), false)
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	// Second call should return cached writer (double-check path)
	writer2, err := sp.getOrCreateSegmentWriter(context.Background(), false)
	assert.NoError(t, err)
	assert.Equal(t, writer, writer2)
}

func TestSegmentProcessor_GetOrCreateSegmentWriter_Local_RecoverMode(t *testing.T) {
	sp := newLocalStorageProcessor(t)

	writer, err := sp.getOrCreateSegmentWriter(context.Background(), true)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
}

func TestSegmentProcessor_GetOrCreateSegmentReader_Local_NoFile(t *testing.T) {
	sp := newLocalStorageProcessor(t)

	// No data file exists yet, should return error
	reader, err := sp.getOrCreateSegmentReader(context.Background())
	assert.Error(t, err)
	assert.Nil(t, reader)
}

func TestSegmentProcessor_Fence_WithLocalStorage(t *testing.T) {
	sp := newLocalStorageProcessor(t)

	// Writer is created on-the-fly via getOrCreateSegmentWriter
	lac, err := sp.Fence(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), lac) // no data written, LAC is -1
}

func TestSegmentProcessor_Compact_WithLocalStorage(t *testing.T) {
	sp := newLocalStorageProcessor(t)

	// Local storage does not support compact, should return error
	meta, err := sp.Compact(context.Background())
	assert.Error(t, err)
	assert.Nil(t, meta)
	assert.Contains(t, err.Error(), "not need to compact local file currently")
}

func TestSegmentProcessor_Clean_WithLocalStorage(t *testing.T) {
	sp := newLocalStorageProcessor(t)

	// Clean creates segment impl if needed, then deletes
	err := sp.Clean(context.Background(), 0)
	assert.NoError(t, err)
}

func TestSegmentProcessor_GetBlocksCount_WithLocalStorage(t *testing.T) {
	sp := newLocalStorageProcessor(t)

	// Create writer first
	_, err := sp.getOrCreateSegmentWriter(context.Background(), false)
	assert.NoError(t, err)

	count, err := sp.GetBlocksCount(context.Background())
	assert.NoError(t, err)
	// local writer may return -1 for block count (not applicable for local storage)
	assert.Equal(t, int64(-1), count)
}

func TestSegmentProcessor_AddEntry_WithLocalStorage(t *testing.T) {
	sp := newLocalStorageProcessor(t)

	entry := &proto.LogEntry{EntryId: 0, Values: []byte("hello")}
	seqNo, err := sp.AddEntry(context.Background(), entry, nil)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, seqNo, int64(0))
}

func TestSegmentProcessor_GetOrCreateSegmentImpl_Service(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()
	sp := NewSegmentProcessor(context.Background(), cfg, "bucket", "root", 1, 0, nil)
	impl := sp.(*segmentProcessor)

	segImpl, err := impl.getOrCreateSegmentImpl(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, segImpl)
}

func TestSegmentProcessor_GetOrCreateSegmentImpl_Minio(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	// default type is minio (empty or "minio" or "default")
	cfg.Woodpecker.Storage.Type = "minio"
	sp := NewSegmentProcessor(context.Background(), cfg, "bucket", "root", 1, 0, nil)
	impl := sp.(*segmentProcessor)

	segImpl, err := impl.getOrCreateSegmentImpl(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, segImpl)

	// Second call returns cached
	segImpl2, err := impl.getOrCreateSegmentImpl(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, segImpl, segImpl2)
}

func newServiceStorageProcessor(t *testing.T) *segmentProcessor {
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()
	sp := NewSegmentProcessor(context.Background(), cfg, "bucket", "root", 1, 0, nil)
	return sp.(*segmentProcessor)
}

func TestSegmentProcessor_GetOrCreateSegmentWriter_Service(t *testing.T) {
	sp := newServiceStorageProcessor(t)

	writer, err := sp.getOrCreateSegmentWriter(context.Background(), false)
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	// Close the writer to clean up goroutines
	writer.Close(context.Background())
}

func TestSegmentProcessor_GetOrCreateSegmentReader_Service_NoFile(t *testing.T) {
	sp := newServiceStorageProcessor(t)

	// No data file exists, should return error
	reader, err := sp.getOrCreateSegmentReader(context.Background())
	assert.Error(t, err)
	assert.Nil(t, reader)
}

// === Tests for MinIO storage mode to cover the object storage reader/writer branches ===

func newMinioStorageProcessor(t *testing.T) (*segmentProcessor, *mocks_objectstorage.ObjectStorage) {
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Storage.Type = "minio"
	mockObjStorage := mocks_objectstorage.NewObjectStorage(t)
	sp := NewSegmentProcessor(context.Background(), cfg, "bucket", "root", 1, 0, mockObjStorage)
	return sp.(*segmentProcessor), mockObjStorage
}

func TestSegmentProcessor_GetOrCreateSegmentWriter_Minio(t *testing.T) {
	sp, mockStorage := newMinioStorageProcessor(t)

	// Non-recovery mode: writer tries to create a segment lock via PutObjectIfNoneMatch
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(fmt.Errorf("lock creation failed")).Maybe()
	// May also call StatObject for footer check
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("not found")).Maybe()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	writer, err := sp.getOrCreateSegmentWriter(context.Background(), false)
	assert.Error(t, err)
	assert.Nil(t, writer)
}

func TestSegmentProcessor_GetOrCreateSegmentWriter_Minio_RecoverMode(t *testing.T) {
	sp, mockStorage := newMinioStorageProcessor(t)

	// Recovery mode: first StatObject for footer, then IsObjectNotExistsError,
	// then recoverFromFullListing with WalkWithObjects
	// With no footer and empty walk, recovery succeeds (nothing to recover)
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("not found")).Maybe()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()
	mockStorage.EXPECT().WalkWithObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	writer, err := sp.getOrCreateSegmentWriter(context.Background(), true)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	// Clean up
	writer.Close(context.Background())
}

func TestSegmentProcessor_GetOrCreateSegmentWriter_Minio_RecoverMode_GetObjectError(t *testing.T) {
	sp, mockStorage := newMinioStorageProcessor(t)

	// Recovery mode: StatObject returns error but not "not exists" -> goes to recoverFromFooter
	// recoverFromFooter calls GetObject which fails
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("network error")).Maybe()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(false).Maybe()
	mockStorage.EXPECT().GetObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("get object failed")).Maybe()

	writer, err := sp.getOrCreateSegmentWriter(context.Background(), true)
	assert.Error(t, err)
	assert.Nil(t, writer)
}

func TestSegmentProcessor_GetOrCreateSegmentReader_Minio(t *testing.T) {
	sp, mockStorage := newMinioStorageProcessor(t)

	// Reader tries to read footer via StatObject; if it fails, IsObjectNotExistsError is checked
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("stat failed")).Maybe()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(false).Maybe()

	reader, err := sp.getOrCreateSegmentReader(context.Background())
	assert.Error(t, err)
	assert.Nil(t, reader)
}

func TestSegmentProcessor_GetOrCreateSegmentReader_Minio_NoFooter(t *testing.T) {
	sp, mockStorage := newMinioStorageProcessor(t)

	// StatObject returns not-found error, IsObjectNotExistsError returns true (no footer yet)
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("not found")).Maybe()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()
	mockStorage.EXPECT().WalkWithObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	mockStorage.EXPECT().GetObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("not found")).Maybe()

	reader, err := sp.getOrCreateSegmentReader(context.Background())
	// May succeed (no data, no footer) or fail
	if err != nil {
		assert.Nil(t, reader)
	} else {
		assert.NotNil(t, reader)
		reader.Close(context.Background())
	}
}

func TestSegmentProcessor_Fence_GetOrCreateWriterError(t *testing.T) {
	sp, mockStorage := newMinioStorageProcessor(t)
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("storage unavailable")).Maybe()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(false).Maybe()
	mockStorage.EXPECT().GetObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("get object failed")).Maybe()

	lac, err := sp.Fence(context.Background())
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lac)
}

func TestSegmentProcessor_Compact_GetOrCreateWriterError(t *testing.T) {
	sp, mockStorage := newMinioStorageProcessor(t)
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("storage unavailable")).Maybe()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(false).Maybe()
	mockStorage.EXPECT().GetObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("get object failed")).Maybe()

	meta, err := sp.Compact(context.Background())
	assert.Error(t, err)
	assert.Nil(t, meta)
}

func TestSegmentProcessor_Clean_GetOrCreateImplError(t *testing.T) {
	sp := newTestProcessor(t)
	mockSegment := mocks_storage.NewSegment(t)
	mockSegment.EXPECT().DeleteFileData(mock.Anything, 2).Return(1, fmt.Errorf("partial delete error"))
	sp.currentSegmentImpl = mockSegment

	err := sp.Clean(context.Background(), 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "partial delete error")
}

func TestSegmentProcessor_UpdateSegmentLastAddConfirmed_GetReaderError(t *testing.T) {
	sp, mockStorage := newMinioStorageProcessor(t)
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("stat failed")).Maybe()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(false).Maybe()

	err := sp.UpdateSegmentLastAddConfirmed(context.Background(), 10)
	assert.Error(t, err)
}

func TestSegmentProcessor_GetSegmentLastAddConfirmed_GetReaderError(t *testing.T) {
	sp, mockStorage := newMinioStorageProcessor(t)
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("stat failed")).Maybe()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(false).Maybe()

	_, err := sp.GetSegmentLastAddConfirmed(context.Background())
	assert.Error(t, err)
}

func TestSegmentProcessor_ReadBatchEntriesAdv_GetReaderError(t *testing.T) {
	sp, mockStorage := newMinioStorageProcessor(t)
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("stat failed")).Maybe()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(false).Maybe()

	_, err := sp.ReadBatchEntriesAdv(context.Background(), 0, 100, nil)
	assert.Error(t, err)
}

func TestSegmentProcessor_AddEntry_GetWriterError(t *testing.T) {
	sp, mockStorage := newMinioStorageProcessor(t)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(fmt.Errorf("lock creation failed")).Maybe()
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("not found")).Maybe()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	entry := &proto.LogEntry{EntryId: 0, Values: []byte("data")}
	_, err := sp.AddEntry(context.Background(), entry, nil)
	assert.Error(t, err)
}

func TestSegmentProcessor_Close_WithLocalWriterAndReader(t *testing.T) {
	sp := newLocalStorageProcessor(t)

	// Create writer
	_, err := sp.getOrCreateSegmentWriter(context.Background(), false)
	assert.NoError(t, err)
	assert.NotNil(t, sp.currentSegmentWriter)

	err = sp.Close(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, sp.currentSegmentWriter)
	assert.Nil(t, sp.currentSegmentReader)
}
