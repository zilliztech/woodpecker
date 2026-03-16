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

package objectstorage

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	commonObjectStorage "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// newTestConfig creates a config suitable for unit testing the writer
func newTestConfig() *config.Configuration {
	return &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        config.NewByteSize(1024 * 1024),
					MaxInterval:     config.NewDurationMillisecondsFromInt(1000),
					MaxFlushThreads: 4,
					MaxFlushSize:    config.NewByteSize(1024 * 1024),
					MaxFlushRetries: 3,
					RetryInterval:   config.NewDurationMillisecondsFromInt(100),
				},
				SegmentCompactionPolicy: config.SegmentCompactionPolicy{
					MaxBytes:           config.NewByteSize(4 * 1024 * 1024),
					MaxParallelUploads: 4,
					MaxParallelReads:   4,
				},
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "enable",
				},
			},
		},
	}
}

// newTestMinioFileWriter creates a MinioFileWriter directly (bypassing lock creation and goroutines)
// for unit testing internal state and simple method behavior.
func newTestMinioFileWriter() *MinioFileWriter {
	cfg := newTestConfig()
	syncPolicyConfig := &cfg.Woodpecker.Logstore.SegmentSyncPolicy
	maxBufferEntries := int64(syncPolicyConfig.MaxEntries)

	w := &MinioFileWriter{
		logId:               1,
		segmentId:           0,
		logIdStr:            "1",
		segmentIdStr:        "0",
		nsStr:               "test-bucket/test-base",
		segmentFileKey:      "test-base/1/0",
		bucket:              "test-bucket",
		maxBufferSize:       syncPolicyConfig.MaxBytes.Int64(),
		maxBufferEntries:    maxBufferEntries,
		maxIntervalMs:       syncPolicyConfig.MaxInterval.Milliseconds(),
		syncPolicyConfig:    syncPolicyConfig,
		compactPolicyConfig: &cfg.Woodpecker.Logstore.SegmentCompactionPolicy,
		fencePolicyConfig:   &cfg.Woodpecker.Logstore.FencePolicy,
		fileClose:           make(chan struct{}, 1),
		fastSyncTriggerSize: syncPolicyConfig.MaxFlushSize.Int64(),
		pool:                conc.NewPool[*blockUploadResult](4, conc.WithPreAlloc(true)),
		flushingTaskList:    make(chan *blockUploadTask, 4),
	}
	w.firstEntryID.Store(-1)
	w.lastEntryID.Store(-1)
	w.lastBlockID.Store(-1)
	w.lastSubmittedUploadingBlockID.Store(-1)
	w.lastSubmittedUploadingEntryID.Store(-1)
	w.closed.Store(false)
	w.storageWritable.Store(true)
	w.allUploadingTaskDone.Store(false)
	w.flushingBufferSize.Store(0)
	w.finalized.Store(false)
	w.fenced.Store(false)
	w.headerWritten.Store(false)
	w.lastSyncTimestamp.Store(time.Now().UnixMilli())
	w.lastModifiedTime.Store(time.Now().UnixMilli())

	newBuffer := cache.NewSequentialBuffer(1, 0, 0, maxBufferEntries, w.nsStr)
	w.buffer.Store(newBuffer)

	return w
}

// mockResultChannel implements channel.ResultChannel for tests
type mockResultChannel struct {
	results []*channel.AppendResult
}

func newMockResultChannel() *mockResultChannel {
	return &mockResultChannel{
		results: make([]*channel.AppendResult, 0),
	}
}

func (m *mockResultChannel) GetIdentifier() string {
	return "mock-result-channel"
}

func (m *mockResultChannel) SendResult(ctx context.Context, result *channel.AppendResult) error {
	m.results = append(m.results, result)
	return nil
}

func (m *mockResultChannel) ReadResult(ctx context.Context) (*channel.AppendResult, error) {
	if len(m.results) == 0 {
		return nil, nil
	}
	r := m.results[0]
	m.results = m.results[1:]
	return r, nil
}

func (m *mockResultChannel) Close(ctx context.Context) error {
	return nil
}

func (m *mockResultChannel) IsClosed() bool {
	return false
}

// ===== Writer Unit Tests =====

func TestNewMinioFileWriter_StructFields(t *testing.T) {
	w := newTestMinioFileWriter()

	assert.Equal(t, int64(1), w.logId)
	assert.Equal(t, int64(0), w.segmentId)
	assert.Equal(t, "test-base/1/0", w.segmentFileKey)
	assert.Equal(t, "test-bucket", w.bucket)
	assert.Equal(t, int64(-1), w.firstEntryID.Load())
	assert.Equal(t, int64(-1), w.lastEntryID.Load())
	assert.Equal(t, int64(-1), w.lastBlockID.Load())
	assert.Equal(t, int64(-1), w.lastSubmittedUploadingBlockID.Load())
	assert.Equal(t, int64(-1), w.lastSubmittedUploadingEntryID.Load())
	assert.False(t, w.closed.Load())
	assert.True(t, w.storageWritable.Load())
	assert.False(t, w.finalized.Load())
	assert.False(t, w.fenced.Load())
	assert.False(t, w.headerWritten.Load())
	assert.NotNil(t, w.buffer.Load())
	assert.NotNil(t, w.pool)
}

func TestMinioFileWriter_GetId(t *testing.T) {
	w := newTestMinioFileWriter()
	assert.Equal(t, int64(0), w.GetId())

	w.segmentId = 42
	assert.Equal(t, int64(42), w.GetId())
}

func TestMinioFileWriter_GetFirstEntryId(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	// Initially -1
	assert.Equal(t, int64(-1), w.GetFirstEntryId(ctx))

	// After setting
	w.firstEntryID.Store(0)
	assert.Equal(t, int64(0), w.GetFirstEntryId(ctx))

	w.firstEntryID.Store(100)
	assert.Equal(t, int64(100), w.GetFirstEntryId(ctx))
}

func TestMinioFileWriter_GetLastEntryId(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	// Initially -1
	assert.Equal(t, int64(-1), w.GetLastEntryId(ctx))

	// After setting
	w.lastEntryID.Store(0)
	assert.Equal(t, int64(0), w.GetLastEntryId(ctx))

	w.lastEntryID.Store(99)
	assert.Equal(t, int64(99), w.GetLastEntryId(ctx))
}

func TestMinioFileWriter_GetBlockCount(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	// Initially -1 (no blocks submitted), count should be 0
	assert.Equal(t, int64(0), w.GetBlockCount(ctx))

	w.lastSubmittedUploadingBlockID.Store(0)
	assert.Equal(t, int64(1), w.GetBlockCount(ctx)) // block ID 0 = 1 block

	w.lastSubmittedUploadingBlockID.Store(5)
	assert.Equal(t, int64(6), w.GetBlockCount(ctx)) // block IDs 0-5 = 6 blocks
}

func TestMinioFileWriter_IsFenced(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	fenced, err := w.IsFenced(ctx)
	assert.NoError(t, err)
	assert.False(t, fenced)

	w.fenced.Store(true)
	fenced, err = w.IsFenced(ctx)
	assert.NoError(t, err)
	assert.True(t, fenced)
}

func TestMinioFileWriter_WriteDataAsync_EmptyData(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	resultCh := newMockResultChannel()

	_, err := w.WriteDataAsync(ctx, 0, []byte{}, resultCh)
	require.Error(t, err)
	assert.True(t, werr.ErrEmptyPayload.Is(err))
}

func TestMinioFileWriter_WriteDataAsync_NilData(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	resultCh := newMockResultChannel()

	_, err := w.WriteDataAsync(ctx, 0, nil, resultCh)
	require.Error(t, err)
	assert.True(t, werr.ErrEmptyPayload.Is(err))
}

func TestMinioFileWriter_WriteDataAsync_AfterClose(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.closed.Store(true)
	resultCh := newMockResultChannel()

	_, err := w.WriteDataAsync(ctx, 0, []byte("test data"), resultCh)
	require.Error(t, err)
	assert.True(t, werr.ErrFileWriterAlreadyClosed.Is(err))
}

func TestMinioFileWriter_WriteDataAsync_AfterFinalized(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.finalized.Store(true)
	resultCh := newMockResultChannel()

	_, err := w.WriteDataAsync(ctx, 0, []byte("test data"), resultCh)
	require.Error(t, err)
	assert.True(t, werr.ErrFileWriterFinalized.Is(err))
}

func TestMinioFileWriter_WriteDataAsync_AfterFenced(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.fenced.Store(true)
	resultCh := newMockResultChannel()

	_, err := w.WriteDataAsync(ctx, 0, []byte("test data"), resultCh)
	require.Error(t, err)
	assert.True(t, werr.ErrSegmentFenced.Is(err))
}

func TestMinioFileWriter_WriteDataAsync_StorageNotWritable(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.storageWritable.Store(false)
	resultCh := newMockResultChannel()

	_, err := w.WriteDataAsync(ctx, 0, []byte("test data"), resultCh)
	require.Error(t, err)
	assert.True(t, werr.ErrStorageNotWritable.Is(err))
}

func TestMinioFileWriter_WriteDataAsync_AlreadyWrittenEntry(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.lastEntryID.Store(10)
	resultCh := newMockResultChannel()

	// Trying to write entryId 5 which is <= lastEntryID 10
	id, err := w.WriteDataAsync(ctx, 5, []byte("test data"), resultCh)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), id)
	// The result channel should have been notified directly
	assert.Len(t, resultCh.results, 1)
}

func TestMinioFileWriter_WriteDataAsync_AlreadySubmittedEntry(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.lastEntryID.Store(-1)
	w.lastSubmittedUploadingEntryID.Store(10)
	resultCh := newMockResultChannel()

	// Trying to write entryId 5 which is <= lastSubmittedUploadingEntryID 10
	id, err := w.WriteDataAsync(ctx, 5, []byte("test data"), resultCh)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), id)
	// No result notification because it was already submitted for upload
	assert.Empty(t, resultCh.results)
}

func TestMinioFileWriter_WriteDataAsync_Success(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	resultCh := newMockResultChannel()

	// Write entry 0 - should succeed and be written to buffer
	id, err := w.WriteDataAsync(ctx, 0, []byte("hello"), resultCh)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id)

	// Write entry 1
	id, err = w.WriteDataAsync(ctx, 1, []byte("world"), resultCh)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), id)

	// Verify buffer has data
	buf := w.buffer.Load()
	assert.Greater(t, buf.DataSize.Load(), int64(0))
}

func TestMinioFileWriter_Close_Idempotent(t *testing.T) {
	w := newTestMinioFileWriter()

	// Mark as already done to bypass flush waiting
	w.allUploadingTaskDone.Store(true)

	ctx := context.Background()

	// First close
	err := w.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, w.closed.Load())

	// Second close should be idempotent (returns nil)
	err = w.Close(ctx)
	assert.NoError(t, err)
}

func TestMinioFileWriter_Finalize_AlreadyFinalized(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.finalized.Store(true)
	w.lastEntryID.Store(42)

	lastId, err := w.Finalize(ctx, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), lastId)
}

func TestMinioFileWriter_Fence_AlreadyFenced(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.fenced.Store(true)
	w.lastEntryID.Store(10)

	lastId, err := w.Fence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), lastId)
}

func TestMinioFileWriter_Fence_AlreadyFinalized(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.footerRecord = &codec.FooterRecord{TotalBlocks: 3}
	w.lastEntryID.Store(99)

	lastId, err := w.Fence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(99), lastId)
}

func TestMinioFileWriter_PrepareMultiBlockData_Empty(t *testing.T) {
	w := newTestMinioFileWriter()
	partitions, firstEntryIds, sizes := w.prepareMultiBlockDataIfNecessary(nil, 0)
	assert.Nil(t, partitions)
	assert.Nil(t, firstEntryIds)
	assert.Nil(t, sizes)
}

func TestMinioFileWriter_PrepareMultiBlockData_SingleBlock(t *testing.T) {
	w := newTestMinioFileWriter()
	entries := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("data0")},
		{EntryId: 1, Data: []byte("data1")},
	}

	partitions, firstEntryIds, sizes := w.prepareMultiBlockDataIfNecessary(entries, 0)
	assert.Len(t, partitions, 1)
	assert.Len(t, firstEntryIds, 1)
	assert.Len(t, sizes, 1)
	assert.Equal(t, int64(0), firstEntryIds[0])
	assert.Equal(t, 2, len(partitions[0]))
}

func TestMinioFileWriter_PrepareMultiBlockData_MultiBlock(t *testing.T) {
	// Set maxFlushSize very small to force multiple blocks
	w := newTestMinioFileWriter()
	w.syncPolicyConfig.MaxFlushSize = config.NewByteSize(5)

	entries := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("aaaaa")}, // 5 bytes = hits threshold
		{EntryId: 1, Data: []byte("bbbbb")}, // 5 bytes = hits threshold
		{EntryId: 2, Data: []byte("cc")},    // remaining
	}

	partitions, firstEntryIds, sizes := w.prepareMultiBlockDataIfNecessary(entries, 0)

	// Should create multiple partitions since each entry hits the 5 byte threshold
	assert.GreaterOrEqual(t, len(partitions), 2)
	assert.Equal(t, len(partitions), len(firstEntryIds))
	assert.Equal(t, len(partitions), len(sizes))
	assert.Equal(t, int64(0), firstEntryIds[0])
}

func TestMinioFileWriter_FastFlushFailUnsafe(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	resultCh1 := newMockResultChannel()
	resultCh2 := newMockResultChannel()

	blockData := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("data0"), NotifyChan: resultCh1, EnqueueTime: time.Now()},
		{EntryId: 1, Data: []byte("data1"), NotifyChan: resultCh2, EnqueueTime: time.Now()},
	}

	testErr := werr.ErrStorageNotWritable
	w.fastFlushFailUnsafe(ctx, blockData, testErr)

	// Both channels should have received error results
	assert.Len(t, resultCh1.results, 1)
	assert.Len(t, resultCh2.results, 1)
}

func TestMinioFileWriter_FastFlushSuccessUnsafe(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	resultCh1 := newMockResultChannel()
	resultCh2 := newMockResultChannel()

	blockInfo := &BlockInfo{
		FirstEntryID: 0,
		LastEntryID:  1,
		BlockKey:     "test-base/1/0/0.blk",
		BlockID:      0,
		Size:         100,
	}
	blockData := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("data0"), NotifyChan: resultCh1, EnqueueTime: time.Now()},
		{EntryId: 1, Data: []byte("data1"), NotifyChan: resultCh2, EnqueueTime: time.Now()},
	}

	w.fastFlushSuccessUnsafe(ctx, blockInfo, blockData)

	// Both channels should have received success results
	assert.Len(t, resultCh1.results, 1)
	assert.Len(t, resultCh2.results, 1)

	// blockIndexes should be updated
	assert.Len(t, w.blockIndexes, 1)
	assert.Equal(t, int32(0), w.blockIndexes[0].BlockNumber)
	assert.Equal(t, int64(0), w.blockIndexes[0].FirstEntryID)
	assert.Equal(t, int64(1), w.blockIndexes[0].LastEntryID)
}

func TestMinioFileWriter_QuickSyncFailUnsafe(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	err := w.quickSyncFailUnsafe(ctx, werr.ErrStorageNotWritable)
	assert.Error(t, err)
	assert.True(t, werr.ErrStorageNotWritable.Is(err))
}

// ===== Helper function tests =====

func TestGetSegmentFileKey(t *testing.T) {
	key := getSegmentFileKey("base/dir", 1, 0)
	assert.Equal(t, "base/dir/1/0", key)

	key = getSegmentFileKey("root", 42, 99)
	assert.Equal(t, "root/42/99", key)
}

func TestGetBlockKey(t *testing.T) {
	key := getBlockKey("base/1/0", 0)
	assert.Equal(t, "base/1/0/0.blk", key)

	key = getBlockKey("base/1/0", 5)
	assert.Equal(t, "base/1/0/5.blk", key)
}

func TestGetFooterBlockKey(t *testing.T) {
	key := getFooterBlockKey("base/1/0")
	assert.Equal(t, "base/1/0/footer.blk", key)
}

func TestGetSegmentLockKey(t *testing.T) {
	key := getSegmentLockKey("base/1/0")
	assert.Equal(t, "base/1/0/write.lock", key)
}

func TestMinioFileWriter_WriteDataAsync_MultipleSequentialEntries(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	// Write several sequential entries
	for i := int64(0); i < 5; i++ {
		resultCh := newMockResultChannel()
		id, err := w.WriteDataAsync(ctx, i, []byte("test data"), resultCh)
		assert.NoError(t, err)
		assert.Equal(t, i, id)
	}

	// Verify buffer has data
	buf := w.buffer.Load()
	assert.Greater(t, buf.DataSize.Load(), int64(0))
}

func TestMinioFileWriter_StateTransitions(t *testing.T) {
	// Test that writer correctly enforces state ordering
	ctx := context.Background()
	w := newTestMinioFileWriter()
	resultCh := newMockResultChannel()

	// Initially writable
	_, err := w.WriteDataAsync(ctx, 0, []byte("data"), resultCh)
	assert.NoError(t, err)

	// After fencing, should reject writes
	w.fenced.Store(true)
	_, err = w.WriteDataAsync(ctx, 1, []byte("data"), resultCh)
	assert.True(t, werr.ErrSegmentFenced.Is(err))

	// Reset fence, set finalized
	w.fenced.Store(false)
	w.finalized.Store(true)
	_, err = w.WriteDataAsync(ctx, 1, []byte("data"), resultCh)
	assert.True(t, werr.ErrFileWriterFinalized.Is(err))

	// Reset finalized, set closed
	w.finalized.Store(false)
	w.closed.Store(true)
	_, err = w.WriteDataAsync(ctx, 1, []byte("data"), resultCh)
	assert.True(t, werr.ErrFileWriterAlreadyClosed.Is(err))
}

// ===== serialize tests =====

func TestMinioFileWriter_Serialize_Block0_WithHeaderRecord(t *testing.T) {
	w := newTestMinioFileWriter()
	entries := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("hello")},
		{EntryId: 1, Data: []byte("world")},
	}

	data := w.serialize(0, entries)
	assert.NotEmpty(t, data)
	assert.True(t, w.headerWritten.Load()) // Header should be written for block 0

	// Verify the serialized data can be decoded back
	records, err := codec.DecodeRecordList(data)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(records), 3) // HeaderRecord + BlockHeaderRecord + at least 1 DataRecord

	// First record should be HeaderRecord
	_, ok := records[0].(*codec.HeaderRecord)
	assert.True(t, ok)

	// Second record should be BlockHeaderRecord
	blockHeader, ok := records[1].(*codec.BlockHeaderRecord)
	assert.True(t, ok)
	assert.Equal(t, int32(0), blockHeader.BlockNumber)
	assert.Equal(t, int64(0), blockHeader.FirstEntryID)
	assert.Equal(t, int64(1), blockHeader.LastEntryID)
}

func TestMinioFileWriter_Serialize_Block1_NoHeaderRecord(t *testing.T) {
	w := newTestMinioFileWriter()
	entries := []*cache.BufferEntry{
		{EntryId: 10, Data: []byte("data10")},
	}

	data := w.serialize(1, entries)
	assert.NotEmpty(t, data)
	assert.False(t, w.headerWritten.Load()) // Header is NOT written for block != 0

	records, err := codec.DecodeRecordList(data)
	assert.NoError(t, err)
	// First record should be BlockHeaderRecord (no HeaderRecord)
	blockHeader, ok := records[0].(*codec.BlockHeaderRecord)
	assert.True(t, ok)
	assert.Equal(t, int32(1), blockHeader.BlockNumber)
	assert.Equal(t, int64(10), blockHeader.FirstEntryID)
	assert.Equal(t, int64(10), blockHeader.LastEntryID)
}

func TestMinioFileWriter_Serialize_EmptyEntries(t *testing.T) {
	w := newTestMinioFileWriter()
	data := w.serialize(0, []*cache.BufferEntry{})
	assert.Empty(t, data)
}

// ===== extractDataRecords tests =====

func TestMinioFileWriter_ExtractDataRecords(t *testing.T) {
	w := newTestMinioFileWriter()

	// Serialize a block with header + block header + data records
	entries := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("hello")},
		{EntryId: 1, Data: []byte("world")},
	}
	blockData := w.serialize(0, entries)

	// Extract only data records
	dataRecords, err := w.extractDataRecords(blockData)
	assert.NoError(t, err)
	assert.NotEmpty(t, dataRecords)

	// Decoded records should only be DataRecord type
	records, err := codec.DecodeRecordList(dataRecords)
	assert.NoError(t, err)
	for _, r := range records {
		assert.Equal(t, codec.DataRecordType, r.Type())
	}
}

func TestMinioFileWriter_ExtractDataRecords_NoDataRecords(t *testing.T) {
	w := newTestMinioFileWriter()
	// A valid block with only header record and block header, no data records
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	data := codec.EncodeRecord(headerRecord)
	result, err := w.extractDataRecords(data)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

// ===== isOriginalBlockFile tests =====

func TestMinioFileWriter_IsOriginalBlockFile(t *testing.T) {
	w := newTestMinioFileWriter()

	// Valid original block files
	assert.True(t, w.isOriginalBlockFile("test-base/1/0/0.blk"))
	assert.True(t, w.isOriginalBlockFile("test-base/1/0/5.blk"))
	assert.True(t, w.isOriginalBlockFile("test-base/1/0/123.blk"))

	// Not under our segment directory
	assert.False(t, w.isOriginalBlockFile("other-base/1/0/0.blk"))

	// Footer and lock files should be excluded
	assert.False(t, w.isOriginalBlockFile("test-base/1/0/footer.blk"))
	assert.False(t, w.isOriginalBlockFile("test-base/1/0/write.lock"))

	// Merged block files (m_ prefix) should be excluded
	assert.False(t, w.isOriginalBlockFile("test-base/1/0/m_0.blk"))
	assert.False(t, w.isOriginalBlockFile("test-base/1/0/m_5.blk"))

	// Non-blk files
	assert.False(t, w.isOriginalBlockFile("test-base/1/0/something.txt"))

	// Invalid block id
	assert.False(t, w.isOriginalBlockFile("test-base/1/0/abc.blk"))
}

// ===== convertBlockIndexesToKeys tests =====

func TestMinioFileWriter_ConvertBlockIndexesToKeys(t *testing.T) {
	w := newTestMinioFileWriter()

	// Empty input
	keys := w.convertBlockIndexesToKeys(nil)
	assert.Nil(t, keys)

	keys = w.convertBlockIndexesToKeys([]*codec.IndexRecord{})
	assert.Nil(t, keys)

	// Normal input
	indexes := []*codec.IndexRecord{
		{BlockNumber: 0},
		{BlockNumber: 1},
		{BlockNumber: 2},
	}
	keys = w.convertBlockIndexesToKeys(indexes)
	assert.Len(t, keys, 3)
	assert.Equal(t, "test-base/1/0/0.blk", keys[0])
	assert.Equal(t, "test-base/1/0/1.blk", keys[1])
	assert.Equal(t, "test-base/1/0/2.blk", keys[2])
}

// ===== planMergeBlockTasks tests =====

func TestMinioFileWriter_PlanMergeBlockTasks_Empty(t *testing.T) {
	w := newTestMinioFileWriter()
	w.blockIndexes = nil
	tasks := w.planMergeBlockTasks(1024 * 1024)
	assert.Empty(t, tasks)
}

func TestMinioFileWriter_PlanMergeBlockTasks_SingleBlock(t *testing.T) {
	w := newTestMinioFileWriter()
	w.firstEntryID.Store(0)
	w.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
	}
	tasks := w.planMergeBlockTasks(1024 * 1024)
	assert.Len(t, tasks, 1)
	assert.Len(t, tasks[0].blocks, 1)
	assert.Equal(t, int64(10), tasks[0].nextEntryID)
}

func TestMinioFileWriter_PlanMergeBlockTasks_MultipleBlocks_Merged(t *testing.T) {
	w := newTestMinioFileWriter()
	w.firstEntryID.Store(0)
	w.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
		{BlockNumber: 1, BlockSize: 100, FirstEntryID: 10, LastEntryID: 19},
		{BlockNumber: 2, BlockSize: 100, FirstEntryID: 20, LastEntryID: 29},
	}
	// Target large enough to merge all blocks
	tasks := w.planMergeBlockTasks(1024 * 1024)
	assert.Len(t, tasks, 1)
	assert.Len(t, tasks[0].blocks, 3)
}

func TestMinioFileWriter_PlanMergeBlockTasks_MultipleBlocks_Split(t *testing.T) {
	w := newTestMinioFileWriter()
	w.firstEntryID.Store(0)
	w.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, BlockSize: 500, FirstEntryID: 0, LastEntryID: 9},
		{BlockNumber: 1, BlockSize: 500, FirstEntryID: 10, LastEntryID: 19},
		{BlockNumber: 2, BlockSize: 500, FirstEntryID: 20, LastEntryID: 29},
	}
	// Target of 600 should cause split after first two blocks (500+500 >= 600)
	tasks := w.planMergeBlockTasks(600)
	assert.GreaterOrEqual(t, len(tasks), 2)
}

// ===== serializeFooterAndIndexes tests =====

func TestSerializeFooterAndIndexes(t *testing.T) {
	ctx := context.Background()
	blocks := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
		{BlockNumber: 1, StartOffset: 1, BlockSize: 200, FirstEntryID: 10, LastEntryID: 19},
	}
	data, footer := serializeFooterAndIndexes(ctx, blocks)
	assert.NotEmpty(t, data)
	assert.NotNil(t, footer)
	assert.Equal(t, int32(2), footer.TotalBlocks)
	assert.Equal(t, uint64(300), footer.TotalSize)
	assert.Equal(t, int64(19), footer.LAC)
}

func TestSerializeFooterAndIndexes_Empty(t *testing.T) {
	ctx := context.Background()
	data, footer := serializeFooterAndIndexes(ctx, []*codec.IndexRecord{})
	// Footer still created with zero blocks
	assert.NotEmpty(t, data) // footer record itself
	assert.NotNil(t, footer)
	assert.Equal(t, int32(0), footer.TotalBlocks)
}

// ===== serializeCompactedFooterAndIndexes tests =====

func TestMinioFileWriter_SerializeCompactedFooterAndIndexes(t *testing.T) {
	w := newTestMinioFileWriter()
	ctx := context.Background()
	blocks := []*codec.IndexRecord{
		{BlockNumber: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
	}
	footer := &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 10,
		TotalSize:    100,
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0),
	}
	data := w.serializeCompactedFooterAndIndexes(ctx, blocks, footer)
	assert.NotEmpty(t, data)
}

// ===== parseBlockIdFromBlockKey tests =====

func TestParseBlockIdFromBlockKey(t *testing.T) {
	t.Run("regular_block", func(t *testing.T) {
		id, isMerge, err := parseBlockIdFromBlockKey("base/1/0/5.blk")
		assert.NoError(t, err)
		assert.Equal(t, int64(5), id)
		assert.False(t, isMerge)
	})

	t.Run("merged_block", func(t *testing.T) {
		id, isMerge, err := parseBlockIdFromBlockKey("base/1/0/m_3.blk")
		assert.NoError(t, err)
		assert.Equal(t, int64(3), id)
		assert.True(t, isMerge)
	})

	t.Run("block_0", func(t *testing.T) {
		id, isMerge, err := parseBlockIdFromBlockKey("base/1/0/0.blk")
		assert.NoError(t, err)
		assert.Equal(t, int64(0), id)
		assert.False(t, isMerge)
	})

	t.Run("invalid_name", func(t *testing.T) {
		_, _, err := parseBlockIdFromBlockKey("base/1/0/abc.blk")
		assert.Error(t, err)
	})
}

// ===== getHostname tests =====

func TestGetHostname(t *testing.T) {
	hostname := getHostname()
	assert.NotEmpty(t, hostname)
	assert.NotEqual(t, "unknown", hostname)
}

// ===== getMergedBlockKey tests =====

func TestGetMergedBlockKey(t *testing.T) {
	key := getMergedBlockKey("base/1/0", 0)
	assert.Equal(t, "base/1/0/m_0.blk", key)

	key = getMergedBlockKey("base/1/0", 3)
	assert.Equal(t, "base/1/0/m_3.blk", key)
}

// ===== awaitAllFlushTasks tests =====

func TestMinioFileWriter_AwaitAllFlushTasks_AlreadyDone(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.allUploadingTaskDone.Store(true)

	err := w.awaitAllFlushTasks(ctx)
	assert.NoError(t, err)
}

// ===== waitIfFlushingBufferSizeExceededUnsafe tests =====

func TestMinioFileWriter_WaitIfFlushingBufferSizeExceeded_NormalCase(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	// Flushing size is 0 which is less than maxBufferSize
	w.flushingBufferSize.Store(0)

	err := w.waitIfFlushingBufferSizeExceededUnsafe(ctx)
	assert.NoError(t, err)
}

func TestMinioFileWriter_WaitIfFlushingBufferSizeExceeded_ContextCancelled(t *testing.T) {
	w := newTestMinioFileWriter()
	// Set flushing buffer size exceeding the limit
	w.flushingBufferSize.Store(w.maxBufferSize + 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := w.waitIfFlushingBufferSizeExceededUnsafe(ctx)
	assert.Error(t, err)
}

// ===== releaseSegmentLock tests =====

func TestMinioFileWriter_ReleaseSegmentLock_EmptyKey(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.lockObjectKey = ""

	err := w.releaseSegmentLock(ctx)
	assert.NoError(t, err)
}

func TestMinioFileWriter_ReleaseSegmentLock_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.lockObjectKey = "test-base/1/0/write.lock"

	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(nil).Once()

	err := w.releaseSegmentLock(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "", w.lockObjectKey)
}

func TestMinioFileWriter_ReleaseSegmentLock_AlreadyRemoved(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.lockObjectKey = "test-base/1/0/write.lock"

	notFoundErr := werr.ErrEntryNotFound.WithCauseErrMsg("not found")
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	err := w.releaseSegmentLock(ctx)
	assert.NoError(t, err)
}

func TestMinioFileWriter_ReleaseSegmentLock_OtherError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.lockObjectKey = "test-base/1/0/write.lock"

	otherErr := errors.New("network error")
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(otherErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(otherErr).Return(false).Once()

	err := w.releaseSegmentLock(ctx)
	assert.Error(t, err)
}

// ===== isSegmentLocked tests =====

func TestMinioFileWriter_IsSegmentLocked_NotLocked(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	notFoundErr := werr.ErrEntryNotFound.WithCauseErrMsg("not found")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	locked, err := w.isSegmentLocked(ctx)
	assert.NoError(t, err)
	assert.False(t, locked)
}

func TestMinioFileWriter_IsSegmentLocked_Locked(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(int64(100), false, nil).Once()

	locked, err := w.isSegmentLocked(ctx)
	assert.NoError(t, err)
	assert.True(t, locked)
}

func TestMinioFileWriter_IsSegmentLocked_Error(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	statErr := errors.New("network error")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(int64(0), false, statErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(statErr).Return(false).Once()

	locked, err := w.isSegmentLocked(ctx)
	assert.Error(t, err)
	assert.False(t, locked)
}

// ===== createSegmentLock tests =====

func TestMinioFileWriter_CreateSegmentLock_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	mockClient.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	err := w.createSegmentLock(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "test-base/1/0/write.lock", w.lockObjectKey)
}

func TestMinioFileWriter_CreateSegmentLock_AlreadyLocked(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	mockClient.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(werr.ErrObjectAlreadyExists).Once()

	err := w.createSegmentLock(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already locked")
}

func TestMinioFileWriter_CreateSegmentLock_OtherError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	mockClient.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("put error")).Once()

	err := w.createSegmentLock(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create lock object")
}

// ===== Sync tests =====

func TestMinioFileWriter_Sync_StorageNotWritable(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.storageWritable.Store(false)

	err := w.Sync(ctx)
	assert.Error(t, err)
	assert.True(t, werr.ErrStorageNotWritable.Is(err))
}

func TestMinioFileWriter_Sync_EmptyBuffer(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	err := w.Sync(ctx)
	assert.NoError(t, err)
}

// ===== Compact tests =====

func TestMinioFileWriter_Compact_AlreadyCompacted(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.footerRecord = &codec.FooterRecord{
		TotalBlocks: 1,
		TotalSize:   500,
		Flags:       codec.SetCompacted(0),
	}

	// listOriginalBlockFiles will WalkWithObjects and find nothing
	mockClient.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	size, err := w.Compact(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(500), size)
}

// TestMinioFileWriter_Compact_CancelledContext verifies that Compact returns promptly
// when the context is cancelled, instead of proceeding through the entire compaction.
// Regression test: previously goroutines in the compaction path did not check ctx.Err().
func TestMinioFileWriter_Compact_CancelledContext(t *testing.T) {
	w := newTestMinioFileWriter()
	w.client = mocks_objectstorage.NewObjectStorage(t)

	w.footerRecord = &codec.FooterRecord{
		TotalBlocks:  2,
		TotalRecords: 3,
		TotalSize:    200,
		Version:      codec.FormatVersion,
		Flags:        0, // NOT compacted
	}
	w.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 1},
		{BlockNumber: 1, StartOffset: 1, BlockSize: 100, FirstEntryID: 2, LastEntryID: 2},
	}
	w.firstEntryID.Store(0)
	w.lastEntryID.Store(2)

	// Use an already-cancelled context
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := w.Compact(cancelCtx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestMinioFileWriter_Compact_NotFinalized(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.footerRecord = nil // not finalized

	_, err := w.Compact(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "finalized before compaction")
}

// ===== recoverFromStorageUnsafe tests =====

func TestMinioFileWriter_RecoverFromStorage_NoFooter_NoBlocks(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	notFoundErr := werr.ErrEntryNotFound.WithCauseErrMsg("not found")

	// StatObject for footer.blk returns not found
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Maybe()

	// StatObject for block 0 returns not found too (no blocks)
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()

	err := w.recoverFromStorageUnsafe(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), w.lastEntryID.Load())
}

// ===== recoverFromFullListing tests =====

func TestMinioFileWriter_RecoverFromFullListing_NoBlocks(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	notFoundErr := werr.ErrEntryNotFound.WithCauseErrMsg("not found")

	// StatObject for block 0 returns not found
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Maybe()

	err := w.recoverFromFullListing(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), w.lastEntryID.Load())
	assert.Empty(t, w.blockIndexes)
}

func TestMinioFileWriter_RecoverFromFullListing_FencedBlock(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// Block 0 is fenced
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(100), true, nil).Once()

	err := w.recoverFromFullListing(ctx)
	assert.NoError(t, err)
	assert.True(t, w.fenced.Load())
}

func TestMinioFileWriter_RecoverFromFullListing_StatError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	statErr := errors.New("network error")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, statErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(statErr).Return(false).Once()

	err := w.recoverFromFullListing(ctx)
	assert.Error(t, err)
}

func TestMinioFileWriter_RecoverFromFullListing_WithBlocks(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// Serialize a valid block 0 to use in mock responses
	entries := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("hello")},
		{EntryId: 1, Data: []byte("world")},
	}
	blockData := w.serialize(0, entries)
	readSize := int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)

	// Block 0 exists
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()

	// GetObject for block 0 header
	headerData := blockData[:readSize]
	mockReader := &writerMockFileReader{data: headerData}
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(mockReader, nil).Once()

	notFoundErr := werr.ErrEntryNotFound.WithCauseErrMsg("not found")
	// Block 1 does not exist
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Maybe()

	err := w.recoverFromFullListing(ctx)
	assert.NoError(t, err)
	assert.Len(t, w.blockIndexes, 1)
	assert.Equal(t, int64(0), w.firstEntryID.Load())
	assert.Equal(t, int64(1), w.lastEntryID.Load())
	assert.True(t, w.headerWritten.Load())
}

// ===== recoverFromFooter tests =====

func TestMinioFileWriter_RecoverFromFooter(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// Create footer data using serializeFooterAndIndexes
	blocks := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
		{BlockNumber: 1, StartOffset: 1, BlockSize: 200, FirstEntryID: 10, LastEntryID: 19},
	}
	footerData, _ := serializeFooterAndIndexes(ctx, blocks)

	footerBlockKey := "test-base/1/0/footer.blk"
	footerBlockSize := int64(len(footerData))

	mockReader := &writerMockFileReader{data: footerData}
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerBlockKey, int64(0), footerBlockSize, mock.Anything, mock.Anything).
		Return(mockReader, nil).Once()

	err := w.recoverFromFooter(ctx, footerBlockKey, footerBlockSize)
	assert.NoError(t, err)
	assert.NotNil(t, w.footerRecord)
	assert.True(t, w.finalized.Load())
	assert.False(t, w.storageWritable.Load())
	assert.Len(t, w.blockIndexes, 2)
	assert.Equal(t, int64(0), w.firstEntryID.Load())
	assert.Equal(t, int64(19), w.lastEntryID.Load())
}

func TestMinioFileWriter_RecoverFromFooter_GetObjectError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	footerBlockKey := "test-base/1/0/footer.blk"
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerBlockKey, int64(0), int64(100), mock.Anything, mock.Anything).
		Return(nil, errors.New("get error")).Once()

	err := w.recoverFromFooter(ctx, footerBlockKey, 100)
	assert.Error(t, err)
}

func TestMinioFileWriter_RecoverFromFooter_TooSmall(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	footerBlockKey := "test-base/1/0/footer.blk"
	smallData := []byte("too small")
	mockReader := &writerMockFileReader{data: smallData}
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerBlockKey, int64(0), int64(len(smallData)), mock.Anything, mock.Anything).
		Return(mockReader, nil).Once()

	err := w.recoverFromFooter(ctx, footerBlockKey, int64(len(smallData)))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too small")
}

// ===== Compact with mock - streamMergeAndUploadBlocks returns empty =====

func TestMinioFileWriter_Compact_NoBlocksToCompact(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.footerRecord = &codec.FooterRecord{
		TotalBlocks: 0,
		TotalSize:   0,
		Version:     codec.FormatVersion,
		Flags:       0, // Not compacted
	}
	w.blockIndexes = []*codec.IndexRecord{} // No blocks

	size, err := w.Compact(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), size)
}

// writerMockFileReader implements minioHandler.FileReader for writer tests
type writerMockFileReader struct {
	data     []byte
	position int
}

func (m *writerMockFileReader) Read(p []byte) (n int, err error) {
	if m.position >= len(m.data) {
		return 0, io.EOF
	}
	n = copy(p, m.data[m.position:])
	m.position += n
	if m.position >= len(m.data) {
		err = io.EOF
	}
	return n, err
}

func (m *writerMockFileReader) Close() error {
	return nil
}

func (m *writerMockFileReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n = copy(p, m.data[off:])
	if off+int64(n) >= int64(len(m.data)) {
		err = io.EOF
	}
	return n, err
}

func (m *writerMockFileReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		m.position = int(offset)
	case io.SeekCurrent:
		m.position += int(offset)
	case io.SeekEnd:
		m.position = len(m.data) + int(offset)
	}
	return int64(m.position), nil
}

func (m *writerMockFileReader) Size() (int64, error) {
	return int64(len(m.data)), nil
}

// Verify writerMockFileReader implements minioHandler.FileReader
var _ minioHandler.FileReader = (*writerMockFileReader)(nil)

// ===== Finalize with mock =====

func TestMinioFileWriter_Finalize_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.allUploadingTaskDone.Store(true) // No pending upload tasks

	// Write data first
	resultCh := newMockResultChannel()
	_, err := w.WriteDataAsync(ctx, 0, []byte("hello"), resultCh)
	require.NoError(t, err)
	_, err = w.WriteDataAsync(ctx, 1, []byte("world"), resultCh)
	require.NoError(t, err)

	// The sync will try PutObjectIfNoneMatch for block upload
	mockClient.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	lastId, err := w.Finalize(ctx, -1)
	assert.NoError(t, err)
	assert.True(t, w.finalized.Load())
	_ = lastId
}

// ===== deleteOriginalBlocksByKeys tests =====

func TestMinioFileWriter_DeleteOriginalBlocksByKeys_Empty(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	err := w.deleteOriginalBlocksByKeys(ctx, nil)
	assert.NoError(t, err)

	err = w.deleteOriginalBlocksByKeys(ctx, []string{})
	assert.NoError(t, err)
}

func TestMinioFileWriter_DeleteOriginalBlocksByKeys_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Times(2)

	keys := []string{"test-base/1/0/0.blk", "test-base/1/0/1.blk"}
	err := w.deleteOriginalBlocksByKeys(ctx, keys)
	assert.NoError(t, err)
}

func TestMinioFileWriter_DeleteOriginalBlocksByKeys_PartialFailure(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// First delete succeeds, second fails
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(nil).Once()
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(errors.New("delete failed")).Once()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(false).Maybe()

	keys := []string{"test-base/1/0/0.blk", "test-base/1/0/1.blk"}
	err := w.deleteOriginalBlocksByKeys(ctx, keys)
	assert.Error(t, err)
}

// ===== listOriginalBlockFiles tests =====

func TestMinioFileWriter_ListOriginalBlockFiles(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	mockClient.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", "test-base/1/0/", false, mock.Anything, mock.Anything, mock.Anything).
		Run(func(ctx context.Context, bucketName string, prefix string, recursive bool, fn commonObjectStorage.ChunkObjectWalkFunc, ns string, logId string) {
			fn(&commonObjectStorage.ChunkObjectInfo{FilePath: "test-base/1/0/0.blk"})
			fn(&commonObjectStorage.ChunkObjectInfo{FilePath: "test-base/1/0/1.blk"})
			fn(&commonObjectStorage.ChunkObjectInfo{FilePath: "test-base/1/0/footer.blk"}) // should be excluded
			fn(&commonObjectStorage.ChunkObjectInfo{FilePath: "test-base/1/0/write.lock"}) // should be excluded
			fn(&commonObjectStorage.ChunkObjectInfo{FilePath: "test-base/1/0/m_0.blk"})    // merged, excluded
		}).Return(nil).Once()

	keys, err := w.listOriginalBlockFiles(ctx)
	assert.NoError(t, err)
	assert.Len(t, keys, 2) // only 0.blk and 1.blk
	assert.Contains(t, keys, "test-base/1/0/0.blk")
	assert.Contains(t, keys, "test-base/1/0/1.blk")
}

// ===== readBlockData tests =====

func TestMinioFileWriter_ReadBlockData_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	blockKey := "test-base/1/0/0.blk"
	blockContent := []byte("block data content here")

	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", blockKey, mock.Anything, mock.Anything).
		Return(int64(len(blockContent)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, int64(0), int64(len(blockContent)), mock.Anything, mock.Anything).
		Return(&writerMockFileReader{data: blockContent}, nil).Once()

	data, err := w.readBlockData(ctx, blockKey)
	assert.NoError(t, err)
	assert.Equal(t, blockContent, data)
}

func TestMinioFileWriter_ReadBlockData_StatError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	blockKey := "test-base/1/0/0.blk"
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", blockKey, mock.Anything, mock.Anything).
		Return(int64(0), false, errors.New("stat error")).Once()

	data, err := w.readBlockData(ctx, blockKey)
	assert.Error(t, err)
	assert.Nil(t, data)
}

func TestMinioFileWriter_ReadBlockData_GetError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	blockKey := "test-base/1/0/0.blk"
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", blockKey, mock.Anything, mock.Anything).
		Return(int64(100), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, int64(0), int64(100), mock.Anything, mock.Anything).
		Return(nil, errors.New("get error")).Once()

	data, err := w.readBlockData(ctx, blockKey)
	assert.Error(t, err)
	assert.Nil(t, data)
}

// ===== uploadSingleMergedBlock tests =====

func TestMinioFileWriter_UploadSingleMergedBlock_FirstBlock(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.footerRecord = &codec.FooterRecord{Flags: 0}

	// Create valid data records (the merged block content)
	dr1, _ := codec.ParseData([]byte("hello"))
	dr2, _ := codec.ParseData([]byte("world"))
	mergedData := append(codec.EncodeRecord(dr1), codec.EncodeRecord(dr2)...)

	mockClient.EXPECT().PutObject(mock.Anything, "test-bucket", "test-base/1/0/m_0.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	indexRecord, blockSize, err := w.uploadSingleMergedBlock(ctx, mergedData, 0, 0, 1, true)
	assert.NoError(t, err)
	assert.NotNil(t, indexRecord)
	assert.Greater(t, blockSize, int64(0))
	assert.Equal(t, int32(0), indexRecord.BlockNumber)
	assert.Equal(t, int64(0), indexRecord.FirstEntryID)
	assert.Equal(t, int64(1), indexRecord.LastEntryID)
}

func TestMinioFileWriter_UploadSingleMergedBlock_NonFirstBlock(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.footerRecord = &codec.FooterRecord{Flags: 0}

	dr1, _ := codec.ParseData([]byte("data"))
	mergedData := codec.EncodeRecord(dr1)

	mockClient.EXPECT().PutObject(mock.Anything, "test-bucket", "test-base/1/0/m_1.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	indexRecord, blockSize, err := w.uploadSingleMergedBlock(ctx, mergedData, 1, 10, 10, false)
	assert.NoError(t, err)
	assert.NotNil(t, indexRecord)
	assert.Greater(t, blockSize, int64(0))
	assert.Equal(t, int32(1), indexRecord.BlockNumber)
	assert.Equal(t, int64(10), indexRecord.FirstEntryID)
	assert.Equal(t, int64(10), indexRecord.LastEntryID)
}

func TestMinioFileWriter_UploadSingleMergedBlock_PutError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.footerRecord = &codec.FooterRecord{Flags: 0}

	dr1, _ := codec.ParseData([]byte("data"))
	mergedData := codec.EncodeRecord(dr1)

	mockClient.EXPECT().PutObject(mock.Anything, "test-bucket", "test-base/1/0/m_0.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("put error")).Once()

	indexRecord, blockSize, err := w.uploadSingleMergedBlock(ctx, mergedData, 0, 0, 0, true)
	assert.Error(t, err)
	assert.Nil(t, indexRecord)
	assert.Equal(t, int64(-1), blockSize)
}

// ===== Compact full test with mocked storage =====

func TestMinioFileWriter_Compact_FullFlow(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// Create valid block data
	block0Data := w.serialize(0, []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("hello")},
		{EntryId: 1, Data: []byte("world")},
	})
	block1Data := w.serialize(1, []*cache.BufferEntry{
		{EntryId: 2, Data: []byte("foo")},
	})

	// Set up finalized footer
	w.footerRecord = &codec.FooterRecord{
		TotalBlocks:  2,
		TotalRecords: 3,
		TotalSize:    uint64(len(block0Data) + len(block1Data)),
		Version:      codec.FormatVersion,
		Flags:        0, // NOT compacted
	}
	w.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: uint32(len(block0Data)), FirstEntryID: 0, LastEntryID: 1},
		{BlockNumber: 1, StartOffset: 1, BlockSize: uint32(len(block1Data)), FirstEntryID: 2, LastEntryID: 2},
	}
	w.firstEntryID.Store(0)
	w.lastEntryID.Store(2)

	// readBlockData for block 0
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(block0Data)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(len(block0Data)), mock.Anything, mock.Anything).
		Return(&writerMockFileReader{data: block0Data}, nil).Once()

	// readBlockData for block 1
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(len(block1Data)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", int64(0), int64(len(block1Data)), mock.Anything, mock.Anything).
		Return(&writerMockFileReader{data: block1Data}, nil).Once()

	// uploadSingleMergedBlock for merged block 0
	mockClient.EXPECT().PutObject(mock.Anything, "test-bucket", "test-base/1/0/m_0.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	// upload compacted footer
	mockClient.EXPECT().PutObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	// delete original blocks
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(nil).Once()
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	size, err := w.Compact(ctx)
	assert.NoError(t, err)
	assert.Greater(t, size, int64(0))
	assert.True(t, codec.IsCompacted(w.footerRecord.Flags))
}

// TestMinioFileWriter_Compact_PreservesLAC verifies that compaction preserves the
// original footer's LAC value instead of resetting it to -1.
// Regression test: previously LAC was hardcoded to -1 in the compacted footer.
func TestMinioFileWriter_Compact_PreservesLAC(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// Create valid block data
	block0Data := w.serialize(0, []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("hello")},
		{EntryId: 1, Data: []byte("world")},
	})

	// Set up finalized footer with a specific LAC value
	originalLAC := int64(1)
	w.footerRecord = &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 2,
		TotalSize:    uint64(len(block0Data)),
		Version:      codec.FormatVersion,
		Flags:        0,
		LAC:          originalLAC,
	}
	w.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: uint32(len(block0Data)), FirstEntryID: 0, LastEntryID: 1},
	}
	w.firstEntryID.Store(0)
	w.lastEntryID.Store(1)

	// readBlockData for block 0
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(block0Data)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(len(block0Data)), mock.Anything, mock.Anything).
		Return(&writerMockFileReader{data: block0Data}, nil).Once()

	// uploadSingleMergedBlock
	mockClient.EXPECT().PutObject(mock.Anything, "test-bucket", "test-base/1/0/m_0.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	// upload compacted footer — capture the footer data to verify LAC
	var capturedFooterData []byte
	mockClient.EXPECT().PutObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, _ string, reader io.Reader, size int64, _ string, _ string) {
			capturedFooterData = make([]byte, size)
			_, _ = io.ReadFull(reader, capturedFooterData)
		}).
		Return(nil).Once()

	// delete original block
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	size, err := w.Compact(ctx)
	assert.NoError(t, err)
	assert.Greater(t, size, int64(0))

	// Verify footer in memory has preserved LAC
	assert.Equal(t, originalLAC, w.footerRecord.LAC, "compacted footer must preserve original LAC")
	assert.True(t, codec.IsCompacted(w.footerRecord.Flags))

	// Verify the uploaded footer also has the correct LAC
	require.NotEmpty(t, capturedFooterData, "footer data should have been captured")
	parsedFooter, parseErr := codec.ParseFooterFromBytes(capturedFooterData)
	require.NoError(t, parseErr)
	assert.Equal(t, originalLAC, parsedFooter.LAC, "uploaded compacted footer must preserve original LAC")
}

// TestMinioFileWriter_Compact_MetricsAccuracy verifies that stored-bytes and stored-objects
// gauges are correctly adjusted during compaction: new merged blocks are counted, original
// blocks are decremented after deletion, and the footer overwrite adjusts the byte delta.
// Regression test: previously footer bytes were never tracked, causing gauge drift.
func TestMinioFileWriter_Compact_MetricsAccuracy(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	block0Data := w.serialize(0, []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("hello")},
		{EntryId: 1, Data: []byte("world")},
	})
	block1Data := w.serialize(1, []*cache.BufferEntry{
		{EntryId: 2, Data: []byte("foo")},
	})

	w.footerRecord = &codec.FooterRecord{
		TotalBlocks:  2,
		TotalRecords: 3,
		TotalSize:    uint64(len(block0Data) + len(block1Data)),
		Version:      codec.FormatVersion,
		Flags:        0,
		LAC:          2,
	}
	w.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: uint32(len(block0Data)), FirstEntryID: 0, LastEntryID: 1},
		{BlockNumber: 1, StartOffset: 1, BlockSize: uint32(len(block1Data)), FirstEntryID: 2, LastEntryID: 2},
	}
	w.firstEntryID.Store(0)
	w.lastEntryID.Store(2)

	// Capture old footer size before Compact overwrites it
	oldFooterSize := float64(w.footerRecord.IndexLength) + float64(codec.RecordHeaderSize) + float64(codec.GetFooterRecordSize(w.footerRecord.Version))

	// Read gauge values before compaction
	getGauge := func(gv *prometheus.GaugeVec) float64 {
		g, _ := gv.GetMetricWithLabelValues(metrics.NodeID, w.nsStr, w.logIdStr)
		m := &dto.Metric{}
		_ = g.Write(m)
		return m.GetGauge().GetValue()
	}
	bytesBefore := getGauge(metrics.WpObjectStorageStoredBytes)
	objectsBefore := getGauge(metrics.WpObjectStorageStoredObjects)

	// Mock: read block 0
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(block0Data)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(len(block0Data)), mock.Anything, mock.Anything).
		Return(&writerMockFileReader{data: block0Data}, nil).Once()

	// Mock: read block 1
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(len(block1Data)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", int64(0), int64(len(block1Data)), mock.Anything, mock.Anything).
		Return(&writerMockFileReader{data: block1Data}, nil).Once()

	// Mock: upload merged block 0
	var mergedBlockSize int64
	mockClient.EXPECT().PutObject(mock.Anything, "test-bucket", "test-base/1/0/m_0.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, _ string, _ io.Reader, size int64, _ string, _ string) {
			mergedBlockSize = size
		}).
		Return(nil).Once()

	// Mock: upload compacted footer
	var footerSize int64
	mockClient.EXPECT().PutObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, _ string, _ io.Reader, size int64, _ string, _ string) {
			footerSize = size
		}).
		Return(nil).Once()

	// Mock: delete original blocks
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(nil).Once()
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	_, err := w.Compact(ctx)
	assert.NoError(t, err)

	bytesAfter := getGauge(metrics.WpObjectStorageStoredBytes)
	objectsAfter := getGauge(metrics.WpObjectStorageStoredObjects)

	bytesDelta := bytesAfter - bytesBefore
	objectsDelta := objectsAfter - objectsBefore

	// Expected delta:
	//   +mergedBlockSize (new merged block uploaded)
	//   +(footerSize - oldFooterSize) (footer overwrite adjusts delta)
	//   -(block0 + block1 sizes) (original blocks deleted)
	originalBlockBytes := float64(len(block0Data) + len(block1Data))

	expectedBytesDelta := float64(mergedBlockSize) + (float64(footerSize) - oldFooterSize) - originalBlockBytes
	assert.InDelta(t, expectedBytesDelta, bytesDelta, 1.0, "stored bytes gauge should reflect net compaction change")

	// Objects delta: +1 (merged block) -2 (original blocks), footer unchanged (overwrite)
	assert.Equal(t, float64(-1), objectsDelta, "stored objects should decrease by 1 (2 blocks merged into 1, footer stays)")
}

// ===== Fence with mock =====

func TestMinioFileWriter_Fence_ConditionWriteSuccess(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.lastEntryID.Store(5)

	// PutFencedObject succeeds
	mockClient.EXPECT().PutFencedObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	lastEntry, err := w.Fence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), lastEntry)
	assert.True(t, w.fenced.Load())
}

func TestMinioFileWriter_Fence_WithExistingBlocks(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.lastEntryID.Store(19)
	w.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 9},
		{BlockNumber: 1, FirstEntryID: 10, LastEntryID: 19},
	}

	// Fence at block 2
	mockClient.EXPECT().PutFencedObject(mock.Anything, "test-bucket", "test-base/1/0/2.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	lastEntry, err := w.Fence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(19), lastEntry)
	assert.True(t, w.fenced.Load())
}

// ===== waitIfSegmentLockedWhenConditionWriteDisabled tests =====

func TestMinioFileWriter_WaitIfSegmentLocked_NotLocked(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	notFoundErr := werr.ErrEntryNotFound.WithCauseErrMsg("not found")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	err := w.waitIfSegmentLockedWhenConditionWriteDisabled(ctx)
	assert.NoError(t, err)
}

func TestMinioFileWriter_WaitIfSegmentLocked_CheckError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	statErr := errors.New("network error")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(int64(0), false, statErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(statErr).Return(false).Once()

	err := w.waitIfSegmentLockedWhenConditionWriteDisabled(ctx)
	assert.Error(t, err)
}

func TestMinioFileWriter_WaitIfSegmentLocked_IsLocked_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// Segment is locked
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(int64(10), false, nil).Once()

	// Cancel context immediately to avoid 30s wait
	cancel()

	err := w.waitIfSegmentLockedWhenConditionWriteDisabled(ctx)
	assert.Error(t, err)
}

// ===== NewMinioFileWriter / NewMinioFileWriterWithMode tests =====

func TestNewMinioFileWriter_NormalMode_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	cfg := newTestConfig()

	// Mock createSegmentLock → PutObjectIfNoneMatch for write.lock
	mockClient.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	writer, err := NewMinioFileWriter(ctx, "test-bucket", "test-base", 1, 0, mockClient, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Verify state
	w := writer.(*MinioFileWriter)
	assert.Equal(t, int64(1), w.logId)
	assert.Equal(t, int64(0), w.segmentId)
	assert.Equal(t, "test-base/1/0", w.segmentFileKey)
	assert.False(t, w.closed.Load())
	assert.True(t, w.storageWritable.Load())
	assert.NotNil(t, w.buffer.Load())

	// Clean up goroutines
	w.closed.Store(true)
	w.fileClose <- struct{}{}
	close(w.flushingTaskList)
	time.Sleep(50 * time.Millisecond) // let goroutines exit
	w.pool.Release()
}

func TestNewMinioFileWriter_NormalMode_LockFails(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	cfg := newTestConfig()

	// Mock createSegmentLock failure
	mockClient.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("lock creation failed")).Once()

	writer, err := NewMinioFileWriter(ctx, "test-bucket", "test-base", 1, 0, mockClient, cfg)
	assert.Error(t, err)
	assert.Nil(t, writer)
}

func TestNewMinioFileWriterWithMode_RecoveryMode_NoExistingData(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	cfg := newTestConfig()

	// Footer not found → fall through to recoverFromFullListing
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	// recoverFromFullListing: block 0 not found
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	writer, err := NewMinioFileWriterWithMode(ctx, "test-bucket", "test-base", 1, 0, mockClient, cfg, true)
	require.NoError(t, err)
	require.NotNil(t, writer)

	w := writer.(*MinioFileWriter)
	// No data recovered → lastEntryID stays -1, buffer starts from 0
	assert.Equal(t, int64(-1), w.lastEntryID.Load())

	// Clean up
	w.closed.Store(true)
	w.fileClose <- struct{}{}
	close(w.flushingTaskList)
	time.Sleep(50 * time.Millisecond)
	w.pool.Release()
}

func TestNewMinioFileWriterWithMode_RecoveryMode_WithExistingData(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	cfg := newTestConfig()

	// Footer not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	// recoverFromFullListing: block 0 exists
	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("hello"), []byte("world")})
	readSize0 := int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)
	headerData0 := blockData[:readSize0]

	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), readSize0, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: headerData0}, nil).Once()

	// Block 1 not found
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	writer, err := NewMinioFileWriterWithMode(ctx, "test-bucket", "test-base", 1, 0, mockClient, cfg, true)
	require.NoError(t, err)
	require.NotNil(t, writer)

	w := writer.(*MinioFileWriter)
	// Recovered data: lastEntryID should be 1 (entries 0 and 1)
	assert.Equal(t, int64(1), w.lastEntryID.Load())
	assert.Equal(t, int64(0), w.firstEntryID.Load())

	// Buffer should start from lastEntryID + 1 = 2
	buf := w.buffer.Load()
	assert.Equal(t, int64(2), buf.FirstEntryId)

	// Clean up
	w.closed.Store(true)
	w.fileClose <- struct{}{}
	close(w.flushingTaskList)
	time.Sleep(50 * time.Millisecond)
	w.pool.Release()
}

func TestNewMinioFileWriterWithMode_RecoveryMode_RecoverError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	cfg := newTestConfig()

	// Footer stat returns error (not "not found")
	statErr := errors.New("stat failed")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, statErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(statErr).Return(false).Once()

	// recoverFromFooter: GetObject fails (stat succeeded but returned error was not "not found")
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", int64(0), int64(0), mock.Anything, mock.Anything).
		Return(nil, errors.New("get failed")).Once()

	writer, err := NewMinioFileWriterWithMode(ctx, "test-bucket", "test-base", 1, 0, mockClient, cfg, true)
	assert.Error(t, err)
	assert.Nil(t, writer)
}

// ===== parseBlockIdFromBlockKey tests =====

func TestParseBlockIdFromBlockKey_NormalBlock(t *testing.T) {
	id, isMerge, err := parseBlockIdFromBlockKey("test-base/1/0/5.blk")
	assert.NoError(t, err)
	assert.False(t, isMerge)
	assert.Equal(t, int64(5), id)
}

func TestParseBlockIdFromBlockKey_MergedBlock(t *testing.T) {
	id, isMerge, err := parseBlockIdFromBlockKey("test-base/1/0/m_3.blk")
	assert.NoError(t, err)
	assert.True(t, isMerge)
	assert.Equal(t, int64(3), id)
}

func TestParseBlockIdFromBlockKey_InvalidBlock(t *testing.T) {
	_, _, err := parseBlockIdFromBlockKey("test-base/1/0/footer.blk")
	assert.Error(t, err)
}

func TestParseBlockIdFromBlockKey_InvalidMergedBlock(t *testing.T) {
	_, _, err := parseBlockIdFromBlockKey("test-base/1/0/m_abc.blk")
	assert.Error(t, err)
}

// ===== parseBlockHeaderRecord additional tests =====

func TestMinioFileWriter_ParseBlockHeaderRecord_Block0(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("hello")})
	readSize := int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)
	headerData := blockData[:readSize]

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: headerData}, nil).Once()

	bhr, err := w.parseBlockHeaderRecord(ctx, 0, "test-base/1/0/0.blk")
	assert.NoError(t, err)
	assert.NotNil(t, bhr)
	assert.Equal(t, int32(0), bhr.BlockNumber)
	assert.True(t, w.headerWritten.Load()) // HeaderRecord found → headerWritten set
}

func TestMinioFileWriter_ParseBlockHeaderRecord_Block1(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	blockData := buildValidBlockData(t, 1, 10, [][]byte{[]byte("data10")})
	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)
	headerData := blockData[:readSize]

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: headerData}, nil).Once()

	bhr, err := w.parseBlockHeaderRecord(ctx, 1, "test-base/1/0/1.blk")
	assert.NoError(t, err)
	assert.NotNil(t, bhr)
	assert.Equal(t, int32(1), bhr.BlockNumber)
}

func TestMinioFileWriter_ParseBlockHeaderRecord_GetError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(nil, errors.New("get error")).Once()

	bhr, err := w.parseBlockHeaderRecord(ctx, 1, "test-base/1/0/1.blk")
	assert.Error(t, err)
	assert.Nil(t, bhr)
}

func TestMinioFileWriter_ParseBlockHeaderRecord_EmptyData(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: []byte{}}, nil).Once()

	bhr, err := w.parseBlockHeaderRecord(ctx, 1, "test-base/1/0/1.blk")
	assert.Error(t, err)
	assert.Nil(t, bhr)
	assert.Contains(t, err.Error(), "empty data")
}

func TestMinioFileWriter_ParseBlockHeaderRecord_NoBlockHeader(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// Build data with only a HeaderRecord (no BlockHeaderRecord)
	header := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	headerBytes := codec.EncodeRecord(header)

	readSize := int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)
	// Pad to exact readSize (will fail decode for the second record but still has the first)
	data := make([]byte, readSize)
	copy(data, headerBytes)

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: data}, nil).Once()

	bhr, err := w.parseBlockHeaderRecord(ctx, 0, "test-base/1/0/0.blk")
	assert.Error(t, err)
	assert.Nil(t, bhr)
}

// ===== awaitAllFlushTasks tests =====

func TestMinioFileWriter_AwaitAllFlushTasks_NoRunningTasks(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	// Start ack goroutine to consume the termination signal
	go func() {
		for task := range w.flushingTaskList {
			if task.flushData == nil {
				w.allUploadingTaskDone.Store(true)
				return
			}
		}
	}()

	err := w.awaitAllFlushTasks(ctx)
	assert.NoError(t, err)
	assert.True(t, w.allUploadingTaskDone.Load())
}

func TestMinioFileWriter_AwaitAllFlushTasks_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	w := newTestMinioFileWriter()

	// Cancel immediately - no ack goroutine running, so pool check passes (0 running)
	// but sending termination signal will block, then ctx cancelled
	cancel()

	// Block flushingTaskList so termination signal can't be sent
	// Fill the channel first
	for i := 0; i < cap(w.flushingTaskList); i++ {
		w.flushingTaskList <- &blockUploadTask{
			flushData: []*cache.BufferEntry{},
		}
	}

	err := w.awaitAllFlushTasks(ctx)
	assert.Error(t, err)

	// Drain the channel
	for len(w.flushingTaskList) > 0 {
		<-w.flushingTaskList
	}
}

// ===== Fence tests =====

func TestMinioFileWriter_Fence_Success_NoExistingBlocks(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// PutFencedObject for block 0 (no existing blocks, so fence at block 0)
	mockClient.EXPECT().PutFencedObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	lastEntry, err := w.Fence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), lastEntry) // no data written
	assert.True(t, w.fenced.Load())
}

func TestMinioFileWriter_Fence_Success_WithExistingBlocks(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.lastEntryID.Store(9)
	w.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 9},
	}

	// PutFencedObject for block 1 (after existing block 0)
	mockClient.EXPECT().PutFencedObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	lastEntry, err := w.Fence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(9), lastEntry)
	assert.True(t, w.fenced.Load())
}

func TestMinioFileWriter_Fence_PutError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// PutFencedObject fails with non-retryable error
	mockClient.EXPECT().PutFencedObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(errors.New("put failed")).Once()

	lastEntry, err := w.Fence(ctx)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lastEntry)
	assert.False(t, w.fenced.Load())
}

func TestMinioFileWriter_Fence_WithConditionWriteDisabled_NotLocked(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.fencePolicyConfig = &config.FencePolicyConfig{ConditionWrite: "disable"}

	// isSegmentLocked: lock not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	// PutFencedObject succeeds
	mockClient.EXPECT().PutFencedObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	lastEntry, err := w.Fence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), lastEntry)
	assert.True(t, w.fenced.Load())
}

// ===== cleanupOriginalFilesIfCompacted tests =====

func TestMinioFileWriter_CleanupOriginalFilesIfCompacted_NoOriginalFiles(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// WalkWithObjects returns no original block files
	mockClient.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", "test-base/1/0/", false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	err := w.cleanupOriginalFilesIfCompacted(ctx)
	assert.NoError(t, err)
}

func TestMinioFileWriter_CleanupOriginalFilesIfCompacted_WithOriginalFiles(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// WalkWithObjects returns original block files
	mockClient.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", "test-base/1/0/", false, mock.Anything, mock.Anything, mock.Anything).
		Run(func(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc commonObjectStorage.ChunkObjectWalkFunc, ns string, logId string) {
			walkFunc(&commonObjectStorage.ChunkObjectInfo{FilePath: "test-base/1/0/0.blk"})
			walkFunc(&commonObjectStorage.ChunkObjectInfo{FilePath: "test-base/1/0/1.blk"})
		}).
		Return(nil).Once()

	// deleteOriginalBlocksByKeys deletes each file
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(nil).Once()
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	err := w.cleanupOriginalFilesIfCompacted(ctx)
	assert.NoError(t, err)
}

func TestMinioFileWriter_CleanupOriginalFilesIfCompacted_ListError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	mockClient.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", "test-base/1/0/", false, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("list error")).Once()

	err := w.cleanupOriginalFilesIfCompacted(ctx)
	assert.Error(t, err)
}

// ===== extractDataRecords additional tests =====

func TestMinioFileWriter_ExtractDataRecords_TruncatedData(t *testing.T) {
	w := newTestMinioFileWriter()
	// Truncated data (less than record header size) returns error
	result, err := w.extractDataRecords([]byte{0x01, 0x02})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "truncated record header at offset 0")
	assert.Nil(t, result)
}

func TestMinioFileWriter_ExtractDataRecords_EmptyInput(t *testing.T) {
	w := newTestMinioFileWriter()
	result, err := w.extractDataRecords([]byte{})
	assert.NoError(t, err)
	assert.Empty(t, result)
}

// ===== Close additional tests =====

func TestMinioFileWriter_Close_AlreadyClosed(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.closed.Store(true)

	err := w.Close(ctx)
	assert.NoError(t, err)
}

// ===== rollBufferUnsafe / fastFlushSuccessUnsafe edge case =====

func TestMinioFileWriter_FastFlushSuccessUnsafe_NotifyEntries(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	ch := newMockResultChannel()

	blockInfo := &BlockInfo{
		FirstEntryID: 0,
		LastEntryID:  0,
		BlockKey:     "test-base/1/0/0.blk",
		BlockID:      0,
		Size:         100,
	}
	entries := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("hello"), NotifyChan: ch},
	}

	w.fastFlushSuccessUnsafe(ctx, blockInfo, entries)

	// Verify notification was sent
	assert.GreaterOrEqual(t, len(ch.results), 1)
}

// ===== WriteDataAsync - buffer full triggers Sync =====

func TestMinioFileWriter_WriteDataAsync_BufferFullTriggerSync(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	// Set maxBufferEntries to 3 so entries 0,1 don't trigger Sync but entry 2 does
	w.maxBufferEntries = 3

	// Create a new buffer with MaxEntries=3
	newBuffer := cache.NewSequentialBuffer(1, 0, 0, 3, w.nsStr)
	w.buffer.Store(newBuffer)

	ch := newMockResultChannel()

	// Write three entries to fill the buffer (none trigger Sync yet because
	// pendingAppendId = ExpectedNextEntryId+1 is checked before each write)
	_, err := w.WriteDataAsync(ctx, 0, []byte("entry0"), ch)
	require.NoError(t, err)
	_, err = w.WriteDataAsync(ctx, 1, []byte("entry1"), ch)
	require.NoError(t, err)

	// After writing entry 1, ExpectedNextEntryId=2, so for entry 2:
	// pendingAppendId = 2+1=3 >= 0+3=3 → Sync triggered
	// Set storageWritable to false so Sync returns quickly via quickSyncFailUnsafe
	w.storageWritable.Store(false)

	// Writing entry 2 should trigger Sync, which will fail
	_, err = w.WriteDataAsync(ctx, 2, []byte("entry2"), ch)
	assert.Error(t, err)
	assert.True(t, werr.ErrStorageNotWritable.Is(err))
}

func TestMinioFileWriter_WriteDataAsync_BufferFullSyncError(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()
	w.maxBufferEntries = 3

	newBuffer := cache.NewSequentialBuffer(1, 0, 0, 3, w.nsStr)
	w.buffer.Store(newBuffer)

	ch := newMockResultChannel()

	// Fill buffer with 2 entries (no Sync triggered)
	_, err := w.WriteDataAsync(ctx, 0, []byte("a"), ch)
	require.NoError(t, err)
	_, err = w.WriteDataAsync(ctx, 1, []byte("b"), ch)
	require.NoError(t, err)

	// Make sync fail before writing entry 2
	w.storageWritable.Store(false)

	_, err = w.WriteDataAsync(ctx, 2, []byte("c"), ch)
	assert.Error(t, err)
	// Verify the error comes from Sync path
	assert.True(t, werr.ErrStorageNotWritable.Is(err))
}

// ===== rollBufferUnsafe - empty buffer =====

func TestMinioFileWriter_RollBufferUnsafe_EmptyBuffer(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	// buffer is empty by default (no entries written)
	w.mu.Lock()
	buf, toFlush, firstId, err := w.rollBufferUnsafe(ctx)
	w.mu.Unlock()

	assert.NoError(t, err)
	assert.NotNil(t, buf)
	assert.Empty(t, toFlush)
	assert.Equal(t, int64(-1), firstId)
}

// ===== rollBufferUnsafe - ExpectedNextEntryId == FirstEntryId =====

func TestMinioFileWriter_RollBufferUnsafe_ExpectedNextEqFirst(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	// Write an entry with a gap: put entry at position 1 (but FirstEntryId=0)
	// so that ExpectedNextEntryId stays at 0 (no contiguous entries from start)
	buf := w.buffer.Load()
	// Manually insert an entry at position 1 (skipping 0)
	buf.Entries[1] = &cache.BufferEntry{EntryId: 1, Data: []byte("data")}

	w.mu.Lock()
	resultBuf, toFlush, firstId, err := w.rollBufferUnsafe(ctx)
	w.mu.Unlock()

	assert.NoError(t, err)
	assert.NotNil(t, resultBuf)
	assert.Empty(t, toFlush)
	assert.Equal(t, int64(-1), firstId)
}

// ===== waitIfFlushingBufferSizeExceeded - context cancelled returns context.Canceled =====

func TestMinioFileWriter_WaitIfFlushingBufferSizeExceeded_ContextCancelledReturnsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	w := newTestMinioFileWriter()

	// Set flushing buffer size to exceed max
	w.flushingBufferSize.Store(w.maxBufferSize + 1)

	// Cancel context immediately
	cancel()

	err := w.waitIfFlushingBufferSizeExceededUnsafe(ctx)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// ===== waitIfFlushingBufferSizeExceeded - fileClose signal =====

func TestMinioFileWriter_WaitIfFlushingBufferSizeExceeded_FileCloseSignal(t *testing.T) {
	w := newTestMinioFileWriter()
	ctx := context.Background()

	// Set flushing buffer size to exceed max
	w.flushingBufferSize.Store(w.maxBufferSize + 1)

	// Send fileClose signal
	w.fileClose <- struct{}{}

	err := w.waitIfFlushingBufferSizeExceededUnsafe(ctx)
	assert.Error(t, err)
	assert.True(t, werr.ErrFileWriterAlreadyClosed.Is(err))
}

// ===== Finalize - Sync error =====

func TestMinioFileWriter_Finalize_SyncError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.allUploadingTaskDone.Store(true) // No pending upload tasks

	// Make Sync fail by setting storageWritable=false
	w.storageWritable.Store(false)

	// The Sync error is logged but Finalize continues.
	// awaitAllFlushTasks succeeds (allUploadingTaskDone=true).
	// PutObjectIfNoneMatch for footer is called.
	mockClient.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	lastId, err := w.Finalize(ctx, -1)
	assert.NoError(t, err)
	assert.True(t, w.finalized.Load())
	_ = lastId
}

// ===== Finalize - PutObjectIfNoneMatch fails =====

func TestMinioFileWriter_Finalize_PutFooterFails(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.allUploadingTaskDone.Store(true)

	// Make Sync succeed (no data to flush, empty buffer)
	// PutObjectIfNoneMatch fails with non-ErrObjectAlreadyExists error
	mockClient.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("storage error")).Once()

	lastId, err := w.Finalize(ctx, -1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to put object")
	assert.Equal(t, int64(-1), lastId)
	assert.False(t, w.finalized.Load())
}

// ===== Close - Sync error before close =====

func TestMinioFileWriter_Close_SyncError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.allUploadingTaskDone.Store(true)

	// Make Sync fail
	w.storageWritable.Store(false)

	// releaseSegmentLock: lockObjectKey is empty so it returns nil immediately
	// Close should still succeed (Sync error is only logged)

	err := w.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, w.closed.Load())
}

// ===== Close - releaseSegmentLock fails =====

func TestMinioFileWriter_Close_ReleaseSegmentLockFails(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.allUploadingTaskDone.Store(true)
	w.lockObjectKey = "test-base/1/0/write.lock"

	// RemoveObject fails
	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(errors.New("remove failed")).Once()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(false).Once()

	// Close still succeeds (lock release error is logged but not fatal)
	err := w.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, w.closed.Load())
}

// ===== fastFlushSuccessUnsafe - cumulative offset with multiple blockIndexes =====

func TestMinioFileWriter_FastFlushSuccessUnsafe_CumulativeOffset(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	// Pre-populate with 2 existing block indexes
	w.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 4},
		{BlockNumber: 1, StartOffset: 1, BlockSize: 200, FirstEntryID: 5, LastEntryID: 9},
	}

	ch := newMockResultChannel()
	blockInfo := &BlockInfo{
		FirstEntryID: 10,
		LastEntryID:  14,
		BlockKey:     "test-base/1/0/2.blk",
		BlockID:      2,
		Size:         150,
	}
	entries := []*cache.BufferEntry{
		{EntryId: 10, Data: []byte("a"), NotifyChan: ch},
		{EntryId: 11, Data: []byte("b"), NotifyChan: ch},
	}

	w.fastFlushSuccessUnsafe(ctx, blockInfo, entries)

	// Should have 3 block indexes now
	assert.Equal(t, 3, len(w.blockIndexes))
	assert.Equal(t, int32(2), w.blockIndexes[2].BlockNumber)
	assert.Equal(t, uint32(150), w.blockIndexes[2].BlockSize)
	// Notifications sent
	assert.GreaterOrEqual(t, len(ch.results), 2)
}

// ===== Compact - already compacted =====

func TestMinioFileWriter_Compact_AlreadyCompacted_WithCleanup(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.footerRecord = &codec.FooterRecord{
		TotalBlocks: 2,
		TotalSize:   1000,
		Flags:       codec.SetCompacted(0),
	}

	// WalkWithObjects finds original files to clean up
	mockClient.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", "test-base/1/0/", false, mock.Anything, mock.Anything, mock.Anything).
		Run(func(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc commonObjectStorage.ChunkObjectWalkFunc, ns string, logId string) {
			walkFunc(&commonObjectStorage.ChunkObjectInfo{FilePath: "test-base/1/0/0.blk"})
		}).
		Return(nil).Once()

	mockClient.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	size, err := w.Compact(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), size)
}

// ===== Compact - targetBlockSize <= 0 defaults to 2MB =====

func TestMinioFileWriter_Compact_TargetBlockSizeZero(t *testing.T) {
	ctx := context.Background()
	w := newTestMinioFileWriter()

	// Set MaxBytes to 0
	w.compactPolicyConfig = &config.SegmentCompactionPolicy{
		MaxBytes:           config.NewByteSize(0),
		MaxParallelUploads: 4,
		MaxParallelReads:   4,
	}

	// footerRecord must be non-nil, non-compacted
	w.footerRecord = &codec.FooterRecord{
		TotalBlocks:  0,
		TotalRecords: 0,
		TotalSize:    0,
		Version:      codec.FormatVersion,
		Flags:        0,
	}
	w.blockIndexes = []*codec.IndexRecord{} // No blocks → streamMergeAndUploadBlocks returns empty

	size, err := w.Compact(ctx)
	// No blocks to compact → returns -1, nil
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), size)
}

// ===== Compact - PutObject footer fails =====

func TestMinioFileWriter_Compact_PutFooterFails(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// Create valid block data
	block0Data := w.serialize(0, []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("hello")},
	})

	w.footerRecord = &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 1,
		TotalSize:    uint64(len(block0Data)),
		Version:      codec.FormatVersion,
		Flags:        0,
	}
	w.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: uint32(len(block0Data)), FirstEntryID: 0, LastEntryID: 0},
	}
	w.firstEntryID.Store(0)
	w.lastEntryID.Store(0)

	// readBlockData for block 0
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(block0Data)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(len(block0Data)), mock.Anything, mock.Anything).
		Return(&writerMockFileReader{data: block0Data}, nil).Once()

	// uploadSingleMergedBlock succeeds
	mockClient.EXPECT().PutObject(mock.Anything, "test-bucket", "test-base/1/0/m_0.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	// PutObject for footer fails
	mockClient.EXPECT().PutObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("footer upload failed")).Once()

	size, err := w.Compact(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to upload compacted footer")
	assert.Equal(t, int64(-1), size)
}

// ===== Fence - conditionWriteDisabled path =====

func TestMinioFileWriter_Fence_ConditionWriteDisabled(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.fencePolicyConfig = &config.FencePolicyConfig{ConditionWrite: "disable"}
	w.lastEntryID.Store(5)

	// isSegmentLocked: segment is not locked
	notFoundErr := werr.ErrEntryNotFound.WithCauseErrMsg("not found")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/write.lock", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	// PutFencedObject succeeds
	mockClient.EXPECT().PutFencedObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	lastEntry, err := w.Fence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), lastEntry)
	assert.True(t, w.fenced.Load())
}

// ===== Fence - retry with recovery =====

func TestMinioFileWriter_Fence_RetryWithRecovery(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient
	w.lastEntryID.Store(-1)

	// Serialize block 0 as real data so recovery can parse it
	block0Data := w.serialize(0, []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("data0")},
	})
	block0Size := int64(len(block0Data))

	// readSize for parseBlockHeaderRecord on block 0:
	// (RecordHeaderSize+HeaderRecordSize) + (RecordHeaderSize+BlockHeaderRecordSize)
	block0ReadSize := int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)

	// First attempt: PutFencedObject returns ErrObjectAlreadyExists for block 0
	mockClient.EXPECT().PutFencedObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(werr.ErrObjectAlreadyExists).Once()

	// On retry, recoverFromStorageUnsafe is called:
	// 1. StatObject for footer.blk returns not found → goes to recoverFromFullListing
	notFoundErr := werr.ErrEntryNotFound.WithCauseErrMsg("not found")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	// 2. recoverFromFullListing: StatObject for block 0 → exists (data block)
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(block0Size, false, nil).Once()

	// 3. parseBlockHeaderRecord: GetObject for block 0 header
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), block0ReadSize, mock.Anything, mock.Anything).
		Return(&writerMockFileReader{data: block0Data[:block0ReadSize]}, nil).Once()

	// 4. StatObject for block 1 returns not found → end of listing
	notFoundErr2 := werr.ErrEntryNotFound.WithCauseErrMsg("not found")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr2).Once()

	// 5. After recovery, blockIndexes has block 0, so fence at block 1
	mockClient.EXPECT().PutFencedObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(nil).Once()

	lastEntry, err := w.Fence(ctx)
	assert.NoError(t, err)
	assert.True(t, w.fenced.Load())
	_ = lastEntry
}

// ===== recoverFromFooter - ReadObjectFull error =====

func TestMinioFileWriter_RecoverFromFooter_ReadError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	footerBlockKey := "test-base/1/0/footer.blk"
	footerBlockSize := int64(100)

	// GetObject returns a reader that will produce an error
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerBlockKey, int64(0), footerBlockSize, mock.Anything, mock.Anything).
		Return(nil, errors.New("get object failed")).Once()

	err := w.recoverFromFooter(ctx, footerBlockKey, footerBlockSize)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get object failed")
}

// ===== recoverFromFooter - footer too small =====

func TestMinioFileWriter_RecoverFromFooter_FooterTooSmall(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	footerBlockKey := "test-base/1/0/footer.blk"
	// Create data that's too small for a valid footer
	smallData := []byte{0x01, 0x02, 0x03}
	footerBlockSize := int64(len(smallData))

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerBlockKey, int64(0), footerBlockSize, mock.Anything, mock.Anything).
		Return(&writerMockFileReader{data: smallData}, nil).Once()

	err := w.recoverFromFooter(ctx, footerBlockKey, footerBlockSize)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "footer.blk too small")
}

// ===== recoverFromFooter - ParseFooterFromBytes error =====

func TestMinioFileWriter_RecoverFromFooter_ParseFooterError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	footerBlockKey := "test-base/1/0/footer.blk"
	// Create data that is large enough to pass the size check but has invalid footer content
	minFooterSize := codec.RecordHeaderSize + codec.FooterRecordSizeV5
	invalidData := make([]byte, minFooterSize+10) // garbage data
	for i := range invalidData {
		invalidData[i] = 0xFF
	}
	footerBlockSize := int64(len(invalidData))

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerBlockKey, int64(0), footerBlockSize, mock.Anything, mock.Anything).
		Return(&writerMockFileReader{data: invalidData}, nil).Once()

	err := w.recoverFromFooter(ctx, footerBlockKey, footerBlockSize)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse footer record")
}

func TestMinioFileWriter_Close_ErrorCleansUpResources(t *testing.T) {
	w := newTestMinioFileWriter()

	// awaitAllFlushTasks will try to send a termination signal to flushingTaskList;
	// use a cancelled context so it fails immediately on ctx.Done()
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	closeErr := w.Close(cancelledCtx)
	assert.Error(t, closeErr, "Close should return error when awaitAllFlushTasks fails")

	// Verify cleanup happened despite the error return:
	// - closed flag should be set
	assert.True(t, w.closed.Load(), "closed flag should be set")
	// - pool should be released (calling Release again is safe but pool.Free() should be 0)
	assert.NotNil(t, w.pool, "pool should still exist but be released")
}

func TestMinioFileWriter_Close_Idempotent_NoDoubleClose(t *testing.T) {
	w := newTestMinioFileWriter()

	// Mark as already done so awaitAllFlushTasks returns immediately
	w.allUploadingTaskDone.Store(true)

	ctx := context.Background()

	// First close succeeds
	err := w.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, w.closed.Load())

	// Second close returns nil without panic (idempotent)
	err = w.Close(ctx)
	assert.NoError(t, err)
}

func TestMinioFileWriter_FlushingBufferSize_AccountingAccuracy(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	// Mock PutObjectIfNoneMatch to succeed for all block uploads
	mockClient.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	// Start the ack goroutine (normally started by run(), but we need it for the test)
	go w.ack()

	// Verify initial state
	assert.Equal(t, int64(0), w.flushingBufferSize.Load(), "initial flushingBufferSize should be 0")

	// Write multiple entries with known sizes
	resultCh := newMockResultChannel()
	for i := int64(0); i < 10; i++ {
		data := make([]byte, 100+i*10) // varying sizes: 100, 110, 120, ..., 190
		for j := range data {
			data[j] = byte(i)
		}
		_, err := w.WriteDataAsync(ctx, i, data, resultCh)
		require.NoError(t, err)
	}

	// Trigger sync to submit flush tasks (this increments flushingBufferSize)
	err := w.Sync(ctx)
	require.NoError(t, err)

	// Wait for all pool tasks to complete and ack goroutine to process them
	require.Eventually(t, func() bool {
		return w.pool.Running() == 0
	}, 5*time.Second, 50*time.Millisecond, "pool tasks should complete")

	// Send termination signal to ack goroutine and wait for it to finish
	w.flushingTaskList <- &blockUploadTask{
		flushData:             nil,
		flushDataFirstEntryId: 0,
		flushFuture:           nil,
	}
	require.Eventually(t, func() bool {
		return w.allUploadingTaskDone.Load()
	}, 5*time.Second, 50*time.Millisecond, "ack goroutine should finish")

	// The key assertion: flushingBufferSize must return to exactly 0
	// Before the fix, this would be negative due to serialization overhead mismatch
	finalSize := w.flushingBufferSize.Load()
	assert.Equal(t, int64(0), finalSize,
		"flushingBufferSize should be exactly 0 after all tasks are acked, got %d", finalSize)
}
