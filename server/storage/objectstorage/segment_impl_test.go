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
	"fmt"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage/legacy"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_minio"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

// TestNewSegmentImpl tests the NewSegmentImpl function.
func TestNewSegmentImpl(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*SegmentImpl)
	assert.Equal(t, int64(1), segmentImpl.logId)
	assert.Equal(t, int64(0), segmentImpl.segmentId)
	assert.Equal(t, int64(0), segmentImpl.GetId())
	assert.Equal(t, "test-segment/1/0", segmentImpl.segmentPrefixKey)
	assert.Equal(t, "test-bucket", segmentImpl.bucket)
	assert.Equal(t, int64(1000), segmentImpl.buffer.Load().MaxEntries)
	assert.Equal(t, int64(1024*1024), segmentImpl.maxBufferSize)
	assert.Equal(t, 1000, segmentImpl.maxIntervalMs)
}

// TestNewROSegmentImpl tests the NewROSegmentImpl function.
func TestNewROSegmentImpl(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	client := mocks_minio.NewMinioHandler(t)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	listEmptyChan := make(chan minio.ObjectInfo)
	close(listEmptyChan)
	client.EXPECT().ListObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(listEmptyChan)
	segmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)

	assert.Equal(t, int64(1), segmentImpl.logId)
	assert.Equal(t, int64(0), segmentImpl.segmentId)
	assert.Equal(t, int64(0), segmentImpl.GetId())
	assert.Equal(t, "test-segment/1/0", segmentImpl.segmentPrefixKey)
	assert.Equal(t, "test-bucket", segmentImpl.bucket)
	assert.Empty(t, segmentImpl.fragments)
}

// TestAppendAsyncReachBufferSize tests the AppendAsync function when the buffer size is reached.
func TestAppendAsyncReachBufferSize(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	//client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      10,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncReachBufferSize/1/0", "test-bucket", client, cfg).(*SegmentImpl)

	// Test write entries
	incomeEntryIds := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	chList := make([]channel.ResultChannel, 0)
	for _, entryId := range incomeEntryIds {
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryId))
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), rc)
		assert.NoError(t, err)
		chList = append(chList, rc)
	}

	for idx, rc := range chList {
		subCtx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr)
		assert.Nil(t, result.Err)
		assert.True(t, result.SyncedId >= 0, fmt.Sprintf("syncedId should be positive %d:%d", idx, result.SyncedId))
	}

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(9), flushedLastId)
}

func TestAppendAsyncSomeAndWaitForFlush(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      10,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncSomeAndWaitForFlush/1/0", "test-bucket", client, cfg).(*SegmentImpl)

	// Test write entries
	incomeEntryIds := []int64{0, 1, 2, 3, 4, 5, 6}
	chList := make([]channel.ResultChannel, 0)
	for _, entryId := range incomeEntryIds {
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryId))
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), rc)
		assert.NoError(t, err)
		chList = append(chList, rc)
	}

	for idx, rc := range chList {
		subCtx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr)
		assert.Nil(t, result.Err)
		assert.True(t, result.SyncedId >= 0, fmt.Sprintf("syncedId should be positive %d:%d", idx, result.SyncedId))
	}

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(6), flushedLastId)
}

func TestAppendAsyncOnceAndWaitForFlush(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      10,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncOnceAndWaitForFlush/1/0", "test-bucket", client, cfg).(*SegmentImpl)

	// Test write entries
	incomeEntryIds := []int64{0}
	chList := make([]channel.ResultChannel, 0)
	for _, entryId := range incomeEntryIds {
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryId))
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), rc)
		assert.NoError(t, err)
		chList = append(chList, rc)
	}

	for idx, rc := range chList {
		subCtx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr)
		assert.Nil(t, result.Err)
		assert.True(t, result.SyncedId >= 0, fmt.Sprintf("syncedId should be positive %d:%d", idx, result.SyncedId))
	}

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), flushedLastId)
}

func TestAppendAsyncNoneAndWaitForFlush(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      10,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncNoneAndWaitForFlush/1/0", "test-bucket", client, cfg).(*SegmentImpl)

	// wait for flush interval
	<-time.After(2 * time.Second)

	// check there is no data
	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(-1), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), flushedLastId)
}

func TestAppendAsyncWithHolesAndWaitForFlush(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      10,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncWithHolesAndWaitForFlush/1/0", "test-bucket", client, cfg).(*SegmentImpl)

	// Test out of order
	incomeEntryIds := []int64{1, 3, 4, 8, 9}
	chList := make([]channel.ResultChannel, 0)
	for _, entryId := range incomeEntryIds {
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryId))
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), rc)
		assert.NoError(t, err)
		chList = append(chList, rc)
	}

	timeoutErrs := 0
	for _, rc := range chList {
		subCtx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		fmt.Println(result)
		if readErr != nil {
			if errors.IsAny(readErr, context.Canceled, context.DeadlineExceeded) {
				timeoutErrs++
			}
		}
	}
	assert.Equal(t, len(incomeEntryIds), timeoutErrs)

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(-1), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), flushedLastId)
}

func TestAppendAsyncWithHolesButFillFinally(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      10,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncWithHolesButFillFinally/1/0", "test-bucket", client, cfg).(*SegmentImpl)

	// Test out of order
	incomeEntryIds := []int64{1, 3, 4, 8, 9}
	chList := make([]channel.ResultChannel, 0)
	for _, entryId := range incomeEntryIds {
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryId))
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), rc)
		assert.NoError(t, err)
		chList = append(chList, rc)
	}

	timeoutErrs := 0
	for _, rc := range chList {
		subCtx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		_, readErr := rc.ReadResult(subCtx)
		cancel()
		if readErr != nil {
			if errors.IsAny(readErr, context.Canceled, context.DeadlineExceeded) {
				timeoutErrs++
			}
		}
	}
	assert.Equal(t, len(incomeEntryIds), timeoutErrs)

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(-1), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), flushedLastId)

	// fill finally
	newIncomeEntryIds := []int64{0, 2, 5, 6, 7} // rest of the entries
	for _, entryId := range newIncomeEntryIds {
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryId))
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), rc)
		assert.NoError(t, err)
		chList = append(chList, rc)
	}
	assert.Equal(t, 10, len(chList))

	for idx, rc := range chList {
		subCtx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr)
		assert.Nil(t, result.Err)
		assert.True(t, result.SyncedId >= 0, fmt.Sprintf("syncedId should be positive %d:%d", idx, result.SyncedId))
	}

	flushedFirstId = segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err = segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(9), flushedLastId)
}

// TestAppendAsyncDisorderWithinBounds test appends entries out of order, but within bounds.
func TestAppendAsyncDisorderWithinBounds(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      10,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncDisorderWithinBounds/1/0", "test-bucket", client, cfg).(*SegmentImpl)

	// Test out of order
	incomeEntryIds := []int64{1, 0, 6, 8, 9, 7, 2, 3, 4, 5}
	chList := make([]channel.ResultChannel, 0)
	for _, entryId := range incomeEntryIds {
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryId))
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), rc)
		assert.NoError(t, err)
		chList = append(chList, rc)
	}

	for idx, rc := range chList {
		subCtx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr)
		assert.Nil(t, result.Err)
		assert.True(t, result.SyncedId >= 0, fmt.Sprintf("syncedId should be positive %d:%d", idx, result.SyncedId))
	}

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(9), flushedLastId)
}

func TestAppendAsyncDisorderAndPartialOutOfBounds(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      10,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncDisorderAndPartialOutOfBounds/1/0", "test-bucket", client, cfg).(*SegmentImpl)

	// Test out of order
	incomeEntryIds := []int64{1, 0, 6, 11, 12, 10, 7, 2, 3, 4, 5} // 0-7,10-12
	chList := make([]channel.ResultChannel, 0)
	for _, entryId := range incomeEntryIds {
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryId))
		assignId, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), rc)
		if entryId >= 10 {
			// 10-12 should async write buffer fail
			assert.Equal(t, int64(-1), assignId)
			assert.Error(t, err)
			assert.True(t, werr.ErrWriteBufferFull.Is(err))
		} else {
			// 0-7 should async write buffer success
			assert.Equal(t, entryId, assignId)
			assert.NoError(t, err)
			chList = append(chList, rc)
		}
	}

	for _, rc := range chList {
		subCtx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr)
		assert.Nil(t, result.Err)
		assert.True(t, result.SyncedId >= 0, fmt.Sprintf("syncedId should be in [0,7], but it is: %d", result.SyncedId))
		assert.True(t, result.SyncedId <= 7, fmt.Sprintf("syncedId should be in [0,7], but it is: %d", result.SyncedId))
	}

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(7), flushedLastId)
}

func TestAppendAsyncReachBufferDataSize(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        1000,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1000, // Set to 1000 to trigger sync when buffer reaches 1000 bytes
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	data800 := make([]byte, 800)
	data200 := make([]byte, 200)
	data300 := make([]byte, 300)
	data1k := make([]byte, 1000)

	// test async append 800 + 200 = buffer max data size, sync immediately
	{
		segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncReachBufferDataSize1/1/0", "test-bucket", client, cfg).(*SegmentImpl)
		rc0 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
		assignId0, err := segmentImpl.AppendAsync(context.Background(), 0, data800, rc0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		rc1 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 1))
		assignId1, err := segmentImpl.AppendAsync(context.Background(), 1, data200, rc1)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		// reach the max size, should be immediate flush
		// show check response immediately
		start := time.Now()
		result0, readErr0 := rc0.ReadResult(context.Background())
		assert.NoError(t, readErr0)
		assert.NotNil(t, result0)
		assert.Equal(t, int64(0), result0.SyncedId)
		result1, readErr1 := rc1.ReadResult(context.Background())
		assert.NoError(t, readErr1)
		assert.NotNil(t, result1)
		assert.Equal(t, int64(1), result1.SyncedId)
		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost < 100, fmt.Sprintf("should be immediate flush, but cost %d ms", cost))
		assert.Equal(t, int64(0), segmentImpl.getFirstEntryId())
		flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(1), flushedLastId)
	}

	// test async append 200 + 300, wait for flush
	{
		segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncReachBufferDataSize2/1/0", "test-bucket", client, cfg).(*SegmentImpl)
		rc0 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
		assignId0, err := segmentImpl.AppendAsync(context.Background(), 0, data200, rc0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		rc1 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 1))
		assignId1, err := segmentImpl.AppendAsync(context.Background(), 1, data300, rc1)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		// wait for flush
		start := time.Now()
		result0, readErr0 := rc0.ReadResult(context.Background())
		assert.NoError(t, readErr0)
		assert.NotNil(t, result0)
		assert.Equal(t, int64(0), result0.SyncedId)
		result1, readErr1 := rc1.ReadResult(context.Background())
		assert.NoError(t, readErr1)
		assert.NotNil(t, result1)
		assert.Equal(t, int64(1), result1.SyncedId)

		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost >= 1000, fmt.Sprintf("should wait 1000 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), segmentImpl.getFirstEntryId())
		flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(1), flushedLastId)
	}

	// test async append 200 + 300, wait for flush
	{
		segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncReachBufferDataSize3/1/0", "test-bucket", client, cfg).(*SegmentImpl)
		rc0 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
		assignId0, err := segmentImpl.AppendAsync(context.Background(), 0, data200, rc0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		rc1 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 1))
		assignId1, err := segmentImpl.AppendAsync(context.Background(), 1, data300, rc1)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)

		// wait for flush
		start := time.Now()
		result0, readErr0 := rc0.ReadResult(context.Background())
		assert.NoError(t, readErr0)
		assert.NotNil(t, result0)
		assert.Equal(t, int64(0), result0.SyncedId)
		result1, readErr1 := rc1.ReadResult(context.Background())
		assert.NoError(t, readErr1)
		assert.NotNil(t, result1)
		assert.Equal(t, int64(1), result1.SyncedId)

		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost >= 1000, fmt.Sprintf("should wait 1000 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), segmentImpl.getFirstEntryId())
		flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(1), flushedLastId)
	}

	// test async append 1k + 200,
	// the first append 1k should be immediately flush,
	// the second append 200 should be wait for flush.
	{
		segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncReachBufferDataSize4/1/0", "test-bucket", client, cfg).(*SegmentImpl)
		rc0 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
		assignId0, err := segmentImpl.AppendAsync(context.Background(), 0, data1k, rc0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		// flush immediately
		start := time.Now()
		result0, readErr0 := rc0.ReadResult(context.Background())
		assert.NoError(t, readErr0)
		assert.NotNil(t, result0)
		assert.Equal(t, int64(0), result0.SyncedId)

		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost < 100, fmt.Sprintf("should wait less then 100 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), segmentImpl.getFirstEntryId())
		flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(0), flushedLastId)

		rc1 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 1))
		assignId1, err := segmentImpl.AppendAsync(context.Background(), 1, data200, rc1)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		// wait for flush
		start = time.Now()
		result1, readErr1 := rc1.ReadResult(context.Background())
		assert.NoError(t, readErr1)
		assert.NotNil(t, result1)
		assert.Equal(t, int64(1), result1.SyncedId)

		cost = time.Now().Sub(start).Milliseconds()
		assert.True(t, cost >= 1000, fmt.Sprintf("should wait 1000 ms to auto flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), segmentImpl.getFirstEntryId())
		flushedLastId, err = segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(1), flushedLastId)
	}

	// test append 1(800),3(300), trigger flush 1(800). then append 2(200), wait for flush  2(200) + 3(300)
	{
		segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestAppendAsyncReachBufferDataSize5/1/0", "test-bucket", client, cfg).(*SegmentImpl)
		rc0 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
		assignId0, err := segmentImpl.AppendAsync(context.Background(), 0, data800, rc0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		rc2 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 2))
		assignId2, err := segmentImpl.AppendAsync(context.Background(), 2, data300, rc2)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), assignId2)
		// flush 1(800) immediately
		start := time.Now()
		result0, readErr0 := rc0.ReadResult(context.Background())
		assert.NoError(t, readErr0)
		assert.NotNil(t, result0)
		assert.Equal(t, int64(0), result0.SyncedId)

		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost < 100, fmt.Sprintf("should wait less then 100 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), segmentImpl.getFirstEntryId())
		flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(0), flushedLastId)

		rc1 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 1))
		assignId1, err := segmentImpl.AppendAsync(context.Background(), 1, data200, rc1)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)

		// wait for flush
		start = time.Now()
		result1, readErr1 := rc1.ReadResult(context.Background())
		assert.NoError(t, readErr1)
		assert.NotNil(t, result1)
		assert.Equal(t, int64(1), result1.SyncedId)
		result2, readErr2 := rc2.ReadResult(context.Background())
		assert.NoError(t, readErr2)
		assert.NotNil(t, result2)
		assert.Equal(t, int64(2), result2.SyncedId)

		cost = time.Now().Sub(start).Milliseconds()
		assert.True(t, cost >= 1000*0.8, fmt.Sprintf("should wait 1000 ms to auto flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), segmentImpl.getFirstEntryId())
		flushedLastId, err = segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(2), flushedLastId)
	}
}

// TestSync tests the Sync function.
func TestSync(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestSync/1/0", "test-bucket", client, cfg).(*SegmentImpl)

	// Write some data to buffer
	chList := make([]channel.ResultChannel, 0)
	for i := 0; i < 100; i++ {
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", i))
		assignId, err := segmentImpl.AppendAsync(context.Background(), int64(i), []byte("test_data"), rc)
		assert.Equal(t, int64(i), assignId)
		assert.NoError(t, err)
		chList = append(chList, rc)
	}

	err := segmentImpl.Sync(context.Background())
	assert.NoError(t, err)

	for _, rc := range chList {
		subCtx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr)
		assert.Nil(t, result.Err)
		assert.True(t, result.SyncedId >= 0, fmt.Sprintf("syncedId should be in [0,100), but it is: %d", result.SyncedId))
		assert.True(t, result.SyncedId < 100, fmt.Sprintf("syncedId should be in [0,100), but it is: %d", result.SyncedId))
	}

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(99), flushedLastId)

}

// TestClose tests the Close function.
func TestClose(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(nil)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestClose/1/0", "test-bucket", client, cfg).(*SegmentImpl)

	// Write some data to buffer
	chList := make([]channel.ResultChannel, 0)
	for i := 0; i < 100; i++ {
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", i))
		_, err := segmentImpl.AppendAsync(context.Background(), int64(i), []byte("test_data"), rc)
		assert.NoError(t, err)
		chList = append(chList, rc)
	}
	assert.Equal(t, 100, len(chList))

	// Close log file
	err := segmentImpl.Close(context.TODO())
	assert.NoError(t, err)

	// final flush immediately
	start := time.Now()
	successCount := 0
	for _, rc := range chList {
		subCtx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		if readErr != nil {
			continue
		}
		if result != nil && result.SyncedId >= 0 {
			successCount++
		}
	}
	cost := time.Now().Sub(start).Milliseconds()
	assert.True(t, cost < 1000, fmt.Sprintf("should flush immediately, but cost %d ms", cost))
	assert.Equal(t, 100, successCount)
}

// TestGetId tests the GetId function.
func TestGetId(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "TestGetId/1/0", "test-bucket", client, cfg).(*SegmentImpl)
	assert.Equal(t, int64(1), segmentImpl.logId)
	assert.Equal(t, int64(0), segmentImpl.segmentId)
	assert.Equal(t, int64(0), segmentImpl.GetId())
}

// TestMerge tests the Merge function.
func TestMerge(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentCompactionPolicy: config.SegmentCompactionPolicy{
					MaxBytes: 1024 * 1024,
				},
			},
		},
	}

	// mock fragments
	mockFragment1 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestMerge/1/0/1.frag",
		[]*cache.BufferEntry{
			{EntryId: 100, Data: []byte("entry1"), NotifyChan: nil},
			{EntryId: 101, Data: []byte("entry2"), NotifyChan: nil},
		}, 100, true, false, true)
	mockFragment2 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestMerge/1/0/2.frag",
		[]*cache.BufferEntry{
			{EntryId: 102, Data: []byte("entry3"), NotifyChan: nil},
			{EntryId: 103, Data: []byte("entry4"), NotifyChan: nil},
		}, 102, true, false, true)

	// mock object storage interfaces
	listChan := make(chan minio.ObjectInfo)
	close(listChan)
	client.EXPECT().ListObjects(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(listChan)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)

	roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "TestMerge/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
	roSegmentImpl.fragments = []storage.Fragment{mockFragment1, mockFragment2} // set test fragments
	mergedFrags, entryOffset, fragmentIdOffset, err := roSegmentImpl.Merge(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mergedFrags))
	assert.Equal(t, []int32{100}, entryOffset) // 100 is the first entry id of merged fragment
	assert.Equal(t, []int32{1}, fragmentIdOffset)
	firstEntryId, err := mergedFrags[0].GetFirstEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(100), firstEntryId)
	lastEntryId, err := mergedFrags[0].GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(103), lastEntryId)
}

// TestLoad tests the Load function.
func TestLoad(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	// mock fragments
	mockFragment1 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestMerge/1/0/1.frag",
		[]*cache.BufferEntry{
			{EntryId: 100, Data: []byte("entry1"), NotifyChan: nil},
			{EntryId: 101, Data: []byte("entry2"), NotifyChan: nil},
		}, 100, true, false, true)
	mockFragment2 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestMerge/1/0/2.frag",
		[]*cache.BufferEntry{
			{EntryId: 102, Data: []byte("entry3"), NotifyChan: nil},
			{EntryId: 103, Data: []byte("entry4"), NotifyChan: nil},
		}, 102, true, false, true)

	// mock object storage interfaces
	listChan := make(chan minio.ObjectInfo)
	close(listChan)
	client.EXPECT().ListObjects(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(listChan)
	// Load data
	roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "TestLoad/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
	roSegmentImpl.fragments = []storage.Fragment{mockFragment1, mockFragment2} // set test mock fragments
	totalSize, lastFragment, err := roSegmentImpl.Load(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, mockFragment1.GetSize()+mockFragment2.GetSize(), totalSize)
	assert.NotNil(t, lastFragment)
	firstEntryId, err := lastFragment.GetFirstEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(102), firstEntryId)
	lastEntryId, err := lastFragment.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(103), lastEntryId)
}

// TestNewReader tests the NewReader function.
func TestNewReaderInSegmentImpl(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	// mock fragments
	mockFragment1 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestNewReaderInWriterLogFile/1/0/1.frag",
		[]*cache.BufferEntry{
			{EntryId: 0, Data: []byte("entry1"), NotifyChan: nil},
			{EntryId: 1, Data: []byte("entry2"), NotifyChan: nil},
		}, 0, true, false, true)
	mockFragment2 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestNewReaderInWriterLogFile/1/0/2.frag",
		[]*cache.BufferEntry{
			{EntryId: 2, Data: []byte("entry3"), NotifyChan: nil},
			{EntryId: 3, Data: []byte("entry4"), NotifyChan: nil},
		}, 2, true, false, true)

	// mock object storage interfaces
	listChan := make(chan minio.ObjectInfo)
	close(listChan)
	client.EXPECT().ListObjects(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(listChan)
	// Add StatObject mock for fragment discovery
	client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{
		Code: "NoSuchKey",
	}).Maybe()

	// Create a reader for [0, 100)
	roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "TestNewReaderInWriterLogFile/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
	roSegmentImpl.fragments = []storage.Fragment{mockFragment1, mockFragment2} // set test mock fragments
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   100,
	})
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	// Read data
	for i := 0; i < 4; i++ {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		assert.True(t, hasNext)

		entry, err := reader.ReadNext(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(i), entry.EntryId)
		assert.Equal(t, []byte(fmt.Sprintf("entry%d", i+1)), entry.Values)
	}

	hasNext, err := reader.HasNext(context.TODO())
	assert.NoError(t, err)
	assert.False(t, hasNext)
}

func TestNewReaderInROSegmentImpl(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentCompactionPolicy: config.SegmentCompactionPolicy{
					MaxBytes: 1024 * 1024,
				},
			},
		},
	}

	// Create BufferEntry data instead of [][]byte
	data := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("entry1"), NotifyChan: nil},
		{EntryId: 1, Data: []byte("entry2"), NotifyChan: nil},
		{EntryId: 2, Data: []byte("entry3"), NotifyChan: nil},
		{EntryId: 3, Data: []byte("entry4"), NotifyChan: nil},
	}
	mockFragment1 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestNewReaderInROSegmentImpl/1/0/1.frag",
		data, 0, true, false, true)

	// mock object storage interfaces
	listChan := make(chan minio.ObjectInfo)
	close(listChan)
	client.EXPECT().ListObjects(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(listChan)
	// Add StatObject mock for fragment discovery
	client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{
		Code: "NoSuchKey",
	}).Maybe()

	// Create a reader for [0, 100)
	roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "TestNewReaderInROLogFile/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
	roSegmentImpl.fragments = []storage.Fragment{mockFragment1} // set test mock fragments
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   100,
	})
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	// Read data
	for i := 0; i < 4; i++ {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		assert.True(t, hasNext)

		entry, err := reader.ReadNext(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(i), entry.EntryId)
		assert.Equal(t, []byte(fmt.Sprintf("entry%d", i+1)), entry.Values)
	}

	hasNext, err := reader.HasNext(context.TODO())
	assert.NoError(t, err)
	assert.False(t, hasNext)
}

func TestROLogFileReadDataWithHoles(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentCompactionPolicy: config.SegmentCompactionPolicy{
					MaxBytes: 1024 * 1024,
				},
			},
		},
	}

	// test read data with holes (fragment 1,x,3) in minio
	mockFragment1 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestROLogFileReadDataWithHoles/1/0/0.frag",
		[]*cache.BufferEntry{
			{EntryId: 100, Data: []byte("entry1"), NotifyChan: nil},
			{EntryId: 101, Data: []byte("entry2"), NotifyChan: nil},
		}, 100, true, false, true)
	mockFragment2 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestROLogFileReadDataWithHoles/1/0/1.frag",
		[]*cache.BufferEntry{
			{EntryId: 102, Data: []byte("entry3"), NotifyChan: nil},
			{EntryId: 103, Data: []byte("entry4"), NotifyChan: nil},
		}, 102, true, false, true)
	//mockFragment3 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 3, "TestROLogFileReadDataWithHoles/1/0/2.frag",
	//	[][]byte{[]byte("entry4"), []byte("entry5")}, 104, true, false, true)
	mockFragment4 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 4, "TestROLogFileReadDataWithHoles/1/0/3.frag",
		[]*cache.BufferEntry{
			{EntryId: 106, Data: []byte("entry6"), NotifyChan: nil},
			{EntryId: 107, Data: []byte("entry7"), NotifyChan: nil},
		}, 106, true, false, true)

	mockMergedFragment0 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestROLogFileReadDataWithHoles/1/0/m_0.frag",
		[]*cache.BufferEntry{
			{EntryId: 100, Data: []byte("entry1"), NotifyChan: nil},
			{EntryId: 101, Data: []byte("entry2"), NotifyChan: nil},
		}, 100, true, false, true)
	mockMergedFragment1 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestROLogFileReadDataWithHoles/1/0/m_1.frag",
		[]*cache.BufferEntry{
			{EntryId: 102, Data: []byte("entry3"), NotifyChan: nil},
			{EntryId: 103, Data: []byte("entry4"), NotifyChan: nil},
		}, 102, true, false, true)
	//mockMergedFragment2 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 3, "TestROLogFileReadDataWithHoles/1/0/m_2.frag",
	//	[][]byte{[]byte("entry4"), []byte("entry5")}, 104, true, false, true)
	mockMergedFragment3 := legacy.NewLegacyFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 4, "TestROLogFileReadDataWithHoles/1/0/m_3.frag",
		[]*cache.BufferEntry{
			{EntryId: 106, Data: []byte("entry6"), NotifyChan: nil},
			{EntryId: 107, Data: []byte("entry7"), NotifyChan: nil},
		}, 106, true, false, true)

	listChan := make(chan minio.ObjectInfo, 6) // list return disorder and hole list
	listChan <- minio.ObjectInfo{
		Key:  mockFragment2.GetFragmentKey(),
		Size: mockFragment2.GetSize(),
	}
	listChan <- minio.ObjectInfo{
		Key:  mockMergedFragment0.GetFragmentKey(),
		Size: mockMergedFragment0.GetSize(),
	}
	listChan <- minio.ObjectInfo{
		Key:  mockFragment4.GetFragmentKey(),
		Size: mockFragment4.GetSize(),
	}
	listChan <- minio.ObjectInfo{
		Key:  mockMergedFragment3.GetFragmentKey(),
		Size: mockMergedFragment3.GetSize(),
	}
	listChan <- minio.ObjectInfo{
		Key:  mockMergedFragment1.GetFragmentKey(),
		Size: mockMergedFragment1.GetSize(),
	}
	listChan <- minio.ObjectInfo{
		Key:  mockFragment1.GetFragmentKey(),
		Size: mockFragment1.GetSize(),
	}
	close(listChan)
	client.EXPECT().ListObjects(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(listChan)

	// we should only read data in fragment 1
	roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "TestROLogFileReadDataWithHoles/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
	assert.NotNil(t, roSegmentImpl.fragments)
	assert.Equal(t, 2, len(roSegmentImpl.fragments))
	assert.Equal(t, mockFragment1.GetFragmentKey(), roSegmentImpl.fragments[0].GetFragmentKey())
	assert.Equal(t, mockFragment2.GetFragmentKey(), roSegmentImpl.fragments[1].GetFragmentKey())
	assert.Equal(t, 2, len(roSegmentImpl.mergedFragments))
	assert.Equal(t, mockMergedFragment0.GetFragmentKey(), roSegmentImpl.mergedFragments[0].GetFragmentKey())
	assert.Equal(t, mockMergedFragment1.GetFragmentKey(), roSegmentImpl.mergedFragments[1].GetFragmentKey())
}

// TestDeleteFragments tests the DeleteFragments function.
func TestDeleteFragments(t *testing.T) {
	t.Run("SuccessfulDeletion", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
				},
			},
		}

		// Create a list of mock objects to be returned by ListObjects
		objectCh := make(chan minio.ObjectInfo, 3)
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/0.frag", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/1.frag", Size: 2048}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/m_0.frag", Size: 4096}
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh).Once()

		// Create the LogFile
		roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
		assert.Equal(t, 2, len(roSegmentImpl.fragments))
		assert.Equal(t, 1, len(roSegmentImpl.mergedFragments))

		objectCh2 := make(chan minio.ObjectInfo, 3)
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/0.frag", Size: 1024}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/1.frag", Size: 2048}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/m_0.frag", Size: 4096}
		close(objectCh2)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh2).Once()
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/0.frag", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/1.frag", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/m_0.frag", mock.Anything).Return(nil)
		// Call DeleteFragments
		err := roSegmentImpl.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err)

		// Verify internal state is reset
		assert.Equal(t, 0, len(roSegmentImpl.fragments), "Fragments slice should be empty")
		assert.Equal(t, 0, len(roSegmentImpl.mergedFragments), "MergedFragments slice should be empty")
	})

	t.Run("ListObjectsError", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					SegmentCompactionPolicy: config.SegmentCompactionPolicy{
						MaxBytes: 4 * 1024 * 1024,
					},
				},
			},
		}

		// Create an object channel with an error
		emptyObjectCh := make(chan minio.ObjectInfo, 0)
		close(emptyObjectCh)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(emptyObjectCh).Once()

		// Create the LogFile
		roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)

		errorObjectCh := make(chan minio.ObjectInfo, 1)
		errorObjectCh <- minio.ObjectInfo{Err: errors.New("list error")}
		close(errorObjectCh)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(errorObjectCh).Once()

		// Call DeleteFragments
		err := roSegmentImpl.DeleteFragments(context.Background(), 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to delete")
	})

	t.Run("RemoveObjectError", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					SegmentCompactionPolicy: config.SegmentCompactionPolicy{
						MaxBytes: 4 * 1024 * 1024,
					},
				},
			},
		}

		// Create a list of mock objects to be returned by ListObjects
		objectCh := make(chan minio.ObjectInfo, 2)
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/0.frag", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/1.frag", Size: 2048}
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh).Once()
		// Create the LogFile
		roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)

		objectCh2 := make(chan minio.ObjectInfo, 2)
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/0.frag", Size: 1024}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/1.frag", Size: 2048}
		close(objectCh2)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh2).Once()
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/0.frag", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/1.frag", mock.Anything).Return(errors.New("remove error"))

		// Call DeleteFragments
		err := roSegmentImpl.DeleteFragments(context.Background(), 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to delete")
	})

	t.Run("NoFragmentsToDelete", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					SegmentCompactionPolicy: config.SegmentCompactionPolicy{
						MaxBytes: 4 * 1024 * 1024,
					},
				},
			},
		}

		// Empty object channel
		objectCh := make(chan minio.ObjectInfo)
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh)
		// No need for StatObject call anymore

		// Create the LogFile
		roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)

		// Call DeleteFragments
		err := roSegmentImpl.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err)

		// Verify internal state is reset
		assert.Empty(t, roSegmentImpl.fragments, "Fragments slice should be empty")
	})

	t.Run("SkipNonFragmentFiles", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					SegmentCompactionPolicy: config.SegmentCompactionPolicy{
						MaxBytes: 4 * 1024 * 1024,
					},
				},
			},
		}

		// Create a list of mock objects including non-fragment files
		objectCh := make(chan minio.ObjectInfo, 3)
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/0.frag", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/metadata.json", Size: 256} // Not a fragment
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/m_0.frag", Size: 4096}
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh).Once()
		// No call for metadata.json as it should be skipped

		// Create the readonly segment impl
		roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)

		objectCh2 := make(chan minio.ObjectInfo, 3)
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/0.frag", Size: 1024}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/metadata.json", Size: 256} // Not a fragment
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/m_0.frag", Size: 4096}
		close(objectCh2)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh2).Once()
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/0.frag", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/m_0.frag", mock.Anything).Return(nil)
		//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
		// Call DeleteFragments
		err := roSegmentImpl.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err)
	})
}

func TestPrepareMultiFragmentDataIfNecessary_EmptyData(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 1024,
		},
		fileClose: make(chan struct{}, 1),
	}

	toFlushData := []*cache.BufferEntry{}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return empty partitions
	assert.Equal(t, 0, len(partitions))
	assert.Equal(t, 0, len(partitionFirstEntryIds))
}

func TestPrepareMultiFragmentDataIfNecessary_SingleSmallEntry(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 1024,
		},
		fileClose: make(chan struct{}, 1),
	}

	toFlushData := []*cache.BufferEntry{
		{EntryId: 5, Data: []byte("small"), NotifyChan: nil},
	}
	toFlushDataFirstEntryId := int64(5)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return single partition with one entry
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 1, len(partitions[0]))
	assert.Equal(t, int64(5), partitionFirstEntryIds[0])
	assert.Equal(t, []byte("small"), partitions[0][0].Data)
}

func TestPrepareMultiFragmentDataIfNecessary_MultipleSmallEntries(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 1024,
		},
		fileClose: make(chan struct{}, 1),
	}

	toFlushData := []*cache.BufferEntry{
		{EntryId: 10, Data: []byte("entry1"), NotifyChan: nil},
		{EntryId: 11, Data: []byte("entry2"), NotifyChan: nil},
		{EntryId: 12, Data: []byte("entry3"), NotifyChan: nil},
	}
	toFlushDataFirstEntryId := int64(10)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return single partition with all entries
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 3, len(partitions[0]))
	assert.Equal(t, int64(10), partitionFirstEntryIds[0])
	assert.Equal(t, []byte("entry1"), partitions[0][0].Data)
	assert.Equal(t, []byte("entry2"), partitions[0][1].Data)
	assert.Equal(t, []byte("entry3"), partitions[0][2].Data)
}

func TestPrepareMultiFragmentDataIfNecessary_EntriesExceedMaxSize(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 10, // Very small max size to force partitioning
		},
		fileClose: make(chan struct{}, 1),
	}

	toFlushData := []*cache.BufferEntry{
		{EntryId: 0, Data: make([]byte, 8), NotifyChan: nil}, // 8 bytes
		{EntryId: 1, Data: make([]byte, 5), NotifyChan: nil}, // 5 bytes, total 13 > 10
		{EntryId: 2, Data: make([]byte, 3), NotifyChan: nil}, // 3 bytes
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return two partitions
	assert.Equal(t, 2, len(partitions))
	assert.Equal(t, 2, len(partitionFirstEntryIds))

	// First partition should have first entry only
	assert.Equal(t, 2, len(partitions[0]))
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	// Second partition should have remaining entries
	assert.Equal(t, 1, len(partitions[1]))
	assert.Equal(t, int64(2), partitionFirstEntryIds[1])
}

func TestPrepareMultiFragmentDataIfNecessary_SingleLargeEntry(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 10, // Smaller than the entry
		},
		fileClose: make(chan struct{}, 1),
	}

	toFlushData := []*cache.BufferEntry{
		{EntryId: 100, Data: make([]byte, 20), NotifyChan: nil}, // 20 bytes > 10
	}
	toFlushDataFirstEntryId := int64(100)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should still return single partition (large entries are not split)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 1, len(partitions[0]))
	assert.Equal(t, int64(100), partitionFirstEntryIds[0])
	assert.Equal(t, 20, len(partitions[0][0].Data))
}

func TestPrepareMultiFragmentDataIfNecessary_MultiplePartitions(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 100,
		},
		fileClose: make(chan struct{}, 1),
	}

	toFlushData := []*cache.BufferEntry{
		{EntryId: 50, Data: make([]byte, 60), NotifyChan: nil}, // 60 bytes
		{EntryId: 51, Data: make([]byte, 50), NotifyChan: nil}, // 50 bytes, total 110 > 100
		{EntryId: 52, Data: make([]byte, 30), NotifyChan: nil}, // 30 bytes
		{EntryId: 53, Data: make([]byte, 40), NotifyChan: nil}, // 40 bytes
		{EntryId: 54, Data: make([]byte, 80), NotifyChan: nil}, // 80 bytes, total 150 > 100
	}
	toFlushDataFirstEntryId := int64(50)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return four partitions
	assert.Equal(t, 2, len(partitions))
	assert.Equal(t, 2, len(partitionFirstEntryIds))

	// First partition: 110 bytes
	assert.Equal(t, 2, len(partitions[0]))
	assert.Equal(t, int64(50), partitionFirstEntryIds[0])

	// Second partition: 30 + 40 + 80 = 150 bytes
	assert.Equal(t, 3, len(partitions[1]))
	assert.Equal(t, int64(52), partitionFirstEntryIds[1])
}

func TestPrepareMultiFragmentDataIfNecessary_ZeroMaxFlushSize(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 0, // Edge case
		},
		fileClose: make(chan struct{}, 1),
	}

	toFlushData := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("a"), NotifyChan: nil},
		{EntryId: 1, Data: []byte("b"), NotifyChan: nil},
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Each entry should be in its own partition
	assert.Equal(t, 2, len(partitions))
	assert.Equal(t, 2, len(partitionFirstEntryIds))
	assert.Equal(t, 1, len(partitions[0]))
	assert.Equal(t, 1, len(partitions[1]))
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])
	assert.Equal(t, int64(1), partitionFirstEntryIds[1])
}

func TestPrepareMultiFragmentDataIfNecessary_VaryingSizes(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 100,
		},
		fileClose: make(chan struct{}, 1),
	}

	toFlushData := []*cache.BufferEntry{
		{EntryId: 1000, Data: make([]byte, 30), NotifyChan: nil}, // 30 bytes
		{EntryId: 1001, Data: make([]byte, 40), NotifyChan: nil}, // 40 bytes, total 70
		{EntryId: 1002, Data: make([]byte, 50), NotifyChan: nil}, // 50 bytes, total 120 > 100

		{EntryId: 1003, Data: make([]byte, 20), NotifyChan: nil}, // 20 bytes
		{EntryId: 1004, Data: make([]byte, 25), NotifyChan: nil}, // 25 bytes, total 45
		{EntryId: 1005, Data: make([]byte, 10), NotifyChan: nil}, // 10 bytes, total 15 < 100, but still needs to be in a separate partition for these rest entries
	}
	toFlushDataFirstEntryId := int64(1000)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return three partitions
	assert.Equal(t, 2, len(partitions))
	assert.Equal(t, 2, len(partitionFirstEntryIds))

	// First partition: 30 + 40 + 50 = 120 bytes
	assert.Equal(t, 3, len(partitions[0]))
	assert.Equal(t, 30, len(partitions[0][0].Data))
	assert.Equal(t, 40, len(partitions[0][1].Data))
	assert.Equal(t, 50, len(partitions[0][2].Data))
	assert.Equal(t, int64(1000), partitionFirstEntryIds[0])

	// Second partition:  20 + 25 + 10 = 55 bytes
	assert.Equal(t, 3, len(partitions[1]))
	assert.Equal(t, 20, len(partitions[1][0].Data))
	assert.Equal(t, 25, len(partitions[1][1].Data))
	assert.Equal(t, 10, len(partitions[1][2].Data))
	assert.Equal(t, int64(1003), partitionFirstEntryIds[1])
}

func TestPrepareMultiFragmentDataIfNecessary_EntryIdCalculation(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 2, // Very small size to force multiple partitions
		},
		fileClose: make(chan struct{}, 1),
	}

	// Test entry ID calculation with different starting entry ID
	toFlushData := []*cache.BufferEntry{
		{EntryId: 42, Data: []byte("ab"), NotifyChan: nil}, // 2 bytes (fits in first partition)
		{EntryId: 43, Data: []byte("cd"), NotifyChan: nil}, // 2 bytes (fits in second partition)
		{EntryId: 44, Data: []byte("ef"), NotifyChan: nil}, // 2 bytes (fits in third partition)
	}
	toFlushDataFirstEntryId := int64(42) // Start from entry ID 42

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should create multiple partitions due to small max flush size
	assert.Equal(t, 3, len(partitions))
	assert.Equal(t, 3, len(partitionFirstEntryIds))

	// Verify entry ID progression
	assert.Equal(t, int64(42), partitionFirstEntryIds[0]) // First partition starts at 42
	assert.Equal(t, int64(43), partitionFirstEntryIds[1]) // Second partition starts at 43
	assert.Equal(t, int64(44), partitionFirstEntryIds[2]) // Third partition starts at 44

	// Verify partition contents
	assert.Equal(t, []byte("ab"), partitions[0][0].Data)
	assert.Equal(t, []byte("cd"), partitions[1][0].Data)
	assert.Equal(t, []byte("ef"), partitions[2][0].Data)
}

func TestPrepareMultiFragmentDataIfNecessary_LargeMaxFlushSize(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 1024 * 1024, // 1MB max flush size (very large)
		},
		fileClose: make(chan struct{}, 1),
	}

	// Test with many small entries that should all fit in one partition
	toFlushData := make([]*cache.BufferEntry, 100)
	for i := 0; i < 100; i++ {
		toFlushData[i] = &cache.BufferEntry{
			EntryId:    int64(i),
			Data:       []byte("small_data_entry"), // 16 bytes each
			NotifyChan: nil,
		}
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return single partition with all entries
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 100, len(partitions[0]))
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	// Verify all entries are present
	for i := 0; i < 100; i++ {
		assert.Equal(t, []byte("small_data_entry"), partitions[0][i].Data)
	}
}

func TestPrepareMultiFragmentDataIfNecessary_ExactSizeMatch(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 10, // Exactly 10 bytes max flush size
		},
		fileClose: make(chan struct{}, 1),
	}

	// Test with entries that exactly match the max flush size
	toFlushData := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("1234567890"), NotifyChan: nil}, // Exactly 10 bytes
		{EntryId: 1, Data: []byte("abcdefghij"), NotifyChan: nil}, // Exactly 10 bytes
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return two partitions, each with one entry
	assert.Equal(t, 2, len(partitions))
	assert.Equal(t, 2, len(partitionFirstEntryIds))

	assert.Equal(t, 1, len(partitions[0]))
	assert.Equal(t, []byte("1234567890"), partitions[0][0].Data)
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	assert.Equal(t, 1, len(partitions[1]))
	assert.Equal(t, []byte("abcdefghij"), partitions[1][0].Data)
	assert.Equal(t, int64(1), partitionFirstEntryIds[1])
}

func TestPrepareMultiFragmentDataIfNecessary_NegativeEntryId(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 20,
		},
		fileClose: make(chan struct{}, 1),
	}

	// Test with negative starting entry ID
	toFlushData := []*cache.BufferEntry{
		{EntryId: -10, Data: []byte("data1"), NotifyChan: nil}, // 5 bytes
		{EntryId: -9, Data: []byte("data2"), NotifyChan: nil},  // 5 bytes
	}
	toFlushDataFirstEntryId := int64(-10) // Negative starting entry ID

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should work correctly with negative entry IDs
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 2, len(partitions[0]))
	assert.Equal(t, int64(-10), partitionFirstEntryIds[0])
}

func TestPrepareMultiFragmentDataIfNecessary_SingleByteEntries(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 3, // 3 bytes max flush size
		},
		fileClose: make(chan struct{}, 1),
	}

	// Test with single byte entries
	toFlushData := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("a"), NotifyChan: nil}, // 1 byte
		{EntryId: 1, Data: []byte("b"), NotifyChan: nil}, // 1 byte
		{EntryId: 2, Data: []byte("c"), NotifyChan: nil}, // 1 byte (total: 3 bytes, fits)
		{EntryId: 3, Data: []byte("d"), NotifyChan: nil}, // 1 byte (total: 4 bytes, exceeds, new partition)
		{EntryId: 4, Data: []byte("e"), NotifyChan: nil}, // 1 byte
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return two partitions
	assert.Equal(t, 2, len(partitions))
	assert.Equal(t, 2, len(partitionFirstEntryIds))

	// First partition: a, b, c (3 bytes)
	assert.Equal(t, 3, len(partitions[0]))
	assert.Equal(t, []byte("a"), partitions[0][0].Data)
	assert.Equal(t, []byte("b"), partitions[0][1].Data)
	assert.Equal(t, []byte("c"), partitions[0][2].Data)
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	// Second partition: d, e (2 bytes)
	assert.Equal(t, 2, len(partitions[1]))
	assert.Equal(t, []byte("d"), partitions[1][0].Data)
	assert.Equal(t, []byte("e"), partitions[1][1].Data)
	assert.Equal(t, int64(3), partitionFirstEntryIds[1])
}

func TestPrepareMultiFragmentDataIfNecessary_MaxInt64EntryId(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 100,
		},
		fileClose: make(chan struct{}, 1),
	}

	// Test with very large entry ID
	toFlushData := []*cache.BufferEntry{
		{EntryId: 9223372036854775800, Data: []byte("data1"), NotifyChan: nil},
		{EntryId: 9223372036854775801, Data: []byte("data2"), NotifyChan: nil},
	}
	toFlushDataFirstEntryId := int64(9223372036854775800) // Near max int64

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should work correctly with large entry IDs
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, int64(9223372036854775800), partitionFirstEntryIds[0])
}

func TestPrepareMultiFragmentDataIfNecessary_EmptyEntries(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 10,
		},
		fileClose: make(chan struct{}, 1),
	}

	// Test with empty byte slices
	toFlushData := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte{}, NotifyChan: nil},       // 0 bytes
		{EntryId: 1, Data: []byte("data"), NotifyChan: nil}, // 4 bytes
		{EntryId: 2, Data: []byte{}, NotifyChan: nil},       // 0 bytes
		{EntryId: 3, Data: []byte("more"), NotifyChan: nil}, // 4 bytes
		{EntryId: 4, Data: []byte{}, NotifyChan: nil},       // 0 bytes
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return single partition with all entries (total 8 bytes)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 5, len(partitions[0]))
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	// Verify all entries including empty ones
	assert.Equal(t, []byte{}, partitions[0][0].Data)
	assert.Equal(t, []byte("data"), partitions[0][1].Data)
	assert.Equal(t, []byte{}, partitions[0][2].Data)
	assert.Equal(t, []byte("more"), partitions[0][3].Data)
	assert.Equal(t, []byte{}, partitions[0][4].Data)
}

// TestWaitIfFlushingBufferSizeExceeded tests the waitIfFlushingBufferSizeExceeded method
func TestWaitIfFlushingBufferSizeExceeded(t *testing.T) {
	// Create a mock SegmentImpl with test configuration
	segment := &SegmentImpl{
		maxBufferSize: 1000,                   // Set max buffer size to 1000 bytes
		fileClose:     make(chan struct{}, 1), // Initialize the channel
	}
	segment.flushingBufferSize.Store(0)
	segment.closed.Store(false)

	ctx := context.Background()

	// Test case 1: flushingBufferSize is less than maxBufferSize, should return immediately
	t.Run("FlushingBufferSizeWithinLimit", func(t *testing.T) {
		segment.flushingBufferSize.Store(500) // Set to 500, which is less than 1000

		start := time.Now()
		segment.waitIfFlushingBufferSizeExceededUnsafe(ctx)
		duration := time.Since(start)

		// Should return immediately, so duration should be very small
		assert.Less(t, duration, 50*time.Millisecond, "Should return immediately when buffer size is within limit")
	})

	// Test case 2: flushingBufferSize exceeds maxBufferSize, should wait until it's reduced
	t.Run("FlushingBufferSizeExceedsLimit", func(t *testing.T) {
		segment.flushingBufferSize.Store(1500) // Set to 1500, which exceeds 1000

		var wg sync.WaitGroup
		wg.Add(1)

		start := time.Now()
		go func() {
			defer wg.Done()
			segment.waitIfFlushingBufferSizeExceededUnsafe(ctx)
		}()

		// Wait a bit to ensure the method is waiting
		time.Sleep(50 * time.Millisecond)

		// Reduce the flushing buffer size to below the limit
		segment.flushingBufferSize.Store(800)

		wg.Wait()
		duration := time.Since(start)

		// Should have waited for at least 50ms
		assert.GreaterOrEqual(t, duration, 50*time.Millisecond, "Should wait when buffer size exceeds limit")
		assert.Less(t, duration, 200*time.Millisecond, "Should not wait too long after buffer size is reduced")
	})

	// Test case 3: context cancellation should cause immediate return
	t.Run("ContextCancellation", func(t *testing.T) {
		segment.flushingBufferSize.Store(1500) // Set to exceed limit

		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)

		start := time.Now()
		go func() {
			defer wg.Done()
			segment.waitIfFlushingBufferSizeExceededUnsafe(ctx)
		}()

		// Wait a bit then cancel the context
		time.Sleep(30 * time.Millisecond)
		cancel()

		wg.Wait()
		duration := time.Since(start)

		// Should return quickly after context cancellation
		assert.Less(t, duration, 100*time.Millisecond, "Should return quickly when context is cancelled")
	})

	// Test case 4: segment close should cause immediate return
	t.Run("SegmentClose", func(t *testing.T) {
		segment.flushingBufferSize.Store(1500) // Set to exceed limit
		close(segment.fileClose)               // Ensure it starts as not closed

		var wg sync.WaitGroup
		wg.Add(1)

		start := time.Now()
		go func() {
			defer wg.Done()
			segment.waitIfFlushingBufferSizeExceededUnsafe(ctx)
		}()

		// Wait a bit then close the segment
		time.Sleep(30 * time.Millisecond)
		segment.closed.Store(true)

		wg.Wait()
		duration := time.Since(start)

		// Should return quickly after segment is closed
		assert.Less(t, duration, 100*time.Millisecond, "Should return quickly when segment is closed")

		// Reset for other tests
		segment.closed.Store(false)
	})
}

// TestFlushingBufferSizeManagement tests that flushingBufferSize is properly managed
func TestFlushingBufferSizeManagement(t *testing.T) {
	// Create a mock SegmentImpl
	segment := &SegmentImpl{
		maxBufferSize: 1000,
		syncPolicyConfig: &config.SegmentSyncPolicyConfig{
			MaxFlushSize: 500,
		},
		fileClose: make(chan struct{}, 1),
	}
	segment.flushingBufferSize.Store(0)

	// Test that flushingBufferSize is properly incremented and decremented
	t.Run("FlushingBufferSizeIncrement", func(t *testing.T) {
		initialSize := segment.flushingBufferSize.Load()

		// Simulate adding flush size
		segment.flushingBufferSize.Add(300)
		assert.Equal(t, initialSize+300, segment.flushingBufferSize.Load(), "Flushing buffer size should be incremented")

		// Simulate completing flush
		segment.flushingBufferSize.Add(-300)
		assert.Equal(t, initialSize, segment.flushingBufferSize.Load(), "Flushing buffer size should be decremented after flush completion")
	})
}

func TestSegmentImpl_ObjectStorageLock(t *testing.T) {
	ctx := context.Background()

	t.Run("CreateSegmentWithLock", func(t *testing.T) {
		// Create mock client
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      1000,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
				},
			},
		}

		// Expect lock creation to succeed
		client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket",
			"test-prefix/segment_1_1.lock", mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)

		// Create first segment instance
		segment1 := NewSegmentImpl(ctx, 1, 1, "test-prefix", "test-bucket", client, cfg)
		require.NotNil(t, segment1, "Should create segment successfully")

		// Verify lock object key is set
		segImpl := segment1.(*SegmentImpl)
		assert.Equal(t, "test-prefix/segment_1_1.lock", segImpl.lockObjectKey, "Lock object key should be set correctly")

		// Expect lock removal during close
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket",
			"test-prefix/segment_1_1.lock", mock.Anything).Return(nil)

		// Close segment
		err := segment1.Close(ctx)
		assert.NoError(t, err, "Should close segment successfully")
	})

	t.Run("CreateSegmentWithExistingLock", func(t *testing.T) {
		// Create mock client
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      1000,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
				},
			},
		}

		// Expect lock creation to fail with ErrFragmentAlreadyExists
		client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket",
			"test-prefix/segment_2_2.lock", mock.Anything, mock.Anything).Return(minio.UploadInfo{}, werr.ErrFragmentAlreadyExists)

		// Try to create segment (should fail due to existing lock)
		segment := NewSegmentImpl(ctx, 2, 2, "test-prefix", "test-bucket", client, cfg)
		assert.Nil(t, segment, "Should fail to create segment with existing lock")
	})

	t.Run("CreateSegmentWithLockError", func(t *testing.T) {
		// Create mock client
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      1000,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
				},
			},
		}

		// Expect lock creation to fail with other error
		expectedErr := fmt.Errorf("network error")
		client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket",
			"test-prefix/segment_3_3.lock", mock.Anything, mock.Anything).Return(minio.UploadInfo{}, expectedErr)

		// Try to create segment (should fail due to lock creation error)
		segment := NewSegmentImpl(ctx, 3, 3, "test-prefix", "test-bucket", client, cfg)
		assert.Nil(t, segment, "Should fail to create segment when lock creation fails")
	})

	t.Run("CloseSegmentWithLockRemovalError", func(t *testing.T) {
		// Create mock client
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      1000,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
				},
			},
		}

		// Expect lock creation to succeed
		client.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket",
			"test-prefix/segment_4_4.lock", mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)

		// Create segment
		segment := NewSegmentImpl(ctx, 4, 4, "test-prefix", "test-bucket", client, cfg)
		require.NotNil(t, segment, "Should create segment successfully")

		// Expect lock removal to fail during close
		expectedErr := fmt.Errorf("network error")
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket",
			"test-prefix/segment_4_4.lock", mock.Anything).Return(expectedErr)

		// Close segment (should succeed despite lock removal error)
		err := segment.Close(ctx)
		assert.NoError(t, err, "Should close segment successfully even if lock removal fails")
	})
}

func TestSegmentImpl_Fence(t *testing.T) {
	// Create test configuration
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	t.Run("SegmentImpl_Fence_Basic", func(t *testing.T) {
		ctx := context.Background()

		// Create mock minio handler
		minioHandler := mocks_minio.NewMinioHandler(t)
		bucket := "test-fence-bucket"

		// Mock lock creation for segment
		minioHandler.EXPECT().PutObjectIfNoneMatch(mock.Anything, bucket,
			"test-segment-fence/segment_1_1.lock", mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)

		// Mock fence object creation
		minioHandler.EXPECT().PutFencedObject(mock.Anything, bucket, mock.Anything).Return(minio.UploadInfo{}, nil)

		// Mock lock removal during close
		minioHandler.EXPECT().RemoveObject(mock.Anything, bucket,
			"test-segment-fence/segment_1_1.lock", mock.Anything).Return(nil)

		// Create segment instance
		segmentPrefixKey := "test-segment-fence"
		segment := NewSegmentImpl(ctx, 1, 1, segmentPrefixKey, bucket, minioHandler, cfg).(*SegmentImpl)
		require.NotNil(t, segment, "Segment should not be nil")
		defer segment.Close(ctx)

		// Test: Initially not fenced
		isFenced, err := segment.IsFenced(ctx)
		require.NoError(t, err, "IsFenced should not return error")
		assert.False(t, isFenced, "Segment should not be fenced initially")

		// Test: Fence the segment
		lastEntryId, err := segment.Fence(ctx)
		require.NoError(t, err, "Fence should not return error")
		assert.GreaterOrEqual(t, lastEntryId, int64(-1), "Should return valid last entry ID")

		// Test: Check if segment is fenced after fencing
		isFenced, err = segment.IsFenced(ctx)
		require.NoError(t, err, "IsFenced should not return error")
		assert.True(t, isFenced, "Segment should be fenced after calling Fence")

		// Test: Idempotent behavior - calling Fence again should not error
		lastEntryId2, err := segment.Fence(ctx)
		require.NoError(t, err, "Second Fence call should not return error")
		assert.Equal(t, lastEntryId, lastEntryId2, "Should return same last entry ID on idempotent call")
	})

	t.Run("ROSegmentImpl_Fence_Basic", func(t *testing.T) {
		ctx := context.Background()

		// Create mock minio handler
		minioHandler := mocks_minio.NewMinioHandler(t)
		bucket := "test-ro-fence-bucket"

		// Mock empty list for prefetch (no existing fragments)
		listEmptyChan := make(chan minio.ObjectInfo)
		close(listEmptyChan)
		minioHandler.EXPECT().ListObjects(mock.Anything, bucket, mock.Anything, mock.Anything, mock.Anything).Return(listEmptyChan)

		// Mock object existence check for fragment 0 (should not exist during prefetch)
		minioHandler.EXPECT().StatObject(mock.Anything, bucket, mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("object not found"))

		// Mock fence object creation
		minioHandler.EXPECT().PutFencedObject(mock.Anything, bucket, mock.Anything).Return(minio.UploadInfo{}, nil)

		// Create RO segment instance
		segmentPrefixKey := "test-ro-segment-fence"
		roSegment := NewROSegmentImpl(ctx, 2, 2, segmentPrefixKey, bucket, minioHandler, cfg).(*ROSegmentImpl)
		require.NotNil(t, roSegment, "RO segment should not be nil")
		defer roSegment.Close(ctx)

		// Test: Initially not fenced
		isFenced, err := roSegment.IsFenced(ctx)
		require.NoError(t, err, "IsFenced should not return error")
		assert.False(t, isFenced, "RO segment should not be fenced initially")

		// Test: Fence the RO segment
		lastEntryId, err := roSegment.Fence(ctx)
		require.NoError(t, err, "Fence should not return error")
		assert.GreaterOrEqual(t, lastEntryId, int64(-1), "Should return valid last entry ID")

		// Test: Check if RO segment is fenced after fencing
		isFenced, err = roSegment.IsFenced(ctx)
		require.NoError(t, err, "IsFenced should not return error")
		assert.True(t, isFenced, "RO segment should be fenced after calling Fence")

		// Test: Idempotent behavior - calling Fence again should not error
		lastEntryId2, err := roSegment.Fence(ctx)
		require.NoError(t, err, "Second Fence call should not return error")
		assert.Equal(t, lastEntryId, lastEntryId2, "Should return same last entry ID on idempotent call")
	})

	t.Run("Fence_RetryOnConcurrentCreation", func(t *testing.T) {
		ctx := context.Background()

		// Create mock minio handler
		minioHandler := mocks_minio.NewMinioHandler(t)
		bucket := "test-fence-retry-bucket"
		segmentPrefixKey := "test-segment-fence-retry"

		// Mock lock creation for segment
		minioHandler.EXPECT().PutObjectIfNoneMatch(mock.Anything, bucket,
			"test-segment-fence-retry/segment_4_4.lock", mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)

		// Mock first fence attempt (should fail with already exists)
		minioHandler.EXPECT().PutFencedObject(mock.Anything, bucket, mock.Anything).Return(minio.UploadInfo{}, werr.ErrFragmentAlreadyExists).Once()

		// Mock second fence attempt (should succeed)
		minioHandler.EXPECT().PutFencedObject(mock.Anything, bucket, mock.Anything).Return(minio.UploadInfo{}, nil).Once()

		// Mock lock removal during close
		minioHandler.EXPECT().RemoveObject(mock.Anything, bucket,
			"test-segment-fence-retry/segment_4_4.lock", mock.Anything).Return(nil)

		// Create segment instance
		segment := NewSegmentImpl(ctx, 4, 4, segmentPrefixKey, bucket, minioHandler, cfg).(*SegmentImpl)
		require.NotNil(t, segment, "Segment should not be nil")
		defer segment.Close(ctx)

		// Test: Fence should retry and succeed on second attempt
		lastEntryId, err := segment.Fence(ctx)
		require.NoError(t, err, "Fence should succeed after retry")
		assert.GreaterOrEqual(t, lastEntryId, int64(-1), "Should return valid last entry ID")

		// Test: Verify segment is fenced
		isFenced, err := segment.IsFenced(ctx)
		require.NoError(t, err, "IsFenced should not return error")
		assert.True(t, isFenced, "Segment should be fenced after retry")
	})
}

// Helper function to check if an object exists in object storage
func objectExists(ctx context.Context, client minioHandler.MinioHandler, bucket, objectKey string) (bool, error) {
	_, err := client.StatObject(ctx, bucket, objectKey, minio.StatObjectOptions{})
	if err != nil && minioHandler.IsObjectNotExists(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
