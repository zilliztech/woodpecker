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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_minio"
	"github.com/zilliztech/woodpecker/server/storage"
)

// TestNewSegmentImpl tests the NewSegmentImpl function.
func TestNewSegmentImpl(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	assert.Equal(t, int64(1000), segmentImpl.buffer.Load().MaxSize)
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
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"))
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}

	for _, ch := range chList {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Errorf("Timeout waiting for sync")
		}
	}

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(9), flushedLastId)
}

func TestAppendAsyncSomeAndWaitForFlush(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"))
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}

	for _, ch := range chList {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Errorf("Timeout waiting for sync")
		}
	}

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(6), flushedLastId)
}

func TestAppendAsyncOnceAndWaitForFlush(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"))
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}

	for _, ch := range chList {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Errorf("Timeout waiting for sync")
		}
	}

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), flushedLastId)
}

func TestAppendAsyncNoneAndWaitForFlush(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"))
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}

	timeoutErrs := 0
	for _, ch := range chList {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			timeoutErrs++
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
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"))
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}

	timeoutErrs := 0
	for _, ch := range chList {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			timeoutErrs++
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
		_, ch, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"))
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}
	assert.Equal(t, 10, len(chList))

	for _, ch := range chList {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Errorf("Timeout waiting for sync")
		}
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
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"))
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}

	for _, ch := range chList {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Errorf("Timeout waiting for sync")
		}
	}

	flushedFirstId := segmentImpl.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(9), flushedLastId)
}

func TestAppendAsyncDisorderAndPartialOutOfBounds(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		assignId, ch, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"))
		if entryId >= 10 {
			// 10-12 should async write buffer fail
			assert.Equal(t, int64(-1), assignId)
			assert.Error(t, err)
			assert.True(t, werr.ErrInvalidEntryId.Is(err))
			assert.NotNil(t, ch)
			assert.Equal(t, int64(-1), <-ch)
		} else {
			// 0-7 should async write buffer success
			assert.Equal(t, entryId, assignId)
			assert.NoError(t, err)
			assert.NotNil(t, ch)
			chList = append(chList, ch)
		}
	}

	for _, ch := range chList {
		select {
		case syncedId := <-ch:
			// 0-7 should be flush success
			assert.True(t, syncedId >= 0)
			assert.True(t, syncedId <= 7)
		case <-time.After(2 * time.Second):
			t.Errorf("Timeout waiting for sync")
		}
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
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        1000,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
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
		assignId0, ch0, err := segmentImpl.AppendAsync(context.Background(), 0, data800)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		assignId1, ch1, err := segmentImpl.AppendAsync(context.Background(), 1, data200)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		// reach the max size, should be immediate flush
		// show check response immediately
		start := time.Now()
		assert.Equal(t, int64(0), <-ch0)
		assert.Equal(t, int64(1), <-ch1)
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
		assignId0, ch0, err := segmentImpl.AppendAsync(context.Background(), 0, data200)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		assignId1, ch1, err := segmentImpl.AppendAsync(context.Background(), 1, data300)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		// wait for flush
		start := time.Now()
		assert.Equal(t, int64(0), <-ch0)
		assert.Equal(t, int64(1), <-ch1)
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
		assignId0, ch0, err := segmentImpl.AppendAsync(context.Background(), 0, data200)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		assignId1, ch1, err := segmentImpl.AppendAsync(context.Background(), 1, data300)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		// wait for flush
		start := time.Now()
		assert.Equal(t, int64(0), <-ch0)
		assert.Equal(t, int64(1), <-ch1)
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
		assignId0, ch0, err := segmentImpl.AppendAsync(context.Background(), 0, data1k)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		// flush immediately
		start := time.Now()
		assert.Equal(t, int64(0), <-ch0)
		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost < 100, fmt.Sprintf("should wait less then 100 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), segmentImpl.getFirstEntryId())
		flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(0), flushedLastId)

		assignId1, ch1, err := segmentImpl.AppendAsync(context.Background(), 1, data200)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		// wait for flush
		start = time.Now()
		assert.Equal(t, int64(1), <-ch1)
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
		assignId0, ch0, err := segmentImpl.AppendAsync(context.Background(), 0, data800)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		assignId2, ch2, err := segmentImpl.AppendAsync(context.Background(), 2, data300)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), assignId2)
		// flush 1(800) immediately
		start := time.Now()
		assert.Equal(t, int64(0), <-ch0)
		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost < 100, fmt.Sprintf("should wait less then 100 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), segmentImpl.getFirstEntryId())
		flushedLastId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(0), flushedLastId)

		assignId1, ch1, err := segmentImpl.AppendAsync(context.Background(), 1, data200)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)

		// wait for flush
		start = time.Now()
		assert.Equal(t, int64(1), <-ch1)
		assert.Equal(t, int64(2), <-ch2)
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
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	chList := make([]<-chan int64, 0)
	for i := 0; i < 100; i++ {
		assignId, ch, err := segmentImpl.AppendAsync(context.Background(), int64(i), []byte("test_data"))
		assert.Equal(t, int64(i), assignId)
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}

	err := segmentImpl.Sync(context.Background())
	assert.NoError(t, err)

	for _, ch := range chList {
		select {
		case syncedId := <-ch:
			// success
			assert.True(t, syncedId >= 0)
			assert.True(t, syncedId < 100)
		case <-time.After(1 * time.Second):
			t.Errorf("Timeout waiting for sync")
		}
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
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	chList := make([]<-chan int64, 0)
	for i := 0; i < 100; i++ {
		_, ch, err := segmentImpl.AppendAsync(context.Background(), int64(i), []byte("test_data"))
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}
	assert.Equal(t, 100, len(chList))

	// Close log file
	err := segmentImpl.Close(context.TODO())
	assert.NoError(t, err)

	// final flush immediately
	start := time.Now()
	successCount := 0
	for _, ch := range chList {
		select {
		case syncedId := <-ch:
			if syncedId >= 0 {
				successCount++
			}
		case <-time.After(1 * time.Second):
			t.Errorf("Timeout waiting for sync")
		}
	}
	cost := time.Now().Sub(start).Milliseconds()
	assert.True(t, cost < 1000, fmt.Sprintf("should flush immediately, but cost %d ms", cost))
	assert.Equal(t, 100, successCount)
}

// TestGetId tests the GetId function.
func TestGetId(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        2 * 1024 * 1024 * 1024,
					MaxInterval:     100000, // 100s, means turn off auto sync during the test
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
				LogFileCompactionPolicy: config.LogFileCompactionPolicy{
					MaxBytes: 4 * 1024 * 1024,
				},
			},
		},
	}

	// mock fragments
	mockFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestMerge/1/0/1.frag",
		[][]byte{[]byte("entry1"), []byte("entry2")}, 100, true, false, true)
	mockFragment2 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestMerge/1/0/2.frag",
		[][]byte{[]byte("entry3"), []byte("entry4")}, 102, true, false, true)

	// mock object storage interfaces
	listChan := make(chan minio.ObjectInfo)
	close(listChan)
	client.EXPECT().ListObjects(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(listChan)
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)

	roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "TestMerge/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
	roSegmentImpl.fragments = []*FragmentObject{mockFragment1, mockFragment2} // set test fragments
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
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	mockFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestMerge/1/0/1.frag",
		[][]byte{[]byte("entry1"), []byte("entry2")}, 100, true, false, true)
	mockFragment2 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestMerge/1/0/2.frag",
		[][]byte{[]byte("entry3"), []byte("entry4")}, 102, true, false, true)

	// mock object storage interfaces
	listChan := make(chan minio.ObjectInfo)
	close(listChan)
	client.EXPECT().ListObjects(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(listChan)
	// Load data
	roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "TestLoad/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
	roSegmentImpl.fragments = []*FragmentObject{mockFragment1, mockFragment2} // set test mock fragments
	totalSize, lastFragment, err := roSegmentImpl.Load(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, mockFragment1.size+mockFragment2.size, totalSize)
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
	// when read frag 3, return it is not exists
	client.EXPECT().StatObject(mock.Anything, "test-bucket", "TestNewReaderInWriterLogFile/1/0/3.frag", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{
		Code: "NoSuchKey",
	})
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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

	// Write some data to buffer and sync
	// mock fragments
	mockFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestNewReaderInWriterLogFile/1/0/1.frag",
		[][]byte{[]byte("entry1"), []byte("entry2")}, 0, true, false, true)
	mockFragment2 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestNewReaderInWriterLogFile/1/0/2.frag",
		[][]byte{[]byte("entry3"), []byte("entry4")}, 2, true, false, true)

	// mock object storage interfaces
	listChan := make(chan minio.ObjectInfo)
	close(listChan)
	client.EXPECT().ListObjects(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(listChan)

	// Create a reader for [0, 100)
	roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "TestNewReaderInWriterLogFile/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
	roSegmentImpl.fragments = []*FragmentObject{mockFragment1, mockFragment2} // set test mock fragments
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
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
	data := make([][]byte, 0, 100)
	for i := 0; i < 100; i++ {
		data = append(data, []byte(fmt.Sprintf("test_data%d", i)))
	}
	mockFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestNewReaderInROSegmentImpl/1/0/1.frag",
		data, 0, true, false, true)

	// mock object storage interfaces
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", "TestNewReaderInROSegmentImpl/1/0/2.frag", mock.Anything).Return(minio.ObjectInfo{}, minio.ErrorResponse{
	//	Code: "NoSuchKey",
	//})
	listChan := make(chan minio.ObjectInfo)
	close(listChan)
	client.EXPECT().ListObjects(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(listChan)

	// Create a reader for [0, 100)
	roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "TestNewReaderInROLogFile/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
	roSegmentImpl.fragments = []*FragmentObject{mockFragment1} // set test mock fragments
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   100,
	})
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	// Read data
	for i := 0; i < 100; i++ {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		assert.True(t, hasNext)

		entry, err := reader.ReadNext(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(i), entry.EntryId)
		assert.Equal(t, []byte(fmt.Sprintf("test_data%d", i)), entry.Values)
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
				LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        3000, // 3000 bytes buffer
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1000, // 1000 bytes per frag
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	// mock fragments
	// test read data with holes (fragment 1,x,3) in minio
	mockFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestROLogFileReadDataWithHoles/1/0/1.frag",
		[][]byte{[]byte("entry1"), []byte("entry2")}, 100, true, false, true)
	mockFragment2 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestROLogFileReadDataWithHoles/1/0/2.frag",
		[][]byte{[]byte("entry3"), []byte("entry4")}, 102, true, false, true)
	//mockFragment3 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 3, "TestROLogFileReadDataWithHoles/1/0/3.frag",
	//	[][]byte{[]byte("entry4"), []byte("entry5")}, 104, true, false, true)
	mockFragment4 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 4, "TestROLogFileReadDataWithHoles/1/0/4.frag",
		[][]byte{[]byte("entry6"), []byte("entry7")}, 106, true, false, true)

	mockMergedFragment0 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestROLogFileReadDataWithHoles/1/0/m_0.frag",
		[][]byte{[]byte("entry1"), []byte("entry2")}, 100, true, false, true)
	mockMergedFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestROLogFileReadDataWithHoles/1/0/m_1.frag",
		[][]byte{[]byte("entry3"), []byte("entry4")}, 102, true, false, true)
	//mockMergedFragment2 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 3, "TestROLogFileReadDataWithHoles/1/0/m_2.frag",
	//	[][]byte{[]byte("entry4"), []byte("entry5")}, 104, true, false, true)
	mockMergedFragment3 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 4, "TestROLogFileReadDataWithHoles/1/0/m_3.frag",
		[][]byte{[]byte("entry6"), []byte("entry7")}, 106, true, false, true)

	listChan := make(chan minio.ObjectInfo, 6) // list return disorder and hole list
	listChan <- minio.ObjectInfo{
		Key:  mockFragment2.fragmentKey,
		Size: mockFragment2.size,
	}
	listChan <- minio.ObjectInfo{
		Key:  mockMergedFragment0.fragmentKey,
		Size: mockMergedFragment0.size,
	}
	listChan <- minio.ObjectInfo{
		Key:  mockFragment4.fragmentKey,
		Size: mockFragment4.size,
	}
	listChan <- minio.ObjectInfo{
		Key:  mockMergedFragment3.fragmentKey,
		Size: mockMergedFragment3.size,
	}
	listChan <- minio.ObjectInfo{
		Key:  mockMergedFragment1.fragmentKey,
		Size: mockMergedFragment1.size,
	}
	listChan <- minio.ObjectInfo{
		Key:  mockFragment1.fragmentKey,
		Size: mockFragment1.size,
	}
	close(listChan)
	client.EXPECT().ListObjects(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).Return(listChan)

	// we should only read data in fragment 1
	roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "TestROLogFileReadDataWithHoles/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
	assert.NotNil(t, roSegmentImpl.fragments)
	assert.Equal(t, 2, len(roSegmentImpl.fragments))
	assert.Equal(t, mockFragment1.fragmentKey, roSegmentImpl.fragments[0].fragmentKey)
	assert.Equal(t, mockFragment2.fragmentKey, roSegmentImpl.fragments[1].fragmentKey)
	assert.Equal(t, 2, len(roSegmentImpl.mergedFragments))
	assert.Equal(t, mockMergedFragment0.fragmentKey, roSegmentImpl.mergedFragments[0].fragmentKey)
	assert.Equal(t, mockMergedFragment1.fragmentKey, roSegmentImpl.mergedFragments[1].fragmentKey)
}

func TestFindFragment(t *testing.T) {
	// Create mock Fragment object list
	mockFragments := []*FragmentObject{
		{fragmentId: 1, firstEntryId: 0, lastEntryId: 99, infoFetched: true},    // [0,99]
		{fragmentId: 2, firstEntryId: 100, lastEntryId: 199, infoFetched: true}, // [100,199]
		{fragmentId: 3, firstEntryId: 200, lastEntryId: 299, infoFetched: true}, // [200,299]
	}

	// Create LogFile instance and inject mock data
	roSegmentImpl := &ROSegmentImpl{
		fragments: mockFragments,
	}

	t.Run("Find Fragment in middle position", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(150)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), frag.fragmentId)
	})

	t.Run("Find first Fragment", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(50)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), frag.fragmentId)
	})

	t.Run("Find last Fragment", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(250)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), frag.fragmentId)
	})

	t.Run("entryId after the last Fragment", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(300)
		assert.NoError(t, err)
		assert.Nil(t, frag)
	})

	t.Run("First Fragment boundary values", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), frag.fragmentId)

		frag, err = roSegmentImpl.findFragment(99)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), frag.fragmentId)
	})
	t.Run("Second Fragment boundary values", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(100)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), frag.fragmentId)

		frag, err = roSegmentImpl.findFragment(199)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), frag.fragmentId)
	})

	t.Run("Last Fragment boundary values", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(200)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), frag.fragmentId)

		frag, err = roSegmentImpl.findFragment(299)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), frag.fragmentId)
	})

	t.Run("Return leftmost match when multiple candidate Fragments", func(t *testing.T) {
		// Mock overlapping Fragments
		overlappingFrags := []*FragmentObject{
			{fragmentId: 1, firstEntryId: 0, lastEntryId: 200, infoFetched: true},
			{fragmentId: 2, firstEntryId: 100, lastEntryId: 300, infoFetched: true},
		}
		newLogFile := &ROSegmentImpl{
			fragments: overlappingFrags,
		}

		frag, err := newLogFile.findFragment(150)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), frag.fragmentId)
	})
}

// TestDeleteFragments tests the DeleteFragments function.
func TestDeleteFragments(t *testing.T) {
	t.Run("SuccessfulDeletion", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
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
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/1.frag", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/2.frag", Size: 2048}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/m_0.frag", Size: 4096}
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh).Once()

		// Create the LogFile
		roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)
		assert.Equal(t, 2, len(roSegmentImpl.fragments))
		assert.Equal(t, 1, len(roSegmentImpl.mergedFragments))

		objectCh2 := make(chan minio.ObjectInfo, 3)
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/1.frag", Size: 1024}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/2.frag", Size: 2048}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/m_0.frag", Size: 4096}
		close(objectCh2)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh2).Once()
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/1.frag", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/2.frag", mock.Anything).Return(nil)
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
					LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					LogFileCompactionPolicy: config.LogFileCompactionPolicy{
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
					LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					LogFileCompactionPolicy: config.LogFileCompactionPolicy{
						MaxBytes: 4 * 1024 * 1024,
					},
				},
			},
		}

		// Create a list of mock objects to be returned by ListObjects
		objectCh := make(chan minio.ObjectInfo, 2)
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/1.frag", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/2.frag", Size: 2048}
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh).Once()
		// Create the LogFile
		roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)

		objectCh2 := make(chan minio.ObjectInfo, 2)
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/1.frag", Size: 1024}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/2.frag", Size: 2048}
		close(objectCh2)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh2).Once()
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/1.frag", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/2.frag", mock.Anything).Return(errors.New("remove error"))

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
					LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					LogFileCompactionPolicy: config.LogFileCompactionPolicy{
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
					LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					LogFileCompactionPolicy: config.LogFileCompactionPolicy{
						MaxBytes: 4 * 1024 * 1024,
					},
				},
			},
		}

		// Create a list of mock objects including non-fragment files
		objectCh := make(chan minio.ObjectInfo, 3)
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/1.frag", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/metadata.json", Size: 256} // Not a fragment
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/m_1.frag", Size: 4096}
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh).Once()
		// No call for metadata.json as it should be skipped

		// Create the readonly segment impl
		roSegmentImpl := NewROSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*ROSegmentImpl)

		objectCh2 := make(chan minio.ObjectInfo, 3)
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/1.frag", Size: 1024}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/metadata.json", Size: 256} // Not a fragment
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/m_1.frag", Size: 4096}
		close(objectCh2)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh2).Once()
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/1.frag", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/m_1.frag", mock.Anything).Return(nil)
		//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
		// Call DeleteFragments
		err := roSegmentImpl.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err)
	})
}

func TestPrepareMultiFragmentDataIfNecessary_EmptyData(t *testing.T) {
	// Create a SegmentImpl with test configuration
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 1024, // 1KB max flush size
		},
	}

	// Test with empty data
	toFlushData := [][]byte{}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return empty partitions
	assert.Equal(t, 0, len(partitions))
	assert.Equal(t, 0, len(partitionFirstEntryIds))
}

func TestPrepareMultiFragmentDataIfNecessary_SingleSmallEntry(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 1024, // 1KB max flush size
		},
	}

	// Test with single small entry
	toFlushData := [][]byte{
		[]byte("small data"), // 10 bytes
	}
	toFlushDataFirstEntryId := int64(5)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return single partition with one entry
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 1, len(partitions[0]))
	assert.Equal(t, []byte("small data"), partitions[0][0])
	assert.Equal(t, int64(5), partitionFirstEntryIds[0])
}

func TestPrepareMultiFragmentDataIfNecessary_MultipleSmallEntries(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 1024, // 1KB max flush size
		},
	}

	// Test with multiple small entries that fit in one partition
	toFlushData := [][]byte{
		[]byte("data1"), // 5 bytes
		[]byte("data2"), // 5 bytes
		[]byte("data3"), // 5 bytes
	}
	toFlushDataFirstEntryId := int64(10)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return single partition with all entries
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 3, len(partitions[0]))
	assert.Equal(t, []byte("data1"), partitions[0][0])
	assert.Equal(t, []byte("data2"), partitions[0][1])
	assert.Equal(t, []byte("data3"), partitions[0][2])
	assert.Equal(t, int64(10), partitionFirstEntryIds[0])
}

func TestPrepareMultiFragmentDataIfNecessary_EntriesExceedMaxSize(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 20, // 20 bytes max flush size
		},
	}

	// Test with entries that exceed max flush size
	toFlushData := [][]byte{
		[]byte("data1234567890"), // 14 bytes
		[]byte("data2345"),       // 8 bytes (14+8=22 > 20, should split)
		[]byte("data3"),          // 5 bytes
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return two partitions
	assert.Equal(t, 2, len(partitions))
	assert.Equal(t, 2, len(partitionFirstEntryIds))

	// First partition should contain first entry only
	assert.Equal(t, 1, len(partitions[0]))
	assert.Equal(t, []byte("data1234567890"), partitions[0][0])
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	// Second partition should contain second and third entries
	assert.Equal(t, 2, len(partitions[1]))
	assert.Equal(t, []byte("data2345"), partitions[1][0])
	assert.Equal(t, []byte("data3"), partitions[1][1])
	assert.Equal(t, int64(1), partitionFirstEntryIds[1])
}

func TestPrepareMultiFragmentDataIfNecessary_SingleLargeEntry(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 10, // 10 bytes max flush size
		},
	}

	// Test with single entry larger than max flush size
	largeData := make([]byte, 50) // 50 bytes, larger than max flush size
	for i := range largeData {
		largeData[i] = byte('A')
	}

	toFlushData := [][]byte{largeData}
	toFlushDataFirstEntryId := int64(100)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should still return single partition (large entries are not split)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 1, len(partitions[0]))
	assert.Equal(t, largeData, partitions[0][0])
	assert.Equal(t, int64(100), partitionFirstEntryIds[0])
}

func TestPrepareMultiFragmentDataIfNecessary_MultiplePartitions(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 15, // 15 bytes max flush size
		},
	}

	// Test with entries that create multiple partitions
	toFlushData := [][]byte{
		[]byte("data1"), // 5 bytes
		[]byte("data2"), // 5 bytes (total: 10 bytes, fits)
		[]byte("data3"), // 5 bytes (total: 15 bytes, fits)
		[]byte("data4"), // 5 bytes (total: 20 bytes, exceeds, new partition)
		[]byte("data5"), // 5 bytes (total: 10 bytes in second partition)
		[]byte("data6"), // 5 bytes (total: 15 bytes in second partition)
		[]byte("data7"), // 5 bytes (total: 20 bytes, exceeds, new partition)
	}
	toFlushDataFirstEntryId := int64(50)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return three partitions
	assert.Equal(t, 3, len(partitions))
	assert.Equal(t, 3, len(partitionFirstEntryIds))

	// First partition: data1, data2, data3
	assert.Equal(t, 3, len(partitions[0]))
	assert.Equal(t, []byte("data1"), partitions[0][0])
	assert.Equal(t, []byte("data2"), partitions[0][1])
	assert.Equal(t, []byte("data3"), partitions[0][2])
	assert.Equal(t, int64(50), partitionFirstEntryIds[0])

	// Second partition: data4, data5, data6
	assert.Equal(t, 3, len(partitions[1]))
	assert.Equal(t, []byte("data4"), partitions[1][0])
	assert.Equal(t, []byte("data5"), partitions[1][1])
	assert.Equal(t, []byte("data6"), partitions[1][2])
	assert.Equal(t, int64(53), partitionFirstEntryIds[1])

	// Third partition: data7
	assert.Equal(t, 1, len(partitions[2]))
	assert.Equal(t, []byte("data7"), partitions[2][0])
	assert.Equal(t, int64(56), partitionFirstEntryIds[2])
}

func TestPrepareMultiFragmentDataIfNecessary_ZeroMaxFlushSize(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 0, // 0 bytes max flush size (edge case)
		},
	}

	// Test with zero max flush size
	toFlushData := [][]byte{
		[]byte("a"), // 1 byte
		[]byte("b"), // 1 byte
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Each entry should be in its own partition
	assert.Equal(t, 2, len(partitions))
	assert.Equal(t, 2, len(partitionFirstEntryIds))

	assert.Equal(t, 1, len(partitions[0]))
	assert.Equal(t, []byte("a"), partitions[0][0])
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	assert.Equal(t, 1, len(partitions[1]))
	assert.Equal(t, []byte("b"), partitions[1][0])
	assert.Equal(t, int64(1), partitionFirstEntryIds[1])
}

func TestPrepareMultiFragmentDataIfNecessary_VaryingSizes(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 100, // 100 bytes max flush size
		},
	}

	// Test with varying entry sizes
	toFlushData := [][]byte{
		make([]byte, 30), // 30 bytes
		make([]byte, 40), // 40 bytes (total: 70 bytes, fits)
		make([]byte, 50), // 50 bytes (total: 120 bytes, exceeds, new partition)
		make([]byte, 20), // 20 bytes (total: 70 bytes in second partition)
		make([]byte, 25), // 25 bytes (total: 95 bytes in second partition)
		make([]byte, 10), // 10 bytes (total: 105 bytes, exceeds, new partition)
	}
	toFlushDataFirstEntryId := int64(1000)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return three partitions
	assert.Equal(t, 3, len(partitions))
	assert.Equal(t, 3, len(partitionFirstEntryIds))

	// First partition: 30 + 40 = 70 bytes
	assert.Equal(t, 2, len(partitions[0]))
	assert.Equal(t, 30, len(partitions[0][0]))
	assert.Equal(t, 40, len(partitions[0][1]))
	assert.Equal(t, int64(1000), partitionFirstEntryIds[0])

	// Second partition: 50 + 20 + 25 = 95 bytes
	assert.Equal(t, 3, len(partitions[1]))
	assert.Equal(t, 50, len(partitions[1][0]))
	assert.Equal(t, 20, len(partitions[1][1]))
	assert.Equal(t, 25, len(partitions[1][2]))
	assert.Equal(t, int64(1002), partitionFirstEntryIds[1])

	// Third partition: 10 bytes
	assert.Equal(t, 1, len(partitions[2]))
	assert.Equal(t, 10, len(partitions[2][0]))
	assert.Equal(t, int64(1005), partitionFirstEntryIds[2])
}

func TestPrepareMultiFragmentDataIfNecessary_EntryIdCalculation(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 2, // Very small size to force multiple partitions
		},
	}

	// Test entry ID calculation with different starting entry ID
	toFlushData := [][]byte{
		[]byte("ab"), // 2 bytes (fits in first partition)
		[]byte("cd"), // 2 bytes (fits in second partition)
		[]byte("ef"), // 2 bytes (fits in third partition)
	}
	toFlushDataFirstEntryId := int64(42) // Start from entry ID 42

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should create multiple partitions due to small max flush size
	assert.Equal(t, 3, len(partitions))
	assert.Equal(t, 3, len(partitionFirstEntryIds))

	// Verify entry ID progression
	assert.Equal(t, int64(42), partitionFirstEntryIds[0]) // First partition starts at 42
	assert.Equal(t, int64(43), partitionFirstEntryIds[1]) // Second partition starts at 43
	assert.Equal(t, int64(44), partitionFirstEntryIds[2]) // Third partition starts at 44

	// Verify partition contents
	assert.Equal(t, []byte("ab"), partitions[0][0])
	assert.Equal(t, []byte("cd"), partitions[1][0])
	assert.Equal(t, []byte("ef"), partitions[2][0])
}

func TestPrepareMultiFragmentDataIfNecessary_LargeMaxFlushSize(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 1024 * 1024, // 1MB max flush size (very large)
		},
	}

	// Test with many small entries that should all fit in one partition
	toFlushData := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		toFlushData[i] = []byte("small_data_entry") // 16 bytes each
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return single partition with all entries
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 100, len(partitions[0]))
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	// Verify all entries are present
	for i := 0; i < 100; i++ {
		assert.Equal(t, []byte("small_data_entry"), partitions[0][i])
	}
}

func TestPrepareMultiFragmentDataIfNecessary_ExactSizeMatch(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 10, // Exactly 10 bytes max flush size
		},
	}

	// Test with entries that exactly match the max flush size
	toFlushData := [][]byte{
		[]byte("1234567890"), // Exactly 10 bytes
		[]byte("abcdefghij"), // Exactly 10 bytes
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return two partitions, each with one entry
	assert.Equal(t, 2, len(partitions))
	assert.Equal(t, 2, len(partitionFirstEntryIds))

	assert.Equal(t, 1, len(partitions[0]))
	assert.Equal(t, []byte("1234567890"), partitions[0][0])
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	assert.Equal(t, 1, len(partitions[1]))
	assert.Equal(t, []byte("abcdefghij"), partitions[1][0])
	assert.Equal(t, int64(1), partitionFirstEntryIds[1])
}

func TestPrepareMultiFragmentDataIfNecessary_NegativeEntryId(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 20,
		},
	}

	// Test with negative starting entry ID
	toFlushData := [][]byte{
		[]byte("data1"), // 5 bytes
		[]byte("data2"), // 5 bytes
	}
	toFlushDataFirstEntryId := int64(-10) // Negative starting entry ID

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should work correctly with negative entry IDs
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 2, len(partitions[0]))
	assert.Equal(t, int64(-10), partitionFirstEntryIds[0])
}

func TestPrepareMultiFragmentDataIfNecessary_SingleByteEntries(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 3, // 3 bytes max flush size
		},
	}

	// Test with single byte entries
	toFlushData := [][]byte{
		[]byte("a"), // 1 byte
		[]byte("b"), // 1 byte
		[]byte("c"), // 1 byte (total: 3 bytes, fits)
		[]byte("d"), // 1 byte (total: 4 bytes, exceeds, new partition)
		[]byte("e"), // 1 byte
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return two partitions
	assert.Equal(t, 2, len(partitions))
	assert.Equal(t, 2, len(partitionFirstEntryIds))

	// First partition: a, b, c (3 bytes)
	assert.Equal(t, 3, len(partitions[0]))
	assert.Equal(t, []byte("a"), partitions[0][0])
	assert.Equal(t, []byte("b"), partitions[0][1])
	assert.Equal(t, []byte("c"), partitions[0][2])
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	// Second partition: d, e (2 bytes)
	assert.Equal(t, 2, len(partitions[1]))
	assert.Equal(t, []byte("d"), partitions[1][0])
	assert.Equal(t, []byte("e"), partitions[1][1])
	assert.Equal(t, int64(3), partitionFirstEntryIds[1])
}

func TestPrepareMultiFragmentDataIfNecessary_MaxInt64EntryId(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 100,
		},
	}

	// Test with very large entry ID
	toFlushData := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
	}
	toFlushDataFirstEntryId := int64(9223372036854775800) // Near max int64

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should work correctly with large entry IDs
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, int64(9223372036854775800), partitionFirstEntryIds[0])
}

func TestPrepareMultiFragmentDataIfNecessary_EmptyEntries(t *testing.T) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 10,
		},
	}

	// Test with empty byte slices
	toFlushData := [][]byte{
		[]byte{},       // 0 bytes
		[]byte("data"), // 4 bytes
		[]byte{},       // 0 bytes
		[]byte("more"), // 4 bytes
		[]byte{},       // 0 bytes
	}
	toFlushDataFirstEntryId := int64(0)

	partitions, partitionFirstEntryIds := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return single partition with all entries (total 8 bytes)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 1, len(partitionFirstEntryIds))
	assert.Equal(t, 5, len(partitions[0]))
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	// Verify all entries including empty ones
	assert.Equal(t, []byte{}, partitions[0][0])
	assert.Equal(t, []byte("data"), partitions[0][1])
	assert.Equal(t, []byte{}, partitions[0][2])
	assert.Equal(t, []byte("more"), partitions[0][3])
	assert.Equal(t, []byte{}, partitions[0][4])
}

// BenchmarkPrepareMultiFragmentDataIfNecessary_SmallEntries benchmarks the repackIfNecessary function with small entries
func BenchmarkPrepareMultiFragmentDataIfNecessary_SmallEntries(b *testing.B) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 1024, // 1KB max flush size
		},
	}

	// Create test data with small entries
	toFlushData := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		toFlushData[i] = []byte("small_data_entry") // 16 bytes each
	}
	toFlushDataFirstEntryId := int64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)
	}
}

// BenchmarkPrepareMultiFragmentDataIfNecessary_LargeEntries benchmarks the repackIfNecessary function with large entries
func BenchmarkPrepareMultiFragmentDataIfNecessary_LargeEntries(b *testing.B) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 8192, // 8KB max flush size
		},
	}

	// Create test data with large entries
	toFlushData := make([][]byte, 50)
	for i := 0; i < 50; i++ {
		data := make([]byte, 1024) // 1KB each
		for j := range data {
			data[j] = byte(i % 256)
		}
		toFlushData[i] = data
	}
	toFlushDataFirstEntryId := int64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)
	}
}

// BenchmarkPrepareMultiFragmentDataIfNecessary_ManyPartitions benchmarks the repackIfNecessary function with many small partitions
func BenchmarkPrepareMultiFragmentDataIfNecessary_ManyPartitions(b *testing.B) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 100, // Small size to force many partitions
		},
	}

	// Create test data that will create many partitions
	toFlushData := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		toFlushData[i] = []byte("data") // 4 bytes each
	}
	toFlushDataFirstEntryId := int64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)
	}
}

// BenchmarkPrepareMultiFragmentDataIfNecessary_VaryingSizes benchmarks the repackIfNecessary function with varying entry sizes
func BenchmarkPrepareMultiFragmentDataIfNecessary_VaryingSizes(b *testing.B) {
	segment := &SegmentImpl{
		syncPolicyConfig: &config.LogFileSyncPolicyConfig{
			MaxFlushSize: 2048, // 2KB max flush size
		},
	}

	// Create test data with varying sizes
	toFlushData := make([][]byte, 200)
	for i := 0; i < 200; i++ {
		size := (i%10 + 1) * 10 // Sizes from 10 to 100 bytes
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(i % 256)
		}
		toFlushData[i] = data
	}
	toFlushDataFirstEntryId := int64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)
	}
}

// repackIfNecessaryOld is the old implementation for comparison
func repackIfNecessaryOld(maxFlushSize int64, toFlushData [][]byte, toFlushDataFirstEntryId int64) ([][][]byte, []int64) {
	maxPartitionSize := maxFlushSize
	var partitions = make([][][]byte, 0)
	var partition = make([][]byte, 0)
	var currentSize = 0

	for _, entry := range toFlushData {
		entrySize := len(entry)
		if int64(currentSize+entrySize) > maxPartitionSize && currentSize > 0 {
			partitions = append(partitions, partition)
			partition = make([][]byte, 0)
			currentSize = 0
		}
		partition = append(partition, entry)
		currentSize += entrySize
	}
	if len(partition) > 0 {
		partitions = append(partitions, partition)
	}

	var partitionFirstEntryIds = make([]int64, 0)
	offset := toFlushDataFirstEntryId
	for _, part := range partitions {
		partitionFirstEntryIds = append(partitionFirstEntryIds, offset)
		offset += int64(len(part))
	}

	return partitions, partitionFirstEntryIds
}

// BenchmarkPrepareMultiFragmentDataIfNecessary_Old_SmallEntries benchmarks the old implementation with small entries
func BenchmarkPrepareMultiFragmentDataIfNecessary_Old_SmallEntries(b *testing.B) {
	// Create test data with small entries
	toFlushData := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		toFlushData[i] = []byte("small_data_entry") // 16 bytes each
	}
	toFlushDataFirstEntryId := int64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = repackIfNecessaryOld(1024, toFlushData, toFlushDataFirstEntryId)
	}
}

// BenchmarkPrepareMultiFragmentDataIfNecessary_Old_ManyPartitions benchmarks the old implementation with many partitions
func BenchmarkPrepareMultiFragmentDataIfNecessary_Old_ManyPartitions(b *testing.B) {
	// Create test data that will create many partitions
	toFlushData := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		toFlushData[i] = []byte("data") // 4 bytes each
	}
	toFlushDataFirstEntryId := int64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = repackIfNecessaryOld(100, toFlushData, toFlushDataFirstEntryId)
	}
}
