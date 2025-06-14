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
	"sync"
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
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

// TestNewSegmentImpl tests the NewSegmentImpl function.
func TestNewSegmentImpl(t *testing.T) {
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
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		ch := make(chan int64, 1)
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), ch)
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}

	for idx, ch := range chList {
		select {
		case syncedId := <-ch:
			fmt.Printf("%d synced %d\n", idx, syncedId)
		case <-time.After(5 * time.Second):
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		ch := make(chan int64, 1)
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), ch)
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		ch := make(chan int64, 1)
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), ch)
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		ch := make(chan int64, 1)
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), ch)
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		ch := make(chan int64, 1)
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), ch)
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
		ch := make(chan int64, 1)
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), ch)
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		ch := make(chan int64, 1)
		_, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), ch)
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
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		ch := make(chan int64, 1)
		assignId, err := segmentImpl.AppendAsync(context.Background(), entryId, []byte("test_data"), ch)
		if entryId >= 10 {
			// 10-12 should async write buffer fail
			assert.Equal(t, int64(-1), assignId)
			assert.Error(t, err)
			assert.True(t, werr.ErrInvalidEntryId.Is(err))
			assert.NotNil(t, ch)
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
		ch0 := make(chan int64, 1)
		assignId0, err := segmentImpl.AppendAsync(context.Background(), 0, data800, ch0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		ch1 := make(chan int64, 1)
		assignId1, err := segmentImpl.AppendAsync(context.Background(), 1, data200, ch1)
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
		ch0 := make(chan int64, 1)
		assignId0, err := segmentImpl.AppendAsync(context.Background(), 0, data200, ch0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		ch1 := make(chan int64, 1)
		assignId1, err := segmentImpl.AppendAsync(context.Background(), 1, data300, ch1)
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
		ch0 := make(chan int64, 1)
		assignId0, err := segmentImpl.AppendAsync(context.Background(), 0, data200, ch0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		ch1 := make(chan int64, 1)
		assignId1, err := segmentImpl.AppendAsync(context.Background(), 1, data300, ch1)
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
		ch0 := make(chan int64, 1)
		assignId0, err := segmentImpl.AppendAsync(context.Background(), 0, data1k, ch0)
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

		ch1 := make(chan int64, 1)
		assignId1, err := segmentImpl.AppendAsync(context.Background(), 1, data200, ch1)
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
		ch0 := make(chan int64, 1)
		assignId0, err := segmentImpl.AppendAsync(context.Background(), 0, data800, ch0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		ch2 := make(chan int64, 1)
		assignId2, err := segmentImpl.AppendAsync(context.Background(), 2, data300, ch2)
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

		ch1 := make(chan int64, 1)
		assignId1, err := segmentImpl.AppendAsync(context.Background(), 1, data200, ch1)
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
	chList := make([]<-chan int64, 0)
	for i := 0; i < 100; i++ {
		ch := make(chan int64, 1)
		assignId, err := segmentImpl.AppendAsync(context.Background(), int64(i), []byte("test_data"), ch)
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
	chList := make([]<-chan int64, 0)
	for i := 0; i < 100; i++ {
		ch := make(chan int64, 1)
		_, err := segmentImpl.AppendAsync(context.Background(), int64(i), []byte("test_data"), ch)
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
	mockFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestMerge/1/0/1.frag",
		[]*cache.BufferEntry{
			{EntryId: 100, Data: []byte("entry1"), NotifyChan: nil},
			{EntryId: 101, Data: []byte("entry2"), NotifyChan: nil},
		}, 100, true, false, true)
	mockFragment2 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestMerge/1/0/2.frag",
		[]*cache.BufferEntry{
			{EntryId: 102, Data: []byte("entry3"), NotifyChan: nil},
			{EntryId: 103, Data: []byte("entry4"), NotifyChan: nil},
		}, 102, true, false, true)

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
	mockFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestMerge/1/0/1.frag",
		[]*cache.BufferEntry{
			{EntryId: 100, Data: []byte("entry1"), NotifyChan: nil},
			{EntryId: 101, Data: []byte("entry2"), NotifyChan: nil},
		}, 100, true, false, true)
	mockFragment2 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestMerge/1/0/2.frag",
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
	mockFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestNewReaderInWriterLogFile/1/0/1.frag",
		[]*cache.BufferEntry{
			{EntryId: 0, Data: []byte("entry1"), NotifyChan: nil},
			{EntryId: 1, Data: []byte("entry2"), NotifyChan: nil},
		}, 0, true, false, true)
	mockFragment2 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestNewReaderInWriterLogFile/1/0/2.frag",
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
	mockFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestNewReaderInROSegmentImpl/1/0/1.frag",
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
	roSegmentImpl.fragments = []*FragmentObject{mockFragment1} // set test mock fragments
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
	mockFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestROLogFileReadDataWithHoles/1/0/0.frag",
		[]*cache.BufferEntry{
			{EntryId: 100, Data: []byte("entry1"), NotifyChan: nil},
			{EntryId: 101, Data: []byte("entry2"), NotifyChan: nil},
		}, 100, true, false, true)
	mockFragment2 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestROLogFileReadDataWithHoles/1/0/1.frag",
		[]*cache.BufferEntry{
			{EntryId: 102, Data: []byte("entry3"), NotifyChan: nil},
			{EntryId: 103, Data: []byte("entry4"), NotifyChan: nil},
		}, 102, true, false, true)
	//mockFragment3 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 3, "TestROLogFileReadDataWithHoles/1/0/2.frag",
	//	[][]byte{[]byte("entry4"), []byte("entry5")}, 104, true, false, true)
	mockFragment4 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 4, "TestROLogFileReadDataWithHoles/1/0/3.frag",
		[]*cache.BufferEntry{
			{EntryId: 106, Data: []byte("entry6"), NotifyChan: nil},
			{EntryId: 107, Data: []byte("entry7"), NotifyChan: nil},
		}, 106, true, false, true)

	mockMergedFragment0 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 1, "TestROLogFileReadDataWithHoles/1/0/m_0.frag",
		[]*cache.BufferEntry{
			{EntryId: 100, Data: []byte("entry1"), NotifyChan: nil},
			{EntryId: 101, Data: []byte("entry2"), NotifyChan: nil},
		}, 100, true, false, true)
	mockMergedFragment1 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 2, "TestROLogFileReadDataWithHoles/1/0/m_1.frag",
		[]*cache.BufferEntry{
			{EntryId: 102, Data: []byte("entry3"), NotifyChan: nil},
			{EntryId: 103, Data: []byte("entry4"), NotifyChan: nil},
		}, 102, true, false, true)
	//mockMergedFragment2 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 3, "TestROLogFileReadDataWithHoles/1/0/m_2.frag",
	//	[][]byte{[]byte("entry4"), []byte("entry5")}, 104, true, false, true)
	mockMergedFragment3 := NewFragmentObject(context.TODO(), client, "test-bucket", 1, 0, 4, "TestROLogFileReadDataWithHoles/1/0/m_3.frag",
		[]*cache.BufferEntry{
			{EntryId: 106, Data: []byte("entry6"), NotifyChan: nil},
			{EntryId: 107, Data: []byte("entry7"), NotifyChan: nil},
		}, 106, true, false, true)

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
		assert.Equal(t, int64(2), frag.fragmentId)
	})

	t.Run("Find first Fragment", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(50)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), frag.fragmentId)
	})

	t.Run("Find last Fragment", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(250)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), frag.fragmentId)
	})

	t.Run("entryId after the last Fragment", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(300)
		assert.NoError(t, err)
		assert.Nil(t, frag)
	})

	t.Run("First Fragment boundary values", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(0)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), frag.fragmentId)

		frag, err = roSegmentImpl.findFragment(99)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), frag.fragmentId)
	})
	t.Run("Second Fragment boundary values", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(100)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), frag.fragmentId)

		frag, err = roSegmentImpl.findFragment(199)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), frag.fragmentId)
	})

	t.Run("Last Fragment boundary values", func(t *testing.T) {
		frag, err := roSegmentImpl.findFragment(200)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), frag.fragmentId)

		frag, err = roSegmentImpl.findFragment(299)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), frag.fragmentId)
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
		assert.Equal(t, int64(1), frag.fragmentId)
	})
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
	assert.Equal(t, 1, len(partitions[0]))
	assert.Equal(t, int64(0), partitionFirstEntryIds[0])

	// Second partition should have remaining entries
	assert.Equal(t, 2, len(partitions[1]))
	assert.Equal(t, int64(1), partitionFirstEntryIds[1])
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
	assert.Equal(t, 4, len(partitions))
	assert.Equal(t, 4, len(partitionFirstEntryIds))

	// First partition: 60 bytes
	assert.Equal(t, 1, len(partitions[0]))
	assert.Equal(t, int64(50), partitionFirstEntryIds[0])

	// Second partition: 50 + 30 = 80 bytes
	assert.Equal(t, 2, len(partitions[1]))
	assert.Equal(t, int64(51), partitionFirstEntryIds[1])

	// Third partition: 40 bytes
	assert.Equal(t, 1, len(partitions[2]))
	assert.Equal(t, int64(53), partitionFirstEntryIds[2])

	// Fourth partition: 80 bytes
	assert.Equal(t, 1, len(partitions[3]))
	assert.Equal(t, int64(54), partitionFirstEntryIds[3])
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
		{EntryId: 1004, Data: make([]byte, 25), NotifyChan: nil}, // 25 bytes, total 95
		{EntryId: 1005, Data: make([]byte, 10), NotifyChan: nil}, // 10 bytes, total 105 > 100
	}
	toFlushDataFirstEntryId := int64(1000)

	partitions, partitionFirstEntryIds, _ := segment.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)

	// Should return three partitions
	assert.Equal(t, 3, len(partitions))
	assert.Equal(t, 3, len(partitionFirstEntryIds))

	// First partition: 30 + 40 = 70 bytes
	assert.Equal(t, 2, len(partitions[0]))
	assert.Equal(t, 30, len(partitions[0][0].Data))
	assert.Equal(t, 40, len(partitions[0][1].Data))
	assert.Equal(t, int64(1000), partitionFirstEntryIds[0])

	// Second partition: 50 + 20 + 25 = 95 bytes
	assert.Equal(t, 3, len(partitions[1]))
	assert.Equal(t, 50, len(partitions[1][0].Data))
	assert.Equal(t, 20, len(partitions[1][1].Data))
	assert.Equal(t, 25, len(partitions[1][2].Data))
	assert.Equal(t, int64(1002), partitionFirstEntryIds[1])

	// Third partition: 10 bytes
	assert.Equal(t, 1, len(partitions[2]))
	assert.Equal(t, 10, len(partitions[2][0].Data))
	assert.Equal(t, int64(1005), partitionFirstEntryIds[2])
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
		segment.waitIfFlushingBufferSizeExceeded(ctx)
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
			segment.waitIfFlushingBufferSizeExceeded(ctx)
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
			segment.waitIfFlushingBufferSizeExceeded(ctx)
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
			segment.waitIfFlushingBufferSizeExceeded(ctx)
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
