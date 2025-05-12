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

// TestNewLogFile tests the NewLogFile function.
func TestNewLogFile(t *testing.T) {
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

	logFile := NewLogFile(1, "test-segment", "test-bucket", client, cfg).(*LogFile)
	assert.Equal(t, int64(1), logFile.id)
	assert.Equal(t, "test-segment", logFile.segmentPrefixKey)
	assert.Equal(t, "test-bucket", logFile.bucket)
	assert.Equal(t, int64(1000), logFile.buffer.Load().MaxSize)
	assert.Equal(t, int64(1024*1024), logFile.maxBufferSize)
	assert.Equal(t, 1000, logFile.maxIntervalMs)
}

// TestNewROLogFile tests the NewROLogFile function.
func TestNewROLogFile(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
	logFile := NewROLogFile(1, "test-segment", "test-bucket", client).(*ROLogFile)

	assert.Equal(t, int64(1), logFile.id)
	assert.Equal(t, "test-segment", logFile.segmentPrefixKey)
	assert.Equal(t, "test-bucket", logFile.bucket)
	assert.Empty(t, logFile.fragments)
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

	logFile := NewLogFile(1, "TestAppendAsyncReachBufferSize", "test-bucket", client, cfg).(*LogFile)

	// Test out of order
	incomeEntryIds := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := logFile.AppendAsync(context.Background(), entryId, []byte("test_data"))
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

	flushedFirstId := logFile.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := logFile.GetLastEntryId()
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

	logFile := NewLogFile(1, "TestAppendAsyncSomeAndWaitForFlush", "test-bucket", client, cfg).(*LogFile)

	// Test out of order
	incomeEntryIds := []int64{0, 1, 2, 3, 4, 5, 6}
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := logFile.AppendAsync(context.Background(), entryId, []byte("test_data"))
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

	flushedFirstId := logFile.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := logFile.GetLastEntryId()
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

	logFile := NewLogFile(1, "TestAppendAsyncOnceAndWaitForFlush", "test-bucket", client, cfg).(*LogFile)

	// Test out of order
	incomeEntryIds := []int64{0}
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := logFile.AppendAsync(context.Background(), entryId, []byte("test_data"))
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

	flushedFirstId := logFile.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := logFile.GetLastEntryId()
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

	logFile := NewLogFile(1, "TestAppendAsyncNoneAndWaitForFlush", "test-bucket", client, cfg).(*LogFile)

	// wait for flush interval
	<-time.After(2 * time.Second)

	// check there is no data
	flushedFirstId := logFile.getFirstEntryId()
	assert.Equal(t, int64(-1), flushedFirstId)
	flushedLastId, err := logFile.GetLastEntryId()
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

	logFile := NewLogFile(1, "TestAppendAsyncWithHolesAndWaitForFlush", "test-bucket", client, cfg).(*LogFile)

	// Test out of order
	incomeEntryIds := []int64{1, 3, 4, 8, 9}
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := logFile.AppendAsync(context.Background(), entryId, []byte("test_data"))
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

	flushedFirstId := logFile.getFirstEntryId()
	assert.Equal(t, int64(-1), flushedFirstId)
	flushedLastId, err := logFile.GetLastEntryId()
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

	logFile := NewLogFile(1, "TestAppendAsyncWithHolesButFillFinally", "test-bucket", client, cfg).(*LogFile)

	// Test out of order
	incomeEntryIds := []int64{1, 3, 4, 8, 9}
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := logFile.AppendAsync(context.Background(), entryId, []byte("test_data"))
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

	flushedFirstId := logFile.getFirstEntryId()
	assert.Equal(t, int64(-1), flushedFirstId)
	flushedLastId, err := logFile.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), flushedLastId)

	// fill finally
	newIncomeEntryIds := []int64{0, 2, 5, 6, 7} // rest of the entries
	for _, entryId := range newIncomeEntryIds {
		_, ch, err := logFile.AppendAsync(context.Background(), entryId, []byte("test_data"))
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

	flushedFirstId = logFile.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err = logFile.GetLastEntryId()
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

	logFile := NewLogFile(1, "TestAppendAsyncDisorderWithinBounds", "test-bucket", client, cfg).(*LogFile)

	// Test out of order
	incomeEntryIds := []int64{1, 0, 6, 8, 9, 7, 2, 3, 4, 5}
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		_, ch, err := logFile.AppendAsync(context.Background(), entryId, []byte("test_data"))
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

	flushedFirstId := logFile.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := logFile.GetLastEntryId()
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

	logFile := NewLogFile(1, "TestAppendAsyncDisorderAndPartialOutOfBounds", "test-bucket", client, cfg).(*LogFile)

	// Test out of order
	incomeEntryIds := []int64{1, 0, 6, 11, 12, 10, 7, 2, 3, 4, 5} // 0-7,10-12
	chList := make([]<-chan int64, 0)
	for _, entryId := range incomeEntryIds {
		assignId, ch, err := logFile.AppendAsync(context.Background(), entryId, []byte("test_data"))
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

	flushedFirstId := logFile.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := logFile.GetLastEntryId()
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
		logFile := NewLogFile(1, "TestAppendAsyncReachBufferDataSize1", "test-bucket", client, cfg).(*LogFile)
		assignId0, ch0, err := logFile.AppendAsync(context.Background(), 0, data800)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		assignId1, ch1, err := logFile.AppendAsync(context.Background(), 1, data200)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		// reach the max size, should be immediate flush
		// show check response immediately
		start := time.Now()
		assert.Equal(t, int64(0), <-ch0)
		assert.Equal(t, int64(1), <-ch1)
		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost < 100, fmt.Sprintf("should be immediate flush, but cost %d ms", cost))
		assert.Equal(t, int64(0), logFile.getFirstEntryId())
		flushedLastId, err := logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), flushedLastId)
	}

	// test async append 200 + 300, wait for flush
	{
		logFile := NewLogFile(1, "TestAppendAsyncReachBufferDataSize2", "test-bucket", client, cfg).(*LogFile)
		assignId0, ch0, err := logFile.AppendAsync(context.Background(), 0, data200)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		assignId1, ch1, err := logFile.AppendAsync(context.Background(), 1, data300)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		// wait for flush
		start := time.Now()
		assert.Equal(t, int64(0), <-ch0)
		assert.Equal(t, int64(1), <-ch1)
		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost >= 1000, fmt.Sprintf("should wait 1000 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), logFile.getFirstEntryId())
		flushedLastId, err := logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), flushedLastId)
	}

	// test async append 200 + 300, wait for flush
	{
		logFile := NewLogFile(1, "TestAppendAsyncReachBufferDataSize3", "test-bucket", client, cfg).(*LogFile)
		assignId0, ch0, err := logFile.AppendAsync(context.Background(), 0, data200)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		assignId1, ch1, err := logFile.AppendAsync(context.Background(), 1, data300)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		// wait for flush
		start := time.Now()
		assert.Equal(t, int64(0), <-ch0)
		assert.Equal(t, int64(1), <-ch1)
		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost >= 1000, fmt.Sprintf("should wait 1000 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), logFile.getFirstEntryId())
		flushedLastId, err := logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), flushedLastId)
	}

	// test async append 1k + 200,
	// the first append 1k should be immediately flush,
	// the second append 200 should be wait for flush.
	{
		logFile := NewLogFile(1, "TestAppendAsyncReachBufferDataSize4", "test-bucket", client, cfg).(*LogFile)
		assignId0, ch0, err := logFile.AppendAsync(context.Background(), 0, data1k)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		// flush immediately
		start := time.Now()
		assert.Equal(t, int64(0), <-ch0)
		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost < 100, fmt.Sprintf("should wait 1000 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), logFile.getFirstEntryId())
		flushedLastId, err := logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), flushedLastId)

		assignId1, ch1, err := logFile.AppendAsync(context.Background(), 1, data200)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		// wait for flush
		start = time.Now()
		assert.Equal(t, int64(1), <-ch1)
		cost = time.Now().Sub(start).Milliseconds()
		assert.True(t, cost >= 1000, fmt.Sprintf("should wait 1000 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), logFile.getFirstEntryId())
		flushedLastId, err = logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), flushedLastId)
	}

	// test append 1(800),3(300), trigger flush 1(800). then append 2(200), wait for flush  2(200) + 3(300)
	{
		logFile := NewLogFile(1, "TestAppendAsyncReachBufferDataSize5", "test-bucket", client, cfg).(*LogFile)
		assignId0, ch0, err := logFile.AppendAsync(context.Background(), 0, data800)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		assignId2, ch2, err := logFile.AppendAsync(context.Background(), 2, data300)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), assignId2)
		// flush 1(800) immediately
		start := time.Now()
		assert.Equal(t, int64(0), <-ch0)
		cost := time.Now().Sub(start).Milliseconds()
		assert.True(t, cost < 100, fmt.Sprintf("should wait 1000 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), logFile.getFirstEntryId())
		flushedLastId, err := logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), flushedLastId)

		assignId1, ch1, err := logFile.AppendAsync(context.Background(), 1, data200)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)

		// wait for flush
		start = time.Now()
		assert.Equal(t, int64(1), <-ch1)
		assert.Equal(t, int64(2), <-ch2)
		cost = time.Now().Sub(start).Milliseconds()
		assert.True(t, cost >= 1000, fmt.Sprintf("should wait 1000 ms to flush, but only cost %d ms", cost))
		assert.Equal(t, int64(0), logFile.getFirstEntryId())
		flushedLastId, err = logFile.GetLastEntryId()
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

	logFile := NewLogFile(1, "TestSync", "test-bucket", client, cfg).(*LogFile)

	// Write some data to buffer
	chList := make([]<-chan int64, 0)
	for i := 0; i < 100; i++ {
		assignId, ch, err := logFile.AppendAsync(context.Background(), int64(i), []byte("test_data"))
		assert.Equal(t, int64(i), assignId)
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}

	err := logFile.Sync(context.Background())
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

	flushedFirstId := logFile.getFirstEntryId()
	assert.Equal(t, int64(0), flushedFirstId)
	flushedLastId, err := logFile.GetLastEntryId()
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

	logFile := NewLogFile(1, "TestClose", "test-bucket", client, cfg).(*LogFile)

	// Write some data to buffer
	chList := make([]<-chan int64, 0)
	for i := 0; i < 100; i++ {
		_, ch, err := logFile.AppendAsync(context.Background(), int64(i), []byte("test_data"))
		assert.NoError(t, err)
		assert.NotNil(t, ch)
		chList = append(chList, ch)
	}
	assert.Equal(t, 100, len(chList))

	// Close log file
	err := logFile.Close()
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

	logFile := NewLogFile(1, "TestGetId", "test-bucket", client, cfg).(*LogFile)
	assert.Equal(t, int64(1), logFile.GetId())
}

// TestMerge tests the Merge function.
func TestMerge(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))

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

	logFile := NewLogFile(1, "TestMerge", "test-bucket", client, cfg).(*LogFile)

	// write data and flush fragment 1
	for i := 0; i < 100; i++ {
		_, _, err := logFile.AppendAsync(context.Background(), int64(i), []byte("test_data"))
		assert.NoError(t, err)
	}
	err := logFile.Sync(context.Background())
	assert.NoError(t, err)

	// write data and flush fragment 2
	for i := 100; i < 200; i++ {
		_, _, err := logFile.AppendAsync(context.Background(), int64(i), []byte("test_data"))
		assert.NoError(t, err)
	}
	err = logFile.Sync(context.Background())
	assert.NoError(t, err)

	// Merge fragments
	roLogFile := NewROLogFile(1, "TestMerge", "test-bucket", client).(*ROLogFile)
	mergedFrags, entryOffset, fragmentIdOffset, err := roLogFile.Merge(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mergedFrags))
	assert.Equal(t, []int32{0}, entryOffset)
	assert.Equal(t, []int32{1}, fragmentIdOffset)
	firstEntryId, err := mergedFrags[0].GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), firstEntryId)
	lastEntryId, err := mergedFrags[0].GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, int64(199), lastEntryId)
}

// TestLoad tests the Load function.
func TestLoad(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
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

	logFile := NewLogFile(1, "TestLoad", "test-bucket", client, cfg).(*LogFile)

	// Write some data to buffer and sync
	for i := 0; i < 100; i++ {
		_, _, err := logFile.AppendAsync(context.Background(), int64(i), []byte("test_data"))
		assert.NoError(t, err)
	}

	err := logFile.Sync(context.Background())
	assert.NoError(t, err)

	// Load data
	roLogFile := NewROLogFile(1, "TestLoad", "test-bucket", client).(*ROLogFile)
	totalSize, lastFragment, err := roLogFile.Load(context.Background())
	assert.NoError(t, err)
	assert.True(t, int64(100*len("test_data")) < totalSize)
	assert.NotNil(t, lastFragment)
	firstEntryId, err := lastFragment.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), firstEntryId)
	lastEntryId, err := lastFragment.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, int64(99), lastEntryId)
}

// TestNewReader tests the NewReader function.
func TestNewReaderInWriterLogFile(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	// when read frag 2, return it is not exists
	client.EXPECT().StatObject(mock.Anything, "test-bucket", "TestNewReaderInWriterLogFile/1/2.frag", mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))
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

	logFile := NewLogFile(1, "TestNewReaderInWriterLogFile", "test-bucket", client, cfg).(*LogFile)

	// Write some data to buffer and sync
	for i := 0; i < 100; i++ {
		_, _, err := logFile.AppendAsync(context.Background(), int64(i), []byte("test_data"))
		assert.NoError(t, err)
	}
	err := logFile.Sync(context.Background())
	assert.NoError(t, err)

	// Create a reader for [0, 100)
	roLogFile := NewROLogFile(1, "TestNewReaderInWriterLogFile", "test-bucket", client).(*ROLogFile)
	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   100,
	})
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	// Read data
	for i := 0; i < 100; i++ {
		hasNext := reader.HasNext()
		assert.True(t, hasNext)

		entry, err := reader.ReadNext()
		assert.NoError(t, err)
		assert.Equal(t, int64(i), entry.EntryId)
		assert.Equal(t, []byte("test_data"), entry.Values)
	}

	hasNext := reader.HasNext()
	assert.False(t, hasNext)
}

func TestNewReaderInROLogFile(t *testing.T) {
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

	logFile := NewLogFile(1, "TestNewReaderInROLogFile", "test-bucket", client, cfg).(*LogFile)
	for i := 0; i < 100; i++ {
		_, _, err := logFile.AppendAsync(context.Background(), int64(i), []byte("test_data"))
		assert.NoError(t, err)
	}
	err := logFile.Sync(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, int(logFile.lastFragmentId))

	// Get 1.frag from cache, show no need to mock 1.frag
	// mock read 1.frag data
	//client.EXPECT().GetObjectDataAndInfo(mock.Anything, "test-bucket", "TestNewReaderInROLogFile/1/1.frag", mock.Anything).Return(bytes.NewReader(mockData), int64(len(mockData)), nil)
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", "TestNewReaderInROLogFile/1/1.frag", mock.Anything).Return(minio.ObjectInfo{}, nil)

	// mock 2.fra does not exists
	client.EXPECT().StatObject(mock.Anything, "test-bucket", "TestNewReaderInROLogFile/1/2.frag", mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))

	// Create a reader for [0, 100)
	roLogFile := NewROLogFile(1, "TestNewReaderInROLogFile", "test-bucket", client).(*ROLogFile)
	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   100,
	})
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	// Read data
	for i := 0; i < 100; i++ {
		hasNext := reader.HasNext()
		assert.True(t, hasNext)

		entry, err := reader.ReadNext()
		assert.NoError(t, err)
		assert.Equal(t, int64(i), entry.EntryId)
		assert.Equal(t, []byte("test_data"), entry.Values)
	}

	hasNext := reader.HasNext()
	assert.False(t, hasNext)
}

func TestROLogFileReadDataWithHoles(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	// buffer split to 3 partitions, concurrently flush 1,2,3 frags
	// 1.frag put success
	client.EXPECT().PutObject(mock.Anything, "test-bucket", "TestROLogFileReadDataWithHoles/1/1.frag", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	// 2.frag put failed
	client.EXPECT().PutObject(mock.Anything, "test-bucket", "TestROLogFileReadDataWithHoles/1/2.frag", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, errors.New("put failed"))
	// 3.frag put success
	client.EXPECT().PutObject(mock.Anything, "test-bucket", "TestROLogFileReadDataWithHoles/1/3.frag", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
	// because read 1.frag from cache, no need to mock 1.frag Load related method calls, like GetObjectDataAndInfo/StateObject
	//mock 1.frag exists
	//client.EXPECT().StatObject(mock.Anything, "test-bucket", "TestROLogFileReadDataWithHoles/1/1.frag", mock.Anything).Return(minio.ObjectInfo{}, nil)
	// mock 2.frag not exists
	client.EXPECT().StatObject(mock.Anything, "test-bucket", "TestROLogFileReadDataWithHoles/1/2.frag", mock.Anything).Return(minio.ObjectInfo{}, errors.New("error"))

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

	data1000 := make([]byte, 1000)
	// test write data with holes to minio, (fragment 1,x,3)
	{ // write 3 frag
		logFile := NewLogFile(1, "TestROLogFileReadDataWithHoles", "test-bucket", client, cfg).(*LogFile)
		assignId0, ch0, err := logFile.AppendAsync(context.Background(), 0, data1000)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), assignId0)
		assignId1, ch1, err := logFile.AppendAsync(context.Background(), 1, data1000)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), assignId1)
		assignId2, ch2, err := logFile.AppendAsync(context.Background(), 2, data1000)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), assignId2)

		// entry 0 success
		assert.Equal(t, int64(0), <-ch0)

		// entry 1 fail
		select {
		case syncedId := <-ch1:
			assert.Equal(t, int64(-1), syncedId)
		case <-time.After(2 * time.Second):
			te := errors.New("time out")
			assert.Error(t, te)
		}
		// entry 2 fail
		select {
		case syncedId := <-ch2:
			assert.Equal(t, int64(-1), syncedId)
		case <-time.After(2 * time.Second):
			te := errors.New("time out")
			assert.Error(t, te)
		}

		// assert there 1 fragment created
		assert.Equal(t, 1, int(logFile.lastFragmentId))
		assert.Equal(t, int64(0), logFile.getFirstEntryId())
		flushedLastId, err := logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), flushedLastId)

		// because read 1.frag from cache, no need to mock 1.frag Load related method calls, like GetObjectDataAndInfo/StateObject
		//frag1Data, err := serializeFragment(logFile.fragments[0])
		//assert.NoError(t, err)
		// fragment 1 data
		//client.EXPECT().GetObjectDataAndInfo(mock.Anything, "test-bucket", "TestROLogFileReadDataWithHoles/1/1.frag", mock.Anything).Return(bytes.NewReader(frag1Data), int64(len(frag1Data)), nil)
	}

	// test read data with holes (fragment 1,x,3) in minio
	// we should only read data in fragment 1
	{
		roLogFile := NewROLogFile(1, "TestROLogFileReadDataWithHoles", "test-bucket", client).(*ROLogFile)
		reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
			StartSequenceNum: 0,
			EndSequenceNum:   3,
		})
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		// read data from frag 1 success
		hasNext := reader.HasNext()
		assert.True(t, hasNext)
		entry, err := reader.ReadNext()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), entry.EntryId)
		assert.Equal(t, data1000, entry.Values)
		// read data from frag 2 fail, no more data
		hasNext = reader.HasNext()
		assert.False(t, hasNext)
	}
}

func TestFindFragment(t *testing.T) {
	// Create mock Fragment object list
	mockFragments := []*FragmentObject{
		{fragmentId: 1, firstEntryId: 0, lastEntryId: 99, infoFetched: true},    // [0,99]
		{fragmentId: 2, firstEntryId: 100, lastEntryId: 199, infoFetched: true}, // [100,199]
		{fragmentId: 3, firstEntryId: 200, lastEntryId: 299, infoFetched: true}, // [200,299]
	}

	// Create LogFile instance and inject mock data
	logFile := &ROLogFile{
		fragments: mockFragments,
	}

	t.Run("Find Fragment in middle position", func(t *testing.T) {
		frag, err := logFile.findFragment(150)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), frag.fragmentId)
	})

	t.Run("Find first Fragment", func(t *testing.T) {
		frag, err := logFile.findFragment(50)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), frag.fragmentId)
	})

	t.Run("Find last Fragment", func(t *testing.T) {
		frag, err := logFile.findFragment(250)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), frag.fragmentId)
	})

	t.Run("entryId after the last Fragment", func(t *testing.T) {
		frag, err := logFile.findFragment(300)
		assert.NoError(t, err)
		assert.Nil(t, frag)
	})

	t.Run("First Fragment boundary values", func(t *testing.T) {
		frag, err := logFile.findFragment(0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), frag.fragmentId)

		frag, err = logFile.findFragment(99)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), frag.fragmentId)
	})
	t.Run("Second Fragment boundary values", func(t *testing.T) {
		frag, err := logFile.findFragment(100)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), frag.fragmentId)

		frag, err = logFile.findFragment(199)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), frag.fragmentId)
	})

	t.Run("Last Fragment boundary values", func(t *testing.T) {
		frag, err := logFile.findFragment(200)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), frag.fragmentId)

		frag, err = logFile.findFragment(299)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), frag.fragmentId)
	})

	t.Run("Return leftmost match when multiple candidate Fragments", func(t *testing.T) {
		// Mock overlapping Fragments
		overlappingFrags := []*FragmentObject{
			{fragmentId: 1, firstEntryId: 0, lastEntryId: 200, infoFetched: true},
			{fragmentId: 2, firstEntryId: 100, lastEntryId: 300, infoFetched: true},
		}
		newLogFile := &ROLogFile{
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
		//cfg := &config.Configuration{
		//	Woodpecker: config.WoodpeckerConfig{
		//		Logstore: config.LogstoreConfig{
		//			LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
		//				MaxEntries:      10,
		//				MaxBytes:        1024 * 1024,
		//				MaxInterval:     1000,
		//				MaxFlushThreads: 5,
		//				MaxFlushSize:    1024 * 1024,
		//				MaxFlushRetries: 3,
		//				RetryInterval:   100,
		//			},
		//		},
		//	},
		//}

		// Create a list of mock objects to be returned by ListObjects
		objectCh := make(chan minio.ObjectInfo, 3)
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/fragment_1.frag", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/fragment_2.frag", Size: 2048}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/m_1.frag", Size: 4096}
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/", false, mock.Anything).Return(objectCh)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/fragment_1.frag", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/fragment_2.frag", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/m_1.frag", mock.Anything).Return(nil)
		client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
		// No need for StatObject call anymore

		// Create the LogFile
		logFile := NewROLogFile(1, "test-segment", "test-bucket", client).(*ROLogFile)

		// Add some fragments to the LogFile to verify they're cleared
		logFile.fragments = []*FragmentObject{
			{fragmentId: 1, firstEntryId: 0, lastEntryId: 9},
			{fragmentId: 2, firstEntryId: 10, lastEntryId: 19},
		}

		// Call DeleteFragments
		err := logFile.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err)

		// Verify internal state is reset
		assert.Empty(t, logFile.fragments, "Fragments slice should be empty")
	})

	t.Run("ListObjectsError", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		//cfg := &config.Configuration{
		//	Woodpecker: config.WoodpeckerConfig{
		//		Logstore: config.LogstoreConfig{
		//			LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
		//				MaxEntries:      10,
		//				MaxBytes:        1024 * 1024,
		//				MaxInterval:     1000,
		//				MaxFlushThreads: 5,
		//				MaxFlushSize:    1024 * 1024,
		//				MaxFlushRetries: 3,
		//				RetryInterval:   100,
		//			},
		//		},
		//	},
		//}

		// Create an object channel with an error
		errorObjectCh := make(chan minio.ObjectInfo, 1)
		errorObjectCh <- minio.ObjectInfo{Err: errors.New("list error")}
		close(errorObjectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/", false, mock.Anything).Return(errorObjectCh)
		client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
		// No need for StatObject call anymore

		// Create the LogFile
		logFile := NewROLogFile(1, "test-segment", "test-bucket", client).(*ROLogFile)

		// Call DeleteFragments
		err := logFile.DeleteFragments(context.Background(), 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to delete")
	})

	t.Run("RemoveObjectError", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		//cfg := &config.Configuration{
		//	Woodpecker: config.WoodpeckerConfig{
		//		Logstore: config.LogstoreConfig{
		//			LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
		//				MaxEntries:      10,
		//				MaxBytes:        1024 * 1024,
		//				MaxInterval:     1000,
		//				MaxFlushThreads: 5,
		//				MaxFlushSize:    1024 * 1024,
		//				MaxFlushRetries: 3,
		//				RetryInterval:   100,
		//			},
		//		},
		//	},
		//}

		// Create a list of mock objects to be returned by ListObjects
		objectCh := make(chan minio.ObjectInfo, 2)
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/fragment_1.frag", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/fragment_2.frag", Size: 2048}
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/", false, mock.Anything).Return(objectCh)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/fragment_1.frag", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/fragment_2.frag", mock.Anything).Return(errors.New("remove error"))
		client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)

		// Create the LogFile
		logFile := NewROLogFile(1, "test-segment", "test-bucket", client).(*ROLogFile)

		// Call DeleteFragments
		err := logFile.DeleteFragments(context.Background(), 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to delete")
	})

	t.Run("NoFragmentsToDelete", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
		//cfg := &config.Configuration{
		//	Woodpecker: config.WoodpeckerConfig{
		//		Logstore: config.LogstoreConfig{
		//			LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
		//				MaxEntries:      10,
		//				MaxBytes:        1024 * 1024,
		//				MaxInterval:     1000,
		//				MaxFlushThreads: 5,
		//				MaxFlushSize:    1024 * 1024,
		//				MaxFlushRetries: 3,
		//				RetryInterval:   100,
		//			},
		//		},
		//	},
		//}

		// Empty object channel
		objectCh := make(chan minio.ObjectInfo)
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/", false, mock.Anything).Return(objectCh)
		// No need for StatObject call anymore

		// Create the LogFile
		logFile := NewROLogFile(1, "test-segment", "test-bucket", client).(*ROLogFile)

		// Call DeleteFragments
		err := logFile.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err)

		// Verify internal state is reset
		assert.Empty(t, logFile.fragments, "Fragments slice should be empty")
	})

	t.Run("SkipNonFragmentFiles", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		//cfg := &config.Configuration{
		//	Woodpecker: config.WoodpeckerConfig{
		//		Logstore: config.LogstoreConfig{
		//			LogFileSyncPolicy: config.LogFileSyncPolicyConfig{
		//				MaxEntries:      10,
		//				MaxBytes:        1024 * 1024,
		//				MaxInterval:     1000,
		//				MaxFlushThreads: 5,
		//				MaxFlushSize:    1024 * 1024,
		//				MaxFlushRetries: 3,
		//				RetryInterval:   100,
		//			},
		//		},
		//	},
		//}

		// Create a list of mock objects including non-fragment files
		objectCh := make(chan minio.ObjectInfo, 3)
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/fragment_1.frag", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/metadata.json", Size: 256} // Not a fragment
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/m_1.frag", Size: 4096}
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/", false, mock.Anything).Return(objectCh)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/fragment_1.frag", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/m_1.frag", mock.Anything).Return(nil)
		client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
		// No call for metadata.json as it should be skipped
		// 不再需要 StatObject 调用

		// Create the LogFile
		logFile := NewROLogFile(1, "test-segment", "test-bucket", client).(*ROLogFile)

		// Call DeleteFragments
		err := logFile.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err)
	})
}
