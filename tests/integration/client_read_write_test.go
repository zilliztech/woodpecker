package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func TestReadTheWrittenDataSequentially(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestReadTheWrittenDataSequentially")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration()
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}

			// CreateLog if not exists
			logName := "test_log" + time.Now().Format("20060102150405")
			client.CreateLog(context.Background(), logName)

			// OpenLog
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			assert.NoError(t, openErr)

			// Test write
			// OpenWriter
			logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, openWriterErr)
			resultChan := make([]<-chan *log.WriteResult, 1000)
			for i := 0; i < 1000; i++ {
				writeResultChan := logWriter.WriteAsync(context.Background(),
					&log.WriterMessage{
						Payload: []byte(fmt.Sprintf("hello world %d", i)),
						Properties: map[string]string{
							"key": fmt.Sprintf("value%d", i),
						},
					},
				)
				resultChan[i] = writeResultChan
			}
			failedIds := make([]*log.WriteResult, 0)
			successIds := make([]*log.LogMessageId, 0)
			for i := 0; i < 1000; i++ {
				writeResult := <-resultChan[i]
				if writeResult.Err != nil {
					failedIds = append(failedIds, writeResult)
				} else {
					successIds = append(successIds, writeResult.LogMessageId)
				}
			}

			assert.Equal(t, 0, len(failedIds))
			assert.Equal(t, 1000, len(successIds))
			for idx, msgId := range successIds {
				if idx == 0 {
					continue
				}
				if msgId.SegmentId == successIds[idx-1].SegmentId {
					assert.True(t, msgId.EntryId == successIds[idx-1].EntryId+1)
				} else {
					assert.True(t, msgId.SegmentId > successIds[idx-1].SegmentId)
				}
			}
			closeErr := logWriter.Close(context.Background())
			assert.NoError(t, closeErr)
			fmt.Println("Test Write finished")

			// Test read all from earliest
			{
				earliest := log.EarliestLogMessageID()
				logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &earliest, "client-earliest-reader")
				assert.NoError(t, openReaderErr)
				readMsgs := make([]*log.LogMessage, 0)
				for i := 0; i < 1000; i++ {
					msg, err := logReader.ReadNext(context.Background())
					assert.NoError(t, err)
					assert.NotNil(t, msg)
					readMsgs = append(readMsgs, msg)
				}
				assert.Equal(t, 1000, len(readMsgs))
				for idx, msg := range readMsgs {
					// The sequence of data read and written is the same
					assert.Equal(t, successIds[idx].SegmentId, msg.Id.SegmentId)
					if idx == 0 {
						continue
					}
					// The order of reading data is incremented one by one
					if msg.Id.SegmentId == readMsgs[idx-1].Id.SegmentId {
						assert.True(t, msg.Id.EntryId == readMsgs[idx-1].Id.EntryId+1)
					} else {
						assert.True(t, msg.Id.SegmentId > readMsgs[idx-1].Id.SegmentId)
					}
				}

				closeReaderErr := logReader.Close(context.Background())
				assert.NoError(t, closeReaderErr)
			}

			// Test read from point
			{
				start := &log.LogMessageId{
					SegmentId: 0,
					EntryId:   50,
				}
				logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start, "client-specific-position-reader")
				assert.NoError(t, openReaderErr)
				readMsgs := make([]*log.LogMessage, 0)
				for i := 0; i < 950; i++ {
					msg, err := logReader.ReadNext(context.Background())
					assert.NoError(t, err)
					assert.NotNil(t, msg)
					readMsgs = append(readMsgs, msg)
				}
				assert.Equal(t, 950, len(readMsgs))
				for idx, msg := range readMsgs {
					// The sequence of data read and written is the same
					assert.Equal(t, successIds[idx+50].SegmentId, msg.Id.SegmentId)
					if idx == 0 {
						continue
					}
					// The order of reading data is incremented one by one
					if msg.Id.SegmentId == readMsgs[idx-1].Id.SegmentId {
						assert.True(t, msg.Id.EntryId == readMsgs[idx-1].Id.EntryId+1)
					} else {
						assert.True(t, msg.Id.SegmentId > readMsgs[idx-1].Id.SegmentId)
					}
				}
				closeReaderErr := logReader.Close(context.Background())
				assert.NoError(t, closeReaderErr)
			}

			// Test read from latest
			{
				latest := log.LatestLogMessageID()
				logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &latest, "client-latest-reader")
				assert.NoError(t, openReaderErr)
				readOne := false
				go func() {
					_, err := logReader.ReadNext(context.Background())
					if err == nil {
						readOne = true
					}
				}()
				time.Sleep(2 * time.Second)
				assert.False(t, readOne)
				closeReaderErr := logReader.Close(context.Background())
				assert.NoError(t, closeReaderErr)
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Println("Test Read finished")
		})
	}
}

func TestReadWriteLoop(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestReadWriteLoop")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration()
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}
			// CreateLog if not exists
			logName := "test_log" + time.Now().Format("20060102150405")
			client.CreateLog(context.Background(), logName)

			// write/read loop test
			for i := 0; i < 3; i++ {
				// test write
				writtenIds := testWrite(t, client, logName, 1000)
				// test read
				testRead(t, client, logName, writtenIds, 1000)
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Println("Test Read finished")
		})
	}
}

func testWrite(t *testing.T, client woodpecker.Client, logName string, count int) []*log.LogMessageId {
	// OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), logName)
	assert.NoError(t, openErr)

	// Test write
	// OpenWriter
	logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, openWriterErr)
	resultChan := make([]<-chan *log.WriteResult, count)
	for i := 0; i < count; i++ {
		writeResultChan := logWriter.WriteAsync(context.Background(),
			&log.WriterMessage{
				Payload: []byte(fmt.Sprintf("hello world %d", i)),
				Properties: map[string]string{
					"key": fmt.Sprintf("value%d", i),
				},
			},
		)
		resultChan[i] = writeResultChan
	}
	failedIds := make([]*log.WriteResult, 0)
	successIds := make([]*log.LogMessageId, 0)
	for i := 0; i < count; i++ {
		writeResult := <-resultChan[i]
		if writeResult.Err != nil {
			failedIds = append(failedIds, writeResult)
		} else {
			successIds = append(successIds, writeResult.LogMessageId)
		}
	}

	assert.Equal(t, 0, len(failedIds))
	assert.Equal(t, count, len(successIds))
	for idx, msgId := range successIds {
		if idx == 0 {
			continue
		}
		if msgId.SegmentId == successIds[idx-1].SegmentId {
			assert.True(t, msgId.EntryId == successIds[idx-1].EntryId+1)
		} else {
			assert.True(t, msgId.SegmentId > successIds[idx-1].SegmentId)
		}
	}
	closeErr := logWriter.Close(context.Background())
	assert.NoError(t, closeErr)
	fmt.Printf("Test Write finished %v,%v \n", successIds[0], successIds[count-1])
	return successIds
}

func testRead(t *testing.T, client woodpecker.Client, logName string, writtenIds []*log.LogMessageId, count int) {
	// OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), logName)
	assert.NoError(t, openErr)
	// Test read all from earliest
	from := writtenIds[0]
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), from, "client-from-position-reader")
	assert.NoError(t, openReaderErr)
	readMsgs := make([]*log.LogMessage, 0)
	for i := 0; i < count; i++ {
		msg, err := logReader.ReadNext(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, msg)
		readMsgs = append(readMsgs, msg)
	}
	assert.Equal(t, count, len(readMsgs))
	for idx, msg := range readMsgs {
		// The sequence of data read and written is the same
		assert.Equal(t, writtenIds[idx].SegmentId, msg.Id.SegmentId)
		if idx == 0 {
			continue
		}
		// The order of reading data is incremented one by one
		if msg.Id.SegmentId == readMsgs[idx-1].Id.SegmentId {
			assert.True(t, msg.Id.EntryId == readMsgs[idx-1].Id.EntryId+1)
		} else {
			assert.True(t, msg.Id.SegmentId > readMsgs[idx-1].Id.SegmentId)
		}
	}

	closeReaderErr := logReader.Close(context.Background())
	assert.NoError(t, closeReaderErr)
	fmt.Printf("Test Read finished %v,%v \n", readMsgs[0].Id, readMsgs[count-1].Id)
}

func TestMultiAppendSyncLoop(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMultiAppendSyncLoop")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration()
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}
			// CreateLog if not exists
			logName := "test_log" + time.Now().Format("20060102150405")
			client.CreateLog(context.Background(), logName)

			// sync write loop test
			count := 1_00
			for i := 0; i < 3; i++ {
				// test write
				writtenIds := testMultiAppendSync(t, client, logName, count)
				assert.Equal(t, count, len(writtenIds))
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Println("Test write finished")
		})
	}
}

func testMultiAppendSync(t *testing.T, client woodpecker.Client, logName string, count int) []*log.LogMessageId {
	// OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), logName)
	assert.NoError(t, openErr)

	// Test write
	// OpenWriter
	logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, openWriterErr)

	concurrency := make(chan int, 5)
	wg := sync.WaitGroup{}
	resultList := make([]*log.WriteResult, count)
	for i := 0; i < count; i++ {
		concurrency <- i
		wg.Add(1)
		go func(no int) {
			writeResult := logWriter.Write(context.Background(),
				&log.WriterMessage{
					Payload:    []byte(fmt.Sprintf("hello world %d", i)),
					Properties: map[string]string{"key": fmt.Sprintf("value%d", i)},
				},
			)
			//resultList = append(resultList, writeResult)
			resultList[no] = writeResult
			if writeResult.Err != nil {
				fmt.Printf("write failed %v \n", writeResult.Err)
			} else {
				fmt.Printf("write %d %d success \n", writeResult.LogMessageId.SegmentId, writeResult.LogMessageId.EntryId)
			}
			<-concurrency
			wg.Done()
		}(i)
	}
	wg.Wait()
	failedIds := make([]*log.WriteResult, 0)
	successIds := make([]*log.LogMessageId, 0)
	for _, wr := range resultList {
		if wr.Err != nil {
			failedIds = append(failedIds, wr)
		} else {
			successIds = append(successIds, wr.LogMessageId)
		}
	}

	assert.Equal(t, 0, len(failedIds))
	assert.Equal(t, count, len(successIds))
	if count != len(successIds) {
		fmt.Printf("unexpected success num")
	}
	fmt.Printf("success: ")
	for _, msgId := range successIds {
		fmt.Printf("%d:%d ,", msgId.SegmentId, msgId.EntryId)
	}
	fmt.Printf("\n")
	sort.Slice(successIds, func(i, j int) bool {
		if successIds[i].SegmentId < successIds[j].SegmentId {
			return true
		} else if successIds[i].SegmentId > successIds[j].SegmentId {
			return false
		} else {
			return successIds[i].EntryId < successIds[j].EntryId
		}
	})
	fmt.Printf("success order: ")
	for _, msgId := range successIds {
		fmt.Printf("%d:%d ,", msgId.SegmentId, msgId.EntryId)
	}
	fmt.Printf("\n")
	fmt.Printf("successIds:%v \n", successIds)
	for idx, msgId := range successIds {
		if idx == 0 {
			continue
		}
		if msgId.SegmentId == successIds[idx-1].SegmentId {
			if msgId.EntryId != successIds[idx-1].EntryId+1 {
				fmt.Printf("idx:%d,msgId:%v,successIds:%v \n", idx, msgId, successIds)
			}
			assert.Equal(t, msgId.EntryId, successIds[idx-1].EntryId+1, fmt.Sprintf("expected:%d actual:%d ", msgId.EntryId, successIds[idx-1].EntryId+1))
		} else {
			assert.True(t, msgId.SegmentId > successIds[idx-1].SegmentId, fmt.Sprintf("expected:%d actual:%d ", msgId.SegmentId, successIds[idx-1].SegmentId))
		}
	}
	closeErr := logWriter.Close(context.Background())
	assert.NoError(t, closeErr)
	return successIds
}

func TestTailReadBlockingBehavior(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestTailReadBlockingBehavior")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration()
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// CreateLog if not exists
			logName := "test_log_tail_read_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open the log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// First open a reader starting from the "latest" position
			latest := log.LatestLogMessageID()
			logReader, err := logHandle.OpenLogReader(context.Background(), &latest, "client-latest-reader-sync")
			assert.NoError(t, err)

			totalMessages := 20
			readMessages := make([]*log.LogMessage, 0, totalMessages)
			readCh := make(chan *log.LogMessage, totalMessages)
			readErrorCh := make(chan error, 1)
			readCompleteCh := make(chan struct{})

			// Start a goroutine to continuously read messages
			go func() {
				for {
					msg, err := logReader.ReadNext(context.Background())
					if err != nil {
						readErrorCh <- err
						return
					}

					readCh <- msg
					readMessages = append(readMessages, msg)
					fmt.Printf("Read message seg:%d entry:%d payload:%v props:%v\n", msg.Id.SegmentId, msg.Id.EntryId, msg.Payload, msg.Properties)

					// If we've read enough messages, signal completion
					if len(readMessages) == totalMessages {
						close(readCompleteCh)
						break
					}
				}
			}()

			// Now open a writer and write messages at intervals
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Write messages in batches with delays
			writeMessages := make([]*log.LogMessageId, 0, totalMessages)
			batchSize := 5
			numBatches := totalMessages / batchSize

			for batch := 0; batch < numBatches; batch++ {
				// Write a batch of messages
				for i := 0; i < batchSize; i++ {
					idx := batch*batchSize + i
					result := logWriter.Write(context.Background(), &log.WriterMessage{
						Payload: []byte(fmt.Sprintf("message %d", idx)),
						Properties: map[string]string{
							"k1": fmt.Sprintf("%d", batch),
							"k2": fmt.Sprintf("%d", i),
						},
					})

					assert.NoError(t, result.Err)
					writeMessages = append(writeMessages, result.LogMessageId)
					fmt.Printf("Written message %d: %v\n", idx, result.LogMessageId)
				}

				// Sleep between batches to simulate time-delayed writes
				time.Sleep(1 * time.Second)
			}

			// Wait for all reads to complete
			select {
			case <-readCompleteCh:
				fmt.Println("All messages read successfully")
			case err := <-readErrorCh:
				t.Fatalf("Error reading messages: %v", err)
			case <-time.After(20 * time.Second):
				t.Fatalf("Timeout waiting for messages to be read")
			}

			// Verify that the timeout goroutine completes, confirming blocking behavior
			more := false
			go func() {
				m, e := logReader.ReadNext(context.Background())
				t.Logf("Reader did not exhibit expected blocking behavior, but got m: %v e:%v", m, e)
				more = true
			}()
			time.Sleep(3 * time.Second)
			assert.False(t, more, "Reader did not exhibit expected blocking behavior")

			// Verify the messages we read match what we wrote
			assert.Equal(t, totalMessages, len(readMessages))
			for i, msg := range readMessages {
				assert.Equal(t, writeMessages[i].SegmentId, msg.Id.SegmentId)
				assert.Equal(t, writeMessages[i].EntryId, msg.Id.EntryId)
				assert.Equal(t, fmt.Sprintf("message %d", i), string(msg.Payload))
			}

			// Clean up
			err = logReader.Close(context.Background())
			assert.NoError(t, err)

			err = logWriter.Close(context.Background())
			assert.NoError(t, err)

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Println("Test completed successfully")
		})
	}
}

func TestTailReadBlockingAfterWriting(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestTailReadBlockingBehavior")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration()
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// CreateLog if not exists
			logName := "test_log_tail_read_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open the log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			totalMessages := 20
			// Now open a writer and write messages at intervals
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Write messages in batches with delays
			writeMessages := make([]*log.LogMessageId, 0, totalMessages)
			batchSize := 5
			numBatches := totalMessages / batchSize
			for batch := 0; batch < numBatches; batch++ {
				// Write a batch of messages
				for i := 0; i < batchSize; i++ {
					idx := batch*batchSize + i
					result := logWriter.Write(context.Background(), &log.WriterMessage{
						Payload: []byte(fmt.Sprintf("message %d", idx)),
						Properties: map[string]string{
							"k1": fmt.Sprintf("%d", batch),
							"k2": fmt.Sprintf("%d", i),
						},
					})

					assert.NoError(t, result.Err)
					writeMessages = append(writeMessages, result.LogMessageId)
					fmt.Printf("Written message %d: %v\n", idx, result.LogMessageId)
				}
			}

			// Verify that tail read timeout
			latest := log.LatestLogMessageID()
			logReader, err := logHandle.OpenLogReader(context.Background(), &latest, "client-latest-reader-async")
			assert.NoError(t, err)
			more := false
			go func() {
				m, e := logReader.ReadNext(context.Background())
				t.Logf("Reader did not exhibit expected blocking behavior, but got m: %v e:%v", m, e)
				more = true
			}()
			time.Sleep(3 * time.Second)
			assert.False(t, more, "Reader did not exhibit expected blocking behavior")

			// Clean up
			err = logReader.Close(context.Background())
			assert.NoError(t, err)
			err = logWriter.Close(context.Background())
			assert.NoError(t, err)
			fmt.Println("Test completed successfully")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}
