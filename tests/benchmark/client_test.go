package benchmark

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func TestE2EWrite(t *testing.T) {
	startGopsAgent()

	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ###  CreateLog if not exists
	client.GetMetadataProvider().CreateLog(context.Background(), "test_log")

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}
	logHandle.GetName()

	//	### OpenWriter
	logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
	if openWriterErr != nil {
		fmt.Printf("Open writer failed, err:%v\n", openWriterErr)
		panic(openWriterErr)
	}
	writeResultChan := logWriter.WriteAsync(context.Background(),
		&log.WriterMessage{
			Payload: []byte("hello world 1"),
		},
	)
	writeResult := <-writeResultChan
	if writeResult.Err != nil {
		fmt.Println(writeResult.Err)
		panic(writeResult.Err)
	}
	fmt.Printf("write success, returned recordId:%v\n", writeResult.LogMessageId)
	//writeResult = logWriter.Write(context.Background(), []byte("hello world 2"))
	//if writeResult.Err != nil {
	//	fmt.Println(writeResult.Err)
	//	panic(writeResult.Err)
	//}
	//fmt.Printf("write success, returned recordId:%v\n", writeResult.LogMessageId)
}

func TestAsyncWritePerformance(t *testing.T) {
	startGopsAgent()
	startMetrics()
	entrySize := 1 * 1024    // 1KB per row
	batchCount := 16_000     // 64MB per batch to wait or retry if any failed exists
	writeCount := 10_000_000 // total 10M rows to write

	// ### Create client
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ###  CreateLog if not exists
	client.CreateLog(context.Background(), "test_log")

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}
	logHandle.GetName()

	//	### OpenWriter
	logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
	if openWriterErr != nil {
		fmt.Printf("Open writer failed, err:%v\n", openWriterErr)
		panic(openWriterErr)
	}

	// gen static data
	payloadStaticData, err := generateRandomBytes(entrySize) // 1KB per row
	assert.NoError(t, err)

	// ### Write
	successCount := 0
	writingResultChan := make([]<-chan *log.WriteResult, 0) // 10M*1k=10GB
	writingMessages := make([]*log.WriterMessage, 0)
	failMessages := make([]*log.WriterMessage, 0)
	for i := 0; i < writeCount; i++ {
		// append async
		msg := &log.WriterMessage{
			Payload: payloadStaticData,
			Properties: map[string]string{
				"key": fmt.Sprintf("value%d", i),
			},
		}
		resultChan := logWriter.WriteAsync(context.Background(), msg)
		writingResultChan = append(writingResultChan, resultChan)
		writingMessages = append(writingMessages, msg)

		// wait for batch finish
		if len(writingMessages)%batchCount == 0 { // wait 64000 entries or 64MB to completed
			fmt.Printf("start wait for %d entries\n", len(writingMessages))
			for idx, ch := range writingResultChan {
				writeResult := <-ch // TODO， 这个chan不应该是segment暴露出来的，而是writer的chan，因为segment可能会fence 滚动
				if writeResult.Err != nil {
					fmt.Printf(writeResult.Err.Error())
					failMessages = append(failMessages, writingMessages[idx])
				} else {
					fmt.Printf("write success, returned recordId:%v \n", writeResult.LogMessageId)
					successCount++
				}
			}
			fmt.Printf("finish wait for %d entries. success:%d , failed: %d, i: %d \n", len(writingMessages), successCount, len(failMessages), i)
			time.Sleep(1 * time.Second) // wait a moment to avoid too much retry
			writingResultChan = make([]<-chan *log.WriteResult, 0)
			writingMessages = make([]*log.WriterMessage, 0)
			for _, m := range failMessages {
				retryCh := logWriter.WriteAsync(context.Background(), m)
				writingResultChan = append(writingResultChan, retryCh)
				writingMessages = append(writingMessages, m)
			}
			failMessages = make([]*log.WriterMessage, 0)
		}
	}

	// wait&retry the rest
	{
		for idx, ch := range writingResultChan {
			writeResult := <-ch
			if writeResult.Err != nil {
				fmt.Printf(writeResult.Err.Error())
				failMessages = append(failMessages, writingMessages[idx])
			} else {
				successCount++
			}
		}
		time.Sleep(1 * time.Second) // wait a moment to avoid too much retry
		for {
			if len(failMessages) == 0 {
				break
			}
			writingResultChan = make([]<-chan *log.WriteResult, 0)
			writingMessages = make([]*log.WriterMessage, 0)
			for _, m := range failMessages {
				retryCh := logWriter.WriteAsync(context.Background(), m)
				writingResultChan = append(writingResultChan, retryCh)
				writingMessages = append(writingMessages, m)
			}
			failMessages = make([]*log.WriterMessage, 0)
			for idx, ch := range writingResultChan {
				writeResult := <-ch
				if writeResult.Err != nil {
					fmt.Printf(writeResult.Err.Error())
					failMessages = append(failMessages, writingMessages[idx])
				} else {
					successCount++
				}
			}
		}
	}

	// ### close and print result
	fmt.Printf("start close log writer \n")
	closeErr := logWriter.Close(context.Background())
	if closeErr != nil {
		fmt.Printf("close failed, err:%v\n", closeErr)
		panic(closeErr)
	}
	fmt.Printf("Test Write finished  %d entries\n", successCount)
}

// TestWrite example to show how to use woodpecker client to write msg to  unbounded log
func TestReadThroughput(t *testing.T) {
	startGopsAgent()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}

	//	### OpenReader
	start := &log.LogMessageId{
		SegmentId: 21,
		EntryId:   0,
	}
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start)
	if openReaderErr != nil {
		fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
		panic(openReaderErr)
	}

	// 调用reader遍历所有的数据 logReader.ReadNext(context.Background())
	for {
		msg, err := logReader.ReadNext(context.Background())
		if err != nil {
			fmt.Printf("read failed, err:%v\n", err)
			panic(err)
		} else {
			fmt.Printf("read success, msg:%v\n", msg)
		}
	}

	fmt.Printf("Test Read finished\n")
}

func TestReadFromEarliest(t *testing.T) {
	startGopsAgent()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}

	//	### OpenReader
	earliest := log.EarliestLogMessageID()
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &earliest)
	if openReaderErr != nil {
		fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
		panic(openReaderErr)
	}

	// 调用reader遍历所有的数据 logReader.ReadNext(context.Background())
	totalEntries := 0
	for {
		msg, err := logReader.ReadNext(context.Background())
		if err != nil {
			fmt.Printf("read failed, err:%v\n", err)
			break
		} else {
			fmt.Printf("read success, msg:%v\n", msg)
		}
		totalEntries += 1
		if totalEntries%10000 == 0 {
			fmt.Printf(" read %d success, the msg:%v \n", totalEntries, msg)
		}
	}
	fmt.Printf("final read %d success \n", totalEntries)

	fmt.Printf("Test Read finished\n")
}

func TestReadFromLatest(t *testing.T) {
	startGopsAgent()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}

	//	### OpenReader
	latest := log.LatestLogMessageID()
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &latest)
	if openReaderErr != nil {
		fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
		panic(openReaderErr)
	}

	more := false
	go func() {
		msg, err := logReader.ReadNext(context.Background())
		if err != nil {
			fmt.Printf("read failed, err:%v\n", err)
			t.Error(err)
		} else {
			more = true
			fmt.Printf("read success, msg:%v\n", msg)
		}
	}()
	time.Sleep(time.Second * 2)
	assert.False(t, more, "should read nothing and timeout")
}

func TestReadFromSpecifiedPosition(t *testing.T) {
	startGopsAgent()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}

	//	### OpenReader
	start := &log.LogMessageId{
		SegmentId: 5,
		EntryId:   0,
	}
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start)
	if openReaderErr != nil {
		fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
		panic(openReaderErr)
	}

	// 调用reader遍历所有的数据 logReader.ReadNext(context.Background())
	for {
		msg, err := logReader.ReadNext(context.Background())
		if err != nil {
			fmt.Printf("read failed, err:%v\n", err)
			t.Error(err)
		} else {
			fmt.Printf("read success, msg:%v\n", msg)
		}
	}

	fmt.Printf("Test Read finished\n")
}
