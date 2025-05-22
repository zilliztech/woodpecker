// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmark

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
)

func TestAppendAsync(t *testing.T) {
	segmentPrefixKey := "test-segment"
	bucket := "test-bucket"
	logFileId := int64(1)

	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Minio.BucketName = bucket
	client, err := minioHandler.NewMinioHandler(context.Background(), cfg)

	assert.NoError(t, err)
	logFile := objectstorage.NewLogFile(1, 0, logFileId, segmentPrefixKey, bucket, client, cfg)
	assert.NotNil(t, logFile)
	objectLogFile := logFile.(*objectstorage.LogFile)
	assert.NotNil(t, objectLogFile)

	// Test appending a valid entry
	entryId, ch, _ := logFile.AppendAsync(context.Background(), 0, []byte("data0"))
	assert.Equal(t, int64(0), entryId)
	assert.NotNil(t, ch)
	select {
	case result := <-ch:
		assert.Equal(t, int64(0), result)
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timeout waiting for channel")
	}

	// Test appending another valid entry
	entryId, ch, _ = logFile.AppendAsync(context.Background(), 1, []byte("data1"))
	assert.Equal(t, int64(1), entryId)
	assert.NotNil(t, ch)
	select {
	case result := <-ch:
		assert.Equal(t, int64(1), result)
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timeout waiting for channel")
	}

	// Test appending an entry with an invalid ID
	entryId, ch, _ = logFile.AppendAsync(context.Background(), 3, []byte("data3"))
	assert.Equal(t, int64(3), entryId)
	assert.NotNil(t, ch)
	var timeoutErr error
	select {
	case result := <-ch:
		assert.Equal(t, int64(-1), result)
	case <-time.After(2000 * time.Millisecond):
		timeoutErr = errors.New("timeout")
	}
	assert.Error(t, timeoutErr)

	// Test appending an entry that exceeds the buffer size
	entryId, ch, _ = logFile.AppendAsync(context.Background(), 2, []byte("data2"))
	for i := 4; i < 100_000; i++ {
		logFile.AppendAsync(context.Background(), int64(i), []byte("data"))
	}
	entryId, ch, _ = logFile.AppendAsync(context.Background(), 100_000, []byte("data"))
	assert.Equal(t, int64(100_000), entryId)
	assert.NotNil(t, ch)
	select {
	case result := <-ch:
		assert.Equal(t, int64(100_000), result)
	case <-time.After(5000 * time.Millisecond):
		t.Error("Timeout waiting for channel")
	}
}

func TestNewReader(t *testing.T) {
	segmentPrefixKey := "test-segment-reader"
	bucket := "test-bucket"
	logFileId := int64(1)

	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Minio.BucketName = bucket
	client, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)
	logFile := objectstorage.NewLogFile(1, 0, logFileId, segmentPrefixKey, bucket, client, cfg)
	assert.NotNil(t, logFile)

	// Append some data to the log file
	_, ch1, _ := logFile.AppendAsync(context.Background(), 0, []byte("data0"))
	_, ch2, _ := logFile.AppendAsync(context.Background(), 1, []byte("data1"))
	_, ch3, _ := logFile.AppendAsync(context.Background(), 2, []byte("data2"))

	// Wait for the data to be appended
	select {
	case <-ch1:
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timeout waiting for channel")
	}
	select {
	case <-ch2:
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timeout waiting for channel")
	}
	select {
	case <-ch3:
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timeout waiting for channel")
	}

	// Create a reader for the log file
	reader, err := logFile.NewReader(context.Background(), storage.ReaderOpt{StartSequenceNum: 0, EndSequenceNum: 3})
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	// Read all data from the reader
	entries := make([]*proto.LogEntry, 0)
	for {
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext()
		assert.NoError(t, err)
		entries = append(entries, entry)
	}

	// Verify the read data
	assert.Len(t, entries, 3)
	assert.Equal(t, int64(0), entries[0].EntryId)
	assert.Equal(t, "data0", string(entries[0].Values))
	assert.Equal(t, int64(1), entries[1].EntryId)
	assert.Equal(t, "data1", string(entries[1].Values))
	assert.Equal(t, int64(2), entries[2].EntryId)
	assert.Equal(t, "data2", string(entries[2].Values))
}

func TestNewReaderForManyFragments(t *testing.T) {
	segmentPrefixKey := "test-segment-reader-many-fragments"
	bucket := "test-bucket"
	logFileId := int64(1)

	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Minio.BucketName = bucket
	client, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)
	logFile := objectstorage.NewLogFile(1, 0, logFileId, segmentPrefixKey, bucket, client, cfg)
	assert.NotNil(t, logFile)

	// Append some data to the log file
	for i := 0; i < 5; i++ {
		_, ch, _ := logFile.AppendAsync(context.Background(), int64(i), []byte(fmt.Sprintf("data%d", i)))
		select {
		case <-ch:
		case <-time.After(2000 * time.Millisecond):
			t.Error("Timeout waiting for channel")
		}
	}

	// Create a reader for the log file
	reader, err := logFile.NewReader(context.Background(), storage.ReaderOpt{StartSequenceNum: 0, EndSequenceNum: -1})
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	// Read all data from the reader
	entries := make([]*proto.LogEntry, 0)
	for {
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		fmt.Printf("read one ... ")
		entry, err := reader.ReadNext()
		assert.NoError(t, err)
		entries = append(entries, entry)
	}

	// Verify the read data
	assert.Len(t, entries, 5)
	assert.Equal(t, int64(0), entries[0].EntryId)
	assert.Equal(t, "data0", string(entries[0].Values))
	assert.Equal(t, int64(1), entries[1].EntryId)
	assert.Equal(t, "data1", string(entries[1].Values))
	assert.Equal(t, int64(2), entries[2].EntryId)
	assert.Equal(t, "data2", string(entries[2].Values))
	assert.Equal(t, int64(3), entries[3].EntryId)
	assert.Equal(t, "data3", string(entries[3].Values))
	assert.Equal(t, int64(4), entries[4].EntryId)
	assert.Equal(t, "data4", string(entries[4].Values))
}
