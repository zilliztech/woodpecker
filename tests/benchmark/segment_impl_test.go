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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
)

func TestAppendAsync(t *testing.T) {
	segmentPrefixKey := "test-segment"
	bucket := "test-bucket"
	segmentId := int64(1)

	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Minio.BucketName = bucket
	client, err := minioHandler.NewMinioHandler(context.Background(), cfg)

	assert.NoError(t, err)
	segmentImpl := objectstorage.NewSegmentImpl(context.TODO(), 1, segmentId, segmentPrefixKey, bucket, client, cfg)
	assert.NotNil(t, segmentImpl)
	objectSegmentImpl := segmentImpl.(*objectstorage.SegmentImpl)
	assert.NotNil(t, objectSegmentImpl)

	// Test appending a valid entry
	rc0 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
	entryId, _ := segmentImpl.AppendAsync(context.Background(), 0, []byte("data0"), rc0)
	assert.Equal(t, int64(0), entryId)

	subCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	result, readErr := rc0.ReadResult(subCtx)
	cancel()
	assert.NoError(t, readErr, "Timeout waiting for channel")
	assert.Equal(t, int64(0), result.SyncedId)

	// Test appending another valid entry
	rc1 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 1))
	entryId, _ = segmentImpl.AppendAsync(context.Background(), 1, []byte("data1"), rc1)
	assert.Equal(t, int64(1), entryId)
	subCtx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	result1, readErr1 := rc1.ReadResult(subCtx1)
	cancel1()
	assert.NoError(t, readErr1, "Timeout waiting for channel")
	assert.Equal(t, int64(1), result1.SyncedId)

	// Test appending an entry with an invalid ID
	rc3 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 3))
	entryId, _ = segmentImpl.AppendAsync(context.Background(), 3, []byte("data3"), rc3)
	assert.Equal(t, int64(3), entryId)
	subCtx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
	result3, readErr3 := rc3.ReadResult(subCtx3)
	cancel3()
	assert.Error(t, readErr3)
	assert.Nil(t, result3)
	assert.True(t, errors.IsAny(readErr3, context.Canceled, context.DeadlineExceeded))

	// Test appending an entry that exceeds the buffer size
	rc4 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 2))
	entryId, _ = segmentImpl.AppendAsync(context.Background(), 2, []byte("data2"), rc4)
	for i := 4; i < 100_000; i++ {
		rc000 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", i))
		segmentImpl.AppendAsync(context.Background(), int64(i), []byte("data"), rc000)
	}
	rc5 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 100_000))
	entryId, _ = segmentImpl.AppendAsync(context.Background(), 100_000, []byte("data"), rc5)
	assert.Equal(t, int64(100_000), entryId)

	subCtx5, cancel5 := context.WithTimeout(context.Background(), 5*time.Second)
	result5, readErr5 := rc5.ReadResult(subCtx5)
	cancel5()
	assert.NoError(t, readErr5)
	assert.Equal(t, int64(100_000), result5.SyncedId)
}

func TestNewReader(t *testing.T) {
	segmentPrefixKey := "test-segment-reader"
	bucket := "test-bucket"
	segmentId := int64(1)

	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Minio.BucketName = bucket
	client, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)
	segmentImpl := objectstorage.NewSegmentImpl(context.TODO(), 1, segmentId, segmentPrefixKey, bucket, client, cfg)
	assert.NotNil(t, segmentImpl)

	// Append some data to the log file
	rc1 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
	rc2 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 1))
	rc3 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 2))
	_, _ = segmentImpl.AppendAsync(context.Background(), 0, []byte("data0"), rc1)
	_, _ = segmentImpl.AppendAsync(context.Background(), 1, []byte("data1"), rc2)
	_, _ = segmentImpl.AppendAsync(context.Background(), 2, []byte("data2"), rc3)

	// Wait for the data to be appended
	{
		subCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		result, readErr := rc1.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr, "Timeout waiting for channel")
		assert.Equal(t, int64(0), result.SyncedId)
	}
	{
		subCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		result, readErr := rc2.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr, "Timeout waiting for channel")
		assert.Equal(t, int64(1), result.SyncedId)
	}
	{
		subCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		result, readErr := rc3.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr, "Timeout waiting for channel")
		assert.Equal(t, int64(2), result.SyncedId)
	}

	// Create a reader for the log file
	roSegmentImpl := objectstorage.NewROSegmentImpl(context.TODO(), 1, segmentId, segmentPrefixKey, bucket, client, cfg)
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{StartSequenceNum: 0, EndSequenceNum: 3})
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	// Read all data from the reader
	entries := make([]*proto.LogEntry, 0)
	for {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext(context.TODO())
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
	segmentId := int64(1)

	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Minio.BucketName = bucket
	client, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)
	segmentImpl := objectstorage.NewSegmentImpl(context.TODO(), 1, segmentId, segmentPrefixKey, bucket, client, cfg)
	assert.NotNil(t, segmentImpl)

	// Append some data to the log file
	for i := 0; i < 5; i++ {
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", i))
		_, _ = segmentImpl.AppendAsync(context.Background(), int64(i), []byte(fmt.Sprintf("data%d", i)), rc)

		subCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr)
		assert.Equal(t, int64(i), result.SyncedId)
	}

	// Create a reader for the log file
	roSegmentImpl := objectstorage.NewROSegmentImpl(context.TODO(), 1, segmentId, segmentPrefixKey, bucket, client, cfg)
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{StartSequenceNum: 0, EndSequenceNum: -1})
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	// Read all data from the reader
	entries := make([]*proto.LogEntry, 0)
	for {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		fmt.Printf("read one ... ")
		entry, err := reader.ReadNext(context.TODO())
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
