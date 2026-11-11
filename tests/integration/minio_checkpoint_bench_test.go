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

package integration

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/codec/checkpoint"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
)

type checkpointBenchmarkStorage struct {
	storageclient.ObjectStorage
	stats atomic.Int64
	gets  atomic.Int64
}

func (s *checkpointBenchmarkStorage) StatObject(ctx context.Context, bucket, key, namespace, logID string) (int64, bool, error) {
	s.stats.Add(1)
	return s.ObjectStorage.StatObject(ctx, bucket, key, namespace, logID)
}

func (s *checkpointBenchmarkStorage) GetObject(ctx context.Context, bucket, key string, offset, size int64, namespace, logID string) (minioHandler.FileReader, error) {
	s.gets.Add(1)
	return s.ObjectStorage.GetObject(ctx, bucket, key, offset, size, namespace, logID)
}

// BenchmarkMinioCheckpointRecovery compares recovery of the same immutable,
// unfinished segment before and after adding a controlled prefix checkpoint.
// It times only recovery-writer construction, excluding seeding, Close, fencing
// and finalization. No wall-clock speedup is asserted. It requires real MinIO.
//
// Run with a fixed sample count, for example:
// go test ./tests/integration -run '^$' -bench '^BenchmarkMinioCheckpointRecovery$' -benchtime=10x -count=1
func BenchmarkMinioCheckpointRecovery(b *testing.B) {
	for _, blocks := range []int{635, 909} {
		b.Run(fmt.Sprintf("%d_blocks", blocks), func(b *testing.B) {
			b.StopTimer()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			require.NoError(b, err)
			previousLevel := logger.GetLevel()
			require.NoError(b, logger.SetLevel("warn"))
			b.Cleanup(func() { _ = logger.SetLevel(previousLevel) })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			b.Cleanup(cancel)
			client, err := storageclient.NewObjectStorage(ctx, cfg)
			require.NoError(b, err)
			bucket := cfg.Minio.BucketName
			prefix := fmt.Sprintf("bench-checkpoint-%d-%d", blocks, time.Now().UnixNano())
			const logID, segmentID = int64(78), int64(83)
			segmentKey := fmt.Sprintf("%s/%d/%d", prefix, logID, segmentID)
			b.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				segment := objectstorage.NewSegmentImpl(cleanupCtx, bucket, prefix, logID, segmentID, client, cfg)
				_, cleanupErr := segment.DeleteFileData(cleanupCtx, 0)
				require.NoError(b, cleanupErr)
			})

			// Force one acknowledged entry per block. Disable automatic checkpoints so
			// the covered prefix and tail length are exact and independent of async lag.
			cfg.Woodpecker.Logstore.SegmentSyncPolicy.CheckpointThreshold = 0
			cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval = config.NewDurationMillisecondsFromInt(3600000)
			cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.NewByteSize(200 * 1024)
			writer, err := objectstorage.NewMinioFileWriter(ctx, bucket, prefix, logID, segmentID, client, cfg)
			require.NoError(b, err)
			b.Cleanup(func() { _ = writer.Close(context.Background()) })
			payload := make([]byte, 57*1024)
			for i := range payload {
				payload[i] = byte(i % 251)
			}
			for entryID := int64(0); entryID < int64(blocks); entryID++ {
				resultCh := channel.NewLocalResultChannel(fmt.Sprintf("bench-%d", entryID))
				_, err = writer.WriteDataAsync(ctx, entryID, payload, resultCh)
				require.NoError(b, err)
				require.NoError(b, writer.Sync(ctx))
				result, readErr := resultCh.ReadResult(ctx)
				require.NoError(b, readErr)
				require.NoError(b, result.Err)
			}
			require.NoError(b, writer.Close(ctx))
			// Build the exact intermediate snapshot from the known one-entry blocks.
			covered := blocks / 10 * 10
			indexes := make([]*codec.IndexRecord, covered)
			var segmentBytes int64
			for block := 0; block < blocks; block++ {
				size, fenced, statErr := client.StatObject(ctx, bucket, fmt.Sprintf("%s/%d.blk", segmentKey, block), "bench", "78")
				require.NoError(b, statErr)
				require.False(b, fenced)
				segmentBytes += size
				if block < covered {
					indexes[block] = &codec.IndexRecord{BlockNumber: int32(block), StartOffset: int64(block), BlockSize: uint32(size), FirstEntryID: int64(block), LastEntryID: int64(block)}
				}
			}
			raw, err := checkpoint.Serialize(checkpoint.New(indexes))
			require.NoError(b, err)
			checkpointKey := segmentKey + "/checkpoint.blk"
			size, _, err := client.StatObject(ctx, bucket, checkpointKey, "bench", "78")
			require.True(b, client.IsObjectNotExistsError(err), "baseline must have no checkpoint, got size %d", size)

			for _, withCheckpoint := range []bool{false, true} {
				mode := "full_scan"
				if withCheckpoint {
					mode = "checkpoint"
					require.NoError(b, client.PutObject(ctx, bucket, checkpointKey, bytes.NewReader(raw), int64(len(raw)), "bench", "78"))
				}
				b.Run(mode, func(b *testing.B) {
					b.StopTimer()
					observed := &checkpointBenchmarkStorage{ObjectStorage: client}
					elapsed := make([]time.Duration, 0, b.N)
					var statCalls, getCalls int64
					for sample := 0; sample < b.N; sample++ {
						startStats, startGets := observed.stats.Load(), observed.gets.Load()
						b.StartTimer()
						started := time.Now()
						recovered, recoverErr := objectstorage.NewMinioFileWriterWithMode(ctx, bucket, prefix, logID, segmentID, observed, cfg, true)
						duration := time.Since(started)
						b.StopTimer()
						require.NoError(b, recoverErr)
						require.Equal(b, int64(blocks-1), recovered.GetLastEntryId(ctx))
						statCalls += observed.stats.Load() - startStats
						getCalls += observed.gets.Load() - startGets
						elapsed = append(elapsed, duration)
						require.NoError(b, recovered.Close(ctx))
					}
					sort.Slice(elapsed, func(i, j int) bool { return elapsed[i] < elapsed[j] })
					median := float64(elapsed[len(elapsed)/2])
					if len(elapsed)%2 == 0 {
						median = (float64(elapsed[len(elapsed)/2-1]) + median) / 2
					}
					b.ReportMetric(median/float64(time.Millisecond), "median_ms")
					b.ReportMetric(float64(elapsed[0])/float64(time.Millisecond), "min_ms")
					b.ReportMetric(float64(elapsed[len(elapsed)-1])/float64(time.Millisecond), "max_ms")
					b.ReportMetric(float64(statCalls)/float64(b.N), "stat/op")
					b.ReportMetric(float64(getCalls)/float64(b.N), "get/op")
					b.ReportMetric(float64(segmentBytes), "segment_B")
					if withCheckpoint {
						b.ReportMetric(float64(len(raw)), "checkpoint_B")
						b.ReportMetric(float64(blocks-covered), "tail_blocks")
					}
				})
			}
		})
	}
}
