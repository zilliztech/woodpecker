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
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// TestEmbedMinioRolloverConditionWrite diagnoses #437 against external MinIO and
// etcd. It deliberately drains each segment before rolling, keeps the same
// writer alive, and compares enable/disable for both size and interval rolls.
// Its dedicated embedded-MinIO CI workflow opts in; other runs skip this suite
// because the fallback fencing waits can take several minutes.
// Rollover appends must not wait for the fallback fence; cross-segment read and
// explicit writer-close latency are recorded separately.
//
//	WP_TEST_ROLLOVER_REPRO=1 go test ./tests/integration \
//	  -run '^TestEmbedMinioRolloverConditionWrite$' -count=1 -v -timeout=10m
func TestEmbedMinioRolloverConditionWrite(t *testing.T) {
	if os.Getenv("WP_TEST_ROLLOVER_REPRO") != "1" {
		t.Skip("set WP_TEST_ROLLOVER_REPRO=1 to run the external MinIO/etcd latency reproduction")
	}
	for _, mode := range []string{"enable", "disable"} {
		for _, trigger := range []string{"size", "interval", "frequent_size"} {
			t.Run(mode+"_"+trigger, func(t *testing.T) {
				segmentCount := 3
				if trigger == "frequent_size" {
					segmentCount = 21 // Twenty consecutive rolls while old fences are still running.
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()
				cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
				require.NoError(t, err)
				cfg.Woodpecker.Storage.Type = "minio"
				// Log deletion persists a local tombstone even in embedded MinIO mode.
				cfg.Woodpecker.Storage.RootPath = t.TempDir()
				cfg.Woodpecker.Logstore.FencePolicy.ConditionWrite = mode
				// Keep the auditor from rolling/compacting behind the measured writes.
				cfg.Woodpecker.Client.Auditor.MaxInterval = config.NewDurationSecondsFromInt(3600)
				cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = config.NewDurationSecondsFromInt(3600)
				cfg.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks = 1000000
				cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize = config.NewByteSize(1 << 30)
				msg := &log.WriteMessage{Payload: []byte("normal-rollover-without-writer-restart")}
				encoded, err := log.MarshalMessage(msg)
				require.NoError(t, err)
				if trigger != "interval" {
					cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize = config.NewByteSize(int64(2 * len(encoded)))
				} else {
					cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = config.NewDurationSecondsFromInt(1)
				}
				if ep := os.Getenv("WP_TEST_ETCD_ENDPOINTS"); ep != "" {
					cfg.Etcd.Endpoints = strings.Split(ep, ",")
				}
				// Object keys and log names are unique even if legacy etcd metadata
				// makes the metadata provider reuse the historical root prefix.
				id := fmt.Sprintf("rollover-repro-%s-%s-%d", mode, trigger, time.Now().UnixNano())
				cfg.Etcd.RootPath = id
				cfg.Minio.RootPath = id
				cfg.Trace.Exporter = "noop"
				require.NoError(t, woodpecker.StopEmbedLogStore())
				cli, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				require.NoError(t, err)
				t.Cleanup(func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
					defer cleanupCancel()
					require.NoError(t, cli.Close(cleanupCtx))
					require.NoError(t, woodpecker.StopEmbedLogStore())
				})
				require.Equal(t, mode == "disable", cfg.Woodpecker.Logstore.FencePolicy.IsConditionWriteDisabled())
				objects, err := storageclient.NewObjectStorage(ctx, cfg)
				require.NoError(t, err)
				require.NoError(t, cli.CreateLog(ctx, id))
				lh, err := cli.OpenLog(ctx, id)
				require.NoError(t, err)
				writer, err := lh.OpenLogWriter(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
					defer cleanupCancel()
					require.NoError(t, writer.Close(cleanupCtx))
				})
				t.Logf("REPRO mode=%s trigger=%s writer=%T logId=%d root=%s", mode, trigger, writer, lh.GetId(), cfg.Minio.RootPath)
				var first *log.LogMessageId
				var previous *log.LogMessageId
				var rollingLatencies, ordinaryLatencies []time.Duration
				for segment := 0; segment < segmentCount; segment++ {
					if previous != nil {
						// A successful synchronous Write delivers its callback before
						// the executor necessarily removes the op from its queue.
						time.Sleep(100 * time.Millisecond)
						if trigger == "interval" {
							time.Sleep(1100 * time.Millisecond)
						}
						base := fmt.Sprintf("%s/%d/%d", cfg.Minio.RootPath, lh.GetId(), previous.SegmentId)
						size, _, err := objects.StatObject(ctx, cfg.Minio.BucketName, base+"/write.lock", "repro", "0")
						require.NoError(t, err, "the live writer's lock exists before normal rollover")
						body, err := objects.GetObject(ctx, cfg.Minio.BucketName, base+"/write.lock", 0, size, "repro", "0")
						require.NoError(t, err)
						lock, err := io.ReadAll(body)
						require.NoError(t, body.Close())
						require.NoError(t, err)
						require.Contains(t, string(lock), fmt.Sprintf("pid=%d\n", os.Getpid()))
						t.Logf("BEFORE_ROLL mode=%s trigger=%s old=%d/%d ownLock=true", mode, trigger, previous.SegmentId, previous.EntryId)
					}
					for entry := 0; entry < 2; entry++ {
						started := time.Now()
						result := writer.Write(ctx, msg)
						elapsed := time.Since(started)
						require.NoError(t, result.Err)
						assert.Less(t, elapsed, 5*time.Second, "every append must stay within the write latency budget")
						require.NotNil(t, result.LogMessageId)
						current := result.LogMessageId
						if first == nil {
							first = current
						}
						require.Equal(t, first.SegmentId+int64(segment), current.SegmentId)
						require.EqualValues(t, entry, current.EntryId)
						isRollover := segment > 0 && entry == 0
						t.Logf("APPEND mode=%s trigger=%s id=%d/%d rollover=%t elapsed=%s", mode, trigger, current.SegmentId, current.EntryId, isRollover, elapsed)
						if isRollover {
							rollingLatencies = append(rollingLatencies, elapsed)
							footer := fmt.Sprintf("%s/%d/%d/footer.blk", cfg.Minio.RootPath, lh.GetId(), previous.SegmentId)
							_, _, err := objects.StatObject(ctx, cfg.Minio.BucketName, footer, "repro", "0")
							if err != nil {
								require.True(t, objects.IsObjectNotExistsError(err), "unexpected footer stat error: %v", err)
							}
							t.Logf("AFTER_ROLL mode=%s trigger=%s oldFinalized=%t", mode, trigger, err == nil)
						} else {
							ordinaryLatencies = append(ordinaryLatencies, elapsed)
						}
						previous = current
					}
				}
				require.Len(t, rollingLatencies, segmentCount-1, "must exercise every planned rollover")
				// Compare the same writer's rollover and ordinary writes. The one-second
				// allowance covers metadata/object-store jitter while catching stalls
				// that would be hidden by a loose absolute timeout or an average.
				p95 := func(latencies []time.Duration) time.Duration {
					slices.Sort(latencies)
					return latencies[(95*len(latencies)+99)/100-1]
				}
				ordinaryP95, rollingP95 := p95(ordinaryLatencies), p95(rollingLatencies)
				assert.LessOrEqual(t, rollingP95, ordinaryP95+time.Second, "rollover p95 must stay close to ordinary-write p95")
				t.Logf("LATENCY mode=%s trigger=%s rolls=%d writes=%d ordinaryP95=%s rollingP95=%s rollingMax=%s", mode, trigger, len(rollingLatencies), 2*segmentCount, ordinaryP95, rollingP95, rollingLatencies[len(rollingLatencies)-1])
				reader, err := lh.OpenLogReader(ctx, first, id+"-reader")
				require.NoError(t, err)
				for i := 0; i < 2*segmentCount; i++ {
					started := time.Now()
					message, err := reader.ReadNext(ctx)
					elapsed := time.Since(started)
					require.NoError(t, err)
					require.Equal(t, msg.Payload, message.Payload)
					require.EqualValues(t, first.SegmentId+int64(i/2), message.Id.SegmentId)
					require.EqualValues(t, i%2, message.Id.EntryId)
					t.Logf("READ mode=%s trigger=%s id=%d/%d elapsed=%s", mode, trigger, message.Id.SegmentId, message.Id.EntryId, elapsed)
				}
				require.NoError(t, reader.Close(ctx))
				started := time.Now()
				require.NoError(t, writer.Close(ctx))
				t.Logf("CLOSE mode=%s trigger=%s elapsed=%s readBack=%d/%d", mode, trigger, time.Since(started), 2*segmentCount, 2*segmentCount)
				segments, err := lh.GetSegments(ctx)
				require.NoError(t, err)
				for i := int64(0); i < int64(segmentCount); i++ {
					segmentID := first.SegmentId + i
					require.Equal(t, proto.SegmentState_Completed, segments[segmentID].Metadata.State)
					footer := fmt.Sprintf("%s/%d/%d/footer.blk", cfg.Minio.RootPath, lh.GetId(), segmentID)
					_, _, err := objects.StatObject(ctx, cfg.Minio.BucketName, footer, "repro", "0")
					require.NoError(t, err, "writer close must wait for all segment completions")
				}
				require.NoError(t, cli.DeleteLog(ctx, id))
			})
		}
	}
}
