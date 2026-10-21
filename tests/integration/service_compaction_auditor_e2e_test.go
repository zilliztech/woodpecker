// Copyright (C) 2026 Zilliz. All rights reserved.
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

package integration

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker/client"
	"github.com/zilliztech/woodpecker/woodpecker/log"
	"github.com/zilliztech/woodpecker/woodpecker/quorum"
)

// This decorator preserves the real SDK, gRPC, server compaction, etcd CAS and
// MinIO paths. Only scheduling and lost RPC responses are injected. In particular,
// neither the test nor the decorator calls SegmentHandle.Compact: both writers'
// production auditor goroutines must discover and compact the Completed segment.
type auditorCompactionPool struct {
	client.LogStoreClientPool
	role           string
	barrier        *auditorCompactionBarrier
	compactTarget  string
	loseResponses  bool
	calls          atomic.Int64
	serverSuccess  atomic.Int64
	responseErrors atomic.Int64
	firstSuccess   sync.Once
}

type auditorCompactionBarrier struct {
	entered   chan string
	start     chan struct{}
	published chan struct{}
	otherDone chan struct{}
	release   chan struct{}
	publish   sync.Once
	finish    sync.Once
}

type auditorCompactionClient struct {
	client.LogStoreClient
	pool   *auditorCompactionPool
	target string
}

func (p *auditorCompactionPool) GetLogStoreClient(ctx context.Context, target string) (client.LogStoreClient, error) {
	cli, err := p.LogStoreClientPool.GetLogStoreClient(ctx, target)
	if err != nil {
		return nil, err
	}
	return &auditorCompactionClient{LogStoreClient: cli, pool: p, target: target}, nil
}

func (c *auditorCompactionClient) SegmentCompact(ctx context.Context, bucket, root string, logID, segmentID, lac int64) (*proto.SegmentMetadata, error) {
	p := c.pool
	if segmentID != 0 {
		return c.LogStoreClient.SegmentCompact(ctx, bucket, root, logID, segmentID, lac)
	}
	call := p.calls.Add(1)
	if call == 1 {
		p.barrier.entered <- p.role
		select {
		case <-p.barrier.start:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	target := c.target
	if p.compactTarget != "" {
		target = p.compactTarget
	}
	// Reacquire after the barrier: replicas were restarted to recover the
	// deliberately reblocked files, so a pre-barrier connection may be stale.
	cli, err := p.LogStoreClientPool.GetLogStoreClient(ctx, target)
	if err != nil {
		return nil, err
	}
	result, err := cli.SegmentCompact(ctx, bucket, root, logID, segmentID, lac)
	if err != nil {
		return nil, err
	}
	p.serverSuccess.Add(1)
	if p.role == "old" {
		// Keep metadata Completed until the second auditor has run too. The
		// server has already published a complete remote result at this point.
		var holdErr error
		p.firstSuccess.Do(func() {
			p.barrier.publish.Do(func() { close(p.barrier.published) })
			select {
			case <-p.barrier.release:
			case <-ctx.Done():
				holdErr = ctx.Err()
			}
		})
		if holdErr != nil {
			return nil, holdErr
		}
	}
	if p.role == "new" {
		if p.loseResponses && call <= 3 {
			// Simulate a lost response AFTER successful real server execution,
			// across every node attempt of this auditor's first compaction pass.
			p.responseErrors.Add(1)
			if call == 3 {
				p.barrier.finish.Do(func() { close(p.barrier.otherDone) })
			}
			return nil, status.Error(codes.Unavailable, "injected lost compaction response")
		}
		p.barrier.finish.Do(func() { close(p.barrier.otherDone) })
	}
	return result, nil
}

type auditorCompactionMetadata struct {
	meta.MetadataProvider
	sealedSnapshots atomic.Int64
	conflicts       atomic.Int64
}

func (m *auditorCompactionMetadata) GetAllSegmentMetadata(ctx context.Context, name string) (map[int64]*meta.SegmentMeta, error) {
	segments, err := m.MetadataProvider.GetAllSegmentMetadata(ctx, name)
	if err == nil && segments[0] != nil && segments[0].Metadata.State == proto.SegmentState_Sealed {
		m.sealedSnapshots.Add(1)
	}
	return segments, err
}

func (m *auditorCompactionMetadata) UpdateSegmentMetadata(ctx context.Context, name string, logID int64, segment *meta.SegmentMeta, previous proto.SegmentState) error {
	err := m.MetadataProvider.UpdateSegmentMetadata(ctx, name, logID, segment, previous)
	if segment.Metadata.SegNo == 0 && segment.Metadata.State == proto.SegmentState_Sealed && werr.ErrMetadataRevisionInvalid.Is(err) {
		m.conflicts.Add(1)
	}
	return err
}

// Rewrite a stopped replica's finalized file using the same encoded DataRecords
// but explicitly different flush boundaries. Restart then exercises real on-disk
// recovery; no writer cache is modified behind a running server's back.
func reblockAuditorReplica(t *testing.T, path string, groupSize, entries int) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	records, err := codec.DecodeRecordList(raw)
	require.NoError(t, err)
	var header *codec.HeaderRecord
	var footer *codec.FooterRecord
	var data [][]byte
	for _, record := range records {
		switch record := record.(type) {
		case *codec.HeaderRecord:
			header = record
		case *codec.FooterRecord:
			footer = record
		case *codec.DataRecord:
			data = append(data, codec.EncodeRecord(record))
		}
	}
	require.NotNil(t, header)
	require.NotNil(t, footer)
	require.Len(t, data, entries)
	require.Equal(t, int64(entries-1), footer.LAC)
	var output bytes.Buffer
	output.Write(codec.EncodeRecord(header))
	var indexes []*codec.IndexRecord
	for first := 0; first < entries; first += groupSize {
		last := min(first+groupSize, entries)
		payload := bytes.Join(data[first:last], nil)
		block := &codec.BlockHeaderRecord{
			BlockNumber: int32(len(indexes)), FirstEntryID: int64(first), LastEntryID: int64(last - 1),
			BlockLength: uint32(len(payload)), BlockCrc: crc32.ChecksumIEEE(payload),
		}
		encoded := codec.EncodeRecord(block)
		indexes = append(indexes, &codec.IndexRecord{
			BlockNumber: block.BlockNumber, StartOffset: int64(output.Len()),
			BlockSize: uint32(len(encoded) + len(payload)), FirstEntryID: block.FirstEntryID, LastEntryID: block.LastEntryID,
		})
		output.Write(encoded)
		output.Write(payload)
	}
	footer.TotalBlocks = int32(len(indexes))
	footer.IndexOffset = uint64(output.Len())
	footer.IndexLength = uint32(len(indexes) * (codec.RecordHeaderSize + codec.IndexRecordSize))
	footer.TotalSize = uint64(output.Len()) + uint64(footer.IndexLength)
	for _, index := range indexes {
		output.Write(codec.EncodeRecord(index))
	}
	output.Write(codec.EncodeRecord(footer))
	require.NoError(t, os.WriteFile(path, output.Bytes(), 0o644))
}

func TestStagedStorageService_ConcurrentAuditorsCompaction(t *testing.T) {
	for _, loseResponses := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_response_%t", loseResponses), func(t *testing.T) {
			const entries = 24
			cfg := corruptionE2EConfig(t)
			cfg.Woodpecker.Client.Auditor.CompactionAttemptTimeout = config.NewDurationSecondsFromInt(120)
			cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = config.NewDurationSecondsFromInt(3600)
			cfg.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes = config.NewByteSize(400)
			cfg.Woodpecker.Logstore.FencePolicy.ConditionWrite = "disable"
			cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, 3, t.TempDir(), cfg)
			defer cluster.StopMultiNodeCluster(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			base := newCorruptionClient(t, ctx, cfg, seeds)
			name := fmt.Sprintf("concurrent-auditors-%d", time.Now().UnixNano())
			require.NoError(t, base.CreateLog(ctx, name))
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				_, _ = base.DeleteLogSync(cleanup, name)
			}()
			objects, err := storageclient.NewObjectStorage(ctx, cfg)
			require.NoError(t, err)
			barrier := &auditorCompactionBarrier{
				entered: make(chan string, 2), start: make(chan struct{}),
				published: make(chan struct{}), otherDone: make(chan struct{}), release: make(chan struct{}),
			}
			newHandle := func(role, compactTarget string) (log.LogHandle, *auditorCompactionPool, *auditorCompactionMetadata) {
				pool := &auditorCompactionPool{
					LogStoreClientPool: client.NewLogStoreClientPool(cfg.Woodpecker.Logstore.GRPCConfig.GetClientMaxSendSize(), cfg.Woodpecker.Logstore.GRPCConfig.GetClientMaxRecvSize()),
					role:               role, barrier: barrier, compactTarget: compactTarget, loseResponses: loseResponses,
				}
				t.Cleanup(func() { _ = pool.Close(context.Background()) })
				discovery, err := quorum.NewQuorumDiscovery(ctx, &cfg.Woodpecker.Client.Quorum, pool)
				require.NoError(t, err)
				provider := &auditorCompactionMetadata{MetadataProvider: base.GetMetadataProvider()}
				logMeta, segments, err := provider.OpenLog(ctx, name)
				require.NoError(t, err)
				handle := log.NewLogHandle(name, logMeta.Metadata.LogId, segments, provider, pool, cfg, discovery.SelectQuorum, objects)
				return handle, pool, provider
			}
			oldHandle, oldPool, oldMeta := newHandle("old", "")
			oldWriter, err := oldHandle.OpenLogWriter(ctx)
			require.NoError(t, err)
			defer oldWriter.Close(context.Background())
			want := make([][]byte, entries)
			for i := range entries {
				want[i] = []byte(fmt.Sprintf("entry-%03d-%s", i, strings.Repeat("x", 64+i%5)))
				result := oldWriter.Write(ctx, &log.WriteMessage{Payload: want[i]})
				require.NoError(t, result.Err)
				require.Equal(t, int64(0), result.LogMessageId.SegmentId)
			}
			quorumInfo, err := oldHandle.GetCurrentWritableSegmentHandle(ctx).GetQuorumInfo(ctx)
			require.NoError(t, err)
			require.Len(t, quorumInfo.Nodes, 3)
			newLogHandle, newPool, newMeta := newHandle("new", quorumInfo.Nodes[1])
			// Take over without closing the idle old writer: both real auditor
			// loops survive and independently discover the now-Completed segment.
			newWriter, err := newLogHandle.OpenLogWriter(ctx)
			require.NoError(t, err)
			defer newWriter.Close(context.Background())
			seen := make(map[string]bool)
			for len(seen) < 2 {
				select {
				case role := <-barrier.entered:
					seen[role] = true
				case <-ctx.Done():
					t.Fatal("both auditors must initiate compaction: ", ctx.Err())
				}
			}
			nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)
			for i, node := range nodes {
				_, err = cluster.LeaveNodeWithIndex(t, node)
				require.NoError(t, err)
				groupSize := []int{entries, 3, 1}[i]
				reblockAuditorReplica(t, corruptionDataLogPath(cluster, cfg, node, oldHandle.GetId(), 0), groupSize, entries)
				restartCorruptionNode(t, cluster, node)
			}
			// Reconnect both real client pools and assert that recovery loaded
			// 1, 8 and 24 physical blocks for the same 24 logical entries.
			for i, node := range quorumInfo.Nodes {
				for _, pool := range []*auditorCompactionPool{oldPool, newPool} {
					require.Eventually(t, func() bool {
						cli, err := pool.LogStoreClientPool.GetLogStoreClient(ctx, node)
						if err != nil {
							return false
						}
						lac, err := cli.GetLastAddConfirmed(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, oldHandle.GetId(), 0)
						return err == nil && lac == entries-1
					}, 15*time.Second, 100*time.Millisecond, "restarted replica %s must be ready", node)
				}
				raw, err := os.ReadFile(corruptionDataLogPath(cluster, cfg, nodes[i], oldHandle.GetId(), 0))
				require.NoError(t, err)
				footer, err := codec.ParseFooterFromBytes(raw)
				require.NoError(t, err)
				require.Equal(t, []int32{1, 8, 24}[i], footer.TotalBlocks)
			}
			logNS := metrics.BuildLogNs(cfg.Minio.BucketName, cfg.Minio.RootPath)
			failures := metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logNS, strconv.FormatInt(oldHandle.GetId(), 10), "compact_to_sealed", "error")
			failuresBefore := testutil.ToFloat64(failures)
			close(barrier.start)
			for _, ready := range []<-chan struct{}{barrier.published, barrier.otherDone} {
				select {
				case <-ready:
				case <-ctx.Done():
					t.Fatal("concurrent compactions did not execute: ", ctx.Err())
				}
			}
			if !loseResponses {
				// Establish the CAS ordering explicitly: the new auditor commits
				// Sealed before the held old RPC is allowed to update metadata.
				requireCompactedCleanupSegmentState(t, ctx, newLogHandle, 0, proto.SegmentState_Sealed, 20*time.Second)
			}
			readAll := func(handle log.LogHandle, readerName string) {
				reader, err := handle.OpenLogReader(ctx, &log.LogMessageId{SegmentId: 0, EntryId: 0}, readerName)
				require.NoError(t, err)
				defer reader.Close(context.Background())
				for i := range entries {
					message, err := readEntry(ctx, reader)
					require.NoError(t, err, "entry %d", i)
					require.Equal(t, &log.LogMessageId{SegmentId: 0, EntryId: int64(i)}, message.Id)
					require.Equal(t, want[i], message.Payload)
				}
			}
			// A footer already exists even if both metadata updates have not yet
			// completed. Read through the real SDK while the first RPC is held.
			readAll(newLogHandle, "while-compaction-response-held")
			close(barrier.release)
			requireCompactedCleanupSegmentState(t, ctx, newLogHandle, 0, proto.SegmentState_Sealed, 20*time.Second)
			require.Eventually(t, func() bool {
				return oldMeta.sealedSnapshots.Load() >= 2 && newMeta.sealedSnapshots.Load() >= 2
			}, 15*time.Second, 50*time.Millisecond, "both auditors must continue cycling after the conflict/failure")
			require.Positive(t, oldPool.serverSuccess.Load())
			require.Positive(t, newPool.serverSuccess.Load())
			if loseResponses {
				require.Equal(t, int64(3), newPool.responseErrors.Load())
				require.Greater(t, testutil.ToFloat64(failures), failuresBefore, "one complete auditor compaction pass must fail")
			} else {
				require.Equal(t, int64(1), oldMeta.conflicts.Load(), "the held RPC must lose the real etcd CAS")
				require.Equal(t, failuresBefore, testutil.ToFloat64(failures), "same-LAC metadata conflict must be idempotent")
			}
			fresh, err := base.OpenLog(ctx, name)
			require.NoError(t, err)
			defer fresh.Close(context.Background())
			readAll(fresh, "fresh-reader-after-concurrent-compaction")
			// The surviving writer remains writable, and a reader can cross the
			// compacted segment's boundary into its successor without loss/duplication.
			marker := newWriter.Write(ctx, &log.WriteMessage{Payload: []byte("after-concurrent-compaction")})
			require.NoError(t, marker.Err)
			require.Greater(t, marker.LogMessageId.SegmentId, int64(0))
			reader, err := fresh.OpenLogReader(ctx, &log.LogMessageId{SegmentId: 0, EntryId: 0}, "read-through-compaction")
			require.NoError(t, err)
			defer reader.Close(context.Background())
			for i := range entries {
				message, err := readEntry(ctx, reader)
				require.NoError(t, err)
				require.Equal(t, want[i], message.Payload)
				require.Equal(t, int64(i), message.Id.EntryId)
			}
			message, err := readEntry(ctx, reader)
			require.NoError(t, err)
			require.Equal(t, marker.LogMessageId, message.Id)
			require.Equal(t, []byte("after-concurrent-compaction"), message.Payload)
		})
	}
}
