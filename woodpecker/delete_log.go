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

package woodpecker

import (
	"context"
	stdsync "sync"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

// defaultLogDeleteConcurrency bounds how many logs DeleteAllLogs deletes at once. Logs have
// disjoint object prefixes and disjoint metadata keys, so they are safe to run in parallel;
// the bound exists so a few hundred logs do not fan out at once against etcd and the object
// store.
const defaultLogDeleteConcurrency = 8

// DeleteStats reports what a delete actually removed, tier by tier.
//
// It exists because a delete that removed nothing is indistinguishable, by return value
// alone, from a delete that had nothing to remove: os.RemoveAll on a missing directory
// succeeds and an empty object prefix lists clean. A caller pointed at the wrong
// bucket/rootPath therefore gets a confident success while every byte survives.
//
// None of these counts is an error on its own — a log whose data is already compacted has
// no local directory, and a freshly cleared instance has no objects. They matter in
// aggregate, which is why they are reported rather than judged here.
type DeleteStats struct {
	// Logs whose metadata was deleted.
	Logs int
	// Objects removed from object storage.
	ObjectsDeleted int
	// Successful fence calls, counted per node per log.
	NodesFenced int
	// Of those, the ones that found a local directory to remove. Always 0 for an
	// asynchronous delete, which returns before the reclaim task runs.
	NodesWithLocalData int
}

// Add accumulates another delete's counts.
func (d *DeleteStats) Add(other DeleteStats) {
	d.Logs += other.Logs
	d.ObjectsDeleted += other.ObjectsDeleted
	d.NodesFenced += other.NodesFenced
	d.NodesWithLocalData += other.NodesWithLocalData
}

// deleteLogUnsafe removes every trace of one log: the node-local staged data, the objects in
// object storage, and finally the metadata.
//
// Ordering matters and is not an implementation detail:
//
//  1. MarkLogDeleted fan-out — must come first and must succeed everywhere. EvictLog is what
//     closes the segment processors and raises the deleting gate, i.e. what actually stops a
//     node from writing. Deleting objects while a node is still compacting would let it write
//     a fresh footer afterwards and recreate the orphan we just removed.
//  2. Object storage — driven by the log's prefix, not by segment metadata (see
//     deleteLogObjects for why metadata cannot describe everything that is out there).
//  3. Metadata last, so a failure anywhere above leaves the log discoverable and the whole
//     operation retry-safe.
//
// With sync=true step 1 also reclaims each node's local data before replying, so a caller
// that must observe an empty WAL before restarting Milvus can rely on the return.
//
// Idempotency contract:
//   - GetLogMeta not-found (ErrMetadataRead class) → nil no-op.
//   - MarkLogDeleted is idempotent on the node side.
//   - Object deletion re-enumerates, so an interrupted run resumes from whatever is left.
//   - Metadata is deleted last, so a partial failure is retry-safe.
func deleteLogUnsafe(
	ctx context.Context,
	md meta.MetadataProvider,
	pool client.LogStoreClientPool,
	cfg *config.Configuration,
	storage storageclient.ObjectStorage,
	logName string,
	sync bool,
) (DeleteStats, error) {
	var stats DeleteStats
	// Step 1: resolve logId; treat not-found as a successful no-op.
	logMeta, err := md.GetLogMeta(ctx, logName)
	if err != nil {
		if werr.ErrMetadataRead.Is(err) {
			logger.Ctx(ctx).Info("deleteLogUnsafe: log not found, treating as already deleted",
				zap.String("logName", logName))
			return stats, nil
		}
		return stats, err
	}
	logId := logMeta.Metadata.GetLogId()

	// Step 2: stop every node that could still be writing this log.
	fenced, withData, err := markLogDeletedOnNodes(ctx, md, pool, cfg, logName, logId, sync)
	stats.NodesFenced += fenced
	stats.NodesWithLocalData += withData
	if err != nil {
		return stats, err
	}

	// Step 3: object storage. Only the object-backed modes have anything here; a local-only
	// deployment keeps its data in the node directories reclaimed by step 2.
	if !cfg.Woodpecker.Storage.IsStorageLocal() {
		// Refuse rather than proceed: without a storage client we cannot clean the objects,
		// and deleting the metadata anyway would strand them with nothing left to enumerate
		// them by. Failing here keeps the log discoverable so a retry can finish the job.
		if storage == nil {
			return stats, werr.ErrMetadataWrite.WithCauseErrMsg(
				"object storage client unavailable; refusing to delete metadata while objects remain")
		}
		deleted, _, objErr := deleteLogObjects(ctx, storage, cfg, logId)
		stats.ObjectsDeleted += deleted
		if objErr != nil {
			logger.Ctx(ctx).Warn("deleteLogUnsafe: object cleanup failed, keeping metadata for retry",
				zap.String("logName", logName), zap.Int64("logId", logId), zap.Error(objErr))
			return stats, objErr
		}
	}

	// Step 4: everything else is gone — now the metadata can go.
	if delErr := md.DeleteLogMetadata(ctx, logName, false); delErr != nil {
		logger.Ctx(ctx).Warn("deleteLogUnsafe: DeleteLogMetadata failed",
			zap.String("logName", logName), zap.Error(delErr))
		return stats, delErr
	}
	stats.Logs++

	logger.Ctx(ctx).Info("deleteLogUnsafe: log deleted successfully",
		zap.String("logName", logName), zap.Int64("logId", logId), zap.Bool("sync", sync),
		zap.Int("objectsDeleted", stats.ObjectsDeleted),
		zap.Int("nodesFenced", stats.NodesFenced),
		zap.Int("nodesWithLocalData", stats.NodesWithLocalData))
	return stats, nil
}

// markLogDeletedOnNodes fences the log on every node that could hold data for it.
//
// In service mode the node set comes from the quorum recorded in segment metadata. In the
// object-storage and local modes there is no quorum to read, and the embedded logstore is
// reachable through the pool's local client, so the fan-out is driven by the pool itself.
func markLogDeletedOnNodes(
	ctx context.Context,
	md meta.MetadataProvider,
	pool client.LogStoreClientPool,
	cfg *config.Configuration,
	logName string,
	logId int64,
	sync bool,
) (fenced int, withLocalData int, err error) {
	nodes, err := logQuorumNodes(ctx, md, logName)
	if err != nil {
		return 0, 0, err
	}
	bucketName := cfg.Minio.BucketName
	rootPath := cfg.Minio.RootPath

	if len(nodes) == 0 {
		if cfg.Woodpecker.Storage.IsStorageService() {
			// Service mode with no quorum recorded: every segment was already truncated
			// away, so no node is serving this log. Nothing to fence.
			logger.Ctx(ctx).Info("deleteLogUnsafe: no quorum nodes recorded, nothing to fence",
				zap.String("logName", logName), zap.Int64("logId", logId))
			return 0, 0, nil
		}
		// Embedded deployments (minio / local) run a single in-process logstore and the
		// local pool ignores the target, so any target reaches it. Fence it so it stops
		// serving and — with sync — reclaims its directory before we touch anything else.
		lsClient, getErr := pool.GetLogStoreClient(ctx, "")
		if getErr != nil {
			return 0, 0, getErr
		}
		hadData, markErr := lsClient.MarkLogDeleted(ctx, bucketName, rootPath, logId, sync)
		if markErr != nil {
			return 0, 0, markErr
		}
		if hadData {
			return 1, 1, nil
		}
		return 1, 0, nil
	}

	logger.Ctx(ctx).Info("deleteLogUnsafe: fencing log on quorum nodes",
		zap.String("logName", logName), zap.Int64("logId", logId),
		zap.Strings("nodes", nodes), zap.Bool("sync", sync))

	for _, node := range nodes {
		lsClient, getErr := pool.GetLogStoreClient(ctx, node)
		if getErr != nil {
			logger.Ctx(ctx).Warn("deleteLogUnsafe: failed to get logstore client",
				zap.String("node", node), zap.Error(getErr))
			return fenced, withLocalData, getErr
		}
		hadData, markErr := lsClient.MarkLogDeleted(ctx, bucketName, rootPath, logId, sync)
		if markErr != nil {
			logger.Ctx(ctx).Warn("deleteLogUnsafe: MarkLogDeleted failed",
				zap.String("node", node), zap.Int64("logId", logId), zap.Error(markErr))
			return fenced, withLocalData, markErr
		}
		fenced++
		if hadData {
			withLocalData++
		}
	}
	return fenced, withLocalData, nil
}

// logQuorumNodes returns the distinct quorum endpoints recorded across a log's segments.
func logQuorumNodes(ctx context.Context, md meta.MetadataProvider, logName string) ([]string, error) {
	segMetas, err := md.GetAllSegmentMetadata(ctx, logName)
	if err != nil {
		return nil, err
	}
	nodeSet := make(map[string]struct{})
	for _, seg := range segMetas {
		if seg.Metadata == nil {
			continue
		}
		quorum := seg.Metadata.GetQuorum()
		if quorum == nil {
			continue
		}
		for _, node := range quorum.GetNodes() {
			if node == "" {
				continue
			}
			nodeSet[node] = struct{}{}
		}
	}
	nodes := make([]string, 0, len(nodeSet))
	for node := range nodeSet {
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// deleteAllLogsUnsafe deletes every log, a bounded number at a time.
func deleteAllLogsUnsafe(
	ctx context.Context,
	md meta.MetadataProvider,
	pool client.LogStoreClientPool,
	cfg *config.Configuration,
	storage storageclient.ObjectStorage,
	syncDelete bool,
) (DeleteStats, error) {
	var total DeleteStats
	logs, err := md.ListLogs(ctx)
	if err != nil {
		return total, err
	}
	if len(logs) == 0 {
		return total, nil
	}

	workers := defaultLogDeleteConcurrency
	if len(logs) < workers {
		workers = len(logs)
	}
	var (
		mu       stdsync.Mutex
		firstErr error
		wg       stdsync.WaitGroup
	)
	jobs := make(chan string)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for logName := range jobs {
				stats, delErr := deleteLogUnsafe(ctx, md, pool, cfg, storage, logName, syncDelete)
				mu.Lock()
				total.Add(stats)
				if delErr != nil && firstErr == nil {
					firstErr = delErr
				}
				mu.Unlock()
				if delErr != nil {
					logger.Ctx(ctx).Warn("deleteAllLogsUnsafe: failed to delete log",
						zap.String("logName", logName), zap.Error(delErr))
				}
			}
		}()
	}
	for _, logName := range logs {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return total, ctx.Err()
		case jobs <- logName:
		}
	}
	close(jobs)
	wg.Wait()

	// Report, do not judge. A clear that fenced nodes but found nothing anywhere is the
	// signature of a wrong bucket/rootPath — and also of an instance that was already
	// empty, which is why this warns rather than fails.
	if syncDelete && firstErr == nil && total.Logs > 0 &&
		total.ObjectsDeleted == 0 && total.NodesWithLocalData == 0 {
		logger.Ctx(ctx).Warn("deleteAllLogsUnsafe: deleted metadata but found no data on any tier; "+
			"verify the bucket and rootPath point at this instance's WAL",
			zap.Int("logs", total.Logs), zap.Int("nodesFenced", total.NodesFenced),
			zap.String("bucket", cfg.Minio.BucketName), zap.String("rootPath", cfg.Minio.RootPath))
	}
	return total, firstErr
}
