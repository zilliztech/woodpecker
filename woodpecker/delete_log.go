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
) error {
	// Step 1: resolve logId; treat not-found as a successful no-op.
	logMeta, err := md.GetLogMeta(ctx, logName)
	if err != nil {
		if werr.ErrMetadataRead.Is(err) {
			logger.Ctx(ctx).Info("deleteLogUnsafe: log not found, treating as already deleted",
				zap.String("logName", logName))
			return nil
		}
		return err
	}
	logId := logMeta.Metadata.GetLogId()

	// Step 2: stop every node that could still be writing this log.
	if err := markLogDeletedOnNodes(ctx, md, pool, cfg, logName, logId, sync); err != nil {
		return err
	}

	// Step 3: object storage. Only the object-backed modes have anything here; a local-only
	// deployment keeps its data in the node directories reclaimed by step 2.
	if !cfg.Woodpecker.Storage.IsStorageLocal() {
		// Refuse rather than proceed: without a storage client we cannot clean the objects,
		// and deleting the metadata anyway would strand them with nothing left to enumerate
		// them by. Failing here keeps the log discoverable so a retry can finish the job.
		if storage == nil {
			return werr.ErrMetadataWrite.WithCauseErrMsg(
				"object storage client unavailable; refusing to delete metadata while objects remain")
		}
		if _, _, objErr := deleteLogObjects(ctx, storage, cfg, logId); objErr != nil {
			logger.Ctx(ctx).Warn("deleteLogUnsafe: object cleanup failed, keeping metadata for retry",
				zap.String("logName", logName), zap.Int64("logId", logId), zap.Error(objErr))
			return objErr
		}
	}

	// Step 4: everything else is gone — now the metadata can go.
	if delErr := md.DeleteLogMetadata(ctx, logName, false); delErr != nil {
		logger.Ctx(ctx).Warn("deleteLogUnsafe: DeleteLogMetadata failed",
			zap.String("logName", logName), zap.Error(delErr))
		return delErr
	}

	logger.Ctx(ctx).Info("deleteLogUnsafe: log deleted successfully",
		zap.String("logName", logName), zap.Int64("logId", logId), zap.Bool("sync", sync))
	return nil
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
) error {
	nodes, err := logQuorumNodes(ctx, md, logName)
	if err != nil {
		return err
	}
	bucketName := cfg.Minio.BucketName
	rootPath := cfg.Minio.RootPath

	if len(nodes) == 0 {
		if cfg.Woodpecker.Storage.IsStorageService() {
			// Service mode with no quorum recorded: every segment was already truncated
			// away, so no node is serving this log. Nothing to fence.
			logger.Ctx(ctx).Info("deleteLogUnsafe: no quorum nodes recorded, nothing to fence",
				zap.String("logName", logName), zap.Int64("logId", logId))
			return nil
		}
		// Embedded deployments (minio / local) run a single in-process logstore and the
		// local pool ignores the target, so any target reaches it. Fence it so it stops
		// serving and — with sync — reclaims its directory before we touch anything else.
		lsClient, getErr := pool.GetLogStoreClient(ctx, "")
		if getErr != nil {
			return getErr
		}
		return lsClient.MarkLogDeleted(ctx, bucketName, rootPath, logId, sync)
	}

	logger.Ctx(ctx).Info("deleteLogUnsafe: fencing log on quorum nodes",
		zap.String("logName", logName), zap.Int64("logId", logId),
		zap.Strings("nodes", nodes), zap.Bool("sync", sync))

	for _, node := range nodes {
		lsClient, getErr := pool.GetLogStoreClient(ctx, node)
		if getErr != nil {
			logger.Ctx(ctx).Warn("deleteLogUnsafe: failed to get logstore client",
				zap.String("node", node), zap.Error(getErr))
			return getErr
		}
		if markErr := lsClient.MarkLogDeleted(ctx, bucketName, rootPath, logId, sync); markErr != nil {
			logger.Ctx(ctx).Warn("deleteLogUnsafe: MarkLogDeleted failed",
				zap.String("node", node), zap.Int64("logId", logId), zap.Error(markErr))
			return markErr
		}
	}
	return nil
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
) error {
	logs, err := md.ListLogs(ctx)
	if err != nil {
		return err
	}
	if len(logs) == 0 {
		return nil
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
				if delErr := deleteLogUnsafe(ctx, md, pool, cfg, storage, logName, syncDelete); delErr != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = delErr
					}
					mu.Unlock()
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
			return ctx.Err()
		case jobs <- logName:
		}
	}
	close(jobs)
	wg.Wait()
	return firstErr
}
