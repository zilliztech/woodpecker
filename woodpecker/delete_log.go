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
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/retry"
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
	// Nodes that accepted the delete mark, counted per node per log.
	NodesMarked int
	// Of those, the ones that found a local directory to remove. Always 0 for an
	// asynchronous delete, which returns before the reclaim task runs.
	NodesWithLocalData int
	// SkippedNodes maps a node address to the log ids whose delete mark it never received,
	// because it stayed unreachable through every retry and the caller opted into
	// WithSkipUnreachableNodes. Empty otherwise — without that option an unreachable node
	// fails the delete instead.
	//
	// Each entry is staged data left on that node forever: nothing references it once the
	// metadata is gone, and no reclaim will run because the node never got a delete marker.
	// That residue is bounded and harmless — logId never repeats, so a future log cannot
	// land in the same directory — but only an operator can tell scrap hardware from a node
	// that is coming back, so the detail is reported rather than swallowed.
	SkippedNodes map[string][]int64
}

// recordSkippedNode notes that a log's delete mark never reached a node.
func (d *DeleteStats) recordSkippedNode(node string, logId int64) {
	if d.SkippedNodes == nil {
		d.SkippedNodes = make(map[string][]int64, 1)
	}
	d.SkippedNodes[node] = append(d.SkippedNodes[node], logId)
}

// Add accumulates another delete's counts.
func (d *DeleteStats) Add(other DeleteStats) {
	d.Logs += other.Logs
	d.ObjectsDeleted += other.ObjectsDeleted
	d.NodesMarked += other.NodesMarked
	d.NodesWithLocalData += other.NodesWithLocalData
	for node, logIds := range other.SkippedNodes {
		for _, logId := range logIds {
			d.recordSkippedNode(node, logId)
		}
	}
}

// DeleteOption configures a delete.
type DeleteOption func(*deleteOptions)

type deleteOptions struct {
	skipUnreachableNodes bool
}

func newDeleteOptions(opts []DeleteOption) deleteOptions {
	var o deleteOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// WithSkipUnreachableNodes lets a delete finish without a node that stays unreachable
// through every retry, instead of failing.
//
// Off by default, and deliberately not something retries can decide on their own. Retrying
// cannot tell "this node is scrap" from "this node is down for a ten-minute maintenance
// window" — both fail every attempt inside any sane budget, and only the operator knows
// which it is. A network partition is worse still: from the client it is indistinguishable
// from a dead node, and skipping one would delete objects out from under a node that is
// still alive and possibly still writing, since marking a log deleted is also what closes its segment
// processors and raises the deleting gate.
//
// Only transport failures are ever skipped. A reachable node that rejects the mark is
// reporting a real problem, not a connectivity one, and still fails the delete.
//
// What was skipped comes back in DeleteStats.SkippedNodes so the decision leaves a trace.
func WithSkipUnreachableNodes() DeleteOption {
	return func(o *deleteOptions) { o.skipUnreachableNodes = true }
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
	opts deleteOptions,
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
	markStats, err := markLogDeletedOnNodes(ctx, md, pool, cfg, logName, logId, sync, opts)
	stats.Add(markStats)
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
		zap.Int("nodesMarked", stats.NodesMarked),
		zap.Int("nodesWithLocalData", stats.NodesWithLocalData))
	return stats, nil
}

// markLogDeletedOnNodes marks the log deleted on every node that could hold data for it.
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
	opts deleteOptions,
) (stats DeleteStats, err error) {
	nodes, err := logQuorumNodes(ctx, md, logName)
	if err != nil {
		return stats, err
	}
	bucketName := cfg.Minio.BucketName
	rootPath := cfg.Minio.RootPath

	if len(nodes) == 0 {
		if cfg.Woodpecker.Storage.IsStorageService() {
			// Service mode with no quorum recorded: every segment was already truncated
			// away, so no node is serving this log. Nothing to mark.
			logger.Ctx(ctx).Info("deleteLogUnsafe: no quorum nodes recorded, nothing to mark",
				zap.String("logName", logName), zap.Int64("logId", logId))
			return stats, nil
		}
		// Embedded deployments (minio / local) run a single in-process logstore and the
		// local pool ignores the target, so any target reaches it. Mark it so it stops
		// serving and — with sync — reclaims its directory before we touch anything else.
		hadData, deleteMarkErr := markLogDeletedOnNode(ctx, pool, "", bucketName, rootPath, logId, sync)
		if deleteMarkErr != nil {
			return stats, deleteMarkErr
		}
		stats.NodesMarked = 1
		if hadData {
			stats.NodesWithLocalData = 1
		}
		return stats, nil
	}

	logger.Ctx(ctx).Info("deleteLogUnsafe: fencing log on quorum nodes",
		zap.String("logName", logName), zap.Int64("logId", logId),
		zap.Strings("nodes", nodes), zap.Bool("sync", sync))

	for _, node := range nodes {
		hadData, deleteMarkErr := markLogDeletedOnNode(ctx, pool, node, bucketName, rootPath, logId, sync)
		if deleteMarkErr == nil {
			stats.NodesMarked++
			if hadData {
				stats.NodesWithLocalData++
			}
			continue
		}
		// Skipping is opt-in and limited to nodes we could not reach. A reachable node that
		// rejected the mark is reporting a real problem and still fails the delete.
		if opts.skipUnreachableNodes && werr.IsTransportError(deleteMarkErr) {
			logger.Ctx(ctx).Warn("deleteLogUnsafe: node unreachable after retries, skipping as requested; "+
				"its staged data for this log stays on disk",
				zap.String("node", node), zap.String("logName", logName),
				zap.Int64("logId", logId), zap.Error(deleteMarkErr))
			stats.recordSkippedNode(node, logId)
			continue
		}
		logger.Ctx(ctx).Warn("deleteLogUnsafe: failed to mark log deleted on node",
			zap.String("node", node), zap.Int64("logId", logId),
			zap.Bool("transport", werr.IsTransportError(deleteMarkErr)), zap.Error(deleteMarkErr))
		return stats, deleteMarkErr
	}
	return stats, nil
}

// Delete-mark bounds. Package vars so tests can shrink them, matching the convention
// appendFirstResponseTimeout established in logstore_client_remote.go.
var (
	// markAttempts is how many times one node's delete mark is tried before it counts as
	// unreachable. Transport failures only: the pool evicts its cached connection on each
	// one, so a retry re-resolves DNS. That is exactly what a pod mid-rolling-restart or a
	// peer that came back with a new IP needs, and without a retry the eviction is wasted
	// because there is never a next call.
	markAttempts   uint = 3
	markRetrySleep      = time.Second
	markMaxSleep        = 4 * time.Second

	// markAttemptTimeout bounds ONE attempt. grpc.WaitForReady(false) already makes an
	// unconnectable peer fail fast, but it does nothing for a peer that accepts the
	// connection and then never answers — a hung process or a black-holing network — which
	// would otherwise wedge the whole delete with no way out but Ctrl-C.
	//
	// Deliberately generous. A synchronous mark has the node os.RemoveAll its staged
	// directory for that log, which is legitimately slow when the log has a large
	// un-compacted tail. Cutting off a reclaim that is making progress would be worse than
	// the hang this prevents.
	markAttemptTimeout = 2 * time.Minute
)

// markLogDeletedOnNode marks a log deleted on one node, retrying transport failures.
//
// Returns whether the node had local data, and the last error if every attempt failed. The
// error is returned unwrapped so the caller can still classify it with werr.IsTransportError
// when deciding whether skipping is allowed.
func markLogDeletedOnNode(
	ctx context.Context,
	pool client.LogStoreClientPool,
	node, bucketName, rootPath string,
	logId int64,
	sync bool,
) (bool, error) {
	var hadData bool
	err := retry.Do(ctx, func() error {
		attemptCtx, cancel := context.WithTimeout(ctx, markAttemptTimeout)
		defer cancel()

		lsClient, getErr := pool.GetLogStoreClient(attemptCtx, node)
		if getErr != nil {
			return getErr
		}
		var markErr error
		hadData, markErr = lsClient.MarkLogDeleted(attemptCtx, bucketName, rootPath, logId, sync)
		return markErr
	},
		retry.Attempts(markAttempts),
		retry.Sleep(markRetrySleep),
		retry.MaxSleepTime(markMaxSleep),
		// Retry only what a retry can fix. A reachable node that rejects the mark is
		// reporting a real problem; retrying it just turns a clear error into a timeout.
		retry.RetryErr(werr.IsTransportError),
	)
	return hadData, err
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
	opts deleteOptions,
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
				stats, delErr := deleteLogUnsafe(ctx, md, pool, cfg, storage, logName, syncDelete, opts)
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

	// Report, do not judge. A clear that reached nodes but found nothing anywhere is the
	// signature of a wrong bucket/rootPath — and also of an instance that was already
	// empty, which is why this warns rather than fails.
	if syncDelete && firstErr == nil && total.Logs > 0 &&
		total.ObjectsDeleted == 0 && total.NodesWithLocalData == 0 {
		logger.Ctx(ctx).Warn("deleteAllLogsUnsafe: deleted metadata but found no data on any tier; "+
			"verify the bucket and rootPath point at this instance's WAL",
			zap.Int("logs", total.Logs), zap.Int("nodesMarked", total.NodesMarked),
			zap.String("bucket", cfg.Minio.BucketName), zap.String("rootPath", cfg.Minio.RootPath))
	}
	return total, firstErr
}

// sweepParkedLogObjects deletes the objects of logs that are soft-deleted but still parked.
//
// A soft delete parks LogMeta under logs-deleted/ and nothing ever reads that prefix again, so
// parked logs are invisible to ListLogs and therefore to DeleteAllLogs. That was harmless
// while parked records simply accumulated; it stops being harmless when ClearMeta deletes the
// prefix, because for a log deleted by an older client — one that did not clean object storage
// — the parked record is the last thing that knows the objects exist.
//
// Runs before the metadata is cleared, and a failure aborts the clear, on the same principle
// as deleteLogUnsafe: never destroy the handle on data you have not managed to delete.
func sweepParkedLogObjects(
	ctx context.Context,
	md meta.MetadataProvider,
	cfg *config.Configuration,
	storage storageclient.ObjectStorage,
) (int, error) {
	if cfg.Woodpecker.Storage.IsStorageLocal() {
		return 0, nil // no object storage; parked logs left only node-local data
	}
	ids, err := md.ListParkedLogIds(ctx)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	if storage == nil {
		return 0, werr.ErrMetadataWrite.WithCauseErrMsg(
			"object storage client unavailable; refusing to clear metadata while parked logs may still have objects")
	}
	total := 0
	for _, logId := range ids {
		deleted, _, delErr := deleteLogObjects(ctx, storage, cfg, logId)
		total += deleted
		if delErr != nil {
			logger.Ctx(ctx).Warn("sweepParkedLogObjects: cleanup failed, keeping parked metadata for retry",
				zap.Int64("logId", logId), zap.Error(delErr))
			return total, delErr
		}
	}
	logger.Ctx(ctx).Info("sweepParkedLogObjects: reclaimed objects of soft-deleted logs",
		zap.Int("parkedLogs", len(ids)), zap.Int("objectsDeleted", total))
	return total, nil
}
