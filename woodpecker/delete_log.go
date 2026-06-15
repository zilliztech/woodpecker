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

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

// deleteLogUnsafe implements the client-side log-deletion composition:
//  1. Resolve the logId from metadata; idempotent if already gone.
//  2. Collect the union of quorum node endpoints from all segment metadata.
//  3. Fan out MarkLogDeleted to each distinct node.
//  4. Only after ALL nodes are marked, call DeleteLogMetadata(logName, false).
//
// Idempotency contract:
//   - GetLogMeta not-found (ErrMetadataRead class) → nil no-op.
//   - MarkLogDeleted is idempotent on the node side.
//   - Metadata is deleted last, so a partial failure is retry-safe.
func deleteLogUnsafe(
	ctx context.Context,
	md meta.MetadataProvider,
	pool client.LogStoreClientPool,
	cfg *config.Configuration,
	logName string,
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

	// Step 2: collect all distinct quorum-node endpoints from segment metadata.
	segMetas, err := md.GetAllSegmentMetadata(ctx, logName)
	if err != nil {
		return err
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

	// Step 3: fan out MarkLogDeleted to every distinct node.
	bucketName := cfg.Minio.BucketName
	rootPath := cfg.Minio.RootPath

	for node := range nodeSet {
		lsClient, getErr := pool.GetLogStoreClient(ctx, node)
		if getErr != nil {
			logger.Ctx(ctx).Warn("deleteLogUnsafe: failed to get logstore client",
				zap.String("node", node), zap.Error(getErr))
			return getErr
		}
		if markErr := lsClient.MarkLogDeleted(ctx, bucketName, rootPath, logId); markErr != nil {
			logger.Ctx(ctx).Warn("deleteLogUnsafe: MarkLogDeleted failed",
				zap.String("node", node), zap.Int64("logId", logId), zap.Error(markErr))
			return markErr
		}
	}

	// Step 4: all nodes marked — now remove the metadata.
	if delErr := md.DeleteLogMetadata(ctx, logName, false); delErr != nil {
		logger.Ctx(ctx).Warn("deleteLogUnsafe: DeleteLogMetadata failed",
			zap.String("logName", logName), zap.Error(delErr))
		return delErr
	}

	logger.Ctx(ctx).Info("deleteLogUnsafe: log deleted successfully",
		zap.String("logName", logName), zap.Int64("logId", logId))
	return nil
}

// deleteAllLogsUnsafe lists all logs and calls deleteLogUnsafe for each one.
func deleteAllLogsUnsafe(
	ctx context.Context,
	md meta.MetadataProvider,
	pool client.LogStoreClientPool,
	cfg *config.Configuration,
) error {
	logs, err := md.ListLogs(ctx)
	if err != nil {
		return err
	}
	for _, logName := range logs {
		if err := deleteLogUnsafe(ctx, md, pool, cfg, logName); err != nil {
			return err
		}
	}
	return nil
}
