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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
)

// defaultObjectDeleteConcurrency bounds the in-flight deletes for a single log. Object
// stores rate-limit per prefix, and a log can hold thousands of blocks, so an unbounded
// fan-out buys nothing and risks throttling the rest of the cluster.
const defaultObjectDeleteConcurrency = 16

// Object names woodpecker writes under {rootPath}/{logId}/{segmentId}/.
// Keep in sync with getBlockKey / getMergedBlockKey / getFooterBlockKey / getSegmentLockKey
// in server/storage/objectstorage/writer_impl.go.
var (
	blockObjectRe       = regexp.MustCompile(`^\d+\.blk$`)
	mergedBlockObjectRe = regexp.MustCompile(`^m_\d+\.blk$`)
)

const (
	footerObjectName = "footer.blk"
	lockObjectName   = "write.lock"
)

// logObjectPrefix returns the object-storage prefix holding everything for one log.
//
// baseDir is Minio.RootPath: the writer builds keys as {baseDir}/{logId}/{segmentId}/...
// (getSegmentFileKey), and logIds are never reused, so this prefix is exclusive to one log
// for the lifetime of the instance — including against any log created later.
func logObjectPrefix(rootPath string, logId int64) string {
	return fmt.Sprintf("%s/%d/", strings.TrimSuffix(rootPath, "/"), logId)
}

// isLogObject reports whether a key relative to a log's prefix is one woodpecker wrote.
//
// This is a guard, not an optimisation. Deleting the prefix wholesale would turn a wrong
// bucket / rootPath argument into a mass deletion of somebody else's data; matching the
// known layout bounds the damage to objects woodpecker could plausibly have written.
func isLogObject(rel string) bool {
	segment, name, found := strings.Cut(rel, "/")
	if !found || strings.Contains(name, "/") {
		return false
	}
	// The segment directory must be a plain number — a second guard on the path shape.
	if _, err := strconv.ParseInt(segment, 10, 64); err != nil {
		return false
	}
	return name == footerObjectName || name == lockObjectName ||
		blockObjectRe.MatchString(name) || mergedBlockObjectRe.MatchString(name)
}

// deleteLogObjects removes a log's objects from object storage.
//
// It enumerates the log's prefix rather than walking segment metadata: metadata cannot
// describe objects it never learned about — segments whose metadata write failed after the
// objects landed, write locks, partial blocks from an aborted flush, and blocks whose
// per-segment cleanup failed and was tolerated with a warn. Enumerating the prefix also
// reclaims orphans that accumulated before this code existed.
//
// Idempotent: a prefix with nothing left under it is a successful no-op.
//
// Returns the number of objects deleted and the keys that were left in place because they
// did not match woodpecker's layout. A non-empty skip list means either a new object type
// was introduced without updating the matcher, or the caller aimed at the wrong prefix.
// Both are worth surfacing rather than swallowing.
func deleteLogObjects(ctx context.Context, storage storageclient.ObjectStorage, cfg *config.Configuration, logId int64) (int, []string, error) {
	if storage == nil {
		return 0, nil, nil
	}
	bucket := cfg.Minio.BucketName
	prefix := logObjectPrefix(cfg.Minio.RootPath, logId)
	logNs := metrics.BuildLogNs(bucket, cfg.Minio.RootPath)
	logIdStr := strconv.FormatInt(logId, 10)

	var matched, skipped []string
	walkErr := storage.WalkWithObjects(ctx, bucket, prefix, true, func(obj *storageclient.ChunkObjectInfo) bool {
		rel := strings.TrimPrefix(obj.FilePath, prefix)
		if isLogObject(rel) {
			matched = append(matched, obj.FilePath)
		} else {
			skipped = append(skipped, obj.FilePath)
		}
		return true
	}, logNs, logIdStr)
	if walkErr != nil {
		return 0, skipped, walkErr
	}
	if len(matched) == 0 {
		logger.Ctx(ctx).Info("deleteLogObjects: nothing to delete",
			zap.String("prefix", prefix), zap.Int("skipped", len(skipped)))
		return 0, skipped, nil
	}

	deleted, err := removeObjectsConcurrently(ctx, storage, bucket, matched, logNs, logIdStr)
	logger.Ctx(ctx).Info("deleteLogObjects done",
		zap.String("prefix", prefix), zap.Int("deleted", deleted),
		zap.Int("skipped", len(skipped)), zap.Error(err))
	if len(skipped) > 0 {
		logger.Ctx(ctx).Warn("deleteLogObjects: objects under the log prefix did not match the expected layout and were left in place",
			zap.String("prefix", prefix), zap.Strings("sample", firstN(skipped, 10)),
			zap.Int("total", len(skipped)))
	}
	return deleted, skipped, err
}

// removeObjectsConcurrently deletes keys with a bounded worker pool and returns the first
// error, after every worker has drained. Partial progress is fine: the caller's retry
// re-enumerates and only sees what is left.
func removeObjectsConcurrently(ctx context.Context, storage storageclient.ObjectStorage, bucket string, keys []string, logNs, logIdStr string) (int, error) {
	workers := defaultObjectDeleteConcurrency
	if len(keys) < workers {
		workers = len(keys)
	}
	var (
		mu       sync.Mutex
		deleted  int
		firstErr error
		wg       sync.WaitGroup
	)
	jobs := make(chan string)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				err := storage.RemoveObject(ctx, bucket, key, logNs, logIdStr)
				mu.Lock()
				switch {
				case err == nil, storage.IsObjectNotExistsError(err):
					// Already gone counts as deleted: this operation is idempotent.
					deleted++
				default:
					if firstErr == nil {
						firstErr = err
					}
					logger.Ctx(ctx).Warn("deleteLogObjects: failed to remove object",
						zap.String("key", key), zap.Error(err))
				}
				mu.Unlock()
			}
		}()
	}
	for _, key := range keys {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return deleted, ctx.Err()
		case jobs <- key:
		}
	}
	close(jobs)
	wg.Wait()
	return deleted, firstErr
}

func firstN(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
