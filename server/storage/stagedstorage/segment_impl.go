// Copyright (C) 2025 Zilliz. All rights reserved.
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

package stagedstorage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/server/storage"
)

const (
	SegmentScopeName = "StagedSegment"
)

var _ storage.Segment = (*StagedSegmentImpl)(nil)

// StagedSegmentImpl cleans data based on the segment state. If it is not compacted,
// it cleans files like diskSegmentImpl. If it is compacted, it cleans objects like minio SegmentImpl.
// TODO reuse code with Disk&Minio SegmentImpl
type StagedSegmentImpl struct {
	mu              sync.Mutex
	cfg             *config.Configuration
	logId           int64
	segmentId       int64
	segmentDir      string
	segmentFilePath string
	// minio related fields
	bucket         string
	segmentFileKey string
	client         minioHandler.MinioHandler
}

// NewStagedSegmentImpl is used to create a new Segment, which is used to write data to both local and object storage
func NewStagedSegmentImpl(ctx context.Context, bucket string, baseDir string, logId int64, segId int64, client minioHandler.MinioHandler, cfg *config.Configuration) storage.Segment {
	segmentDir := getSegmentDir(baseDir, logId, segId)
	filePath := getSegmentFilePath(baseDir, logId, segId)
	segmentFileKey := fmt.Sprintf("%d/%d", logId, segId)

	logger.Ctx(ctx).Debug("new StagedSegmentImpl created",
		zap.String("segmentFilePath", filePath),
		zap.String("segmentFileKey", segmentFileKey),
		zap.String("bucket", bucket))

	segmentImpl := &StagedSegmentImpl{
		cfg:             cfg,
		logId:           logId,
		segmentId:       segId,
		segmentDir:      segmentDir,
		segmentFilePath: filePath,
		bucket:          bucket,
		segmentFileKey:  segmentFileKey,
		client:          client,
	}
	return segmentImpl
}

func (rs *StagedSegmentImpl) DeleteFileData(ctx context.Context, flag int) (int, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "DeleteFileData")
	defer sp.End()
	rs.mu.Lock()
	defer rs.mu.Unlock()

	startTime := time.Now()
	logId := fmt.Sprintf("%d", rs.logId)

	logger.Ctx(ctx).Info("Starting to delete segment data (minio + local)",
		zap.String("segmentDir", rs.segmentDir),
		zap.String("segmentFileKey", rs.segmentFileKey),
		zap.String("bucket", rs.bucket),
		zap.Int("flag", flag))

	totalDeleted := 0
	var allErrors []error

	// Step 1: Delete minio objects first
	if rs.client != nil {
		minioDeleted, minioErr := rs.deleteMinioObjects(ctx, flag)
		totalDeleted += minioDeleted
		if minioErr != nil {
			logger.Ctx(ctx).Warn("Failed to delete some minio objects",
				zap.String("segmentFileKey", rs.segmentFileKey),
				zap.Error(minioErr))
			allErrors = append(allErrors, minioErr)
		}
	}

	// Step 2: Delete local files
	localDeleted, localErr := rs.deleteLocalFiles(ctx, flag)
	totalDeleted += localDeleted
	if localErr != nil {
		logger.Ctx(ctx).Warn("Failed to delete some local files",
			zap.String("segmentDir", rs.segmentDir),
			zap.Error(localErr))
		allErrors = append(allErrors, localErr)
	}

	// Update metrics
	if len(allErrors) > 0 {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, "delete_segment", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "delete_segment", "error").Observe(float64(time.Since(startTime).Milliseconds()))
	} else {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, "delete_segment", "success").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "delete_segment", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	}

	logger.Ctx(ctx).Info("Completed quorum segment deletion",
		zap.String("segmentDir", rs.segmentDir),
		zap.String("segmentFileKey", rs.segmentFileKey),
		zap.Int("totalDeleted", totalDeleted),
		zap.Int("errorCount", len(allErrors)))

	if len(allErrors) > 0 {
		return totalDeleted, fmt.Errorf("failed to delete some files, errors: %v", allErrors)
	}
	return totalDeleted, nil
}

// deleteMinioObjects deletes objects from minio storage
func (rs *StagedSegmentImpl) deleteMinioObjects(ctx context.Context, flag int) (int, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "deleteMinioObjects")
	defer sp.End()

	logger.Ctx(ctx).Info("Starting to delete minio objects",
		zap.String("segmentFileKey", rs.segmentFileKey),
		zap.String("bucket", rs.bucket),
		zap.Int("flag", flag))

	// List all objects in the segment directory
	listPrefix := fmt.Sprintf("%s/", rs.segmentFileKey)
	objectCh := rs.client.ListObjects(ctx, rs.bucket, listPrefix, false, minio.ListObjectsOptions{})

	var objectsToDelete []string
	var deletedCount int
	var errorCount int

	// Collect all objects to delete
	for objInfo := range objectCh {
		if objInfo.Err != nil {
			logger.Ctx(ctx).Warn("error listing objects during deletion",
				zap.String("segmentFileKey", rs.segmentFileKey),
				zap.Error(objInfo.Err))
			return deletedCount, objInfo.Err
		}

		// Determine what to delete based on flag
		shouldDelete := false
		switch flag {
		case 0:
			// Delete all blocks and footer
			if strings.HasSuffix(objInfo.Key, ".blk") {
				shouldDelete = true
			}
		case 1: // Delete only regular blocks (not merged)
			if strings.HasSuffix(objInfo.Key, ".blk") && !strings.Contains(objInfo.Key, "/m_") && !strings.Contains(objInfo.Key, "/footer.blk") {
				shouldDelete = true
			}
		case 2: // Delete only merged blocks
			if strings.HasSuffix(objInfo.Key, ".blk") && strings.Contains(objInfo.Key, "/m_") {
				shouldDelete = true
			}
		default:
			// Delete all files including footer
			shouldDelete = strings.HasSuffix(objInfo.Key, ".blk")
		}

		if shouldDelete {
			objectsToDelete = append(objectsToDelete, objInfo.Key)
		}
	}

	logger.Ctx(ctx).Info("collected minio objects for deletion",
		zap.String("segmentFileKey", rs.segmentFileKey),
		zap.Int("objectCount", len(objectsToDelete)),
		zap.Int("flag", flag))

	// Delete objects
	for _, objectKey := range objectsToDelete {
		err := rs.client.RemoveObject(ctx, rs.bucket, objectKey, minio.RemoveObjectOptions{})
		if err != nil {
			// Log error but continue with other deletions
			logger.Ctx(ctx).Warn("failed to delete minio object",
				zap.String("segmentFileKey", rs.segmentFileKey),
				zap.String("objectKey", objectKey),
				zap.Error(err))
			errorCount++
		} else {
			logger.Ctx(ctx).Debug("successfully deleted minio object",
				zap.String("segmentFileKey", rs.segmentFileKey),
				zap.String("objectKey", objectKey))
			deletedCount++
		}
	}

	logger.Ctx(ctx).Info("minio objects deletion completed",
		zap.String("segmentFileKey", rs.segmentFileKey),
		zap.Int("deletedCount", deletedCount),
		zap.Int("errorCount", errorCount))

	if errorCount > 0 {
		return deletedCount, fmt.Errorf("failed to delete %d out of %d minio objects", errorCount, len(objectsToDelete))
	}

	return deletedCount, nil
}

// deleteLocalFiles deletes files from local filesystem
func (rs *StagedSegmentImpl) deleteLocalFiles(ctx context.Context, flag int) (int, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "deleteLocalFiles")
	defer sp.End()

	logger.Ctx(ctx).Info("Starting to delete local files",
		zap.String("segmentDir", rs.segmentDir),
		zap.Int("flag", flag))

	// Read directory contents
	entries, err := os.ReadDir(rs.segmentDir)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Ctx(ctx).Info("Local directory does not exist, nothing to delete",
				zap.String("segmentDir", rs.segmentDir))
			return 0, nil
		}
		return 0, err
	}

	var deleteErrors []error
	deletedCount := 0

	// Filter and delete segment files
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		shouldDelete := false
		fileName := entry.Name()

		// Determine what to delete based on flag and file type
		switch flag {
		case 0:
			// Delete all segment-related files
			shouldDelete = strings.HasSuffix(fileName, ".log") ||
				strings.HasSuffix(fileName, ".lock") ||
				strings.HasSuffix(fileName, ".fence")
		case 1, 2:
			// For partial deletion, only delete .log files (main segment data)
			shouldDelete = strings.HasSuffix(fileName, ".log")
		default:
			// Delete all files
			shouldDelete = true
		}

		if shouldDelete {
			filePath := filepath.Join(rs.segmentDir, fileName)

			// Delete file
			if err := os.Remove(filePath); err != nil {
				logger.Ctx(ctx).Warn("Failed to delete local file",
					zap.String("filePath", filePath),
					zap.Error(err))
				deleteErrors = append(deleteErrors, err)
			} else {
				logger.Ctx(ctx).Debug("Successfully deleted local file",
					zap.String("filePath", filePath))
				deletedCount++
			}
		}
	}

	logger.Ctx(ctx).Info("local files deletion completed",
		zap.String("segmentDir", rs.segmentDir),
		zap.Int("deletedCount", deletedCount),
		zap.Int("errorCount", len(deleteErrors)))

	if len(deleteErrors) > 0 {
		return deletedCount, fmt.Errorf("failed to delete %d local files", len(deleteErrors))
	}

	return deletedCount, nil
}
