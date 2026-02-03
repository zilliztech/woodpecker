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
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/objectstorage"
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
	rootPath       string
	segmentFileKey string
	client         objectstorage.ObjectStorage
	logIdStr       string // for metrics label only
	nsStr          string // for metrics namespace label
}

// NewStagedSegmentImpl is used to create a new Segment, which is used to write data to both local and object storage
func NewStagedSegmentImpl(ctx context.Context, bucket string, rootPath string, localBaseDir string, logId int64, segId int64, storageCli objectstorage.ObjectStorage, cfg *config.Configuration) storage.Segment {
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	segmentFileKey := fmt.Sprintf("%d/%d", logId, segId)

	logger.Ctx(ctx).Debug("new StagedSegmentImpl created",
		zap.String("segmentFilePath", filePath),
		zap.String("segmentFileKey", segmentFileKey),
		zap.String("bucket", bucket),
		zap.String("rootPath", rootPath))

	segmentImpl := &StagedSegmentImpl{
		cfg:             cfg,
		logId:           logId,
		segmentId:       segId,
		segmentDir:      segmentDir,
		segmentFilePath: filePath,
		bucket:          bucket,
		rootPath:        rootPath,
		segmentFileKey:  segmentFileKey,
		client:          storageCli,
		logIdStr:        strconv.FormatInt(logId, 10),
		nsStr:           bucket + "/" + rootPath,
	}
	return segmentImpl
}

func (rs *StagedSegmentImpl) DeleteFileData(ctx context.Context, flag int) (int, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "DeleteFileData")
	defer sp.End()
	rs.mu.Lock()
	defer rs.mu.Unlock()

	startTime := time.Now()
	logId := strconv.FormatInt(rs.logId, 10)

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
	listPrefix := fmt.Sprintf("%s/%s", rs.rootPath, rs.segmentFileKey)
	type objectToDelete struct {
		path string
		size int64
	}
	var objectsToDelete []objectToDelete
	var deletedCount int
	var errorCount int

	// Collect all objects to delete using WalkWithObjects
	walkErr := rs.client.WalkWithObjects(ctx, rs.bucket, listPrefix, false, func(objInfo *objectstorage.ChunkObjectInfo) bool {
		// Determine what to delete based on flag
		shouldDelete := false
		switch flag {
		case 0:
			// Delete all blocks
			if strings.HasSuffix(objInfo.FilePath, ".blk") {
				shouldDelete = true
			}
		case 1: // Delete only regular blocks (not merged)
			if strings.HasSuffix(objInfo.FilePath, ".blk") && !strings.Contains(objInfo.FilePath, "/m_") {
				shouldDelete = true
			}
		case 2: // Delete only merged blocks
			if strings.HasSuffix(objInfo.FilePath, ".blk") && strings.Contains(objInfo.FilePath, "/m_") {
				shouldDelete = true
			}
		default:
			// Delete all files
			shouldDelete = false
		}

		// Skip lock files unless explicitly deleting all
		if strings.HasSuffix(objInfo.FilePath, ".lock") && flag == 0 {
			shouldDelete = true
		}

		if shouldDelete {
			objectsToDelete = append(objectsToDelete, objectToDelete{path: objInfo.FilePath, size: objInfo.Size})
		}
		return true // continue walking
	}, rs.nsStr, rs.logIdStr)
	if walkErr != nil {
		logger.Ctx(ctx).Warn("error listing blocks during deletion",
			zap.String("segmentFileKey", rs.segmentFileKey),
			zap.Error(walkErr))
		return deletedCount, walkErr
	}

	logger.Ctx(ctx).Info("collected objects for deletion",
		zap.String("segmentFileKey", rs.segmentFileKey),
		zap.Int("objectCount", len(objectsToDelete)),
		zap.Int("flag", flag))

	// Delete objects
	for _, obj := range objectsToDelete {
		err := rs.client.RemoveObject(ctx, rs.bucket, obj.path, rs.nsStr, rs.logIdStr)
		if err != nil {
			// Log error but continue with other deletions
			logger.Ctx(ctx).Warn("failed to delete block",
				zap.String("segmentFileKey", rs.segmentFileKey),
				zap.String("objectKey", obj.path),
				zap.Error(err))
			errorCount++
		} else {
			logger.Ctx(ctx).Debug("successfully deleted block",
				zap.String("segmentFileKey", rs.segmentFileKey),
				zap.String("objectKey", obj.path))
			deletedCount++
			metrics.WpObjectStorageStoredBytes.WithLabelValues(rs.nsStr, rs.logIdStr).Sub(float64(obj.size))
			metrics.WpObjectStorageStoredObjects.WithLabelValues(rs.nsStr, rs.logIdStr).Dec()
		}
	}

	logger.Ctx(ctx).Info("segment blocks deletion completed",
		zap.String("segmentFileKey", rs.segmentFileKey),
		zap.Int("deletedCount", deletedCount),
		zap.Int("errorCount", errorCount),
		zap.Int("flag", flag))

	if errorCount > 0 {
		return deletedCount, fmt.Errorf("failed to delete %d out of %d objects", errorCount, len(objectsToDelete))
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
			isDataFile := fileName == "data.log"

			// Get file size before deleting (only needed for data files)
			var fileSize int64
			if isDataFile {
				if info, infoErr := entry.Info(); infoErr == nil {
					fileSize = info.Size()
				}
			}

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
				if isDataFile {
					metrics.WpFileStoredBytes.WithLabelValues(rs.nsStr, rs.logIdStr).Sub(float64(fileSize))
					metrics.WpFileStoredCount.WithLabelValues(rs.nsStr, rs.logIdStr).Dec()
				}
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
