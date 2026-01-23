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

package disk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/server/storage"
)

const (
	SegmentScopeName = "DiskSegment"
)

var _ storage.Segment = (*DiskSegmentImpl)(nil)

type DiskSegmentImpl struct {
	mu              sync.Mutex
	cfg             *config.Configuration
	logId           int64
	segmentId       int64
	segmentDir      string
	segmentFilePath string
}

// NewDiskSegmentImpl is used to create a new Segment, which is used to write data to object storage
func NewDiskSegmentImpl(ctx context.Context, baseDir string, logId int64, segId int64, cfg *config.Configuration) storage.Segment {
	segmentDir := getSegmentDir(baseDir, logId, segId)
	filePath := getSegmentFilePath(baseDir, logId, segId)
	logger.Ctx(ctx).Debug("new SegmentImpl created", zap.String("segmentFilePath", filePath))
	segmentImpl := &DiskSegmentImpl{
		cfg:             cfg,
		logId:           logId,
		segmentId:       segId,
		segmentDir:      segmentDir,
		segmentFilePath: filePath,
	}
	return segmentImpl
}

func (rs *DiskSegmentImpl) DeleteFileData(ctx context.Context, flag int) (int, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "DeleteFileData")
	defer sp.End()
	rs.mu.Lock()
	defer rs.mu.Unlock()

	startTime := time.Now()
	logId := fmt.Sprintf("%d", rs.logId)

	logger.Ctx(ctx).Info("Starting to delete segment files",
		zap.String("segmentDir", rs.segmentDir),
		zap.Int("flag", flag))

	// Read directory contents
	entries, err := os.ReadDir(rs.segmentDir)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Ctx(ctx).Info("Directory does not exist, nothing to delete",
				zap.String("segmentDir", rs.segmentDir))
			return 0, nil
		}
		return 0, err
	}

	var deleteErrors []error
	deletedCount := 0

	// Delete files and directories based on flag
	for _, entry := range entries {
		fileName := entry.Name()
		filePath := filepath.Join(rs.segmentDir, fileName)
		shouldDelete := false

		// Handle fence directories (directories named like {blockId}.blk)
		if entry.IsDir() {
			// Fence directories are named like "0.blk", "1.blk", etc.
			if strings.HasSuffix(fileName, ".blk") && !strings.HasPrefix(fileName, "m_") {
				// This is likely a fence directory
				switch flag {
				case 0: // Delete all
					shouldDelete = true
				case 1: // Delete only original blocks (not merged)
					shouldDelete = true
				}
			}
			if shouldDelete {
				if err := os.RemoveAll(filePath); err != nil {
					logger.Ctx(ctx).Warn("Failed to delete fence directory",
						zap.String("path", filePath),
						zap.Error(err))
					deleteErrors = append(deleteErrors, err)
				} else {
					logger.Ctx(ctx).Debug("Successfully deleted fence directory",
						zap.String("path", filePath))
					deletedCount++
				}
			}
			continue
		}

		// Determine what to delete based on flag and file type
		switch flag {
		case 0: // Delete all segment-related files
			shouldDelete = rs.shouldDeleteFile(fileName)
		case 1: // Delete only original blocks (not merged)
			shouldDelete = rs.shouldDeleteOriginalBlocks(fileName)
		case 2: // Delete only merged blocks
			shouldDelete = rs.shouldDeleteMergedBlocks(fileName)
		default:
			// Delete all segment-related files
			shouldDelete = rs.shouldDeleteFile(fileName)
		}

		if shouldDelete {
			if err := os.Remove(filePath); err != nil {
				logger.Ctx(ctx).Warn("Failed to delete file",
					zap.String("filePath", filePath),
					zap.Error(err))
				deleteErrors = append(deleteErrors, err)
			} else {
				logger.Ctx(ctx).Debug("Successfully deleted file",
					zap.String("filePath", filePath))
				deletedCount++
			}
		}
	}

	// Update metrics
	if len(deleteErrors) > 0 {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, "delete_segment", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "delete_segment", "error").Observe(float64(time.Since(startTime).Milliseconds()))
	} else {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, "delete_segment", "success").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "delete_segment", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	}

	logger.Ctx(ctx).Info("Completed segment deletion",
		zap.String("segmentDir", rs.segmentDir),
		zap.Int("deletedCount", deletedCount),
		zap.Int("errorCount", len(deleteErrors)))

	if len(deleteErrors) > 0 {
		return deletedCount, fmt.Errorf("failed to delete %d segment files", len(deleteErrors))
	}
	return deletedCount, nil
}

// shouldDeleteFile determines if a file should be deleted (flag=0, delete all)
func (rs *DiskSegmentImpl) shouldDeleteFile(fileName string) bool {
	// Legacy single-file format
	if strings.HasSuffix(fileName, ".log") {
		return true
	}
	// Block files (N.blk, m_N.blk)
	if strings.HasSuffix(fileName, ".blk") {
		return true
	}
	// Inflight files (N.blk.inflight, m_N.blk.inflight)
	if strings.HasSuffix(fileName, ".blk.inflight") {
		return true
	}
	// Completed files (N.blk.completed, m_N.blk.completed)
	if strings.HasSuffix(fileName, ".blk.completed") {
		return true
	}
	// Lock files
	if strings.HasSuffix(fileName, ".lock") {
		return true
	}
	// Fence flag files
	if strings.HasSuffix(fileName, ".fence") {
		return true
	}
	return false
}

// shouldDeleteOriginalBlocks determines if a file should be deleted (flag=1, original blocks only)
func (rs *DiskSegmentImpl) shouldDeleteOriginalBlocks(fileName string) bool {
	// Legacy single-file format
	if strings.HasSuffix(fileName, ".log") {
		return true
	}
	// Skip merged blocks (m_N.blk)
	if strings.HasPrefix(fileName, "m_") {
		return false
	}
	// Original block files (N.blk, but not footer.blk)
	if strings.HasSuffix(fileName, ".blk") && fileName != "footer.blk" {
		return true
	}
	// Original inflight files (N.blk.inflight, but not m_N.blk.inflight)
	if strings.HasSuffix(fileName, ".blk.inflight") {
		return true
	}
	// Original completed files (N.blk.completed, but not m_N.blk.completed)
	if strings.HasSuffix(fileName, ".blk.completed") {
		return true
	}
	return false
}

// shouldDeleteMergedBlocks determines if a file should be deleted (flag=2, merged blocks only)
func (rs *DiskSegmentImpl) shouldDeleteMergedBlocks(fileName string) bool {
	// Merged block files (m_N.blk)
	if strings.HasPrefix(fileName, "m_") && strings.HasSuffix(fileName, ".blk") {
		return true
	}
	// Merged inflight files (m_N.blk.inflight)
	if strings.HasPrefix(fileName, "m_") && strings.HasSuffix(fileName, ".blk.inflight") {
		return true
	}
	// Merged completed files (m_N.blk.completed)
	if strings.HasPrefix(fileName, "m_") && strings.HasSuffix(fileName, ".blk.completed") {
		return true
	}
	return false
}
