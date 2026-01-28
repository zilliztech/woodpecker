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

	logger.Ctx(ctx).Info("Starting to delete segment file",
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

	// Filter and delete segment files
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			segmentFilePath := filepath.Join(rs.segmentDir, entry.Name())

			// Delete file
			if err := os.Remove(segmentFilePath); err != nil {
				logger.Ctx(ctx).Warn("Failed to delete segment file",
					zap.String("segmentFilePath", segmentFilePath),
					zap.Error(err))
				deleteErrors = append(deleteErrors, err)
			} else {
				logger.Ctx(ctx).Info("Successfully deleted segment file",
					zap.String("segmentFilePath", segmentFilePath))
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

	logger.Ctx(ctx).Info("Completed fragment deletion",
		zap.String("segmentDir", rs.segmentDir),
		zap.Int("deletedCount", deletedCount),
		zap.Int("errorCount", len(deleteErrors)))

	if len(deleteErrors) > 0 {
		return deletedCount, fmt.Errorf("failed to delete %d segment files", len(deleteErrors))
	}
	return deletedCount, nil
}
