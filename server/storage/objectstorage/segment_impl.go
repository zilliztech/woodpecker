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

package objectstorage

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/server/storage"
)

const (
	SegmentScopeName = "Segment"
)

var _ storage.Segment = (*SegmentImpl)(nil)

type SegmentImpl struct {
	mu             sync.Mutex
	cfg            *config.Configuration
	client         minioHandler.MinioHandler
	bucket         string
	logId          int64
	segmentId      int64
	segmentFileKey string
}

// NewSegmentImpl is used to create a new Segment, which is used to write data to object storage
func NewSegmentImpl(ctx context.Context, bucket string, baseDir string, logId int64, segId int64, objectCli minioHandler.MinioHandler, cfg *config.Configuration) storage.Segment {
	segmentFileKey := getSegmentFileKey(baseDir, logId, segId)
	logger.Ctx(ctx).Debug("new SegmentImpl created", zap.String("segmentFileKey", segmentFileKey))
	segmentImpl := &SegmentImpl{
		cfg:            cfg,
		logId:          logId,
		segmentId:      segId,
		client:         objectCli,
		segmentFileKey: segmentFileKey,
		bucket:         bucket,
	}
	return segmentImpl
}

func (s *SegmentImpl) GetId() int64 {
	return s.segmentId
}

func (s *SegmentImpl) DeleteFileData(ctx context.Context, flag int) (int, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "DeleteFileData")
	defer sp.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	logger.Ctx(ctx).Info("starting segment file data deletion",
		zap.String("segmentFileKey", s.segmentFileKey),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int("flag", flag))

	// List all objects in the segment directory
	listPrefix := fmt.Sprintf("%s/", s.segmentFileKey)
	objectCh := s.client.ListObjects(ctx, s.bucket, listPrefix, false, minio.ListObjectsOptions{})

	var objectsToDelete []string
	var deletedCount int
	var errorCount int

	// Collect all objects to delete
	for objInfo := range objectCh {
		if objInfo.Err != nil {
			logger.Ctx(ctx).Error("error listing blocks during deletion",
				zap.String("segmentFileKey", s.segmentFileKey),
				zap.Error(objInfo.Err))
			return deletedCount, objInfo.Err
		}

		// Determine what to delete based on flag
		shouldDelete := false
		switch flag {
		case 0:
			// Delete all blocks
			if strings.HasSuffix(objInfo.Key, ".blk") {
				shouldDelete = true
			}
		case 1: // Delete only regular blocks (not merged)
			if strings.HasSuffix(objInfo.Key, ".blk") && !strings.Contains(objInfo.Key, "/m_") {
				shouldDelete = true
			}
		case 2: // Delete only merged blocks
			if strings.HasSuffix(objInfo.Key, ".blk") && strings.Contains(objInfo.Key, "/m_") {
				shouldDelete = true
			}
		default:
			// Delete all files
			shouldDelete = false
		}

		// Skip lock files unless explicitly deleting all
		if strings.HasSuffix(objInfo.Key, ".lock") && flag == 0 {
			shouldDelete = true
		}

		if shouldDelete {
			objectsToDelete = append(objectsToDelete, objInfo.Key)
		}
	}

	logger.Ctx(ctx).Info("collected objects for deletion",
		zap.String("segmentFileKey", s.segmentFileKey),
		zap.Int("objectCount", len(objectsToDelete)),
		zap.Int("flag", flag))

	// Delete objects
	for _, objectKey := range objectsToDelete {
		err := s.client.RemoveObject(ctx, s.bucket, objectKey, minio.RemoveObjectOptions{})
		if err != nil {
			// Log error but continue with other deletions
			logger.Ctx(ctx).Warn("failed to delete block",
				zap.String("segmentFileKey", s.segmentFileKey),
				zap.String("objectKey", objectKey),
				zap.Error(err))
			errorCount++
		} else {
			logger.Ctx(ctx).Debug("successfully deleted block",
				zap.String("segmentFileKey", s.segmentFileKey),
				zap.String("objectKey", objectKey))
			deletedCount++
		}
	}

	logger.Ctx(ctx).Info("segment blocks deletion completed",
		zap.String("segmentFileKey", s.segmentFileKey),
		zap.Int("deletedCount", deletedCount),
		zap.Int("errorCount", errorCount),
		zap.Int("flag", flag))

	if errorCount > 0 {
		return deletedCount, fmt.Errorf("failed to delete %d out of %d objects", errorCount, len(objectsToDelete))
	}

	return deletedCount, nil
}
