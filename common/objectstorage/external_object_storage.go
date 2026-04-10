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

package objectstorage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/external"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
)

var _ ObjectStorage = (*ExternalObjectStorage)(nil)

type ExternalObjectStorage struct {
	cfg *config.Configuration
}

// externalFileReader implements minioHandler.FileReader interface
type externalFileReader struct {
	data   []byte
	offset int64
	size   int64
}

func (r *externalFileReader) Read(p []byte) (n int, err error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.offset:])
	r.offset += int64(n)
	return n, nil
}

func (r *externalFileReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= r.size {
		return 0, io.EOF
	}
	n = copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *externalFileReader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.offset + offset
	case io.SeekEnd:
		newOffset = r.size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	if newOffset < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if newOffset > r.size {
		newOffset = r.size
	}
	r.offset = newOffset
	return newOffset, nil
}

func (r *externalFileReader) Close() error {
	r.data = nil
	return nil
}

func (r *externalFileReader) Size() (int64, error) {
	return r.size, nil
}

func newExternalObjectStorage(ctx context.Context, c *config.Configuration) (*ExternalObjectStorage, error) {
	if err := external.InitStorageV2FileSystem(c); err != nil {
		return nil, fmt.Errorf("failed to initialize external object storage: %w", err)
	}
	return &ExternalObjectStorage{
		cfg: c,
	}, nil
}

// buildPath constructs the full path from bucketName and objectName
func (g *ExternalObjectStorage) buildPath(bucketName, objectName string) string {
	if strings.HasPrefix(objectName, bucketName+"/") {
		// objectName already contains bucketName
		return objectName
	}
	return fmt.Sprintf("%s/%s", bucketName, objectName)
}

func (g *ExternalObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64, operatingNamespace string, operatingLogId string) (minioHandler.FileReader, error) {
	start := time.Now()
	path := g.buildPath(bucketName, objectName)

	// Read the entire file
	data, err := external.ReadFile(path)
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "get_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "get_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	fileSize := int64(len(data))

	// Handle offset and size
	var startOffset int64
	var readSize int64

	if offset < 0 {
		startOffset = 0
	} else if offset >= fileSize {
		// Offset beyond file size, return empty reader
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "get_object", "success").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "get_object", "success").Observe(float64(time.Since(start).Milliseconds()))
		return &externalFileReader{
			data:   []byte{},
			offset: 0,
			size:   0,
		}, nil
	} else {
		startOffset = offset
	}

	if size <= 0 {
		readSize = fileSize - startOffset
	} else {
		readSize = size
		if startOffset+readSize > fileSize {
			readSize = fileSize - startOffset
		}
	}

	// Extract the requested portion
	extractedData := data[startOffset : startOffset+readSize]

	metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "get_object", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "get_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	return &externalFileReader{
		data:   extractedData,
		offset: 0,
		size:   readSize,
	}, nil
}

func (g *ExternalObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error {
	start := time.Now()
	path := g.buildPath(bucketName, objectName)

	// Read all data from reader
	data, err := io.ReadAll(reader)
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return fmt.Errorf("failed to read from reader: %w", err)
	}

	// Write file without metadata
	if err := external.WriteFile(path, data, nil); err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}
	metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

func (g *ExternalObjectStorage) PutObjectIfNoneMatch(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error {
	start := time.Now()
	path := g.buildPath(bucketName, objectName)

	// Read all data from reader
	data, err := io.ReadAll(reader)
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "error").Observe(float64(time.Since(start).Milliseconds()))
		return fmt.Errorf("failed to read from reader: %w", err)
	}

	// Write file without metadata
	err = external.WriteFile(path, data, map[string]string{
		minioHandler.FencedObjectMetaKey: "false",
	})
	if err != nil && g.IsPreconditionFailedError(err) {
		_, isFencedObject, stateErr := g.StatObject(ctx, bucketName, objectName, operatingNamespace, operatingLogId)
		if stateErr != nil {
			// return normal err, let task retry
			metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "error").Inc()
			metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "error").Observe(float64(time.Since(start).Milliseconds()))
			return stateErr
		}
		if isFencedObject {
			logger.Ctx(ctx).Info("object already exists and it is a fence object", zap.String("objectName", objectName))
			metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "fenced").Inc()
			metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "fenced").Observe(float64(time.Since(start).Milliseconds()))
			return werr.ErrSegmentFenced.WithCauseErrMsg("already fenced")
		}
		// means it is a normal object already uploaded before this retry, idempotent flush success
		logger.Ctx(ctx).Info("object already exists, idempotent flush success", zap.String("objectKey", objectName))
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "already_exists").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "already_exists").Observe(float64(time.Since(start).Milliseconds()))
		return werr.ErrObjectAlreadyExists
	}

	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}

	metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_object_if_none_match", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

func (g *ExternalObjectStorage) PutFencedObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error {
	start := time.Now()
	path := g.buildPath(bucketName, objectName)
	// Create a fenced object
	putErr := external.WriteFile(path, []byte{'F'}, map[string]string{
		minioHandler.FencedObjectMetaKey: "true",
	})

	// if the object already exists, check if it's a fenced object
	if putErr != nil && g.IsPreconditionFailedError(putErr) {
		// check if the object exists
		_, isFenced, stateErr := g.StatObject(ctx, bucketName, objectName, operatingNamespace, operatingLogId)
		if stateErr != nil {
			// return normal err
			metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_fenced_object", "error").Inc()
			metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_fenced_object", "error").Observe(float64(time.Since(start).Milliseconds()))
			return stateErr
		}
		if isFenced {
			// already fenced, return success (idempotent)
			logger.Ctx(ctx).Info("found fenced object exists, skip", zap.String("objectName", objectName))
			metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_fenced_object", "success").Inc()
			metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_fenced_object", "success").Observe(float64(time.Since(start).Milliseconds()))
			return nil
		}
		// return normal err
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_fenced_object", "already_exists").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_fenced_object", "already_exists").Observe(float64(time.Since(start).Milliseconds()))
		return werr.ErrObjectAlreadyExists
	}

	if putErr != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_fenced_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_fenced_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return putErr
	}

	metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_fenced_object", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "put_fenced_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

func (g *ExternalObjectStorage) StatObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) (int64, bool, error) {
	start := time.Now()
	path := g.buildPath(bucketName, objectName)

	// Get file stats
	stats, err := external.GetFileStats(path)
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "stat_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "stat_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, false, fmt.Errorf("failed to get file stats: %w", err)
	}

	// Check if it's a fenced object by checking metadata
	isFenced := false
	if stats.Metadata != nil {
		if val, ok := stats.Metadata[minioHandler.FencedObjectMetaKey]; ok && val == "true" {
			isFenced = true
		}
	}

	metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "stat_object", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "stat_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	return stats.Size, isFenced, nil
}

func (g *ExternalObjectStorage) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc, operatingNamespace string, operatingLogId string) error {
	start := time.Now()
	// Build the full path to list
	listPath := g.buildPath(bucketName, prefix)

	// List directory contents
	entries, err := external.ListDir(listPath, recursive)
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "walk_with_objects", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "walk_with_objects", "error").Observe(float64(time.Since(start).Milliseconds()))
		return fmt.Errorf("failed to list directory %s: %w", listPath, err)
	}

	// Filter and process entries
	for _, entry := range entries {
		// Skip directories, only process files
		if entry.IsDir {
			continue
		}

		// Extract object name from full path
		// The entry.Path is the full path (e.g., "bucket/path/to/file.txt")
		// We need to extract the relative path from bucketName
		objectName := entry.Path
		if strings.HasPrefix(objectName, bucketName+"/") {
			objectName = strings.TrimPrefix(objectName, bucketName+"/")
		}

		// Filter by prefix if specified
		if prefix != "" && !strings.HasPrefix(objectName, prefix) {
			continue
		}

		// Convert modification time from nanoseconds to time.Time
		var modifyTime time.Time
		if entry.MTimeNs > 0 {
			modifyTime = time.Unix(0, entry.MTimeNs)
		} else {
			modifyTime = time.Now() // Fallback to current time if not available
		}

		// Call walk function
		chunkInfo := &ChunkObjectInfo{
			FilePath:   objectName,
			ModifyTime: modifyTime,
		}

		// If walkFunc returns false, stop walking
		if !walkFunc(chunkInfo) {
			metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "walk_with_objects", "success").Inc()
			metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "walk_with_objects", "success").Observe(float64(time.Since(start).Milliseconds()))
			return nil
		}
	}

	metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "walk_with_objects", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "walk_with_objects", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

func (g *ExternalObjectStorage) RemoveObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error {
	start := time.Now()
	path := g.buildPath(bucketName, objectName)
	if err := external.DeleteFile(path); err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "remove_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "remove_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}
	metrics.WpObjectStorageOperationsTotal.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "remove_object", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues(metrics.NodeID, operatingNamespace, operatingLogId, "remove_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

func (g *ExternalObjectStorage) IsObjectNotExistsError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	// Check for common "not found" error patterns
	return strings.Contains(errMsg, "not found") ||
		strings.Contains(errMsg, "Not found") ||
		strings.Contains(errMsg, "NotFound") ||
		strings.Contains(errMsg, "does not exist") ||
		strings.Contains(errMsg, "Path does not exist")
}

func (g *ExternalObjectStorage) IsPreconditionFailedError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	// Check for precondition failed errors (e.g., "already exists" for PutObjectIfNoneMatch)
	return strings.Contains(errMsg, "already exists") ||
		strings.Contains(errMsg, "PreconditionFailed") ||
		strings.Contains(errMsg, "condition write") ||
		strings.Contains(errMsg, "Condition Write")
}
