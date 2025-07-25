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

package minio

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
)

const (
	ObjectStorageScopeName = "ObjectStorage"
	FencedObjectMetaKey    = "Fenced"
)

type ObjectReader interface {
	io.Reader
	io.Closer
}

// ReadObjectFull reads all content from ObjectReader and returns a byte slice
// It efficiently handles data streams of unknown size by dynamically expanding the buffer to avoid excessive memory allocations
func ReadObjectFull(ctx context.Context, objectReader ObjectReader, initReadBufSize int64) ([]byte, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ObjectStorageScopeName, "ReadObjectFull")
	defer sp.End()
	start := time.Now()
	// Initial buffer size - 1MB is a reasonable starting point
	buf := make([]byte, 0, initReadBufSize)

	// Temporary read buffer
	readBuf := make([]byte, 32*1024) // 32KB read block
	bytesRead := int64(0)

	for {
		// Read a chunk of data
		n, err := objectReader.Read(readBuf)

		// If data is read, append to result buffer
		if n > 0 {
			buf = append(buf, readBuf[:n]...)
			bytesRead += int64(n)
		}

		// Handle EOF and errors
		if err == io.EOF {
			// Normal completion
			break
		} else if err != nil {
			// Error occurred
			metrics.WpObjectStorageOperationsTotal.WithLabelValues("read_object_full", "error").Inc()
			metrics.WpObjectStorageOperationLatency.WithLabelValues("read_object_full", "error").Observe(float64(time.Since(start).Milliseconds()))
			return nil, err
		}
	}

	// Track metrics for successful read
	metrics.WpObjectStorageOperationsTotal.WithLabelValues("read_object_full", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues("read_object_full", "success").Observe(float64(time.Since(start).Milliseconds()))
	metrics.WpObjectStorageBytesTransferred.WithLabelValues("read").Add(float64(bytesRead))

	return buf, nil
}

//go:generate mockery --dir=./common/minio --name=MinioHandler --structname=MinioHandler --output=mocks/mocks_minio --filename=mock_minio_handler.go --with-expecter=true  --outpkg=mocks_minio
type MinioHandler interface {
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	GetObjectDataAndInfo(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (ObjectReader, int64, int64, error)
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	PutObjectIfNoneMatch(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error)
	PutFencedObject(ctx context.Context, bucketName, objectName string) (minio.UploadInfo, error)
	RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error
	StatObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (minio.ObjectInfo, error)
	CopyObject(ctx context.Context, dst minio.CopyDestOptions, src minio.CopySrcOptions) (minio.UploadInfo, error)
	ListObjects(ctx context.Context, bucketName, prefix string, recursive bool, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
}

var _ MinioHandler = (*minioHandlerImpl)(nil)

type minioHandlerImpl struct {
	client *minio.Client
}

func NewMinioHandler(ctx context.Context, cfg *config.Configuration) (MinioHandler, error) {
	minioCli, err := newMinioClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &minioHandlerImpl{
		client: minioCli,
	}, nil
}

func NewMinioHandlerWithClient(ctx context.Context, minioCli *minio.Client) (MinioHandler, error) {
	return &minioHandlerImpl{
		client: minioCli,
	}, nil
}

func (m *minioHandlerImpl) GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ObjectStorageScopeName, "GetObject")
	defer sp.End()
	start := time.Now()
	obj, err := m.client.GetObject(ctx, bucketName, objectName, opts)
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("get_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("get_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}
	metrics.WpObjectStorageOperationsTotal.WithLabelValues("get_object", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues("get_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	return obj, nil
}

func (m *minioHandlerImpl) GetObjectDataAndInfo(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (ObjectReader, int64, int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ObjectStorageScopeName, "GetObjectDataAndInfo")
	defer sp.End()
	start := time.Now()
	obj, err := m.client.GetObject(ctx, bucketName, objectName, opts)
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("get_object_data_info", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("get_object_data_info", "error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, 0, -1, err
	}
	info, err := obj.Stat()
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("get_object_data_info", "error_stat").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("get_object_data_info", "error_stat").Observe(float64(time.Since(start).Milliseconds()))
		return nil, 0, -1, err
	}
	metrics.WpObjectStorageOperationsTotal.WithLabelValues("get_object_data_info", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues("get_object_data_info", "success").Observe(float64(time.Since(start).Milliseconds()))
	metrics.WpObjectStorageBytesTransferred.WithLabelValues("get_object").Add(float64(info.Size))
	return obj, info.Size, info.LastModified.UnixMilli(), err
}

func (m *minioHandlerImpl) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ObjectStorageScopeName, "PutObject")
	defer sp.End()
	start := time.Now()
	info, err := m.client.PutObject(ctx, bucketName, objectName, reader, objectSize, opts)
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("put_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("put_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return info, err
	}
	metrics.WpObjectStorageOperationsTotal.WithLabelValues("put_object", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues("put_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	metrics.WpObjectStorageBytesTransferred.WithLabelValues("put_object").Add(float64(info.Size))
	return info, nil
}

func (m *minioHandlerImpl) PutObjectIfNoneMatch(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ObjectStorageScopeName, "PutObjectIfNoneMatch")
	defer sp.End()
	start := time.Now()
	opts := minio.PutObjectOptions{}
	opts.SetMatchETagExcept("*")
	info, err := m.client.PutObject(ctx, bucketName, objectName, reader, objectSize, opts)
	if err != nil && IsPreconditionFailed(err) {
		logger.Ctx(ctx).Warn("object already exists", zap.String("objectName", objectName))
		objInfo, stateErr := m.client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
		if stateErr != nil {
			// return normal err, let task retry
			return info, stateErr
		}
		if IsFencedObject(objInfo) {
			return info, werr.ErrSegmentFenced.WithCauseErrMsg("already fenced")
		}
		// means it is a normal object already uploaded before this retry, idempotent flush success
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("condition_put_object", "success").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("condition_put_object", "success").Observe(float64(time.Since(start).Milliseconds()))
		metrics.WpObjectStorageBytesTransferred.WithLabelValues("condition_put_object").Add(float64(info.Size))
		logger.Ctx(ctx).Info("fragment already exists, idempotent flush success", zap.String("fragmentObjectKey", objectName))
		return info, werr.ErrObjectAlreadyExists
	}
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("condition_put_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("condition_put_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return info, err
	}
	metrics.WpObjectStorageOperationsTotal.WithLabelValues("condition_put_object", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues("condition_put_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	metrics.WpObjectStorageBytesTransferred.WithLabelValues("condition_put_object").Add(float64(info.Size))
	return info, nil
}

func (m *minioHandlerImpl) PutFencedObject(ctx context.Context, bucketName, objectName string) (minio.UploadInfo, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ObjectStorageScopeName, "PutFencedObject")
	defer sp.End()
	start := time.Now()
	opts := minio.PutObjectOptions{}
	opts.SetMatchETagExcept("*")
	opts.UserMetadata = map[string]string{
		FencedObjectMetaKey: "true",
	}
	fencedObjectReader := bytes.NewReader([]byte("F"))
	info, err := m.client.PutObject(ctx, bucketName, objectName, fencedObjectReader, 1, opts)
	if err != nil && IsPreconditionFailed(err) {
		objInfo, stateErr := m.client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
		if stateErr != nil {
			// return normal err
			return info, stateErr
		}
		if IsFencedObject(objInfo) {
			// already fenced, return success
			logger.Ctx(ctx).Info("found fenced object exists, skip", zap.String("objectName", objectName))
			return info, nil
		}
		// return normal err
		return info, werr.ErrObjectAlreadyExists
	}
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("put_fenced_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("put_fenced_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return info, err
	}
	metrics.WpObjectStorageOperationsTotal.WithLabelValues("put_fenced_object", "success").Inc()
	metrics.WpObjectStorageOperationLatency.WithLabelValues("put_fenced_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	metrics.WpObjectStorageBytesTransferred.WithLabelValues("put_fenced_object").Add(float64(info.Size))
	return info, nil
}

func (m *minioHandlerImpl) RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ObjectStorageScopeName, "RemoveObject")
	defer sp.End()
	start := time.Now()
	err := m.client.RemoveObject(ctx, bucketName, objectName, opts)
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("remove_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("remove_object", "error").Observe(float64(time.Since(start).Milliseconds()))
	} else {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("remove_object", "success").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("remove_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return err
}

func (m *minioHandlerImpl) StatObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (minio.ObjectInfo, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ObjectStorageScopeName, "StatObject")
	defer sp.End()
	start := time.Now()
	info, err := m.client.StatObject(ctx, bucketName, objectName, opts)
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("stat_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("stat_object", "error").Observe(float64(time.Since(start).Milliseconds()))
	} else {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("stat_object", "success").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("stat_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return info, err
}

func (m *minioHandlerImpl) CopyObject(ctx context.Context, dest minio.CopyDestOptions, src minio.CopySrcOptions) (minio.UploadInfo, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ObjectStorageScopeName, "CopyObject")
	defer sp.End()
	start := time.Now()
	uploadInfo, err := m.client.CopyObject(ctx, dest, src)
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("copy_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("copy_object", "error").Observe(float64(time.Since(start).Milliseconds()))
	} else {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues("copy_object", "success").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues("copy_object", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return uploadInfo, err
}

func (m *minioHandlerImpl) ListObjects(ctx context.Context, bucketName, prefix string, recursive bool, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ObjectStorageScopeName, "ListObjects")
	defer sp.End()
	// We can't track completion metrics here as this returns a channel
	// Instead, we'll increment the operation count for the method call
	metrics.WpObjectStorageOperationsTotal.WithLabelValues("list_objects", "called").Inc()

	opts.Recursive = recursive
	opts.Prefix = prefix
	return m.client.ListObjects(ctx, bucketName, opts)
}
