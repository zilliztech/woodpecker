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
	"io"

	"github.com/labstack/gommon/log"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
)

var _ ObjectStorage = (*MinioObjectStorage)(nil)

type MinioObjectStorage struct {
	minioHandler minioHandler.MinioHandler
	cfg          *config.Configuration
}

func newMinioObjectStorageWithConfig(ctx context.Context, c *config.Configuration) (*MinioObjectStorage, error) {
	minIOClient, err := minioHandler.NewMinioHandler(ctx, c)
	if err != nil {
		return nil, err
	}
	return &MinioObjectStorage{
		minioHandler: minIOClient,
		cfg:          c}, nil
}

func (minioObjectStorage *MinioObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64, operatingNamespace string, operatingLogId string) (minioHandler.FileReader, error) {
	opts := minio.GetObjectOptions{}
	if offset >= 0 {
		err := opts.SetRange(offset, offset+size-1)
		if err != nil {
			log.Warn("failed to set range", zap.String("bucket", bucketName), zap.String("path", objectName), zap.Error(err))
			return nil, err
		}
	}
	object, err := minioObjectStorage.minioHandler.GetObject(ctx, bucketName, objectName, opts, operatingNamespace, operatingLogId)
	if err != nil {
		return nil, err
	}
	return &minioHandler.ObjectReader{
		Object: object,
	}, nil
}

func (minioObjectStorage *MinioObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error {
	_, err := minioObjectStorage.minioHandler.PutObject(ctx, bucketName, objectName, reader, objectSize, minio.PutObjectOptions{}, operatingNamespace, operatingLogId)
	return err
}

func (minioObjectStorage *MinioObjectStorage) PutObjectIfNoneMatch(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error {
	_, err := minioObjectStorage.minioHandler.PutObjectIfNoneMatch(ctx, bucketName, objectName, reader, objectSize, operatingNamespace, operatingLogId)
	return err
}

func (minioObjectStorage *MinioObjectStorage) PutFencedObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error {
	_, err := minioObjectStorage.minioHandler.PutFencedObject(ctx, bucketName, objectName, operatingNamespace, operatingLogId)
	return err
}

func (minioObjectStorage *MinioObjectStorage) StatObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) (int64, bool, error) {
	info, err := minioObjectStorage.minioHandler.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{}, operatingNamespace, operatingLogId)
	if err != nil {
		return -1, false, err
	}
	isFencedObject := minioHandler.IsFencedObject(info)
	return info.Size, isFencedObject, err
}

func (minioObjectStorage *MinioObjectStorage) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc, operatingNamespace string, operatingLogId string) (err error) {
	in := minioObjectStorage.minioHandler.ListObjects(ctx, bucketName, prefix, recursive, minio.ListObjectsOptions{
		MaxKeys: minioObjectStorage.cfg.Minio.ListObjectsMaxKeys,
	}, operatingNamespace, operatingLogId)
	for object := range in {
		if object.Err != nil {
			return object.Err
		}
		if !walkFunc(&ChunkObjectInfo{FilePath: object.Key, ModifyTime: object.LastModified, Size: object.Size}) {
			return nil
		}
	}
	return nil
}

func (minioObjectStorage *MinioObjectStorage) RemoveObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error {
	err := minioObjectStorage.minioHandler.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{}, operatingNamespace, operatingLogId)
	return err
}

func (minioObjectStorage *MinioObjectStorage) IsPreconditionFailedError(err error) bool {
	return minioHandler.IsPreconditionFailed(err)
}

func (minioObjectStorage *MinioObjectStorage) IsObjectNotExistsError(err error) bool {
	return minioHandler.IsObjectNotExists(err)
}
