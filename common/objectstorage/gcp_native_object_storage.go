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

	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
)

var _ ObjectStorage = (*GcpNativeObjectStorage)(nil)

type GcpNativeObjectStorage struct {
	client *minioHandler.MinioHandler
}

func (g *GcpNativeObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64, operatingNamespace string, operatingLogId string) (minioHandler.FileReader, error) {
	panic("implement me")
}

func (g *GcpNativeObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error {
	panic("implement me")
}

func (g *GcpNativeObjectStorage) PutObjectIfNoneMatch(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error {
	panic("implement me")
}

func (g *GcpNativeObjectStorage) PutFencedObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error {
	panic("implement me")
}

func (g *GcpNativeObjectStorage) StatObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) (int64, bool, error) {
	panic("implement me")
}

func (g *GcpNativeObjectStorage) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc, operatingNamespace string, operatingLogId string) error {
	panic("implement me")
}

func (g *GcpNativeObjectStorage) RemoveObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error {
	panic("implement me")
}

func newGcpNativeObjectStorageWithConfig(ctx context.Context, c *config.Configuration) (*GcpNativeObjectStorage, error) {
	panic("Not support GCP Native object storage yet.")
}

func (g *GcpNativeObjectStorage) IsObjectNotExistsError(err error) bool {
	panic("implement me")
}

func (g *GcpNativeObjectStorage) IsPreconditionFailedError(err error) bool {
	panic("implement me")
}
