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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
)

type ChunkObjectInfo struct {
	FilePath   string
	ModifyTime time.Time
}

type ChunkObjectWalkFunc func(chunkObjectInfo *ChunkObjectInfo) bool

//go:generate mockery --dir=./common/objectstorage --name=ObjectStorage --structname=ObjectStorage --output=mocks/mocks_objectstorage --filename=mock_object_storage.go --with-expecter=true  --outpkg=mocks_objectstorage
type ObjectStorage interface {
	GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (minioHandler.FileReader, error)
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error
	PutObjectIfNoneMatch(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error
	PutFencedObject(ctx context.Context, bucketName, objectName string) error
	StatObject(ctx context.Context, bucketName, objectName string) (int64, bool, error)
	WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) error
	RemoveObject(ctx context.Context, bucketName, objectName string) error
	IsObjectNotExistsError(err error) bool
	IsPreconditionFailedError(err error) bool
}

func NewObjectStorage(ctx context.Context, c *config.Configuration) (ObjectStorage, error) {
	var client ObjectStorage
	var err error
	if c.Minio.CloudProvider == minioHandler.CloudProviderAzure {
		client, err = newAzureObjectStorageWithConfig(ctx, c)
	} else if c.Minio.CloudProvider == minioHandler.CloudProviderGCPNative {
		client, err = newGcpNativeObjectStorageWithConfig(ctx, c)
	} else {
		client, err = newMinioObjectStorageWithConfig(ctx, c)
	}
	if err != nil {
		return nil, err
	}
	logger.Ctx(ctx).Info("ObjectStorage Client init success.", zap.String("remote", c.Minio.CloudProvider), zap.String("bucketname", c.Minio.BucketName), zap.String("root", c.Minio.RootPath))
	return client, nil
}

// checkOnce ensures the conditional write support check is only run once
var checkOnce sync.Once

// CheckIfConditionWriteSupport checks if ObjectStorage supports conditional writes
func CheckIfConditionWriteSupport(ctx context.Context, objectStorage ObjectStorage, bucketName string, basePath string) {
	checkOnce.Do(func() {
		doCheckIfConditionWriteSupport(ctx, objectStorage, bucketName, basePath)
	})
}

// doCheckIfConditionWriteSupport checks if ObjectStorage supports PutObjectIfNoneMatch and PutFencedObject
// This function will panic if the ObjectStorage does not support conditional write features
func doCheckIfConditionWriteSupport(ctx context.Context, objectStorageClient ObjectStorage, bucketName string, basePath string) {
	// Test object keys
	testObjectKey := fmt.Sprintf("%s/conditional_write_test_object", basePath)
	fencedObjectKey := fmt.Sprintf("%s/conditional_write_test_fenced_object", basePath)

	defer func() {
		// Clean up test objects
		_ = objectStorageClient.RemoveObject(ctx, bucketName, testObjectKey)
		_ = objectStorageClient.RemoveObject(ctx, bucketName, fencedObjectKey)
	}()

	// Test 1: PutObjectIfNoneMatch should succeed for non-existing object
	testData := "test-conditional-write-data"
	testReader := strings.NewReader(testData)
	err := objectStorageClient.PutObjectIfNoneMatch(ctx, bucketName, testObjectKey, testReader, int64(len(testData)))
	if err != nil {
		panic(fmt.Sprintf("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch not supported or failed. "+
			"BucketName: %s, ObjectKey: %s, Error: %v", bucketName, testObjectKey, err))
	}

	// Test 2: PutObjectIfNoneMatch should fail for existing object
	testReader2 := strings.NewReader(testData)
	err = objectStorageClient.PutObjectIfNoneMatch(ctx, bucketName, testObjectKey, testReader2, int64(len(testData)))
	if err == nil {
		panic("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return error for existing object, " +
			"but it succeeded unexpectedly")
	}

	if !werr.ErrObjectAlreadyExists.Is(err) {
		panic(fmt.Sprintf("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return ErrObjectAlreadyExists "+
			"for existing object, but got: %v", err))
	}

	// Test 3: PutFencedObject should succeed
	err = objectStorageClient.PutFencedObject(ctx, bucketName, fencedObjectKey)
	if err != nil {
		panic(fmt.Sprintf("CheckIfConditionWriteSupport failed: PutFencedObject not supported or failed. "+
			"BucketName: %s, ObjectKey: %s, Error: %v", bucketName, fencedObjectKey, err))
	}

	// Test 4: PutObjectIfNoneMatch should fail for fenced object
	testReader3 := strings.NewReader(testData)
	err = objectStorageClient.PutObjectIfNoneMatch(ctx, bucketName, fencedObjectKey, testReader3, int64(len(testData)))
	if err == nil {
		panic("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return error for fenced object, " +
			"but it succeeded unexpectedly")
	}
	if !werr.ErrSegmentFenced.Is(err) {
		panic(fmt.Sprintf("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return ErrSegmentFenced "+
			"for fenced object, but got: %v", err))
	}

	// Test 5: PutFencedObject should be idempotent
	err = objectStorageClient.PutFencedObject(ctx, bucketName, fencedObjectKey)
	if err != nil {
		panic(fmt.Sprintf("CheckIfConditionWriteSupport failed: PutFencedObject should be idempotent "+
			"for existing fenced object, but got error: %v", err))
	}

	logger.Ctx(ctx).Info("Conditional write support check passed successfully")
}
