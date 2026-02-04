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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
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
	Size       int64
}

type ChunkObjectWalkFunc func(chunkObjectInfo *ChunkObjectInfo) bool

//go:generate mockery --dir=./common/objectstorage --name=ObjectStorage --structname=ObjectStorage --output=mocks/mocks_objectstorage --filename=mock_object_storage.go --with-expecter=true  --outpkg=mocks_objectstorage
type ObjectStorage interface {
	GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64, operatingNamespace string, operatingLogId string) (minioHandler.FileReader, error)
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error
	PutObjectIfNoneMatch(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error
	PutFencedObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error
	StatObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) (int64, bool, error)
	WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc, operatingNamespace string, operatingLogId string) error
	RemoveObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error
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

// CheckIfConditionWriteSupport checks if ObjectStorage supports conditional writes
func CheckIfConditionWriteSupport(ctx context.Context, objectStorage ObjectStorage, bucketName string, basePath string) (bool, error) {
	return doCheckIfConditionWriteSupport(ctx, objectStorage, bucketName, basePath)
}

// doCheckIfConditionWriteSupport checks if ObjectStorage supports PutObjectIfNoneMatch and PutFencedObject
// This function will check if the ObjectStorage support conditional write features
func doCheckIfConditionWriteSupport(ctx context.Context, objectStorageClient ObjectStorage, bucketName string, basePath string) (bool, error) {
	// Test object keys
	checkID := generateUniqueTestID()
	testObjectKey := fmt.Sprintf("%s/conditional_write_%s", basePath, checkID)
	fencedObjectKey := fmt.Sprintf("%s/conditional_write_fenced_%s", basePath, checkID)

	requestNamespace := fmt.Sprintf("%s/%s", bucketName, basePath)
	requestLogId := "default" // only for checking

	defer func() {
		// Clean up test objects
		_ = objectStorageClient.RemoveObject(ctx, bucketName, testObjectKey, requestNamespace, requestLogId)
		_ = objectStorageClient.RemoveObject(ctx, bucketName, fencedObjectKey, requestNamespace, requestLogId)
	}()

	// Test 1: PutObjectIfNoneMatch should succeed for non-existing object
	testData := "test-conditional-write-data"
	testReader := strings.NewReader(testData)
	err := objectStorageClient.PutObjectIfNoneMatch(ctx, bucketName, testObjectKey, testReader, int64(len(testData)), requestNamespace, requestLogId)
	if err != nil {
		return false, fmt.Errorf("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch not supported or failed. "+
			"BucketName: %s, ObjectKey: %s, Error: %v", bucketName, testObjectKey, err)
	}

	// Test 2: PutObjectIfNoneMatch should fail for existing object
	testReader2 := strings.NewReader(testData)
	err = objectStorageClient.PutObjectIfNoneMatch(ctx, bucketName, testObjectKey, testReader2, int64(len(testData)), requestNamespace, requestLogId)
	if err == nil {
		return false, fmt.Errorf("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return error for existing object, " +
			"but it succeeded unexpectedly")
	}

	if !werr.ErrObjectAlreadyExists.Is(err) {
		return false, fmt.Errorf("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return ErrObjectAlreadyExists "+
			"for existing object, but got: %v", err)
	}

	// Test: put fence block with the same objectName should fail
	err = objectStorageClient.PutFencedObject(ctx, bucketName, testObjectKey, requestNamespace, requestLogId)
	if err == nil {
		return false, fmt.Errorf("checkIfConditionWriteSupport failed: PutFencedObject should return error for existing object," +
			"but it succeeded. This indicates fenced object detection is not working properly")
	}

	if !werr.ErrObjectAlreadyExists.Is(err) {
		return false, fmt.Errorf("checkIfConditionWriteSupport failed: PutFencedObject should return ErrObjectAlreadyExists for existing object,"+
			"but got: %v. This indicates the error handling for fenced object is incorrect", err)
	}

	// Test 3: PutFencedObject should succeed
	err = objectStorageClient.PutFencedObject(ctx, bucketName, fencedObjectKey, requestNamespace, requestLogId)
	if err != nil {
		return false, fmt.Errorf("CheckIfConditionWriteSupport failed: PutFencedObject not supported or failed. "+
			"BucketName: %s, ObjectKey: %s, Error: %v", bucketName, fencedObjectKey, err)
	}

	// Test 4: PutObjectIfNoneMatch should fail for fenced object
	testReader3 := strings.NewReader(testData)
	err = objectStorageClient.PutObjectIfNoneMatch(ctx, bucketName, fencedObjectKey, testReader3, int64(len(testData)), requestNamespace, requestLogId)
	if err == nil {
		return false, fmt.Errorf("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return error for fenced object, " +
			"but it succeeded unexpectedly")
	}
	if !werr.ErrSegmentFenced.Is(err) {
		return false, fmt.Errorf("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return ErrSegmentFenced "+
			"for fenced object, but got: %v", err)
	}

	// Test 5: PutFencedObject should be idempotent
	err = objectStorageClient.PutFencedObject(ctx, bucketName, fencedObjectKey, requestNamespace, requestLogId)
	if err != nil {
		return false, fmt.Errorf("CheckIfConditionWriteSupport failed: PutFencedObject should be idempotent "+
			"for existing fenced object, but got error: %v", err)
	}

	logger.Ctx(ctx).Info("Conditional write support check passed successfully")

	return true, nil
}

// generateUniqueTestID generates a unique identifier for test objects
// combining timestamp, process ID and random bytes to avoid conflicts
func generateUniqueTestID() string {
	// Get current timestamp in nanoseconds
	timestamp := time.Now().UnixNano()

	// Get process ID
	pid := os.Getpid()

	// Generate 8 random bytes
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	if err != nil {
		// Fallback to timestamp-based randomness if crypto/rand fails
		randomBytes = []byte(fmt.Sprintf("%08x", timestamp&0xffffffff))
	}
	randomHex := hex.EncodeToString(randomBytes)

	// Combine all components: timestamp-pid-random
	return fmt.Sprintf("test-%d-%d-%s", timestamp, pid, randomHex)
}
