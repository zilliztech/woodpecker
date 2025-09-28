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
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/werr"
)

// resetCheckOnce resets the global sync.Once for testing purposes
func resetCheckOnce() {
	checkOnce = sync.Once{}
}

// testMinioHandler is a simple test implementation of MinioHandler
type testMinioHandler struct {
	putObjectIfNoneMatchFunc func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error)
	putFencedObjectFunc      func(ctx context.Context, bucketName, objectName string) (minio.UploadInfo, error)
	removeObjectFunc         func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error
}

func (t *testMinioHandler) PutObjectIfNoneMatch(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error) {
	if t.putObjectIfNoneMatchFunc != nil {
		return t.putObjectIfNoneMatchFunc(ctx, bucketName, objectName, reader, objectSize)
	}
	return minio.UploadInfo{}, nil
}

func (t *testMinioHandler) PutFencedObject(ctx context.Context, bucketName, objectName string) (minio.UploadInfo, error) {
	if t.putFencedObjectFunc != nil {
		return t.putFencedObjectFunc(ctx, bucketName, objectName)
	}
	return minio.UploadInfo{}, nil
}

func (t *testMinioHandler) RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
	if t.removeObjectFunc != nil {
		return t.removeObjectFunc(ctx, bucketName, objectName, opts)
	}
	return nil
}

// Implement other MinioHandler methods as no-ops for testing
func (t *testMinioHandler) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	return minio.UploadInfo{}, nil
}

func (t *testMinioHandler) GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error) {
	return nil, nil
}

func (t *testMinioHandler) GetObjectDataAndInfo(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (FileReader, int64, int64, error) {
	return nil, 0, 0, nil
}

func (t *testMinioHandler) StatObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, nil
}

func (t *testMinioHandler) ListObjects(ctx context.Context, bucketName, prefix string, recursive bool, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo)
	close(ch)
	return ch
}

func (t *testMinioHandler) CopyObject(ctx context.Context, dst minio.CopyDestOptions, src minio.CopySrcOptions) (minio.UploadInfo, error) {
	return minio.UploadInfo{}, nil
}

func TestDoCheckIfConditionWriteSupport_Success(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket"
	basePath := "test-path"

	callCount := 0
	handler := &testMinioHandler{
		putObjectIfNoneMatchFunc: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error) {
			callCount++
			if callCount == 1 {
				// First call should succeed
				return minio.UploadInfo{ETag: "test-etag"}, nil
			} else if callCount == 2 {
				// Second call should return ErrObjectAlreadyExists
				return minio.UploadInfo{}, werr.ErrObjectAlreadyExists
			} else if callCount == 3 {
				// Third call (with fenced object) should return ErrSegmentFenced
				return minio.UploadInfo{}, werr.ErrSegmentFenced
			}
			return minio.UploadInfo{}, nil
		},
		putFencedObjectFunc: func(ctx context.Context, bucketName, objectName string) (minio.UploadInfo, error) {
			// Both calls should succeed (first creation, then idempotent)
			return minio.UploadInfo{ETag: "fenced-etag"}, nil
		},
		removeObjectFunc: func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
			return nil
		},
	}

	// Test that the function runs successfully without panicking
	assert.NotPanics(t, func() {
		doCheckIfConditionWriteSupport(ctx, handler, bucketName, basePath)
	}, "doCheckIfConditionWriteSupport should not panic with proper test setup")

	t.Logf("doCheckIfConditionWriteSupport passed successfully")
}

func TestDoCheckIfConditionWriteSupport_UnsupportedPutObjectIfNoneMatch(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket"
	basePath := "test-path"

	handler := &testMinioHandler{
		putObjectIfNoneMatchFunc: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error) {
			return minio.UploadInfo{}, fmt.Errorf("unsupported operation")
		},
		removeObjectFunc: func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
			return nil
		},
	}

	// Test that the function panics when PutObjectIfNoneMatch is not supported
	assert.Panics(t, func() {
		doCheckIfConditionWriteSupport(ctx, handler, bucketName, basePath)
	}, "doCheckIfConditionWriteSupport should panic when PutObjectIfNoneMatch is not supported")

	t.Logf("doCheckIfConditionWriteSupport correctly panicked for unsupported PutObjectIfNoneMatch")
}

func TestDoCheckIfConditionWriteSupport_UnsupportedPutFencedObject(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket"
	basePath := "test-path"

	callCount := 0
	handler := &testMinioHandler{
		putObjectIfNoneMatchFunc: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error) {
			callCount++
			if callCount == 1 {
				return minio.UploadInfo{ETag: "test-etag"}, nil
			} else if callCount == 2 {
				return minio.UploadInfo{}, werr.ErrObjectAlreadyExists
			}
			return minio.UploadInfo{}, nil
		},
		putFencedObjectFunc: func(ctx context.Context, bucketName, objectName string) (minio.UploadInfo, error) {
			return minio.UploadInfo{}, fmt.Errorf("fenced objects not supported")
		},
		removeObjectFunc: func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
			return nil
		},
	}

	// Test that the function panics when PutFencedObject is not supported
	assert.Panics(t, func() {
		doCheckIfConditionWriteSupport(ctx, handler, bucketName, basePath)
	}, "doCheckIfConditionWriteSupport should panic when PutFencedObject is not supported")

	t.Logf("doCheckIfConditionWriteSupport correctly panicked for unsupported PutFencedObject")
}

func TestDoCheckIfConditionWriteSupport_IncorrectErrorHandling(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket"
	basePath := "test-path"

	callCount := 0
	handler := &testMinioHandler{
		putObjectIfNoneMatchFunc: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error) {
			callCount++
			if callCount == 1 {
				return minio.UploadInfo{ETag: "test-etag"}, nil
			} else if callCount == 2 {
				// Return wrong error type
				return minio.UploadInfo{}, fmt.Errorf("some other error")
			}
			return minio.UploadInfo{}, nil
		},
		removeObjectFunc: func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
			return nil
		},
	}

	// Test that the function panics when error handling is incorrect
	assert.Panics(t, func() {
		doCheckIfConditionWriteSupport(ctx, handler, bucketName, basePath)
	}, "doCheckIfConditionWriteSupport should panic when error handling is incorrect")

	t.Logf("doCheckIfConditionWriteSupport correctly panicked for incorrect error handling")
}

func TestDoCheckIfConditionWriteSupport_NoErrorWhenExpected(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket"
	basePath := "test-path"

	callCount := 0
	handler := &testMinioHandler{
		putObjectIfNoneMatchFunc: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error) {
			callCount++
			if callCount == 1 {
				return minio.UploadInfo{ETag: "test-etag"}, nil
			} else if callCount == 2 {
				// Should return error but doesn't
				return minio.UploadInfo{ETag: "another-etag"}, nil
			}
			return minio.UploadInfo{}, nil
		},
		removeObjectFunc: func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
			return nil
		},
	}

	// Test that the function panics when conditional write protection is not working
	assert.Panics(t, func() {
		doCheckIfConditionWriteSupport(ctx, handler, bucketName, basePath)
	}, "doCheckIfConditionWriteSupport should panic when conditional write protection is not working")

	t.Logf("doCheckIfConditionWriteSupport correctly panicked when conditional write protection failed")
}

func TestDoCheckIfConditionWriteSupport_BucketNotFound(t *testing.T) {
	ctx := context.Background()
	bucketName := "non-existent-bucket"
	basePath := "test-path"

	handler := &testMinioHandler{
		putObjectIfNoneMatchFunc: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error) {
			return minio.UploadInfo{}, minio.ErrorResponse{
				Code:       "NoSuchBucket",
				Message:    "The specified bucket does not exist",
				BucketName: bucketName,
			}
		},
		removeObjectFunc: func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
			return nil
		},
	}

	// Test that the function panics when bucket is not found
	assert.Panics(t, func() {
		doCheckIfConditionWriteSupport(ctx, handler, bucketName, basePath)
	}, "doCheckIfConditionWriteSupport should panic when bucket is not found")

	t.Logf("doCheckIfConditionWriteSupport correctly panicked for bucket not found")
}

func TestCheckIfConditionWriteSupportWithSyncOnce(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket"
	basePath := "test-path"

	// Reset sync.Once for this test
	resetCheckOnce()

	callCount := 0
	handler := &testMinioHandler{
		putObjectIfNoneMatchFunc: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error) {
			callCount++
			if callCount == 1 {
				return minio.UploadInfo{ETag: "test-etag"}, nil
			} else if callCount == 2 {
				return minio.UploadInfo{}, werr.ErrObjectAlreadyExists
			} else if callCount == 3 {
				return minio.UploadInfo{}, werr.ErrSegmentFenced
			}
			return minio.UploadInfo{}, nil
		},
		putFencedObjectFunc: func(ctx context.Context, bucketName, objectName string) (minio.UploadInfo, error) {
			return minio.UploadInfo{ETag: "fenced-etag"}, nil
		},
		removeObjectFunc: func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
			return nil
		},
	}

	// Test the public function with sync.Once
	assert.NotPanics(t, func() {
		CheckIfConditionWriteSupport(ctx, handler, bucketName, basePath)
	}, "CheckIfConditionWriteSupport should not panic with proper test setup")

	// Test that subsequent calls are ignored due to sync.Once
	assert.NotPanics(t, func() {
		CheckIfConditionWriteSupport(ctx, handler, bucketName, basePath)
	}, "Subsequent calls to CheckIfConditionWriteSupport should be ignored")

	t.Logf("CheckIfConditionWriteSupport with sync.Once works correctly")
}
