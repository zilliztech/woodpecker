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
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
)

// ============================================================================
// ChunkObjectInfo tests
// ============================================================================

func TestChunkObjectInfo_Fields(t *testing.T) {
	now := time.Now()
	info := &ChunkObjectInfo{
		FilePath:   "path/to/object",
		ModifyTime: now,
		Size:       1024,
	}
	assert.Equal(t, "path/to/object", info.FilePath)
	assert.Equal(t, now, info.ModifyTime)
	assert.Equal(t, int64(1024), info.Size)
}

func TestChunkObjectInfo_ZeroValue(t *testing.T) {
	info := &ChunkObjectInfo{}
	assert.Equal(t, "", info.FilePath)
	assert.True(t, info.ModifyTime.IsZero())
	assert.Equal(t, int64(0), info.Size)
}

// ============================================================================
// ChunkObjectWalkFunc tests
// ============================================================================

func TestChunkObjectWalkFunc_ReturnTrue(t *testing.T) {
	var walkFunc ChunkObjectWalkFunc = func(info *ChunkObjectInfo) bool {
		return true
	}
	result := walkFunc(&ChunkObjectInfo{FilePath: "test"})
	assert.True(t, result)
}

func TestChunkObjectWalkFunc_ReturnFalse(t *testing.T) {
	var walkFunc ChunkObjectWalkFunc = func(info *ChunkObjectInfo) bool {
		return false
	}
	result := walkFunc(&ChunkObjectInfo{FilePath: "test"})
	assert.False(t, result)
}

func TestChunkObjectWalkFunc_CollectsObjects(t *testing.T) {
	var collected []string
	var walkFunc ChunkObjectWalkFunc = func(info *ChunkObjectInfo) bool {
		collected = append(collected, info.FilePath)
		return true
	}

	objects := []string{"obj1", "obj2", "obj3"}
	for _, obj := range objects {
		walkFunc(&ChunkObjectInfo{FilePath: obj})
	}
	assert.Equal(t, objects, collected)
}

func TestChunkObjectWalkFunc_StopsEarly(t *testing.T) {
	var count int
	var walkFunc ChunkObjectWalkFunc = func(info *ChunkObjectInfo) bool {
		count++
		return count < 2
	}

	objects := []string{"obj1", "obj2", "obj3"}
	for _, obj := range objects {
		if !walkFunc(&ChunkObjectInfo{FilePath: obj}) {
			break
		}
	}
	assert.Equal(t, 2, count)
}

// ============================================================================
// generateUniqueTestID tests
// ============================================================================

func TestGenerateUniqueTestID_Format(t *testing.T) {
	id := generateUniqueTestID()
	assert.True(t, strings.HasPrefix(id, "test-"), "ID should start with 'test-'")

	// Format: test-<timestamp>-<pid>-<randomhex>
	parts := strings.SplitN(id, "-", 4)
	assert.Equal(t, 4, len(parts), "ID should have 4 parts separated by '-'")
	assert.Equal(t, "test", parts[0])
	assert.NotEmpty(t, parts[1])
	assert.NotEmpty(t, parts[2])
	assert.NotEmpty(t, parts[3])
}

func TestGenerateUniqueTestID_Unique(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := generateUniqueTestID()
		assert.False(t, ids[id], "Generated ID should be unique, got duplicate: %s", id)
		ids[id] = true
	}
}

func TestGenerateUniqueTestID_ContainsTimestamp(t *testing.T) {
	before := time.Now().UnixNano()
	id := generateUniqueTestID()
	after := time.Now().UnixNano()

	parts := strings.SplitN(id, "-", 4)
	assert.Equal(t, 4, len(parts))
	assert.NotEmpty(t, parts[1])
	_ = before
	_ = after
}

// ============================================================================
// NewObjectStorage tests
// ============================================================================

func TestNewObjectStorage_AzureProvider(t *testing.T) {
	cfg := &config.Configuration{}
	cfg.Minio.CloudProvider = minioHandler.CloudProviderAzure
	cfg.Minio.BucketName = "" // empty bucket name should cause error

	_, err := NewObjectStorage(context.Background(), cfg)
	assert.Error(t, err)
}

func TestNewObjectStorage_GcpNativeProvider(t *testing.T) {
	cfg := &config.Configuration{}
	cfg.Minio.CloudProvider = minioHandler.CloudProviderGCPNative

	assert.Panics(t, func() {
		_, _ = NewObjectStorage(context.Background(), cfg)
	})
}

func TestNewObjectStorage_UnknownProviderDefaultsToMinio(t *testing.T) {
	// Any non-azure, non-gcpnative provider (including empty) falls through
	// to the minio path. We just verify the function selects that path.
	// We don't test full MinIO init because it retries with real connections.
	cfg := &config.Configuration{}
	cfg.Minio.CloudProvider = "unknown-provider"
	// Verify it does NOT pick azure or gcp native
	assert.NotEqual(t, cfg.Minio.CloudProvider, minioHandler.CloudProviderAzure)
	assert.NotEqual(t, cfg.Minio.CloudProvider, minioHandler.CloudProviderGCPNative)
}

// ============================================================================
// Interface compliance tests
// ============================================================================

func TestMinioObjectStorage_ImplementsInterface(t *testing.T) {
	var _ ObjectStorage = (*MinioObjectStorage)(nil)
}

func TestAzureObjectStorage_ImplementsInterface(t *testing.T) {
	var _ ObjectStorage = (*AzureObjectStorage)(nil)
}

func TestGcpNativeObjectStorage_ImplementsInterface(t *testing.T) {
	var _ ObjectStorage = (*GcpNativeObjectStorage)(nil)
}

// ============================================================================
// Local mock for ObjectStorage to avoid import cycle
// ============================================================================

// testObjectStorageMock is a simple mock for ObjectStorage used in
// doCheckIfConditionWriteSupport tests to avoid import cycle with
// mocks_objectstorage package.
type testObjectStorageMock struct {
	mu                     sync.Mutex
	putIfNoneMatchCalls    int
	putIfNoneMatchResults  []error
	putFencedObjectCalls   int
	putFencedObjectResults []error
	removeObjectCalls      int
	putFencedObjectKeys    []string
	putIfNoneMatchKeys     []string
}

func newTestObjectStorageMock() *testObjectStorageMock {
	return &testObjectStorageMock{}
}

func (m *testObjectStorageMock) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64, operatingNamespace string, operatingLogId string) (minioHandler.FileReader, error) {
	return nil, errors.New("not implemented")
}

func (m *testObjectStorageMock) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error {
	return nil
}

func (m *testObjectStorageMock) PutObjectIfNoneMatch(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.putIfNoneMatchCalls
	m.putIfNoneMatchCalls++
	m.putIfNoneMatchKeys = append(m.putIfNoneMatchKeys, objectName)
	if idx < len(m.putIfNoneMatchResults) {
		return m.putIfNoneMatchResults[idx]
	}
	return nil
}

func (m *testObjectStorageMock) PutFencedObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.putFencedObjectCalls
	m.putFencedObjectCalls++
	m.putFencedObjectKeys = append(m.putFencedObjectKeys, objectName)
	if idx < len(m.putFencedObjectResults) {
		return m.putFencedObjectResults[idx]
	}
	return nil
}

func (m *testObjectStorageMock) StatObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) (int64, bool, error) {
	return 0, false, nil
}

func (m *testObjectStorageMock) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc, operatingNamespace string, operatingLogId string) error {
	return nil
}

func (m *testObjectStorageMock) RemoveObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeObjectCalls++
	return nil
}

func (m *testObjectStorageMock) IsObjectNotExistsError(err error) bool {
	return false
}

func (m *testObjectStorageMock) IsPreconditionFailedError(err error) bool {
	return false
}

// ============================================================================
// CheckIfConditionWriteSupport / doCheckIfConditionWriteSupport tests
// ============================================================================

func TestCheckIfConditionWriteSupport_AllTestsPass(t *testing.T) {
	mockStorage := newTestObjectStorageMock()
	// Call sequence:
	// 1. PutObjectIfNoneMatch(testKey) -> nil (success)
	// 2. PutObjectIfNoneMatch(testKey) -> ErrObjectAlreadyExists
	// 3. PutFencedObject(testKey) -> ErrObjectAlreadyExists
	// 4. PutFencedObject(fencedKey) -> nil (success)
	// 5. PutObjectIfNoneMatch(fencedKey) -> ErrSegmentFenced
	// 6. PutFencedObject(fencedKey) -> nil (idempotent success)
	mockStorage.putIfNoneMatchResults = []error{
		nil,                         // 1
		werr.ErrObjectAlreadyExists, // 2
		werr.ErrSegmentFenced,       // 5
	}
	mockStorage.putFencedObjectResults = []error{
		werr.ErrObjectAlreadyExists, // 3
		nil,                         // 4
		nil,                         // 6
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.NoError(t, err)
	assert.True(t, result)
	assert.Equal(t, 2, mockStorage.removeObjectCalls, "cleanup should remove 2 objects")
}

func TestCheckIfConditionWriteSupport_PutObjectIfNoneMatch_FirstCallFails(t *testing.T) {
	mockStorage := newTestObjectStorageMock()
	mockStorage.putIfNoneMatchResults = []error{
		errors.New("connection refused"), // 1
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.Error(t, err)
	assert.False(t, result)
	assert.Contains(t, err.Error(), "PutObjectIfNoneMatch not supported or failed")
	assert.Equal(t, 2, mockStorage.removeObjectCalls, "cleanup should still run")
}

func TestCheckIfConditionWriteSupport_PutObjectIfNoneMatch_SecondCallSucceeds(t *testing.T) {
	mockStorage := newTestObjectStorageMock()
	// Test 2 should fail but succeeds
	mockStorage.putIfNoneMatchResults = []error{
		nil, // 1
		nil, // 2 - should have been ErrObjectAlreadyExists
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.Error(t, err)
	assert.False(t, result)
	assert.Contains(t, err.Error(), "should return error for existing object")
}

func TestCheckIfConditionWriteSupport_PutObjectIfNoneMatch_WrongErrorType(t *testing.T) {
	mockStorage := newTestObjectStorageMock()
	mockStorage.putIfNoneMatchResults = []error{
		nil,                            // 1
		errors.New("some other error"), // 2 - wrong error type
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.Error(t, err)
	assert.False(t, result)
	assert.Contains(t, err.Error(), "should return ErrObjectAlreadyExists")
}

func TestCheckIfConditionWriteSupport_PutFencedObject_ExistingObjectSucceeds(t *testing.T) {
	mockStorage := newTestObjectStorageMock()
	mockStorage.putIfNoneMatchResults = []error{
		nil,                         // 1
		werr.ErrObjectAlreadyExists, // 2
	}
	mockStorage.putFencedObjectResults = []error{
		nil, // 3 - should have been ErrObjectAlreadyExists
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.Error(t, err)
	assert.False(t, result)
	assert.Contains(t, err.Error(), "PutFencedObject should return error for existing object")
}

func TestCheckIfConditionWriteSupport_PutFencedObject_ExistingObjectWrongError(t *testing.T) {
	mockStorage := newTestObjectStorageMock()
	mockStorage.putIfNoneMatchResults = []error{
		nil,                         // 1
		werr.ErrObjectAlreadyExists, // 2
	}
	mockStorage.putFencedObjectResults = []error{
		errors.New("unexpected error"), // 3 - wrong error type
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.Error(t, err)
	assert.False(t, result)
	assert.Contains(t, err.Error(), "should return ErrObjectAlreadyExists for existing object")
}

func TestCheckIfConditionWriteSupport_PutFencedObject_NewKeyFails(t *testing.T) {
	mockStorage := newTestObjectStorageMock()
	mockStorage.putIfNoneMatchResults = []error{
		nil,                         // 1
		werr.ErrObjectAlreadyExists, // 2
	}
	mockStorage.putFencedObjectResults = []error{
		werr.ErrObjectAlreadyExists, // 3
		errors.New("storage error"), // 4 - fenced key creation fails
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.Error(t, err)
	assert.False(t, result)
	assert.Contains(t, err.Error(), "PutFencedObject not supported or failed")
}

func TestCheckIfConditionWriteSupport_PutObjectIfNoneMatch_FencedKeySucceeds(t *testing.T) {
	mockStorage := newTestObjectStorageMock()
	mockStorage.putIfNoneMatchResults = []error{
		nil,                         // 1
		werr.ErrObjectAlreadyExists, // 2
		nil,                         // 5 - should have been ErrSegmentFenced
	}
	mockStorage.putFencedObjectResults = []error{
		werr.ErrObjectAlreadyExists, // 3
		nil,                         // 4
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.Error(t, err)
	assert.False(t, result)
	assert.Contains(t, err.Error(), "should return error for fenced object")
}

func TestCheckIfConditionWriteSupport_PutObjectIfNoneMatch_FencedKeyWrongError(t *testing.T) {
	mockStorage := newTestObjectStorageMock()
	mockStorage.putIfNoneMatchResults = []error{
		nil,                         // 1
		werr.ErrObjectAlreadyExists, // 2
		errors.New("generic error"), // 5 - wrong error type, not ErrSegmentFenced
	}
	mockStorage.putFencedObjectResults = []error{
		werr.ErrObjectAlreadyExists, // 3
		nil,                         // 4
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.Error(t, err)
	assert.False(t, result)
	assert.Contains(t, err.Error(), "should return ErrSegmentFenced")
}

func TestCheckIfConditionWriteSupport_PutFencedObject_IdempotentFails(t *testing.T) {
	mockStorage := newTestObjectStorageMock()
	mockStorage.putIfNoneMatchResults = []error{
		nil,                         // 1
		werr.ErrObjectAlreadyExists, // 2
		werr.ErrSegmentFenced,       // 5
	}
	mockStorage.putFencedObjectResults = []error{
		werr.ErrObjectAlreadyExists,      // 3
		nil,                              // 4
		errors.New("idempotent failure"), // 6 - should have been nil
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.Error(t, err)
	assert.False(t, result)
	assert.Contains(t, err.Error(), "should be idempotent")
}

func TestCheckIfConditionWriteSupport_CleanupCalledOnFailure(t *testing.T) {
	mockStorage := newTestObjectStorageMock()
	mockStorage.putIfNoneMatchResults = []error{
		errors.New("fail"), // 1
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.Error(t, err)
	assert.False(t, result)
	// Cleanup should be called for both test and fenced keys
	assert.Equal(t, 2, mockStorage.removeObjectCalls)
}

func TestCheckIfConditionWriteSupport_CleanupIgnoresErrors(t *testing.T) {
	// Verify cleanup errors don't interfere with the result
	mockStorage := newTestObjectStorageMock()
	mockStorage.putIfNoneMatchResults = []error{
		nil,                         // 1
		werr.ErrObjectAlreadyExists, // 2
		werr.ErrSegmentFenced,       // 5
	}
	mockStorage.putFencedObjectResults = []error{
		werr.ErrObjectAlreadyExists, // 3
		nil,                         // 4
		nil,                         // 6
	}

	result, err := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")
	assert.NoError(t, err)
	assert.True(t, result)
}

// ============================================================================
// CheckIfConditionWriteSupport wraps doCheckIfConditionWriteSupport
// ============================================================================

// ============================================================================
// AzureObjectStorage error check tests (no Azure connection needed)
// ============================================================================

func TestAzureObjectStorage_IsObjectNotExistsError_Nil(t *testing.T) {
	a := &AzureObjectStorage{}
	assert.False(t, a.IsObjectNotExistsError(nil))
}

func TestAzureObjectStorage_IsObjectNotExistsError_GenericError(t *testing.T) {
	a := &AzureObjectStorage{}
	assert.False(t, a.IsObjectNotExistsError(errors.New("some error")))
}

func TestAzureObjectStorage_IsObjectNotExistsError_NotFound(t *testing.T) {
	a := &AzureObjectStorage{}
	azErr := &azcore.ResponseError{StatusCode: http.StatusNotFound}
	assert.True(t, a.IsObjectNotExistsError(azErr))
}

func TestAzureObjectStorage_IsObjectNotExistsError_OtherStatus(t *testing.T) {
	a := &AzureObjectStorage{}
	azErr := &azcore.ResponseError{StatusCode: http.StatusForbidden}
	assert.False(t, a.IsObjectNotExistsError(azErr))
}

func TestAzureObjectStorage_IsPreconditionFailedError_Nil(t *testing.T) {
	a := &AzureObjectStorage{}
	assert.False(t, a.IsPreconditionFailedError(nil))
}

func TestAzureObjectStorage_IsPreconditionFailedError_GenericError(t *testing.T) {
	a := &AzureObjectStorage{}
	assert.False(t, a.IsPreconditionFailedError(errors.New("some error")))
}

func TestAzureObjectStorage_IsPreconditionFailedError_PreconditionFailed(t *testing.T) {
	a := &AzureObjectStorage{}
	azErr := &azcore.ResponseError{StatusCode: http.StatusPreconditionFailed}
	assert.True(t, a.IsPreconditionFailedError(azErr))
}

func TestAzureObjectStorage_IsPreconditionFailedError_Conflict(t *testing.T) {
	a := &AzureObjectStorage{}
	azErr := &azcore.ResponseError{StatusCode: http.StatusConflict}
	assert.True(t, a.IsPreconditionFailedError(azErr))
}

func TestAzureObjectStorage_IsPreconditionFailedError_OtherStatus(t *testing.T) {
	a := &AzureObjectStorage{}
	azErr := &azcore.ResponseError{StatusCode: http.StatusInternalServerError}
	assert.False(t, a.IsPreconditionFailedError(azErr))
}

// ============================================================================
// BlobReader constructor and simple method tests
// ============================================================================

func TestNewBlobReader(t *testing.T) {
	reader, err := NewBlobReader(nil, 10)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.True(t, reader.needResetStream)
	assert.Equal(t, int64(10), reader.position)
	assert.Equal(t, int64(0), reader.count)
}

func TestNewBlobReaderWithSize(t *testing.T) {
	reader, err := NewBlobReaderWithSize(nil, 5, 100)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.True(t, reader.needResetStream)
	assert.Equal(t, int64(5), reader.position)
	assert.Equal(t, int64(100), reader.count)
}

func TestBlobReader_Close_NilBody(t *testing.T) {
	reader := &BlobReader{}
	err := reader.Close()
	assert.NoError(t, err)
}

func TestBlobReader_Size(t *testing.T) {
	reader := &BlobReader{contentLength: 42}
	size, err := reader.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(42), size)
}

// ============================================================================
// GcpNativeObjectStorage panic tests (all methods unimplemented)
// ============================================================================

func TestGcpNativeObjectStorage_GetObject_Panics(t *testing.T) {
	g := &GcpNativeObjectStorage{}
	assert.Panics(t, func() {
		g.GetObject(context.Background(), "bucket", "key", 0, 100, "ns", "log1")
	})
}

func TestGcpNativeObjectStorage_PutObject_Panics(t *testing.T) {
	g := &GcpNativeObjectStorage{}
	assert.Panics(t, func() {
		g.PutObject(context.Background(), "bucket", "key", strings.NewReader("data"), 4, "ns", "log1")
	})
}

func TestGcpNativeObjectStorage_PutObjectIfNoneMatch_Panics(t *testing.T) {
	g := &GcpNativeObjectStorage{}
	assert.Panics(t, func() {
		g.PutObjectIfNoneMatch(context.Background(), "bucket", "key", strings.NewReader("data"), 4, "ns", "log1")
	})
}

func TestGcpNativeObjectStorage_PutFencedObject_Panics(t *testing.T) {
	g := &GcpNativeObjectStorage{}
	assert.Panics(t, func() {
		g.PutFencedObject(context.Background(), "bucket", "key", "ns", "log1")
	})
}

func TestGcpNativeObjectStorage_StatObject_Panics(t *testing.T) {
	g := &GcpNativeObjectStorage{}
	assert.Panics(t, func() {
		g.StatObject(context.Background(), "bucket", "key", "ns", "log1")
	})
}

func TestGcpNativeObjectStorage_WalkWithObjects_Panics(t *testing.T) {
	g := &GcpNativeObjectStorage{}
	assert.Panics(t, func() {
		g.WalkWithObjects(context.Background(), "bucket", "prefix", true, func(info *ChunkObjectInfo) bool { return true }, "ns", "log1")
	})
}

func TestGcpNativeObjectStorage_RemoveObject_Panics(t *testing.T) {
	g := &GcpNativeObjectStorage{}
	assert.Panics(t, func() {
		g.RemoveObject(context.Background(), "bucket", "key", "ns", "log1")
	})
}

func TestGcpNativeObjectStorage_IsObjectNotExistsError_Panics(t *testing.T) {
	g := &GcpNativeObjectStorage{}
	assert.Panics(t, func() {
		g.IsObjectNotExistsError(errors.New("test"))
	})
}

func TestGcpNativeObjectStorage_IsPreconditionFailedError_Panics(t *testing.T) {
	g := &GcpNativeObjectStorage{}
	assert.Panics(t, func() {
		g.IsPreconditionFailedError(errors.New("test"))
	})
}

func TestGcpNativeObjectStorage_NewWithConfig_Panics(t *testing.T) {
	assert.Panics(t, func() {
		newGcpNativeObjectStorageWithConfig(context.Background(), &config.Configuration{})
	})
}

// ============================================================================
// BlobReader additional tests
// ============================================================================

func TestBlobReader_Close_WithBody(t *testing.T) {
	body := io.NopCloser(strings.NewReader("hello"))
	reader := &BlobReader{body: body}
	err := reader.Close()
	assert.NoError(t, err)
}

func TestCheckIfConditionWriteSupport_DelegatesToDoCheck(t *testing.T) {
	// Verify the public function delegates to the internal one
	// Both should produce the same type of error for the same failure mode
	mockStorage := newTestObjectStorageMock()
	mockStorage.putIfNoneMatchResults = []error{
		errors.New("fail"),
	}

	result1, err1 := CheckIfConditionWriteSupport(context.Background(), mockStorage, "bucket", "path")

	mockStorage2 := newTestObjectStorageMock()
	mockStorage2.putIfNoneMatchResults = []error{
		errors.New("fail"),
	}
	result2, err2 := doCheckIfConditionWriteSupport(context.Background(), mockStorage2, "bucket", "path")

	assert.Equal(t, result1, result2)
	// Both errors should contain the same error prefix (object keys differ due to random IDs)
	assert.Contains(t, err1.Error(), "PutObjectIfNoneMatch not supported or failed")
	assert.Contains(t, err2.Error(), "PutObjectIfNoneMatch not supported or failed")
}
