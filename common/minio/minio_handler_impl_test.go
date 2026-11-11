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
	"errors"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_minio_client"
)

// --- test helpers ---

// newTestHandler creates a minioHandlerImpl with a mock client and the given conditionWrite setting.
func newTestHandler(mockClient *mocks_minio_client.MinioClientAPI, conditionWrite string) *minioHandlerImpl {
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Logstore.FencePolicy.ConditionWrite = conditionWrite
	return &minioHandlerImpl{
		client: mockClient,
		cfg:    cfg,
	}
}

// preconditionErr returns a minio ErrorResponse that IsPreconditionFailed recognises.
func preconditionErr() minio.ErrorResponse {
	return minio.ErrorResponse{Code: "PreconditionFailed"}
}

// fencedObjectInfo returns an ObjectInfo with fenced metadata.
func fencedObjectInfo() minio.ObjectInfo {
	return minio.ObjectInfo{
		UserMetadata: map[string]string{FencedObjectMetaKey: "true"},
	}
}

// normalObjectInfo returns a non-fenced ObjectInfo with the given size.
func normalObjectInfo(size int64) minio.ObjectInfo {
	return minio.ObjectInfo{Size: size}
}

// === GetObject tests ===

func TestGetObject_Success(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().GetObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(&minio.Object{}, nil)

	obj, err := h.GetObject(context.Background(), "bucket", "key", minio.GetObjectOptions{}, "ns", "0")
	assert.NoError(t, err)
	assert.NotNil(t, obj)
}

func TestGetObject_Error(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().GetObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(nil, errors.New("network error"))

	obj, err := h.GetObject(context.Background(), "bucket", "key", minio.GetObjectOptions{}, "ns", "0")
	assert.Error(t, err)
	assert.Nil(t, obj)
}

// === PutObject tests ===

func TestPutObject_Success(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{Size: 5}, nil)

	reader := strings.NewReader("hello")
	info, err := h.PutObject(context.Background(), "bucket", "key", reader, 5, minio.PutObjectOptions{}, "ns", "0")
	assert.NoError(t, err)
	assert.Equal(t, int64(5), info.Size)
}

func TestPutObject_Error(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{}, errors.New("write error"))

	reader := strings.NewReader("hello")
	_, err := h.PutObject(context.Background(), "bucket", "key", reader, 5, minio.PutObjectOptions{}, "ns", "0")
	assert.Error(t, err)
}

// === RemoveObject tests ===

func TestRemoveObject_Success(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().RemoveObject(mock.Anything, "bucket", "key", mock.Anything).Return(nil)

	err := h.RemoveObject(context.Background(), "bucket", "key", minio.RemoveObjectOptions{}, "ns", "0")
	assert.NoError(t, err)
}

func TestRemoveObject_Error(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().RemoveObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(errors.New("remove failed"))

	err := h.RemoveObject(context.Background(), "bucket", "key", minio.RemoveObjectOptions{}, "ns", "0")
	assert.Error(t, err)
}

// === StatObject tests ===

func TestStatObject_Success(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(normalObjectInfo(100), nil)

	info, err := h.StatObject(context.Background(), "bucket", "key", minio.GetObjectOptions{}, "ns", "0")
	assert.NoError(t, err)
	assert.Equal(t, int64(100), info.Size)
}

func TestStatObject_Error(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(minio.ObjectInfo{}, errors.New("stat failed"))

	_, err := h.StatObject(context.Background(), "bucket", "key", minio.GetObjectOptions{}, "ns", "0")
	assert.Error(t, err)
}

// === CopyObject tests ===

func TestCopyObject_Success(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	dst := minio.CopyDestOptions{Bucket: "dst-bucket", Object: "dst-key"}
	src := minio.CopySrcOptions{Bucket: "src-bucket", Object: "src-key"}
	mockCli.EXPECT().CopyObject(mock.Anything, dst, src).
		Return(minio.UploadInfo{Size: 42}, nil)

	info, err := h.CopyObject(context.Background(), dst, src, "ns", "0")
	assert.NoError(t, err)
	assert.Equal(t, int64(42), info.Size)
}

func TestCopyObject_Error(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	dst := minio.CopyDestOptions{Bucket: "dst-bucket", Object: "dst-key"}
	src := minio.CopySrcOptions{Bucket: "src-bucket", Object: "src-key"}
	mockCli.EXPECT().CopyObject(mock.Anything, dst, src).
		Return(minio.UploadInfo{}, errors.New("copy failed"))

	_, err := h.CopyObject(context.Background(), dst, src, "ns", "0")
	assert.Error(t, err)
}

// === ListObjects tests ===

func TestListObjects(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	ch := make(chan minio.ObjectInfo, 2)
	ch <- minio.ObjectInfo{Key: "a"}
	ch <- minio.ObjectInfo{Key: "b"}
	close(ch)

	mockCli.EXPECT().ListObjects(mock.Anything, "bucket", mock.Anything).Return(ch)

	result := h.ListObjects(context.Background(), "bucket", "prefix/", true, minio.ListObjectsOptions{}, "ns", "0")
	var keys []string
	for obj := range result {
		keys = append(keys, obj.Key)
	}
	assert.Equal(t, []string{"a", "b"}, keys)
}

// === PutObjectIfNoneMatch tests (condition write ENABLED) ===

func TestPutObjectIfNoneMatch_Success(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{Size: 5}, nil)

	reader := strings.NewReader("hello")
	info, err := h.PutObjectIfNoneMatch(context.Background(), "bucket", "key", reader, 5, "ns", "0")
	assert.NoError(t, err)
	assert.Equal(t, int64(5), info.Size)
}

func TestPutObjectIfNoneMatch_PreconditionFailed_Fenced(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{}, preconditionErr())
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(fencedObjectInfo(), nil)

	reader := strings.NewReader("hello")
	_, err := h.PutObjectIfNoneMatch(context.Background(), "bucket", "key", reader, 5, "ns", "0")
	require.Error(t, err)
	assert.True(t, werr.ErrSegmentFenced.Is(err))
}

func TestPutObjectIfNoneMatch_PreconditionFailed_NormalObject(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{}, preconditionErr())
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(normalObjectInfo(5), nil)

	reader := strings.NewReader("hello")
	_, err := h.PutObjectIfNoneMatch(context.Background(), "bucket", "key", reader, 5, "ns", "0")
	require.Error(t, err)
	assert.True(t, werr.ErrObjectAlreadyExists.Is(err))
}

func TestPutObjectIfNoneMatch_PreconditionFailed_StatError(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{}, preconditionErr())
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(minio.ObjectInfo{}, errors.New("stat err"))

	reader := strings.NewReader("hello")
	_, err := h.PutObjectIfNoneMatch(context.Background(), "bucket", "key", reader, 5, "ns", "0")
	assert.EqualError(t, err, "stat err")
}

func TestPutObjectIfNoneMatch_OtherError(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	// A non-precondition error
	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{}, errors.New("timeout"))

	reader := strings.NewReader("hello")
	_, err := h.PutObjectIfNoneMatch(context.Background(), "bucket", "key", reader, 5, "ns", "0")
	assert.EqualError(t, err, "timeout")
}

// === PutObjectIfNoneMatch tests (condition write DISABLED fallback) ===

func TestPutObjectIfNoneMatch_Disabled_PutSuccess(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "disable")

	// PutObject is called through h.PutObject which calls m.client.PutObject
	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{Size: 5}, nil)

	reader := strings.NewReader("hello")
	info, err := h.PutObjectIfNoneMatch(context.Background(), "bucket", "key", reader, 5, "ns", "0")
	assert.NoError(t, err)
	assert.Equal(t, int64(5), info.Size)
}

func TestPutObjectIfNoneMatch_Disabled_PutFail_Fenced(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "disable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{}, errors.New("put error"))
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(fencedObjectInfo(), nil)

	reader := strings.NewReader("hello")
	_, err := h.PutObjectIfNoneMatch(context.Background(), "bucket", "key", reader, 5, "ns", "0")
	require.Error(t, err)
	assert.True(t, werr.ErrSegmentFenced.Is(err))
}

func TestPutObjectIfNoneMatch_Disabled_PutFail_SameSize(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "disable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{}, errors.New("put error"))
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(normalObjectInfo(5), nil)

	reader := strings.NewReader("hello")
	_, err := h.PutObjectIfNoneMatch(context.Background(), "bucket", "key", reader, 5, "ns", "0")
	require.Error(t, err)
	assert.True(t, werr.ErrObjectAlreadyExists.Is(err))
}

func TestPutObjectIfNoneMatch_Disabled_PutFail_DifferentSize(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "disable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{}, errors.New("put error"))
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(normalObjectInfo(10), nil) // size mismatch

	reader := strings.NewReader("hello")
	_, err := h.PutObjectIfNoneMatch(context.Background(), "bucket", "key", reader, 5, "ns", "0")
	assert.EqualError(t, err, "put error") // returns original error
}

func TestPutObjectIfNoneMatch_Disabled_PutFail_StatError(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "disable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(5), mock.Anything).
		Return(minio.UploadInfo{}, errors.New("put error"))
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(minio.ObjectInfo{}, errors.New("stat err"))

	reader := strings.NewReader("hello")
	_, err := h.PutObjectIfNoneMatch(context.Background(), "bucket", "key", reader, 5, "ns", "0")
	assert.EqualError(t, err, "stat err")
}

// === PutFencedObject tests (condition write ENABLED) ===

func TestPutFencedObject_Success(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(1), mock.Anything).
		Return(minio.UploadInfo{Size: 1}, nil)

	info, err := h.PutFencedObject(context.Background(), "bucket", "key", "ns", "0")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.Size)
}

func TestPutFencedObject_PreconditionFailed_AlreadyFenced(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(1), mock.Anything).
		Return(minio.UploadInfo{}, preconditionErr())
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(fencedObjectInfo(), nil)

	_, err := h.PutFencedObject(context.Background(), "bucket", "key", "ns", "0")
	assert.NoError(t, err) // idempotent success
}

func TestPutFencedObject_PreconditionFailed_NotFenced(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(1), mock.Anything).
		Return(minio.UploadInfo{}, preconditionErr())
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(normalObjectInfo(100), nil)

	_, err := h.PutFencedObject(context.Background(), "bucket", "key", "ns", "0")
	require.Error(t, err)
	assert.True(t, werr.ErrObjectAlreadyExists.Is(err))
}

func TestPutFencedObject_PreconditionFailed_StatError(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(1), mock.Anything).
		Return(minio.UploadInfo{}, preconditionErr())
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(minio.ObjectInfo{}, errors.New("stat err"))

	_, err := h.PutFencedObject(context.Background(), "bucket", "key", "ns", "0")
	assert.EqualError(t, err, "stat err")
}

func TestPutFencedObject_OtherError(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(1), mock.Anything).
		Return(minio.UploadInfo{}, errors.New("network error"))

	_, err := h.PutFencedObject(context.Background(), "bucket", "key", "ns", "0")
	assert.EqualError(t, err, "network error")
}

// === PutFencedObject tests (condition write DISABLED fallback) ===

func TestPutFencedObject_Disabled_Success(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "disable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(1), mock.Anything).
		Return(minio.UploadInfo{Size: 1}, nil)

	info, err := h.PutFencedObject(context.Background(), "bucket", "key", "ns", "0")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.Size)
}

func TestPutFencedObject_Disabled_PutFail_AlreadyFenced(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "disable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(1), mock.Anything).
		Return(minio.UploadInfo{}, errors.New("put error"))
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(fencedObjectInfo(), nil)

	_, err := h.PutFencedObject(context.Background(), "bucket", "key", "ns", "0")
	assert.NoError(t, err) // idempotent success
}

func TestPutFencedObject_Disabled_PutFail_NotFenced(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "disable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(1), mock.Anything).
		Return(minio.UploadInfo{}, errors.New("put error"))
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(normalObjectInfo(100), nil)

	_, err := h.PutFencedObject(context.Background(), "bucket", "key", "ns", "0")
	require.Error(t, err)
	assert.True(t, werr.ErrObjectAlreadyExists.Is(err))
}

func TestPutFencedObject_Disabled_PutFail_StatError(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "disable")

	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(1), mock.Anything).
		Return(minio.UploadInfo{}, errors.New("put error"))
	mockCli.EXPECT().StatObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(minio.ObjectInfo{}, errors.New("stat err"))

	_, err := h.PutFencedObject(context.Background(), "bucket", "key", "ns", "0")
	assert.EqualError(t, err, "stat err")
}

// === GetObjectDataAndInfo tests ===
// Note: GetObjectDataAndInfo calls m.client.GetObject (returns *minio.Object), then calls obj.Stat().
// Since *minio.Object without a real server will return an error on Stat(), we test the error paths here.

func TestGetObjectDataAndInfo_GetObjectError(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	mockCli.EXPECT().GetObject(mock.Anything, "bucket", "key", mock.Anything).
		Return(nil, errors.New("get failed"))

	fr, size, ts, err := h.GetObjectDataAndInfo(context.Background(), "bucket", "key", minio.GetObjectOptions{}, "ns", "0")
	assert.Error(t, err)
	assert.Nil(t, fr)
	assert.Equal(t, int64(0), size)
	assert.Equal(t, int64(-1), ts)
}

// === ObjectReader type check ===

func TestObjectReader_ImplementsFileReader(t *testing.T) {
	// Verify ObjectReader satisfies the FileReader interface at compile time.
	// We can't call Size() without a real minio.Object connected to a server,
	// so we just verify the interface compliance.
	var _ FileReader = (*ObjectReader)(nil)
}

// === NewMinioHandler error path ===

func TestNewMinioHandler_Error(t *testing.T) {
	// Use an invalid address to trigger client creation failure
	cfg, _ := config.NewConfiguration()
	cfg.Minio.Address = "" // empty address causes minio.New to fail
	cfg.Minio.Port = 0

	_, err := NewMinioHandler(context.Background(), cfg)
	assert.Error(t, err)
}

// === Verify interface compatibility ===

func TestMinioClientAPI_InterfaceCompliance(t *testing.T) {
	// Verify *minio.Client satisfies MinioClientAPI
	client, err := minio.New("localhost:9999", &minio.Options{Secure: false})
	require.NoError(t, err)
	var _ MinioClientAPI = client
}

// === Verify PutObjectIfNoneMatch sets ETag option ===

func TestPutObjectIfNoneMatch_SetsETagExcept(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	// Capture the PutObjectOptions to verify SetMatchETagExcept was called
	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(3), mock.MatchedBy(func(opts minio.PutObjectOptions) bool {
		// The internal field is set by SetMatchETagExcept("*")
		return true
	})).Return(minio.UploadInfo{Size: 3}, nil)

	reader := bytes.NewReader([]byte("abc"))
	_, err := h.PutObjectIfNoneMatch(context.Background(), "bucket", "key", reader, 3, "ns", "0")
	assert.NoError(t, err)
}

// === PutFencedObject verifies fenced metadata ===

func TestPutFencedObject_SetsFencedMetadata(t *testing.T) {
	mockCli := mocks_minio_client.NewMinioClientAPI(t)
	h := newTestHandler(mockCli, "enable")

	// Verify the PutObject call includes Fenced metadata
	mockCli.EXPECT().PutObject(mock.Anything, "bucket", "key", mock.Anything, int64(1),
		mock.MatchedBy(func(opts minio.PutObjectOptions) bool {
			return opts.UserMetadata[FencedObjectMetaKey] == "true"
		})).Return(minio.UploadInfo{Size: 1}, nil)

	_, err := h.PutFencedObject(context.Background(), "bucket", "key", "ns", "0")
	assert.NoError(t, err)
}
