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
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
)

// ============================================================================
// Manual mocks for Azure interfaces
// ============================================================================

type mockBlobClient struct {
	downloadStreamFunc func(ctx context.Context, o *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error)
	uploadStreamFunc   func(ctx context.Context, body io.Reader, o *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error)
	deleteFunc         func(ctx context.Context, o *blob.DeleteOptions) (blob.DeleteResponse, error)
	getPropertiesFunc  func(ctx context.Context, o *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error)
}

func (m *mockBlobClient) DownloadStream(ctx context.Context, o *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
	return m.downloadStreamFunc(ctx, o)
}

func (m *mockBlobClient) UploadStream(ctx context.Context, body io.Reader, o *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
	return m.uploadStreamFunc(ctx, body, o)
}

func (m *mockBlobClient) Delete(ctx context.Context, o *blob.DeleteOptions) (blob.DeleteResponse, error) {
	return m.deleteFunc(ctx, o)
}

func (m *mockBlobClient) GetProperties(ctx context.Context, o *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
	return m.getPropertiesFunc(ctx, o)
}

type mockServiceClient struct {
	blobClient   AzureBlobClient
	flatPager    FlatBlobPager
	hierPager    HierarchyBlobPager
	blobClientFn func(containerName, blobName string) AzureBlobClient
	flatPagerFn  func(containerName string, opts *azblob.ListBlobsFlatOptions) FlatBlobPager
	hierPagerFn  func(containerName, delimiter string, opts *container.ListBlobsHierarchyOptions) HierarchyBlobPager
}

func (m *mockServiceClient) GetBlobClient(containerName, blobName string) AzureBlobClient {
	if m.blobClientFn != nil {
		return m.blobClientFn(containerName, blobName)
	}
	return m.blobClient
}

func (m *mockServiceClient) NewListBlobsFlatPager(containerName string, opts *azblob.ListBlobsFlatOptions) FlatBlobPager {
	if m.flatPagerFn != nil {
		return m.flatPagerFn(containerName, opts)
	}
	return m.flatPager
}

func (m *mockServiceClient) NewListBlobsHierarchyPager(containerName, delimiter string, opts *container.ListBlobsHierarchyOptions) HierarchyBlobPager {
	if m.hierPagerFn != nil {
		return m.hierPagerFn(containerName, delimiter, opts)
	}
	return m.hierPager
}

type mockFlatPager struct {
	pages []container.ListBlobsFlatResponse
	idx   int
}

func (m *mockFlatPager) More() bool {
	return m.idx < len(m.pages)
}

func (m *mockFlatPager) NextPage(_ context.Context) (container.ListBlobsFlatResponse, error) {
	if m.idx >= len(m.pages) {
		return container.ListBlobsFlatResponse{}, fmt.Errorf("no more pages")
	}
	page := m.pages[m.idx]
	m.idx++
	return page, nil
}

type mockFlatPagerWithError struct {
	err error
}

func (m *mockFlatPagerWithError) More() bool { return true }
func (m *mockFlatPagerWithError) NextPage(_ context.Context) (container.ListBlobsFlatResponse, error) {
	return container.ListBlobsFlatResponse{}, m.err
}

type mockHierPager struct {
	pages []container.ListBlobsHierarchyResponse
	idx   int
}

func (m *mockHierPager) More() bool {
	return m.idx < len(m.pages)
}

func (m *mockHierPager) NextPage(_ context.Context) (container.ListBlobsHierarchyResponse, error) {
	if m.idx >= len(m.pages) {
		return container.ListBlobsHierarchyResponse{}, fmt.Errorf("no more pages")
	}
	page := m.pages[m.idx]
	m.idx++
	return page, nil
}

type mockHierPagerWithError struct {
	err error
}

func (m *mockHierPagerWithError) More() bool { return true }
func (m *mockHierPagerWithError) NextPage(_ context.Context) (container.ListBlobsHierarchyResponse, error) {
	return container.ListBlobsHierarchyResponse{}, m.err
}

// helpers
func ptr[T any](v T) *T { return &v }

func newDownloadStreamResponse(data string) blob.DownloadStreamResponse {
	contentLen := int64(len(data))
	return blob.DownloadStreamResponse{
		DownloadResponse: blob.DownloadResponse{
			Body:          io.NopCloser(strings.NewReader(data)),
			ContentLength: &contentLen,
		},
	}
}

func newGetPropertiesResponse(size int64, metadata map[string]*string) blob.GetPropertiesResponse {
	return blob.GetPropertiesResponse{
		ContentLength: &size,
		Metadata:      metadata,
	}
}

func preconditionError() error {
	return &azcore.ResponseError{StatusCode: http.StatusPreconditionFailed}
}

// ============================================================================
// BlobReader tests
// ============================================================================

func TestBlobReader_Read_Success(t *testing.T) {
	mock := &mockBlobClient{
		downloadStreamFunc: func(_ context.Context, o *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			assert.Equal(t, int64(0), o.Range.Offset)
			return newDownloadStreamResponse("hello"), nil
		},
	}

	reader, err := NewBlobReaderWithSize(mock, 0, 5)
	require.NoError(t, err)

	buf := make([]byte, 5)
	n, err := reader.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(buf))
	assert.Equal(t, int64(5), reader.position)
	assert.False(t, reader.needResetStream)
}

func TestBlobReader_Read_DownloadError(t *testing.T) {
	mock := &mockBlobClient{
		downloadStreamFunc: func(_ context.Context, _ *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			return blob.DownloadStreamResponse{}, fmt.Errorf("download failed")
		},
	}

	reader, _ := NewBlobReaderWithSize(mock, 0, 5)
	buf := make([]byte, 5)
	n, err := reader.Read(buf)
	assert.Error(t, err)
	assert.Equal(t, 0, n)
	assert.Contains(t, err.Error(), "download failed")
}

func TestBlobReader_Read_BodyError(t *testing.T) {
	contentLen := int64(5)
	mock := &mockBlobClient{
		downloadStreamFunc: func(_ context.Context, _ *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			return blob.DownloadStreamResponse{
				DownloadResponse: blob.DownloadResponse{
					ContentLength: &contentLen,
					Body:          io.NopCloser(&errorReader{}),
				},
			}, nil
		},
	}

	reader, _ := NewBlobReaderWithSize(mock, 0, 5)
	buf := make([]byte, 5)
	n, err := reader.Read(buf)
	assert.Error(t, err)
	assert.Equal(t, 0, n)
}

type errorReader struct{}

func (e *errorReader) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("body read error")
}

func TestBlobReader_Read_SecondCallNoResetStream(t *testing.T) {
	downloadCalls := 0
	mock := &mockBlobClient{
		downloadStreamFunc: func(_ context.Context, _ *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			downloadCalls++
			return newDownloadStreamResponse("hello world"), nil
		},
	}

	reader, _ := NewBlobReaderWithSize(mock, 0, 11)
	buf := make([]byte, 5)
	_, _ = reader.Read(buf) // first call → downloads stream
	assert.Equal(t, 1, downloadCalls)

	buf2 := make([]byte, 6)
	_, _ = reader.Read(buf2) // second call → reads from body, no download
	assert.Equal(t, 1, downloadCalls)
}

func TestBlobReader_ReadAt_Success(t *testing.T) {
	mock := &mockBlobClient{
		downloadStreamFunc: func(_ context.Context, o *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			assert.Equal(t, int64(5), o.Range.Offset)
			assert.Equal(t, int64(3), o.Range.Count)
			return newDownloadStreamResponse("wor"), nil
		},
	}

	reader, _ := NewBlobReader(mock, 0)
	buf := make([]byte, 3)
	n, err := reader.ReadAt(buf, 5)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, "wor", string(buf))
}

func TestBlobReader_ReadAt_Error(t *testing.T) {
	mock := &mockBlobClient{
		downloadStreamFunc: func(_ context.Context, _ *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
			return blob.DownloadStreamResponse{}, fmt.Errorf("read at error")
		},
	}

	reader, _ := NewBlobReader(mock, 0)
	buf := make([]byte, 3)
	_, err := reader.ReadAt(buf, 5)
	assert.Error(t, err)
}

func TestBlobReader_Seek_SeekStart(t *testing.T) {
	mock := &mockBlobClient{
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return newGetPropertiesResponse(100, nil), nil
		},
	}

	reader, _ := NewBlobReader(mock, 10)
	newPos, err := reader.Seek(50, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(50), newPos)
	assert.Equal(t, int64(50), reader.position)
	assert.True(t, reader.needResetStream)
}

func TestBlobReader_Seek_SeekCurrent(t *testing.T) {
	mock := &mockBlobClient{
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return newGetPropertiesResponse(100, nil), nil
		},
	}

	reader, _ := NewBlobReader(mock, 20)
	newPos, err := reader.Seek(10, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(30), newPos) // 20 + 10
}

func TestBlobReader_Seek_SeekEnd(t *testing.T) {
	mock := &mockBlobClient{
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return newGetPropertiesResponse(100, nil), nil
		},
	}

	reader, _ := NewBlobReader(mock, 0)
	newPos, err := reader.Seek(-10, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(90), newPos) // 100 + (-10)
}

func TestBlobReader_Seek_InvalidWhence(t *testing.T) {
	mock := &mockBlobClient{
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return newGetPropertiesResponse(100, nil), nil
		},
	}

	reader, _ := NewBlobReader(mock, 0)
	_, err := reader.Seek(0, 999)
	assert.Error(t, err)
}

func TestBlobReader_Seek_GetPropertiesError(t *testing.T) {
	mock := &mockBlobClient{
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return blob.GetPropertiesResponse{}, fmt.Errorf("properties error")
		},
	}

	reader, _ := NewBlobReader(mock, 0)
	_, err := reader.Seek(0, io.SeekStart)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "properties error")
}

// ============================================================================
// AzureObjectStorage tests
// ============================================================================

func newTestAzureStorage(blobClient AzureBlobClient) *AzureObjectStorage {
	return &AzureObjectStorage{
		client: &mockServiceClient{blobClient: blobClient},
	}
}

func TestAzureObjectStorage_GetObject(t *testing.T) {
	mock := &mockBlobClient{}
	storage := newTestAzureStorage(mock)

	reader, err := storage.GetObject(context.Background(), "bucket", "key", 0, 100, "ns", "log1")
	require.NoError(t, err)
	assert.NotNil(t, reader)
	br, ok := reader.(*BlobReader)
	assert.True(t, ok)
	assert.Equal(t, int64(0), br.position)
	assert.Equal(t, int64(100), br.count)
}

func TestAzureObjectStorage_PutObject_Success(t *testing.T) {
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, _ io.Reader, _ *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			return blockblob.UploadStreamResponse{}, nil
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutObject(context.Background(), "bucket", "key", strings.NewReader("data"), 4, "ns", "log1")
	assert.NoError(t, err)
}

func TestAzureObjectStorage_PutObject_Error(t *testing.T) {
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, _ io.Reader, _ *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			return blockblob.UploadStreamResponse{}, fmt.Errorf("upload error")
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutObject(context.Background(), "bucket", "key", strings.NewReader("data"), 4, "ns", "log1")
	assert.Error(t, err)
}

func TestAzureObjectStorage_PutObjectIfNoneMatch_Success(t *testing.T) {
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, _ io.Reader, opts *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			assert.NotNil(t, opts.AccessConditions)
			return blockblob.UploadStreamResponse{}, nil
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutObjectIfNoneMatch(context.Background(), "bucket", "key", strings.NewReader("data"), 4, "ns", "log1")
	assert.NoError(t, err)
}

func TestAzureObjectStorage_PutObjectIfNoneMatch_PreconditionFailed_Fenced(t *testing.T) {
	fenced := "true"
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, _ io.Reader, _ *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			return blockblob.UploadStreamResponse{}, preconditionError()
		},
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return newGetPropertiesResponse(1, map[string]*string{minioHandler.FencedObjectMetaKey: &fenced}), nil
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutObjectIfNoneMatch(context.Background(), "bucket", "key", strings.NewReader("data"), 4, "ns", "log1")
	assert.Error(t, err)
	assert.True(t, werr.ErrSegmentFenced.Is(err))
}

func TestAzureObjectStorage_PutObjectIfNoneMatch_PreconditionFailed_Normal(t *testing.T) {
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, _ io.Reader, _ *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			return blockblob.UploadStreamResponse{}, preconditionError()
		},
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return newGetPropertiesResponse(100, nil), nil
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutObjectIfNoneMatch(context.Background(), "bucket", "key", strings.NewReader("data"), 4, "ns", "log1")
	assert.Error(t, err)
	assert.True(t, werr.ErrObjectAlreadyExists.Is(err))
}

func TestAzureObjectStorage_PutObjectIfNoneMatch_PreconditionFailed_StatError(t *testing.T) {
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, _ io.Reader, _ *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			return blockblob.UploadStreamResponse{}, preconditionError()
		},
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return blob.GetPropertiesResponse{}, fmt.Errorf("stat error")
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutObjectIfNoneMatch(context.Background(), "bucket", "key", strings.NewReader("data"), 4, "ns", "log1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stat error")
}

func TestAzureObjectStorage_PutObjectIfNoneMatch_OtherError(t *testing.T) {
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, _ io.Reader, _ *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			return blockblob.UploadStreamResponse{}, fmt.Errorf("network error")
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutObjectIfNoneMatch(context.Background(), "bucket", "key", strings.NewReader("data"), 4, "ns", "log1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "network error")
}

func TestAzureObjectStorage_PutFencedObject_Success(t *testing.T) {
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, body io.Reader, opts *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			// Verify fenced metadata
			assert.NotNil(t, opts.Metadata)
			assert.Equal(t, "true", *opts.Metadata[minioHandler.FencedObjectMetaKey])
			// Verify body is "F"
			data, _ := io.ReadAll(body)
			assert.Equal(t, "F", string(data))
			return blockblob.UploadStreamResponse{}, nil
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutFencedObject(context.Background(), "bucket", "key", "ns", "log1")
	assert.NoError(t, err)
}

func TestAzureObjectStorage_PutFencedObject_AlreadyFenced(t *testing.T) {
	fenced := "true"
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, _ io.Reader, _ *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			return blockblob.UploadStreamResponse{}, preconditionError()
		},
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return newGetPropertiesResponse(1, map[string]*string{minioHandler.FencedObjectMetaKey: &fenced}), nil
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutFencedObject(context.Background(), "bucket", "key", "ns", "log1")
	assert.NoError(t, err) // idempotent success
}

func TestAzureObjectStorage_PutFencedObject_NotFenced(t *testing.T) {
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, _ io.Reader, _ *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			return blockblob.UploadStreamResponse{}, preconditionError()
		},
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return newGetPropertiesResponse(100, nil), nil
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutFencedObject(context.Background(), "bucket", "key", "ns", "log1")
	assert.Error(t, err)
	assert.True(t, werr.ErrObjectAlreadyExists.Is(err))
}

func TestAzureObjectStorage_PutFencedObject_StatError(t *testing.T) {
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, _ io.Reader, _ *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			return blockblob.UploadStreamResponse{}, preconditionError()
		},
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return blob.GetPropertiesResponse{}, fmt.Errorf("stat error")
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutFencedObject(context.Background(), "bucket", "key", "ns", "log1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stat error")
}

func TestAzureObjectStorage_PutFencedObject_OtherError(t *testing.T) {
	mock := &mockBlobClient{
		uploadStreamFunc: func(_ context.Context, _ io.Reader, _ *azblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error) {
			return blockblob.UploadStreamResponse{}, fmt.Errorf("network error")
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.PutFencedObject(context.Background(), "bucket", "key", "ns", "log1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "network error")
}

func TestAzureObjectStorage_StatObject_Success_NotFenced(t *testing.T) {
	mock := &mockBlobClient{
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return newGetPropertiesResponse(256, map[string]*string{}), nil
		},
	}
	storage := newTestAzureStorage(mock)

	size, isFenced, err := storage.StatObject(context.Background(), "bucket", "key", "ns", "log1")
	require.NoError(t, err)
	assert.Equal(t, int64(256), size)
	assert.False(t, isFenced)
}

func TestAzureObjectStorage_StatObject_Success_Fenced(t *testing.T) {
	fenced := "true"
	mock := &mockBlobClient{
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return newGetPropertiesResponse(1, map[string]*string{minioHandler.FencedObjectMetaKey: &fenced}), nil
		},
	}
	storage := newTestAzureStorage(mock)

	size, isFenced, err := storage.StatObject(context.Background(), "bucket", "key", "ns", "log1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), size)
	assert.True(t, isFenced)
}

func TestAzureObjectStorage_StatObject_Error(t *testing.T) {
	mock := &mockBlobClient{
		getPropertiesFunc: func(_ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return blob.GetPropertiesResponse{}, fmt.Errorf("not found")
		},
	}
	storage := newTestAzureStorage(mock)

	_, _, err := storage.StatObject(context.Background(), "bucket", "key", "ns", "log1")
	assert.Error(t, err)
}

func TestAzureObjectStorage_RemoveObject_Success(t *testing.T) {
	mock := &mockBlobClient{
		deleteFunc: func(_ context.Context, _ *blob.DeleteOptions) (blob.DeleteResponse, error) {
			return blob.DeleteResponse{}, nil
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.RemoveObject(context.Background(), "bucket", "key", "ns", "log1")
	assert.NoError(t, err)
}

func TestAzureObjectStorage_RemoveObject_Error(t *testing.T) {
	mock := &mockBlobClient{
		deleteFunc: func(_ context.Context, _ *blob.DeleteOptions) (blob.DeleteResponse, error) {
			return blob.DeleteResponse{}, fmt.Errorf("delete error")
		},
	}
	storage := newTestAzureStorage(mock)

	err := storage.RemoveObject(context.Background(), "bucket", "key", "ns", "log1")
	assert.Error(t, err)
}

// ============================================================================
// WalkWithObjects tests
// ============================================================================

func TestAzureObjectStorage_WalkWithObjects_Flat_Success(t *testing.T) {
	now := time.Now()
	pager := &mockFlatPager{
		pages: []container.ListBlobsFlatResponse{
			{
				ListBlobsFlatSegmentResponse: container.ListBlobsFlatSegmentResponse{
					Segment: &container.BlobFlatListSegment{
						BlobItems: []*container.BlobItem{
							{Name: ptr("obj1"), Properties: &container.BlobProperties{LastModified: &now, ContentLength: ptr(int64(100))}},
							{Name: ptr("obj2"), Properties: &container.BlobProperties{LastModified: &now, ContentLength: ptr(int64(200))}},
						},
					},
				},
			},
		},
	}

	storage := &AzureObjectStorage{client: &mockServiceClient{flatPager: pager}}

	var collected []string
	err := storage.WalkWithObjects(context.Background(), "bucket", "prefix/", true, func(info *ChunkObjectInfo) bool {
		collected = append(collected, info.FilePath)
		return true
	}, "ns", "log1")

	assert.NoError(t, err)
	assert.Equal(t, []string{"obj1", "obj2"}, collected)
}

func TestAzureObjectStorage_WalkWithObjects_Flat_StopsEarly(t *testing.T) {
	now := time.Now()
	pager := &mockFlatPager{
		pages: []container.ListBlobsFlatResponse{
			{
				ListBlobsFlatSegmentResponse: container.ListBlobsFlatSegmentResponse{
					Segment: &container.BlobFlatListSegment{
						BlobItems: []*container.BlobItem{
							{Name: ptr("obj1"), Properties: &container.BlobProperties{LastModified: &now, ContentLength: ptr(int64(100))}},
							{Name: ptr("obj2"), Properties: &container.BlobProperties{LastModified: &now, ContentLength: ptr(int64(200))}},
						},
					},
				},
			},
		},
	}

	storage := &AzureObjectStorage{client: &mockServiceClient{flatPager: pager}}
	count := 0
	err := storage.WalkWithObjects(context.Background(), "bucket", "prefix/", true, func(info *ChunkObjectInfo) bool {
		count++
		return false // stop after first
	}, "ns", "log1")

	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestAzureObjectStorage_WalkWithObjects_Flat_Error(t *testing.T) {
	pager := &mockFlatPagerWithError{err: fmt.Errorf("pager error")}
	storage := &AzureObjectStorage{client: &mockServiceClient{flatPager: pager}}

	err := storage.WalkWithObjects(context.Background(), "bucket", "prefix/", true, func(info *ChunkObjectInfo) bool {
		return true
	}, "ns", "log1")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pager error")
}

func TestAzureObjectStorage_WalkWithObjects_Hierarchy_Success(t *testing.T) {
	now := time.Now()
	pager := &mockHierPager{
		pages: []container.ListBlobsHierarchyResponse{
			{
				ListBlobsHierarchySegmentResponse: container.ListBlobsHierarchySegmentResponse{
					Segment: &container.BlobHierarchyListSegment{
						BlobItems: []*container.BlobItem{
							{Name: ptr("prefix/file1"), Properties: &container.BlobProperties{LastModified: &now, ContentLength: ptr(int64(50))}},
						},
						BlobPrefixes: []*container.BlobPrefix{
							{Name: ptr("prefix/subdir/")},
						},
					},
				},
			},
		},
	}

	storage := &AzureObjectStorage{client: &mockServiceClient{hierPager: pager}}

	var collected []string
	err := storage.WalkWithObjects(context.Background(), "bucket", "prefix/", false, func(info *ChunkObjectInfo) bool {
		collected = append(collected, info.FilePath)
		return true
	}, "ns", "log1")

	assert.NoError(t, err)
	assert.Equal(t, []string{"prefix/file1", "prefix/subdir/"}, collected)
}

func TestAzureObjectStorage_WalkWithObjects_Hierarchy_StopsOnBlob(t *testing.T) {
	now := time.Now()
	pager := &mockHierPager{
		pages: []container.ListBlobsHierarchyResponse{
			{
				ListBlobsHierarchySegmentResponse: container.ListBlobsHierarchySegmentResponse{
					Segment: &container.BlobHierarchyListSegment{
						BlobItems: []*container.BlobItem{
							{Name: ptr("prefix/file1"), Properties: &container.BlobProperties{LastModified: &now, ContentLength: ptr(int64(50))}},
						},
						BlobPrefixes: []*container.BlobPrefix{
							{Name: ptr("prefix/subdir/")},
						},
					},
				},
			},
		},
	}

	storage := &AzureObjectStorage{client: &mockServiceClient{hierPager: pager}}
	count := 0
	err := storage.WalkWithObjects(context.Background(), "bucket", "prefix/", false, func(info *ChunkObjectInfo) bool {
		count++
		return false // stop on first blob
	}, "ns", "log1")

	assert.NoError(t, err)
	assert.Equal(t, 1, count) // stopped before prefix
}

func TestAzureObjectStorage_WalkWithObjects_Hierarchy_StopsOnPrefix(t *testing.T) {
	pager := &mockHierPager{
		pages: []container.ListBlobsHierarchyResponse{
			{
				ListBlobsHierarchySegmentResponse: container.ListBlobsHierarchySegmentResponse{
					Segment: &container.BlobHierarchyListSegment{
						BlobItems:    []*container.BlobItem{},
						BlobPrefixes: []*container.BlobPrefix{{Name: ptr("prefix/a/")}, {Name: ptr("prefix/b/")}},
					},
				},
			},
		},
	}

	storage := &AzureObjectStorage{client: &mockServiceClient{hierPager: pager}}
	count := 0
	err := storage.WalkWithObjects(context.Background(), "bucket", "prefix/", false, func(info *ChunkObjectInfo) bool {
		count++
		return false // stop on first prefix
	}, "ns", "log1")

	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestAzureObjectStorage_WalkWithObjects_Hierarchy_Error(t *testing.T) {
	pager := &mockHierPagerWithError{err: fmt.Errorf("hier error")}
	storage := &AzureObjectStorage{client: &mockServiceClient{hierPager: pager}}

	err := storage.WalkWithObjects(context.Background(), "bucket", "prefix/", false, func(info *ChunkObjectInfo) bool {
		return true
	}, "ns", "log1")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hier error")
}

func TestAzureObjectStorage_WalkWithObjects_Flat_Empty(t *testing.T) {
	pager := &mockFlatPager{pages: []container.ListBlobsFlatResponse{}}
	storage := &AzureObjectStorage{client: &mockServiceClient{flatPager: pager}}

	count := 0
	err := storage.WalkWithObjects(context.Background(), "bucket", "prefix/", true, func(info *ChunkObjectInfo) bool {
		count++
		return true
	}, "ns", "log1")

	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

// ============================================================================
// azureServiceClientWrapper tests
// ============================================================================

func TestAzureServiceClientWrapper_GetBlobClient(t *testing.T) {
	connStr := "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net"
	svcClient, err := service.NewClientFromConnectionString(connStr, &service.ClientOptions{})
	require.NoError(t, err)

	wrapper := &azureServiceClientWrapper{client: svcClient}
	blobClient := wrapper.GetBlobClient("test-container", "test-blob")
	assert.NotNil(t, blobClient)
}

func TestAzureServiceClientWrapper_NewListBlobsFlatPager(t *testing.T) {
	connStr := "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net"
	svcClient, err := service.NewClientFromConnectionString(connStr, &service.ClientOptions{})
	require.NoError(t, err)

	wrapper := &azureServiceClientWrapper{client: svcClient}
	prefix := "prefix/"
	pager := wrapper.NewListBlobsFlatPager("test-container", &azblob.ListBlobsFlatOptions{Prefix: &prefix})
	assert.NotNil(t, pager)
}

func TestAzureServiceClientWrapper_NewListBlobsHierarchyPager(t *testing.T) {
	connStr := "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net"
	svcClient, err := service.NewClientFromConnectionString(connStr, &service.ClientOptions{})
	require.NoError(t, err)

	wrapper := &azureServiceClientWrapper{client: svcClient}
	prefix := "prefix/"
	pager := wrapper.NewListBlobsHierarchyPager("test-container", "/", &container.ListBlobsHierarchyOptions{Prefix: &prefix})
	assert.NotNil(t, pager)
}

// ============================================================================
// newAzureObjectStorageClient tests
// ============================================================================

func TestNewAzureObjectStorageClient_EmptyBucketName(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testaccount"
	cfg.Minio.SecretAccessKey = "dGVzdGtleQ=="
	cfg.Minio.Address = "core.windows.net"
	cfg.Minio.BucketName = ""

	_, err := newAzureObjectStorageClient(context.Background(), cfg)
	require.Error(t, err)
	assert.True(t, werr.ErrInvalidConfiguration.Is(err))
	assert.Contains(t, err.Error(), "invalid empty bucket name")
}

func TestNewAzureObjectStorageClient_NonIAM_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testaccount"
	cfg.Minio.SecretAccessKey = "dGVzdGtleQ=="
	cfg.Minio.Address = "core.windows.net"
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newAzureObjectStorageClient(ctx, cfg)
	assert.Error(t, err)
}

func TestNewAzureObjectStorageClient_NonIAM_FromEnvConnectionString(t *testing.T) {
	// Set connection string via env
	connStr := "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net"
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", connStr)

	cfg, _ := config.NewConfiguration()
	cfg.Minio.UseIAM = false
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newAzureObjectStorageClient(ctx, cfg)
	assert.Error(t, err) // cancelled ctx → bucket check fails
}

func TestNewAzureObjectStorageClient_IAM_InvalidCredentials(t *testing.T) {
	// Clear Azure env vars to make IAM credential creation fail
	t.Setenv("AZURE_CLIENT_ID", "")
	t.Setenv("AZURE_TENANT_ID", "")
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", "")

	cfg, _ := config.NewConfiguration()
	cfg.Minio.UseIAM = true
	cfg.Minio.AccessKeyID = "testaccount"
	cfg.Minio.Address = "core.windows.net"
	cfg.Minio.BucketName = "test-bucket"

	_, err := newAzureObjectStorageClient(context.Background(), cfg)
	// NewWorkloadIdentityCredential fails with empty token file path
	assert.Error(t, err)
}

func TestNewAzureObjectStorageWithConfig_EmptyBucketName(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testaccount"
	cfg.Minio.SecretAccessKey = "dGVzdGtleQ=="
	cfg.Minio.Address = "core.windows.net"
	cfg.Minio.BucketName = ""

	_, err := newAzureObjectStorageWithConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.True(t, werr.ErrInvalidConfiguration.Is(err))
}

func TestNewAzureObjectStorageWithConfig_NonIAM_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testaccount"
	cfg.Minio.SecretAccessKey = "dGVzdGtleQ=="
	cfg.Minio.Address = "core.windows.net"
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newAzureObjectStorageWithConfig(ctx, cfg)
	assert.Error(t, err)
}
