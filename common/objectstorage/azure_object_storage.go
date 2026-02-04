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
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/common/werr"
)

var CheckBucketRetryAttempts uint = 20

var _ minioHandler.FileReader = (*BlobReader)(nil)

// BlobReader is implemented because Azure's stream body does not have ReadAt and Seek interfaces.
// BlobReader is not concurrency safe.
type BlobReader struct {
	client          *blockblob.Client
	position        int64
	count           int64
	body            io.ReadCloser
	contentLength   int64
	needResetStream bool
}

// NewBlobReader construct a full blob reader
// Deprecated
func NewBlobReader(client *blockblob.Client, offset int64) (*BlobReader, error) {
	return &BlobReader{client: client, position: offset, count: 0, needResetStream: true}, nil
}

func NewBlobReaderWithSize(client *blockblob.Client, offset int64, size int64) (*BlobReader, error) {
	return &BlobReader{client: client, position: offset, count: size, needResetStream: true}, nil
}

func (b *BlobReader) Read(p []byte) (n int, err error) {
	ctx := context.Background()

	if b.needResetStream {
		opts := &azblob.DownloadStreamOptions{
			Range: blob.HTTPRange{
				Offset: b.position,
				Count:  b.count,
			},
		}
		object, err := b.client.DownloadStream(ctx, opts)
		if err != nil {
			return 0, err
		}
		b.body = object.Body
		b.contentLength = *object.ContentLength
	}

	n, err = b.body.Read(p)
	if err != nil {
		return n, err
	}
	b.position += int64(n)
	b.needResetStream = false
	return n, nil
}

func (b *BlobReader) Close() error {
	if b.body != nil {
		return b.body.Close()
	}
	return nil
}

func (b *BlobReader) ReadAt(p []byte, off int64) (n int, err error) {
	httpRange := blob.HTTPRange{
		Offset: off,
		Count:  int64(len(p)),
	}
	object, err := b.client.DownloadStream(context.Background(), &blob.DownloadStreamOptions{
		Range: httpRange,
	})
	if err != nil {
		return 0, err
	}
	defer object.Body.Close()
	return io.ReadFull(object.Body, p)
}

func (b *BlobReader) Seek(offset int64, whence int) (int64, error) {
	props, err := b.client.GetProperties(context.Background(), &blob.GetPropertiesOptions{})
	if err != nil {
		return 0, err
	}
	size := *props.ContentLength
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = b.position + offset
	case io.SeekEnd:
		newOffset = size + offset
	default:
		return 0, werr.ErrFileReaderSeek.WithCauseErrMsg("invalid whence")
	}

	b.position = newOffset
	b.needResetStream = true
	return newOffset, nil
}

func (b *BlobReader) Size() (int64, error) {
	return b.contentLength, nil
}

var _ ObjectStorage = (*AzureObjectStorage)(nil)

type AzureObjectStorage struct {
	*service.Client
}

func newAzureObjectStorageWithConfig(ctx context.Context, c *config.Configuration) (*AzureObjectStorage, error) {
	client, err := newAzureObjectStorageClient(ctx, c)
	if err != nil {
		return nil, err
	}
	return &AzureObjectStorage{Client: client}, nil
}

func newAzureObjectStorageClient(ctx context.Context, c *config.Configuration) (*service.Client, error) {
	var client *service.Client
	var err error
	if c.Minio.UseIAM {
		cred, credErr := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
			ClientID:      os.Getenv("AZURE_CLIENT_ID"),
			TenantID:      os.Getenv("AZURE_TENANT_ID"),
			TokenFilePath: os.Getenv("AZURE_FEDERATED_TOKEN_FILE"),
		})
		if credErr != nil {
			return nil, credErr
		}
		client, err = service.NewClient("https://"+c.Minio.AccessKeyID+".blob."+c.Minio.Address+"/", cred, &service.ClientOptions{})
	} else {
		connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
		if connectionString == "" {
			connectionString = "DefaultEndpointsProtocol=https;AccountName=" + c.Minio.AccessKeyID +
				";AccountKey=" + c.Minio.SecretAccessKey + ";EndpointSuffix=" + c.Minio.Address
		}
		client, err = service.NewClientFromConnectionString(connectionString, &service.ClientOptions{})
	}
	if err != nil {
		return nil, err
	}
	if c.Minio.BucketName == "" {
		return nil, werr.ErrInvalidConfiguration.WithCauseErrMsg("invalid empty bucket name")
	}
	// check valid in first query
	checkBucketFn := func() error {
		_, err := client.NewContainerClient(c.Minio.BucketName).GetProperties(ctx, &container.GetPropertiesOptions{})
		if err != nil {
			switch err := err.(type) {
			case *azcore.ResponseError:
				if c.Minio.CreateBucket && err.ErrorCode == string(bloberror.ContainerNotFound) {
					_, createErr := client.NewContainerClient(c.Minio.BucketName).Create(ctx, &azblob.CreateContainerOptions{})
					if createErr != nil {
						return createErr
					}
					return nil
				}
			}
		}
		return err
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (a *AzureObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64, operatingNamespace string, operatingLogId string) (minioHandler.FileReader, error) {
	return NewBlobReaderWithSize(a.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName), offset, size)
}

func (a *AzureObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error {
	_, err := a.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).UploadStream(ctx, reader, &azblob.UploadStreamOptions{})
	return err
}

func (a *AzureObjectStorage) PutObjectIfNoneMatch(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, operatingNamespace string, operatingLogId string) error {
	start := time.Now()
	eTagAny := azcore.ETag("*")
	_, err := a.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).UploadStream(ctx, reader, &azblob.UploadStreamOptions{
		AccessConditions: &azblob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfNoneMatch: &eTagAny,
			},
		},
	})

	if err != nil && a.IsPreconditionFailedError(err) {
		objSize, isFencedObject, stateErr := a.StatObject(ctx, bucketName, objectName, operatingNamespace, operatingLogId)
		if stateErr != nil {
			// return normal err, let task retry
			return stateErr
		}
		if isFencedObject {
			logger.Ctx(ctx).Info("object already exists and it is a fence object", zap.String("objectName", objectName))
			return werr.ErrSegmentFenced.WithCauseErrMsg("already fenced")
		}
		// means it is a normal object already uploaded before this retry, idempotent flush success
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(operatingNamespace, operatingLogId, "condition_put_object", "success").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(operatingNamespace, operatingLogId, "condition_put_object", "success").Observe(float64(time.Since(start).Milliseconds()))
		metrics.WpObjectStorageBytesTransferred.WithLabelValues(operatingNamespace, operatingLogId, "condition_put_object").Add(float64(objSize))
		logger.Ctx(ctx).Info("object already exists, idempotent flush success", zap.String("objectKey", objectName))
		return werr.ErrObjectAlreadyExists
	}
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(operatingNamespace, operatingLogId, "condition_put_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(operatingNamespace, operatingLogId, "condition_put_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}

	return err
}

func (a *AzureObjectStorage) PutFencedObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error {
	start := time.Now()
	fencedObjectReader := bytes.NewReader([]byte("F"))
	eTagAny := azcore.ETag("*")
	isFencedObject := "true"
	_, err := a.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).UploadStream(ctx, fencedObjectReader, &azblob.UploadStreamOptions{
		AccessConditions: &azblob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfNoneMatch: &eTagAny,
			},
		},
		Metadata: map[string]*string{
			minioHandler.FencedObjectMetaKey: &isFencedObject,
		},
	})
	if err != nil && a.IsPreconditionFailedError(err) {
		_, isFenced, stateErr := a.StatObject(ctx, bucketName, objectName, operatingNamespace, operatingLogId)
		if stateErr != nil {
			// return normal err
			return stateErr
		}
		if isFenced {
			// already fenced, return success
			logger.Ctx(ctx).Info("found fenced object exists, skip", zap.String("objectName", objectName))
			return nil
		}
		// return normal err
		return werr.ErrObjectAlreadyExists
	}
	if err != nil {
		metrics.WpObjectStorageOperationsTotal.WithLabelValues(operatingNamespace, operatingLogId, "put_fenced_object", "error").Inc()
		metrics.WpObjectStorageOperationLatency.WithLabelValues(operatingNamespace, operatingLogId, "put_fenced_object", "error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}

	return err
}

func (a *AzureObjectStorage) StatObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) (int64, bool, error) {
	info, err := a.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).GetProperties(ctx, &blob.GetPropertiesOptions{})
	if err != nil {
		return 0, false, err
	}
	isFencedObject := info.Metadata[minioHandler.FencedObjectMetaKey]
	if isFencedObject != nil && len(*isFencedObject) > 0 && *isFencedObject == "true" {
		return *info.ContentLength, true, nil
	}
	return *info.ContentLength, false, nil
}

func (a *AzureObjectStorage) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc, operatingNamespace string, operatingLogId string) error {
	if recursive {
		pager := a.Client.NewContainerClient(bucketName).NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
			Prefix: &prefix,
		})
		for pager.More() {
			pageResp, err := pager.NextPage(ctx)
			if err != nil {
				return err
			}
			for _, blobItem := range pageResp.Segment.BlobItems {
				if !walkFunc(&ChunkObjectInfo{FilePath: *blobItem.Name, ModifyTime: *blobItem.Properties.LastModified, Size: *blobItem.Properties.ContentLength}) {
					return nil
				}
			}
		}
	} else {
		pager := a.Client.NewContainerClient(bucketName).NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
			Prefix: &prefix,
		})
		for pager.More() {
			pageResp, err := pager.NextPage(ctx)
			if err != nil {
				return err
			}

			for _, blobItem := range pageResp.Segment.BlobItems {
				if !walkFunc(&ChunkObjectInfo{FilePath: *blobItem.Name, ModifyTime: *blobItem.Properties.LastModified, Size: *blobItem.Properties.ContentLength}) {
					return nil
				}
			}
			for _, blobPrefix := range pageResp.Segment.BlobPrefixes {
				if !walkFunc(&ChunkObjectInfo{FilePath: *blobPrefix.Name, ModifyTime: time.Now()}) {
					return nil
				}
			}
		}
	}
	return nil
}

func (a *AzureObjectStorage) RemoveObject(ctx context.Context, bucketName, objectName string, operatingNamespace string, operatingLogId string) error {
	_, err := a.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).Delete(ctx, &blob.DeleteOptions{})
	return err
}

// IsObjectNotExistsError check if the error is object not exists
// error code:https://learn.microsoft.com/en-us/azure/storage/common/storage-ref-azcopy-error-codes
func (a *AzureObjectStorage) IsObjectNotExistsError(err error) bool {
	var azError *azcore.ResponseError
	if err != nil && errors.As(err, &azError) {
		return azError.StatusCode == http.StatusNotFound
	}
	return false
}

// IsPreconditionFailedError check if the error is precondition failed
// error code:https://learn.microsoft.com/en-us/azure/storage/common/storage-ref-azcopy-error-codes
func (a *AzureObjectStorage) IsPreconditionFailedError(err error) bool {
	var azError *azcore.ResponseError
	if err != nil && errors.As(err, &azError) {
		return azError.StatusCode == http.StatusPreconditionFailed || azError.StatusCode == http.StatusConflict
	}
	return false
}
