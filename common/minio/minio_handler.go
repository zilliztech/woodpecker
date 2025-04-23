package minio

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7"

	"github.com/zilliztech/woodpecker/common/config"
)

//go:generate mockery --dir=./common/minio --name=MinioHandler --structname=MinioHandler --output=mocks/mocks_minio --filename=mock_minio_handler.go --with-expecter=true  --outpkg=mocks_minio
type MinioHandler interface {
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	GetObjectDataAndInfo(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.Reader, int64, error)
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error
	StatObject(ctx context.Context, bucketName, prefix string, opts minio.GetObjectOptions) (minio.ObjectInfo, error)
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
	return m.client.GetObject(ctx, bucketName, objectName, opts)
}
func (m *minioHandlerImpl) GetObjectDataAndInfo(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.Reader, int64, error) {
	obj, err := m.client.GetObject(ctx, bucketName, objectName, opts)
	if err != nil {
		return nil, -1, err
	}
	info, err := obj.Stat()
	if err != nil {
		return nil, -1, err
	}
	return obj, info.LastModified.UnixMilli(), err
}

func (m *minioHandlerImpl) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	return m.client.PutObject(ctx, bucketName, objectName, reader, objectSize, opts)
}

func (m *minioHandlerImpl) RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
	return m.client.RemoveObject(ctx, bucketName, objectName, opts)
}

func (m *minioHandlerImpl) StatObject(ctx context.Context, bucketName, prefix string, opts minio.GetObjectOptions) (minio.ObjectInfo, error) {
	return m.client.StatObject(ctx, bucketName, prefix, opts)
}

func (m *minioHandlerImpl) ListObjects(ctx context.Context, bucketName, prefix string, recursive bool, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	opts.Recursive = recursive
	opts.Prefix = prefix
	return m.client.ListObjects(ctx, bucketName, opts)
}
