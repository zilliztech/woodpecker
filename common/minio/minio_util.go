package minio

import (
	"context"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func NewMinioClient(ctx context.Context, baseBucket string) (*minio.Client, error) {
	var creds *credentials.Credentials
	creds = credentials.NewStaticV4("minioadmin", "minioadmin", "")
	minioClient, err := minio.New("localhost:9000", &minio.Options{
		Creds:  creds,
		Secure: false,
	})
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}

	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minioClient.BucketExists(ctx, baseBucket)
		if err != nil {
			return err
		}
		if !bucketExists {
			err := minioClient.MakeBucket(ctx, baseBucket, minio.MakeBucketOptions{})
			if err != nil {
				return err
			}
		}
		return nil
	}
	// check and create root bucket if not exists
	err = checkBucketFn()
	if err != nil {
		return nil, err
	}
	return minioClient, nil
}
