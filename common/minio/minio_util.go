package minio

import (
	"context"
	"fmt"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/common/werr"
)

const (
	CheckBucketRetryAttempts = 20
)

func NewMinioClientFromConfig(ctx context.Context, cfg *config.Configuration) (*minio.Client, error) {
	var creds *credentials.Credentials
	if cfg.Minio.UseIAM {
		creds = credentials.NewIAM(cfg.Minio.IamEndpoint)
	} else {
		creds = credentials.NewStaticV4(cfg.Minio.AccessKeyID, cfg.Minio.SecretAccessKey, "")
	}

	if cfg.Minio.UseSSL && len(cfg.Minio.Ssl.TlsCACert) > 0 {
		err := os.Setenv("SSL_CERT_FILE", cfg.Minio.Ssl.TlsCACert)
		if err != nil {
			return nil, err
		}
	}

	minioClient, err := minio.New(cfg.Minio.Address, &minio.Options{
		Creds:  creds,
		Secure: cfg.Minio.UseSSL,
	})
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}

	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minioClient.BucketExists(ctx, cfg.Minio.BucketName)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to check blob bucket exist", zap.String("bucket", cfg.Minio.BucketName), zap.Error(err))
			return err
		}
		if !bucketExists {
			if cfg.Minio.CreateBucket {
				logger.Ctx(ctx).Info("blob bucket not exist, create bucket.", zap.String("bucket name", cfg.Minio.BucketName))
				err := minioClient.MakeBucket(ctx, cfg.Minio.BucketName, minio.MakeBucketOptions{})
				if err != nil {
					logger.Ctx(ctx).Warn("failed to create blob bucket", zap.String("bucket", cfg.Minio.BucketName), zap.Error(err))
					return err
				}
			} else {
				return werr.ErrConfigError.WithCauseErrMsg(fmt.Sprintf("bucket %s not Existed", cfg.Minio.BucketName))
			}
		}
		return nil
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}
	return minioClient, nil
}

//
//// Deprecated: TestOnly TODO
//func NewMinioClient(ctx context.Context, baseBucket string) (*minio.Client, error) {
//	var creds *credentials.Credentials
//	creds = credentials.NewStaticV4("minioadmin", "minioadmin", "")
//	minioClient, err := minio.New("localhost:9000", &minio.Options{
//		Creds:  creds,
//		Secure: false,
//	})
//	// options nil or invalid formatted endpoint, don't need to retry
//	if err != nil {
//		return nil, err
//	}
//
//	var bucketExists bool
//	// check valid in first query
//	checkBucketFn := func() error {
//		bucketExists, err = minioClient.BucketExists(ctx, baseBucket)
//		if err != nil {
//			return err
//		}
//		if !bucketExists {
//			err := minioClient.MakeBucket(ctx, baseBucket, minio.MakeBucketOptions{})
//			if err != nil {
//				return err
//			}
//		}
//		return nil
//	}
//	// check and create root bucket if not exists
//	err = checkBucketFn()
//	if err != nil {
//		return nil, err
//	}
//	return minioClient, nil
//}
//
//// Deprecated: TestOnly TODO
//func NewMinioClientWithSSL(ctx context.Context, baseBucket string) (*minio.Client, error) {
//	var creds *credentials.Credentials
//	creds = credentials.NewIAM("")
//
//	err := os.Setenv("SSL_CERT_FILE", "/path/to/public.crt")
//	if err != nil {
//		return nil, err
//	}
//
//	minioClient, err := minio.New("s3.us-west-2.amazonaws.com", &minio.Options{
//		Creds:  creds,
//		Secure: true,
//	})
//	// options nil or invalid formatted endpoint, don't need to retry
//	if err != nil {
//		return nil, err
//	}
//	return minioClient, nil
//}
