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

	minioClient, err := minio.New(
		fmt.Sprintf("%s:%d", cfg.Minio.Address, cfg.Minio.Port),
		&minio.Options{
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
