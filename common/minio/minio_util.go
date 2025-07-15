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
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/minio/aliyun"
	"github.com/zilliztech/woodpecker/common/minio/gcp"
	"github.com/zilliztech/woodpecker/common/minio/tencent"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/common/werr"
)

const (
	CheckBucketRetryAttempts = 20
	CloudProviderGCP         = "gcp"
	CloudProviderGCPNative   = "gcpnative"
	CloudProviderAWS         = "aws"
	CloudProviderAliyun      = "aliyun"
	CloudProviderAzure       = "azure"
	CloudProviderTencent     = "tencent"
)

func newMinioClient(ctx context.Context, cfg *config.Configuration) (*minio.Client, error) {
	var creds *credentials.Credentials
	newMinioFn := minio.New
	bucketLookupType := minio.BucketLookupAuto

	if cfg.Minio.UseVirtualHost {
		bucketLookupType = minio.BucketLookupDNS
	}

	matchedDefault := false
	switch cfg.Minio.CloudProvider {
	case CloudProviderAliyun:
		// auto doesn't work for aliyun, so we set to dns deliberately
		bucketLookupType = minio.BucketLookupDNS
		if cfg.Minio.UseIAM {
			newMinioFn = aliyun.NewMinioClient
		} else {
			creds = credentials.NewStaticV4(cfg.Minio.AccessKeyID, cfg.Minio.SecretAccessKey, "")
		}
	case CloudProviderGCP:
		newMinioFn = gcp.NewMinioClient
		if !cfg.Minio.UseIAM {
			creds = credentials.NewStaticV2(cfg.Minio.AccessKeyID, cfg.Minio.SecretAccessKey, "")
		}
	case CloudProviderTencent:
		bucketLookupType = minio.BucketLookupDNS
		newMinioFn = tencent.NewMinioClient
		if !cfg.Minio.UseIAM {
			creds = credentials.NewStaticV4(cfg.Minio.AccessKeyID, cfg.Minio.SecretAccessKey, "")
		}
	case CloudProviderAzure:
		return nil, werr.ErrConfigError.WithCauseErrMsg("Woodpecker does not support the Azure cloud provider yet.")
	case CloudProviderGCPNative:
		return nil, werr.ErrConfigError.WithCauseErrMsg("Woodpecker does not support the gcp native cloud provider yet.")
	default:
		// aws, minio
		matchedDefault = true
	}

	// Compatibility logic. If the cloud provider is not specified in the request,
	// it shall be inferred based on the service address.
	if matchedDefault {
		matchedDefault = false
		switch {
		case strings.Contains(cfg.Minio.Address, gcp.GcsDefaultAddress):
			newMinioFn = gcp.NewMinioClient
			if !cfg.Minio.UseIAM {
				creds = credentials.NewStaticV2(cfg.Minio.AccessKeyID, cfg.Minio.SecretAccessKey, "")
			}
		case strings.Contains(cfg.Minio.Address, aliyun.OSSAddressFeatureString):
			// auto doesn't work for aliyun, so we set to dns deliberately
			bucketLookupType = minio.BucketLookupDNS
			if cfg.Minio.UseIAM {
				newMinioFn = aliyun.NewMinioClient
			} else {
				creds = credentials.NewStaticV4(cfg.Minio.AccessKeyID, cfg.Minio.SecretAccessKey, "")
			}
		default:
			matchedDefault = true
		}
	}

	if matchedDefault {
		// aws, minio
		if cfg.Minio.UseIAM {
			creds = credentials.NewIAM("")
		} else {
			creds = credentials.NewStaticV4(cfg.Minio.AccessKeyID, cfg.Minio.SecretAccessKey, "")
		}
	}

	// We must set the cert path by os environment variable "SSL_CERT_FILE",
	// because the minio.DefaultTransport() need this path to read the file content,
	// we shouldn't read this file by ourself.
	if cfg.Minio.UseSSL && len(cfg.Minio.Ssl.TlsCACert) > 0 {
		err := os.Setenv("SSL_CERT_FILE", cfg.Minio.Ssl.TlsCACert)
		if err != nil {
			return nil, err
		}
	}

	minioOpts := &minio.Options{
		BucketLookup: bucketLookupType,
		Creds:        creds,
		Secure:       cfg.Minio.UseSSL,
		Region:       cfg.Minio.Region,
	}
	minIOClient, err := newMinioFn(fmt.Sprintf("%s:%d", cfg.Minio.Address, cfg.Minio.Port), minioOpts)
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}
	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minIOClient.BucketExists(ctx, cfg.Minio.BucketName)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to check blob bucket exist", zap.String("bucket", cfg.Minio.BucketName), zap.Error(err))
			return err
		}
		if !bucketExists {
			if cfg.Minio.CreateBucket {
				logger.Ctx(ctx).Info("blob bucket not exist, create bucket.", zap.String("bucket name", cfg.Minio.BucketName))
				err := minIOClient.MakeBucket(ctx, cfg.Minio.BucketName, minio.MakeBucketOptions{})
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
	return minIOClient, nil
}

// Deprecated
func newMinioClientFromConfig(ctx context.Context, cfg *config.Configuration) (*minio.Client, error) {
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

func IsPreconditionFailed(err error) bool {
	return minio.ToErrorResponse(err).Code == "PreconditionFailed"
}

func IsObjectNotExists(err error) bool {
	return minio.ToErrorResponse(err).Code == "NoSuchKey"
}

func IsFencedObject(objInfo minio.ObjectInfo) bool {
	isFencedObject := objInfo.UserMetadata[FencedObjectMetaKey]
	if len(isFencedObject) > 0 && isFencedObject == "true" {
		return true
	}
	return false
}
