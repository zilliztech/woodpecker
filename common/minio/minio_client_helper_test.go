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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
)

// === newMinioClient cloud provider tests ===

func TestNewMinioClient_Azure_ReturnsError(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderAzure
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000

	_, err := newMinioClient(context.Background(), cfg)
	require.Error(t, err)
	assert.True(t, werr.ErrConfigError.Is(err))
	assert.Contains(t, err.Error(), "Azure")
}

func TestNewMinioClient_GCPNative_ReturnsError(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderGCPNative
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000

	_, err := newMinioClient(context.Background(), cfg)
	require.Error(t, err)
	assert.True(t, werr.ErrConfigError.Is(err))
	assert.Contains(t, err.Error(), "gcp native")
}

func TestNewMinioClient_InvalidEndpoint(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderAWS
	cfg.Minio.Address = ""
	cfg.Minio.Port = 0

	_, err := newMinioClient(context.Background(), cfg)
	assert.Error(t, err)
}

func TestNewMinioClient_Aliyun_StaticCreds_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderAliyun
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err)
}

func TestNewMinioClient_GCP_StaticCreds_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderGCP
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "storage.googleapis.com"
	cfg.Minio.Port = 443
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err)
}

func TestNewMinioClient_Tencent_StaticCreds_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderTencent
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err)
}

func TestNewMinioClient_AWS_StaticCreds_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderAWS
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err)
}

func TestNewMinioClient_AWS_IAM_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderAWS
	cfg.Minio.UseIAM = true
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err)
}

func TestNewMinioClient_UseVirtualHost(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderAWS
	cfg.Minio.UseVirtualHost = true
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err) // cancelled ctx → bucket check fails
}

// === Address-based cloud provider inference tests ===

func TestNewMinioClient_InferGCP_FromAddress_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = "" // default, triggers address inference
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "storage.googleapis.com"
	cfg.Minio.Port = 443
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err)
}

func TestNewMinioClient_InferAliyun_FromAddress_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = "" // default, triggers address inference
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "oss.aliyuncs.com"
	cfg.Minio.Port = 443
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err)
}

func TestNewMinioClient_InferAliyun_FromAddress_IAM_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = "" // default, triggers address inference
	cfg.Minio.UseIAM = true
	cfg.Minio.Address = "oss.aliyuncs.com"
	cfg.Minio.Port = 443
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err)
}

func TestNewMinioClient_DefaultProvider_StaticCreds_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = "" // default → matchedDefault stays true
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "my-custom-s3.example.com"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err)
}

func TestNewMinioClient_DefaultProvider_IAM_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = "" // default
	cfg.Minio.UseIAM = true
	cfg.Minio.Address = "my-custom-s3.example.com"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err)
}

// === SSL cert env variable test ===

func TestNewMinioClient_SSL_SetsCertEnv(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderAzure // use Azure to get early error return after env is set
	cfg.Minio.UseSSL = true
	cfg.Minio.Ssl.TlsCACert = "/tmp/test-cert.pem"
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000

	// Clear any existing value
	t.Setenv("SSL_CERT_FILE", "")

	_, err := newMinioClient(context.Background(), cfg)
	// Azure returns config error, but the env should NOT be set because Azure returns before SSL setup
	require.Error(t, err)
}

func TestNewMinioClient_SSL_SetsCertEnv_AWS(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderAWS
	cfg.Minio.UseSSL = true
	cfg.Minio.Ssl.TlsCACert = "/tmp/test-cert-aws.pem"
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	t.Setenv("SSL_CERT_FILE", "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err) // cancelled ctx

	// Verify SSL_CERT_FILE was set
	assert.Equal(t, "/tmp/test-cert-aws.pem", os.Getenv("SSL_CERT_FILE"))
}

func TestNewMinioClient_SSL_NoCert(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.CloudProvider = CloudProviderAWS
	cfg.Minio.UseSSL = true
	cfg.Minio.Ssl.TlsCACert = "" // empty cert path
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	t.Setenv("SSL_CERT_FILE", "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClient(ctx, cfg)
	assert.Error(t, err) // cancelled ctx

	// SSL_CERT_FILE should not be set when TlsCACert is empty
	assert.Empty(t, os.Getenv("SSL_CERT_FILE"))
}

// === newMinioClientFromConfig tests ===

func TestNewMinioClientFromConfig_InvalidEndpoint(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.Address = ""
	cfg.Minio.Port = 0

	_, err := newMinioClientFromConfig(context.Background(), cfg)
	assert.Error(t, err)
}

func TestNewMinioClientFromConfig_StaticCreds_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClientFromConfig(ctx, cfg)
	assert.Error(t, err)
}

func TestNewMinioClientFromConfig_IAM_CancelledCtx(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.UseIAM = true
	cfg.Minio.IamEndpoint = "http://169.254.169.254"
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClientFromConfig(ctx, cfg)
	assert.Error(t, err)
}

func TestNewMinioClientFromConfig_SSL_SetsCertEnv(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Minio.UseSSL = true
	cfg.Minio.Ssl.TlsCACert = "/tmp/test-cert-from-config.pem"
	cfg.Minio.UseIAM = false
	cfg.Minio.AccessKeyID = "testkey"
	cfg.Minio.SecretAccessKey = "testsecret"
	cfg.Minio.Address = "localhost"
	cfg.Minio.Port = 9000
	cfg.Minio.BucketName = "test-bucket"

	t.Setenv("SSL_CERT_FILE", "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newMinioClientFromConfig(ctx, cfg)
	assert.Error(t, err) // cancelled ctx

	assert.Equal(t, "/tmp/test-cert-from-config.pem", os.Getenv("SSL_CERT_FILE"))
}

// === Constants tests ===

func TestCloudProviderConstants(t *testing.T) {
	assert.Equal(t, "gcp", CloudProviderGCP)
	assert.Equal(t, "gcpnative", CloudProviderGCPNative)
	assert.Equal(t, "aws", CloudProviderAWS)
	assert.Equal(t, "aliyun", CloudProviderAliyun)
	assert.Equal(t, "azure", CloudProviderAzure)
	assert.Equal(t, "tencent", CloudProviderTencent)
	assert.Equal(t, 20, CheckBucketRetryAttempts)
}
