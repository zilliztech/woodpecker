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

package aliyun

import (
	"net/http"
	"strings"

	"github.com/aliyun/credentials-go/credentials" // >= v1.2.6
	"github.com/cockroachdb/errors"
	"github.com/labstack/gommon/log"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/signer"

	"github.com/zilliztech/woodpecker/common/minio/utils"
)

const (
	OSSAddressFeatureString = "aliyuncs.com"
	OSSDefaultAddress       = "oss.aliyuncs.com"
)

// NewMinioClient returns a minio.Client which is compatible for aliyun OSS
func NewMinioClient(address string, opts *minio.Options) (*minio.Client, error) {
	if opts == nil {
		opts = &minio.Options{}
	}
	if address == "" {
		address = OSSDefaultAddress
		opts.Secure = true
	}

	// Set up credentials if not provided
	if opts.Creds == nil {
		credProvider, err := NewCredentialProvider()
		if err != nil {
			return nil, errors.Wrap(err, "failed to create credential provider")
		}
		opts.Creds = minioCred.New(credProvider)
	}

	// Set up custom transport for header transformations if not already provided
	if opts.Transport == nil {
		transportWrap, err := NewWrapHTTPTransport(opts.Secure, opts.Region, opts.Creds)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create wrap transport")
		}
		opts.Transport = transportWrap
	}

	return minio.New(address, opts)
}

// Credential is defined to mock aliyun credential.Credentials
//
//go:generate mockery --name=Credential --with-expecter
type Credential interface {
	credentials.Credential
}

// CredentialProvider implements "github.com/minio/minio-go/v7/pkg/credentials".Provider
// also implements transport
type CredentialProvider struct {
	// aliyunCreds doesn't provide a way to get the expire time, so we use the cache to check if it's expired
	// when aliyunCreds.GetAccessKeyId is different from the cache, we know it's expired
	akCache     string
	aliyunCreds Credential
}

func NewCredentialProvider() (minioCred.Provider, error) {
	aliyunCreds, err := credentials.NewCredential(nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create aliyun credential")
	}
	// backend, err := minio.DefaultTransport(true)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "failed to create default transport")
	// }
	return &CredentialProvider{aliyunCreds: aliyunCreds}, nil
}

// Retrieve returns nil if it successfully retrieved the value.
// Error is returned if the value were not obtainable, or empty.
// according to the caller minioCred.Credentials.Get(),
// it already has a lock, so we don't need to worry about concurrency
func (c *CredentialProvider) Retrieve() (minioCred.Value, error) {
	ret := minioCred.Value{}
	ak, err := c.aliyunCreds.GetAccessKeyId()
	if err != nil {
		return ret, errors.Wrap(err, "failed to get access key id from aliyun credential")
	}
	ret.AccessKeyID = *ak
	sk, err := c.aliyunCreds.GetAccessKeySecret()
	if err != nil {
		return minioCred.Value{}, errors.Wrap(err, "failed to get access key secret from aliyun credential")
	}
	securityToken, err := c.aliyunCreds.GetSecurityToken()
	if err != nil {
		return minioCred.Value{}, errors.Wrap(err, "failed to get security token from aliyun credential")
	}
	ret.SecretAccessKey = *sk
	c.akCache = *ak
	ret.SessionToken = *securityToken
	return ret, nil
}

// IsExpired returns if the credentials are no longer valid, and need
// to be retrieved.
// according to the caller minioCred.Credentials.IsExpired(),
// it already has a lock, so we don't need to worry about concurrency
func (c CredentialProvider) IsExpired() bool {
	ak, err := c.aliyunCreds.GetAccessKeyId()
	if err != nil {
		log.Warn("failed to get access key id from aliyun credential, assume it's expired")
		return true
	}
	return *ak != c.akCache
}

// WrapHTTPTransport wraps http.Transport, add header transformations to support Aliyun OSS
type WrapHTTPTransport struct {
	creds   *minioCred.Credentials
	backend transport
	region  string
}

// transport abstracts http.Transport to simplify test
type transport interface {
	RoundTrip(req *http.Request) (*http.Response, error)
}

// NewWrapHTTPTransport constructs a new WrapHTTPTransport
func NewWrapHTTPTransport(secure bool, region string, creds *minioCred.Credentials) (*WrapHTTPTransport, error) {
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create default transport")
	}
	return &WrapHTTPTransport{
		creds:   creds,
		backend: backend,
		region:  region,
	}, nil
}

// RoundTrip wraps original http.RoundTripper by transforming headers for Aliyun OSS compatibility
func (t *WrapHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Create a copy of the request to avoid modifying the original
	reqCopy := req.Clone(req.Context())

	// For MinIO "If-None-Match:*" (object must not exist), add "x-oss-forbid-overwrite: true" for Aliyun OSS.
	if ifNoneMatch := reqCopy.Header.Get("If-None-Match"); ifNoneMatch == "*" {
		reqCopy.Header.Set("x-oss-forbid-overwrite", "true")
		reqCopy.Header.Del("If-None-Match")
	}

	// Map MinIO user metadata headers ("x-amz-meta-*") to OSS-compatible headers ("x-oss-meta-*").
	// We add OSS headers but keep the original AMZ headers for signature compatibility
	for key, values := range reqCopy.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
			suffix := key[len("x-amz-meta-"):]
			newKey := "x-oss-meta-" + suffix
			for _, v := range values {
				reqCopy.Header.Add(newKey, v)
			}
			reqCopy.Header.Del(key)
		}
	}

	value, valueErr := t.creds.Get()
	if valueErr != nil {
		return nil, valueErr
	}
	location := utils.GetBucketLocation(*reqCopy.URL, t.region)
	reqCopy = signer.SignV4(*reqCopy, value.AccessKeyID, value.SecretAccessKey, value.SessionToken, location)

	// ---- call backend ----
	resp, respErr := t.backend.RoundTrip(reqCopy)
	if respErr != nil {
		return nil, respErr
	}

	// Translate metadata headers back for MinIO SDK (read)
	for key, values := range resp.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-oss-meta-") {
			suffix := key[len("x-oss-meta-"):]
			newKey := "x-amz-meta-" + suffix
			for _, v := range values {
				resp.Header.Add(newKey, v)
			}
			resp.Header.Del(key)
		}
	}

	return resp, nil
}
