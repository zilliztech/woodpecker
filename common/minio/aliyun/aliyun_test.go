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
	"fmt"
	"net/http"
	"testing"

	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/mocks/mocks_aliyun"
)

// mockTransport implements the transport interface for testing.
type mockTransport struct {
	roundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req)
}

func strPtr(s string) *string { return &s }

// --- CredentialProvider tests ---

func TestCredentialProvider_Retrieve_Success(t *testing.T) {
	mockCred := mocks_aliyun.NewMockCredential(t)
	mockCred.EXPECT().GetAccessKeyId().Return(strPtr("AKID-123"), nil)
	mockCred.EXPECT().GetAccessKeySecret().Return(strPtr("SK-456"), nil)
	mockCred.EXPECT().GetSecurityToken().Return(strPtr("token-789"), nil)

	cp := &CredentialProvider{aliyunCreds: mockCred}
	val, err := cp.Retrieve()
	require.NoError(t, err)
	assert.Equal(t, "AKID-123", val.AccessKeyID)
	assert.Equal(t, "SK-456", val.SecretAccessKey)
	assert.Equal(t, "token-789", val.SessionToken)
	assert.Equal(t, "AKID-123", cp.akCache)
}

func TestCredentialProvider_Retrieve_AccessKeyIdError(t *testing.T) {
	mockCred := mocks_aliyun.NewMockCredential(t)
	mockCred.EXPECT().GetAccessKeyId().Return(nil, fmt.Errorf("ak error"))

	cp := &CredentialProvider{aliyunCreds: mockCred}
	_, err := cp.Retrieve()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get access key id")
}

func TestCredentialProvider_Retrieve_SecretKeyError(t *testing.T) {
	mockCred := mocks_aliyun.NewMockCredential(t)
	mockCred.EXPECT().GetAccessKeyId().Return(strPtr("AKID"), nil)
	mockCred.EXPECT().GetAccessKeySecret().Return(nil, fmt.Errorf("sk error"))

	cp := &CredentialProvider{aliyunCreds: mockCred}
	_, err := cp.Retrieve()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get access key secret")
}

func TestCredentialProvider_Retrieve_SecurityTokenError(t *testing.T) {
	mockCred := mocks_aliyun.NewMockCredential(t)
	mockCred.EXPECT().GetAccessKeyId().Return(strPtr("AKID"), nil)
	mockCred.EXPECT().GetAccessKeySecret().Return(strPtr("SK"), nil)
	mockCred.EXPECT().GetSecurityToken().Return(nil, fmt.Errorf("token error"))

	cp := &CredentialProvider{aliyunCreds: mockCred}
	_, err := cp.Retrieve()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get security token")
}

func TestCredentialProvider_IsExpired_NotExpired(t *testing.T) {
	mockCred := mocks_aliyun.NewMockCredential(t)
	mockCred.EXPECT().GetAccessKeyId().Return(strPtr("AKID-same"), nil)

	cp := CredentialProvider{aliyunCreds: mockCred, akCache: "AKID-same"}
	assert.False(t, cp.IsExpired())
}

func TestCredentialProvider_IsExpired_Expired(t *testing.T) {
	mockCred := mocks_aliyun.NewMockCredential(t)
	mockCred.EXPECT().GetAccessKeyId().Return(strPtr("AKID-new"), nil)

	cp := CredentialProvider{aliyunCreds: mockCred, akCache: "AKID-old"}
	assert.True(t, cp.IsExpired())
}

func TestCredentialProvider_IsExpired_Error(t *testing.T) {
	mockCred := mocks_aliyun.NewMockCredential(t)
	mockCred.EXPECT().GetAccessKeyId().Return(nil, fmt.Errorf("err"))

	cp := CredentialProvider{aliyunCreds: mockCred}
	assert.True(t, cp.IsExpired()) // error → assume expired
}

// --- RoundTrip tests ---

func TestRoundTrip_IfNoneMatch_ToForbidOverwrite(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "TOKEN")

	var capturedReq *http.Request
	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "cn-hangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}

	req, _ := http.NewRequest("PUT", "http://oss-cn-hangzhou.aliyuncs.com/bucket/key", nil)
	req.Header.Set("If-None-Match", "*")

	resp, err := tp.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "true", capturedReq.Header.Get("x-oss-forbid-overwrite"))
	assert.Empty(t, capturedReq.Header.Get("If-None-Match"))
}

func TestRoundTrip_MetadataHeaders_AmzToOss(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")

	var capturedReq *http.Request
	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "cn-hangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}

	req, _ := http.NewRequest("PUT", "http://oss-cn-hangzhou.aliyuncs.com/bucket/key", nil)
	req.Header.Set("X-Amz-Meta-MyKey", "myval")

	_, err := tp.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, "myval", capturedReq.Header.Get("x-oss-meta-MyKey"))
	assert.Empty(t, capturedReq.Header.Get("X-Amz-Meta-MyKey"))
}

func TestRoundTrip_ResponseHeaders_OssToAmz(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")

	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "cn-hangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				resp := &http.Response{StatusCode: 200, Header: http.Header{}}
				resp.Header.Set("X-Oss-Meta-Custom", "val123")
				return resp, nil
			},
		},
	}

	req, _ := http.NewRequest("GET", "http://oss-cn-hangzhou.aliyuncs.com/bucket/key", nil)
	resp, err := tp.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, "val123", resp.Header.Get("x-amz-meta-Custom"))
	assert.Empty(t, resp.Header.Get("X-Oss-Meta-Custom"))
}

func TestRoundTrip_NoTransformForNormalHeaders(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")

	var capturedReq *http.Request
	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "cn-hangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}

	req, _ := http.NewRequest("GET", "http://oss-cn-hangzhou.aliyuncs.com/bucket/key", nil)
	req.Header.Set("Content-Type", "application/json")

	_, err := tp.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, "application/json", capturedReq.Header.Get("Content-Type"))
}

func TestRoundTrip_BackendError(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")

	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "cn-hangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("network error")
			},
		},
	}

	req, _ := http.NewRequest("GET", "http://oss-cn-hangzhou.aliyuncs.com/bucket/key", nil)
	_, err := tp.RoundTrip(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "network error")
}

func TestRoundTrip_RequestSigned(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "SESSION")

	var capturedReq *http.Request
	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "cn-hangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}

	req, _ := http.NewRequest("GET", "http://oss-cn-hangzhou.aliyuncs.com/bucket/key", nil)
	_, err := tp.RoundTrip(req)
	require.NoError(t, err)
	// SignV4 should have added an Authorization header
	assert.Contains(t, capturedReq.Header.Get("Authorization"), "AWS4-HMAC-SHA256")
}

func TestRoundTrip_CredentialsError(t *testing.T) {
	// Create creds with a provider that always fails
	creds := minioCred.New(&failingProvider{})

	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "cn-hangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}

	req, _ := http.NewRequest("GET", "http://oss-cn-hangzhou.aliyuncs.com/bucket/key", nil)
	_, err := tp.RoundTrip(req)
	assert.Error(t, err)
}

type failingProvider struct{}

func (f *failingProvider) Retrieve() (minioCred.Value, error) {
	return minioCred.Value{}, fmt.Errorf("cred retrieval failed")
}

func (f *failingProvider) IsExpired() bool { return true }

// --- NewWrapHTTPTransport tests ---

func TestNewWrapHTTPTransport_Success(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "TOKEN")
	tp, err := NewWrapHTTPTransport(true, "cn-hangzhou", creds)
	require.NoError(t, err)
	assert.NotNil(t, tp)
	assert.Equal(t, "cn-hangzhou", tp.region)
	assert.NotNil(t, tp.backend)
	assert.NotNil(t, tp.creds)
}

// --- NewMinioClient tests ---

func TestNewMinioClient_WithCredsAndTransport(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "TOKEN")
	opts := &minio.Options{
		Creds: creds,
		Transport: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}
	client, err := NewMinioClient("play.min.io", opts)
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNewMinioClient_EmptyAddressSetsDefault(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "TOKEN")
	opts := &minio.Options{
		Creds: creds,
		Transport: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}
	client, err := NewMinioClient("", opts)
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.True(t, opts.Secure)
}

func TestNewMinioClient_NilOpts_CredentialError(t *testing.T) {
	// nil opts creates a new empty Options struct. Without aliyun credentials
	// configured, NewCredentialProvider will fail.
	_, err := NewMinioClient("play.min.io", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create credential provider")
}

func TestNewMinioClient_CreatesTransportWhenNil(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "TOKEN")
	opts := &minio.Options{
		Creds:  creds,
		Region: "cn-hangzhou",
	}
	client, err := NewMinioClient("oss-cn-hangzhou.aliyuncs.com", opts)
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.NotNil(t, opts.Transport)
}
