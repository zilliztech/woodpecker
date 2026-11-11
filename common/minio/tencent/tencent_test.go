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

package tencent

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/mocks/mocks_tencent"
)

// mockTransport implements the transport interface for testing.
type mockTransport struct {
	roundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req)
}

// --- CredentialProvider tests ---

func TestCredentialProvider_Retrieve(t *testing.T) {
	mockCred := mocks_tencent.NewMockCredential(t)
	mockCred.EXPECT().GetSecretId().Return("SID-123")
	mockCred.EXPECT().GetSecretKey().Return("SKEY-456")
	mockCred.EXPECT().GetToken().Return("token-789")

	cp := &CredentialProvider{tencentCreds: mockCred}
	val, err := cp.Retrieve()
	require.NoError(t, err)
	assert.Equal(t, "SID-123", val.AccessKeyID)
	assert.Equal(t, "SKEY-456", val.SecretAccessKey)
	assert.Equal(t, "token-789", val.SessionToken)
	assert.Equal(t, "SID-123", cp.akCache)
}

func TestCredentialProvider_IsExpired_NotExpired(t *testing.T) {
	mockCred := mocks_tencent.NewMockCredential(t)
	mockCred.EXPECT().GetSecretId().Return("SID-same")

	cp := CredentialProvider{tencentCreds: mockCred, akCache: "SID-same"}
	assert.False(t, cp.IsExpired())
}

func TestCredentialProvider_IsExpired_Expired(t *testing.T) {
	mockCred := mocks_tencent.NewMockCredential(t)
	mockCred.EXPECT().GetSecretId().Return("SID-new")

	cp := CredentialProvider{tencentCreds: mockCred, akCache: "SID-old"}
	assert.True(t, cp.IsExpired())
}

// --- RoundTrip tests ---

func TestRoundTrip_IfNoneMatch_ToCosForbidOverwrite(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "TOKEN")

	var capturedReq *http.Request
	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "ap-guangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}

	req, _ := http.NewRequest("PUT", "http://cos.ap-guangzhou.myqcloud.com/bucket/key", nil)
	req.Header.Set("If-None-Match", "*")

	resp, err := tp.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "true", capturedReq.Header.Get("x-cos-forbid-overwrite"))
	assert.Empty(t, capturedReq.Header.Get("If-None-Match"))
}

func TestRoundTrip_MetadataHeaders_AmzToCos(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")

	var capturedReq *http.Request
	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "ap-guangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}

	req, _ := http.NewRequest("PUT", "http://cos.ap-guangzhou.myqcloud.com/bucket/key", nil)
	req.Header.Set("X-Amz-Meta-Custom", "value1")

	_, err := tp.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, "value1", capturedReq.Header.Get("x-cos-meta-Custom"))
	assert.Empty(t, capturedReq.Header.Get("X-Amz-Meta-Custom"))
}

func TestRoundTrip_ResponseHeaders_CosToAmz(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")

	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "ap-guangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				resp := &http.Response{StatusCode: 200, Header: http.Header{}}
				resp.Header.Set("X-Cos-Meta-MyKey", "myval")
				return resp, nil
			},
		},
	}

	req, _ := http.NewRequest("GET", "http://cos.ap-guangzhou.myqcloud.com/bucket/key", nil)
	resp, err := tp.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, "myval", resp.Header.Get("x-amz-meta-MyKey"))
	assert.Empty(t, resp.Header.Get("X-Cos-Meta-MyKey"))
}

func TestRoundTrip_NoTransformForNormalHeaders(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")

	var capturedReq *http.Request
	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "ap-guangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}

	req, _ := http.NewRequest("GET", "http://cos.ap-guangzhou.myqcloud.com/bucket/key", nil)
	req.Header.Set("Content-Type", "application/json")

	_, err := tp.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, "application/json", capturedReq.Header.Get("Content-Type"))
}

func TestRoundTrip_BackendError(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")

	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "ap-guangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("network error")
			},
		},
	}

	req, _ := http.NewRequest("GET", "http://cos.ap-guangzhou.myqcloud.com/bucket/key", nil)
	_, err := tp.RoundTrip(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "network error")
}

func TestRoundTrip_RequestSigned(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "SESSION")

	var capturedReq *http.Request
	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "ap-guangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}

	req, _ := http.NewRequest("GET", "http://cos.ap-guangzhou.myqcloud.com/bucket/key", nil)
	_, err := tp.RoundTrip(req)
	require.NoError(t, err)
	// SignV4 should add Authorization header
	assert.Contains(t, capturedReq.Header.Get("Authorization"), "AWS4-HMAC-SHA256")
}

func TestRoundTrip_CredentialsError(t *testing.T) {
	creds := minioCred.New(&failingProvider{})

	tp := &WrapHTTPTransport{
		creds:  creds,
		region: "ap-guangzhou",
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
	}

	req, _ := http.NewRequest("GET", "http://cos.ap-guangzhou.myqcloud.com/bucket/key", nil)
	_, err := tp.RoundTrip(req)
	assert.Error(t, err)
}

type failingProvider struct{}

func (f *failingProvider) Retrieve() (minioCred.Value, error) {
	return minioCred.Value{}, fmt.Errorf("cred retrieval failed")
}

func (f *failingProvider) IsExpired() bool { return true }

// --- NewMinioClient tests ---

func TestNewMinioClient_WithPresetCredsAndTransport(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")
	transport := &mockTransport{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
		},
	}

	opts := &minio.Options{
		Creds:     creds,
		Transport: transport,
		Region:    "ap-guangzhou",
	}

	client, err := NewMinioClient("cos.ap-guangzhou.myqcloud.com", opts)
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNewMinioClient_EmptyAddress_SetsDefault(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")
	transport := &mockTransport{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
		},
	}

	opts := &minio.Options{
		Creds:     creds,
		Transport: transport,
		Region:    "ap-guangzhou",
	}

	client, err := NewMinioClient("", opts)
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.True(t, opts.Secure)
}

func TestNewMinioClient_NilOpts_CredentialError(t *testing.T) {
	_, err := NewMinioClient("cos.ap-guangzhou.myqcloud.com", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create credential provider")
}

func TestNewMinioClient_NilCreds_CredentialError(t *testing.T) {
	opts := &minio.Options{
		Region: "ap-guangzhou",
	}
	_, err := NewMinioClient("cos.ap-guangzhou.myqcloud.com", opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create credential provider")
}

func TestNewMinioClient_NilTransport_CreatesDefault(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")
	opts := &minio.Options{
		Creds:  creds,
		Region: "ap-guangzhou",
	}

	client, err := NewMinioClient("cos.ap-guangzhou.myqcloud.com", opts)
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.NotNil(t, opts.Transport)
}

// --- NewWrapHTTPTransport tests ---

func TestNewWrapHTTPTransport_Insecure(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")
	transport, err := NewWrapHTTPTransport(false, "ap-guangzhou", creds)
	require.NoError(t, err)
	assert.NotNil(t, transport)
	assert.Equal(t, "ap-guangzhou", transport.region)
	assert.Equal(t, creds, transport.creds)
	assert.NotNil(t, transport.backend)
}

func TestNewWrapHTTPTransport_Secure(t *testing.T) {
	creds := minioCred.NewStaticV4("AKID", "SECRET", "")
	transport, err := NewWrapHTTPTransport(true, "ap-beijing", creds)
	require.NoError(t, err)
	assert.NotNil(t, transport)
	assert.Equal(t, "ap-beijing", transport.region)
}

// --- NewCredentialProvider tests ---

func TestNewCredentialProvider_FailsWithoutEnv(t *testing.T) {
	// Without proper tencent cloud environment configuration, this should fail
	_, err := NewCredentialProvider()
	assert.Error(t, err)
}
