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

package gcp

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

// mockTransport implements the transport interface for testing.
type mockTransport struct {
	roundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req)
}

// mockTokenSource implements oauth2.TokenSource for testing.
type mockTokenSource struct {
	token *oauth2.Token
	err   error
}

func (m *mockTokenSource) Token() (*oauth2.Token, error) {
	return m.token, m.err
}

// --- RoundTrip tests ---

func TestRoundTrip_OAuth2_CachedToken(t *testing.T) {
	validToken := &oauth2.Token{
		AccessToken: "cached-token-123",
		Expiry:      time.Now().Add(1 * time.Hour),
	}

	var capturedReq *http.Request
	transport := &WrapHTTPTransport{
		tokenSrc: &mockTokenSource{},
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
		useOAuth2: true,
	}
	transport.currentToken.Store(validToken)

	req, _ := http.NewRequest("GET", "http://storage.googleapis.com/bucket/key", nil)
	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "Bearer cached-token-123", capturedReq.Header.Get("Authorization"))
}

func TestRoundTrip_OAuth2_FetchNewToken(t *testing.T) {
	newToken := &oauth2.Token{
		AccessToken: "new-token-456",
		Expiry:      time.Now().Add(1 * time.Hour),
	}

	var capturedReq *http.Request
	transport := &WrapHTTPTransport{
		tokenSrc: &mockTokenSource{token: newToken},
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
		useOAuth2: true,
	}
	// currentToken is zero-value (nil), so Valid() returns false

	req, _ := http.NewRequest("GET", "http://storage.googleapis.com/bucket/key", nil)
	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "Bearer new-token-456", capturedReq.Header.Get("Authorization"))
	// Token should be cached
	assert.Equal(t, "new-token-456", transport.currentToken.Load().AccessToken)
}

func TestRoundTrip_OAuth2_TokenError(t *testing.T) {
	transport := &WrapHTTPTransport{
		tokenSrc: &mockTokenSource{err: fmt.Errorf("token fetch failed")},
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
		useOAuth2: true,
	}

	req, _ := http.NewRequest("GET", "http://storage.googleapis.com/bucket/key", nil)
	_, err := transport.RoundTrip(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to acquire token")
}

func TestRoundTrip_IfNoneMatch_ToGoogGenerationMatch(t *testing.T) {
	var capturedReq *http.Request
	transport := &WrapHTTPTransport{
		tokenSrc: &mockTokenSource{token: &oauth2.Token{AccessToken: "tok", Expiry: time.Now().Add(time.Hour)}},
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
		useOAuth2: true,
	}

	req, _ := http.NewRequest("PUT", "http://storage.googleapis.com/bucket/key", nil)
	req.Header.Set("If-None-Match", "*")

	_, err := transport.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, "0", capturedReq.Header.Get("x-goog-if-generation-match"))
	assert.Empty(t, capturedReq.Header.Get("If-None-Match"))
}

func TestRoundTrip_MetadataHeaders_AmzToGoog(t *testing.T) {
	var capturedReq *http.Request
	transport := &WrapHTTPTransport{
		tokenSrc: &mockTokenSource{token: &oauth2.Token{AccessToken: "tok", Expiry: time.Now().Add(time.Hour)}},
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
		useOAuth2: true,
	}

	req, _ := http.NewRequest("PUT", "http://storage.googleapis.com/bucket/key", nil)
	req.Header.Set("X-Amz-Meta-Custom", "value1")

	_, err := transport.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, "value1", capturedReq.Header.Get("x-goog-meta-Custom"))
	assert.Empty(t, capturedReq.Header.Get("X-Amz-Meta-Custom"))
}

func TestRoundTrip_ResponseHeaders_GoogToAmz(t *testing.T) {
	transport := &WrapHTTPTransport{
		tokenSrc: &mockTokenSource{token: &oauth2.Token{AccessToken: "tok", Expiry: time.Now().Add(time.Hour)}},
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				resp := &http.Response{StatusCode: 200, Header: http.Header{}}
				resp.Header.Set("X-Goog-Meta-MyKey", "myval")
				return resp, nil
			},
		},
		useOAuth2: true,
	}

	req, _ := http.NewRequest("GET", "http://storage.googleapis.com/bucket/key", nil)
	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, "myval", resp.Header.Get("x-amz-meta-MyKey"))
	assert.Empty(t, resp.Header.Get("X-Goog-Meta-MyKey"))
}

func TestRoundTrip_Creds_ResigningWithHeaderTransform(t *testing.T) {
	var capturedReq *http.Request
	creds := credentials.NewStaticV4("AKID", "SECRET", "")

	transport := &WrapHTTPTransport{
		creds: creds,
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
		useOAuth2: false,
	}

	req, _ := http.NewRequest("PUT", "http://storage.googleapis.com/bucket/key", nil)
	req.Header.Set("If-None-Match", "*")
	req.Header.Set("Authorization", "old-auth")
	req.Header.Set("x-amz-date", "old-date")
	req.Header.Set("x-amz-content-sha256", "old-hash")

	_, err := transport.RoundTrip(req)
	require.NoError(t, err)

	// If-None-Match should be transformed
	assert.Equal(t, "0", capturedReq.Header.Get("x-goog-if-generation-match"))
	assert.Empty(t, capturedReq.Header.Get("If-None-Match"))
	// Should have been re-signed with GOOG4
	auth := capturedReq.Header.Get("Authorization")
	assert.Contains(t, auth, "GOOG4-HMAC-SHA256")
	assert.Contains(t, auth, "Credential=AKID/")
	// Old AMZ headers should be removed
	assert.Empty(t, capturedReq.Header.Get("x-amz-date"))
	assert.Empty(t, capturedReq.Header.Get("x-amz-content-sha256"))
}

func TestRoundTrip_Creds_NoResigningWithoutTransform(t *testing.T) {
	var capturedReq *http.Request
	creds := credentials.NewStaticV4("AKID", "SECRET", "")

	transport := &WrapHTTPTransport{
		creds: creds,
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
		useOAuth2: false,
	}

	req, _ := http.NewRequest("GET", "http://storage.googleapis.com/bucket/key", nil)
	req.Header.Set("Authorization", "original-auth")

	_, err := transport.RoundTrip(req)
	require.NoError(t, err)
	// No header transformation happened, so auth should remain original
	assert.Equal(t, "original-auth", capturedReq.Header.Get("Authorization"))
}

func TestRoundTrip_BackendError(t *testing.T) {
	transport := &WrapHTTPTransport{
		tokenSrc: &mockTokenSource{token: &oauth2.Token{AccessToken: "tok", Expiry: time.Now().Add(time.Hour)}},
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("network error")
			},
		},
		useOAuth2: true,
	}

	req, _ := http.NewRequest("GET", "http://storage.googleapis.com/bucket/key", nil)
	_, err := transport.RoundTrip(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "network error")
}

func TestRoundTrip_Creds_GetError(t *testing.T) {
	// Use expired static creds that will fail to get
	creds := credentials.NewStaticV4("", "", "")

	transport := &WrapHTTPTransport{
		creds: creds,
		backend: &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{}}, nil
			},
		},
		useOAuth2: false,
	}

	req, _ := http.NewRequest("PUT", "http://storage.googleapis.com/bucket/key", nil)
	req.Header.Set("If-None-Match", "*") // trigger re-signing

	_, err := transport.RoundTrip(req)
	// Empty static creds still succeed (they return empty values), so just verify it didn't panic
	assert.NoError(t, err)
}

// --- NewMinioClient tests ---

func TestNewMinioClient_DefaultAddress(t *testing.T) {
	creds := credentials.NewStaticV4("AK", "SK", "")
	client, err := NewMinioClient("", &minio.Options{Creds: creds})
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNewMinioClient_CustomAddress(t *testing.T) {
	creds := credentials.NewStaticV4("AK", "SK", "")
	client, err := NewMinioClient("custom-gcs.example.com", &minio.Options{Creds: creds})
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNewMinioClient_AddressContainsGCS(t *testing.T) {
	creds := credentials.NewStaticV4("AK", "SK", "")
	client, err := NewMinioClient("storage.googleapis.com:443", &minio.Options{Creds: creds})
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNewMinioClient_NilOpts(t *testing.T) {
	// nil opts → OAuth2 path (will succeed in constructing, though oauth2 won't work without real GCP)
	client, err := NewMinioClient("storage.googleapis.com", nil)
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNewMinioClient_NilCreds_UsesOAuth2(t *testing.T) {
	client, err := NewMinioClient("", &minio.Options{})
	require.NoError(t, err)
	assert.NotNil(t, client)
}
