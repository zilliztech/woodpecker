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
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- sha256Hex ---

func TestSha256Hex(t *testing.T) {
	assert.Equal(t, emptySHA256, sha256Hex(""))
	assert.Equal(t, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", sha256Hex("hello"))
}

// --- hmacSHA256 ---

func TestHmacSHA256(t *testing.T) {
	result := hmacSHA256([]byte("key"), []byte("data"))
	assert.Len(t, result, 32) // SHA256 produces 32 bytes

	// Same inputs produce same output
	result2 := hmacSHA256([]byte("key"), []byte("data"))
	assert.Equal(t, result, result2)

	// Different inputs produce different output
	result3 := hmacSHA256([]byte("key"), []byte("other"))
	assert.NotEqual(t, result, result3)
}

// --- calculateGoog4Signature ---

func TestCalculateGoog4Signature(t *testing.T) {
	sig := calculateGoog4Signature("mySecret", "20240101", "string-to-sign")
	assert.Len(t, sig, 64) // hex-encoded SHA256 = 64 chars
	// Deterministic
	assert.Equal(t, sig, calculateGoog4Signature("mySecret", "20240101", "string-to-sign"))
	// Different secret → different signature
	assert.NotEqual(t, sig, calculateGoog4Signature("otherSecret", "20240101", "string-to-sign"))
	// Different date → different signature
	assert.NotEqual(t, sig, calculateGoog4Signature("mySecret", "20240102", "string-to-sign"))
}

// --- getPayloadHash ---

func TestGetPayloadHash(t *testing.T) {
	t.Run("header present", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://example.com", nil)
		req.Header.Set("x-goog-content-sha256", "abc123")
		assert.Equal(t, "abc123", getPayloadHash(req))
	})

	t.Run("header absent returns empty hash", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://example.com", nil)
		assert.Equal(t, emptySHA256, getPayloadHash(req))
	})
}

// --- calculatePayloadHash ---

func TestCalculatePayloadHash(t *testing.T) {
	t.Run("nil body", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://example.com", nil)
		hash, err := calculatePayloadHash(req)
		require.NoError(t, err)
		assert.Equal(t, emptySHA256, hash)
	})

	t.Run("non-empty body", func(t *testing.T) {
		body := bytes.NewBufferString("hello world")
		req, _ := http.NewRequest("PUT", "http://example.com", body)
		req.ContentLength = -1 // force the ContentLength branch

		hash, err := calculatePayloadHash(req)
		require.NoError(t, err)
		assert.NotEqual(t, emptySHA256, hash)
		assert.Len(t, hash, 64)

		// Body should be restored for subsequent reads
		restored, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		assert.Equal(t, "hello world", string(restored))

		// ContentLength should be set
		assert.Equal(t, int64(11), req.ContentLength)
	})

	t.Run("body with ContentLength already set", func(t *testing.T) {
		body := bytes.NewBufferString("data")
		req, _ := http.NewRequest("PUT", "http://example.com", body)
		req.ContentLength = 4

		hash, err := calculatePayloadHash(req)
		require.NoError(t, err)
		assert.Len(t, hash, 64)
		assert.Equal(t, int64(4), req.ContentLength) // unchanged
	})

	t.Run("read error", func(t *testing.T) {
		req, _ := http.NewRequest("PUT", "http://example.com", nil)
		req.Body = io.NopCloser(&errorReader{})

		_, err := calculatePayloadHash(req)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read request body")
	})
}

type errorReader struct{}

func (e *errorReader) Read(_ []byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

// --- buildCanonicalQueryString ---

func TestBuildCanonicalQueryString(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert.Equal(t, "", buildCanonicalQueryString(url.Values{}))
	})

	t.Run("single param", func(t *testing.T) {
		v := url.Values{"prefix": {"test"}}
		assert.Equal(t, "prefix=test", buildCanonicalQueryString(v))
	})

	t.Run("multiple params sorted", func(t *testing.T) {
		v := url.Values{"z-key": {"val"}, "a-key": {"val"}}
		result := buildCanonicalQueryString(v)
		assert.True(t, strings.HasPrefix(result, "a-key="))
	})

	t.Run("special characters escaped", func(t *testing.T) {
		v := url.Values{"key": {"val/ue"}}
		result := buildCanonicalQueryString(v)
		assert.Contains(t, result, "val") // escaped properly
	})

	t.Run("multi-value key", func(t *testing.T) {
		v := url.Values{"key": {"a", "b"}}
		result := buildCanonicalQueryString(v)
		assert.Contains(t, result, "key=a")
		assert.Contains(t, result, "key=b")
	})
}

// --- getSignedHeaders ---

func TestGetSignedHeaders(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	req.Header.Set("Host", "example.com")
	req.Header.Set("X-Goog-Date", "20240101T000000Z")
	req.Header.Set("Content-Type", "application/json")

	signed := getSignedHeaders(req)
	parts := strings.Split(signed, ";")
	// Should be lowercase and sorted
	for i := 1; i < len(parts); i++ {
		assert.True(t, parts[i-1] <= parts[i], "headers should be sorted: %s > %s", parts[i-1], parts[i])
	}
	assert.Contains(t, signed, "host")
	assert.Contains(t, signed, "x-goog-date")
	assert.Contains(t, signed, "content-type")
}

// --- buildCanonicalHeaders ---

func TestBuildCanonicalHeaders(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	req.Header.Set("Host", "example.com")
	req.Header.Set("X-Goog-Date", "20240101T000000Z")

	canonical := buildCanonicalHeaders(req)
	// Should end with trailing newline
	assert.True(t, strings.HasSuffix(canonical, "\n"))
	// Should be lowercase
	assert.Contains(t, canonical, "host:example.com")
	assert.Contains(t, canonical, "x-goog-date:20240101T000000Z")
	// Values should be trimmed
	req.Header.Set("X-Custom", "  spaced  ")
	canonical = buildCanonicalHeaders(req)
	assert.Contains(t, canonical, "x-custom:spaced")
}

// --- buildCanonicalRequest ---

func TestBuildCanonicalRequest(t *testing.T) {
	t.Run("GET with path", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://example.com/bucket/key?prefix=abc", nil)
		req.Header.Set("host", "example.com")
		req.Header.Set("x-goog-content-sha256", emptySHA256)

		cr := buildCanonicalRequest(req)
		lines := strings.Split(cr, "\n")
		assert.Equal(t, "GET", lines[0])                   // method
		assert.Equal(t, "/bucket/key", lines[1])           // canonical URI
		assert.Contains(t, cr, "prefix=abc")               // query string
		assert.True(t, strings.HasSuffix(cr, emptySHA256)) // payload hash
	})

	t.Run("empty path defaults to /", func(t *testing.T) {
		reqURL, _ := url.Parse("http://example.com")
		reqURL.Path = ""
		req := &http.Request{Method: "GET", URL: reqURL, Header: http.Header{}}
		req.Header.Set("x-goog-content-sha256", emptySHA256)

		cr := buildCanonicalRequest(req)
		lines := strings.Split(cr, "\n")
		assert.Equal(t, "/", lines[1])
	})

	t.Run("PUT with body hash", func(t *testing.T) {
		req, _ := http.NewRequest("PUT", "http://example.com/obj", nil)
		req.Header.Set("host", "example.com")
		req.Header.Set("x-goog-content-sha256", "abcdef1234567890")

		cr := buildCanonicalRequest(req)
		assert.Equal(t, "PUT", strings.Split(cr, "\n")[0])
		assert.True(t, strings.HasSuffix(cr, "abcdef1234567890"))
	})
}

// --- signRequestGoog4 (integration) ---

func TestSignRequestGoog4(t *testing.T) {
	t.Run("GET no body", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://storage.googleapis.com/bucket/object", nil)

		err := signRequestGoog4(req, "GOOG_ACCESS_KEY", "GOOG_SECRET")
		require.NoError(t, err)

		// Authorization header should be set
		auth := req.Header.Get("Authorization")
		assert.True(t, strings.HasPrefix(auth, signatureAlgorithm+" Credential=GOOG_ACCESS_KEY/"))
		assert.Contains(t, auth, "/auto/storage/goog4_request")
		assert.Contains(t, auth, "SignedHeaders=")
		assert.Contains(t, auth, "Signature=")

		// Required headers should be set
		assert.NotEmpty(t, req.Header.Get("x-goog-date"))
		assert.NotEmpty(t, req.Header.Get("x-goog-content-sha256"))
		assert.Equal(t, "storage.googleapis.com", req.Header.Get("host"))
	})

	t.Run("PUT with body", func(t *testing.T) {
		body := bytes.NewBufferString(`{"data":"value"}`)
		req, _ := http.NewRequest("PUT", "http://storage.googleapis.com/bucket/object", body)

		err := signRequestGoog4(req, "KEY", "SECRET")
		require.NoError(t, err)

		auth := req.Header.Get("Authorization")
		assert.True(t, strings.HasPrefix(auth, signatureAlgorithm+" Credential=KEY/"))

		// Body should still be readable
		restored, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		assert.Equal(t, `{"data":"value"}`, string(restored))

		// Payload hash should NOT be empty hash since we had a body
		assert.NotEqual(t, emptySHA256, req.Header.Get("x-goog-content-sha256"))
	})

	t.Run("body read error", func(t *testing.T) {
		req, _ := http.NewRequest("PUT", "http://storage.googleapis.com/bucket/object", nil)
		req.Body = io.NopCloser(&errorReader{})

		err := signRequestGoog4(req, "KEY", "SECRET")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to calculate payload hash")
	})
}
