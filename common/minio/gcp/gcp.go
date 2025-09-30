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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/atomic"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// WrapHTTPTransport wraps http.Transport, add an auth header to support GCP native auth
type WrapHTTPTransport struct {
	tokenSrc     oauth2.TokenSource       // For OAuth2 authentication
	creds        *credentials.Credentials // For MinIO credentials authentication
	backend      transport
	currentToken atomic.Pointer[oauth2.Token]
	useOAuth2    bool // Flag to determine auth method
}

// transport abstracts http.Transport to simplify test
type transport interface {
	RoundTrip(req *http.Request) (*http.Response, error)
}

// NewWrapHTTPTransport constructs a new WrapHTTPTransport with OAuth2
func NewWrapHTTPTransport(secure bool) (*WrapHTTPTransport, error) {
	tokenSrc := google.ComputeTokenSource("")
	// in fact never return err
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create default transport")
	}
	return &WrapHTTPTransport{
		tokenSrc:  tokenSrc,
		backend:   backend,
		useOAuth2: true,
	}, nil
}

// NewWrapHTTPTransportWithCreds constructs a new WrapHTTPTransport with MinIO credentials
func NewWrapHTTPTransportWithCreds(secure bool, creds *credentials.Credentials) (*WrapHTTPTransport, error) {
	// in fact never return err
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create default transport")
	}
	return &WrapHTTPTransport{
		creds:     creds,
		backend:   backend,
		useOAuth2: false,
	}, nil
}

// RoundTrip wraps original http.RoundTripper by Adding a Bearer token acquired from tokenSrc
// TODO GOOGLE-SUPPORT-DEBUG
func (t *WrapHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Print original request headers
	fmt.Printf("=== GCP Transport - Original Request ===\n")
	fmt.Printf("Method: %s\n", req.Method)
	fmt.Printf("URL: %s\n", req.URL.String())
	fmt.Printf("Headers:\n")
	for key, values := range req.Header {
		for _, value := range values {
			fmt.Printf("  %s: %s\n", key, value)
		}
	}
	fmt.Printf("=========================================\n")

	// Handle authentication based on the configured method
	if t.useOAuth2 {
		// OAuth2 authentication for GCP native API
		fmt.Printf("GCP Transport - Using OAuth2 authentication\n")
		currentToken := t.currentToken.Load()
		if currentToken.Valid() {
			req.Header.Set("Authorization", "Bearer "+currentToken.AccessToken)
			fmt.Printf("GCP Transport - Using cached token\n")
		} else {
			newToken, err := t.tokenSrc.Token()
			if err != nil {
				return nil, errors.Wrap(err, "failed to acquire token")
			}
			t.currentToken.Store(newToken)
			req.Header.Set("Authorization", "Bearer "+newToken.AccessToken)
			fmt.Printf("GCP Transport - Acquired new token\n")
		}
	} else {
		// MinIO credentials authentication - check if we need to handle HMAC-SHA1 for GCS
		fmt.Printf("GCP Transport - Using MinIO credentials authentication (S3 compatible)\n")

		// For GCS S3-compatible API, we might need to validate the credentials format
		if t.creds != nil {
			credValue, err := t.creds.Get()
			if err == nil {
				fmt.Printf("GCP Transport - Using AccessKey: %s (SessionToken: %s)\n",
					credValue.AccessKeyID,
					func() string {
						if credValue.SessionToken != "" {
							return "present"
						}
						return "none"
					}())
			} else {
				fmt.Printf("GCP Transport - Failed to get credentials: %v\n", err)
			}
		}

		// Let MinIO SDK handle the signing, but log the process
		fmt.Printf("GCP Transport - MinIO SDK will handle AWS signature v4/v2 signing\n")
	}

	// For MinIO "If-None-Match:*" (object must not exist), use "x-goog-if-generation-match: 0" on Google Cloud Storage.
	if ifNoneMatch := req.Header.Get("If-None-Match"); ifNoneMatch == "*" {
		req.Header.Set("x-goog-if-generation-match", "0")
		fmt.Printf("GCP Transport - Transformed If-None-Match: * → x-goog-if-generation-match: 0\n")
	}

	// Map MinIO user metadata headers ("x-amz-meta-*") to GCS-compatible headers ("x-goog-meta-*").
	transformedHeaders := make(map[string][]string)
	for key, values := range req.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
			suffix := key[len("x-amz-meta-"):]
			newKey := "x-goog-meta-" + suffix
			transformedHeaders[key] = values
			for _, v := range values {
				req.Header.Add(newKey, v)
			}
			req.Header.Del(key)
		}
	}

	if len(transformedHeaders) > 0 {
		fmt.Printf("GCP Transport - Transformed metadata headers:\n")
		for oldKey, values := range transformedHeaders {
			suffix := oldKey[len("x-amz-meta-"):]
			newKey := "x-goog-meta-" + suffix
			fmt.Printf("  %s → %s: %v\n", oldKey, newKey, values)
		}
	}

	// Print transformed request headers before sending
	fmt.Printf("=== GCP Transport - Transformed Request ===\n")
	fmt.Printf("Headers:\n")
	for key, values := range req.Header {
		for _, value := range values {
			fmt.Printf("  %s: %s\n", key, value)
		}
	}
	fmt.Printf("==========================================\n")

	// ---- call backend ----
	resp, respErr := t.backend.RoundTrip(req)
	if respErr != nil {
		fmt.Printf("GCP Transport - Backend request failed: %v\n", respErr)
		return nil, respErr
	}

	// Print original response headers
	fmt.Printf("=== GCP Transport - Original Response ===\n")
	fmt.Printf("Status Code: %d\n", resp.StatusCode)
	fmt.Printf("Headers:\n")
	for key, values := range resp.Header {
		for _, value := range values {
			fmt.Printf("  %s: %s\n", key, value)
		}
	}
	fmt.Printf("=========================================\n")

	// Translate metadata headers back for MinIO SDK (read)
	responseTransformed := make(map[string][]string)
	for key, values := range resp.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-goog-meta-") {
			suffix := key[len("x-goog-meta-"):]
			newKey := "x-amz-meta-" + suffix
			responseTransformed[key] = values
			for _, v := range values {
				resp.Header.Add(newKey, v)
			}
			resp.Header.Del(key)
		}
	}

	if len(responseTransformed) > 0 {
		fmt.Printf("GCP Transport - Transformed response metadata headers:\n")
		for oldKey, values := range responseTransformed {
			suffix := oldKey[len("x-goog-meta-"):]
			newKey := "x-amz-meta-" + suffix
			fmt.Printf("  %s → %s: %v\n", oldKey, newKey, values)
		}
	}

	// Print final response headers
	fmt.Printf("=== GCP Transport - Final Response ===\n")
	fmt.Printf("Headers:\n")
	for key, values := range resp.Header {
		for _, value := range values {
			fmt.Printf("  %s: %s\n", key, value)
		}
	}
	fmt.Printf("======================================\n")

	return resp, nil
}

const GcsDefaultAddress = "storage.googleapis.com"

// NewMinioClient returns a minio.Client which is compatible for GCS
func NewMinioClient(address string, opts *minio.Options) (*minio.Client, error) {
	fmt.Println("==========TestObjectStoragePutObjectIfNoneMatch new gcp client start ===========")
	if opts == nil {
		opts = &minio.Options{}
	}
	if address == "" {
		address = GcsDefaultAddress
		opts.Secure = true
	}

	// adhoc to remove port of gcs address to let minio-go know it's gcs
	if strings.Contains(address, GcsDefaultAddress) {
		address = GcsDefaultAddress
	}

	// Always use our custom transport for header transformation
	var transport *WrapHTTPTransport
	var err error

	if opts.Creds != nil {
		fmt.Println("==========TestObjectStoragePutObjectIfNoneMatch new gcp client, with existing creds ===========")

		// Check if we can get the credentials
		credValue, err := opts.Creds.Get()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get GCP credentials")
		}

		fmt.Printf("GCP Client - AccessKey: %s, SecretKey: %s, SessionToken: %s\n",
			credValue.AccessKeyID,
			func() string {
				if len(credValue.SecretAccessKey) > 0 {
					maxLen := 8
					if len(credValue.SecretAccessKey) < maxLen {
						maxLen = len(credValue.SecretAccessKey)
					}
					return credValue.SecretAccessKey[:maxLen] + "..."
				}
				return "empty"
			}(),
			func() string {
				if credValue.SessionToken != "" {
					return "present"
				}
				return "none"
			}())

		// Important: For GCS S3-compatible API, we might need to ensure proper configuration
		// Set the signature version to v2 for GCS compatibility if not already set
		if opts.BucketLookup == 0 {
			opts.BucketLookup = minio.BucketLookupPath // GCS typically uses path-style
		}

		// Use credentials-aware transport that handles both auth and header transformation
		transport, err = NewWrapHTTPTransportWithCreds(opts.Secure, opts.Creds)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create credentials transport")
		}
	} else {
		fmt.Println("==========TestObjectStoragePutObjectIfNoneMatch  new gcp client, with OAuth2 ===========")
		// Use OAuth2 transport
		transport, err = NewWrapHTTPTransport(opts.Secure)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create OAuth2 transport")
		}
		opts.Creds = credentials.NewStaticV2("", "", "")
	}

	opts.Transport = transport
	return minio.New(address, opts)
}
