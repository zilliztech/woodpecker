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
	tokenSrc     oauth2.TokenSource
	backend      transport
	currentToken atomic.Pointer[oauth2.Token]
	useOAuth2    bool // Flag to determine auth method
}

// transport abstracts http.Transport to simplify test
type transport interface {
	RoundTrip(req *http.Request) (*http.Response, error)
}

// NewWrapHTTPTransportWithOAuth2 constructs a new WrapHTTPTransport with OAuth2
func NewWrapHTTPTransportWithOAuth2(secure bool) (*WrapHTTPTransport, error) {
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
func NewWrapHTTPTransportWithCreds(secure bool) (*WrapHTTPTransport, error) {
	// in fact never return err
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create default transport")
	}
	return &WrapHTTPTransport{
		backend:   backend,
		useOAuth2: false,
	}, nil
}

// RoundTrip wraps original http.RoundTripper by Adding a Bearer token acquired from tokenSrc
func (t *WrapHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Handle authentication based on the configured method
	if t.useOAuth2 {
		// OAuth2 authentication for GCP native API
		currentToken := t.currentToken.Load()
		if currentToken.Valid() {
			req.Header.Set("Authorization", "Bearer "+currentToken.AccessToken)
		} else {
			newToken, err := t.tokenSrc.Token()
			if err != nil {
				return nil, errors.Wrap(err, "failed to acquire token")
			}
			t.currentToken.Store(newToken)
			req.Header.Set("Authorization", "Bearer "+newToken.AccessToken)
		}
	}

	// For MinIO "If-None-Match:*" (object must not exist), use "x-goog-if-generation-match: 0" on Google Cloud Storage.
	if ifNoneMatch := req.Header.Get("If-None-Match"); ifNoneMatch == "*" {
		req.Header.Set("x-goog-if-generation-match", "0")
	}

	// Map MinIO user metadata headers ("x-amz-meta-*") to GCS-compatible headers ("x-goog-meta-*").
	for key, values := range req.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
			suffix := key[len("x-amz-meta-"):]
			newKey := "x-goog-meta-" + suffix
			for _, v := range values {
				req.Header.Add(newKey, v)
			}
			req.Header.Del(key)
		}
	}

	// ---- call backend ----
	resp, respErr := t.backend.RoundTrip(req)
	if respErr != nil {
		return nil, respErr
	}

	// Translate metadata headers back for MinIO SDK (read)
	for key, values := range resp.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-goog-meta-") {
			suffix := key[len("x-goog-meta-"):]
			newKey := "x-amz-meta-" + suffix
			for _, v := range values {
				resp.Header.Add(newKey, v)
			}
			resp.Header.Del(key)
		}
	}

	return resp, nil
}

const GcsDefaultAddress = "storage.googleapis.com"

// NewMinioClient returns a minio.Client which is compatible for GCS
func NewMinioClient(address string, opts *minio.Options) (*minio.Client, error) {
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
		// Use credentials-aware transport that handles both auth and header transformation
		transport, err = NewWrapHTTPTransportWithCreds(opts.Secure)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create credentials transport")
		}
	} else {
		// Use OAuth2 transport
		transport, err = NewWrapHTTPTransportWithOAuth2(opts.Secure)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create OAuth2 transport")
		}
		opts.Creds = credentials.NewStaticV2("", "", "")
	}

	opts.Transport = transport
	return minio.New(address, opts)
}
