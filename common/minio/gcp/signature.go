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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	signatureAlgorithm = "GOOG4-HMAC-SHA256"
	emptySHA256        = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// signRequestGoog4 signs the HTTP request using GOOG4-HMAC-SHA256 algorithm
// This is Google Cloud Storage's equivalent of AWS Signature V4
func signRequestGoog4(req *http.Request, accessKey, secretKey string) error {
	// Get current time
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	timeStamp := now.Format("20060102T150405Z")

	// Calculate and set payload hash (required by GCS)
	payloadHash, err := calculatePayloadHash(req)
	if err != nil {
		return errors.Wrap(err, "failed to calculate payload hash")
	}
	req.Header.Set("x-goog-content-sha256", payloadHash)

	// Add required headers
	req.Header.Set("x-goog-date", timeStamp)
	req.Header.Set("host", req.Host)

	// Step 1: Create canonical request
	canonicalRequest := buildCanonicalRequest(req)

	// Step 2: Create string to sign
	credentialScope := fmt.Sprintf("%s/auto/storage/goog4_request", dateStamp)
	hashedCanonicalRequest := sha256Hex(canonicalRequest)
	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s",
		signatureAlgorithm,
		timeStamp,
		credentialScope,
		hashedCanonicalRequest)

	// Step 3: Calculate signature
	signature := calculateGoog4Signature(secretKey, dateStamp, stringToSign)

	// Step 4: Add authorization header
	signedHeaders := getSignedHeaders(req)
	authorizationHeader := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		signatureAlgorithm,
		accessKey,
		credentialScope,
		signedHeaders,
		signature)
	req.Header.Set("Authorization", authorizationHeader)

	return nil
}

// buildCanonicalRequest creates the canonical request string for GOOG4 signing
func buildCanonicalRequest(req *http.Request) string {
	// HTTP Method
	method := req.Method

	// Canonical URI
	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	// Canonical Query String
	canonicalQueryString := buildCanonicalQueryString(req.URL.Query())

	// Canonical Headers
	canonicalHeaders := buildCanonicalHeaders(req)

	// Signed Headers
	signedHeaders := getSignedHeaders(req)

	// Payload Hash
	payloadHash := getPayloadHash(req)

	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		payloadHash)
}

// buildCanonicalQueryString creates canonical query string
func buildCanonicalQueryString(values url.Values) string {
	if len(values) == 0 {
		return ""
	}

	var keys []string
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		for _, v := range values[k] {
			parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

// buildCanonicalHeaders creates canonical headers string
func buildCanonicalHeaders(req *http.Request) string {
	var headers []string
	headerMap := make(map[string][]string)

	for k, v := range req.Header {
		lowerKey := strings.ToLower(k)
		headerMap[lowerKey] = v
	}

	var keys []string
	for k := range headerMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		values := headerMap[k]
		for _, v := range values {
			headers = append(headers, fmt.Sprintf("%s:%s", k, strings.TrimSpace(v)))
		}
	}

	return strings.Join(headers, "\n") + "\n"
}

// getSignedHeaders returns the list of signed headers
func getSignedHeaders(req *http.Request) string {
	var headers []string
	for k := range req.Header {
		headers = append(headers, strings.ToLower(k))
	}
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

// calculatePayloadHash calculates the SHA256 hash of the request body
// and restores the body for subsequent use
func calculatePayloadHash(req *http.Request) (string, error) {
	// For requests without body
	if req.Body == nil {
		// Empty body SHA256
		return emptySHA256, nil
	}

	// Read the body
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return "", errors.Wrap(err, "failed to read request body")
	}

	// Close the original body
	req.Body.Close()

	// Calculate SHA256 hash
	hash := sha256.Sum256(bodyBytes)
	hashHex := hex.EncodeToString(hash[:])

	// Restore the body for subsequent use
	req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	// Also set Content-Length if not already set
	if req.ContentLength == -1 {
		req.ContentLength = int64(len(bodyBytes))
	}

	return hashHex, nil
}

// getPayloadHash returns the SHA256 hash of the request body from the header
func getPayloadHash(req *http.Request) string {
	// If content hash is already provided in header, use it
	if contentHash := req.Header.Get("x-goog-content-sha256"); contentHash != "" {
		return contentHash
	}

	// This shouldn't happen if signRequestGoog4 was called first
	// Return empty body hash as fallback
	return emptySHA256
}

// calculateGoog4Signature calculates the GOOG4-HMAC-SHA256 signature
func calculateGoog4Signature(secretKey, dateStamp, stringToSign string) string {
	// Signing key derivation (similar to AWS Signature V4)
	kDate := hmacSHA256([]byte("GOOG4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte("auto"))
	kService := hmacSHA256(kRegion, []byte("storage"))
	kSigning := hmacSHA256(kService, []byte("goog4_request"))

	// Calculate signature
	signature := hmacSHA256(kSigning, []byte(stringToSign))
	return hex.EncodeToString(signature)
}

// hmacSHA256 calculates HMAC-SHA256
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// sha256Hex returns the SHA256 hash of a string as hex
func sha256Hex(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}
