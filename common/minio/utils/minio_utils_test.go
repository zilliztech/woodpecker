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

package utils

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetBucketLocation(t *testing.T) {
	testCases := []struct {
		name           string
		u              url.URL
		regionOverride string
		expected       string
	}{
		// regionOverride takes precedence
		{
			name:           "regionOverride non-empty",
			u:              url.URL{Host: "s3.amazonaws.com"},
			regionOverride: "ap-southeast-1",
			expected:       "ap-southeast-1",
		},
		{
			name:           "regionOverride with regional URL",
			u:              url.URL{Host: "s3.eu-west-1.amazonaws.com"},
			regionOverride: "us-west-2",
			expected:       "us-west-2",
		},
		// URL contains a region
		{
			name:           "S3 regional endpoint",
			u:              url.URL{Host: "s3.eu-west-1.amazonaws.com"},
			regionOverride: "",
			expected:       "eu-west-1",
		},
		{
			name:           "S3 dualstack regional endpoint",
			u:              url.URL{Host: "s3.dualstack.us-west-1.amazonaws.com"},
			regionOverride: "",
			expected:       "us-west-1",
		},
		{
			name:           "S3 China region",
			u:              url.URL{Host: "s3.cn-north-1.amazonaws.com.cn"},
			regionOverride: "",
			expected:       "cn-north-1",
		},
		{
			name:           "S3 VPCE endpoint",
			u:              url.URL{Host: "bucket.vpce-1a2b3c4d-5e6f.s3.us-east-1.vpce.amazonaws.com"},
			regionOverride: "",
			expected:       "us-east-1",
		},
		// URL has no region → fallback to us-east-1
		{
			name:           "S3 global endpoint fallback",
			u:              url.URL{Host: "s3.amazonaws.com"},
			regionOverride: "",
			expected:       "us-east-1",
		},
		{
			name:           "non-AWS host fallback",
			u:              url.URL{Host: "storage.googleapis.com"},
			regionOverride: "",
			expected:       "us-east-1",
		},
		{
			name:           "IP address fallback",
			u:              url.URL{Host: "192.168.1.1"},
			regionOverride: "",
			expected:       "us-east-1",
		},
		{
			name:           "localhost fallback",
			u:              url.URL{Host: "localhost:9000"},
			regionOverride: "",
			expected:       "us-east-1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetBucketLocation(tc.u, tc.regionOverride)
			assert.Equal(t, tc.expected, result)
		})
	}
}
