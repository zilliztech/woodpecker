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

	"github.com/minio/minio-go/v7/pkg/s3utils"
)

// GetBucketLocation gets the bucket location from request url or regionOverride
func GetBucketLocation(u url.URL, regionOverride string) string {
	if regionOverride != "" {
		return regionOverride
	}
	region := s3utils.GetRegionFromURL(u)
	if region == "" {
		region = "us-east-1"
	}
	return region
}
