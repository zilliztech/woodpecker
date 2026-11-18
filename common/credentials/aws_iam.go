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

// Package credentials supplies the AWS IAM policy shared by Woodpecker and its callers.
package credentials

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	miniocredentials "github.com/minio/minio-go/v7/pkg/credentials"
)

// ErrEC2MetadataDisabled indicates that no workload identity was configured and
// the caller explicitly disabled the remaining EC2 credential source.
var ErrEC2MetadataDisabled = errors.New("AWS EC2 metadata credentials are disabled by AWS_EC2_METADATA_DISABLED")

// NewAWSIAM selects the ordinary AWS IAM source shared with milvus-storage:
// configured Web Identity (environment or AWS config profile), then Container,
// then EC2 unless AWS_EC2_METADATA_DISABLED=true. Static access keys are ignored.
// Retrieval and refresh stay with minio-go; a configured source never falls back
// to a different identity after an error. Configure the process environment
// before creating the client. Token file contents can rotate afterwards.
func NewAWSIAM() *miniocredentials.Credentials {
	return NewAWSIAMWithEndpoint("")
}

// NewAWSIAMWithEndpoint preserves the legacy custom IAM endpoint option while
// applying the same source selection as NewAWSIAM.
func NewAWSIAMWithEndpoint(endpoint string) *miniocredentials.Credentials {
	return newAWSIAM(endpoint, &http.Client{Transport: http.DefaultTransport})
}

func newAWSIAM(endpoint string, client *http.Client) *miniocredentials.Credentials {
	profile := loadAWSIAMProfile()
	iam := &miniocredentials.IAM{Client: client, Endpoint: endpoint}
	iam.EKSIdentity.TokenFile = profile["web_identity_token_file"]
	iam.EKSIdentity.RoleARN = profile["role_arn"]
	iam.EKSIdentity.RoleSessionName = profile["role_session_name"]
	// Retain minio-go's AWS_REGION precedence. Supply the SDK/profile fallback
	// without altering the process environment or the client's S3 region.
	iam.Region = os.Getenv("AWS_DEFAULT_REGION")
	if iam.Region == "" {
		iam.Region = profile["region"]
	}

	keys := []string{"AWS_WEB_IDENTITY_TOKEN_FILE"}
	useEC2 := false
	switch {
	case os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != "" || iam.EKSIdentity.TokenFile != "":
		keys = append(keys, "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_REGION")
	case os.Getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") != "":
		keys = append(keys, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN")
	case os.Getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI") != "":
		keys = append(keys, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
			"AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
	default:
		useEC2 = true
		keys = append(keys, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI")
	}
	environment := make(map[string]string, len(keys))
	for _, key := range keys {
		environment[key] = os.Getenv(key)
	}
	return miniocredentials.New(&awsIAM{IAM: iam, environment: environment, useEC2: useEC2})
}

type awsIAM struct {
	*miniocredentials.IAM
	environment map[string]string
	useEC2      bool
}

func (p *awsIAM) Retrieve() (miniocredentials.Value, error) {
	if p.useEC2 && strings.EqualFold(os.Getenv("AWS_EC2_METADATA_DISABLED"), "true") {
		return miniocredentials.Value{}, ErrEC2MetadataDisabled
	}
	// minio-go re-reads environment variables at each refresh. Reject changes
	// that could switch its selected identity instead of silently changing roles.
	// This does not freeze the contents of projected token files.
	for key, value := range p.environment {
		if os.Getenv(key) != value {
			return miniocredentials.Value{}, fmt.Errorf("AWS IAM configuration changed after client creation (%s); recreate the client", key)
		}
	}
	return p.IAM.Retrieve()
}
