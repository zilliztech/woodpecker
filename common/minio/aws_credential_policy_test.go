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

package minio

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	minioclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	awscredentials "github.com/zilliztech/woodpecker/common/credentials"
)

type awsPolicyTransport func(*http.Request) (*http.Response, error)

func (f awsPolicyTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestMinioAWSIAMCallerPolicy(t *testing.T) {
	factories := []struct {
		name   string
		create func(context.Context, *config.Configuration) (*minioclient.Client, error)
	}{
		{"ordinary", newMinioClient},
		{"deprecated", newMinioClientFromConfig},
	}
	for _, factory := range factories {
		for _, scenario := range []string{"profile_irsa", "imds_disabled", "explicit_keys"} {
			t.Run(factory.name+"/"+scenario, func(t *testing.T) {
				for _, key := range []string{
					"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_DEFAULT_PROFILE",
					"AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_REGION", "AWS_DEFAULT_REGION",
					"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
				} {
					t.Setenv(key, "")
				}
				t.Setenv("AWS_EC2_METADATA_DISABLED", "TrUe")
				t.Setenv("AWS_ACCESS_KEY_ID", "IGNORED_ENV_KEY")
				t.Setenv("AWS_SECRET_ACCESS_KEY", "IGNORED_ENV_SECRET")
				configPath := filepath.Join(t.TempDir(), "aws-config")
				require.NoError(t, os.WriteFile(configPath, nil, 0o600))
				t.Setenv("AWS_CONFIG_FILE", configPath)
				t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "missing-credentials"))
				if scenario == "profile_irsa" {
					token := filepath.Join(t.TempDir(), "token")
					require.NoError(t, os.WriteFile(token, []byte("SYNTHETIC_WEB_TOKEN"), 0o600))
					require.NoError(t, os.WriteFile(configPath, []byte(fmt.Sprintf("[profile test]\nweb_identity_token_file=%s\nrole_arn=arn:aws:iam::123456789012:role/test\nregion=us-west-2\n", token)), 0o600))
					t.Setenv("AWS_PROFILE", "test")
					// A conflicting container source must not replace profile IRSA.
					t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1:1/container")
				}
				var s3Calls, stsCalls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					s3Calls.Add(1)
					wantKey := "IRSA_KEY"
					if scenario == "explicit_keys" {
						wantKey = "CONFIG_KEY"
					}
					require.Contains(t, r.Header.Get("Authorization"), "Credential="+wantKey+"/")
					if r.URL.Query().Has("location") {
						_, _ = io.WriteString(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-west-2</LocationConstraint>`)
						return
					}
					w.WriteHeader(http.StatusOK)
				}))
				defer server.Close()
				transport := http.DefaultTransport
				http.DefaultTransport = awsPolicyTransport(func(r *http.Request) (*http.Response, error) {
					if strings.HasPrefix(r.URL.Host, "sts.") {
						stsCalls.Add(1)
						require.Equal(t, "profile_irsa", scenario)
						require.NoError(t, r.ParseForm())
						require.Equal(t, "SYNTHETIC_WEB_TOKEN", r.Form.Get("WebIdentityToken"))
						body := fmt.Sprintf(`<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>IRSA_KEY</AccessKeyId><SecretAccessKey>SYNTHETIC_SECRET</SecretAccessKey><SessionToken>SYNTHETIC_SESSION</SessionToken><Expiration>%s</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
						return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
					}
					require.True(t, strings.HasPrefix(r.URL.String(), server.URL+"/"), "unexpected credential source: %s", r.URL)
					return transport.RoundTrip(r)
				})
				t.Cleanup(func() { http.DefaultTransport = transport })
				host, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
				require.NoError(t, err)
				portNumber, err := strconv.Atoi(port)
				require.NoError(t, err)
				cfg, err := config.NewConfiguration()
				require.NoError(t, err)
				cfg.Minio.Address = host
				cfg.Minio.Port = portNumber
				cfg.Minio.CloudProvider = CloudProviderAWS
				cfg.Minio.UseIAM = scenario != "explicit_keys"
				cfg.Minio.AccessKeyID = "CONFIG_KEY"
				cfg.Minio.SecretAccessKey = "CONFIG_SECRET"
				cfg.Minio.Region = "us-west-2"
				cfg.Minio.UseSSL = false
				cfg.Minio.CreateBucket = false
				client, err := factory.create(context.Background(), cfg)
				require.NoError(t, err)
				_, err = client.BucketExists(context.Background(), "test-bucket")
				if scenario == "imds_disabled" {
					require.ErrorIs(t, err, awscredentials.ErrEC2MetadataDisabled)
					require.Zero(t, stsCalls.Load())
					require.Zero(t, s3Calls.Load())
				} else {
					require.NoError(t, err)
					require.Positive(t, s3Calls.Load())
					if scenario == "profile_irsa" {
						require.Equal(t, int32(1), stsCalls.Load())
					} else {
						require.Zero(t, stsCalls.Load())
					}
				}
			})
		}
	}
}
