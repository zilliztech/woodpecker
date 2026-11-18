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

package credentials

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func cleanAWSEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_DEFAULT_PROFILE",
		"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "AWS_EC2_METADATA_DISABLED",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("AWS_CONFIG_FILE", writeTestFile(t, "config", ""))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", writeTestFile(t, "credentials", "[default]\naws_access_key_id=IGNORED_FILE_AK\naws_secret_access_key=IGNORED_FILE_SK\n"))
}

func writeTestFile(t *testing.T, name, data string) string {
	t.Helper()
	filename := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(filename, []byte(data), 0o600))
	return filename
}

func response(req *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: req}
}

func credentialsJSON(accessKey string) string {
	return fmt.Sprintf(`{"Code":"Success","AccessKeyId":%q,"SecretAccessKey":"SYNTHETIC_SECRET","Token":"SYNTHETIC_SESSION","Expiration":%q}`,
		accessKey, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
}

func stsXML(accessKey string) string {
	return fmt.Sprintf(`<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>%s</AccessKeyId><SecretAccessKey>SYNTHETIC_SECRET</SecretAccessKey><SessionToken>SYNTHETIC_SESSION</SessionToken><Expiration>%s</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`, accessKey, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
}

// The transport never delegates to the network. Every unexpected identity source
// fails the test, including a fallback that might otherwise return valid keys.
func iamClient(t *testing.T, wantSource string, verify func(*http.Request)) (*http.Client, *[]string) {
	t.Helper()
	calls := []string{}
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.Method+" "+req.URL.String())
		if verify != nil {
			verify(req)
		}
		switch wantSource {
		case "irsa":
			require.Equal(t, http.MethodPost, req.Method)
			require.True(t, strings.HasPrefix(req.URL.Host, "sts."), req.URL.String())
			return response(req, http.StatusOK, stsXML("IRSA_KEY")), nil
		case "container":
			require.Equal(t, http.MethodGet, req.Method)
			require.Equal(t, "/container", req.URL.Path)
			return response(req, http.StatusOK, credentialsJSON("CONTAINER_KEY")), nil
		case "ec2":
			require.Equal(t, "169.254.169.254", req.URL.Host)
			switch req.URL.Path {
			case "/latest/api/token":
				require.Equal(t, http.MethodPut, req.Method)
				return response(req, http.StatusOK, "SYNTHETIC_IMDS_TOKEN"), nil
			case "/latest/meta-data/iam/security-credentials/":
				return response(req, http.StatusOK, "test-node-role"), nil
			case "/latest/meta-data/iam/security-credentials/test-node-role":
				return response(req, http.StatusOK, credentialsJSON("NODE_KEY")), nil
			}
		}
		t.Fatalf("unexpected credential request: %s %s", req.Method, req.URL)
		return nil, fmt.Errorf("unexpected credential request")
	})}, &calls
}

func TestAWSIAMSourceSelection(t *testing.T) {
	cases := []struct {
		name, source, disabled string
		irsa, relative, full   bool
	}{
		{"irsa_before_both_containers", "irsa", "", true, true, true},
		{"irsa_with_imds_disabled", "irsa", "TRUE", true, true, true},
		{"ecs_before_full_uri", "container", "", false, true, true},
		{"container_with_imds_disabled", "container", "TrUe", false, false, true},
		{"node_default", "ec2", "", false, false, false},
		{"node_explicit_false", "ec2", "false", false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cleanAWSEnv(t)
			t.Setenv("AWS_ACCESS_KEY_ID", "IGNORED_ENV_AK")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "IGNORED_ENV_SK")
			t.Setenv("AWS_EC2_METADATA_DISABLED", tc.disabled)
			if tc.irsa {
				t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", writeTestFile(t, "token", "irsa-token"))
				t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test")
			}
			if tc.relative {
				t.Setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/container")
			}
			if tc.full {
				t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1/container")
			}
			client, calls := iamClient(t, tc.source, nil)
			creds := newAWSIAM("", client)
			value, err := creds.Get()
			require.NoError(t, err)
			require.Equal(t, map[string]string{"irsa": "IRSA_KEY", "container": "CONTAINER_KEY", "ec2": "NODE_KEY"}[tc.source], value.AccessKeyID)
			require.NotEmpty(t, *calls)
		})
	}
}

func TestAWSIAMProfileResolution(t *testing.T) {
	cases := []struct {
		name, profile, defaultProfile, section string
		envToken, envRole, envSession          bool
	}{
		{"profile_only", "workload", "", "profile workload", false, false, false},
		{"env_token_profile_role", "workload", "", "profile workload", true, false, false},
		{"profile_token_env_role", "workload", "", "profile workload", false, true, true},
		{"default_profile_precedes_profile", "other", "workload", "profile workload", false, false, false},
		{"default_section", "", "", "default", false, false, false},
		{"profile_default_section", "", "", "profile default", false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cleanAWSEnv(t)
			profileToken := writeTestFile(t, "profile-token", "profile-token-content")
			config := fmt.Sprintf("[%s]\nweb_identity_token_file = %s\nrole_arn = arn:aws:iam::123456789012:role/profile\nrole_session_name = profile-session\nregion = us-west-2\n", tc.section, profileToken)
			t.Setenv("AWS_CONFIG_FILE", writeTestFile(t, "config", config))
			t.Setenv("AWS_PROFILE", tc.profile)
			t.Setenv("AWS_DEFAULT_PROFILE", tc.defaultProfile)
			t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1/container")
			wantToken, wantRole, wantSession := "profile-token-content", "arn:aws:iam::123456789012:role/profile", "profile-session"
			if tc.envToken {
				wantToken = "environment-token-content"
				t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", writeTestFile(t, "env-token", wantToken))
			}
			if tc.envRole {
				wantRole = "arn:aws:iam::123456789012:role/environment"
				t.Setenv("AWS_ROLE_ARN", wantRole)
			}
			if tc.envSession {
				wantSession = "environment-session"
				t.Setenv("AWS_ROLE_SESSION_NAME", wantSession)
			}
			client, _ := iamClient(t, "irsa", func(req *http.Request) {
				require.NoError(t, req.ParseForm())
				require.Equal(t, wantToken, req.Form.Get("WebIdentityToken"))
				require.Equal(t, wantRole, req.Form.Get("RoleArn"))
				require.Equal(t, wantSession, req.Form.Get("RoleSessionName"))
				require.Equal(t, "sts.us-west-2.amazonaws.com", req.URL.Host)
			})
			creds := newAWSIAM("", client)
			value, err := creds.Get()
			require.NoError(t, err)
			require.Equal(t, "IRSA_KEY", value.AccessKeyID)
		})
	}
}

func TestAWSIAMProfileAbsenceAndStaticKeys(t *testing.T) {
	for _, configText := range []string{
		"[default]\naws_access_key_id=IGNORED_CONFIG_AK\naws_secret_access_key=IGNORED_CONFIG_SK\n",
		"[workload]\nweb_identity_token_file=/not-a-valid-named-profile\n",
		"[profile other]\nweb_identity_token_file=/not-the-selected-profile\n",
	} {
		t.Run(configText[:strings.IndexByte(configText, '\n')], func(t *testing.T) {
			cleanAWSEnv(t)
			t.Setenv("AWS_CONFIG_FILE", writeTestFile(t, "config", configText))
			t.Setenv("AWS_PROFILE", "workload")
			t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1/container")
			client, _ := iamClient(t, "container", nil)
			creds := newAWSIAM("", client)
			value, err := creds.Get()
			require.NoError(t, err)
			require.Equal(t, "CONTAINER_KEY", value.AccessKeyID)
		})
	}
	t.Run("missing_file", func(t *testing.T) {
		cleanAWSEnv(t)
		t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "missing"))
		t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
		creds := NewAWSIAM()
		_, err := creds.Get()
		require.ErrorIs(t, err, ErrEC2MetadataDisabled)
	})
}

func TestAWSIAMDefaultProfileAliasesAndQuotes(t *testing.T) {
	for _, sections := range [][2]string{{"default", "profile default"}, {"profile default", "default"}} {
		t.Run(sections[0], func(t *testing.T) {
			cleanAWSEnv(t)
			config := fmt.Sprintf("[%s]\nweb_identity_token_file=/first-profile\n[%s]\nrole_arn=arn:aws:iam::123456789012:role/second\n", sections[0], sections[1])
			t.Setenv("AWS_CONFIG_FILE", writeTestFile(t, "config", config))
			t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
			creds := NewAWSIAM()
			_, err := creds.Get()
			require.ErrorIs(t, err, ErrEC2MetadataDisabled)
		})
	}
	t.Run("quotes_are_literal_token_path", func(t *testing.T) {
		cleanAWSEnv(t)
		t.Setenv("AWS_CONFIG_FILE", writeTestFile(t, "config", "[default]\nweb_identity_token_file=\"\"\nrole_arn=synthetic-role\n"))
		t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1/container")
		client, calls := iamClient(t, "none", nil)
		creds := newAWSIAM("", client)
		_, err := creds.Get()
		require.Error(t, err)
		require.Empty(t, *calls)
	})
}

func TestAWSIAMDisabledAndEnvironmentChanges(t *testing.T) {
	for _, disabled := range []string{"true", "TRUE", "TrUe"} {
		t.Run(disabled, func(t *testing.T) {
			cleanAWSEnv(t)
			t.Setenv("AWS_EC2_METADATA_DISABLED", disabled)
			client, calls := iamClient(t, "none", nil)
			creds := newAWSIAM("", client)
			_, err := creds.Get()
			require.ErrorIs(t, err, ErrEC2MetadataDisabled)
			require.Empty(t, *calls)
		})
	}
	t.Run("disable_before_refresh", func(t *testing.T) {
		cleanAWSEnv(t)
		client, calls := iamClient(t, "ec2", nil)
		creds := newAWSIAM("", client)
		_, err := creds.Get()
		require.NoError(t, err)
		before := len(*calls)
		t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
		creds.Expire()
		_, err = creds.Get()
		require.ErrorIs(t, err, ErrEC2MetadataDisabled)
		require.Len(t, *calls, before)
	})
	t.Run("container_removed_before_refresh", func(t *testing.T) {
		cleanAWSEnv(t)
		t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1/container")
		client, calls := iamClient(t, "container", nil)
		creds := newAWSIAM("", client)
		_, err := creds.Get()
		require.NoError(t, err)
		t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "")
		creds.Expire()
		_, err = creds.Get()
		require.ErrorContains(t, err, "configuration changed")
		require.Len(t, *calls, 1)
	})
}

func TestAWSIAMFailuresNeverFallback(t *testing.T) {
	for _, source := range []string{"unreadable_irsa", "sts_error", "container_error", "invalid_container"} {
		t.Run(source, func(t *testing.T) {
			cleanAWSEnv(t)
			t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1/container")
			calls := 0
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				if source == "sts_error" {
					require.Equal(t, http.MethodPost, req.Method)
					return response(req, http.StatusForbidden, `<ErrorResponse><Error><Code>AccessDenied</Code><Message>synthetic</Message></Error></ErrorResponse>`), nil
				}
				require.Equal(t, "container_error", source)
				require.Equal(t, "/container", req.URL.Path)
				return response(req, http.StatusInternalServerError, "synthetic failure"), nil
			})}
			if source == "unreadable_irsa" || source == "sts_error" {
				path := filepath.Join(t.TempDir(), "missing-token")
				if source == "sts_error" {
					path = writeTestFile(t, "token", "synthetic-token")
				}
				t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", path)
				t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test")
			}
			if source == "invalid_container" {
				t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://192.0.2.1/container")
			}
			creds := newAWSIAM("", client)
			_, err := creds.Get()
			require.Error(t, err)
			if source == "unreadable_irsa" || source == "invalid_container" {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls)
			}
		})
	}
}

func TestAWSIAMTokenFileRotation(t *testing.T) {
	for _, source := range []string{"irsa", "container"} {
		t.Run(source, func(t *testing.T) {
			cleanAWSEnv(t)
			tokenPath := writeTestFile(t, "token", "first-token")
			if source == "irsa" {
				t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tokenPath)
				t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test")
			} else {
				t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1/container")
				t.Setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", tokenPath)
			}
			tokens := []string{}
			client, calls := iamClient(t, source, func(req *http.Request) {
				if source == "irsa" {
					require.NoError(t, req.ParseForm())
					tokens = append(tokens, req.Form.Get("WebIdentityToken"))
				} else {
					tokens = append(tokens, req.Header.Get("Authorization"))
				}
			})
			creds := newAWSIAM("", client)
			_, err := creds.Get()
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(tokenPath, []byte("rotated-token"), 0o600))
			_, err = creds.Get()
			require.NoError(t, err)
			require.Len(t, *calls, 1, "valid credentials should remain cached")
			creds.Expire()
			_, err = creds.Get()
			require.NoError(t, err)
			require.Equal(t, []string{"first-token", "rotated-token"}, tokens)
		})
	}
}
