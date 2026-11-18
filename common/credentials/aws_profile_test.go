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
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// These expectations were checked against the AWS SDK C++ 1.11.921 config cache
// used by milvus-storage, including cases where a parser changes source selection.
func TestParseAWSIAMProfile(t *testing.T) {
	tests := []struct {
		name, profile, contents string
		expected                map[string]string
	}{
		{"default", "default", "[default]\nweb_identity_token_file=/token\n", map[string]string{"web_identity_token_file": "/token"}},
		{"prefixed-default", "default", "[profile default]\nweb_identity_token_file=/token\n", map[string]string{"web_identity_token_file": "/token"}},
		{"named", "named", "[profile named]\nweb_identity_token_file=/token\nrole_arn=role\nrole_session_name=session\nregion=us-east-1\n", map[string]string{"web_identity_token_file": "/token", "role_arn": "role", "role_session_name": "session", "region": "us-east-1"}},
		{"unprefixed-name-ignored", "named", "[named]\nweb_identity_token_file=/token\n", nil},
		{"missing-profile", "absent", "[default]\nweb_identity_token_file=/token\n", nil},
		{"invalid-selected-section-discarded", "named", "[profile named]\nweb_identity_token_file=/token\nBROKEN LINE\nrole_arn=role\n", nil},
		{"later-invalid-section-keeps-completed-profile", "named", "[profile named]\nweb_identity_token_file=/token\n[profile other]\nBROKEN LINE\n", map[string]string{"web_identity_token_file": "/token"}},
		{"invalid-line-stops-subsequent-profiles", "named", "BROKEN LINE\n[profile named]\nweb_identity_token_file=/token\n", nil},
		{"unknown-section-ignored", "named", "[unknown]\nBROKEN LINE\n[profile named]\nweb_identity_token_file=/token\n", map[string]string{"web_identity_token_file": "/token"}},
		{"default-alias-last-token-wins", "default", "[default]\nweb_identity_token_file=/first\nrole_arn=role\nregion=us-east-1\n[profile default]\nweb_identity_token_file=/second\n", map[string]string{"web_identity_token_file": "/second", "role_arn": "role", "region": "us-east-1"}},
		{"duplicate-profile-clears-raw-values", "named", "[profile named]\nweb_identity_token_file=/first\nrole_session_name=session\nrole_arn=role\n[profile named]\nregion=us-east-1\n", map[string]string{"role_arn": "role", "region": "us-east-1"}},
		{"quoted-token-preserved", "named", "[profile named]\nweb_identity_token_file=\"/token\"\n", map[string]string{"web_identity_token_file": "\"/token\""}},
		{"empty-quotes-still-configure-irsa", "named", "[profile named]\nweb_identity_token_file=\"\"\n", map[string]string{"web_identity_token_file": "\"\""}},
		{"inline-comments-and-crlf", "named", "  [profile named]# header comment\r\nweb_identity_token_file = /token # comment\r\nrole_arn=role;literal\r\n", map[string]string{"web_identity_token_file": "/token", "role_arn": "role;literal"}},
		{"static-and-process-credentials-ignored", "named", "[profile named]\naws_access_key_id=STATIC\naws_secret_access_key=STATIC\ncredential_process=unused\n", map[string]string{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, parseAWSIAMProfile(test.contents, test.profile))
		})
	}
}

func TestLoadAWSIAMProfile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(file, []byte("[default]\nweb_identity_token_file=/default\n[profile named]\nweb_identity_token_file=/named\n[profile preferred]\nweb_identity_token_file=/preferred\n"), 0o600))
	t.Setenv("AWS_CONFIG_FILE", file)
	t.Setenv("AWS_PROFILE", "named")
	t.Setenv("AWS_DEFAULT_PROFILE", "preferred")
	profile := loadAWSIAMProfile()
	require.Equal(t, "/preferred", profile["web_identity_token_file"])

	t.Setenv("AWS_DEFAULT_PROFILE", "")
	profile = loadAWSIAMProfile()
	require.Equal(t, "/named", profile["web_identity_token_file"])

	t.Setenv("AWS_PROFILE", "")
	profile = loadAWSIAMProfile()
	require.Equal(t, "/default", profile["web_identity_token_file"])

	t.Setenv("AWS_PROFILE", "absent")
	profile = loadAWSIAMProfile()
	require.Empty(t, profile)

	t.Setenv("AWS_CONFIG_FILE", file+".missing")
	profile = loadAWSIAMProfile()
	require.Empty(t, profile)
}

func TestLoadAWSIAMProfileDefaultFile(t *testing.T) {
	home := t.TempDir()
	if runtime.GOOS == "windows" {
		t.Setenv("USERPROFILE", home)
	} else {
		t.Setenv("HOME", home)
	}
	t.Setenv("AWS_CONFIG_FILE", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	require.NoError(t, os.Mkdir(filepath.Join(home, ".aws"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(home, ".aws", "config"), []byte("[default]\nweb_identity_token_file=/home-token\n"), 0o600))
	profile := loadAWSIAMProfile()
	require.Equal(t, "/home-token", profile["web_identity_token_file"])
}
