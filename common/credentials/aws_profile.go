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
	"regexp"
	"strings"
)

var awsIAMProfileSection = regexp.MustCompile(`^\[[\t ]*(?:profile[\t ]+([%+\-./0-9:@A-Z_a-z]+)|(default))[\t ]*\][\t ]*(?:[#;].*)?$`)

func loadAWSIAMProfile() map[string]string {
	filename := os.Getenv("AWS_CONFIG_FILE")
	if filename == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil
		}
		filename = filepath.Join(home, ".aws", "config")
	}
	contents, err := os.ReadFile(filename) // #nosec G703 -- AWS_CONFIG_FILE intentionally selects an operator-provided file.
	if err != nil {
		// The C++ SDK has no cached config profile when the file cannot be read.
		return nil
	}
	// Match AWS SDK C++ GetConfigProfileName, including DEFAULT_PROFILE first.
	name := os.Getenv("AWS_DEFAULT_PROFILE")
	if name == "" {
		name = os.Getenv("AWS_PROFILE")
	}
	if name == "" {
		name = "default"
	}
	return parseAWSIAMProfile(string(contents), name)
}

// parseAWSIAMProfile reads only the four Web Identity settings. It preserves the
// SDK's profile-section commit behavior: an invalid property line discards the
// active profile and stops parsing, without erasing previously completed ones.
// It does not implement static credentials, SSO, or credential_process providers.
func parseAWSIAMProfile(contents, name string) map[string]string {
	var selected, values map[string]string
	section := ""
	unknown := false
	commit := func() {
		if section != name {
			return
		}
		// The SDK replaces raw token/session values on duplicate sections, while
		// its typed role and region fields retain values omitted by a later one.
		for _, key := range []string{"role_arn", "region"} {
			if _, present := values[key]; !present {
				if previous, existed := selected[key]; existed {
					values[key] = previous
				}
			}
		}
		selected = values
	}
	for _, raw := range strings.Split(contents, "\n") {
		line := strings.TrimSuffix(raw, "\r")
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		for _, comment := range []string{" #", " ;", "\t#", "\t;"} {
			if at := strings.Index(line, comment); at >= 0 {
				line = line[:at]
			}
		}
		if len(line) < 3 || strings.TrimSpace(line) == "" {
			continue
		}
		if strings.Contains(line, "[") && strings.Contains(line, "]") {
			commit()
			section, values, unknown = "", nil, true
			if match := awsIAMProfileSection.FindStringSubmatch(strings.TrimSpace(line)); match != nil {
				section, values, unknown = match[1]+match[2], make(map[string]string, 4), false
			}
			continue
		}
		if unknown {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || section == "" {
			return selected
		}
		switch key = strings.TrimSpace(key); key {
		case "web_identity_token_file", "role_arn", "role_session_name", "region":
			// Quotes are literal in AWS SDK C++ config values, including "".
			values[key] = strings.TrimSpace(value)
		}
	}
	commit()
	return selected
}
