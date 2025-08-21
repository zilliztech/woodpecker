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

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		hasError bool
	}{
		{"256M", 256 * 1024 * 1024, false},
		{"256MB", 256 * 1024 * 1024, false},
		{"2G", 2 * 1024 * 1024 * 1024, false},
		{"2GB", 2 * 1024 * 1024 * 1024, false},
		{"1024K", 1024 * 1024, false},
		{"1024KB", 1024 * 1024, false},
		{"1T", 1024 * 1024 * 1024 * 1024, false},
		{"1TB", 1024 * 1024 * 1024 * 1024, false},
		{"1024", 1024, false},
		{"", 0, false},
		{"256m", 256 * 1024 * 1024, false},  // lowercase
		{"256mb", 256 * 1024 * 1024, false}, // lowercase with b
		{"invalid", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseSize(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestByteSizeUnmarshal(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		expected int64
		hasError bool
	}{
		{"string with MB unit", "size: 100MB", 100 * 1024 * 1024, false},
		{"string with GB unit", "size: 2GB", 2 * 1024 * 1024 * 1024, false},
		{"string with KB unit", "size: 512KB", 512 * 1024, false},
		{"plain number", "size: 1024", 1024, false},
		{"integer value", "size: 2048", 2048, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config struct {
				Size ByteSize `yaml:"size"`
			}
			err := yaml.Unmarshal([]byte(tt.yaml), &config)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, config.Size.Int64())
			}
		})
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		defaultUnit time.Duration
		expectedSec int
		expectedMs  int
		hasError    bool
	}{
		{"10s", "10s", time.Second, 10, 10000, false},
		{"5m", "5m", time.Second, 300, 300000, false},
		{"1h", "1h", time.Second, 3600, 3600000, false},
		{"500ms", "500ms", time.Millisecond, 0, 500, false},
		{"1500ms", "1500ms", time.Millisecond, 1, 1500, false},
		{"2h30m", "2h30m", time.Second, 9000, 9000000, false},
		{"plain number as seconds", "60", time.Second, 60, 60000, false},
		{"plain number as milliseconds", "200", time.Millisecond, 0, 200, false},
		{"empty string", "", time.Second, 0, 0, false},
		{"invalid", "invalid", time.Second, 0, 0, true},
		{"invalid unit", "10x", time.Second, 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration, err := parseDuration(tt.input, tt.defaultUnit)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedSec, int(duration.Seconds()))
				assert.Equal(t, tt.expectedMs, int(duration.Milliseconds()))
			}
		})
	}
}

func TestDurationSecondsUnmarshal(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		expectedSec int
		expectedMs  int
		hasError    bool
	}{
		{"string with s unit", "duration: 10s", 10, 10000, false},
		{"string with ms unit", "duration: 500ms", 0, 500, false},
		{"string with m unit", "duration: 2m", 120, 120000, false},
		{"plain number (default seconds)", "duration: 30", 30, 30000, false},
		{"integer value (default seconds)", "duration: 60", 60, 60000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config struct {
				Duration DurationSeconds `yaml:"duration"`
			}
			err := yaml.Unmarshal([]byte(tt.yaml), &config)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedSec, config.Duration.Seconds())
				assert.Equal(t, tt.expectedMs, config.Duration.Milliseconds())
			}
		})
	}
}

func TestDurationMillisecondsUnmarshal(t *testing.T) {
	tests := []struct {
		name       string
		yaml       string
		expectedMs int
		hasError   bool
	}{
		{"string with ms unit", "duration: 500ms", 500, false},
		{"string with s unit", "duration: 2s", 2000, false},
		{"plain number (default milliseconds)", "duration: 300", 300, false},
		{"integer value (default milliseconds)", "duration: 1000", 1000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config struct {
				Duration DurationMilliseconds `yaml:"duration"`
			}
			err := yaml.Unmarshal([]byte(tt.yaml), &config)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMs, config.Duration.Milliseconds())
			}
		})
	}
}
