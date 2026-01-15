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

package external

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
)

func TestInitLocalLoonFileSystem(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "loon_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Test local filesystem initialization
	err = InitLocalLoonFileSystem(tempDir)
	assert.NoError(t, err, "Failed to initialize local Loon filesystem")

	// Verify we can get the singleton handle
	handle, err := GetFileSystemSingletonHandle()
	assert.NoError(t, err, "Failed to get filesystem singleton handle")
	assert.NotEqual(t, 0, handle, "Filesystem handle should not be zero")
}

func TestInitRemoteLoonFileSystem(t *testing.T) {
	// Load configuration
	configPath := "../../config/woodpecker.yaml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skip("Config file not found, skipping remote filesystem test")
		return
	}

	cfg, err := config.NewConfiguration(configPath)
	require.NoError(t, err)

	// Configure for MinIO
	cfg.Woodpecker.Storage.Type = "minio"
	cfg.Minio.BucketName = "a-bucket"

	// Try to initialize remote filesystem
	err = InitRemoteLoonFileSystem(cfg)
	assert.NoError(t, err, "Failed to initialize remote Loon filesystem")

	// Verify we can get the singleton handle
	handle, err := GetFileSystemSingletonHandle()
	assert.NoError(t, err, "Failed to get filesystem singleton handle")
	assert.NotEqual(t, 0, handle, "Filesystem handle should not be zero")
}

func TestInitStorageV2FileSystem(t *testing.T) {
	// Create a temporary directory for local testing
	tempDir, err := os.MkdirTemp("", "loon_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Load configuration
	configPath := "../../config/woodpecker.yaml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skip("Config file not found, skipping test")
		return
	}

	cfg, err := config.NewConfiguration(configPath)
	require.NoError(t, err)

	t.Run("InitLocalStorage", func(t *testing.T) {
		// Configure for local storage
		cfg.Woodpecker.Storage.Type = "local"
		cfg.Woodpecker.Storage.RootPath = tempDir

		err := InitStorageV2FileSystem(cfg)
		assert.NoError(t, err, "Failed to initialize local storage through unified API")

		// Verify handle
		handle, err := GetFileSystemSingletonHandle()
		assert.NoError(t, err, "Failed to get filesystem handle")
		assert.NotEqual(t, 0, handle, "Handle should not be zero")
	})

	t.Run("InitRemoteStorage", func(t *testing.T) {
		// Configure for remote storage (MinIO)
		cfg.Woodpecker.Storage.Type = "minio"
		cfg.Minio.BucketName = "a-bucket"

		err := InitStorageV2FileSystem(cfg)
		assert.NoError(t, err, "Failed to initialize remote storage")

		// Verify handle
		handle, err := GetFileSystemSingletonHandle()
		assert.NoError(t, err, "Failed to get filesystem handle")
		assert.NotEqual(t, 0, handle, "Handle should not be zero")
	})
}

func TestBuildLoonProperties(t *testing.T) {
	t.Run("EmptyProperties", func(t *testing.T) {
		props := buildLoonProperties([]string{}, []string{})
		assert.NotNil(t, props, "Should return non-nil properties")
		assert.Equal(t, 0, int(props.count), "Count should be zero")
	})

	t.Run("ValidProperties", func(t *testing.T) {
		keys := []string{"key1", "key2", "key3"}
		values := []string{"value1", "value2", "value3"}

		props := buildLoonProperties(keys, values)
		assert.NotNil(t, props, "Should return non-nil properties")
		assert.Equal(t, len(keys), int(props.count), "Count should match")

		// Clean up
		freeLoonProperties(props)
	})

	t.Run("MismatchedKeysValues", func(t *testing.T) {
		keys := []string{"key1", "key2"}
		values := []string{"value1"}

		props := buildLoonProperties(keys, values)
		assert.Nil(t, props, "Should return nil for mismatched arrays")
	})
}

func TestBuildLocalProperties(t *testing.T) {
	testPath := "/tmp/test"
	props := buildLocalProperties(testPath)

	assert.NotNil(t, props, "Should return non-nil properties")
	assert.Greater(t, int(props.count), 0, "Should have properties")

	// Clean up
	freeLoonProperties(props)
}

func TestBuildRemoteProperties(t *testing.T) {
	cfg := &config.Configuration{
		Minio: config.MinioConfig{
			Address:          "localhost",
			Port:             9000,
			AccessKeyID:      "minioadmin",
			SecretAccessKey:  "minioadmin",
			BucketName:       "test-bucket",
			RootPath:         "test-root",
			UseSSL:           false,
			CloudProvider:    "aws",
			LogLevel:         "info",
			Region:           "us-east-1",
			UseVirtualHost:   false,
			RequestTimeoutMs: 1000,
		},
	}

	props := buildRemoteProperties(cfg)
	assert.NotNil(t, props, "Should return non-nil properties")
	assert.Greater(t, int(props.count), 0, "Should have properties")

	// Clean up
	freeLoonProperties(props)
}

func TestPropertyKeys(t *testing.T) {
	// Test that our Go variables (initialized from C functions at package load time)
	// match the expected property key format defined in the C library.
	// These values are dynamically fetched from loon_property_fs_*() functions.
	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{"StorageType", PropertyFsStorageType, "fs.storage_type"},
		{"RootPath", PropertyFsRootPath, "fs.root_path"},
		{"Address", PropertyFsAddress, "fs.address"},
		{"BucketName", PropertyFsBucketName, "fs.bucket_name"},
		{"Region", PropertyFsRegion, "fs.region"},
		{"AccessKeyID", PropertyFsAccessKeyID, "fs.access_key_id"},
		{"AccessKeyValue", PropertyFsAccessKeyValue, "fs.access_key_value"},
		{"UseIAM", PropertyFsUseIAM, "fs.use_iam"},
		{"IamEndpoint", PropertyFsIamEndpoint, "fs.iam_endpoint"},
		{"GcpCredentialJSON", PropertyFsGcpCredentialJSON, "fs.gcp_credential_json"},
		{"UseSSL", PropertyFsUseSSL, "fs.use_ssl"},
		{"SSLCACert", PropertyFsSSLCACert, "fs.ssl_ca_cert"},
		{"UseVirtualHost", PropertyFsUseVirtualHost, "fs.use_virtual_host"},
		{"RequestTimeoutMs", PropertyFsRequestTimeoutMs, "fs.request_timeout_ms"},
		{"LogLevel", PropertyFsLogLevel, "fs.log_level"},
		{"CloudProvider", PropertyFsCloudProvider, "fs.cloud_provider"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotEmpty(t, tt.key, "Property key %s should not be empty", tt.name)
			assert.Equal(t, tt.expected, tt.key,
				"Property key %s should match expected value from C library", tt.name)
		})
	}
}

func TestPropertyKeysUsedInBuildFunctions(t *testing.T) {
	t.Run("LocalProperties", func(t *testing.T) {
		testPath := "/tmp/test"
		props := buildLocalProperties(testPath)
		require.NotNil(t, props, "Should return non-nil properties")
		defer freeLoonProperties(props)

		// We should have at least storage_type and root_path
		assert.GreaterOrEqual(t, int(props.count), 2, "Should have at least 2 properties")

		// Parse properties into a map for easier verification
		propsMap := parseLoonPropertiesToMap(props)

		// Verify key-value pairs
		assert.Equal(t, "local", propsMap[PropertyFsStorageType], "Storage type should be 'local'")
		assert.Equal(t, testPath, propsMap[PropertyFsRootPath], "Root path should match input")
	})

	t.Run("RemoteProperties", func(t *testing.T) {
		cfg := &config.Configuration{
			Minio: config.MinioConfig{
				Address:           "localhost",
				Port:              9000,
				AccessKeyID:       "minioadmin",
				SecretAccessKey:   "minioadmin",
				BucketName:        "test-bucket",
				RootPath:          "test-root",
				UseSSL:            false,
				CloudProvider:     "aws",
				LogLevel:          "info",
				Region:            "us-east-1",
				UseVirtualHost:    false,
				RequestTimeoutMs:  1000,
				IamEndpoint:       "iam.aws.com",
				GcpCredentialJSON: "",
				Ssl: config.MinioSslConfig{
					TlsCACert: "/path/to/cert",
				},
				UseIAM: true,
			},
		}

		props := buildRemoteProperties(cfg)
		require.NotNil(t, props, "Should return non-nil properties")
		defer freeLoonProperties(props)

		// Remote properties should have many more fields
		assert.GreaterOrEqual(t, int(props.count), 10, "Should have at least 10 properties")

		// Parse properties into a map for easier verification
		propsMap := parseLoonPropertiesToMap(props)

		// Verify key-value pairs match the configuration
		assert.Equal(t, "remote", propsMap[PropertyFsStorageType], "Storage type should be 'remote'")
		assert.Equal(t, "localhost:9000", propsMap[PropertyFsAddress], "Address should match host:port")
		assert.Equal(t, cfg.Minio.BucketName, propsMap[PropertyFsBucketName], "Bucket name should match")
		assert.Equal(t, cfg.Minio.AccessKeyID, propsMap[PropertyFsAccessKeyID], "Access key ID should match")
		assert.Equal(t, cfg.Minio.SecretAccessKey, propsMap[PropertyFsAccessKeyValue], "Secret key should match")
		assert.Equal(t, cfg.Minio.RootPath, propsMap[PropertyFsRootPath], "Root path should match")
		assert.Equal(t, cfg.Minio.CloudProvider, propsMap[PropertyFsCloudProvider], "Cloud provider should match")
		assert.Equal(t, cfg.Minio.IamEndpoint, propsMap[PropertyFsIamEndpoint], "IAM endpoint should match")
		assert.Equal(t, "false", propsMap[PropertyFsUseSSL], "UseSSL should match")
		assert.Equal(t, cfg.Minio.Ssl.TlsCACert, propsMap[PropertyFsSSLCACert], "SSL CA cert should match")
		assert.Equal(t, "true", propsMap[PropertyFsUseIAM], "UseIAM should match")
		assert.Equal(t, normalizeLogLevel(cfg.Minio.LogLevel), propsMap[PropertyFsLogLevel], "Log level should match (normalized)")
		assert.Equal(t, cfg.Minio.Region, propsMap[PropertyFsRegion], "Region should match")
		assert.Equal(t, "false", propsMap[PropertyFsUseVirtualHost], "UseVirtualHost should match")
		assert.Equal(t, "1000", propsMap[PropertyFsRequestTimeoutMs], "Request timeout should match")
		assert.Equal(t, "", propsMap[PropertyFsGcpCredentialJSON], "GCP credential should match")

		t.Logf("Parsed properties: %+v", propsMap)
	})
}

func TestNormalizeLogLevel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"Empty", "", "Info"},
		{"Fatal lowercase", "fatal", "Fatal"},
		{"Error lowercase", "error", "Error"},
		{"Warn lowercase", "warn", "Warn"},
		{"Warning", "warning", "Warn"},
		{"Info lowercase", "info", "Info"},
		{"Debug lowercase", "debug", "Debug"},
		{"Trace lowercase", "trace", "Trace"},
		{"Off lowercase", "off", "Off"},
		{"Fatal uppercase", "FATAL", "Fatal"},
		{"Error mixed case", "ErRoR", "Error"},
		{"Unknown value", "unknown", "Info"},
		{"Invalid value", "xyz", "Info"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeLogLevel(tt.input)
			assert.Equal(t, tt.expected, result,
				"normalizeLogLevel(%q) should return %q", tt.input, tt.expected)
		})
	}
}
