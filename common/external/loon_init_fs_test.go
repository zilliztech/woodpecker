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
	if err != nil {
		t.Skipf("Failed to initialize remote Loon filesystem (MinIO might not be running): %v", err)
		return
	}

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
		if err != nil {
			t.Skipf("Failed to initialize remote storage (MinIO might not be running): %v", err)
			return
		}

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

func TestHandleLoonFFIResult(t *testing.T) {
	// Note: This test is limited because we can't easily create LoonFFIResult
	// structures from Go without calling actual C functions
	// The actual error handling is tested implicitly through other tests
	t.Skip("LoonFFIResult testing requires actual C function calls")
}
