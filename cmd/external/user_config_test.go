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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
)

func TestLoadUserConfig_Basic(t *testing.T) {
	// Create a temporary YAML file for testing
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_config.yaml")

	yamlContent := `
woodpecker:
  meta:
    type: etcd
    prefix: test-woodpecker
  client:
    segmentAppend:
      queueSize: 5000
      maxRetries: 5
    segmentRollingPolicy:
      maxSize: 512M
      maxInterval: 20m
      maxBlocks: 2000
    auditor:
      maxInterval: 20s
  logstore:
    segmentSyncPolicy:
      maxInterval: 300ms
      maxIntervalForLocalStorage: 20ms
      maxBytes: 512M
      maxEntries: 20000
      maxFlushRetries: 10
      retryInterval: 2000ms
      maxFlushSize: 4M
      maxFlushThreads: 64
    segmentCompactionPolicy:
      maxSize: 4M
      maxParallelUploads: 8
      maxParallelReads: 16
    segmentReadPolicy:
      maxBatchSize: 32M
      maxFetchThreads: 64
  storage:
    type: local
    rootPath: /tmp/test

etcd:
  endpoints:
    - localhost:2379
  rootPath: test-root
  requestTimeout: 5000

minio:
  address: localhost:9000
  accessKeyID: testuser
  secretAccessKey: testpass
  bucketName: test-bucket
  requestTimeoutMs: 5000

log:
  level: debug
  format: json
  stdout: true

trace:
  exporter: jaeger
  sampleFraction: 0.5
`

	err := os.WriteFile(configFile, []byte(yamlContent), 0644)
	require.NoError(t, err)

	// Load user config
	userConfig, err := LoadUserConfig(configFile)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

	// Verify Woodpecker configuration
	assert.NotNil(t, userConfig.Woodpecker)
	assert.Equal(t, "etcd", userConfig.Woodpecker.Meta.Type)
	assert.Equal(t, "test-woodpecker", userConfig.Woodpecker.Meta.Prefix)
	assert.Equal(t, 5000, userConfig.Woodpecker.Client.SegmentAppend.QueueSize)
	assert.Equal(t, 5, userConfig.Woodpecker.Client.SegmentAppend.MaxRetries)
	assert.Equal(t, int64(512*1024*1024), userConfig.Woodpecker.Client.SegmentRollingPolicy.MaxSize.Int64())
	assert.Equal(t, 20*60, userConfig.Woodpecker.Client.SegmentRollingPolicy.MaxInterval.Seconds())
	assert.Equal(t, int64(2000), userConfig.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks)

	// Verify Etcd configuration
	assert.NotNil(t, userConfig.Etcd)
	assert.Equal(t, []string{"localhost:2379"}, userConfig.Etcd.Endpoints)
	assert.Equal(t, "test-root", userConfig.Etcd.RootPath)
	assert.Equal(t, 5000, userConfig.Etcd.RequestTimeout.Milliseconds())

	// Verify Minio configuration
	assert.NotNil(t, userConfig.Minio)
	assert.Equal(t, "localhost:9000", userConfig.Minio.Address)
	assert.Equal(t, "testuser", userConfig.Minio.AccessKeyID)
	assert.Equal(t, "testpass", userConfig.Minio.SecretAccessKey)
	assert.Equal(t, "test-bucket", userConfig.Minio.BucketName)
	assert.Equal(t, 5000, userConfig.Minio.RequestTimeoutMs.Milliseconds())

	// Verify Log configuration
	assert.NotNil(t, userConfig.Log)
	assert.Equal(t, "debug", userConfig.Log.Level)
	assert.Equal(t, "json", userConfig.Log.Format)
	assert.True(t, userConfig.Log.Stdout)

	// Verify Trace configuration
	assert.NotNil(t, userConfig.Trace)
	assert.Equal(t, "jaeger", userConfig.Trace.Exporter)
	assert.Equal(t, 0.5, userConfig.Trace.SampleFraction)
}

func TestApplyToConfig(t *testing.T) {
	// Create default configuration
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	// Store original values
	originalMetaType := cfg.Woodpecker.Meta.Type
	originalMetaPrefix := cfg.Woodpecker.Meta.Prefix
	originalQueueSize := cfg.Woodpecker.Client.SegmentAppend.QueueSize
	originalEtcdRootPath := cfg.Etcd.RootPath
	originalLogLevel := cfg.Log.Level

	// Create user config with partial overrides
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "override_config.yaml")

	yamlContent := `
woodpecker:
  meta:
    prefix: custom-prefix
  client:
    segmentAppend:
      queueSize: 8000
    segmentRollingPolicy:
      maxSize: 1GB

etcd:
  rootPath: custom-etcd-root

log:
  level: error
`

	err = os.WriteFile(configFile, []byte(yamlContent), 0644)
	require.NoError(t, err)

	userConfig, err := LoadUserConfig(configFile)
	require.NoError(t, err)

	// Apply user config
	err = userConfig.ApplyToConfig(cfg)
	require.NoError(t, err)

	// Verify overridden values
	assert.Equal(t, "custom-prefix", cfg.Woodpecker.Meta.Prefix)
	assert.Equal(t, 8000, cfg.Woodpecker.Client.SegmentAppend.QueueSize)
	assert.Equal(t, int64(1024*1024*1024), cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize.Int64())
	assert.Equal(t, "custom-etcd-root", cfg.Etcd.RootPath)
	assert.Equal(t, "error", cfg.Log.Level)

	// Verify non-overridden values remain unchanged
	assert.Equal(t, originalMetaType, cfg.Woodpecker.Meta.Type)
	assert.NotEqual(t, originalMetaPrefix, cfg.Woodpecker.Meta.Prefix)                   // This was changed
	assert.NotEqual(t, originalQueueSize, cfg.Woodpecker.Client.SegmentAppend.QueueSize) // This was changed
	assert.NotEqual(t, originalEtcdRootPath, cfg.Etcd.RootPath)                          // This was changed
	assert.NotEqual(t, originalLogLevel, cfg.Log.Level)                                  // This was changed
}

func TestApplyToConfig_OnlyOverrideNonZero(t *testing.T) {
	// Create default configuration
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	// Store original values
	originalMetaType := cfg.Woodpecker.Meta.Type
	originalQueueSize := cfg.Woodpecker.Client.SegmentAppend.QueueSize
	originalMaxRetries := cfg.Woodpecker.Client.SegmentAppend.MaxRetries

	// Create user config with some zero values (should not override)
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "partial_config.yaml")

	yamlContent := `
woodpecker:
  meta:
    prefix: new-prefix
  client:
    segmentAppend:
      queueSize: 9000
      # maxRetries is not set, should not override
`

	err = os.WriteFile(configFile, []byte(yamlContent), 0644)
	require.NoError(t, err)

	userConfig, err := LoadUserConfig(configFile)
	require.NoError(t, err)

	// Apply user config
	err = userConfig.ApplyToConfig(cfg)
	require.NoError(t, err)

	// Verify only non-zero values were overridden
	assert.Equal(t, originalMetaType, cfg.Woodpecker.Meta.Type)                          // Not set in user config, unchanged
	assert.Equal(t, "new-prefix", cfg.Woodpecker.Meta.Prefix)                            // Set in user config, changed
	assert.Equal(t, 9000, cfg.Woodpecker.Client.SegmentAppend.QueueSize)                 // Set in user config, changed
	assert.Equal(t, originalMaxRetries, cfg.Woodpecker.Client.SegmentAppend.MaxRetries)  // Not set in user config, unchanged
	assert.NotEqual(t, originalQueueSize, cfg.Woodpecker.Client.SegmentAppend.QueueSize) // This was changed
}

func TestLoadUserConfigMultipleFiles(t *testing.T) {
	// Create temporary YAML files for testing
	tempDir := t.TempDir()
	configFile1 := filepath.Join(tempDir, "config1.yaml")
	configFile2 := filepath.Join(tempDir, "config2.yaml")

	yamlContent1 := `
woodpecker:
  meta:
    type: etcd
    prefix: config1-prefix
  client:
    segmentAppend:
      queueSize: 1000

etcd:
  rootPath: config1-root
`

	yamlContent2 := `
woodpecker:
  meta:
    prefix: config2-prefix
  client:
    segmentAppend:
      queueSize: 2000
      maxRetries: 10

etcd:
  requestTimeout: 3000
`

	err := os.WriteFile(configFile1, []byte(yamlContent1), 0644)
	require.NoError(t, err)
	err = os.WriteFile(configFile2, []byte(yamlContent2), 0644)
	require.NoError(t, err)

	// Load both config files (config2 should override config1)
	userConfig, err := LoadUserConfig(configFile1, configFile2)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

	// Verify merged values
	// Type from config1 (not overridden by config2)
	assert.Equal(t, "etcd", userConfig.Woodpecker.Meta.Type)
	// Prefix from config2 (overrides config1)
	assert.Equal(t, "config2-prefix", userConfig.Woodpecker.Meta.Prefix)
	// QueueSize from config2 (overrides config1)
	assert.Equal(t, 2000, userConfig.Woodpecker.Client.SegmentAppend.QueueSize)
	// MaxRetries only in config2
	assert.Equal(t, 10, userConfig.Woodpecker.Client.SegmentAppend.MaxRetries)
	// RootPath from config1 (not overridden by config2)
	assert.Equal(t, "config1-root", userConfig.Etcd.RootPath)
	// RequestTimeout from config2
	assert.Equal(t, 3000, userConfig.Etcd.RequestTimeout.Milliseconds())
}

func TestLoadUserConfigWithUnits(t *testing.T) {
	// Create a temporary YAML file with various units
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "units_config.yaml")

	yamlContent := `
woodpecker:
  client:
    segmentRollingPolicy:
      maxSize: 256MB
      maxInterval: 10m
    auditor:
      maxInterval: 30s
  logstore:
    segmentSyncPolicy:
      maxInterval: 500ms
      maxIntervalForLocalStorage: 5ms
      maxBytes: 128M
      retryInterval: 1s
      maxFlushSize: 2MB
    segmentCompactionPolicy:
      maxBytes: 4M
    segmentReadPolicy:
      maxBatchSize: 16MB

minio:
  requestTimeoutMs: 10s

etcd:
  requestTimeout: 5s
`

	err := os.WriteFile(configFile, []byte(yamlContent), 0644)
	require.NoError(t, err)

	// Load user config
	userConfig, err := LoadUserConfig(configFile)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

	// Verify ByteSize parsing
	assert.Equal(t, int64(256*1024*1024), userConfig.Woodpecker.Client.SegmentRollingPolicy.MaxSize.Int64())
	assert.Equal(t, int64(128*1024*1024), userConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes.Int64())
	assert.Equal(t, int64(2*1024*1024), userConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize.Int64())
	assert.Equal(t, int64(4*1024*1024), userConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes.Int64())
	assert.Equal(t, int64(16*1024*1024), userConfig.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize.Int64())

	// Verify DurationSeconds parsing (default unit: seconds)
	assert.Equal(t, 10*60, userConfig.Woodpecker.Client.SegmentRollingPolicy.MaxInterval.Seconds())
	assert.Equal(t, 30, userConfig.Woodpecker.Client.Auditor.MaxInterval.Seconds())

	// Verify DurationMilliseconds parsing (default unit: milliseconds)
	assert.Equal(t, 500, userConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval.Milliseconds())
	assert.Equal(t, 5, userConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage.Milliseconds())
	assert.Equal(t, 1000, userConfig.Woodpecker.Logstore.SegmentSyncPolicy.RetryInterval.Milliseconds())
	assert.Equal(t, 10000, userConfig.Minio.RequestTimeoutMs.Milliseconds())
	assert.Equal(t, 5000, userConfig.Etcd.RequestTimeout.Milliseconds())
}

func TestLoadUserConfigInvalidYAML(t *testing.T) {
	// Test with invalid YAML - should return error
	tempDir := t.TempDir()
	invalidFile := filepath.Join(tempDir, "invalid.yaml")
	err := os.WriteFile(invalidFile, []byte("invalid: yaml: content: ["), 0644)
	require.NoError(t, err)

	_, err = LoadUserConfig(invalidFile)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse")
}

func TestLoadUserConfigEmpty(t *testing.T) {
	// Test with no files
	_, err := LoadUserConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no configuration files provided")
}

func TestLoadUserConfigEmptyYAML(t *testing.T) {
	// Test with an empty YAML file
	tempDir := t.TempDir()
	emptyFile := filepath.Join(tempDir, "empty.yaml")
	err := os.WriteFile(emptyFile, []byte(""), 0644)
	require.NoError(t, err)

	// Should not error, just return an empty config
	userConfig, err := LoadUserConfig(emptyFile)
	require.NoError(t, err)
	assert.NotNil(t, userConfig)
	assert.Nil(t, userConfig.Woodpecker)
	assert.Nil(t, userConfig.Etcd)
	assert.Nil(t, userConfig.Minio)
	assert.Nil(t, userConfig.Log)
	assert.Nil(t, userConfig.Trace)
}

func TestLoadUserConfigNonExistentFile(t *testing.T) {
	// Test with a non-existent file - should return empty config, not error
	tempDir := t.TempDir()
	nonExistentFile := filepath.Join(tempDir, "does_not_exist.yaml")

	userConfig, err := LoadUserConfig(nonExistentFile)
	require.NoError(t, err)
	assert.NotNil(t, userConfig)
	assert.Nil(t, userConfig.Woodpecker)
	assert.Nil(t, userConfig.Etcd)
	assert.Nil(t, userConfig.Minio)
	assert.Nil(t, userConfig.Log)
	assert.Nil(t, userConfig.Trace)
}

func TestLoadUserConfigMixedExistentAndNonExistent(t *testing.T) {
	// Test with a mix of existent and non-existent files
	tempDir := t.TempDir()

	// Create one file that exists
	existingFile := filepath.Join(tempDir, "existing.yaml")
	yamlContent := `
woodpecker:
  meta:
    prefix: test-prefix
log:
  level: debug
`
	err := os.WriteFile(existingFile, []byte(yamlContent), 0644)
	require.NoError(t, err)

	// Reference a file that doesn't exist
	nonExistentFile := filepath.Join(tempDir, "does_not_exist.yaml")

	// Load both files - should only load the existing one
	userConfig, err := LoadUserConfig(nonExistentFile, existingFile)
	require.NoError(t, err)
	assert.NotNil(t, userConfig)

	// Verify that the existing file was loaded
	assert.NotNil(t, userConfig.Woodpecker)
	assert.Equal(t, "test-prefix", userConfig.Woodpecker.Meta.Prefix)
	assert.NotNil(t, userConfig.Log)
	assert.Equal(t, "debug", userConfig.Log.Level)
}

func TestLoadUserConfigAllNonExistent(t *testing.T) {
	// Test with multiple non-existent files - should return empty config
	tempDir := t.TempDir()

	file1 := filepath.Join(tempDir, "file1.yaml")
	file2 := filepath.Join(tempDir, "file2.yaml")
	file3 := filepath.Join(tempDir, "file3.yaml")

	userConfig, err := LoadUserConfig(file1, file2, file3)
	require.NoError(t, err)
	assert.NotNil(t, userConfig)
	assert.Nil(t, userConfig.Woodpecker)
	assert.Nil(t, userConfig.Etcd)
	assert.Nil(t, userConfig.Minio)
	assert.Nil(t, userConfig.Log)
	assert.Nil(t, userConfig.Trace)
}

func TestApplyToConfigWithEmptyUserConfig(t *testing.T) {
	// Test that applying an empty user config doesn't change anything
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	// Store original values
	originalMetaType := cfg.Woodpecker.Meta.Type
	originalMetaPrefix := cfg.Woodpecker.Meta.Prefix
	originalQueueSize := cfg.Woodpecker.Client.SegmentAppend.QueueSize
	originalLogLevel := cfg.Log.Level

	// Create an empty user config (like from non-existent files)
	emptyUserConfig := &UserConfig{}

	// Apply empty config
	err = emptyUserConfig.ApplyToConfig(cfg)
	require.NoError(t, err)

	// Verify nothing changed
	assert.Equal(t, originalMetaType, cfg.Woodpecker.Meta.Type)
	assert.Equal(t, originalMetaPrefix, cfg.Woodpecker.Meta.Prefix)
	assert.Equal(t, originalQueueSize, cfg.Woodpecker.Client.SegmentAppend.QueueSize)
	assert.Equal(t, originalLogLevel, cfg.Log.Level)
}
