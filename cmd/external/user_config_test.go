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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
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

func TestLoadUserConfigNonExistent(t *testing.T) {
	// Test loading non-existent file should return empty config without error
	userConfig, err := LoadUserConfig("/non/existent/path/user.yaml")
	require.NoError(t, err, "Non-existent file should not cause error")
	assert.NotNil(t, userConfig, "Should return empty config")

	// Verify it's an empty config
	assert.Empty(t, userConfig.Woodpecker.Meta.Type)
	assert.Empty(t, userConfig.Log.Level)

	// Test that applying empty config doesn't panic or cause error
	baseConfig := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Meta: config.MetaConfig{
				Type:   "etcd",
				Prefix: "woodpecker",
			},
		},
		Log: config.LogConfig{
			Level: "info",
		},
	}

	err = userConfig.ApplyToConfig(baseConfig)
	require.NoError(t, err, "Applying empty config should not cause error")

	// Verify base config remains unchanged
	assert.Equal(t, "etcd", baseConfig.Woodpecker.Meta.Type)
	assert.Equal(t, "info", baseConfig.Log.Level)
}

func TestLoadUserConfig(t *testing.T) {
	// Create a temporary config file
	yamlContent := `
woodpecker:
  meta:
    type: etcd
    prefix: test-woodpecker
  client:
    segmentAppend:
      queueSize: 5000
      maxRetries: 2
    segmentRollingPolicy:
      maxSize: 128M
      maxInterval: 5m
      maxBlocks: 500
  storage:
    type: minio
    rootPath: /test/woodpecker
log:
  level: debug
  format: json
etcd:
  endpoints:
    - localhost:2379
    - localhost:2380
  rootPath: test-root
minio:
  address: minio
  port: 9000
  accessKeyID: testkey
  secretAccessKey: testsecret
  bucketName: test-bucket
`
	tmpFile, err := os.CreateTemp("", "user_config_*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(yamlContent)
	require.NoError(t, err)
	tmpFile.Close()

	// Load user config
	userConfig, err := LoadUserConfig(tmpFile.Name())
	require.NoError(t, err)
	assert.NotNil(t, userConfig)

	// Verify loaded values
	assert.Equal(t, "etcd", userConfig.Woodpecker.Meta.Type)
	assert.Equal(t, "test-woodpecker", userConfig.Woodpecker.Meta.Prefix)
	assert.Equal(t, "debug", userConfig.Log.Level)
	assert.Equal(t, "json", userConfig.Log.Format)
}

func TestApplyToConfig(t *testing.T) {
	// Create a temporary config file with all fields
	yamlContent := `
woodpecker:
  meta:
    type: etcd
    prefix: custom-woodpecker
  client:
    segmentAppend:
      queueSize: 5000
    segmentRollingPolicy:
      maxSize: 256M
    quorum:
      quorumBufferPools:
        pool1: "node1:8080, node2:8080"
  logstore:
    segmentSyncPolicy:
      maxInterval: 500ms
      maxBytes: 512M
    segmentCompactionPolicy:
      maxSize: 64M
    segmentReadPolicy:
      maxBatchSize: 32M
  storage:
    type: minio
    rootPath: /custom/path
log:
  level: debug
etcd:
  endpoints:
    - localhost:2379
    - localhost:2380
minio:
  address: minio
  port: 9000
  bucketName: test-bucket
`
	tmpFile, err := os.CreateTemp("", "user_config_*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(yamlContent)
	require.NoError(t, err)
	tmpFile.Close()

	// Load user config
	userConfig, err := LoadUserConfig(tmpFile.Name())
	require.NoError(t, err)

	// Create base woodpecker config
	baseConfig := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Meta: config.MetaConfig{
				Type:   "etcd",
				Prefix: "woodpecker",
			},
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  100,
					MaxRetries: 2,
				},
			},
			Storage: config.StorageConfig{
				Type:     "local",
				RootPath: "/tmp/woodpecker",
			},
		},
		Log: config.LogConfig{
			Level:  "info",
			Format: "text",
		},
		Etcd: config.EtcdConfig{
			Endpoints: []string{"localhost:2379"},
			RootPath:  "woodpecker",
		},
		Minio: config.MinioConfig{
			Address:         "localhost",
			Port:            9000,
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
		},
	}

	// Apply user config
	err = userConfig.ApplyToConfig(baseConfig)
	require.NoError(t, err)

	// Verify overrides were applied
	assert.Equal(t, "custom-woodpecker", baseConfig.Woodpecker.Meta.Prefix)
	assert.Equal(t, "etcd", baseConfig.Woodpecker.Meta.Type)
	assert.Equal(t, 5000, baseConfig.Woodpecker.Client.SegmentAppend.QueueSize)
	assert.Equal(t, int64(256*1024*1024), baseConfig.Woodpecker.Client.SegmentRollingPolicy.MaxSize)
	assert.Equal(t, "minio", baseConfig.Woodpecker.Storage.Type)
	assert.Equal(t, "debug", baseConfig.Log.Level)
	assert.Equal(t, []string{"localhost:2379", "localhost:2380"}, baseConfig.Etcd.Endpoints)
	assert.Equal(t, "minio", baseConfig.Minio.Address)
	assert.Equal(t, "test-bucket", baseConfig.Minio.BucketName)
}

func TestApplyQuorumConfig(t *testing.T) {
	yamlContent := `
woodpecker:
  client:
    quorum:
      quorumBufferPools:
        region1: "node1:8080, node2:8080"
        region2: "node3:8080, node4:8080"
      quorumSelectStrategy:
        affinityMode: hard
        replicas: 5
        strategy: cross-region
        customPlacement:
          replica1:
            region: us-west
            az: us-west-1a
            resourceGroup: rg1
          replica2:
            region: us-east
            az: us-east-1b
            resourceGroup: rg2
`
	tmpFile, err := os.CreateTemp("", "user_config_*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(yamlContent)
	require.NoError(t, err)
	tmpFile.Close()

	userConfig, err := LoadUserConfig(tmpFile.Name())
	require.NoError(t, err)

	baseConfig := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				Quorum: config.QuorumConfig{
					BufferPools: []config.QuorumBufferPool{
						{
							Name:  "default-pool",
							Seeds: []string{"node1:8080"},
						},
					},
					SelectStrategy: config.QuorumSelectStrategy{
						AffinityMode: "soft",
						Replicas:     3,
						Strategy:     "random",
					},
				},
			},
		},
	}

	err = userConfig.ApplyToConfig(baseConfig)
	require.NoError(t, err)

	// Verify quorum config was applied
	assert.Len(t, baseConfig.Woodpecker.Client.Quorum.BufferPools, 2)

	// Find region1 pool
	var region1Pool *config.QuorumBufferPool
	var region2Pool *config.QuorumBufferPool
	for i := range baseConfig.Woodpecker.Client.Quorum.BufferPools {
		if baseConfig.Woodpecker.Client.Quorum.BufferPools[i].Name == "region1" {
			region1Pool = &baseConfig.Woodpecker.Client.Quorum.BufferPools[i]
		}
		if baseConfig.Woodpecker.Client.Quorum.BufferPools[i].Name == "region2" {
			region2Pool = &baseConfig.Woodpecker.Client.Quorum.BufferPools[i]
		}
	}

	require.NotNil(t, region1Pool, "region1 pool should exist")
	require.NotNil(t, region2Pool, "region2 pool should exist")
	assert.Equal(t, []string{"node1:8080", "node2:8080"}, region1Pool.Seeds)
	assert.Equal(t, []string{"node3:8080", "node4:8080"}, region2Pool.Seeds)

	// Verify select strategy
	assert.Equal(t, "hard", baseConfig.Woodpecker.Client.Quorum.SelectStrategy.AffinityMode)
	assert.Equal(t, 5, baseConfig.Woodpecker.Client.Quorum.SelectStrategy.Replicas)
	assert.Equal(t, "cross-region", baseConfig.Woodpecker.Client.Quorum.SelectStrategy.Strategy)

	// Verify custom placement
	assert.Len(t, baseConfig.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement, 2)

	// Find replica1 and replica2
	var replica1 *config.CustomPlacement
	var replica2 *config.CustomPlacement
	for i := range baseConfig.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement {
		if baseConfig.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[i].Name == "replica1" {
			replica1 = &baseConfig.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[i]
		}
		if baseConfig.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[i].Name == "replica2" {
			replica2 = &baseConfig.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[i]
		}
	}

	require.NotNil(t, replica1, "replica1 placement should exist")
	require.NotNil(t, replica2, "replica2 placement should exist")
	assert.Equal(t, "us-west", replica1.Region)
	assert.Equal(t, "us-west-1a", replica1.Az)
	assert.Equal(t, "rg1", replica1.ResourceGroup)
	assert.Equal(t, "us-east", replica2.Region)
	assert.Equal(t, "us-east-1b", replica2.Az)
	assert.Equal(t, "rg2", replica2.ResourceGroup)
}

func TestApplyLogstoreConfig(t *testing.T) {
	yamlContent := `
woodpecker:
  logstore:
    segmentSyncPolicy:
      maxInterval: 500ms
      maxBytes: 512M
    segmentCompactionPolicy:
      maxSize: 64M
    segmentReadPolicy:
      maxBatchSize: 32M
`
	tmpFile, err := os.CreateTemp("", "user_config_*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(yamlContent)
	require.NoError(t, err)
	tmpFile.Close()

	userConfig, err := LoadUserConfig(tmpFile.Name())
	require.NoError(t, err)

	baseConfig := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxInterval: 1000,
					MaxBytes:    100000000,
				},
				SegmentCompactionPolicy: config.SegmentCompactionPolicy{
					MaxBytes: 32000000,
				},
				SegmentReadPolicy: config.SegmentReadPolicyConfig{
					MaxBatchSize: 16000000,
				},
			},
		},
	}

	err = userConfig.ApplyToConfig(baseConfig)
	require.NoError(t, err)

	// Verify logstore config was applied
	assert.Equal(t, 500, baseConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval)
	assert.Equal(t, int64(512*1024*1024), baseConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes)
	assert.Equal(t, int64(64*1024*1024), baseConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes)
	assert.Equal(t, int64(32*1024*1024), baseConfig.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize)
}

func TestNumericValues(t *testing.T) {
	// Test that numeric values (without units) also work
	yamlContent := `
woodpecker:
  client:
    segmentRollingPolicy:
      maxSize: 268435456
      maxInterval: 600
  logstore:
    segmentSyncPolicy:
      maxInterval: 500
      maxBytes: 536870912
`
	tmpFile, err := os.CreateTemp("", "user_config_*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(yamlContent)
	require.NoError(t, err)
	tmpFile.Close()

	userConfig, err := LoadUserConfig(tmpFile.Name())
	require.NoError(t, err)

	baseConfig := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentRollingPolicy: config.SegmentRollingPolicyConfig{
					MaxSize:     100000000,
					MaxInterval: 800,
				},
			},
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxInterval: 1000,
					MaxBytes:    100000000,
				},
			},
		},
	}

	err = userConfig.ApplyToConfig(baseConfig)
	require.NoError(t, err)

	// Verify numeric values were applied correctly
	assert.Equal(t, int64(268435456), baseConfig.Woodpecker.Client.SegmentRollingPolicy.MaxSize)
	assert.Equal(t, 600, baseConfig.Woodpecker.Client.SegmentRollingPolicy.MaxInterval)
	assert.Equal(t, 500, baseConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval)
	assert.Equal(t, int64(536870912), baseConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes)
}
