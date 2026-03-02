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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewConfiguration test new Configuration
func TestNewConfiguration(t *testing.T) {
	tempFile, err := os.OpenFile("../../config/woodpecker.yaml", os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(t, err)
	// load configuration
	config, err := NewConfiguration(tempFile.Name())
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// test configuration
	assert.Equal(t, "etcd", config.Woodpecker.Meta.Type)
	assert.Equal(t, "woodpecker", config.Woodpecker.Meta.Prefix)
	assert.Equal(t, 10000, config.Woodpecker.Client.SegmentAppend.QueueSize)
	assert.Equal(t, 3, config.Woodpecker.Client.SegmentAppend.MaxRetries)
	assert.Equal(t, int64(256000000), config.Woodpecker.Client.SegmentRollingPolicy.MaxSize.Int64())
	assert.Equal(t, 600, config.Woodpecker.Client.SegmentRollingPolicy.MaxInterval.Seconds())
	assert.Equal(t, int64(1000), config.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks)
	assert.Equal(t, 10, config.Woodpecker.Client.Auditor.MaxInterval.Seconds())
	assert.Equal(t, 1, len(config.Woodpecker.Client.Quorum.BufferPools))
	assert.Equal(t, "default-region-pool", config.Woodpecker.Client.Quorum.BufferPools[0].Name)
	assert.Equal(t, []string{}, config.Woodpecker.Client.Quorum.BufferPools[0].Seeds)
	assert.Equal(t, "soft", config.Woodpecker.Client.Quorum.SelectStrategy.AffinityMode)
	assert.Equal(t, 3, config.Woodpecker.Client.Quorum.SelectStrategy.Replicas)
	assert.Equal(t, "random", config.Woodpecker.Client.Quorum.SelectStrategy.Strategy)
	assert.Equal(t, 0, len(config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement))
	assert.Equal(t, 3, config.Woodpecker.Client.SessionMonitor.CheckInterval.Seconds())
	assert.Equal(t, 5, config.Woodpecker.Client.SessionMonitor.MaxFailures)
	// DirectRead config from yaml
	assert.False(t, config.Woodpecker.Client.DirectRead.Enabled)
	assert.Equal(t, int64(16777216), config.Woodpecker.Client.DirectRead.MaxBatchSize.Int64())
	assert.Equal(t, 4, config.Woodpecker.Client.DirectRead.MaxFetchThreads)
	// Test the getter methods for backward compatibility
	assert.Equal(t, 3, config.Woodpecker.Client.Quorum.GetEnsembleSize())
	assert.Equal(t, 3, config.Woodpecker.Client.Quorum.GetWriteQuorumSize())
	assert.Equal(t, 2, config.Woodpecker.Client.Quorum.GetAckQuorumSize())
	assert.Equal(t, 200, config.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval.Milliseconds())
	assert.Equal(t, 10, config.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage.Milliseconds())
	assert.Equal(t, 10000, config.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries)
	assert.Equal(t, int64(256000000), config.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes.Int64())
	assert.Equal(t, 5, config.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushRetries)
	assert.Equal(t, 1000, config.Woodpecker.Logstore.SegmentSyncPolicy.RetryInterval.Milliseconds())
	assert.Equal(t, int64(2000000), config.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize.Int64())
	assert.Equal(t, 32, config.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads)
	assert.Equal(t, int64(2000000), config.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes.Int64())
	assert.Equal(t, 4, config.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelUploads)
	assert.Equal(t, 8, config.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelReads)
	assert.Equal(t, int64(2000000), config.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize.Int64())
	assert.Equal(t, 32, config.Woodpecker.Logstore.SegmentReadPolicy.MaxFetchThreads)
	assert.Equal(t, 259200, config.Woodpecker.Logstore.RetentionPolicy.TTL) // 72h = 259200s
	assert.Equal(t, "auto", config.Woodpecker.Logstore.FencePolicy.ConditionWrite)
	assert.Equal(t, 536870912, config.Woodpecker.Logstore.GRPCConfig.GetServerMaxSendSize())
	assert.Equal(t, 268435456, config.Woodpecker.Logstore.GRPCConfig.GetServerMaxRecvSize())
	assert.Equal(t, 268435456, config.Woodpecker.Logstore.GRPCConfig.GetClientMaxSendSize())
	assert.Equal(t, 536870912, config.Woodpecker.Logstore.GRPCConfig.GetClientMaxRecvSize())
	assert.Equal(t, 60, config.Woodpecker.Logstore.ProcessorCleanupPolicy.CleanupInterval.Seconds())
	assert.Equal(t, 300, config.Woodpecker.Logstore.ProcessorCleanupPolicy.MaxIdleTime.Seconds())
	assert.Equal(t, 15, config.Woodpecker.Logstore.ProcessorCleanupPolicy.ShutdownTimeout.Seconds())
	assert.Equal(t, "minio", config.Woodpecker.Storage.Type)
	assert.Equal(t, "/var/lib/woodpecker", config.Woodpecker.Storage.RootPath)
	assert.Equal(t, "info", config.Log.Level)
	assert.Equal(t, "text", config.Log.Format)
	assert.True(t, config.Log.Stdout)
	assert.Equal(t, "./logs", config.Log.File.RootPath)
	assert.Equal(t, 300, config.Log.File.MaxSize)
	assert.Equal(t, 20, config.Log.File.MaxBackups)
	assert.Equal(t, 10, config.Log.File.MaxAge)
	assert.Equal(t, "noop", config.Trace.Exporter)
	assert.Equal(t, 1.0, config.Trace.SampleFraction)
	assert.Equal(t, 10, config.Trace.InitTimeout.Seconds())
	assert.Equal(t, "http://localhost:14268/api/traces", config.Trace.Jaeger.URL)
	assert.Equal(t, "127.0.0.1:4317", config.Trace.Otlp.Endpoint)
	assert.Equal(t, "grpc", config.Trace.Otlp.Method)
	assert.False(t, config.Trace.Otlp.Secure)
	assert.Equal(t, []string{"localhost:2379"}, config.Etcd.GetEndpoints())
	assert.Equal(t, "by-dev", config.Etcd.RootPath)
	assert.Equal(t, "meta", config.Etcd.MetaSubPath)
	assert.Equal(t, "kv", config.Etcd.KvSubPath)
	assert.Equal(t, "info", config.Etcd.Log.Level)
	assert.Equal(t, "stdout", config.Etcd.Log.Path)
	assert.False(t, config.Etcd.Ssl.Enabled)
	assert.Equal(t, 10000, config.Etcd.RequestTimeout.Milliseconds())
	assert.False(t, config.Etcd.Use.Embed)
	assert.Equal(t, "localhost", config.Minio.Address)
	assert.Equal(t, 9000, config.Minio.Port)
	assert.Equal(t, "minioadmin", config.Minio.AccessKeyID)
	assert.Equal(t, "minioadmin", config.Minio.SecretAccessKey)
	assert.False(t, config.Minio.UseSSL)

	assert.Equal(t, "/path/to/public.crt", config.Minio.Ssl.TlsCACert)
	assert.Equal(t, "a-bucket", config.Minio.BucketName)
	assert.True(t, config.Minio.CreateBucket)
	assert.Equal(t, "files", config.Minio.RootPath)
	assert.False(t, config.Minio.UseIAM)
	assert.Equal(t, "aws", config.Minio.CloudProvider)
	assert.Equal(t, "", config.Minio.GcpCredentialJSON)
	assert.Equal(t, "", config.Minio.IamEndpoint)
	assert.Equal(t, "fatal", config.Minio.LogLevel)
	assert.Equal(t, "", config.Minio.Region)
	assert.False(t, config.Minio.UseVirtualHost)
	assert.Equal(t, 10000, config.Minio.RequestTimeoutMs.Milliseconds())
	assert.Equal(t, 0, config.Minio.ListObjectsMaxKeys)

	defaultConfig, err := NewConfiguration()
	assert.NoError(t, err)

	// test default configuration
	assert.Equal(t, "etcd", defaultConfig.Woodpecker.Meta.Type)
	assert.Equal(t, "woodpecker", defaultConfig.Woodpecker.Meta.Prefix)
	assert.Equal(t, 100, defaultConfig.Woodpecker.Client.SegmentAppend.QueueSize)
	assert.Equal(t, 2, defaultConfig.Woodpecker.Client.SegmentAppend.MaxRetries)
	assert.Equal(t, int64(100000000), defaultConfig.Woodpecker.Client.SegmentRollingPolicy.MaxSize.Int64())
	assert.Equal(t, 800, defaultConfig.Woodpecker.Client.SegmentRollingPolicy.MaxInterval.Seconds())
	assert.Equal(t, int64(1000), defaultConfig.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks)
	assert.Equal(t, 5, defaultConfig.Woodpecker.Client.Auditor.MaxInterval.Seconds())
	assert.Equal(t, 1, len(defaultConfig.Woodpecker.Client.Quorum.BufferPools))
	assert.Equal(t, "default-pool", defaultConfig.Woodpecker.Client.Quorum.BufferPools[0].Name)
	assert.Equal(t, []string{}, defaultConfig.Woodpecker.Client.Quorum.BufferPools[0].Seeds)
	assert.Equal(t, "soft", defaultConfig.Woodpecker.Client.Quorum.SelectStrategy.AffinityMode)
	assert.Equal(t, 3, defaultConfig.Woodpecker.Client.Quorum.SelectStrategy.Replicas)
	assert.Equal(t, "random", defaultConfig.Woodpecker.Client.Quorum.SelectStrategy.Strategy)
	assert.Equal(t, 0, len(defaultConfig.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement))
	assert.Equal(t, 3, defaultConfig.Woodpecker.Client.SessionMonitor.CheckInterval.Seconds())
	assert.Equal(t, 5, defaultConfig.Woodpecker.Client.SessionMonitor.MaxFailures)
	// DirectRead default config
	assert.False(t, defaultConfig.Woodpecker.Client.DirectRead.Enabled)
	assert.Equal(t, int64(16*1024*1024), defaultConfig.Woodpecker.Client.DirectRead.MaxBatchSize.Int64())
	assert.Equal(t, 4, defaultConfig.Woodpecker.Client.DirectRead.MaxFetchThreads)
	// Test the getter methods for backward compatibility
	assert.Equal(t, 3, defaultConfig.Woodpecker.Client.Quorum.GetEnsembleSize())
	assert.Equal(t, 3, defaultConfig.Woodpecker.Client.Quorum.GetWriteQuorumSize())
	assert.Equal(t, 2, defaultConfig.Woodpecker.Client.Quorum.GetAckQuorumSize())
	assert.Equal(t, 1000, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval.Milliseconds())
	assert.Equal(t, 5, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage.Milliseconds())
	assert.Equal(t, 2000, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries)
	assert.Equal(t, int64(100000000), defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes.Int64())
	assert.Equal(t, 3, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushRetries)
	assert.Equal(t, 2000, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.RetryInterval.Milliseconds())
	assert.Equal(t, int64(16000000), defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize.Int64())
	assert.Equal(t, 8, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads)
	assert.Equal(t, int64(32000000), defaultConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes.Int64())
	assert.Equal(t, 4, defaultConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelUploads)
	assert.Equal(t, 8, defaultConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelReads)
	assert.Equal(t, int64(16000000), defaultConfig.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize.Int64())
	assert.Equal(t, 32, defaultConfig.Woodpecker.Logstore.SegmentReadPolicy.MaxFetchThreads)
	assert.Equal(t, 259200, defaultConfig.Woodpecker.Logstore.RetentionPolicy.TTL) // 72h = 259200s
	assert.Equal(t, "auto", defaultConfig.Woodpecker.Logstore.FencePolicy.ConditionWrite)
	assert.Equal(t, 536870912, defaultConfig.Woodpecker.Logstore.GRPCConfig.GetServerMaxSendSize())
	assert.Equal(t, 268435456, defaultConfig.Woodpecker.Logstore.GRPCConfig.GetServerMaxRecvSize())
	assert.Equal(t, 268435456, defaultConfig.Woodpecker.Logstore.GRPCConfig.GetClientMaxSendSize())
	assert.Equal(t, 536870912, defaultConfig.Woodpecker.Logstore.GRPCConfig.GetClientMaxRecvSize())
	assert.Equal(t, 60, defaultConfig.Woodpecker.Logstore.ProcessorCleanupPolicy.CleanupInterval.Seconds())
	assert.Equal(t, 300, defaultConfig.Woodpecker.Logstore.ProcessorCleanupPolicy.MaxIdleTime.Seconds())
	assert.Equal(t, 15, defaultConfig.Woodpecker.Logstore.ProcessorCleanupPolicy.ShutdownTimeout.Seconds())
	assert.Equal(t, "default", defaultConfig.Woodpecker.Storage.Type)
	assert.Equal(t, "/tmp/woodpecker", defaultConfig.Woodpecker.Storage.RootPath)
	assert.Equal(t, "info", defaultConfig.Log.Level)
	assert.Equal(t, "text", defaultConfig.Log.Format)
	assert.True(t, defaultConfig.Log.Stdout)
	assert.Equal(t, "./logs", defaultConfig.Log.File.RootPath)
	assert.Equal(t, 100, defaultConfig.Log.File.MaxSize)
	assert.Equal(t, 10, defaultConfig.Log.File.MaxBackups)
	assert.Equal(t, 30, defaultConfig.Log.File.MaxAge)
	assert.Equal(t, "noop", defaultConfig.Trace.Exporter)
	assert.Equal(t, 1.0, defaultConfig.Trace.SampleFraction)
	assert.Equal(t, 10, defaultConfig.Trace.InitTimeout.Seconds())
	assert.Equal(t, "http://localhost:14268/api/traces", defaultConfig.Trace.Jaeger.URL)
	assert.Equal(t, "localhost:4317", defaultConfig.Trace.Otlp.Endpoint)
	assert.Equal(t, "grpc", defaultConfig.Trace.Otlp.Method)
	assert.False(t, defaultConfig.Trace.Otlp.Secure)
	assert.Equal(t, []string{"localhost:2379"}, defaultConfig.Etcd.GetEndpoints())
	assert.Equal(t, "woodpecker", defaultConfig.Etcd.RootPath)
	assert.Equal(t, "meta", defaultConfig.Etcd.MetaSubPath)
	assert.Equal(t, "kv", defaultConfig.Etcd.KvSubPath)
	assert.Equal(t, "info", defaultConfig.Etcd.Log.Level)
	assert.Equal(t, "./logs", defaultConfig.Etcd.Log.Path)
	assert.False(t, defaultConfig.Etcd.Ssl.Enabled)
	assert.Equal(t, 10000, defaultConfig.Etcd.RequestTimeout.Milliseconds())
	assert.False(t, defaultConfig.Etcd.Use.Embed)
	assert.Equal(t, "localhost", defaultConfig.Minio.Address)
	assert.Equal(t, 9000, defaultConfig.Minio.Port)
	assert.Equal(t, "minioadmin", defaultConfig.Minio.AccessKeyID)
	assert.Equal(t, "minioadmin", defaultConfig.Minio.SecretAccessKey)
	assert.False(t, defaultConfig.Minio.UseSSL)
	assert.Equal(t, "/path/to/public.crt", defaultConfig.Minio.Ssl.TlsCACert)
	assert.Equal(t, "a-bucket", defaultConfig.Minio.BucketName)
	assert.True(t, defaultConfig.Minio.CreateBucket)
	assert.Equal(t, "files", defaultConfig.Minio.RootPath)
	assert.False(t, defaultConfig.Minio.UseIAM)
	assert.Equal(t, "aws", defaultConfig.Minio.CloudProvider)
	assert.Equal(t, "", defaultConfig.Minio.GcpCredentialJSON)
	assert.Equal(t, "", defaultConfig.Minio.IamEndpoint)
	assert.Equal(t, "fatal", defaultConfig.Minio.LogLevel)
	assert.Equal(t, "", defaultConfig.Minio.Region)
	assert.False(t, defaultConfig.Minio.UseVirtualHost)
	assert.Equal(t, 1000, defaultConfig.Minio.RequestTimeoutMs.Milliseconds())
	assert.Equal(t, 0, defaultConfig.Minio.ListObjectsMaxKeys)
}

func TestConfigurationOverwrite(t *testing.T) {
	// config file 1
	cfgFile, err := os.OpenFile("../../config/woodpecker.yaml", os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(t, err)

	// config file2
	extraCfgContent := `woodpecker:
  meta:
    type: etcd
    prefix: woodpecker
  client:
    segmentAppend:
      queueSize: 20000
      maxRetries: 22
    segmentRollingPolicy:
      maxSize: 22220000000
      maxInterval: 2200
      maxBlocks: 2000
    auditor:
      maxInterval: 10
    quorum:
      quorumBufferPools:
        - name: region-a-pool
          seeds: ["test-node1", "test-node2"]
        - name: region-b-pool
          seeds: ["test-node3", "test-node4", "test-node5"]
      quorumSelectStrategy:
        affinityMode: hard
        replicas: 5
        strategy: custom
        customPlacement:
          - name: replica-1
            region: "region-a-pool"
            az: "test-az-1"
            resourceGroup: "test-rg-1"
          - name: replica-2
            region: "region-a-pool"
            az: "test-az-2"
            resourceGroup: "test-rg-2"
          - name: replica-3
            region: "region-b-pool"
            az: "test-az-1"
            resourceGroup: "test-rg-3"
          - name: replica-4
            region: "region-b-pool"
            az: "test-az-2"
            resourceGroup: "test-rg-4"
          - name: replica-5
            region: "region-a-pool"
            az: "test-az-3"
            resourceGroup: "test-rg-5"
  logstore:
    segmentReadPolicy:
      maxBatchSize: 32000000
      maxFetchThreads: 64`
	extraCfgFile, err := os.CreateTemp("/tmp", "custom_*.yaml")
	assert.NoError(t, err)
	defer extraCfgFile.Close()
	_, err = extraCfgFile.WriteString(extraCfgContent)
	assert.NoError(t, err)
	config, err := NewConfiguration(cfgFile.Name(), extraCfgFile.Name())
	assert.NoError(t, err)
	assert.NotNil(t, config)

	// check configuration
	assert.Equal(t, "etcd", config.Woodpecker.Meta.Type)
	assert.Equal(t, "woodpecker", config.Woodpecker.Meta.Prefix)
	assert.Equal(t, 20000, config.Woodpecker.Client.SegmentAppend.QueueSize)
	assert.Equal(t, 22, config.Woodpecker.Client.SegmentAppend.MaxRetries)
	assert.Equal(t, int64(22220000000), config.Woodpecker.Client.SegmentRollingPolicy.MaxSize.Int64())
	assert.Equal(t, 2200, config.Woodpecker.Client.SegmentRollingPolicy.MaxInterval.Seconds())
	assert.Equal(t, int64(2000), config.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks)
	assert.Equal(t, 10, config.Woodpecker.Client.Auditor.MaxInterval.Seconds())
	assert.Equal(t, 2, len(config.Woodpecker.Client.Quorum.BufferPools))
	assert.Equal(t, "region-a-pool", config.Woodpecker.Client.Quorum.BufferPools[0].Name)
	assert.Equal(t, []string{"test-node1", "test-node2"}, config.Woodpecker.Client.Quorum.BufferPools[0].Seeds)
	assert.Equal(t, "region-b-pool", config.Woodpecker.Client.Quorum.BufferPools[1].Name)
	assert.Equal(t, []string{"test-node3", "test-node4", "test-node5"}, config.Woodpecker.Client.Quorum.BufferPools[1].Seeds)
	assert.Equal(t, "hard", config.Woodpecker.Client.Quorum.SelectStrategy.AffinityMode)
	assert.Equal(t, 5, config.Woodpecker.Client.Quorum.SelectStrategy.Replicas)
	assert.Equal(t, "custom", config.Woodpecker.Client.Quorum.SelectStrategy.Strategy)
	assert.Equal(t, 5, len(config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement))
	assert.Equal(t, "replica-1", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[0].Name)
	assert.Equal(t, "region-a-pool", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[0].Region)
	assert.Equal(t, "test-az-1", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[0].Az)
	assert.Equal(t, "test-rg-1", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[0].ResourceGroup)
	assert.Equal(t, "replica-2", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[1].Name)
	assert.Equal(t, "region-a-pool", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[1].Region)
	assert.Equal(t, "test-az-2", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[1].Az)
	assert.Equal(t, "test-rg-2", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[1].ResourceGroup)
	assert.Equal(t, "replica-3", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[2].Name)
	assert.Equal(t, "region-b-pool", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[2].Region)
	assert.Equal(t, "test-az-1", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[2].Az)
	assert.Equal(t, "test-rg-3", config.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement[2].ResourceGroup)
	// Test the getter methods for backward compatibility with overridden values
	assert.Equal(t, 5, config.Woodpecker.Client.Quorum.GetEnsembleSize())
	assert.Equal(t, 5, config.Woodpecker.Client.Quorum.GetWriteQuorumSize())
	assert.Equal(t, 3, config.Woodpecker.Client.Quorum.GetAckQuorumSize())
	assert.Equal(t, int64(32000000), config.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize.Int64())
	assert.Equal(t, 64, config.Woodpecker.Logstore.SegmentReadPolicy.MaxFetchThreads)
}

// TestQuorumConfigValidation tests the validation logic for quorum configuration
func TestQuorumConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      QuorumConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid configuration with strategy",
			config: QuorumConfig{
				SelectStrategy: QuorumSelectStrategy{
					AffinityMode: "soft",
					Replicas:     3,
					Strategy:     "single-az-single-rg",
				},
			},
			expectError: false,
		},
		{
			name: "Valid configuration with replicas=5",
			config: QuorumConfig{
				SelectStrategy: QuorumSelectStrategy{
					AffinityMode: "soft",
					Replicas:     5,
					Strategy:     "random",
				},
			},
			expectError: false,
		},
		{
			name: "Invalid affinity mode",
			config: QuorumConfig{
				SelectStrategy: QuorumSelectStrategy{
					AffinityMode: "invalid",
					Replicas:     3,
					Strategy:     "single-az-single-rg",
				},
			},
			expectError: true,
			errorMsg:    "invalid affinity mode 'invalid'",
		},
		{
			name: "Empty buffer pools",
			config: QuorumConfig{
				BufferPools: []QuorumBufferPool{}, // Empty buffer pools should cause error
				SelectStrategy: QuorumSelectStrategy{
					AffinityMode: "soft",
					Replicas:     3,
					Strategy:     "random",
				},
			},
			expectError: true,
			errorMsg:    "at least one buffer pool must be configured",
		},
		{
			name: "Valid custom placement configuration",
			config: QuorumConfig{
				BufferPools: []QuorumBufferPool{
					{Name: "us-east-1", Seeds: []string{"seed1:8080"}},
					{Name: "us-west-2", Seeds: []string{"seed2:8080"}},
				},
				SelectStrategy: QuorumSelectStrategy{
					AffinityMode: "hard",
					Replicas:     3,
					Strategy:     "custom",
					CustomPlacement: []CustomPlacement{
						{
							Name:          "replica-1",
							Region:        "us-east-1",
							Az:            "us-east-1a",
							ResourceGroup: "rg-1",
						},
						{
							Name:          "replica-2",
							Region:        "us-east-1",
							Az:            "us-east-1b",
							ResourceGroup: "rg-2",
						},
						{
							Name:          "replica-3",
							Region:        "us-west-2",
							Az:            "us-west-2a",
							ResourceGroup: "rg-3",
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a Configuration with the test QuorumConfig
			config := &Configuration{
				Woodpecker: WoodpeckerConfig{
					Meta: MetaConfig{
						Type:   "etcd",
						Prefix: "test",
					},
					Client: ClientConfig{
						SegmentAppend: SegmentAppendConfig{
							QueueSize:  100,
							MaxRetries: 2,
						},
						SegmentRollingPolicy: SegmentRollingPolicyConfig{
							MaxSize:     NewByteSize(100000000),
							MaxInterval: NewDurationSecondsFromInt(800),
							MaxBlocks:   1000,
						},
						Auditor: AuditorConfig{
							MaxInterval: NewDurationSecondsFromInt(5),
						},
						Quorum: tt.config,
					},
					Logstore: LogstoreConfig{
						SegmentSyncPolicy: SegmentSyncPolicyConfig{
							MaxInterval:                NewDurationMillisecondsFromInt(1000),
							MaxIntervalForLocalStorage: NewDurationMillisecondsFromInt(5),
							MaxEntries:                 2000,
							MaxBytes:                   NewByteSize(100000000),
							MaxFlushRetries:            3,
							RetryInterval:              NewDurationMillisecondsFromInt(2000),
							MaxFlushSize:               NewByteSize(16000000),
							MaxFlushThreads:            8,
						},
						SegmentCompactionPolicy: SegmentCompactionPolicy{
							MaxBytes:           NewByteSize(32000000),
							MaxParallelUploads: 4,
							MaxParallelReads:   8,
						},
						SegmentReadPolicy: SegmentReadPolicyConfig{
							MaxBatchSize:    NewByteSize(16000000),
							MaxFetchThreads: 32,
						},
					},
					Storage: StorageConfig{
						Type:     "default",
						RootPath: "/tmp/test",
					},
				},
			}

			// Set storage type to "service" to enable quorum validation
			config.Woodpecker.Storage.Type = "service"

			// Add required BufferPools for quorum config validation to pass
			// But skip for the "Empty buffer pools" test case
			if len(config.Woodpecker.Client.Quorum.BufferPools) == 0 && tt.name != "Empty buffer pools" {
				config.Woodpecker.Client.Quorum.BufferPools = []QuorumBufferPool{
					{
						Name:  "test-pool",
						Seeds: []string{"test-seed:8080"},
					},
				}
			}

			err := config.Validate()
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestQuorumConfigReplicasHandling tests the replica-based configuration
func TestQuorumConfigReplicasHandling(t *testing.T) {
	// Test with replicas = 3
	config3 := QuorumConfig{
		SelectStrategy: QuorumSelectStrategy{
			Replicas: 3,
		},
	}

	assert.Equal(t, 3, config3.GetEnsembleSize())
	assert.Equal(t, 3, config3.GetWriteQuorumSize())
	assert.Equal(t, 2, config3.GetAckQuorumSize()) // (3/2)+1 = 2

	// Test with replicas = 5
	config5 := QuorumConfig{
		SelectStrategy: QuorumSelectStrategy{
			Replicas: 5,
		},
	}

	assert.Equal(t, 5, config5.GetEnsembleSize())
	assert.Equal(t, 5, config5.GetWriteQuorumSize())
	assert.Equal(t, 3, config5.GetAckQuorumSize()) // (5/2)+1 = 3

	// Test with invalid replicas (should default to 3)
	configInvalid := QuorumConfig{
		SelectStrategy: QuorumSelectStrategy{
			Replicas: 4, // Invalid value
		},
	}

	assert.Equal(t, 3, configInvalid.GetEnsembleSize()) // Should default to 3
	assert.Equal(t, 3, configInvalid.GetWriteQuorumSize())
	assert.Equal(t, 2, configInvalid.GetAckQuorumSize()) // (3/2)+1 = 2
}

// TestCustomPlacementConfiguration tests the custom placement functionality
func TestCustomPlacementConfiguration(t *testing.T) {
	config := QuorumConfig{
		SelectStrategy: QuorumSelectStrategy{
			AffinityMode: "hard",
			Replicas:     5,
			Strategy:     "custom",
			CustomPlacement: []CustomPlacement{
				{
					Name:          "replica-1",
					Region:        "region-a",
					Az:            "az-1",
					ResourceGroup: "rg-primary",
				},
				{
					Name:          "replica-2",
					Region:        "region-a",
					Az:            "az-2",
					ResourceGroup: "rg-secondary",
				},
				{
					Name:          "replica-3",
					Region:        "region-b",
					Az:            "az-1",
					ResourceGroup: "rg-tertiary",
				},
				{
					Name:          "replica-4",
					Region:        "region-b",
					Az:            "az-2",
					ResourceGroup: "rg-backup",
				},
				{
					Name:          "replica-5",
					Region:        "region-c",
					Az:            "az-1",
					ResourceGroup: "rg-archive",
				},
			},
		},
	}

	// Test configuration validation via full Configuration
	fullConfig := &Configuration{
		Woodpecker: WoodpeckerConfig{
			Meta: MetaConfig{
				Type:   "etcd",
				Prefix: "test",
			},
			Client: ClientConfig{
				SegmentAppend: SegmentAppendConfig{
					QueueSize:  100,
					MaxRetries: 2,
				},
				SegmentRollingPolicy: SegmentRollingPolicyConfig{
					MaxSize:     NewByteSize(100000000),
					MaxInterval: NewDurationSecondsFromInt(800),
					MaxBlocks:   1000,
				},
				Auditor: AuditorConfig{
					MaxInterval: NewDurationSecondsFromInt(5),
				},
				Quorum: config,
			},
			Logstore: LogstoreConfig{
				SegmentSyncPolicy: SegmentSyncPolicyConfig{
					MaxInterval:                NewDurationMillisecondsFromInt(1000),
					MaxIntervalForLocalStorage: NewDurationMillisecondsFromInt(5),
					MaxEntries:                 2000,
					MaxBytes:                   NewByteSize(100000000),
					MaxFlushRetries:            3,
					RetryInterval:              NewDurationMillisecondsFromInt(2000),
					MaxFlushSize:               NewByteSize(16000000),
					MaxFlushThreads:            8,
				},
				SegmentCompactionPolicy: SegmentCompactionPolicy{
					MaxBytes:           NewByteSize(32000000),
					MaxParallelUploads: 4,
					MaxParallelReads:   8,
				},
				SegmentReadPolicy: SegmentReadPolicyConfig{
					MaxBatchSize:    NewByteSize(16000000),
					MaxFetchThreads: 32,
				},
			},
			Storage: StorageConfig{
				Type:     "default",
				RootPath: "/tmp/test",
			},
		},
	}

	// Set storage type to "service" to enable quorum validation
	fullConfig.Woodpecker.Storage.Type = "service"

	// Add buffer pools for the custom placement regions
	fullConfig.Woodpecker.Client.Quorum.BufferPools = []QuorumBufferPool{
		{Name: "region-a", Seeds: []string{"seed-a:8080"}},
		{Name: "region-b", Seeds: []string{"seed-b:8080"}},
		{Name: "region-c", Seeds: []string{"seed-c:8080"}},
	}

	err := fullConfig.Validate()
	assert.NoError(t, err)

	// Test quorum calculations
	assert.Equal(t, 5, config.GetEnsembleSize())
	assert.Equal(t, 5, config.GetWriteQuorumSize())
	assert.Equal(t, 3, config.GetAckQuorumSize())

	// Test custom placement structure
	assert.Equal(t, 5, len(config.SelectStrategy.CustomPlacement))

	// Test specific placement entries
	placement := config.SelectStrategy.CustomPlacement
	assert.Equal(t, "replica-1", placement[0].Name)
	assert.Equal(t, "region-a", placement[0].Region)
	assert.Equal(t, "az-1", placement[0].Az)
	assert.Equal(t, "rg-primary", placement[0].ResourceGroup)

	assert.Equal(t, "replica-5", placement[4].Name)
	assert.Equal(t, "region-c", placement[4].Region)
	assert.Equal(t, "az-1", placement[4].Az)
	assert.Equal(t, "rg-archive", placement[4].ResourceGroup)
}

// TestDirectReadConfig tests the DirectRead configuration defaults and YAML override
func TestDirectReadConfig(t *testing.T) {
	t.Run("default values", func(t *testing.T) {
		cfg, err := NewConfiguration()
		assert.NoError(t, err)

		dr := cfg.Woodpecker.Client.DirectRead
		assert.False(t, dr.Enabled)
		assert.Equal(t, int64(16*1024*1024), dr.MaxBatchSize.Int64()) // 16MB
		assert.Equal(t, 4, dr.MaxFetchThreads)
	})

	t.Run("yaml override", func(t *testing.T) {
		content := `woodpecker:
  meta:
    type: etcd
    prefix: woodpecker
  client:
    directRead:
      enabled: true
      maxBatchSize: 33554432
      maxFetchThreads: 8
  storage:
    type: default
    rootPath: /tmp/test`
		tmpFile, err := os.CreateTemp("", "direct_read_*.yaml")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		_, err = tmpFile.WriteString(content)
		assert.NoError(t, err)
		tmpFile.Close()

		cfg, err := NewConfiguration(tmpFile.Name())
		assert.NoError(t, err)

		dr := cfg.Woodpecker.Client.DirectRead
		assert.True(t, dr.Enabled)
		assert.Equal(t, int64(33554432), dr.MaxBatchSize.Int64()) // 32MB
		assert.Equal(t, 8, dr.MaxFetchThreads)
	})

	t.Run("partial override keeps defaults", func(t *testing.T) {
		content := `woodpecker:
  meta:
    type: etcd
    prefix: woodpecker
  client:
    directRead:
      enabled: true
  storage:
    type: default
    rootPath: /tmp/test`
		tmpFile, err := os.CreateTemp("", "direct_read_partial_*.yaml")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		_, err = tmpFile.WriteString(content)
		assert.NoError(t, err)
		tmpFile.Close()

		cfg, err := NewConfiguration(tmpFile.Name())
		assert.NoError(t, err)

		dr := cfg.Woodpecker.Client.DirectRead
		assert.True(t, dr.Enabled)
		// Non-overridden fields should keep defaults
		assert.Equal(t, int64(16*1024*1024), dr.MaxBatchSize.Int64())
		assert.Equal(t, 4, dr.MaxFetchThreads)
	})
}
