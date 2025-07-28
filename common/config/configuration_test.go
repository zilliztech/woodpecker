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
	assert.Equal(t, int64(256000000), config.Woodpecker.Client.SegmentRollingPolicy.MaxSize)
	assert.Equal(t, 600, config.Woodpecker.Client.SegmentRollingPolicy.MaxInterval)
	assert.Equal(t, int64(1000), config.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks)
	assert.Equal(t, 10, config.Woodpecker.Client.Auditor.MaxInterval)
	assert.Equal(t, 200, config.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval)
	assert.Equal(t, 10, config.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage)
	assert.Equal(t, 10000, config.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries)
	assert.Equal(t, int64(256000000), config.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes)
	assert.Equal(t, 5, config.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushRetries)
	assert.Equal(t, 1000, config.Woodpecker.Logstore.SegmentSyncPolicy.RetryInterval)
	assert.Equal(t, int64(2000000), config.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize)
	assert.Equal(t, 32, config.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads)
	assert.Equal(t, int64(2000000), config.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes)
	assert.Equal(t, 4, config.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelUploads)
	assert.Equal(t, 8, config.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelReads)
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
	assert.Equal(t, 10, config.Trace.InitTimeout)
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
	assert.Equal(t, 10000, config.Etcd.RequestTimeout)
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
	assert.Equal(t, 10000, config.Minio.RequestTimeoutMs)
	assert.Equal(t, 0, config.Minio.ListObjectsMaxKeys)

	defaultConfig, err := NewConfiguration()
	assert.NoError(t, err)

	// test default configuration
	assert.Equal(t, "etcd", defaultConfig.Woodpecker.Meta.Type)
	assert.Equal(t, "woodpecker", defaultConfig.Woodpecker.Meta.Prefix)
	assert.Equal(t, 100, defaultConfig.Woodpecker.Client.SegmentAppend.QueueSize)
	assert.Equal(t, 2, defaultConfig.Woodpecker.Client.SegmentAppend.MaxRetries)
	assert.Equal(t, int64(100000000), defaultConfig.Woodpecker.Client.SegmentRollingPolicy.MaxSize)
	assert.Equal(t, 800, defaultConfig.Woodpecker.Client.SegmentRollingPolicy.MaxInterval)
	assert.Equal(t, int64(1000), defaultConfig.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks)
	assert.Equal(t, 5, defaultConfig.Woodpecker.Client.Auditor.MaxInterval)
	assert.Equal(t, 1000, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval)
	assert.Equal(t, 5, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage)
	assert.Equal(t, 2000, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries)
	assert.Equal(t, int64(100000000), defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes)
	assert.Equal(t, 3, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushRetries)
	assert.Equal(t, 2000, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.RetryInterval)
	assert.Equal(t, int64(16000000), defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize)
	assert.Equal(t, 8, defaultConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads)
	assert.Equal(t, int64(32000000), defaultConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes)
	assert.Equal(t, 4, defaultConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelUploads)
	assert.Equal(t, 8, defaultConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelReads)
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
	assert.Equal(t, 10, defaultConfig.Trace.InitTimeout)
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
	assert.Equal(t, 10, defaultConfig.Etcd.RequestTimeout)
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
	assert.Equal(t, 1000, defaultConfig.Minio.RequestTimeoutMs)
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
      maxInterval: 10`
	extraCfgFile, err := os.CreateTemp("/tmp", "custom_*.yaml")
	defer extraCfgFile.Close()
	assert.NoError(t, err)
	_, err = extraCfgFile.WriteString(extraCfgContent)
	assert.NoError(t, err)
	config, err := NewConfiguration(cfgFile.Name(), extraCfgFile.Name())

	// check configuration
	assert.Equal(t, "etcd", config.Woodpecker.Meta.Type)
	assert.Equal(t, "woodpecker", config.Woodpecker.Meta.Prefix)
	assert.Equal(t, 20000, config.Woodpecker.Client.SegmentAppend.QueueSize)
	assert.Equal(t, 22, config.Woodpecker.Client.SegmentAppend.MaxRetries)
	assert.Equal(t, int64(22220000000), config.Woodpecker.Client.SegmentRollingPolicy.MaxSize)
	assert.Equal(t, 2200, config.Woodpecker.Client.SegmentRollingPolicy.MaxInterval)
	assert.Equal(t, int64(2000), config.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks)
	assert.Equal(t, 10, config.Woodpecker.Client.Auditor.MaxInterval)
}
