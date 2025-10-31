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
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/common/config"
)

// LocalStorageConfig stores the local storage configuration
type LocalStorageConfig struct {
	Path string `yaml:"path"`
}

// UserConfig stores the user-provided configuration that can override the default configuration
// It only includes the configurations we care about: etcd, localStorage, minio, woodpecker, trace, log
type UserConfig struct {
	Etcd         *config.EtcdConfig       `yaml:"etcd"`
	LocalStorage *LocalStorageConfig      `yaml:"localStorage"`
	Minio        *config.MinioConfig      `yaml:"minio"`
	Woodpecker   *config.WoodpeckerConfig `yaml:"woodpecker"`
	Trace        *config.TraceConfig      `yaml:"trace"`
	Log          *config.LogConfig        `yaml:"log"`
}

// ApplyToConfig applies the user configuration to the given configuration, only overriding non-zero values
func (c *UserConfig) ApplyToConfig(cfg *config.Configuration) error {
	if c.Etcd != nil {
		mergeEtcdConfig(&cfg.Etcd, c.Etcd)
	}

	if c.Minio != nil {
		mergeMinioConfig(&cfg.Minio, c.Minio)
	}

	if c.Woodpecker != nil {
		mergeWoodpeckerConfig(&cfg.Woodpecker, c.Woodpecker)
	}

	if c.Trace != nil {
		mergeTraceConfig(&cfg.Trace, c.Trace)
	}

	if c.Log != nil {
		mergeLogConfig(&cfg.Log, c.Log)
	}

	return nil
}

// LoadUserConfig loads user configuration from YAML files.
// Multiple files can be provided, and they will be merged in order (later files override earlier ones).
// If a file does not exist, it will be skipped without error.
// If all files do not exist, an empty UserConfig will be returned.
func LoadUserConfig(files ...string) (*UserConfig, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no configuration files provided")
	}

	userConfig := &UserConfig{}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			// Skip if file does not exist
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("failed to read config file %s: %w", file, err)
		}

		// Skip empty files
		if len(data) == 0 {
			continue
		}

		tempConfig := &UserConfig{}
		if err := yaml.Unmarshal(data, tempConfig); err != nil {
			return nil, fmt.Errorf("failed to parse config file %s: %w", file, err)
		}

		// Merge tempConfig into userConfig
		if tempConfig.Etcd != nil {
			if userConfig.Etcd == nil {
				userConfig.Etcd = &config.EtcdConfig{}
			}
			mergeEtcdConfig(userConfig.Etcd, tempConfig.Etcd)
		}

		if tempConfig.LocalStorage != nil {
			if userConfig.LocalStorage == nil {
				userConfig.LocalStorage = &LocalStorageConfig{}
			}
			if tempConfig.LocalStorage.Path != "" {
				userConfig.LocalStorage.Path = tempConfig.LocalStorage.Path
			}
		}

		if tempConfig.Minio != nil {
			if userConfig.Minio == nil {
				userConfig.Minio = &config.MinioConfig{}
			}
			mergeMinioConfig(userConfig.Minio, tempConfig.Minio)
		}

		if tempConfig.Woodpecker != nil {
			if userConfig.Woodpecker == nil {
				userConfig.Woodpecker = &config.WoodpeckerConfig{}
			}
			mergeWoodpeckerConfig(userConfig.Woodpecker, tempConfig.Woodpecker)
		}

		if tempConfig.Trace != nil {
			if userConfig.Trace == nil {
				userConfig.Trace = &config.TraceConfig{}
			}
			mergeTraceConfig(userConfig.Trace, tempConfig.Trace)
		}

		if tempConfig.Log != nil {
			if userConfig.Log == nil {
				userConfig.Log = &config.LogConfig{}
			}
			mergeLogConfig(userConfig.Log, tempConfig.Log)
		}
	}

	return userConfig, nil
}

// mergeEtcdConfig merges src into dst, only overriding non-zero values
func mergeEtcdConfig(dst, src *config.EtcdConfig) {
	if len(src.Endpoints) > 0 {
		dst.Endpoints = src.Endpoints
	}
	if src.RootPath != "" {
		dst.RootPath = src.RootPath
	}
	if src.MetaSubPath != "" {
		dst.MetaSubPath = src.MetaSubPath
	}
	if src.KvSubPath != "" {
		dst.KvSubPath = src.KvSubPath
	}
	if src.RequestTimeout.Milliseconds() > 0 {
		dst.RequestTimeout = src.RequestTimeout
	}

	// Merge nested structs
	if src.Log.Level != "" {
		dst.Log.Level = src.Log.Level
	}
	if src.Log.Path != "" {
		dst.Log.Path = src.Log.Path
	}

	if src.Ssl.TlsCert != "" {
		dst.Ssl.TlsCert = src.Ssl.TlsCert
	}
	if src.Ssl.TlsKey != "" {
		dst.Ssl.TlsKey = src.Ssl.TlsKey
	}
	if src.Ssl.TlsCACert != "" {
		dst.Ssl.TlsCACert = src.Ssl.TlsCACert
	}
	if src.Ssl.TlsMinVersion != "" {
		dst.Ssl.TlsMinVersion = src.Ssl.TlsMinVersion
	}
	// Note: Enabled is bool, needs special handling
	dst.Ssl.Enabled = src.Ssl.Enabled

	if src.Use.Embed {
		dst.Use.Embed = src.Use.Embed
	}

	if src.Data.Dir != "" {
		dst.Data.Dir = src.Data.Dir
	}

	if src.Auth.UserName != "" {
		dst.Auth.UserName = src.Auth.UserName
	}
	if src.Auth.Password != "" {
		dst.Auth.Password = src.Auth.Password
	}
	dst.Auth.Enabled = src.Auth.Enabled
}

// mergeMinioConfig merges src into dst, only overriding non-zero values
func mergeMinioConfig(dst, src *config.MinioConfig) {
	if src.Address != "" {
		dst.Address = src.Address
	}
	if src.Port > 0 {
		dst.Port = src.Port
	}
	if src.AccessKeyID != "" {
		dst.AccessKeyID = src.AccessKeyID
	}
	if src.SecretAccessKey != "" {
		dst.SecretAccessKey = src.SecretAccessKey
	}
	if src.BucketName != "" {
		dst.BucketName = src.BucketName
	}
	if src.RootPath != "" {
		dst.RootPath = src.RootPath
	}
	if src.CloudProvider != "" {
		dst.CloudProvider = src.CloudProvider
	}
	if src.GcpCredentialJSON != "" {
		dst.GcpCredentialJSON = src.GcpCredentialJSON
	}
	if src.IamEndpoint != "" {
		dst.IamEndpoint = src.IamEndpoint
	}
	if src.LogLevel != "" {
		dst.LogLevel = src.LogLevel
	}
	if src.Region != "" {
		dst.Region = src.Region
	}
	if src.RequestTimeoutMs.Milliseconds() > 0 {
		dst.RequestTimeoutMs = src.RequestTimeoutMs
	}
	if src.ListObjectsMaxKeys > 0 {
		dst.ListObjectsMaxKeys = src.ListObjectsMaxKeys
	}

	// Bool fields
	dst.UseSSL = src.UseSSL
	dst.CreateBucket = src.CreateBucket
	dst.UseIAM = src.UseIAM
	dst.UseVirtualHost = src.UseVirtualHost

	// Nested struct
	if src.Ssl.TlsCACert != "" {
		dst.Ssl.TlsCACert = src.Ssl.TlsCACert
	}
}

// mergeWoodpeckerConfig merges src into dst, only overriding non-zero values
func mergeWoodpeckerConfig(dst, src *config.WoodpeckerConfig) {
	// Meta
	if src.Meta.Type != "" {
		dst.Meta.Type = src.Meta.Type
	}
	if src.Meta.Prefix != "" {
		dst.Meta.Prefix = src.Meta.Prefix
	}

	// Client
	if src.Client.SegmentAppend.QueueSize > 0 {
		dst.Client.SegmentAppend.QueueSize = src.Client.SegmentAppend.QueueSize
	}
	if src.Client.SegmentAppend.MaxRetries > 0 {
		dst.Client.SegmentAppend.MaxRetries = src.Client.SegmentAppend.MaxRetries
	}

	if src.Client.SegmentRollingPolicy.MaxSize.Int64() > 0 {
		dst.Client.SegmentRollingPolicy.MaxSize = src.Client.SegmentRollingPolicy.MaxSize
	}
	if src.Client.SegmentRollingPolicy.MaxInterval.Seconds() > 0 {
		dst.Client.SegmentRollingPolicy.MaxInterval = src.Client.SegmentRollingPolicy.MaxInterval
	}
	if src.Client.SegmentRollingPolicy.MaxBlocks > 0 {
		dst.Client.SegmentRollingPolicy.MaxBlocks = src.Client.SegmentRollingPolicy.MaxBlocks
	}

	if src.Client.Auditor.MaxInterval.Seconds() > 0 {
		dst.Client.Auditor.MaxInterval = src.Client.Auditor.MaxInterval
	}

	// Quorum
	if len(src.Client.Quorum.BufferPools) > 0 {
		dst.Client.Quorum.BufferPools = src.Client.Quorum.BufferPools
	}
	if src.Client.Quorum.SelectStrategy.AffinityMode != "" {
		dst.Client.Quorum.SelectStrategy.AffinityMode = src.Client.Quorum.SelectStrategy.AffinityMode
	}
	if src.Client.Quorum.SelectStrategy.Replicas > 0 {
		dst.Client.Quorum.SelectStrategy.Replicas = src.Client.Quorum.SelectStrategy.Replicas
	}
	if src.Client.Quorum.SelectStrategy.Strategy != "" {
		dst.Client.Quorum.SelectStrategy.Strategy = src.Client.Quorum.SelectStrategy.Strategy
	}
	if len(src.Client.Quorum.SelectStrategy.CustomPlacement) > 0 {
		dst.Client.Quorum.SelectStrategy.CustomPlacement = src.Client.Quorum.SelectStrategy.CustomPlacement
	}

	// Logstore - SegmentSyncPolicy
	if src.Logstore.SegmentSyncPolicy.MaxInterval.Milliseconds() > 0 {
		dst.Logstore.SegmentSyncPolicy.MaxInterval = src.Logstore.SegmentSyncPolicy.MaxInterval
	}
	if src.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage.Milliseconds() > 0 {
		dst.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage = src.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage
	}
	if src.Logstore.SegmentSyncPolicy.MaxEntries > 0 {
		dst.Logstore.SegmentSyncPolicy.MaxEntries = src.Logstore.SegmentSyncPolicy.MaxEntries
	}
	if src.Logstore.SegmentSyncPolicy.MaxBytes.Int64() > 0 {
		dst.Logstore.SegmentSyncPolicy.MaxBytes = src.Logstore.SegmentSyncPolicy.MaxBytes
	}
	if src.Logstore.SegmentSyncPolicy.MaxFlushRetries > 0 {
		dst.Logstore.SegmentSyncPolicy.MaxFlushRetries = src.Logstore.SegmentSyncPolicy.MaxFlushRetries
	}
	if src.Logstore.SegmentSyncPolicy.RetryInterval.Milliseconds() > 0 {
		dst.Logstore.SegmentSyncPolicy.RetryInterval = src.Logstore.SegmentSyncPolicy.RetryInterval
	}
	if src.Logstore.SegmentSyncPolicy.MaxFlushSize.Int64() > 0 {
		dst.Logstore.SegmentSyncPolicy.MaxFlushSize = src.Logstore.SegmentSyncPolicy.MaxFlushSize
	}
	if src.Logstore.SegmentSyncPolicy.MaxFlushThreads > 0 {
		dst.Logstore.SegmentSyncPolicy.MaxFlushThreads = src.Logstore.SegmentSyncPolicy.MaxFlushThreads
	}

	// Logstore - SegmentCompactionPolicy
	if src.Logstore.SegmentCompactionPolicy.MaxBytes.Int64() > 0 {
		dst.Logstore.SegmentCompactionPolicy.MaxBytes = src.Logstore.SegmentCompactionPolicy.MaxBytes
	}
	if src.Logstore.SegmentCompactionPolicy.MaxParallelUploads > 0 {
		dst.Logstore.SegmentCompactionPolicy.MaxParallelUploads = src.Logstore.SegmentCompactionPolicy.MaxParallelUploads
	}
	if src.Logstore.SegmentCompactionPolicy.MaxParallelReads > 0 {
		dst.Logstore.SegmentCompactionPolicy.MaxParallelReads = src.Logstore.SegmentCompactionPolicy.MaxParallelReads
	}

	// Logstore - SegmentReadPolicy
	if src.Logstore.SegmentReadPolicy.MaxBatchSize.Int64() > 0 {
		dst.Logstore.SegmentReadPolicy.MaxBatchSize = src.Logstore.SegmentReadPolicy.MaxBatchSize
	}
	if src.Logstore.SegmentReadPolicy.MaxFetchThreads > 0 {
		dst.Logstore.SegmentReadPolicy.MaxFetchThreads = src.Logstore.SegmentReadPolicy.MaxFetchThreads
	}

	// Storage
	if src.Storage.Type != "" {
		dst.Storage.Type = src.Storage.Type
	}
	if src.Storage.RootPath != "" {
		dst.Storage.RootPath = src.Storage.RootPath
	}
}

// mergeTraceConfig merges src into dst, only overriding non-zero values
func mergeTraceConfig(dst, src *config.TraceConfig) {
	if src.Exporter != "" {
		dst.Exporter = src.Exporter
	}
	if src.SampleFraction > 0 {
		dst.SampleFraction = src.SampleFraction
	}
	if src.InitTimeout.Seconds() > 0 {
		dst.InitTimeout = src.InitTimeout
	}

	// Jaeger
	if src.Jaeger.URL != "" {
		dst.Jaeger.URL = src.Jaeger.URL
	}

	// Otlp
	if src.Otlp.Endpoint != "" {
		dst.Otlp.Endpoint = src.Otlp.Endpoint
	}
	if src.Otlp.Method != "" {
		dst.Otlp.Method = src.Otlp.Method
	}
	dst.Otlp.Secure = src.Otlp.Secure
}

// mergeLogConfig merges src into dst, only overriding non-zero values
func mergeLogConfig(dst, src *config.LogConfig) {
	if src.Level != "" {
		dst.Level = src.Level
	}
	if src.Format != "" {
		dst.Format = src.Format
	}
	dst.Stdout = src.Stdout

	// File
	if src.File.RootPath != "" {
		dst.File.RootPath = src.File.RootPath
	}
	if src.File.MaxSize > 0 {
		dst.File.MaxSize = src.File.MaxSize
	}
	if src.File.MaxAge > 0 {
		dst.File.MaxAge = src.File.MaxAge
	}
	if src.File.MaxBackups > 0 {
		dst.File.MaxBackups = src.File.MaxBackups
	}
}
