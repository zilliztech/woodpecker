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
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/common/config"
)

// UserSegmentAppendConfig represents the external segment append configuration format
type UserSegmentAppendConfig struct {
	QueueSize  int `yaml:"queueSize"`
	MaxRetries int `yaml:"maxRetries"`
}

// UserSegmentRollingPolicyConfig represents the external segment rolling policy configuration format
type UserSegmentRollingPolicyConfig struct {
	MaxSize     string `yaml:"maxSize"`     // e.g., "256M", "2G"
	MaxInterval string `yaml:"maxInterval"` // e.g., "10m", "1h"
	MaxBlocks   int64  `yaml:"maxBlocks"`
}

// UserAuditorConfig represents the external auditor configuration format
type UserAuditorConfig struct {
	MaxInterval string `yaml:"maxInterval"` // e.g., "10s"
}

// UserCustomPlacement represents the external custom placement configuration format
type UserCustomPlacement struct {
	Region        string `yaml:"region"`
	Az            string `yaml:"az"`
	ResourceGroup string `yaml:"resourceGroup"`
}

// UserQuorumSelectStrategy represents the external quorum selection strategy configuration format
type UserQuorumSelectStrategy struct {
	AffinityMode    string                         `yaml:"affinityMode"`
	Replicas        int                            `yaml:"replicas"`
	Strategy        string                         `yaml:"strategy"`
	CustomPlacement map[string]UserCustomPlacement `yaml:"customPlacement"` // map: replica1, replica2, etc.
}

// UserQuorumConfig represents the external quorum configuration format
type UserQuorumConfig struct {
	QuorumBufferPools    map[string]string        `yaml:"quorumBufferPools"` // map: region1 -> comma-separated seeds string
	QuorumSelectStrategy UserQuorumSelectStrategy `yaml:"quorumSelectStrategy"`
}

// UserClientConfig represents the external client configuration format
type UserClientConfig struct {
	SegmentAppend        UserSegmentAppendConfig        `yaml:"segmentAppend"`
	SegmentRollingPolicy UserSegmentRollingPolicyConfig `yaml:"segmentRollingPolicy"`
	Auditor              UserAuditorConfig              `yaml:"auditor"`
	Quorum               UserQuorumConfig               `yaml:"quorum"`
}

// UserSegmentSyncPolicyConfig represents the external segment sync policy configuration format
type UserSegmentSyncPolicyConfig struct {
	MaxInterval                string `yaml:"maxInterval"`                // e.g., "200ms"
	MaxIntervalForLocalStorage string `yaml:"maxIntervalForLocalStorage"` // e.g., "10ms"
	MaxBytes                   string `yaml:"maxBytes"`                   // e.g., "256M"
	MaxEntries                 int    `yaml:"maxEntries"`
	MaxFlushRetries            int    `yaml:"maxFlushRetries"`
	RetryInterval              string `yaml:"retryInterval"` // e.g., "1000ms"
	MaxFlushSize               string `yaml:"maxFlushSize"`  // e.g., "2M"
	MaxFlushThreads            int    `yaml:"maxFlushThreads"`
}

// UserSegmentCompactionPolicyConfig represents the external segment compaction policy configuration format
type UserSegmentCompactionPolicyConfig struct {
	MaxSize            string `yaml:"maxSize"` // e.g., "2M"
	MaxParallelUploads int    `yaml:"maxParallelUploads"`
	MaxParallelReads   int    `yaml:"maxParallelReads"`
}

// UserSegmentReadPolicyConfig represents the external segment read policy configuration format
type UserSegmentReadPolicyConfig struct {
	MaxBatchSize    string `yaml:"maxBatchSize"` // e.g., "16M"
	MaxFetchThreads int    `yaml:"maxFetchThreads"`
}

// UserLogstoreConfig represents the external logstore configuration format
type UserLogstoreConfig struct {
	SegmentSyncPolicy       UserSegmentSyncPolicyConfig       `yaml:"segmentSyncPolicy"`
	SegmentCompactionPolicy UserSegmentCompactionPolicyConfig `yaml:"segmentCompactionPolicy"`
	SegmentReadPolicy       UserSegmentReadPolicyConfig       `yaml:"segmentReadPolicy"`
}

// UserStorageConfig represents the external storage configuration format
type UserStorageConfig struct {
	Type     string `yaml:"type"`
	RootPath string `yaml:"rootPath"`
}

// UserWoodpeckerConfig represents the external woodpecker configuration format
type UserWoodpeckerConfig struct {
	Meta     config.MetaConfig  `yaml:"meta"`     // Reuse internal struct
	Client   UserClientConfig   `yaml:"client"`   // Custom due to unit formats
	Logstore UserLogstoreConfig `yaml:"logstore"` // Custom due to unit formats
	Storage  UserStorageConfig  `yaml:"storage"`  // Custom for flexibility
}

// UserLocalStorageConfig represents the local storage configuration
type UserLocalStorageConfig struct {
	Path string `yaml:"path"`
}

// UserMQConfig represents the message queue configuration
type UserMQConfig struct {
	Type string `yaml:"type"`
}

// UserConfig represents the external user configuration format
// This is compatible with Milvus-style YAML configuration
type UserConfig struct {
	Woodpecker   UserWoodpeckerConfig   `yaml:"woodpecker"`
	LocalStorage UserLocalStorageConfig `yaml:"localStorage"`
	MQ           UserMQConfig           `yaml:"mq"`
	Log          config.LogConfig       `yaml:"log"`   // Reuse internal struct
	Trace        config.TraceConfig     `yaml:"trace"` // Reuse internal struct
	Etcd         config.EtcdConfig      `yaml:"etcd"`  // Reuse internal struct
	Minio        config.MinioConfig     `yaml:"minio"` // Reuse internal struct
}

// LoadUserConfig loads external user configuration from YAML file(s)
// If a file does not exist, it will be skipped silently (returns empty config)
// This allows optional configuration files without causing errors
func LoadUserConfig(files ...string) (*UserConfig, error) {
	userConfig := &UserConfig{}

	if len(files) == 0 {
		return userConfig, nil
	}

	// Read and merge all configuration files
	for _, filePath := range files {
		data, err := os.ReadFile(filePath)
		if err != nil {
			// If file doesn't exist, skip it silently (optional config file)
			if os.IsNotExist(err) {
				continue
			}
			// For other errors (permission denied, etc.), return error
			return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
		}
		if len(data) == 0 {
			continue
		}

		err = yaml.Unmarshal(data, userConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to parse config file %s: %w", filePath, err)
		}
	}

	return userConfig, nil
}

// ApplyToConfig applies the user configuration to the internal Woodpecker configuration
// This method converts external configuration format to internal format and overrides settings
// It safely handles nil or empty user configurations
func (u *UserConfig) ApplyToConfig(cfg *config.Configuration) error {
	if cfg == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	// If user config is nil or empty, nothing to apply
	if u == nil {
		return nil
	}

	// Apply Woodpecker configuration
	if err := u.applyWoodpeckerConfig(&cfg.Woodpecker); err != nil {
		return fmt.Errorf("failed to apply woodpecker config: %w", err)
	}

	// Apply Log configuration (direct copy, already compatible)
	if u.Log.Level != "" {
		cfg.Log = u.Log
	}

	// Apply Trace configuration (direct copy, already compatible)
	if u.Trace.Exporter != "" {
		cfg.Trace = u.Trace
	}

	// Apply Etcd configuration (direct copy, already compatible)
	if len(u.Etcd.Endpoints) > 0 {
		cfg.Etcd = u.Etcd
	}

	// Apply Minio configuration (direct copy, already compatible)
	if u.Minio.Address != "" {
		cfg.Minio = u.Minio
	}

	return nil
}

// applyWoodpeckerConfig applies woodpecker-specific configuration
func (u *UserConfig) applyWoodpeckerConfig(cfg *config.WoodpeckerConfig) error {
	// Apply Meta configuration (direct copy, already compatible)
	if u.Woodpecker.Meta.Type != "" {
		cfg.Meta = u.Woodpecker.Meta
	}

	// Apply Client configuration
	if err := u.applyClientConfig(&cfg.Client); err != nil {
		return fmt.Errorf("failed to apply client config: %w", err)
	}

	// Apply Logstore configuration
	if err := u.applyLogstoreConfig(&cfg.Logstore); err != nil {
		return fmt.Errorf("failed to apply logstore config: %w", err)
	}

	// Apply Storage configuration
	if u.Woodpecker.Storage.Type != "" {
		cfg.Storage.Type = u.Woodpecker.Storage.Type
	}
	if u.Woodpecker.Storage.RootPath != "" {
		cfg.Storage.RootPath = u.Woodpecker.Storage.RootPath
	}

	return nil
}

// applyClientConfig applies client-specific configuration
func (u *UserConfig) applyClientConfig(cfg *config.ClientConfig) error {
	// Apply SegmentAppend configuration
	if u.Woodpecker.Client.SegmentAppend.QueueSize > 0 {
		cfg.SegmentAppend.QueueSize = u.Woodpecker.Client.SegmentAppend.QueueSize
	}
	if u.Woodpecker.Client.SegmentAppend.MaxRetries >= 0 {
		cfg.SegmentAppend.MaxRetries = u.Woodpecker.Client.SegmentAppend.MaxRetries
	}

	// Apply SegmentRollingPolicy configuration
	if u.Woodpecker.Client.SegmentRollingPolicy.MaxSize != "" {
		size, err := parseSize(u.Woodpecker.Client.SegmentRollingPolicy.MaxSize)
		if err != nil {
			return fmt.Errorf("failed to parse maxSize: %w", err)
		}
		cfg.SegmentRollingPolicy.MaxSize = size
	}
	if u.Woodpecker.Client.SegmentRollingPolicy.MaxInterval != "" {
		duration, err := parseDuration(u.Woodpecker.Client.SegmentRollingPolicy.MaxInterval, time.Second)
		if err != nil {
			return fmt.Errorf("failed to parse maxInterval: %w", err)
		}
		cfg.SegmentRollingPolicy.MaxInterval = int(duration.Seconds())
	}
	if u.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks > 0 {
		cfg.SegmentRollingPolicy.MaxBlocks = u.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks
	}

	// Apply Auditor configuration
	if u.Woodpecker.Client.Auditor.MaxInterval != "" {
		duration, err := parseDuration(u.Woodpecker.Client.Auditor.MaxInterval, time.Second)
		if err != nil {
			return fmt.Errorf("failed to parse auditor maxInterval: %w", err)
		}
		cfg.Auditor.MaxInterval = int(duration.Seconds())
	}

	// Apply Quorum configuration
	if err := u.applyQuorumConfig(&cfg.Quorum); err != nil {
		return fmt.Errorf("failed to apply quorum config: %w", err)
	}

	return nil
}

// applyQuorumConfig applies quorum-specific configuration
func (u *UserConfig) applyQuorumConfig(cfg *config.QuorumConfig) error {
	// Apply buffer pools (convert from map with comma-separated strings to slice)
	if len(u.Woodpecker.Client.Quorum.QuorumBufferPools) > 0 {
		cfg.BufferPools = make([]config.QuorumBufferPool, 0, len(u.Woodpecker.Client.Quorum.QuorumBufferPools))
		for name, seedsStr := range u.Woodpecker.Client.Quorum.QuorumBufferPools {
			// Skip empty seed strings
			if seedsStr == "" {
				continue
			}
			// Parse comma-separated seeds
			seeds := parseSeeds(seedsStr)
			if len(seeds) > 0 {
				cfg.BufferPools = append(cfg.BufferPools, config.QuorumBufferPool{
					Name:  name,
					Seeds: seeds,
				})
			}
		}
	}

	// Apply select strategy
	strategy := &u.Woodpecker.Client.Quorum.QuorumSelectStrategy
	if strategy.AffinityMode != "" {
		cfg.SelectStrategy.AffinityMode = strategy.AffinityMode
	}
	if strategy.Replicas > 0 {
		cfg.SelectStrategy.Replicas = strategy.Replicas
	}
	if strategy.Strategy != "" {
		cfg.SelectStrategy.Strategy = strategy.Strategy
	}

	// Apply custom placement (convert from map to slice)
	if len(strategy.CustomPlacement) > 0 {
		cfg.SelectStrategy.CustomPlacement = make([]config.CustomPlacement, 0, len(strategy.CustomPlacement))
		for name, placement := range strategy.CustomPlacement {
			cfg.SelectStrategy.CustomPlacement = append(cfg.SelectStrategy.CustomPlacement, config.CustomPlacement{
				Name:          name, // Use the map key as the name (replica1, replica2, etc.)
				Region:        placement.Region,
				Az:            placement.Az,
				ResourceGroup: placement.ResourceGroup,
			})
		}
	}

	return nil
}

// applyLogstoreConfig applies logstore-specific configuration
func (u *UserConfig) applyLogstoreConfig(cfg *config.LogstoreConfig) error {
	// Apply SegmentSyncPolicy configuration
	syncPolicy := &u.Woodpecker.Logstore.SegmentSyncPolicy
	if syncPolicy.MaxInterval != "" {
		duration, err := parseDuration(syncPolicy.MaxInterval, time.Millisecond)
		if err != nil {
			return fmt.Errorf("failed to parse sync policy maxInterval: %w", err)
		}
		cfg.SegmentSyncPolicy.MaxInterval = int(duration.Milliseconds())
	}
	if syncPolicy.MaxIntervalForLocalStorage != "" {
		duration, err := parseDuration(syncPolicy.MaxIntervalForLocalStorage, time.Millisecond)
		if err != nil {
			return fmt.Errorf("failed to parse sync policy maxIntervalForLocalStorage: %w", err)
		}
		cfg.SegmentSyncPolicy.MaxIntervalForLocalStorage = int(duration.Milliseconds())
	}
	if syncPolicy.MaxBytes != "" {
		size, err := parseSize(syncPolicy.MaxBytes)
		if err != nil {
			return fmt.Errorf("failed to parse sync policy maxBytes: %w", err)
		}
		cfg.SegmentSyncPolicy.MaxBytes = size
	}
	if syncPolicy.MaxEntries > 0 {
		cfg.SegmentSyncPolicy.MaxEntries = syncPolicy.MaxEntries
	}
	if syncPolicy.MaxFlushRetries >= 0 {
		cfg.SegmentSyncPolicy.MaxFlushRetries = syncPolicy.MaxFlushRetries
	}
	if syncPolicy.RetryInterval != "" {
		duration, err := parseDuration(syncPolicy.RetryInterval, time.Millisecond)
		if err != nil {
			return fmt.Errorf("failed to parse sync policy retryInterval: %w", err)
		}
		cfg.SegmentSyncPolicy.RetryInterval = int(duration.Milliseconds())
	}
	if syncPolicy.MaxFlushSize != "" {
		size, err := parseSize(syncPolicy.MaxFlushSize)
		if err != nil {
			return fmt.Errorf("failed to parse sync policy maxFlushSize: %w", err)
		}
		cfg.SegmentSyncPolicy.MaxFlushSize = size
	}
	if syncPolicy.MaxFlushThreads > 0 {
		cfg.SegmentSyncPolicy.MaxFlushThreads = syncPolicy.MaxFlushThreads
	}

	// Apply SegmentCompactionPolicy configuration
	compactionPolicy := &u.Woodpecker.Logstore.SegmentCompactionPolicy
	if compactionPolicy.MaxSize != "" {
		size, err := parseSize(compactionPolicy.MaxSize)
		if err != nil {
			return fmt.Errorf("failed to parse compaction policy maxSize: %w", err)
		}
		cfg.SegmentCompactionPolicy.MaxBytes = size
	}
	if compactionPolicy.MaxParallelUploads > 0 {
		cfg.SegmentCompactionPolicy.MaxParallelUploads = compactionPolicy.MaxParallelUploads
	}
	if compactionPolicy.MaxParallelReads > 0 {
		cfg.SegmentCompactionPolicy.MaxParallelReads = compactionPolicy.MaxParallelReads
	}

	// Apply SegmentReadPolicy configuration
	readPolicy := &u.Woodpecker.Logstore.SegmentReadPolicy
	if readPolicy.MaxBatchSize != "" {
		size, err := parseSize(readPolicy.MaxBatchSize)
		if err != nil {
			return fmt.Errorf("failed to parse read policy maxBatchSize: %w", err)
		}
		cfg.SegmentReadPolicy.MaxBatchSize = size
	}
	if readPolicy.MaxFetchThreads > 0 {
		cfg.SegmentReadPolicy.MaxFetchThreads = readPolicy.MaxFetchThreads
	}

	return nil
}

// Helper functions for parsing various formats

// parseSeeds parses comma-separated seed addresses into a slice
func parseSeeds(seeds string) []string {
	if seeds == "" {
		return nil
	}
	var result []string
	for _, seed := range strings.Split(seeds, ",") {
		if trimmed := strings.TrimSpace(seed); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// parseSize parses size strings like "256M", "2GB", "1024" (bytes)
// Supports: G/GB, M/MB, K/KB, or plain numbers
func parseSize(sizeStr string) (int64, error) {
	if sizeStr == "" {
		return 0, nil
	}

	valueStr := strings.ToLower(strings.TrimSpace(sizeStr))
	if valueStr == "" {
		return 0, nil
	}

	// Handle GB/G suffix
	if strings.HasSuffix(valueStr, "gb") || strings.HasSuffix(valueStr, "g") {
		numStr := strings.Split(valueStr, "g")[0]
		size, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid size format '%s': %w", sizeStr, err)
		}
		return size * 1024 * 1024 * 1024, nil
	}

	// Handle MB/M suffix
	if strings.HasSuffix(valueStr, "mb") || strings.HasSuffix(valueStr, "m") {
		numStr := strings.Split(valueStr, "m")[0]
		size, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid size format '%s': %w", sizeStr, err)
		}
		return size * 1024 * 1024, nil
	}

	// Handle KB/K suffix
	if strings.HasSuffix(valueStr, "kb") || strings.HasSuffix(valueStr, "k") {
		numStr := strings.Split(valueStr, "k")[0]
		size, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid size format '%s': %w", sizeStr, err)
		}
		return size * 1024, nil
	}

	// Handle TB/T suffix
	if strings.HasSuffix(valueStr, "tb") || strings.HasSuffix(valueStr, "t") {
		numStr := strings.Split(valueStr, "t")[0]
		size, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid size format '%s': %w", sizeStr, err)
		}
		return size * 1024 * 1024 * 1024 * 1024, nil
	}

	// No unit, parse as plain number (bytes)
	size, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size format '%s': %w", sizeStr, err)
	}
	return size, nil
}

// parseDuration parses duration strings and returns time.Duration
// Uses Go's standard time.ParseDuration for better compatibility
// Also supports plain numbers with a default unit for backward compatibility
func parseDuration(durationStr string, defaultUnit time.Duration) (time.Duration, error) {
	if durationStr == "" {
		return 0, nil
	}

	durationStr = strings.TrimSpace(durationStr)
	if durationStr == "" {
		return 0, nil
	}

	// Try to parse as plain number first (for backward compatibility)
	if value, err := strconv.ParseInt(durationStr, 10, 64); err == nil {
		return time.Duration(value) * defaultUnit, nil
	}

	// Try to parse as Go duration format (supports ns, us, ms, s, m, h)
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		return 0, fmt.Errorf("invalid duration format '%s': %w", durationStr, err)
	}

	return duration, nil
}
