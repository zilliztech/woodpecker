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
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// MetaConfig stores the metadata storage configuration.
type MetaConfig struct {
	Type   string `yaml:"type"`
	Prefix string `yaml:"prefix"`
}

// SegmentRollingPolicyConfig stores the segment rolling policy configuration.
type SegmentRollingPolicyConfig struct {
	MaxSize     ByteSize        `yaml:"maxSize"`
	MaxInterval DurationSeconds `yaml:"maxInterval"`
	MaxBlocks   int64           `yaml:"maxBlocks"`
}

type SegmentAppendConfig struct {
	QueueSize  int `yaml:"queueSize"`
	MaxRetries int `yaml:"maxRetries"`
}

// ClientConfig stores the client configuration.
type ClientConfig struct {
	SegmentAppend        SegmentAppendConfig        `yaml:"segmentAppend"`
	SegmentRollingPolicy SegmentRollingPolicyConfig `yaml:"segmentRollingPolicy"`
	Auditor              AuditorConfig              `yaml:"auditor"`
	Quorum               QuorumConfig               `yaml:"quorum"`
}

type AuditorConfig struct {
	MaxInterval DurationSeconds `yaml:"maxInterval"`
}

// QuorumBufferPool stores the quorum buffer pool configuration.
type QuorumBufferPool struct {
	Name  string   `yaml:"name"`
	Seeds []string `yaml:"seeds"`
}

// CustomPlacement stores the custom node placement for a specific replica.
type CustomPlacement struct {
	Name          string `yaml:"name"`
	Region        string `yaml:"region"`
	Az            string `yaml:"az"`
	ResourceGroup string `yaml:"resourceGroup"`
}

// QuorumSelectStrategy stores the quorum selection strategy configuration.
type QuorumSelectStrategy struct {
	AffinityMode    string            `yaml:"affinityMode"`
	Replicas        int               `yaml:"replicas"`
	Strategy        string            `yaml:"strategy"`
	CustomPlacement []CustomPlacement `yaml:"customPlacement"`
}

// QuorumConfig stores the advanced quorum configuration.
type QuorumConfig struct {
	BufferPools    []QuorumBufferPool   `yaml:"quorumBufferPools"`
	SelectStrategy QuorumSelectStrategy `yaml:"quorumSelectStrategy"`
}

// GetEnsembleSize returns the ensemble size.
func (q *QuorumConfig) GetEnsembleSize() int {
	if q.SelectStrategy.Replicas == 3 {
		return 3
	}
	if q.SelectStrategy.Replicas == 5 {
		return 5
	}
	return 3
}

// GetWriteQuorumSize returns the write quorum size.
func (q *QuorumConfig) GetWriteQuorumSize() int {
	return q.GetEnsembleSize()
}

// GetAckQuorumSize returns the ack quorum size.
func (q *QuorumConfig) GetAckQuorumSize() int {
	return (q.GetWriteQuorumSize() / 2) + 1
}

// SegmentReadPolicyConfig stores the segment read policy configuration.
type SegmentReadPolicyConfig struct {
	MaxBatchSize    ByteSize `yaml:"maxBatchSize"`
	MaxFetchThreads int      `yaml:"maxFetchThreads"`
}

// SegmentSyncPolicyConfig stores the log file sync policy configuration.
type SegmentSyncPolicyConfig struct {
	MaxInterval                DurationMilliseconds `yaml:"maxInterval"`
	MaxIntervalForLocalStorage DurationMilliseconds `yaml:"maxIntervalForLocalStorage"`
	MaxEntries                 int                  `yaml:"maxEntries"`
	MaxBytes                   ByteSize             `yaml:"maxBytes"`
	MaxFlushRetries            int                  `yaml:"maxFlushRetries"`
	RetryInterval              DurationMilliseconds `yaml:"retryInterval"`
	MaxFlushSize               ByteSize             `yaml:"maxFlushSize"`
	MaxFlushThreads            int                  `yaml:"maxFlushThreads"`
}

type SegmentCompactionPolicy struct {
	MaxBytes           ByteSize `yaml:"maxBytes"`
	MaxParallelUploads int      `yaml:"maxParallelUploads"`
	MaxParallelReads   int      `yaml:"maxParallelReads"`
}

// RetentionPolicyConfig stores the data retention policy configuration.
type RetentionPolicyConfig struct {
	TTL int `yaml:"ttl"` // Time to live for truncated segments before eligible for GC
}

// FencePolicyConfig stores the fence policy configuration.
type FencePolicyConfig struct {
	ConditionWrite string `yaml:"conditionWrite"`
}

func (f *FencePolicyConfig) IsConditionWriteEnabled() bool {
	return strings.EqualFold(f.ConditionWrite, "enable")
}

func (f *FencePolicyConfig) IsConditionWriteDisabled() bool {
	return strings.EqualFold(f.ConditionWrite, "disable")
}

func (f *FencePolicyConfig) IsConditionWriteAuto() bool {
	return strings.EqualFold(f.ConditionWrite, "auto")
}

func (f *FencePolicyConfig) SetConditionWriteEnableOrNot(enable bool) {
	if enable {
		f.ConditionWrite = "enable"
	} else {
		f.ConditionWrite = "disable"
	}
}

// LogFileConfig stores the log file configuration.
type LogFileConfig struct {
	RootPath   string `yaml:"rootPath"`
	MaxSize    int    `yaml:"maxSize"`
	MaxAge     int    `yaml:"maxAge"`
	MaxBackups int    `yaml:"maxBackups"`
}

// LogConfig stores the log configuration.
type LogConfig struct {
	Level  string        `yaml:"level"`
	File   LogFileConfig `yaml:"file"`
	Format string        `yaml:"format"`
	Stdout bool          `yaml:"stdout"`
}

// JaegerConfig stores the Jaeger configuration.
type JaegerConfig struct {
	URL string `yaml:"url"`
}

// OtlpConfig stores the OTLP configuration.
type OtlpConfig struct {
	Endpoint string `yaml:"endpoint"`
	Method   string `yaml:"method"`
	Secure   bool   `yaml:"secure"`
}

// TraceConfig stores the trace configuration.
type TraceConfig struct {
	Exporter       string          `yaml:"exporter"`
	SampleFraction float64         `yaml:"sampleFraction"`
	Jaeger         JaegerConfig    `yaml:"jaeger"`
	Otlp           OtlpConfig      `yaml:"otlp"`
	InitTimeout    DurationSeconds `yaml:"initTimeoutSeconds"`
}

// EtcdSslConfig stores the ETCD SSL configuration.
type EtcdSslConfig struct {
	Enabled       bool   `yaml:"enabled"`
	TlsCert       string `yaml:"tlsCert"`
	TlsKey        string `yaml:"tlsKey"`
	TlsCACert     string `yaml:"tlsCACert"`
	TlsMinVersion string `yaml:"tlsMinVersion"`
}

// EtcdAuthConfig stores the ETCD authentication configuration.
type EtcdAuthConfig struct {
	Enabled  bool   `yaml:"enabled"`
	UserName string `yaml:"userName"`
	Password string `yaml:"password"`
}

// EtcdDataConfig stores the ETCD data configuration.
type EtcdDataConfig struct {
	Dir string `yaml:"dir"`
}

// EtcdUseConfig stores the ETCD usage configuration.
type EtcdUseConfig struct {
	Embed bool `yaml:"embed"`
}

// EtcdLogConfig stores the ETCD log configuration.
type EtcdLogConfig struct {
	Level string `yaml:"level"`
	Path  string `yaml:"path"`
}

// EtcdConfig stores the ETCD configuration.
type EtcdConfig struct {
	Endpoints      []string             `yaml:"endpoints"`
	RootPath       string               `yaml:"rootPath"`
	MetaSubPath    string               `yaml:"metaSubPath"`
	KvSubPath      string               `yaml:"kvSubPath"`
	Log            EtcdLogConfig        `yaml:"log"`
	Ssl            EtcdSslConfig        `yaml:"ssl"`
	RequestTimeout DurationMilliseconds `yaml:"requestTimeout"`
	Use            EtcdUseConfig        `yaml:"use"`
	Data           EtcdDataConfig       `yaml:"data"`
	Auth           EtcdAuthConfig       `yaml:"auth"`
}

func (etcdCfg *EtcdConfig) GetEndpoints() []string {
	return etcdCfg.Endpoints
}

// MinioSslConfig stores the MinIO SSL configuration.
type MinioSslConfig struct {
	TlsCACert string `yaml:"tlsCACert"`
}

// MinioConfig stores the MinIO configuration.
type MinioConfig struct {
	Address            string               `yaml:"address"`
	Port               int                  `yaml:"port"`
	AccessKeyID        string               `yaml:"accessKeyID"`
	SecretAccessKey    string               `yaml:"secretAccessKey"`
	UseSSL             bool                 `yaml:"useSSL"`
	Ssl                MinioSslConfig       `yaml:"ssl"`
	BucketName         string               `yaml:"bucketName"`
	CreateBucket       bool                 `yaml:"createBucket"`
	RootPath           string               `yaml:"rootPath"`
	UseIAM             bool                 `yaml:"useIAM"`
	CloudProvider      string               `yaml:"cloudProvider"`
	GcpCredentialJSON  string               `yaml:"gcpCredentialJSON"`
	IamEndpoint        string               `yaml:"iamEndpoint"`
	LogLevel           string               `yaml:"logLevel"`
	Region             string               `yaml:"region"`
	UseVirtualHost     bool                 `yaml:"useVirtualHost"`
	RequestTimeoutMs   DurationMilliseconds `yaml:"requestTimeoutMs"`
	ListObjectsMaxKeys int                  `yaml:"listObjectsMaxKeys"`
}

// LogstoreConfig stores the logstore configuration.
type LogstoreConfig struct {
	SegmentSyncPolicy       SegmentSyncPolicyConfig `yaml:"segmentSyncPolicy"`
	SegmentCompactionPolicy SegmentCompactionPolicy `yaml:"segmentCompactionPolicy"`
	SegmentReadPolicy       SegmentReadPolicyConfig `yaml:"segmentReadPolicy"`
	RetentionPolicy         RetentionPolicyConfig   `yaml:"retentionPolicy"`
	FencePolicy             FencePolicyConfig       `yaml:"fencePolicy"`
}

type StorageConfig struct {
	Type     string `yaml:"type"`
	RootPath string `yaml:"rootPath"`
}

func (s *StorageConfig) IsStorageLocal() bool {
	return s.Type == "local"
}

func (s *StorageConfig) IsStorageMinio() bool {
	return s.Type == "minio" || s.Type == "default" || len(s.Type) == 0
}

func (s *StorageConfig) IsStorageService() bool {
	return s.Type == "service"
}

// WoodpeckerConfig stores the complete Woodpecker configuration.
type WoodpeckerConfig struct {
	Meta     MetaConfig     `yaml:"meta"`
	Client   ClientConfig   `yaml:"client"`
	Logstore LogstoreConfig `yaml:"logstore"`
	Storage  StorageConfig  `yaml:"storage"`
}

type Configuration struct {
	Woodpecker WoodpeckerConfig `yaml:"woodpecker"`
	Log        LogConfig        `yaml:"log"`
	Trace      TraceConfig      `yaml:"trace"`
	Etcd       EtcdConfig       `yaml:"etcd"`
	Minio      MinioConfig      `yaml:"minio"`
}

// NewConfiguration reads the WoodpeckerConfig from a YAML file.
func NewConfiguration(files ...string) (*Configuration, error) {
	wpConfig := getDefaultWoodpeckerConfig()
	logConfig := getDefaultLoggerConfig()
	traceConfig := getDefaultTraceConfig()
	etcdConfig := getDefaultEtcdConfig()
	minioConfig := getDefaultMinioConfig()
	config := &Configuration{
		Woodpecker: wpConfig,
		Log:        logConfig,
		Trace:      traceConfig,
		Etcd:       etcdConfig,
		Minio:      minioConfig,
	}
	if len(files) == 0 {
		return config, nil
	}

	// read all files
	for _, filePath := range files {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			continue
		}
		err = yaml.Unmarshal(data, config)
		if err != nil {
			return nil, err
		}
	}

	err := config.Validate()
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (c *Configuration) Validate() error {
	// Validate Woodpecker configuration
	if err := c.validateWoodpeckerConfig(); err != nil {
		return fmt.Errorf("woodpecker config validation failed: %w", err)
	}

	// Validate other configurations if needed
	// TODO: Add validation for dependent configurations, such as trace config, etcd config, minio config, etc.

	return nil
}

func (c *Configuration) validateWoodpeckerConfig() error {
	// Validate Meta configuration
	if err := c.validateMetaConfig(); err != nil {
		return fmt.Errorf("meta config validation failed: %w", err)
	}

	// Validate Client configuration
	if err := c.validateClientConfig(); err != nil {
		return fmt.Errorf("client config validation failed: %w", err)
	}

	// Validate Logstore configuration
	if err := c.validateLogstoreConfig(); err != nil {
		return fmt.Errorf("logstore config validation failed: %w", err)
	}

	// Validate Storage configuration
	if err := c.validateStorageConfig(); err != nil {
		return fmt.Errorf("storage config validation failed: %w", err)
	}

	return nil
}

func (c *Configuration) validateMetaConfig() error {
	meta := &c.Woodpecker.Meta

	// Validate meta type
	validMetaTypes := map[string]bool{"etcd": true}
	if !validMetaTypes[meta.Type] {
		return fmt.Errorf("invalid meta type '%s', must be 'etcd'", meta.Type)
	}

	// Validate prefix
	if len(meta.Prefix) == 0 {
		return fmt.Errorf("meta prefix cannot be empty")
	}

	return nil
}

func (c *Configuration) validateClientConfig() error {
	client := &c.Woodpecker.Client

	// Validate SegmentAppend configuration
	if client.SegmentAppend.QueueSize <= 0 {
		return fmt.Errorf("segment append queue size must be positive, got %d", client.SegmentAppend.QueueSize)
	}
	if client.SegmentAppend.MaxRetries < 0 {
		return fmt.Errorf("segment append max retries cannot be negative, got %d", client.SegmentAppend.MaxRetries)
	}

	// Validate SegmentRollingPolicy configuration
	if client.SegmentRollingPolicy.MaxSize <= 0 {
		return fmt.Errorf("segment rolling policy max size must be positive, got %d", client.SegmentRollingPolicy.MaxSize.Int64())
	}
	if client.SegmentRollingPolicy.MaxInterval.Seconds() <= 0 {
		return fmt.Errorf("segment rolling policy max interval must be positive, got %d", client.SegmentRollingPolicy.MaxInterval.Seconds())
	}
	if client.SegmentRollingPolicy.MaxBlocks <= 0 {
		return fmt.Errorf("segment rolling policy max blocks must be positive, got %d", client.SegmentRollingPolicy.MaxBlocks)
	}

	// Validate Auditor configuration
	if client.Auditor.MaxInterval.Seconds() <= 0 {
		return fmt.Errorf("auditor max interval must be positive, got %d", client.Auditor.MaxInterval.Seconds())
	}

	// Validate Quorum configuration only when storage type is "service"
	if c.Woodpecker.Storage.IsStorageService() {
		if err := c.validateQuorumConfig(); err != nil {
			return fmt.Errorf("quorum config validation failed: %w", err)
		}
	}

	return nil
}

func (c *Configuration) validateQuorumConfig() error {
	q := &c.Woodpecker.Client.Quorum

	// Basic quorum size validation
	ensembleSize := q.GetEnsembleSize()
	writeQuorumSize := q.GetWriteQuorumSize()
	ackQuorumSize := q.GetAckQuorumSize()

	if ensembleSize <= 0 {
		return fmt.Errorf("ensemble size must be positive, got %d", ensembleSize)
	}
	if writeQuorumSize <= 0 {
		return fmt.Errorf("write quorum size must be positive, got %d", writeQuorumSize)
	}
	if ackQuorumSize <= 0 {
		return fmt.Errorf("ack quorum size must be positive, got %d", ackQuorumSize)
	}
	if writeQuorumSize > ensembleSize {
		return fmt.Errorf("write quorum size (%d) cannot be larger than ensemble size (%d)", writeQuorumSize, ensembleSize)
	}
	if ackQuorumSize > writeQuorumSize {
		return fmt.Errorf("ack quorum size (%d) cannot be larger than write quorum size (%d)", ackQuorumSize, writeQuorumSize)
	}

	// Validate affinity mode (allow empty for backward compatibility)
	validAffinityModes := map[string]bool{"soft": true, "hard": true, "": true}
	if !validAffinityModes[q.SelectStrategy.AffinityMode] {
		return fmt.Errorf("invalid affinity mode '%s', must be 'soft' or 'hard'", q.SelectStrategy.AffinityMode)
	}

	// Validate strategy (allow unknown strategies for backward compatibility - they'll default to random)
	validStrategies := map[string]bool{
		"single-az-single-rg": true,
		"single-az-multi-rg":  true,
		"multi-az-single-rg":  true,
		"multi-az-multi-rg":   true,
		"cross-region":        true,
		"custom":              true,
		"random":              true,
		"":                    true, // Allow empty for backward compatibility
	}
	if !validStrategies[q.SelectStrategy.Strategy] {
		// Log warning but don't fail for unknown strategies (backward compatibility)
		fmt.Printf("Warning: unknown strategy '%s', will default to 'random'\n", q.SelectStrategy.Strategy)
	}

	// Validate BufferPools
	if len(q.BufferPools) == 0 {
		return fmt.Errorf("at least one buffer pool must be configured")
	}

	for i, pool := range q.BufferPools {
		if len(pool.Name) == 0 {
			return fmt.Errorf("buffer pool %d name cannot be empty", i)
		}
		if len(pool.Seeds) == 0 {
			return fmt.Errorf("buffer pool '%s' must have at least one seed", pool.Name)
		}
		for j, seed := range pool.Seeds {
			if len(seed) == 0 {
				return fmt.Errorf("buffer pool '%s' seed %d cannot be empty", pool.Name, j)
			}
		}
	}

	// Validate custom placement if strategy is custom
	if q.SelectStrategy.Strategy == "custom" {
		if len(q.SelectStrategy.CustomPlacement) == 0 {
			return fmt.Errorf("custom strategy requires at least one custom placement rule")
		}
		if len(q.SelectStrategy.CustomPlacement) != ensembleSize {
			return fmt.Errorf("custom placement rules count (%d) must equal ensemble size (%d)",
				len(q.SelectStrategy.CustomPlacement), ensembleSize)
		}

		// Validate each custom placement
		for i, placement := range q.SelectStrategy.CustomPlacement {
			if len(placement.Region) == 0 {
				return fmt.Errorf("custom placement rule %d region cannot be empty", i)
			}
			if len(placement.Az) == 0 {
				return fmt.Errorf("custom placement rule %d az cannot be empty", i)
			}
			if len(placement.ResourceGroup) == 0 {
				return fmt.Errorf("custom placement rule %d resource group cannot be empty", i)
			}

			// Verify that the region exists in buffer pools
			found := false
			for _, pool := range q.BufferPools {
				if pool.Name == placement.Region {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("custom placement rule %d references unknown region '%s'", i, placement.Region)
			}
		}
	}

	// Validate cross-region strategy requirements
	if q.SelectStrategy.Strategy == "cross-region" {
		if len(q.BufferPools) < 2 {
			return fmt.Errorf("cross-region strategy requires at least 2 buffer pools, got %d", len(q.BufferPools))
		}
	}

	return nil
}

func (c *Configuration) validateLogstoreConfig() error {
	logstore := &c.Woodpecker.Logstore

	// Validate SegmentSyncPolicy
	if logstore.SegmentSyncPolicy.MaxInterval.Milliseconds() <= 0 {
		return fmt.Errorf("segment sync policy max interval must be positive, got %d", logstore.SegmentSyncPolicy.MaxInterval.Milliseconds())
	}
	if logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage.Milliseconds() <= 0 {
		return fmt.Errorf("segment sync policy max interval for local storage must be positive, got %d", logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage.Milliseconds())
	}
	if logstore.SegmentSyncPolicy.MaxEntries <= 0 {
		return fmt.Errorf("segment sync policy max entries must be positive, got %d", logstore.SegmentSyncPolicy.MaxEntries)
	}
	if logstore.SegmentSyncPolicy.MaxBytes <= 0 {
		return fmt.Errorf("segment sync policy max bytes must be positive, got %d", logstore.SegmentSyncPolicy.MaxBytes.Int64())
	}
	if logstore.SegmentSyncPolicy.MaxFlushRetries < 0 {
		return fmt.Errorf("segment sync policy max flush retries cannot be negative, got %d", logstore.SegmentSyncPolicy.MaxFlushRetries)
	}
	if logstore.SegmentSyncPolicy.RetryInterval.Milliseconds() <= 0 {
		return fmt.Errorf("segment sync policy retry interval must be positive, got %d", logstore.SegmentSyncPolicy.RetryInterval.Milliseconds())
	}
	if logstore.SegmentSyncPolicy.MaxFlushSize <= 0 {
		return fmt.Errorf("segment sync policy max flush size must be positive, got %d", logstore.SegmentSyncPolicy.MaxFlushSize.Int64())
	}
	if logstore.SegmentSyncPolicy.MaxFlushThreads <= 0 {
		return fmt.Errorf("segment sync policy max flush threads must be positive, got %d", logstore.SegmentSyncPolicy.MaxFlushThreads)
	}

	// Validate SegmentCompactionPolicy
	if logstore.SegmentCompactionPolicy.MaxBytes <= 0 {
		return fmt.Errorf("segment compaction policy max bytes must be positive, got %d", logstore.SegmentCompactionPolicy.MaxBytes.Int64())
	}
	if logstore.SegmentCompactionPolicy.MaxParallelUploads <= 0 {
		return fmt.Errorf("segment compaction policy max parallel uploads must be positive, got %d", logstore.SegmentCompactionPolicy.MaxParallelUploads)
	}
	if logstore.SegmentCompactionPolicy.MaxParallelReads <= 0 {
		return fmt.Errorf("segment compaction policy max parallel reads must be positive, got %d", logstore.SegmentCompactionPolicy.MaxParallelReads)
	}

	// Validate SegmentReadPolicy
	if logstore.SegmentReadPolicy.MaxBatchSize <= 0 {
		return fmt.Errorf("segment read policy max batch size must be positive, got %d", logstore.SegmentReadPolicy.MaxBatchSize.Int64())
	}
	if logstore.SegmentReadPolicy.MaxFetchThreads <= 0 {
		return fmt.Errorf("segment read policy max fetch threads must be positive, got %d", logstore.SegmentReadPolicy.MaxFetchThreads)
	}

	return nil
}

func (c *Configuration) validateStorageConfig() error {
	storage := &c.Woodpecker.Storage

	// Validate storage type
	validStorageTypes := map[string]bool{"local": true, "minio": true, "default": true, "service": true, "": true}
	if !validStorageTypes[storage.Type] {
		return fmt.Errorf("invalid storage type '%s', must be one of: local, minio, default, service", storage.Type)
	}

	// Validate root path
	if len(storage.RootPath) == 0 {
		return fmt.Errorf("storage root path cannot be empty")
	}

	return nil
}

func getDefaultWoodpeckerConfig() WoodpeckerConfig {
	return WoodpeckerConfig{
		Meta: MetaConfig{
			Type:   "etcd",
			Prefix: "woodpecker",
		},
		Client: ClientConfig{
			SegmentAppend: SegmentAppendConfig{
				QueueSize:  100,
				MaxRetries: 2,
			},
			SegmentRollingPolicy: SegmentRollingPolicyConfig{
				MaxSize:     ByteSize(100000000),
				MaxInterval: DurationSeconds{Duration: Duration{duration: 800 * 1000000000}}, // 800s
				MaxBlocks:   1000,
			},
			Auditor: AuditorConfig{
				MaxInterval: DurationSeconds{Duration: Duration{duration: 5 * 1000000000}}, // 5s
			},
			Quorum: QuorumConfig{
				BufferPools: []QuorumBufferPool{
					{
						Name:  "default-pool",
						Seeds: []string{},
					},
				},
				SelectStrategy: QuorumSelectStrategy{
					AffinityMode:    "soft",
					Replicas:        3,
					Strategy:        "random",
					CustomPlacement: []CustomPlacement{},
				},
			},
		},
		Logstore: LogstoreConfig{
			SegmentSyncPolicy: SegmentSyncPolicyConfig{
				MaxInterval:                DurationMilliseconds{Duration: Duration{duration: 1000 * 1000000}}, // 1000ms
				MaxIntervalForLocalStorage: DurationMilliseconds{Duration: Duration{duration: 5 * 1000000}},    // 5ms
				MaxEntries:                 2000,
				MaxBytes:                   ByteSize(100000000),
				MaxFlushRetries:            3,
				RetryInterval:              DurationMilliseconds{Duration: Duration{duration: 2000 * 1000000}}, // 2000ms
				MaxFlushSize:               ByteSize(16000000),
				MaxFlushThreads:            8,
			},
			SegmentCompactionPolicy: SegmentCompactionPolicy{
				MaxBytes:           ByteSize(32000000),
				MaxParallelUploads: 4,
				MaxParallelReads:   8,
			},
			SegmentReadPolicy: SegmentReadPolicyConfig{
				MaxBatchSize:    ByteSize(16000000),
				MaxFetchThreads: 32,
			},
			RetentionPolicy: RetentionPolicyConfig{
				TTL: 259200, // 72 hours
			},
			FencePolicy: FencePolicyConfig{
				ConditionWrite: "auto",
			},
		},
		Storage: StorageConfig{
			Type:     "default",
			RootPath: "/tmp/woodpecker",
		},
	}
}

func getDefaultLoggerConfig() LogConfig {
	return LogConfig{
		Level:  "info",
		Format: "text",
		Stdout: true,
		File: LogFileConfig{
			RootPath:   "./logs",
			MaxSize:    100,
			MaxBackups: 10,
			MaxAge:     30,
		},
	}
}

func getDefaultTraceConfig() TraceConfig {
	return TraceConfig{
		Exporter: "noop",
		Jaeger: JaegerConfig{
			URL: "http://localhost:14268/api/traces",
		},
		Otlp: OtlpConfig{
			Endpoint: "localhost:4317",
			Method:   "grpc",
			Secure:   false,
		},
		SampleFraction: 1.0,
		InitTimeout:    DurationSeconds{Duration: Duration{duration: 10 * 1000000000}}, // 10s
	}
}

func getDefaultEtcdConfig() EtcdConfig {
	return EtcdConfig{
		Endpoints:      []string{"localhost:2379"},
		RootPath:       "woodpecker",
		MetaSubPath:    "meta",
		KvSubPath:      "kv",
		Log:            EtcdLogConfig{Level: "info", Path: "./logs"},
		Ssl:            EtcdSslConfig{Enabled: false},
		RequestTimeout: DurationMilliseconds{Duration: Duration{duration: 10000 * time.Millisecond}}, // 10000ms
		Use:            EtcdUseConfig{Embed: false},
	}
}

func getDefaultMinioConfig() MinioConfig {
	return MinioConfig{
		Address:         "localhost",
		Port:            9000,
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UseSSL:          false,
		BucketName:      "a-bucket",
		CreateBucket:    true,
		Ssl: MinioSslConfig{
			TlsCACert: "/path/to/public.crt",
		},
		RootPath:           "files",
		UseIAM:             false,
		CloudProvider:      "aws",
		GcpCredentialJSON:  "",
		IamEndpoint:        "",
		LogLevel:           "fatal",
		Region:             "",
		UseVirtualHost:     false,
		RequestTimeoutMs:   DurationMilliseconds{Duration: Duration{duration: 1000 * 1000000}}, // 1000ms
		ListObjectsMaxKeys: 0,
	}
}
