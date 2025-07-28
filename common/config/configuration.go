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
	"strings"

	"gopkg.in/yaml.v3"
)

// MetaConfig stores the metadata storage configuration.
type MetaConfig struct {
	Type   string `yaml:"type"`
	Prefix string `yaml:"prefix"`
}

// SegmentRollingPolicyConfig stores the segment rolling policy configuration.
type SegmentRollingPolicyConfig struct {
	MaxSize     int64 `yaml:"maxSize"`
	MaxInterval int   `yaml:"maxInterval"`
	MaxBlocks   int64 `yaml:"maxBlocks"`
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
}

type AuditorConfig struct {
	MaxInterval int `yaml:"maxInterval"`
}

// SegmentSyncPolicyConfig stores the log file sync policy configuration.
type SegmentSyncPolicyConfig struct {
	MaxInterval                int   `yaml:"maxInterval"`
	MaxIntervalForLocalStorage int   `yaml:"maxIntervalForLocalStorage"`
	MaxEntries                 int   `yaml:"maxEntries"`
	MaxBytes                   int64 `yaml:"maxBytes"`
	MaxFlushRetries            int   `yaml:"maxFlushRetries"`
	RetryInterval              int   `yaml:"retryInterval"`
	MaxFlushSize               int64 `yaml:"maxFlushSize"`
	MaxFlushThreads            int   `yaml:"maxFlushThreads"`
}

type SegmentCompactionPolicy struct {
	MaxBytes int64 `yaml:"maxBytes"`
}

// FragmentManagerConfig stores the fragment manager configuration.
type FragmentManagerConfig struct {
	MaxBytes    int64 `yaml:"maxBytes"`
	MaxInterval int   `yaml:"maxInterval"`
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
	Exporter       string       `yaml:"exporter"`
	SampleFraction float64      `yaml:"sampleFraction"`
	Jaeger         JaegerConfig `yaml:"jaeger"`
	Otlp           OtlpConfig   `yaml:"otlp"`
	InitTimeout    int          `yaml:"initTimeoutSeconds"`
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
	Endpoints      string         `yaml:"endpoints"`
	RootPath       string         `yaml:"rootPath"`
	MetaSubPath    string         `yaml:"metaSubPath"`
	KvSubPath      string         `yaml:"kvSubPath"`
	Log            EtcdLogConfig  `yaml:"log"`
	Ssl            EtcdSslConfig  `yaml:"ssl"`
	RequestTimeout int            `yaml:"requestTimeout"`
	Use            EtcdUseConfig  `yaml:"use"`
	Data           EtcdDataConfig `yaml:"data"`
	Auth           EtcdAuthConfig `yaml:"auth"`
}

func (etcdCfg *EtcdConfig) GetEndpoints() []string {
	if len(etcdCfg.Endpoints) == 0 {
		return []string{}
	}
	return strings.Split(etcdCfg.Endpoints, ",")
}

// MinioSslConfig stores the MinIO SSL configuration.
type MinioSslConfig struct {
	TlsCACert string `yaml:"tlsCACert"`
}

// MinioConfig stores the MinIO configuration.
type MinioConfig struct {
	Address            string         `yaml:"address"`
	Port               int            `yaml:"port"`
	AccessKeyID        string         `yaml:"accessKeyID"`
	SecretAccessKey    string         `yaml:"secretAccessKey"`
	UseSSL             bool           `yaml:"useSSL"`
	Ssl                MinioSslConfig `yaml:"ssl"`
	BucketName         string         `yaml:"bucketName"`
	CreateBucket       bool           `yaml:"createBucket"`
	RootPath           string         `yaml:"rootPath"`
	UseIAM             bool           `yaml:"useIAM"`
	CloudProvider      string         `yaml:"cloudProvider"`
	GcpCredentialJSON  string         `yaml:"gcpCredentialJSON"`
	IamEndpoint        string         `yaml:"iamEndpoint"`
	LogLevel           string         `yaml:"logLevel"`
	Region             string         `yaml:"region"`
	UseVirtualHost     bool           `yaml:"useVirtualHost"`
	RequestTimeoutMs   int            `yaml:"requestTimeoutMs"`
	ListObjectsMaxKeys int            `yaml:"listObjectsMaxKeys"`
}

// LogstoreConfig stores the logstore configuration.
type LogstoreConfig struct {
	SegmentSyncPolicy       SegmentSyncPolicyConfig `yaml:"segmentSyncPolicy"`
	SegmentCompactionPolicy SegmentCompactionPolicy `yaml:"segmentCompactionPolicy"`
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
	return config, nil
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
				MaxSize:     100000000,
				MaxInterval: 800,
				MaxBlocks:   1000,
			},
			Auditor: AuditorConfig{
				MaxInterval: 5,
			},
		},
		Logstore: LogstoreConfig{
			SegmentSyncPolicy: SegmentSyncPolicyConfig{
				MaxInterval:                1000,
				MaxIntervalForLocalStorage: 5,
				MaxEntries:                 2000,
				MaxBytes:                   100000000,
				MaxFlushRetries:            3,
				RetryInterval:              2000,
				MaxFlushSize:               16000000,
				MaxFlushThreads:            8,
			},
			SegmentCompactionPolicy: SegmentCompactionPolicy{
				MaxBytes: 32000000,
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
		InitTimeout:    10,
	}
}

func getDefaultEtcdConfig() EtcdConfig {
	return EtcdConfig{
		Endpoints:      "localhost:2379",
		RootPath:       "woodpecker",
		MetaSubPath:    "meta",
		KvSubPath:      "kv",
		Log:            EtcdLogConfig{Level: "info", Path: "./logs"},
		Ssl:            EtcdSslConfig{Enabled: false},
		RequestTimeout: 10,
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
		RequestTimeoutMs:   1000,
		ListObjectsMaxKeys: 0,
	}
}
