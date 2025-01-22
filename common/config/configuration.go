package config

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

// Meta holds the meta configuration.
type Meta struct {
	Type   string `yaml:"type"`
	Prefix string `yaml:"prefix"`
}

// SegmentRollingPolicy holds the segment rolling configuration.
type SegmentRollingPolicy struct {
	MaxSize     int `yaml:"maxSize"`
	MaxInterval int `yaml:"maxInterval"`
}

// Client holds the client configuration.
type Client struct {
	SegmentRollingPolicy SegmentRollingPolicy `yaml:"segmentRollingPolicy"`
}

// LogFileSyncPolicy holds the log store sync configuration.
type LogFileSyncPolicy struct {
	MaxInterval int `yaml:"maxInterval"`
	MaxEntries  int `yaml:"maxEntries"`
	MaxBytes    int `yaml:"maxBytes"`
}

// LogStore holds the log store configuration.
type LogStore struct {
	LogFileSyncPolicy LogFileSyncPolicy `yaml:"logFileSyncPolicy"`
}

// Configuration holds the entire configuration structure.
type Configuration struct {
	Meta     Meta     `yaml:"meta"`
	Client   Client   `yaml:"client"`
	LogStore LogStore `yaml:"logstore"`
}

// NewConfiguration loads the configuration from the specified YAML file, or uses default values if the file is missing.
func NewConfiguration(yamlFile string) (*Configuration, error) {
	// Set default values
	config := &Configuration{
		Meta: Meta{
			Type:   "etcd",
			Prefix: "woodpecker",
		},
		Client: Client{
			SegmentRollingPolicy: SegmentRollingPolicy{
				MaxSize:     100000000,
				MaxInterval: 600,
			},
		},
		LogStore: LogStore{
			LogFileSyncPolicy: LogFileSyncPolicy{
				MaxInterval: 1000,
				MaxEntries:  10000,
				MaxBytes:    100000000,
			},
		},
	}

	// Try to read the YAML file
	data, err := os.ReadFile(yamlFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("File does not exist, return default config: %v", err)
			// File does not exist, return default config
			return config, nil
		}
		log.Printf("Read file:%s failed:%v, return default config", yamlFile, err)
		return config, nil
	}

	// File exists, unmarshal YAML into the configuration
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %v", err)
	}

	return config, nil
}
