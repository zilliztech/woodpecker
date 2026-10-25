package diagnostics

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type SourceConfig struct {
	Connections map[string]Connection `yaml:"connections"`
}

type Config struct {
	SchemaVersion string                  `yaml:"schema_version" json:"-"`
	Sources       map[string]SourceConfig `yaml:"sources" json:"-"`
}

// LoadConfig reads only wpdiag's envelope. Module settings remain private and
// source-owned; a referenced cli.yaml is never opened or modified here.
func LoadConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("cannot open wpdiag configuration; set --config")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return Config{}, fmt.Errorf("configuration exceeds its byte limit or cannot be read")
	}
	base, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return Config{}, fmt.Errorf("cannot resolve configuration directory")
	}
	return ParseConfig(data, base)
}

func ParseConfig(data []byte, baseDir string) (Config, error) {
	var c Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if decoder.Decode(&c) != nil {
		return Config{}, fmt.Errorf("invalid wpdiag YAML envelope (values withheld)")
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return Config{}, fmt.Errorf("configuration must contain one YAML document")
	}
	if c.SchemaVersion != ConfigSchema {
		return Config{}, fmt.Errorf("unsupported wpdiag configuration schema")
	}
	for source, group := range c.Sources {
		if !Has(knownSources, source) {
			return Config{}, fmt.Errorf("unknown configuration source")
		}
		for name, connection := range group.Connections {
			if name == "" || len(name) > 128 {
				return Config{}, fmt.Errorf("connection references must have 1..128 characters")
			}
			if connection.Settings.Kind != yaml.MappingNode {
				return Config{}, fmt.Errorf("connection settings must be a source-owned mapping")
			}
			connection.BaseDir = baseDir
			group.Connections[name] = connection
		}
	}
	return c, nil
}

func DecodeSettings(connection Connection, value any) error {
	data, err := yaml.Marshal(connection.Settings)
	if err != nil {
		return fmt.Errorf("invalid connection settings")
	}
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if d.Decode(value) != nil {
		return fmt.Errorf("invalid source connection settings (values withheld)")
	}
	return nil
}
