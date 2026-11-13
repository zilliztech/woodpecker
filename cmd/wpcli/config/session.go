package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const SessionConfigEnv = "WOODPECKER_SESSION_CONFIG"

// SessionConfig supplies independent service, client metadata and storage
// settings for one invocation. Scope never establishes authorization.
type SessionConfig struct {
	SchemaVersion  int            `yaml:"schema_version"`
	Scope          SessionScope   `yaml:"scope,omitempty"`
	ServiceContext *Context       `yaml:"service_context,omitempty"`
	ClientMeta     *EtcdConfig    `yaml:"client_meta,omitempty"`
	ClientStorage  *StorageConfig `yaml:"client_storage,omitempty"`
}

// StorageConfig identifies the selected client's physical data independently of
// its etcd namespace. Field names match the existing client configuration.
type StorageConfig struct {
	Bucket   string `yaml:"bucket"`
	RootPath string `yaml:"root_path"`
}

func (c StorageConfig) Validate() error {
	if strings.TrimSpace(c.Bucket) == "" || strings.TrimSpace(c.RootPath) == "" {
		return fmt.Errorf("client_storage requires a nonempty bucket and root_path together")
	}
	return nil
}

type SessionScope struct {
	WoodpeckerID     string `yaml:"woodpecker_id,omitempty"`
	MilvusInstanceID string `yaml:"milvus_instance_id,omitempty"`
}

// LoadSession reads JSON using the existing configuration field names and YAML
// value semantics (notably string durations). Errors never include file contents,
// which can contain credentials. Connection requirements are checked by consumers.
func LoadSession(path string) (*SessionConfig, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("%s: cannot open configuration file: %w", SessionConfigEnv, err)
	}
	const maxSize = 1 << 20
	if !info.Mode().IsRegular() || info.Size() > maxSize {
		return nil, fmt.Errorf("%s: configuration must be a regular file of at most 1 MiB", SessionConfigEnv)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%s: cannot open configuration file: %w", SessionConfigEnv, err)
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxSize {
		return nil, fmt.Errorf("%s: configuration must be a regular file of at most 1 MiB", SessionConfigEnv)
	}
	body, err := io.ReadAll(io.LimitReader(f, maxSize+1))
	if err != nil || len(body) > maxSize {
		return nil, fmt.Errorf("%s: cannot read configuration file (maximum 1 MiB)", SessionConfigEnv)
	}
	if !json.Valid(body) {
		return nil, fmt.Errorf("%s: invalid JSON", SessionConfigEnv)
	}
	var session SessionConfig
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	decoder.KnownFields(true)
	if err := decoder.Decode(&session); err != nil {
		return nil, fmt.Errorf("%s: invalid configuration fields or types", SessionConfigEnv)
	}
	if session.SchemaVersion != 1 {
		return nil, fmt.Errorf("%s: unsupported schema_version (expected 1)", SessionConfigEnv)
	}
	return &session, nil
}

// ServiceFile adapts the selected service to existing context consumers. It is
// an in-memory view; client metadata never becomes a saved service context.
func (s *SessionConfig) ServiceFile() (*File, error) {
	if s.ServiceContext == nil {
		return nil, fmt.Errorf("%s requires service_context for service commands", SessionConfigEnv)
	}
	f := &File{CurrentContext: "session", Contexts: map[string]Context{"session": *s.ServiceContext}}
	f.normalizeConnections()
	return f, nil
}
