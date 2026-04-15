// Package config loads and resolves the wp CLI configuration (cli.yaml).
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// K8sConfig holds Kubernetes-specific settings for a CLI context.
type K8sConfig struct {
	Namespace   string `yaml:"namespace,omitempty"`
	Cluster     string `yaml:"cluster,omitempty"`
	KubeContext string `yaml:"kube_context,omitempty"`
	Kubeconfig  string `yaml:"kubeconfig,omitempty"`
	Kubectl     string `yaml:"kubectl,omitempty"`
}

// Context is one named cluster context in cli.yaml.
type Context struct {
	Endpoint    string        `yaml:"endpoint"`
	AdminPort   int           `yaml:"admin_port"`
	Timeout     time.Duration `yaml:"timeout"`
	Concurrency int           `yaml:"concurrency"`
	Strict      bool          `yaml:"strict"`
	K8s         K8sConfig     `yaml:"k8s,omitempty"`
}

// Defaults carries the `defaults:` section of cli.yaml.
type Defaults struct {
	Output   string `yaml:"output"`
	NoColor  bool   `yaml:"no_color"`
	PageSize int    `yaml:"page_size"`
}

// File is the parsed cli.yaml document.
type File struct {
	CurrentContext string             `yaml:"current-context"`
	Contexts       map[string]Context `yaml:"contexts"`
	Defaults       Defaults           `yaml:"defaults"`
}

// Load parses a cli.yaml file.
func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cli.yaml: %w", err)
	}
	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse cli.yaml: %w", err)
	}
	return &f, nil
}

// ResolveContext returns the effective context, applying defaults for unset fields.
// If name is empty, uses f.CurrentContext.
func (f *File) ResolveContext(name string) (Context, error) {
	if name == "" {
		name = f.CurrentContext
	}
	if name == "" {
		return Context{}, fmt.Errorf("no context name provided and no current-context set")
	}
	ctx, ok := f.Contexts[name]
	if !ok {
		return Context{}, fmt.Errorf("context %q not found in cli.yaml", name)
	}
	// Apply hardcoded fallback defaults (spec §3.2 bottom layer).
	if ctx.AdminPort == 0 {
		ctx.AdminPort = 9091
	}
	if ctx.Timeout == 0 {
		ctx.Timeout = 30 * time.Second
	}
	if ctx.Concurrency == 0 {
		ctx.Concurrency = 8
	}
	return ctx, nil
}

// DefaultConfigPaths returns the list of probe paths for cli.yaml, in priority order.
func DefaultConfigPaths() []string {
	var paths []string
	if p := os.Getenv("WOODPECKER_CLI_CONFIG"); p != "" {
		paths = append(paths, p)
	}
	if p := os.Getenv("XDG_CONFIG_HOME"); p != "" {
		paths = append(paths, p+"/woodpecker/cli.yaml")
	}
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, home+"/.woodpecker/cli.yaml")
	}
	return paths
}
