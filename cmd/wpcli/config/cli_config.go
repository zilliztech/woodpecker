// Package config loads and resolves the wp CLI configuration (cli.yaml).
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// EtcdConfig holds host-accessible metadata connection settings.
type EtcdConfig struct {
	Endpoints     []string `yaml:"endpoints,omitempty"`
	MetaPrefix    string   `yaml:"meta_prefix,omitempty"`
	Username      string   `yaml:"username,omitempty"`
	Password      string   `yaml:"password,omitempty"`
	Cert          string   `yaml:"cert,omitempty"`
	Key           string   `yaml:"key,omitempty"`
	CACert        string   `yaml:"cacert,omitempty"`
	TLSMinVersion string   `yaml:"tls_min_version,omitempty"`
	TLSEnabled    *bool    `yaml:"tls_enabled,omitempty"`
}

// ObservabilityConfig names private source definitions belonging to one server
// environment. Grafana is a dashboard link, never a query backend.
type ObservabilityConfig struct {
	Prometheus string `yaml:"prometheus,omitempty"`
	Metrics    string `yaml:"metrics,omitempty"`
	Trace      string `yaml:"trace,omitempty"`
	Grafana    string `yaml:"grafana,omitempty"`
	// Connections are inline source settings owned by this server environment.
	Connections map[string]yaml.Node `yaml:"-"`
}

func (o ObservabilityConfig) MetricsReference() string {
	if o.Prometheus != "" {
		return o.Prometheus
	}
	return o.Metrics
}

// ClientMetaConfig is a tenant namespace and storage locator, independent of a
// server environment. Etcd authentication stays in this private configuration.
type ClientMetaConfig struct {
	Etcd         EtcdConfig `yaml:"etcd"`
	Bucket       string     `yaml:"bucket,omitempty"`
	RootPath     string     `yaml:"root_path,omitempty"`
	LogNamespace string     `yaml:"log_namespace,omitempty"`
	LogID        string     `yaml:"log_id,omitempty"`
	ReaderName   string     `yaml:"reader_name,omitempty"`
	TraceService string     `yaml:"trace_service,omitempty"`
}

// Context is one named cluster context in cli.yaml.
type Context struct {
	Endpoint         string              `yaml:"endpoint"`
	AdminPort        int                 `yaml:"admin_port"`
	Timeout          time.Duration       `yaml:"timeout"`
	Concurrency      int                 `yaml:"concurrency"`
	Strict           bool                `yaml:"strict"`
	NodeAdminURLs    map[string]string   `yaml:"node_admin_urls,omitempty"`
	Etcd             EtcdConfig          `yaml:"etcd,omitempty"`
	K8s              K8sConfig           `yaml:"k8s,omitempty"`
	Observability    ObservabilityConfig `yaml:"observability,omitempty"`
	DiagnosticTarget string              `yaml:"diagnostic_target,omitempty"`
	Agent            yaml.Node           `yaml:"agent,omitempty"`
}

// Defaults carries the `defaults:` section of cli.yaml.
type Defaults struct {
	Output   string `yaml:"output"`
	NoColor  bool   `yaml:"no_color"`
	PageSize int    `yaml:"page_size"`
}

// File is the parsed cli.yaml document.
type File struct {
	CurrentContext string                      `yaml:"current-context"`
	Contexts       map[string]Context          `yaml:"contexts"`
	ClientMetas    map[string]ClientMetaConfig `yaml:"client_metas,omitempty"`
	Defaults       Defaults                    `yaml:"defaults"`
	// Diagnostics is decoded by the diagnostic module, keeping its dependencies
	// and source-specific connection schemas out of the operational config package.
	Diagnostics yaml.Node         `yaml:"diagnostics,omitempty"`
	Agent       yaml.Node         `yaml:"agent,omitempty"`
	Links       map[string]string `yaml:"links,omitempty"`
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
	f.normalizeConnections()
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
	// The directory holding this copy of wp, so a fixed cluster can be handed over as one
	// folder: the binary and its cli.yaml together, nothing to pass per invocation.
	//
	// Ahead of the personal locations below because running *that* copy is a more specific
	// statement of intent than a global config. Deliberately not the working directory:
	// adopting a cli.yaml because of where the shell happens to be would let any directory
	// someone else wrote point this tool at a cluster the operator did not choose.
	if exe, err := os.Executable(); err == nil {
		if resolved, linkErr := filepath.EvalSymlinks(exe); linkErr == nil {
			exe = resolved
		}
		paths = append(paths, filepath.Join(filepath.Dir(exe), "cli.yaml"))
	}
	if p := os.Getenv("XDG_CONFIG_HOME"); p != "" {
		paths = append(paths, p+"/woodpecker/cli.yaml")
	}
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, home+"/.woodpecker/cli.yaml")
	}
	return paths
}

// ResolveClientMeta supports old context-etcd definitions only when explicitly
// named as a client profile. Selecting a server context never selects this profile.
func (f *File) ResolveClientMeta(name string) (ClientMetaConfig, error) {
	if v, ok := f.ClientMetas[name]; ok {
		return v, nil
	}
	if strings.HasPrefix(name, "context:") {
		ctx, err := f.ResolveContext(strings.TrimPrefix(name, "context:"))
		if err != nil {
			return ClientMetaConfig{}, err
		}
		return ClientMetaConfig{Etcd: ctx.Etcd}, nil
	}
	return ClientMetaConfig{}, fmt.Errorf("client meta %q not found in cli.yaml", name)
}
