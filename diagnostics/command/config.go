// Package command is the terminal frontend to source-local checks and scenarios.
// It contains no WebUI, Agent or diagnostic rule implementation.
package command

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/checks"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

// LoadDocument resolves the normal wp config search without changing the file.
func LoadDocument(path string) (*config.File, string, error) {
	if path == "" {
		for _, candidate := range config.DefaultConfigPaths() {
			if _, err := os.Stat(candidate); err == nil {
				path = candidate
				break
			}
			if candidate == os.Getenv("WOODPECKER_CLI_CONFIG") {
				return nil, "", fmt.Errorf("cannot read WOODPECKER_CLI_CONFIG; select a readable cli.yaml with --config")
			}
		}
	}
	if path == "" {
		return nil, "", fmt.Errorf("no cli.yaml found; set WOODPECKER_CLI_CONFIG or --config")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, "", fmt.Errorf("cannot resolve cli.yaml")
	}
	info, err := os.Stat(abs)
	if err != nil || info.Size() > 1<<20 {
		return nil, "", fmt.Errorf("cli.yaml is unreadable or exceeds 1 MiB")
	}
	f, err := config.Load(abs)
	if err != nil {
		return nil, "", fmt.Errorf("cannot parse cli.yaml (values withheld)")
	}
	return f, abs, nil
}

func NativeConnection(path, name string) (d.Connection, error) {
	executable, err := os.Executable()
	if err != nil {
		return d.Connection{}, fmt.Errorf("cannot locate wp executable")
	}
	var settings yaml.Node
	if err = settings.Encode(map[string]any{"executable": executable, "context": name, "cli_config": path}); err != nil {
		return d.Connection{}, err
	}
	return d.Connection{Settings: settings, BaseDir: filepath.Dir(path)}, nil
}

// Load resolves only environment connections; all client filters are request inputs.
func Load(path, contextName string, overrides ...map[string]string) (d.Config, string, error) {
	var f *config.File
	var abs string
	var err error
	if sessionPath := os.Getenv(config.SessionConfigEnv); sessionPath != "" && path == "" && contextName == "" {
		var session *config.SessionConfig
		session, err = config.LoadSession(sessionPath)
		if err == nil {
			f, err = session.ServiceFile()
		}
		if err == nil {
			abs, err = filepath.Abs(sessionPath)
		}
	} else {
		f, abs, err = LoadDocument(path)
	}
	if err != nil {
		return d.Config{}, "", err
	}
	if contextName == "" {
		contextName = f.CurrentContext
	}
	if contextName != "" {
		if _, err = f.ResolveContext(contextName); err != nil {
			return d.Config{}, "", err
		}
	}
	c, err := DiagnosticsConfig(f, abs, overrides...)
	if err != nil {
		return c, "", err
	}
	id := "context-" + contextName
	t := evidence.Target{ID: id, Label: "Server context " + contextName, Context: contextName}
	if ctx, ok := f.Contexts[contextName]; ok {
		t.Prometheus = ctx.Observability.MetricsReference()
		t.Metrics = t.Prometheus
		t.Trace = ctx.Observability.Trace
		c.Agent = ctx.Agent
	}
	_ = c.Targets.Encode(map[string]evidence.Target{id: t})
	c.Frontend, c.DefaultTarget = "wp", id
	return c, id, nil
}

// EnvironmentConfig imports environment connections only. Older diagnostic
// profiles, metadata authorities and logical scope defaults are not selected.
func EnvironmentConfig(f *config.File, abs string) (d.Config, error) {
	c := d.Config{SchemaVersion: d.ConfigSchema, Sources: map[string]d.SourceConfig{}, Agent: f.Agent}
	for name, ctx := range f.Contexts {
		for source, node := range ctx.Observability.Connections {
			var envelope struct {
				Enabled  *bool     `yaml:"enabled"`
				Settings yaml.Node `yaml:"settings"`
			}
			if err := node.Decode(&envelope); err != nil {
				return c, fmt.Errorf("invalid context source settings")
			}
			if envelope.Settings.Kind == 0 {
				envelope.Settings = node
			}
			// Context is the environment identity. Keep the internal report field
			// compatible without requiring a second user-configured name. Decode
			// and re-encode so runtime defaults never mutate the source document.
			var settings map[string]any
			if err := envelope.Settings.Decode(&settings); err != nil || settings == nil {
				return c, fmt.Errorf("invalid context source settings")
			}
			settings["deployment"] = name
			if err := envelope.Settings.Encode(settings); err != nil {
				return c, fmt.Errorf("cannot prepare context source settings")
			}
			group := c.Sources[source]
			if group.Connections == nil {
				group.Connections = map[string]d.Connection{}
			}
			group.Connections[config.SourceReference(name, source)] = d.Connection{Enabled: envelope.Enabled, Settings: envelope.Settings, BaseDir: filepath.Dir(abs)}
			c.Sources[source] = group
		}
		if name == f.CurrentContext {
			c.Agent = ctx.Agent
		}
	}
	return c, nil
}

// DiagnosticsConfig adds local assessment rules to the independent connections.
func DiagnosticsConfig(f *config.File, abs string, overrides ...map[string]string) (d.Config, error) {
	c, err := EnvironmentConfig(f, abs)
	if err != nil {
		return c, err
	}
	var paths map[string]string
	if len(overrides) > 0 {
		paths = overrides[0]
	}
	return checks.WithRuleFiles(c, filepath.Dir(abs), paths)
}
