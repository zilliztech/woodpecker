package cmd

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

// resolved carries everything a command needs after context resolution.
type resolved struct {
	Context config.Context
	Client  *client.Client
	Members *client.Memberlist
	// ConfigPath is the cli.yaml or session JSON that supplied Context, empty for an explicit endpoint.
	// Reported so a run can never silently act on a cluster the operator did not pick.
	ConfigPath string
	// ContextName is the context ResolveContext settled on, which is cli.yaml's
	// current-context whenever --context is absent. Empty when no cli.yaml was found.
	ContextName string
}

// resolveAndDiscover loads cli.yaml, applies flag/env overrides, builds the
// admin HTTP client, and fetches the memberlist. This is called at the start
// of nearly every command that touches cluster state.
func resolveAndDiscover() (*resolved, error) {
	r, err := resolveConfiguration()
	if err != nil {
		return nil, err
	}
	ml, err := r.Client.GetMemberlist()
	if err != nil {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("fetch memberlist from %s: %v", r.Context.Endpoint, err))
	}
	r.Members = ml
	r.announceSource()
	r.announceMappings()
	return r, nil
}

// resolveConfiguration shares connection precedence without network access or
// output. Bounded commands can attach their own cancellation to discovery.
func resolveConfiguration() (*resolved, error) {
	ctx, configPath, contextName, err := loadServiceContext()
	if err != nil {
		return nil, err
	}
	sessionSelected := os.Getenv(config.SessionConfigEnv) != ""
	if sessionSelected {
		if Globals.Endpoint != "" {
			ctx.Endpoint = Globals.Endpoint
		}
	} else {
		ctx.Endpoint = resolveEndpoint(Globals.Endpoint, os.Getenv("WOODPECKER_ENDPOINT"), ctx.Endpoint)
	}
	apply := func(name string, nonzero bool) bool {
		if sessionSelected && Globals.changed != nil {
			return Globals.changed[name]
		}
		return nonzero
	}
	if apply("admin-port", Globals.AdminPort != 0) {
		ctx.AdminPort = Globals.AdminPort
	}
	if apply("timeout", Globals.Timeout != 0) {
		ctx.Timeout = Globals.Timeout
	}
	if apply("concurrency", Globals.Concurrency != 0) {
		ctx.Concurrency = Globals.Concurrency
	}
	if apply("strict", Globals.Strict || os.Getenv(shellChildEnv) == "1") {
		ctx.Strict = Globals.Strict
	}
	if sessionSelected {
		Globals.AdminPort, Globals.Timeout, Globals.Concurrency, Globals.Strict = ctx.AdminPort, ctx.Timeout, ctx.Concurrency, ctx.Strict
	}

	// Merge per-invocation overrides without mutating the context's stored map.
	mappings := make(map[string]string, len(ctx.NodeAdminURLs))
	for key, value := range ctx.NodeAdminURLs {
		mappings[key] = value
	}
	for _, entry := range Globals.NodeAdminURLs {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, wperrors.NewUsageError("--node-admin-url requires KEY=URL")
		}
		mappings[key] = value
	}
	normalized, err := client.ValidateNodeAdminURLs(mappings)
	if err != nil {
		if sessionSelected {
			return nil, wperrors.NewConfigError("invalid node_admin_urls: values must be HTTP(S) origins without credentials, path, query, or fragment")
		}
		return nil, wperrors.NewConfigError(err.Error())
	}
	ctx.NodeAdminURLs = normalized
	if ctx.Endpoint == "" {
		return nil, wperrors.NewUsageError("no endpoint configured (set --endpoint, $WOODPECKER_SESSION_CONFIG service_context.endpoint, $WOODPECKER_ENDPOINT, or cli.yaml context)")
	}
	if sessionSelected {
		origins, err := client.ValidateNodeAdminURLs(map[string]string{"seed": ctx.Endpoint})
		if err != nil {
			return nil, wperrors.NewConfigError("service endpoint must be an HTTP(S) origin without credentials, path, query, or fragment")
		}
		ctx.Endpoint = origins["seed"]
	}
	c := client.New(ctx.Endpoint, client.ClientOpts{
		Timeout: ctx.Timeout, AdminPort: ctx.AdminPort, NodeAdminURLs: ctx.NodeAdminURLs,
	})
	return &resolved{Context: ctx, Client: c, ConfigPath: configPath, ContextName: contextName}, nil
}

// loadServiceContext selects a whole target before applying transport overrides.
// With session configuration enabled, explicit targets never inherit the
// session's mappings or credentials.
func loadServiceContext() (config.Context, string, string, error) {
	if path := os.Getenv(config.SessionConfigEnv); path != "" {
		if Globals.changed["endpoint"] && Globals.Endpoint == "" || Globals.changed["context"] && Globals.Context == "" {
			return config.Context{}, "", "", wperrors.NewUsageError("explicit --endpoint and --context must not be empty")
		}
		if Globals.Endpoint != "" {
			f := config.File{Contexts: map[string]config.Context{"explicit": {Endpoint: Globals.Endpoint}}}
			ctx, err := f.ResolveContext("explicit")
			return ctx, "", "", err
		}
		if Globals.Context == "" {
			session, err := config.LoadSession(path)
			if err != nil {
				return config.Context{}, "", "", wperrors.NewConfigError(err.Error())
			}
			f, err := session.ServiceFile()
			if err != nil {
				return config.Context{}, "", "", wperrors.NewConfigError(err.Error())
			}
			ctx, err := f.ResolveContext("session")
			return ctx, path, "session", err
		}
	}
	var ctx config.Context
	var configPath, contextName string
	for _, p := range config.DefaultConfigPaths() {
		f, err := config.Load(p)
		if err == nil {
			c, err := f.ResolveContext(Globals.Context)
			if err != nil {
				return config.Context{}, "", "", wperrors.NewConfigError(err.Error())
			}
			ctx = c
			configPath = p
			// Mirror ResolveContext's own fallback: an absent --context means current-context.
			contextName = Globals.Context
			if contextName == "" {
				contextName = f.CurrentContext
			}
			break
		}
	}

	if os.Getenv(config.SessionConfigEnv) != "" && configPath == "" {
		return config.Context{}, "", "", wperrors.NewConfigError("explicit --context requires a configured cli.yaml context")
	}
	return ctx, configPath, contextName, nil
}

func (r *resolved) announceMappings() {
	if sourceWriter != nil && !renderedOutput() {
		keys := make([]string, 0, len(r.Context.NodeAdminURLs))
		for key := range r.Context.NodeAdminURLs {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(sourceWriter, "wp: node admin mapping %s -> %s\n", key, r.Context.NodeAdminURLs[key])
		}
	}
}

// resolveEndpoint applies the wp endpoint precedence:
// --endpoint flag > $WOODPECKER_ENDPOINT env > cli.yaml context.
// It returns the first non-empty value in that order, or "" if all are empty
// (which the caller reports as a usage error). Env is intentionally above
// cli.yaml so the server images' baked-in WOODPECKER_ENDPOINT gives zero-config
// in-pod ops; pass --endpoint to override from inside a pod.
func resolveEndpoint(flagEndpoint, envEndpoint, ctxEndpoint string) string {
	switch {
	case flagEndpoint != "":
		return flagEndpoint
	case envEndpoint != "":
		return envEndpoint
	default:
		return ctxEndpoint
	}
}

// SourceLine names where this run got its settings and what it is pointed at.
//
// cli.yaml is now discovered from beside the binary as well as the usual locations, which makes
// it possible to be aimed at a cluster nobody chose: a staging config in $HOME and a production
// one shipped in a toolkit directory look identical at the prompt. Saying it out loud is the
// condition attached to that convenience, not a nicety.
func (r *resolved) SourceLine() string {
	src := r.ConfigPath
	if src == "" {
		src = "(no cli.yaml; flags and environment only)"
	}
	ctxName := r.ContextName
	if ctxName == "" {
		ctxName = "(none)"
	}
	return fmt.Sprintf("wp: config %s · context %s · endpoint %s", src, ctxName, r.Context.Endpoint)
}

// sourceWriter receives the source line; set by the root command's PersistentPreRun so it
// follows whatever the caller attached, and left nil when the resolver is used outside a command.
var sourceWriter io.Writer

// announceSource names the cluster this run resolved to, on stderr so stdout stays parseable.
// Suppressed for machine-readable output, where a stray line is at best noise.
func (r *resolved) announceSource() {
	if sourceWriter == nil || Globals.Output == "json" || Globals.Output == "yaml" {
		return
	}
	fmt.Fprintln(sourceWriter, r.SourceLine())
}
