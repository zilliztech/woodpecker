package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

// resolved carries everything a command needs after context resolution.
type resolved struct {
	Context config.Context
	Client  *client.Client
	Members *client.Memberlist
	// ConfigPath is the cli.yaml that supplied Context, empty when none was found.
	// Reported so a run can never silently act on a cluster the operator did not pick.
	ConfigPath string
}

// resolveAndDiscover loads cli.yaml, applies flag/env overrides, builds the
// admin HTTP client, and fetches the memberlist. This is called at the start
// of nearly every command that touches cluster state.
func resolveAndDiscover() (*resolved, error) {
	// 1. Load cli.yaml (if present).
	var ctx config.Context
	var configPath string
	for _, p := range config.DefaultConfigPaths() {
		f, err := config.Load(p)
		if err == nil {
			c, err := f.ResolveContext(Globals.Context)
			if err != nil {
				return nil, wperrors.NewConfigError(err.Error())
			}
			ctx = c
			configPath = p
			break
		}
	}

	// 2. Flag / env overrides on top of context.
	// Endpoint precedence: --endpoint flag > $WOODPECKER_ENDPOINT > cli.yaml context.
	ctx.Endpoint = resolveEndpoint(Globals.Endpoint, os.Getenv("WOODPECKER_ENDPOINT"), ctx.Endpoint)
	if Globals.AdminPort != 0 {
		ctx.AdminPort = Globals.AdminPort
	}
	if Globals.Timeout != 0 {
		ctx.Timeout = Globals.Timeout
	}
	if Globals.Concurrency != 0 {
		ctx.Concurrency = Globals.Concurrency
	}
	if Globals.Strict {
		ctx.Strict = true
	}

	// 3. Validate.
	if ctx.Endpoint == "" {
		return nil, wperrors.NewUsageError("no endpoint configured (set --endpoint, $WOODPECKER_ENDPOINT, or cli.yaml context)")
	}

	// 4. Build the seed client and fetch memberlist.
	c := client.New(ctx.Endpoint, client.ClientOpts{
		Timeout:   ctx.Timeout,
		AdminPort: ctx.AdminPort,
	})
	ml, err := c.GetMemberlist()
	if err != nil {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("fetch memberlist from %s: %v", ctx.Endpoint, err))
	}

	r := &resolved{Context: ctx, Client: c, Members: ml, ConfigPath: configPath}
	r.announceSource()
	return r, nil
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
	ctxName := Globals.Context
	if ctxName == "" {
		ctxName = "default"
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
