package cmd

import (
	"fmt"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

// resolved carries everything a command needs after context resolution.
type resolved struct {
	Context config.Context
	Client  *client.Client
	Members *client.Memberlist
}

// resolveAndDiscover loads cli.yaml, applies flag/env overrides, builds the
// admin HTTP client, and fetches the memberlist. This is called at the start
// of nearly every command that touches cluster state.
func resolveAndDiscover() (*resolved, error) {
	// 1. Load cli.yaml (if present).
	var ctx config.Context
	for _, p := range config.DefaultConfigPaths() {
		f, err := config.Load(p)
		if err == nil {
			c, err := f.ResolveContext(Globals.Context)
			if err != nil {
				return nil, wperrors.NewConfigError(err.Error())
			}
			ctx = c
			break
		}
	}

	// 2. Flag / env overrides on top of context.
	if Globals.Endpoint != "" {
		ctx.Endpoint = Globals.Endpoint
	}
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

	return &resolved{Context: ctx, Client: c, Members: ml}, nil
}
