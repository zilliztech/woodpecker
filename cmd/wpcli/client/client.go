// Package client is the wp CLI's admin HTTP client.
package client

import (
	"net/http"
	"time"
)

// ClientOpts carries client-level options (timeout, admin port override, etc.).
type ClientOpts struct {
	Timeout   time.Duration
	AdminPort int // port to use when constructing peer URLs from gossip hosts; 0 means extract from seed
}

// Client talks to a Woodpecker server admin endpoint.
type Client struct {
	baseURL string
	http    *http.Client
	opts    ClientOpts
}

// New creates a new Client pinned to a single admin URL (the seed).
func New(baseURL string, opts ClientOpts) *Client {
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		baseURL: baseURL,
		http: &http.Client{
			Timeout: timeout,
		},
		opts: opts,
	}
}
