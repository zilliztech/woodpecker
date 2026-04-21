package client

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// FanoutOpts configures fan-out behavior.
type FanoutOpts struct {
	Concurrency int
	Timeout     time.Duration
	Strict      bool
}

// Fanout executes concurrent HTTP requests across a set of peer URLs.
type Fanout struct {
	opts FanoutOpts
	http *http.Client
}

// NewFanout constructs a Fanout client from opts.
func NewFanout(opts FanoutOpts) *Fanout {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 8
	}
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	return &Fanout{
		opts: opts,
		http: &http.Client{Timeout: opts.Timeout},
	}
}

// NodeResult is the per-node outcome of a fan-out request.
type NodeResult struct {
	URL    string
	NodeID string // optional context (for the output layer)
	OK     bool
	Status int
	Body   []byte
	Err    error
}

// FanoutResult is the aggregate across all peers.
type FanoutResult struct {
	Results     []NodeResult
	Reachable   int
	Unreachable int
	strict      bool
}

// StrictFailure returns true when strict mode is on AND at least one peer failed.
func (r *FanoutResult) StrictFailure() bool {
	return r.strict && r.Unreachable > 0
}

// Get issues a concurrent GET to each base URL at the given path.
// nodeIDHint is attached to all results to help downstream labeling (pass "" to skip).
func (f *Fanout) Get(urls []string, path string, nodeIDHint string) *FanoutResult {
	sem := make(chan struct{}, f.opts.Concurrency)
	results := make([]NodeResult, len(urls))
	var wg sync.WaitGroup

	for i, u := range urls {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, base string) {
			defer wg.Done()
			defer func() { <-sem }()

			r := NodeResult{URL: base, NodeID: nodeIDHint}
			req, err := http.NewRequest(http.MethodGet, base+path, nil)
			if err != nil {
				r.Err = err
				results[idx] = r
				return
			}
			req.Header.Set("Accept", "application/json")
			resp, err := f.http.Do(req)
			if err != nil {
				r.Err = err
				results[idx] = r
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			r.Status = resp.StatusCode
			r.Body = body
			r.OK = resp.StatusCode >= 200 && resp.StatusCode < 300
			if !r.OK {
				r.Err = fmt.Errorf("status %d", resp.StatusCode)
			}
			results[idx] = r
		}(i, u)
	}
	wg.Wait()

	agg := &FanoutResult{Results: results, strict: f.opts.Strict}
	for _, r := range results {
		if r.OK {
			agg.Reachable++
		} else {
			agg.Unreachable++
		}
	}
	return agg
}
