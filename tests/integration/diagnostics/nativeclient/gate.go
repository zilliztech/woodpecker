package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

type gateState struct {
	Owner             string    `json:"owner"`
	Blocked           bool      `json:"blocked"`
	Until             time.Time `json:"until"`
	Waiting           int       `json:"waiting"`
	Held              uint64    `json:"held_requests"`
	AutomaticReleases uint64    `json:"automatic_releases"`
}

type gate struct {
	mu      sync.Mutex
	state   gateState
	release chan struct{}
	proxy   http.Handler
}

func (g *gate) current() gateState {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.expire()
	return g.state
}

func (g *gate) expire() {
	if g.state.Blocked && !time.Now().Before(g.state.Until) {
		close(g.release)
		g.state.Blocked = false
		g.state.AutomaticReleases++
	}
}

func (g *gate) control(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "POST" {
		var v struct {
			Owner   string `json:"owner"`
			Blocked bool   `json:"blocked"`
			Hold    int    `json:"hold_seconds"`
		}
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
		d.DisallowUnknownFields()
		if d.Decode(&v) != nil || v.Owner != g.state.Owner || (v.Blocked && (v.Hold < 1 || v.Hold > 90)) {
			http.Error(w, "invalid owner/lease", 400)
			return
		}
		g.mu.Lock()
		g.expire()
		if v.Blocked {
			if g.state.Blocked || g.state.Waiting > 0 {
				g.mu.Unlock()
				http.Error(w, "previous gate not drained", 409)
				return
			}
			g.release = make(chan struct{})
			g.state.Blocked = true
			g.state.Until = time.Now().Add(time.Duration(v.Hold) * time.Second)
		} else if g.state.Blocked {
			close(g.release)
			g.state.Blocked = false
		}
		g.mu.Unlock()
	} else if r.Method != "GET" {
		http.Error(w, "method", 405)
		return
	}
	_ = json.NewEncoder(w).Encode(g.current())
}

func (g *gate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Only this run's object keys are eligible. Bucket setup and Admin stay live.
	if strings.HasPrefix(r.URL.Path, "/native-acceptance/"+g.state.Owner+"/") ||
		(r.URL.Path == "/native-acceptance/" && strings.HasPrefix(r.URL.Query().Get("prefix"), g.state.Owner+"/")) {
		g.mu.Lock()
		g.expire()
		if g.state.Blocked {
			if g.state.Waiting >= 64 {
				g.mu.Unlock()
				http.Error(w, "gate capacity", 503)
				return
			}
			ch, until := g.release, g.state.Until
			g.state.Waiting++
			g.state.Held++
			g.mu.Unlock()
			t := time.NewTimer(time.Until(until))
			defer t.Stop()
			select {
			case <-ch:
			case <-t.C:
			case <-r.Context().Done():
			}
			g.mu.Lock()
			g.state.Waiting--
			g.expire()
			g.mu.Unlock()
			if r.Context().Err() != nil {
				return
			}
		} else {
			g.mu.Unlock()
		}
	}
	g.proxy.ServeHTTP(w, r)
}

func serveGate(ctx context.Context, argv []string) error {
	f := flag.NewFlagSet("gate", flag.ContinueOnError)
	owner := f.String("owner", "", "Run owner")
	if err := f.Parse(argv); err != nil {
		return err
	}
	if !ownerPattern.MatchString(*owner) || f.NArg() != 0 {
		return errors.New("invalid gate owner")
	}
	target, _ := url.Parse("http://127.0.0.1:9000")
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{MaxIdleConns: 64, MaxIdleConnsPerHost: 64, ResponseHeaderTimeout: 20 * time.Second}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "fixture upstream unavailable", 502)
	}
	g := &gate{state: gateState{Owner: *owner}, proxy: proxy}
	data := &http.Server{Addr: ":19000", Handler: g, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 100 * time.Second, WriteTimeout: 115 * time.Second, MaxHeaderBytes: 16 << 10}
	control := &http.Server{Addr: ":19001", Handler: http.HandlerFunc(g.control), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, MaxHeaderBytes: 4 << 10}
	errs := make(chan error, 2)
	go func() { errs <- data.ListenAndServe() }()
	go func() { errs <- control.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			_ = data.Close()
			_ = control.Close()
			return errors.New("gate listener failed")
		}
	}
	g.mu.Lock()
	if g.state.Blocked {
		close(g.release)
		g.state.Blocked = false
	}
	g.mu.Unlock()
	_ = data.Close()
	_ = control.Close()
	return nil
}
