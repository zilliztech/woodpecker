package client

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newEchoServer(code int, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
}

func TestFanout_AllSuccess(t *testing.T) {
	s1 := newEchoServer(200, `{"ok":1}`)
	defer s1.Close()
	s2 := newEchoServer(200, `{"ok":2}`)
	defer s2.Close()

	f := NewFanout(FanoutOpts{Concurrency: 4, Timeout: 5 * time.Second})
	res := f.Get([]string{s1.URL, s2.URL}, "/admin/node/status", "node-id-placeholder-n/a")
	require.Equal(t, 2, res.Reachable)
	require.Equal(t, 0, res.Unreachable)
	require.Len(t, res.Results, 2)
}

func TestFanout_PartialFailure_NotStrict(t *testing.T) {
	s1 := newEchoServer(200, `{"ok":1}`)
	defer s1.Close()
	s2 := newEchoServer(500, `err`)
	defer s2.Close()

	f := NewFanout(FanoutOpts{Concurrency: 4, Timeout: 5 * time.Second, Strict: false})
	res := f.Get([]string{s1.URL, s2.URL}, "/admin/node/status", "")
	require.Equal(t, 1, res.Reachable)
	require.Equal(t, 1, res.Unreachable)
	require.False(t, res.StrictFailure())
}

func TestFanout_PartialFailure_Strict(t *testing.T) {
	s1 := newEchoServer(200, `{"ok":1}`)
	defer s1.Close()
	s2 := newEchoServer(500, `err`)
	defer s2.Close()

	f := NewFanout(FanoutOpts{Concurrency: 4, Timeout: 5 * time.Second, Strict: true})
	res := f.Get([]string{s1.URL, s2.URL}, "/admin/node/status", "")
	require.Equal(t, 1, res.Reachable)
	require.Equal(t, 1, res.Unreachable)
	require.True(t, res.StrictFailure())
}

func TestFanout_ConcurrencyBound(t *testing.T) {
	// All servers sleep; verify no more than `concurrency` are in flight at once.
	var servers []*httptest.Server
	for range 8 {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(200 * time.Millisecond)
			w.WriteHeader(200)
		}))
		defer s.Close()
		servers = append(servers, s)
	}
	var urls []string
	for _, s := range servers {
		urls = append(urls, s.URL)
	}

	f := NewFanout(FanoutOpts{Concurrency: 2, Timeout: 5 * time.Second})
	start := time.Now()
	_ = f.Get(urls, "/admin/node/status", "")
	elapsed := time.Since(start)
	// With 8 requests at 200ms each and concurrency 2, expect roughly >= 800ms.
	require.GreaterOrEqual(t, elapsed, 700*time.Millisecond, "concurrency bound should serialize batches")
}
