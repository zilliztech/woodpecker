// Package candidates stores independent, non-secret input suggestions. It never
// resolves a context or restores an earlier request.
package candidates

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

type (
	Values map[string][]string
	Item   struct {
		Value  string    `json:"value"`
		UsedAt time.Time `json:"used_at"`
	}
)

type Record struct {
	Schema     string    `json:"schema_version"`
	RecordedAt time.Time `json:"recorded_at"`
	Values     Values    `json:"values"`
}

var allowed = map[string]bool{
	"metadata.etcd": true, "metadata.prefix": true, "metadata.bucket": true, "metadata.root_path": true,
	"log_namespace": true, "log_id": true, "log_name": true, "segment_id": true, "reader_name": true,
	"node_id": true, "path": true, "metrics_endpoint": true, "trace_endpoint": true,
	"trace_id": true, "request_id": true,
}

func Directory() string {
	if dir := os.Getenv("WOODPECKER_RUN_CACHE_DIR"); dir != "" {
		return dir
	}
	dir, err := os.UserCacheDir()
	if err != nil {
		dir = os.TempDir()
	}
	return filepath.Join(dir, "woodpecker", "input-candidates")
}

func clean(key, value string) string {
	value = strings.TrimSpace(value)
	if !allowed[key] || value == "" || len(value) > 2048 || strings.ContainsAny(value, "\x00\r\n\t") {
		return ""
	}
	if key == "metadata.etcd" || strings.HasSuffix(key, "_endpoint") {
		for _, endpoint := range strings.Split(value, ",") {
			u, err := url.Parse(strings.TrimSpace(endpoint))
			if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
				return ""
			}
		}
	}
	return value
}

func locked(fn func(string) error) error {
	dir := Directory()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	lock := flock.New(filepath.Join(dir, ".lock"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ok, err := lock.TryLockContext(ctx, 10*time.Millisecond)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("input candidate cache is busy")
	}
	defer lock.Unlock()
	return fn(dir)
}

func files(dir string) ([]string, error) {
	names, err := filepath.Glob(filepath.Join(dir, "run-*.json"))
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	return names, err
}

func read(path string) (Record, error) {
	var r Record
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return r, fmt.Errorf("invalid candidate file")
	}
	body, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(body, &r)
	}
	if r.Schema != "wp.input-candidates/v1" {
		return r, fmt.Errorf("invalid candidate schema")
	}
	return r, err
}

func write(path string, value any) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".candidate-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(append(body, '\n')); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// Save writes one independent temporary file for a successful execution. The
// caller supplies only fields actually used by that execution, including empty
// successful queries and checks which found abnormalities.
func Save(values Values) error {
	filtered := Values{}
	for key, entries := range values {
		seen := map[string]bool{}
		for _, v := range entries {
			if v = clean(key, v); v != "" && !seen[v] {
				filtered[key] = append(filtered[key], v)
				seen[v] = true
			}
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return locked(func(dir string) error {
		now := time.Now().UTC()
		if err := write(filepath.Join(dir, fmt.Sprintf("run-%020d.json", now.UnixNano())), Record{"wp.input-candidates/v1", now, filtered}); err != nil {
			return err
		}
		names, err := files(dir)
		if err != nil {
			return err
		}
		for i, path := range names {
			if i >= 128 {
				if err = os.Remove(path); err != nil {
					return err
				}
			}
		}
		_, _, err = recentConfig(dir)
		return err
	})
}

// list reads suggestions while the caller holds the cross-process lock.
func list(dir string) (map[string][]Item, error) {
	out := map[string][]Item{}
	names, err := files(dir)
	if err != nil {
		return nil, err
	}
	seen := map[string]map[string]bool{}
	for _, path := range names {
		r, err := read(path)
		if err != nil {
			continue
		}
		for key, entries := range r.Values {
			if seen[key] == nil {
				seen[key] = map[string]bool{}
			}
			for _, value := range entries {
				if value = clean(key, value); value != "" && !seen[key][value] && len(out[key]) < 5 {
					out[key] = append(out[key], Item{value, r.RecordedAt})
					seen[key][value] = true
				}
			}
		}
	}
	return out, nil
}

func List() (map[string][]Item, error) {
	var out map[string][]Item
	err := locked(func(dir string) error { var err error; out, err = list(dir); return err })
	return out, err
}

// recentConfig materializes the same independent suggestions offered by inputs.
// Per-run records remain separate; this file is a readable, derived key/list view.
func recentConfig(dir string) (string, string, error) {
	entries, err := list(dir)
	if err != nil {
		return "", "", err
	}
	values := Values{}
	for key, items := range entries {
		for _, item := range items {
			values[key] = append(values[key], item.Value)
		}
	}
	path, err := filepath.Abs(filepath.Join(dir, "recent-inputs.json"))
	if err != nil {
		return "", "", err
	}
	if err = write(path, values); err != nil {
		return "", "", err
	}
	body, err := os.ReadFile(path)
	return path, string(body), err
}

// RecentConfig returns the actual derived candidate file, including caches
// created before the readable index was introduced. It accepts no browser path.
func RecentConfig() (string, string, error) {
	var path, body string
	err := locked(func(dir string) error { var err error; path, body, err = recentConfig(dir); return err })
	return path, body, err
}

// Forget removes only this key/value from candidate files. Reports are separate
// immutable artifacts. New successful use may add the value again.
func Forget(key, value string) error {
	if clean(key, value) == "" {
		return fmt.Errorf("invalid candidate key or value")
	}
	return locked(func(dir string) error {
		names, err := files(dir)
		if err != nil {
			return err
		}
		for _, path := range names {
			r, err := read(path)
			if err != nil {
				continue
			}
			entries, found := r.Values[key]
			if !found {
				continue
			}
			kept := []string{}
			for _, v := range entries {
				if v != value {
					kept = append(kept, v)
				}
			}
			if len(kept) == len(entries) {
				continue
			}
			if len(kept) == 0 {
				delete(r.Values, key)
			} else {
				r.Values[key] = kept
			}
			if err = write(path, r); err != nil {
				return err
			}
		}
		_, _, err = recentConfig(dir)
		return err
	})
}
