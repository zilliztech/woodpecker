package diagnosis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/prom"
)

type Node struct {
	Target Target
	URL    string
}

type Options struct {
	Scenario         string
	Mode             string
	Window           time.Duration
	Interval         time.Duration
	Budget           time.Duration
	RequestTimeout   time.Duration
	Concurrency      int
	Limits           Limits
	LogID            *int64
	SegmentID        *int64
	MetadataRequired bool
	ReadMode         string
	ReadMetadata     func(context.Context) Observation
	ReadScope        *ReadState
}

func (o Options) Validate() error {
	if o.Scenario != "all" && o.Scenario != "write" && o.Scenario != "read" && o.Scenario != "stuck-write" && o.Scenario != "stuck-read" {
		return fmt.Errorf("--scenario must be all, write, read, stuck-write or stuck-read")
	}
	if o.Mode != "snapshot" && o.Mode != "live" {
		return fmt.Errorf("--mode must be snapshot or live (historical retrieval is not supported)")
	}
	if o.Window < time.Millisecond || o.Window > 5*time.Minute || o.Interval < time.Millisecond || o.Interval > o.Window {
		return fmt.Errorf("use 1ms <= --interval <= --window <= 5m")
	}
	if o.Budget < time.Millisecond || o.Budget > 10*time.Minute || o.RequestTimeout < time.Millisecond || o.Concurrency < 1 || o.Concurrency > 32 {
		return fmt.Errorf("use 1ms <= --budget <= 10m, --timeout >= 1ms and --concurrency in 1..32")
	}
	l := o.Limits
	if o.writeEnabled() && (o.writerLimit() < 1 || o.writerLimit() > 64 || l.MaxNodes*l.MaxSamples*o.writerLimit() > 20000) {
		return fmt.Errorf("use --max-writers in 1..64; nodes × samples × writers must not exceed 20000")
	}
	if o.readEnabled() && (o.readerLimit() < 1 || o.readerLimit() > 64 || o.ReadMode != "" && o.ReadMode != "unknown" && o.ReadMode != "tail" && o.ReadMode != "catch-up") {
		return fmt.Errorf("use --max-readers in 1..64 and --read-mode unknown, tail or catch-up")
	}
	if l.MaxNodes < 1 || l.MaxNodes > 32 || l.MaxSamples < 2 || l.MaxSamples > 121 || l.MaxOps < 1 || l.MaxOps > 1000 || l.MaxSeries < 1 || l.MaxSeries > 5000 || l.ResponseBytes < 1024 || l.ResponseBytes > 8<<20 {
		return fmt.Errorf("limits: nodes 1..32, samples 2..121, ops 1..1000, series 1..5000, response bytes 1024..8388608")
	}
	if l.MaxNodes*l.MaxSamples*l.MaxOps > 100000 {
		return fmt.Errorf("nodes × samples × ops must not exceed 100000 retained operation records")
	}
	if l.MaxNodes*l.MaxSamples*l.MaxSeries > 200000 {
		return fmt.Errorf("nodes × samples × series must not exceed 200000 retained counter series")
	}
	if o.rounds() > l.MaxSamples {
		return fmt.Errorf("requested window needs %d samples; increase --max-samples or --interval", o.rounds())
	}
	return nil
}

func (o Options) rounds() int {
	if o.Mode == "snapshot" {
		return 1
	}
	return int((o.Window+o.Interval-1)/o.Interval) + 1
}

// NewReport also supports retaining discovery failures before any node is known.
func NewReport(o Options, serviceContext string) Report {
	mode := "live_window"
	if o.Mode == "snapshot" {
		mode = "snapshot"
	}
	rules := []Rule{}
	for _, rule := range Rules() {
		if o.Scenario == "all" || o.Scenario == rule.Scenario || (o.Scenario == "write" && rule.ID == "write_stall") {
			rules = append(rules, rule)
		}
	}
	l := o.Limits
	if o.writeEnabled() {
		l.MaxWriters = o.writerLimit()
	} else {
		l.MaxWriters = 0
	}
	if o.readEnabled() {
		l.MaxReaders = o.readerLimit()
	} else {
		l.MaxReaders = 0
	}
	l.BudgetMS = o.Budget.Milliseconds()
	windowMS, intervalMS := o.Window.Milliseconds(), o.Interval.Milliseconds()
	if o.Mode == "snapshot" {
		windowMS, intervalMS = 0, 0
	}
	report := Report{
		SchemaVersion: SchemaVersion, RunID: uuid.NewString(), Source: "wp-runtime", Scope: "wp",
		Context: serviceContext, Mode: mode, WindowMS: windowMS, IntervalMS: intervalMS,
		StartedAt: time.Now().UTC(), Status: "completed", Limits: l, Rules: rules,
		Targets: []Target{}, Observations: []Observation{}, Findings: []Finding{}, Gaps: []Gap{},
	}
	if o.readEnabled() {
		report.ReadMode = o.ReadMode
		if report.ReadMode == "" {
			report.ReadMode = "unknown"
		}
	}
	return report
}

// Collect has no backend or configuration discovery of its own. The caller
// resolves the existing wpcli context and supplies explicitly selected nodes.
func Collect(ctx context.Context, report *Report, nodes []Node, o Options) {
	ctx, cancel := context.WithTimeout(ctx, o.Budget)
	defer cancel()
	if err := o.Validate(); err != nil || len(nodes) == 0 || len(nodes) > o.Limits.MaxNodes {
		report.Status, report.FinishedAt = "failed", time.Now().UTC()
		report.Gaps = append(report.Gaps, Gap{Code: "invalid_collection_limits", Detail: "Validate collection options and target count before collecting"})
		return
	}
	for _, node := range nodes {
		report.Targets = append(report.Targets, node.Target)
	}
	started := time.Now()
	client := &http.Client{Timeout: o.RequestTimeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	for round := 0; round < o.rounds(); round++ {
		if round > 0 {
			delay := time.Duration(round) * o.Interval
			if delay > o.Window {
				delay = o.Window
			}
			timer := time.NewTimer(time.Until(started.Add(delay)))
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
		if ctx.Err() != nil {
			break
		}
		if o.readEnabled() && o.ReadMetadata != nil {
			obs := o.ReadMetadata(ctx)
			obs.ID, obs.SourceID, obs.Kind, obs.Round = fmt.Sprintf("%s:metadata:r%d", report.RunID, round), report.RunID+":read-metadata", "read_metadata", round
			report.Observations = append(report.Observations, obs)
			o.ReadScope = nil
			if obs.Status == "observed" {
				o.ReadScope = obs.ReadState
			}
		}
		// Per-node requests are sequential, nodes are bounded-concurrent. Merge in
		// target order so report ordering does not depend on network scheduling.
		results := make([][]Observation, len(nodes))
		semaphore := make(chan struct{}, o.Concurrency)
		var wg sync.WaitGroup
		for i, node := range nodes {
			wg.Add(1)
			go func(i int, node Node) {
				defer wg.Done()
				select {
				case semaphore <- struct{}{}:
					defer func() { <-semaphore }()
				case <-ctx.Done():
					return
				}
				kinds := []string{"metrics", "ops", "registry"}
				if o.writeEnabled() {
					kinds = append(kinds, "writers")
				}
				if o.readEnabled() && o.ReadScope != nil && o.ReadScope.SegmentState == "Active" && readQuorumNode(o.ReadScope, node.Target.NodeID) {
					kinds = append(kinds, "read_position")
				}
				if round == 0 {
					kinds = append(kinds, "build")
				}
				for _, kind := range kinds {
					id := fmt.Sprintf("%s:n%d:r%d:%s", report.RunID, i, round, kind)
					obs := collectOne(ctx, client, node, kind, round, o)
					obs.ID, obs.SourceID = id, fmt.Sprintf("%s:n%d:%s", report.RunID, i, kind)
					results[i] = append(results[i], obs)
				}
			}(i, node)
		}
		wg.Wait()
		for _, observations := range results {
			report.Observations = append(report.Observations, observations...)
		}
		if round == 0 {
			// Measure the requested live interval after the entire baseline round.
			// Slow discovery/initial requests must not shorten the observed window.
			started = time.Now()
		}
	}
	good, bad := 0, 0
	if o.writeEnabled() {
		report.Gaps = append(report.Gaps, Gap{Code: "maintenance_object_store_pressure_not_assessed", Detail: "Writer queues and file.flush operations do not establish maintenance, object-store, disk or fsync causes; inspect backend-specific evidence"})
	}
	for _, obs := range report.Observations {
		if obs.Status != "observed" {
			report.Gaps = append(report.Gaps, Gap{
				Code: obs.Code, NodeID: obs.NodeID, ObservationID: obs.ID,
				Detail: obs.Kind + " evidence was not collected completely; response contents/credentials are not retained",
			})
		}
		if obs.Kind == "metrics" || obs.Kind == "ops" || obs.Kind == "writers" || obs.Kind == "read_metadata" || obs.Kind == "read_position" {
			if obs.Status == "observed" {
				good++
			} else {
				bad++
			}
		}
	}
	if bad > 0 {
		report.Status = "partial"
		if good == 0 {
			report.Status = "failed"
		}
	}
	if ctx.Err() != nil {
		report.Status = "cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			report.Status = "budget_exceeded"
		}
	}
	report.FinishedAt = time.Now().UTC()
	EvaluateContext(ctx, report)
	report.FinishedAt = time.Now().UTC()
}

func collectOne(ctx context.Context, client *http.Client, node Node, kind string, round int, o Options) Observation {
	obs := Observation{NodeID: node.Target.NodeID, Kind: kind, Round: round, StartedAt: time.Now().UTC(), Status: "observed"}
	if kind == "writers" {
		collectWriters(ctx, client, node, &obs, o)
		obs.FinishedAt = time.Now().UTC()
		return obs
	}
	if kind == "read_position" {
		collectReadPosition(ctx, client, node, &obs, o)
		obs.FinishedAt = time.Now().UTC()
		return obs
	}
	path := map[string]string{
		"metrics": "/metrics", "ops": "/admin/runtime/ops?limit=" + strconv.Itoa(o.Limits.MaxOps),
		"registry": "/admin/runtime/ops/stats", "build": "/admin/env",
	}[kind]
	body, code := fetch(ctx, client, node.URL, path, o.Limits.ResponseBytes)
	if code != "" {
		obs.Status, obs.Code = "unavailable", code
		if code == "http_404" || code == "http_405" || code == "http_501" {
			obs.Status = "unsupported"
		}
	} else {
		switch kind {
		case "metrics":
			decodeMetrics(body, &obs, o.Limits.MaxSeries)
			if o.writeEnabled() && obs.Status == "observed" {
				decodeWriteSignals(body, &obs, o.Limits.MaxSeries)
			}
		case "ops":
			if err := json.Unmarshal(body, &obs.Ops); err != nil || obs.Ops == nil {
				obs.Status, obs.Code = "invalid", "invalid_ops_schema"
			} else if len(obs.Ops) > o.Limits.MaxOps {
				obs.Ops = obs.Ops[:o.Limits.MaxOps]
				obs.Status, obs.Code = "truncated", "ops_limit_exceeded"
			}
			seen := map[string]string{}
			for _, op := range obs.Ops {
				if op.ID == "" || op.Type == "" || op.StartedAt.IsZero() {
					obs.Status, obs.Code = "invalid", "incomplete_ops_identity"
				}
				if key, exists := seen[op.ID]; exists && key != opKey(op) {
					obs.Status, obs.Code = "invalid", "conflicting_op_identity"
				}
				seen[op.ID] = opKey(op)
			}
		case "registry":
			var stats struct {
				Capacity *int   `json:"capacity"`
				InUse    *int   `json:"in_use"`
				Evicted  *int64 `json:"evicted_total"`
			}
			if json.Unmarshal(body, &stats) != nil || stats.Capacity == nil || stats.InUse == nil || stats.Evicted == nil ||
				*stats.Capacity <= 0 || *stats.InUse < 0 || *stats.InUse > *stats.Capacity || *stats.Evicted < 0 {
				obs.Status, obs.Code = "invalid", "invalid_registry_schema"
			} else {
				obs.Registry = &Registry{Capacity: *stats.Capacity, InUse: *stats.InUse, Evicted: *stats.Evicted}
			}
		case "build":
			// /admin/env also returns environment variables. Decode only build.version;
			// never include the response, environment, host or error body in evidence.
			var env struct {
				Build struct {
					Version string `json:"version"`
				} `json:"build"`
			}
			if json.Unmarshal(body, &env) != nil || env.Build.Version == "" {
				obs.Status, obs.Code = "unsupported", "build_version_unknown"
			} else if !versionFormat.MatchString(env.Build.Version) {
				obs.Status, obs.Code, obs.Version = "unsupported", "unsupported_version", "unrecognized"
			} else {
				obs.Version = env.Build.Version
				if !supportedVersion(obs.Version) {
					obs.Status, obs.Code = "unsupported", "unsupported_version"
				}
			}
		}
	}
	obs.FinishedAt = time.Now().UTC()
	return obs
}

var versionFormat = regexp.MustCompile(`^(dev|v?[0-9]+\.[0-9]+\.[0-9]+([+-][A-Za-z0-9.-]+)?)$`)

func supportedVersion(v string) bool {
	return v == "dev" || strings.HasPrefix(strings.TrimPrefix(v, "v"), "0.1.")
}

func fetch(ctx context.Context, client *http.Client, base, path string, limit int64) ([]byte, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+path, nil)
	if err != nil {
		return nil, "invalid_endpoint"
	}
	req.Header.Set("Accept", "application/json")
	if path == "/metrics" {
		req.Header.Set("Accept", "text/plain")
	}
	response, err := client.Do(req)
	if err != nil {
		// Do not serialize net/http errors: they may contain URL credentials.
		if ctx.Err() != nil {
			return nil, "cancelled_or_budget_exceeded"
		}
		return nil, "transport_or_request_timeout"
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Sprintf("http_%d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, "response_read_failed"
	}
	if int64(len(body)) > limit {
		return nil, "response_byte_limit"
	}
	return body, ""
}

func decodeMetrics(body []byte, obs *Observation, maxSeries int) {
	families, err := prom.Parse(strings.NewReader(string(body)))
	if err != nil {
		obs.Status, obs.Code = "invalid", "invalid_metrics_schema"
		return
	}
	for _, m := range families["process_start_time_seconds"].GetMetric() {
		if m.GetGauge() != nil && finite(m.GetGauge().GetValue()) && m.GetGauge().GetValue() > 0 && m.TimestampMs == nil {
			value := m.GetGauge().GetValue()
			obs.ProcessStart = &value
		}
	}
	for _, m := range families["woodpecker_server_logstore_operations_total"].GetMetric() {
		if len(obs.Counters) >= maxSeries {
			obs.Status, obs.Code = "truncated", "metric_series_limit"
			break
		}
		if m.GetCounter() == nil || !finite(m.GetCounter().GetValue()) || m.GetCounter().GetValue() < 0 {
			obs.Status, obs.Code = "invalid", "invalid_completion_counter"
			continue
		}
		labels := map[string]string{}
		for k, v := range prom.LabelMap(m) {
			switch k {
			case "node_id", "log_id", "log_ns", "operation", "status":
				labels[k] = v
			}
		}
		obs.Counters = append(obs.Counters, Counter{Labels: labels, Value: m.GetCounter().GetValue(), TimestampMS: m.TimestampMs})
	}
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
