package diagnostics

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var knownSources = []string{"wpcli", "prometheus", "trace", "loki"}

func Has(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func Normalize(s Selection) (Selection, error) {
	s.Scope, s.Flow, s.Stage = strings.ToLower(strings.TrimSpace(s.Scope)), strings.ToLower(strings.TrimSpace(s.Flow)), strings.ToLower(strings.TrimSpace(s.Stage))
	s.Window.Mode = strings.ToLower(strings.TrimSpace(s.Window.Mode))
	if !Has([]string{"wp", "milvus", "e2e"}, s.Scope) || !Has([]string{"write", "read"}, s.Flow) {
		return s, fmt.Errorf("scope must be wp/milvus/e2e and flow must be write/read")
	}
	seen := map[string]bool{}
	sources := []string{}
	for _, source := range s.Sources {
		source = strings.ToLower(strings.TrimSpace(source))
		if !Has(knownSources, source) {
			return s, fmt.Errorf("unknown source; inspect capabilities")
		}
		if !seen[source] {
			sources = append(sources, source)
			seen[source] = true
		}
	}
	if len(sources) == 0 {
		return s, fmt.Errorf("select at least one source")
	}
	s.Sources = sources
	stages := map[string]string{"append": "write", "persistence": "write", "wal_read": "read", "consume": "read", "query": "read"}
	if s.Stage != "" {
		flow, ok := stages[s.Stage]
		if (ok && flow != s.Flow) || !regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`).MatchString(s.Stage) {
			return s, fmt.Errorf("stage is invalid or incompatible with flow")
		}
		if s.Scope == "wp" && (s.Stage == "consume" || s.Stage == "query") {
			return s, fmt.Errorf("consume/query are Milvus stages, not WP-native stages")
		}
	}
	for _, value := range []string{s.Profile, s.Target.Deployment, s.Target.Instance, s.Target.NodeID, s.Target.ReaderID, s.Target.Collection, s.Target.Channel, s.Target.OperationID} {
		if len(value) > 256 || strings.ContainsAny(value, "\x00\r\n") {
			return s, fmt.Errorf("selection identities must be single-line values of at most 256 bytes")
		}
	}
	for _, id := range []string{s.Target.LogID, s.Target.SegmentID} {
		if id == "" {
			continue
		}
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil || n < 0 || strconv.FormatInt(n, 10) != id {
			return s, fmt.Errorf("log/segment identities must be canonical nonnegative int64 decimal strings")
		}
	}
	if s.Target.Identity != "" || s.Target.ServiceCluster != "" || s.Target.TenantAssociation != "" || s.Target.TraceID != "" {
		return s, fmt.Errorf("observed identity/association fields cannot be supplied as selection assertions")
	}
	w := s.Window
	switch w.Mode {
	case "snapshot":
		if w.DurationMS != 0 || w.IntervalMS != 0 || w.From != nil || w.To != nil {
			return s, fmt.Errorf("snapshot cannot select a live/historical interval")
		}
	case "live":
		if w.DurationMS < 1 || w.DurationMS > int64((24*time.Hour)/time.Millisecond) || w.IntervalMS < 1 || w.IntervalMS > w.DurationMS || w.From != nil || w.To != nil {
			return s, fmt.Errorf("live requires a bounded positive duration/interval without historical endpoints")
		}
	case "historical":
		if w.From == nil || w.To == nil || w.From.IsZero() || w.To.IsZero() || !w.From.Before(*w.To) || w.DurationMS != 0 || w.IntervalMS < 0 {
			return s, fmt.Errorf("historical requires from < to and no live duration")
		}
		from, to := w.From.UTC(), w.To.UTC()
		s.Window.From, s.Window.To = &from, &to
	default:
		return s, fmt.Errorf("mode must be snapshot/live/historical")
	}
	return s, nil
}

func ValidateLimits(l Limits) error {
	if l.BudgetMS < 1 || l.BudgetMS > 600000 || l.MaxInputBytes < 1024 || l.MaxInputBytes > 32<<20 || l.MaxOutputBytes < 4096 || l.MaxOutputBytes > 64<<20 || l.MaxTargets < 1 || l.MaxTargets > 32 {
		return fmt.Errorf("limits: budget 1..600000ms, input 1KiB..32MiB, output 4KiB..64MiB, targets 1..32")
	}
	return nil
}

func SelectedTargetFields(t Target) []string {
	values := map[string]string{"deployment": t.Deployment, "instance": t.Instance, "node_id": t.NodeID, "log_id": t.LogID, "segment_id": t.SegmentID, "reader_id": t.ReaderID, "collection": t.Collection, "channel": t.Channel, "operation_id": t.OperationID}
	fields := []string{}
	for _, name := range []string{"deployment", "instance", "node_id", "log_id", "segment_id", "reader_id", "collection", "channel", "operation_id"} {
		if values[name] != "" {
			fields = append(fields, name)
		}
	}
	return fields
}
