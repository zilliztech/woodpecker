package evidence

import (
	"encoding/json"
	"fmt"

	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

type MetricPlan struct {
	Queries []prometheus.WALQuery
	Keys    map[string][]string
	Skip    map[string]string
}

func PlanMetrics(target Target, ids []string, milvus bool) MetricPlan {
	c := Definitions()
	by := map[string]Definition{}
	for _, d := range c.Evidence {
		by[d.ID] = d
	}
	p := MetricPlan{Keys: map[string][]string{}, Skip: map[string]string{}}
	seen := map[string]string{}
	for _, id := range ids {
		d := by[id]
		if d.Source != "metrics" {
			continue
		}
		if milvus {
			p.Skip[id] = "Only Woodpecker-owned metrics are supported"
			continue
		}
		for _, metric := range d.Metrics {
			reg, ok := c.Registry[metric]
			if !ok {
				p.Skip[id] = "Metric label contract has not been verified"
				continue
			}
			match := map[string]string{}
			for k, v := range d.Match {
				match[k] = v
			}
			for k, v := range map[string]string{"log_ns": target.LogNamespace, "log_id": target.LogID, "reader_name": target.ReaderName, "node_id": target.NodeID, "path": target.Path} {
				if v != "" && contains(reg.Labels, k) {
					match[k] = v
				}
			}
			if contains(reg.Labels, "node_id") && !contains(reg.Labels, "log_id") && target.NodeID == "" {
				p.Skip[id] = "Select an explicitly mapped node for node-scoped evidence"
				continue
			}
			kind := "gauge"
			if d.Kind == "p99" {
				kind = "histogram"
			} else if d.Kind == "errors" {
				metric += "_count"
				kind = "counter"
			} else if d.Kind == "rate" || d.Kind == "increase" {
				kind = "counter"
			}
			if !prometheus.ValidateWALMatch(match) {
				p.Skip[id] = "Invalid target matcher"
				continue
			}
			keyBytes, _ := json.Marshal(struct {
				Metric, Kind string
				Match        map[string]string
			}{metric, kind, match})
			key := string(keyBytes)
			queryID, exists := seen[key]
			if !exists {
				queryID = id + fmt.Sprintf(".%d", len(p.Keys[id]))
				seen[key] = queryID
				p.Queries = append(p.Queries, prometheus.WALQuery{ID: queryID, Metric: metric, Kind: kind, Unit: metricUnit(d), Match: match})
			}
			p.Keys[id] = append(p.Keys[id], queryID)
		}
	}
	return p
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
