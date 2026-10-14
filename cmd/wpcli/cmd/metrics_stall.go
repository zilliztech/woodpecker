package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/prom"
)

type pendingOp struct {
	ID   string `json:"op_id"`
	Type string `json:"op_type"`
	Log  int64  `json:"log_id"`
}

func stallOperationMatches(ruleID, opType string) bool {
	if ruleID == "read_stall" {
		return opType == "logstore.get_batch_entries"
	}
	return ruleID == "write_stall" && (opType == "logstore.add_entry" || opType == "logstore.add_entry_batch")
}

// Operations lack tenant labels, so progress is conservatively aggregated across
// instances sharing a log ID on this node. Progress on another log ID cannot hide a stall.
func completedOps(families map[string]*prom.MetricFamily, ruleID, nodeID string, logID int64) float64 {
	var total float64
	mf := families["woodpecker_server_logstore_operations_total"]
	if mf == nil {
		return 0
	}
	for _, m := range mf.GetMetric() {
		labels := prom.LabelMap(m)
		if labels["node_id"] != nodeID || labels["log_id"] != strconv.FormatInt(logID, 10) ||
			!stallOperationMatches(ruleID, "logstore."+labels["operation"]) {
			continue
		}
		if labels["status"] == "success" || (ruleID == "read_stall" && labels["status"] == "end_of_file") {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

func evaluateStalledOps(rule ruleDef, first, last map[string]*prom.MetricFamily, nodeID string, firstOps, lastOps []pendingOp) *finding {
	initial := make(map[string]pendingOp, len(firstOps))
	for _, op := range firstOps {
		if op.ID != "" && stallOperationMatches(rule.ID, op.Type) {
			initial[op.ID] = op
		}
	}
	for _, op := range lastOps {
		old, ok := initial[op.ID]
		if !ok || old != op {
			continue
		}
		before := completedOps(first, rule.ID, nodeID, op.Log)
		after := completedOps(last, rule.ID, nodeID, op.Log)
		if after != before {
			// Includes counter resets: a restart is not evidence of zero progress.
			continue
		}
		return &finding{
			RuleID: rule.ID, Severity: rule.Severity,
			Message: fmt.Sprintf("no successful %s completions in sampled window; operation %s remained pending (type=%s, log_id=%d)",
				strings.TrimSuffix(rule.ID, "_stall"), op.ID, op.Type, op.Log),
			Hints: []string{fmt.Sprintf("wp ops list %s --type %s --log %d", nodeID, op.Type, op.Log)},
		}
	}
	return nil
}
