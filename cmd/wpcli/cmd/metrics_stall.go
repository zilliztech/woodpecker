package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zilliztech/woodpecker/cmd/wpcli/diagnosis"
	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/prom"
)

type pendingOp struct {
	ID   string `json:"op_id"`
	Type string `json:"op_type"`
	Log  int64  `json:"log_id"`
}

func stallOperationMatches(ruleID, opType string) bool {
	return diagnosis.MatchesOperation(ruleID, opType)
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
	convert := func(ops []pendingOp) []diagnosis.Operation {
		var result []diagnosis.Operation
		for _, op := range ops {
			logID := op.Log
			result = append(result, diagnosis.Operation{ID: op.ID, Type: op.Type, LogID: &logID})
		}
		return result
	}
	// Preserve the legacy first/last aggregate heuristic and its output/exit code;
	// wp diagnose uses the stricter versioned per-sample coverage evaluation.
	for _, op := range diagnosis.PersistentOperations([][]diagnosis.Operation{convert(firstOps), convert(lastOps)}, rule.ID) {
		before := completedOps(first, rule.ID, nodeID, *op.LogID)
		after := completedOps(last, rule.ID, nodeID, *op.LogID)
		if after != before {
			// Includes counter resets: a restart is not evidence of zero progress.
			continue
		}
		return &finding{
			RuleID: rule.ID, Severity: rule.Severity,
			Message: fmt.Sprintf("no successful %s completions in sampled window; operation %s remained pending (type=%s, log_id=%d)",
				strings.TrimSuffix(rule.ID, "_stall"), op.ID, op.Type, *op.LogID),
			Hints: []string{fmt.Sprintf("wp ops list %s --type %s --log %d", nodeID, op.Type, *op.LogID)},
		}
	}
	return nil
}
