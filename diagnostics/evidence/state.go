package evidence

import "strings"

// DataState describes evidence availability independently of the diagnostic
// verdict. A missing series never becomes a measured zero or a healthy check.
func DataState(e Evidence) string {
	if strings.HasPrefix(e.Code, "http_") {
		return "source_unavailable"
	}
	switch e.Code {
	case "source_not_configured":
		return "not_configured"
	case "scope_missing", "association_missing":
		return "scope_missing"
	case "no_matching_candidates":
		return "no_matching_candidates"
	case "no_matching_series":
		return "no_data"
	case "histogram_empty_or_no_completions":
		return "no_completed_samples"
	case "insufficient_observations":
		for _, f := range e.Facts {
			if f.Count > 0 {
				return "insufficient_samples"
			}
		}
		return "no_completed_samples"
	case "source_unavailable", "query_unavailable", "target_or_source_unavailable", "metadata_authority_missing", "native_collection_failed", "transport_failed", "response_read_failed", "query_timeout_or_cancelled", "invalid_prometheus_response":
		return "source_unavailable"
	case "deferred_evidence":
		return "deferred"
	}
	if e.Status == "unknown" {
		return "inconclusive"
	}
	return "available"
}

// DisplayState is a presentation label shared by CLI and WebUI, not a new rule.
func DisplayState(e Evidence) string {
	switch DataState(e) {
	case "not_configured":
		return "Not configured"
	case "scope_missing":
		return "Scope unavailable"
	case "no_matching_candidates":
		return "No matching candidates"
	case "no_data":
		return "No data"
	case "no_completed_samples":
		return "No completed samples"
	case "insufficient_samples":
		return "Insufficient samples"
	case "source_unavailable":
		return "Source unavailable"
	case "deferred":
		return "Not implemented"
	}
	switch e.Status {
	case "clear":
		return "Within rule"
	case "abnormal":
		return "Abnormal"
	case "context":
		return "Context"
	case "not_applicable":
		return "Not applicable"
	case "idle":
		return "Idle"
	default:
		return "Inconclusive"
	}
}
