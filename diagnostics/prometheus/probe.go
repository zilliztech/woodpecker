package prometheus

import (
	"context"
	"encoding/json"
	"net/url"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func CheckConnection(ctx context.Context, conn d.Connection) d.ConnectionCheck {
	s, err := settingsFor(conn, atomicPlan)
	if err != nil {
		return d.FailedConnection("invalid_connection_settings", err.Error(), "Review the Prometheus address, deployment and deployment labels in this form or server context.")
	}
	token, user, password, err := auth(s)
	if err != nil {
		return d.FailedConnection("authentication_unavailable", "The configured authentication environment variable is unavailable.", "Set it for the process running wp webui, then retry.")
	}
	return d.CheckQueryAPI(ctx, conn, s.Endpoint, "/api/v1/query", url.Values{"query": {"vector(1)"}, "timeout": {"3s"}}, s.TLS, token, user, password, func(body []byte) d.ConnectionCheck {
		var v struct {
			Status string `json:"status"`
			Data   struct {
				Type   string            `json:"resultType"`
				Result []json.RawMessage `json:"result"`
			} `json:"data"`
		}
		if json.Unmarshal(body, &v) != nil || v.Status != "success" || v.Data.Type != "vector" || len(v.Data.Result) != 1 {
			return d.FailedConnection("invalid_prometheus_response", "The endpoint did not return a successful Prometheus test query.", "Verify its query API and query permissions.")
		}
		return d.ConnectionCheck{Status: "ready", Code: "query_api_ready", Message: "Prometheus query API is accessible.", Next: "Metric availability for the selected filters and time range is checked when you run a diagnosis."}
	})
}
