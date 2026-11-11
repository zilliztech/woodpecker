package jaeger

import (
	"context"
	"encoding/json"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func CheckConnection(ctx context.Context, conn d.Connection) d.ConnectionCheck {
	s, err := settings(conn)
	if err != nil {
		return d.FailedConnection("invalid_connection_settings", err.Error(), "Review the Jaeger query address and service bindings in this form or server context.")
	}
	token, user, password, err := auth(s)
	if err != nil {
		return d.FailedConnection("authentication_unavailable", "The configured authentication environment variable is unavailable.", "Set it for the process running wp webui, then retry.")
	}
	return d.CheckQueryAPI(ctx, conn, s.Endpoint, "/api/services", nil, s.TLS, token, user, password, func(body []byte) d.ConnectionCheck {
		var v struct {
			Data   []string          `json:"data"`
			Errors []json.RawMessage `json:"errors"`
		}
		if json.Unmarshal(body, &v) != nil || v.Data == nil || len(v.Errors) > 0 {
			return d.FailedConnection("invalid_jaeger_response", "The endpoint did not return a valid Jaeger services response.", "Verify the Jaeger query API address and permissions.")
		}
		if len(v.Data) == 0 || s.Search.Service != "" && !d.Has(v.Data, s.Search.Service) {
			return d.ConnectionCheck{Status: "warning", Code: "service_not_listed", Message: "Jaeger query API is accessible, but the selected service is not listed.", Next: "Check the exact Trace service name and whether this Jaeger has received spans for it."}
		}
		return d.ConnectionCheck{Status: "ready", Code: "query_api_ready", Message: "Jaeger query API is accessible.", Next: "Retained spans, selected time range and collection budget are checked when you run a diagnosis."}
	})
}
