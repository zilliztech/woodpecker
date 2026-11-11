package prometheus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func TestVictoriaMetricsProxyAndPartialResults(t *testing.T) {
	for _, name := range []string{"prometheus", "vm-complete", "vm-partial"} {
		t.Run(name, func(t *testing.T) {
			partial := name == "vm-partial"
			prefix := "/api/datasources/proxy/uid/fixture-vm"
			if name == "prometheus" {
				prefix = ""
			}
			t.Setenv("WP_TEST_CLOUD_TOKEN", "fixture-token")
			to := time.Now().Add(-time.Second)
			from := to.Add(-5 * time.Minute)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, prefix+"/api/v1/query", r.URL.Path)
				require.Equal(t, "Bearer fixture-token", r.Header.Get("Authorization"))
				data := map[string]any{"resultType": "vector", "result": []any{map[string]any{"metric": map[string]string{}, "value": []any{float64(to.UnixMilli()) / 1000, "1"}}}}
				if r.URL.Query().Get("query") != "vector(1)" {
					data = map[string]any{"resultType": "matrix", "result": []any{map[string]any{
						"metric": map[string]string{"__name__": "woodpecker_server_logstore_disk_usage_ratio", "cluster": "test", "node_id": "node-1", "instance": "10.0.0.1:9091"},
						"values": [][]any{{float64(from.Add(time.Second).UnixMilli()) / 1000, "0.5"}, {float64(to.UnixMilli()) / 1000, "0.6"}},
					}}}
				}
				body := map[string]any{"status": "success", "data": data}
				if name != "prometheus" {
					body["isPartial"] = partial
					body["stats"] = map[string]any{"seriesFetched": "1"}
				}
				require.NoError(t, json.NewEncoder(w).Encode(body))
			}))
			defer server.Close()
			conn := connection(t, server.URL+prefix, func(s *Settings) { s.BearerTokenEnv = "WP_TEST_CLOUD_TOKEN" })
			probe := CheckConnection(context.Background(), conn)
			collection := CollectChecks(context.Background(), conn, d.Target{NodeID: "node-1"}, from, to,
				[]WALQuery{{ID: "disk", Metric: "woodpecker_server_logstore_disk_usage_ratio", Kind: "gauge"}}, d.DefaultLimits(), nil)
			if partial {
				require.Equal(t, "partial_response", probe.Code)
				require.Equal(t, "partial", collection.Status)
				require.Empty(t, collection.Datasets)
				require.Equal(t, "partial_response", collection.Observations[0].Code)
			} else {
				require.Equal(t, "ready", probe.Status)
				require.Equal(t, "completed", collection.Status)
				require.Len(t, collection.Datasets["disk"].Series, 1)
			}
		})
	}
}
