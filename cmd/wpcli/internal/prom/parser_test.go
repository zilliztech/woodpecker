package prom

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sampleMetrics = `# HELP woodpecker_server_logstore_active_logs Number of active logs
# TYPE woodpecker_server_logstore_active_logs gauge
woodpecker_server_logstore_active_logs{node_id="node-1",namespace="bucket/root"} 5
# HELP woodpecker_server_logstore_operations_total Total operations
# TYPE woodpecker_server_logstore_operations_total counter
woodpecker_server_logstore_operations_total{node_id="node-1",namespace="bucket/root",log_id="42",operation="add_entry",status="success"} 1234
woodpecker_server_logstore_operations_total{node_id="node-1",namespace="bucket/root",log_id="42",operation="add_entry",status="error"} 3
# HELP woodpecker_server_logstore_operation_latency Latency of operations
# TYPE woodpecker_server_logstore_operation_latency histogram
woodpecker_server_logstore_operation_latency_bucket{node_id="node-1",namespace="bucket/root",log_id="42",operation="add_entry",status="success",le="1"} 100
woodpecker_server_logstore_operation_latency_bucket{node_id="node-1",namespace="bucket/root",log_id="42",operation="add_entry",status="success",le="2"} 200
woodpecker_server_logstore_operation_latency_bucket{node_id="node-1",namespace="bucket/root",log_id="42",operation="add_entry",status="success",le="+Inf"} 250
woodpecker_server_logstore_operation_latency_sum{node_id="node-1",namespace="bucket/root",log_id="42",operation="add_entry",status="success"} 5000
woodpecker_server_logstore_operation_latency_count{node_id="node-1",namespace="bucket/root",log_id="42",operation="add_entry",status="success"} 250
`

func TestParse(t *testing.T) {
	families, err := Parse(strings.NewReader(sampleMetrics))
	require.NoError(t, err)
	require.Len(t, families, 3)
	assert.Contains(t, families, "woodpecker_server_logstore_active_logs")
	assert.Contains(t, families, "woodpecker_server_logstore_operations_total")
	assert.Contains(t, families, "woodpecker_server_logstore_operation_latency")
}

func TestListMetrics(t *testing.T) {
	families, err := Parse(strings.NewReader(sampleMetrics))
	require.NoError(t, err)

	list := ListMetrics(families)
	require.Len(t, list, 3)
	// Should be sorted alphabetically
	assert.Equal(t, "woodpecker_server_logstore_active_logs", list[0].Name)
	assert.Equal(t, "gauge", list[0].Type)
	assert.Equal(t, 1, list[0].SeriesCount)
}

func TestGetGaugeValue(t *testing.T) {
	families, err := Parse(strings.NewReader(sampleMetrics))
	require.NoError(t, err)

	mf := families["woodpecker_server_logstore_active_logs"]
	val, ok := GetGaugeValue(mf, map[string]string{"node_id": "node-1"})
	require.True(t, ok)
	assert.Equal(t, float64(5), val)

	_, ok = GetGaugeValue(mf, map[string]string{"node_id": "nonexistent"})
	assert.False(t, ok)
}

func TestGetCounterValue(t *testing.T) {
	families, err := Parse(strings.NewReader(sampleMetrics))
	require.NoError(t, err)

	mf := families["woodpecker_server_logstore_operations_total"]
	val, ok := GetCounterValue(mf, map[string]string{"operation": "add_entry", "status": "success"})
	require.True(t, ok)
	assert.Equal(t, float64(1234), val)
}

func TestGetHistogramCount(t *testing.T) {
	families, err := Parse(strings.NewReader(sampleMetrics))
	require.NoError(t, err)

	mf := families["woodpecker_server_logstore_operation_latency"]
	count, ok := GetHistogramCount(mf, map[string]string{"operation": "add_entry", "status": "success"})
	require.True(t, ok)
	assert.Equal(t, uint64(250), count)
}

func TestLabelsMatch(t *testing.T) {
	families, err := Parse(strings.NewReader(sampleMetrics))
	require.NoError(t, err)

	mf := families["woodpecker_server_logstore_operations_total"]
	m := mf.GetMetric()[0] // first metric
	assert.True(t, LabelsMatch(m, map[string]string{"node_id": "node-1"}))
	assert.False(t, LabelsMatch(m, map[string]string{"node_id": "node-2"}))
}

func TestFormatLabels(t *testing.T) {
	families, err := Parse(strings.NewReader(sampleMetrics))
	require.NoError(t, err)

	mf := families["woodpecker_server_logstore_active_logs"]
	m := mf.GetMetric()[0]
	s := FormatLabels(m)
	assert.Contains(t, s, "node_id=")
	assert.Contains(t, s, "namespace=")
}
