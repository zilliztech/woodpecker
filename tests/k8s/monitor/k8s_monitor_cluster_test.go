package monitor

import "testing"

func TestPromBaseURL_Default(t *testing.T) {
	c := NewK8sMonitorCluster("http://localhost:9090")
	if c.PromBaseURL != "http://localhost:9090" {
		t.Fatalf("got %q", c.PromBaseURL)
	}
}
