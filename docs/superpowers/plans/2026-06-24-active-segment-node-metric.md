# Active-Segment Node Metric Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a client-side Prometheus gauge that records which quorum nodes each log's current active (writable) segment is using, plus two Grafana panels, so node switching/failover can be verified.

**Architecture:** One `GaugeVec` `woodpecker_client_active_segment_node{log_ns, log_id, segment_id, node} = 1`, one series per quorum node. Two package-level helpers in `common/metrics` (mirroring the existing `UpdateSegmentState`) set/clear the series. `segmentHandleImpl` calls *set* at the single point a segment becomes writable (`NewSegmentHandle`, `canWrite==true`) and *clear* at the single point a segment leaves the writable state (the lone `canWriteState.CompareAndSwap(true,false)` in `doCloseWritingAndUpdateMetaIfNecessaryUnsafe`). Two panels are added to the docker and k8s client dashboards.

**Tech Stack:** Go, `github.com/prometheus/client_golang` (incl. `prometheus/testutil`), testify + mockery mocks, Grafana dashboard JSON.

## Global Constraints

- Branch: work on `issue-197-active-segment-node-metric` (already checked out). Do NOT commit to `master`.
- No change to `proto/meta.proto`, no etcd persistence, no change to `SelectQuorum` / `woodpecker/quorum/`, no server-side change, no append hot-path instrumentation.
- Metric naming: `Namespace: woodpeckerNamespace` (`"woodpecker"`), `Subsystem: clientRole` (`"client"`) → full name `woodpecker_client_active_segment_node`. Label set EXACTLY `[]string{"log_ns", "log_id", "segment_id", "node"}`.
- `log_ns` value on the client is `s.metricsNamespace` (= `metrics.BuildMetricsNamespace(cfg.Minio.BucketName, cfg.Minio.RootPath)`); `log_id`/`segment_id` are `strconv.FormatInt(..., 10)`. Reuse these — do not invent a new derivation.
- Dashboards: docker panels use datasource `__DATASOURCE_UID__` and `{log_ns=~"$log_ns"}`; k8s panels use datasource `${prometheus}` and `{cluster=~"$cluster", namespace=~"$namespace", log_ns=~"$log_ns"}`. Do NOT add new template variables. The k8s file must never contain `__DATASOURCE_UID__`.
- Commit messages end with: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## File Structure

- `common/metrics/client_metrics.go` (modify) — add `WpActiveSegmentNode` gauge, register it, add `SetActiveSegmentNodes` / `ClearActiveSegmentNodes` helpers.
- `common/metrics/active_segment_node_test.go` (create) — unit test for the metric + helpers.
- `woodpecker/segment/segment_handle.go` (modify) — call set in `NewSegmentHandle` (`canWrite` branch); call clear in `doCloseWritingAndUpdateMetaIfNecessaryUnsafe` after the CAS.
- `woodpecker/segment/segment_handle_active_node_test.go` (create) — test that a writable handle publishes the series and that closing it removes them.
- `tests/docker/monitor/grafana/templates/dashboard_client.json` (modify) — add Panel 1 (table, id 16) + Panel 2 (timeseries, id 17).
- `tests/k8s/monitor/grafana/templates/dashboard_client_k8s.json` (modify) — add the same two panels with the k8s datasource/labels.

---

### Task 1: Metric definition + set/clear helpers (`common/metrics`)

**Files:**
- Modify: `common/metrics/client_metrics.go` (var block near line 157; registration near line 219; helpers after `UpdateSegmentState` at end of file ~line 234)
- Test: `common/metrics/active_segment_node_test.go` (create)

**Interfaces:**
- Produces:
  - `var WpActiveSegmentNode *prometheus.GaugeVec` — labels `[]string{"log_ns","log_id","segment_id","node"}`.
  - `func SetActiveSegmentNodes(namespace, logId, segmentId string, nodes []string)`
  - `func ClearActiveSegmentNodes(namespace, logId, segmentId string, nodes []string)`

- [ ] **Step 1: Write the failing test**

Create `common/metrics/active_segment_node_test.go`:

```go
// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestActiveSegmentNodes_SetAndClear(t *testing.T) {
	WpActiveSegmentNode.Reset()

	reg := prometheus.NewRegistry()
	reg.MustRegister(WpActiveSegmentNode)

	ns, logId, segId := "bucket/root", "42", "7"
	nodes := []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"}

	SetActiveSegmentNodes(ns, logId, segId, nodes)

	count, err := testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, len(nodes), count, "one series per node after set")
	for _, n := range nodes {
		v := testutil.ToFloat64(WpActiveSegmentNode.WithLabelValues(ns, logId, segId, n))
		assert.Equal(t, float64(1), v)
	}

	ClearActiveSegmentNodes(ns, logId, segId, nodes)
	count, err = testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "series removed after clear")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./common/metrics/ -run TestActiveSegmentNodes_SetAndClear -v`
Expected: COMPILE FAILURE — `undefined: WpActiveSegmentNode`, `undefined: SetActiveSegmentNodes`, `undefined: ClearActiveSegmentNodes`.

- [ ] **Step 3: Add the metric var**

In `common/metrics/client_metrics.go`, immediately after the `WpSegmentHandlePendingAppendOps` definition (the block ending `}, []string{"log_ns", "log_id"})` at ~line 157), add:

```go
	// Active (writable) segment -> quorum node membership. One series per node of
	// the current active segment; value is always 1. Series are deleted when the
	// segment leaves the writable state, so a query reflects only the currently
	// active segment of each log (used to verify node switching/failover).
	WpActiveSegmentNode = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "active_segment_node",
		Help:      "Quorum node membership of each log's current active (writable) segment (value always 1, one series per node)",
	}, []string{"log_ns", "log_id", "segment_id", "node"})
```

- [ ] **Step 4: Register the metric**

In `RegisterClientMetricsWithRegisterer`, immediately after `registerer.MustRegister(WpSegmentHandlePendingAppendOps)` (~line 219), add:

```go
		// Active segment node membership
		registerer.MustRegister(WpActiveSegmentNode)
```

- [ ] **Step 5: Add the helpers**

At the end of `common/metrics/client_metrics.go` (after the `UpdateSegmentState` function), add:

```go
// SetActiveSegmentNodes marks each node of an active (writable) segment's quorum
// with a value of 1 (one series per node). Call when a segment becomes writable.
func SetActiveSegmentNodes(namespace, logId, segmentId string, nodes []string) {
	for _, node := range nodes {
		WpActiveSegmentNode.WithLabelValues(namespace, logId, segmentId, node).Set(1)
	}
}

// ClearActiveSegmentNodes deletes the per-node series for an active segment.
// Call when the segment leaves the writable state (completed / rolling / closed).
func ClearActiveSegmentNodes(namespace, logId, segmentId string, nodes []string) {
	for _, node := range nodes {
		WpActiveSegmentNode.DeleteLabelValues(namespace, logId, segmentId, node)
	}
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./common/metrics/ -run TestActiveSegmentNodes_SetAndClear -v`
Expected: PASS.

- [ ] **Step 7: Verify the whole metrics package still builds/tests**

Run: `go test ./common/metrics/...`
Expected: PASS (no regressions in existing metrics tests).

- [ ] **Step 8: Commit**

```bash
git add common/metrics/client_metrics.go common/metrics/active_segment_node_test.go
git commit -m "feat(metrics): add woodpecker_client_active_segment_node gauge + set/clear helpers

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Wire set/clear into the segment handle lifecycle

**Files:**
- Modify: `woodpecker/segment/segment_handle.go` (set in `NewSegmentHandle` `canWrite` branch ~line 127; clear in `doCloseWritingAndUpdateMetaIfNecessaryUnsafe` right after the CAS ~line 854)
- Test: `woodpecker/segment/segment_handle_active_node_test.go` (create)

**Interfaces:**
- Consumes: `metrics.SetActiveSegmentNodes`, `metrics.ClearActiveSegmentNodes` (Task 1).
- Relies on existing struct fields: `s.metricsNamespace string`, `s.logId int64`, `s.segmentId int64`, `s.quorumInfo *proto.QuorumInfo`. Imports `strconv`, `common/metrics`, `proto` are already present in this file.

> Lifecycle rationale: across the whole file `canWriteState` is set true only in the constructor, and the ONLY true→false transition is the single `CompareAndSwap(true,false)` in `doCloseWritingAndUpdateMetaIfNecessaryUnsafe`. All completion/rolling/fence/log-close paths funnel through it (`ForceCompleteAndClose` → completion → doClose; `FenceAndComplete` → complete → doClose; `logHandleImpl.Close` → `completeAllSegmentHandlesUnsafe` → `ForceCompleteAndClose`). So set-on-construct + clear-after-CAS fires exactly once each per segment.

- [ ] **Step 1: Write the failing test**

Create `woodpecker/segment/segment_handle_active_node_test.go`:

```go
// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segment

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

func TestSegmentHandle_ActiveSegmentNodeMetric_SetAndClear(t *testing.T) {
	metrics.WpActiveSegmentNode.Reset()
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.WpActiveSegmentNode)

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       7,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum: &proto.QuorumInfo{
				Id: 1, Aq: 1, Es: 3, Wq: 3,
				Nodes: []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"},
			},
		},
		Revision: 1,
	}

	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil).(*segmentHandleImpl)

	count, err := testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, 3, count, "writable segment should publish one series per quorum node")

	// Transition out of writable; clear must fire exactly once (CAS true->false).
	err = sh.doCloseWritingAndUpdateMetaIfNecessaryUnsafe(context.Background(), -1)
	assert.NoError(t, err)

	count, err = testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "series removed after segment leaves writable state")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./woodpecker/segment/ -run TestSegmentHandle_ActiveSegmentNodeMetric_SetAndClear -v`
Expected: FAIL — first assertion `assert.Equal(t, 3, count)` fails with `0 != 3` (no set wired yet).

- [ ] **Step 3: Wire the set call**

In `woodpecker/segment/segment_handle.go`, inside `NewSegmentHandle`, replace the `if canWrite {` block:

```go
	if canWrite {
		segmentHandle.canWriteState.Store(true)
		segmentHandle.executor.Start(ctx)
		segmentHandle.completionMgr = newCompletionManager(segmentHandle)
		segmentHandle.completionMgr.Start(context.Background())
	}
```

with:

```go
	if canWrite {
		segmentHandle.canWriteState.Store(true)
		metrics.SetActiveSegmentNodes(
			segmentHandle.metricsNamespace,
			strconv.FormatInt(segmentHandle.logId, 10),
			strconv.FormatInt(segmentHandle.segmentId, 10),
			segmentHandle.quorumInfo.GetNodes(),
		)
		segmentHandle.executor.Start(ctx)
		segmentHandle.completionMgr = newCompletionManager(segmentHandle)
		segmentHandle.completionMgr.Start(context.Background())
	}
```

- [ ] **Step 4: Wire the clear call**

In `doCloseWritingAndUpdateMetaIfNecessaryUnsafe`, replace:

```go
	if !s.canWriteState.CompareAndSwap(true, false) {
		return nil
	}

	start := time.Now()
```

with:

```go
	if !s.canWriteState.CompareAndSwap(true, false) {
		return nil
	}
	// Segment is leaving the writable state -- drop its active-segment node series
	// so the metric only ever reflects the current active segment of each log.
	metrics.ClearActiveSegmentNodes(
		s.metricsNamespace,
		strconv.FormatInt(s.logId, 10),
		strconv.FormatInt(s.segmentId, 10),
		s.quorumInfo.GetNodes(),
	)

	start := time.Now()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./woodpecker/segment/ -run TestSegmentHandle_ActiveSegmentNodeMetric_SetAndClear -v`
Expected: PASS.

- [ ] **Step 6: Run the segment package + build to check for regressions**

Run: `go build ./... && go test ./woodpecker/segment/...`
Expected: build OK; segment tests PASS.

- [ ] **Step 7: Commit**

```bash
git add woodpecker/segment/segment_handle.go woodpecker/segment/segment_handle_active_node_test.go
git commit -m "feat(segment): publish active_segment_node metric on writable set/clear

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Add the two panels to the docker client dashboard

**Files:**
- Modify: `tests/docker/monitor/grafana/templates/dashboard_client.json` (last panel is id 15 "Segment State Count by Log"; the panels array closes right after it)

**Interfaces:**
- Consumes: metric `woodpecker_client_active_segment_node` (Task 1).

- [ ] **Step 1: Insert the two panels**

In `tests/docker/monitor/grafana/templates/dashboard_client.json`, find the end of panel id 15 and the panels-array close:

```json
      "title": "Segment State Count by Log",
      "type": "timeseries"
    }
  ],
```

Replace it with (note the new `,` after the first `}`):

```json
      "title": "Segment State Count by Log",
      "type": "timeseries"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "__DATASOURCE_UID__"
      },
      "fieldConfig": {
        "defaults": {
          "custom": {
            "align": "auto",
            "displayMode": "auto"
          }
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 33
      },
      "id": 16,
      "options": {
        "showHeader": true,
        "sortBy": [
          {
            "displayName": "Log ID",
            "desc": false
          }
        ]
      },
      "targets": [
        {
          "expr": "woodpecker_client_active_segment_node{log_ns=~\"$log_ns\"}",
          "format": "table",
          "instant": true,
          "legendFormat": "",
          "refId": "A"
        }
      ],
      "title": "Active Segment → Nodes (by Log)",
      "transformations": [
        {
          "id": "organize",
          "options": {
            "excludeByName": {
              "Time": true,
              "__name__": true,
              "instance": true,
              "job": true,
              "namespace": true,
              "Value": true
            },
            "renameByName": {
              "log_id": "Log ID",
              "segment_id": "Segment ID",
              "node": "Node"
            }
          }
        }
      ],
      "type": "table"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "__DATASOURCE_UID__"
      },
      "fieldConfig": {
        "defaults": {
          "custom": {
            "drawStyle": "line",
            "fillOpacity": 20,
            "lineWidth": 2,
            "pointSize": 5,
            "showPoints": "auto",
            "spanNulls": false
          },
          "unit": "short"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 33
      },
      "id": 17,
      "options": {
        "legend": {
          "calcs": [
            "lastNotNull"
          ],
          "displayMode": "table",
          "placement": "bottom"
        },
        "tooltip": {
          "mode": "multi",
          "sort": "desc"
        }
      },
      "targets": [
        {
          "expr": "count by (node) (woodpecker_client_active_segment_node{log_ns=~\"$log_ns\"})",
          "legendFormat": "{{node}}",
          "refId": "A"
        }
      ],
      "title": "Active Segments per Node",
      "type": "timeseries"
    }
  ],
```

- [ ] **Step 2: Validate JSON + presence**

Run:
```bash
jq . tests/docker/monitor/grafana/templates/dashboard_client.json >/dev/null && echo "JSON OK"
jq -r '.panels[].title' tests/docker/monitor/grafana/templates/dashboard_client.json | grep -E "Active Segment|Active Segments per Node"
```
Expected: `JSON OK`, and both new titles printed.

- [ ] **Step 3: Confirm docker monitor test still compiles**

Run: `go vet ./tests/docker/monitor/`
Expected: no errors (the docker integration test does not statically validate panels; dashboard import is non-fatal — this just guards the package compiles).

- [ ] **Step 4: Commit**

```bash
git add tests/docker/monitor/grafana/templates/dashboard_client.json
git commit -m "feat(monitor/docker): add active-segment node panels to client dashboard

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Add the two panels to the k8s client dashboard

**Files:**
- Modify: `tests/k8s/monitor/grafana/templates/dashboard_client_k8s.json` (last panel is id 15 "Segment State Count by Log")

**Interfaces:**
- Consumes: metric `woodpecker_client_active_segment_node` (Task 1).

- [ ] **Step 1: Insert the two panels**

In `tests/k8s/monitor/grafana/templates/dashboard_client_k8s.json`, find:

```json
      "title": "Segment State Count by Log",
      "type": "timeseries"
    }
  ],
```

Replace it with:

```json
      "title": "Segment State Count by Log",
      "type": "timeseries"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "${prometheus}"
      },
      "fieldConfig": {
        "defaults": {
          "custom": {
            "align": "auto",
            "displayMode": "auto"
          }
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 33
      },
      "id": 16,
      "options": {
        "showHeader": true,
        "sortBy": [
          {
            "displayName": "Log ID",
            "desc": false
          }
        ]
      },
      "targets": [
        {
          "expr": "woodpecker_client_active_segment_node{cluster=~\"$cluster\", namespace=~\"$namespace\", log_ns=~\"$log_ns\"}",
          "format": "table",
          "instant": true,
          "legendFormat": "",
          "refId": "A"
        }
      ],
      "title": "Active Segment → Nodes (by Log)",
      "transformations": [
        {
          "id": "organize",
          "options": {
            "excludeByName": {
              "Time": true,
              "__name__": true,
              "instance": true,
              "job": true,
              "namespace": true,
              "cluster": true,
              "Value": true
            },
            "renameByName": {
              "log_id": "Log ID",
              "segment_id": "Segment ID",
              "node": "Node"
            }
          }
        }
      ],
      "type": "table"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "${prometheus}"
      },
      "fieldConfig": {
        "defaults": {
          "custom": {
            "drawStyle": "line",
            "fillOpacity": 20,
            "lineWidth": 2,
            "pointSize": 5,
            "showPoints": "auto",
            "spanNulls": false
          },
          "unit": "short"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 33
      },
      "id": 17,
      "options": {
        "legend": {
          "calcs": [
            "lastNotNull"
          ],
          "displayMode": "table",
          "placement": "bottom"
        },
        "tooltip": {
          "mode": "multi",
          "sort": "desc"
        }
      },
      "targets": [
        {
          "expr": "count by (node) (woodpecker_client_active_segment_node{cluster=~\"$cluster\", namespace=~\"$namespace\", log_ns=~\"$log_ns\"})",
          "legendFormat": "{{node}}",
          "refId": "A"
        }
      ],
      "title": "Active Segments per Node",
      "type": "timeseries"
    }
  ],
```

- [ ] **Step 2: Validate JSON + presence + no stray placeholder**

Run:
```bash
jq . tests/k8s/monitor/grafana/templates/dashboard_client_k8s.json >/dev/null && echo "JSON OK"
jq -r '.panels[].title' tests/k8s/monitor/grafana/templates/dashboard_client_k8s.json | grep -E "Active Segment|Active Segments per Node"
grep -c "__DATASOURCE_UID__" tests/k8s/monitor/grafana/templates/dashboard_client_k8s.json
```
Expected: `JSON OK`; both titles printed; the grep count is `0`.

- [ ] **Step 3: Run the k8s dashboard validation test**

Run: `go test ./tests/k8s/monitor/ -run TestK8sDashboards_Valid -v`
Expected: PASS (valid JSON, no `__DATASOURCE_UID__`, has `${prometheus}`, template vars `prometheus/cluster/namespace/log_ns` unchanged).

- [ ] **Step 4: Commit**

```bash
git add tests/k8s/monitor/grafana/templates/dashboard_client_k8s.json
git commit -m "feat(monitor/k8s): add active-segment node panels to client dashboard

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Notes / Optional follow-ups (not required for this plan)

- The docker comprehensive monitor integration test (`TestMonitor_ComprehensiveMetricsVerification`, requires docker + ~3.5 min run) could later add `assertMetricExists(t, cluster, "woodpecker_client_active_segment_node")` under its `ClientMetrics` subtest. Left out of the per-task gates because it needs the full docker stack.
- The test-only constructor `NewSegmentHandleWithAppendOpsQueueWithWritable` deliberately keeps a placeholder quorum and is NOT wired to the metric; no change needed.

## Self-Review

- **Spec coverage:** Core metric (§2 of spec) → Tasks 1–2. Lifecycle event-driven set/clear (§2.2) → Task 2 (set in `NewSegmentHandle`, clear after CAS; funnel argument documented). Dashboard panels for docker + k8s (§3) → Tasks 3–4. Non-goals (no proto/etcd/SelectQuorum/server) → enforced by Global Constraints; nothing in any task touches them. Test-pass-through for `dashboards_test.go` / docker monitor test (§3.3) → Task 3 Step 3, Task 4 Step 3.
- **Placeholder scan:** No TBD/TODO; every code/JSON step contains the full literal content and exact run commands with expected output.
- **Type consistency:** Helper names `SetActiveSegmentNodes` / `ClearActiveSegmentNodes` and metric var `WpActiveSegmentNode` are used identically in Tasks 1 and 2. Label order `log_ns, log_id, segment_id, node` matches between the metric definition, the helpers, and the dashboard `renameByName`/queries. `s.quorumInfo.GetNodes()` is the nil-safe proto getter.
