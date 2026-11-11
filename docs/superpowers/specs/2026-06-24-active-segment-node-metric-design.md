# Issue #197 Design: active-segment node metric + client dashboard panels

- Date: 2026-06-24
- Status: design agreed, pending review
- Scope: woodpecker client-side observability. No proto change, no server change, no change to the quorum selection path (`SelectQuorum`), nothing persisted to etcd.

## 1. Background & Goal

In woodpecker, a log (`logHandleImpl`) has **at most one active (writable) segment** at any moment (`WritableSegmentId`), and **that segment's quorum node set is immutable for its lifetime**. A node "switch" (failover / rolling) is, in essence: the current active segment turns `Completed` (e.g. after append failures), and the log selects a fresh set of nodes to create a **new active segment**.

There is currently no direct observation point to answer: **"After a switch, have all these logs' active segments actually moved onto the newly switched-to nodes?"** The existing metrics (`woodpecker_client_*`) only cover request rate / latency / pending counts, etc. — they don't show "which nodes the current active segment sits on".

This design adds client-side observability:

1. **Core metric**: record the quorum nodes the current active segment of each log is using (keyed by `log_ns` / `log_id` / `segment_id` / `node`).
2. **Dashboard**: add panels to the client dashboard (both the docker and k8s templates) that, for a selected `log_ns`, show which nodes these logs' active segments write to, and let you visually watch old nodes drop to zero and new nodes come up during a switch.

### Non-goals

- No change to `proto/meta.proto`, nothing persisted to etcd. Pure client in-process state + Prometheus exposition.
- No change to server-side metrics / dashboard.
- No new instrumentation on the append hot path.
- **No seed-pool / pool-dimension metric.** Which pool a node belongs to is inferable from the node name, and the client's own pool config changes are already a static record (the source of truth for reverse lookup); changing the `SelectQuorum` signature just for this is not worth it. Revisit the pool dimension only if a concrete PromQL aggregation need arises.

## 2. Metric definition: active segment → nodes

### 2.1 Definition

File: `common/metrics/client_metrics.go` (follows the client-side `clientRole` subsystem and the `log_ns` label convention).

```
woodpecker_client_active_segment_node{log_ns, log_id, segment_id, node} = 1
```

- Type: `GaugeVec`, value is always `1` — a "membership" indicator gauge.
- Semantics: **one series per node** in the current active (writable) segment's quorum. One active segment occupies `Wq` series (typically 3).
- Cardinality: because an old segment's series are deleted on switch, the total series count at any time is ≈ `open logs × Wq`, and does not grow unbounded over time. Although `segment_id` increases monotonically, the series for stale ids have already been deleted, so they don't accumulate.
- Registration: registered in `RegisterClientMetricsWithRegisterer()` (following the existing `sync.Once` registration pattern).

### 2.2 Lifecycle (event-driven + close fallback)

The owner is `segmentHandleImpl` (which holds `quorumInfo.Nodes` and the read/write state), driven by `logHandleImpl` at the writable-segment switch points. Within a segment's lifetime the **set and the clear each happen exactly once** (the node set is immutable), keeping the logic clean.

- **Set (=1)**: when a segment handle becomes writable and its quorum is known — i.e. after `logHandleImpl.GetOrCreateWritableSegmentHandle` → `createNewSegmentMetaWithID` successfully obtains the quorum — call `WpActiveSegmentNode.WithLabelValues(logNs, logId, segId, node).Set(1)` for each node in `quorumInfo.Nodes`.
- **Clear (Delete)**: when the segment leaves the writable state, call `DeleteLabelValues(...)` for each of its nodes, at:
  - the Active→Completed completion path (the completion section in `segment_handle.go`, segment turns `Completed`);
  - rolling / `NotifyWriterInvalidation` (the switch trigger point);
  - **close fallback**: on `segmentHandleImpl` / `logHandleImpl` shutdown, uniformly clean up the active-segment series owned by this handle, so a missed path can't leave stale series behind.

Implementation note: add two helpers on `segmentHandleImpl` (`setActiveSegmentNodeMetrics()` / `clearActiveSegmentNodeMetrics()`) that loop over `quorumInfo.Nodes`, called from the state-transition points and from close. Derive `log_ns` the same way the existing client metrics do (consistent with `WpSegmentHandlePendingAppendOps` etc.).

> Trade-off note: compared with a "periodic reconcile" approach, event-driven is instantly accurate and needs no extra goroutine; the missed-delete risk is bounded by the close fallback plus "only two events per segment".

## 3. Dashboard panels (client dashboard, both docker and k8s)

Key differences between the two templates (which dictate how each panel is written):

| | docker `dashboard_client.json` | k8s `dashboard_client_k8s.json` |
|---|---|---|
| datasource | `__DATASOURCE_UID__` | `${prometheus}` |
| template vars | `log_ns` | `prometheus` / `cluster` / `namespace` / `log_ns` |
| query label filter | `{log_ns=~"$log_ns"}` | `{cluster=~"$cluster", namespace=~"$namespace", log_ns=~"$log_ns"}` |

Each new panel follows its own template's datasource and label conventions. Ready-made reference: the id-14 "Log Name → Log ID Mapping" table panel.

### 3.1 Panel 1 — "Active Segment → Nodes (by Log)" (table)

- Type `table`, target `format:"table"`, `instant:true`.
- Query: `woodpecker_client_active_segment_node{log_ns=~"$log_ns"}` (the k8s version adds `cluster/namespace` filters).
- `organize` transformation: hide `Time` / `__name__` / `instance` / `job` / `namespace` / `Value`; rename columns `log_id→Log ID`, `segment_id→Segment ID`, `node→Node`; sort by Log ID.
- Effect: for the selected `log_ns`, list one row per (log, active segment, node) — i.e. "which nodes each log's active segment is currently writing to".

### 3.2 Panel 2 — "Active Segments per Node" (aggregate timeseries)

- Type `timeseries`.
- Query: `count by (node) (woodpecker_client_active_segment_node{log_ns=~"$log_ns"})` (the k8s version adds `cluster/namespace`), `legendFormat: "{{node}}"`.
- Effect: when a switch happens, the old node's line drops to 0 and the new node's line rises — the most direct way to confirm "everything has moved over".

### 3.3 Common constraints

- Each new panel gets a unique `id`; place `gridPos` at the end of each dashboard. **Do not add new template variables** (`dashboards_test.go` asserts on the template-variable set, so keep it unchanged).
- The existing validation tests for both dashboards must keep passing:
  - k8s: `tests/k8s/monitor/dashboards_test.go` (asserts valid JSON, no leftover `__DATASOURCE_UID__`, contains `${prometheus}`, and template vars include `prometheus/cluster/namespace/log_ns`). The new k8s panel must use the `${prometheus}` datasource and **must not** contain `__DATASOURCE_UID__`.
  - docker: `tests/docker/monitor/monitor_test.go` must keep passing; docker panels keep the `__DATASOURCE_UID__` placeholder.

## 4. Typical usage (switch verification)

- Has the old node been drained: `count(woodpecker_client_active_segment_node{node="10.0.0.5:8000"})` → should be 0 after the switch; non-zero means some active segments haven't moved off.
- Which logs haven't moved yet: `woodpecker_client_active_segment_node{node=~"<old nodes>"}` lists the `log_id`s directly.
- Current global distribution: `count by (node) (woodpecker_client_active_segment_node)`.
- Whether a switch happened: a change in `segment_id` for the same `log_id` marks one switch.
- Which pool a node belongs to: inferred from the node name (or by cross-referencing the static pool config); this metric does not provide it directly.

## 5. Files to change

Code:

1. `common/metrics/client_metrics.go` — add one `GaugeVec` `WpActiveSegmentNode` and register it in `RegisterClientMetricsWithRegisterer()`.
2. `woodpecker/segment/segment_handle.go` — `setActiveSegmentNodeMetrics()` / `clearActiveSegmentNodeMetrics()` helpers; call the clear at Active→Completed, rolling / `NotifyWriterInvalidation`, and close.
3. `woodpecker/log/log_handle.go` — trigger the set after `GetOrCreateWritableSegmentHandle` / `createNewSegmentMetaWithID` successfully obtains the quorum; clean up as a close fallback.

Dashboard:

4. `tests/docker/monitor/grafana/templates/dashboard_client.json` — add Panel 1/2 (`__DATASOURCE_UID__`, `log_ns` filter only).
5. `tests/k8s/monitor/grafana/templates/dashboard_client_k8s.json` — add Panel 1/2 (`${prometheus}`, `cluster/namespace/log_ns` filters).

Tests:

6. Keep `tests/k8s/monitor/dashboards_test.go` and `tests/docker/monitor/monitor_test.go` passing; optionally add assertions that the new metric name / new panel titles are present.

## 6. To pin down during implementation

- The exact `log_ns` derivation on the client side (aligned with how existing client metrics do it).
- The precise choke-point lines for set/clear (pinned during implementation, ensuring full coverage of the rolling / invalidation / close exit paths).
- The contents of `quorumInfo.Nodes` in cross-region / custom-placement scenarios (node endpoint format), so the `node` label matches the dashboard queries.
