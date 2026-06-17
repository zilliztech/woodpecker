# Woodpecker CLI (`wp`) — Design Document

| Field | Content |
|---|---|
| **Status** | ✅ Implemented — Phase 1/2/3 complete |
| **Branch** | `introduce_wp_cli` |
| **Related issue** | [zilliztech/woodpecker#128](https://github.com/zilliztech/woodpecker/issues/128) |
| **Last updated** | 2026-04-13 |
| **Author** | (TBD) |
| **Reviewers** | (TBD) |

## Abstract

`wp` is a **standalone Go binary CLI tool** serving the day-to-day operations of Woodpecker service-mode clusters. Its role is to **replace the SRE's repeated context-switching between `kubectl exec` + `curl /admin` + grep when troubleshooting / intervening in the cluster**, consolidating these actions into a zero-dependency (apart from the woodpecker server admin port) command-line tool.

**Its most unusual design decision** is: **it does not reinvent any external tool**. It does not wrap Jaeger, does not wrap Prometheus PromQL, does not wrap kubectl (except for the optional convenience of Phase 3 hybrid execute mode). Its unique value lies in exposing **runtime data that only the server process itself can see** — the in-flight operation list, in-memory writer buffer, flush queue depth, segment runtime state.

The tool will be delivered in **3 Phases**, ultimately delivering **35 feature subcommands** (7 command families) + **11 brand-new admin endpoints** (plus 4 enhancements or stub fill-ins of existing endpoints) + **1 new common package** (`common/runtime/opregistry/`), introducing no proto changes, data migration, or backward-incompatibility.

---

## Table of Contents

- [1. Architecture Overview](#1-architecture-overview)
- [2. Command Family Booklet](#2-command-family-booklet)
  - [2.A Node Lifecycle](#2a-node-lifecycle)
  - [2.B Cluster Overview](#2b-cluster-overview)
  - [2.C Logstore Runtime](#2c-logstore-runtime)
  - [2.D Metrics Local Analysis](#2d-metrics-local-analysis)
  - [2.E Config / Log Level / Env / pprof](#2e-config--log-level--env--pprof)
  - [2.F K8s Hybrid](#2f-k8s-hybrid)
  - [2.G In-flight Op Registry](#2g-in-flight-op-registry)
- [3. Cross-cutting Concerns](#3-cross-cutting-concerns)
- [4. Test Strategy](#4-test-strategy)
- [5. Phased Delivery Plan](#5-phased-delivery-plan)
- [6. Intrusion Summary (Reviewer Checklist)](#6-intrusion-summary-reviewer-checklist)
- [7. Non-goals](#7-non-goals)

---

## 1. Architecture Overview

### 1.1 Positioning

`wp` is a standalone operations CLI binary, serving the day-to-day operations of Woodpecker service-mode clusters. Its boundary is defined by three rules:

1. **Only interacts with Woodpecker Server nodes**, does not touch the embedded SDK (client) processes
2. **Does not reproduce the external tooling ecosystem** (Jaeger UI / Prometheus PromQL / kubectl), only exposes runtime data inside the process that external tools cannot see
3. **`GET` for reads, `POST` for interventions**, all actions go through existing or newly added `/admin/*` HTTP endpoints on the server; cross-node coordination is fanned out from the CLI side

### 1.2 Relationship to existing components

```
┌──────────────────────────────────────────────────────────────────────┐
│  Operator (human)                                                     │
│     │                                                                  │
│     │  wp <command>                                                    │
│     ▼                                                                  │
│  ┌──────────┐    ~/.woodpecker/cli.yaml (contexts, default endpoint)  │
│  │  wp CLI  │<── $WOODPECKER_ENDPOINT / $WOODPECKER_CONTEXT            │
│  └────┬─────┘    --endpoint, --context flags                           │
│       │                                                                │
│       │ 1) seed: GET /admin/memberlist                                 │
│       │ 2) fan-out: GET /admin/node/status (per node)                  │
│       │ 3) action:  POST /admin/node/decommission etc.                 │
│       ▼                                                                │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │ Woodpecker Server Cluster (service mode)                      │    │
│  │                                                                │    │
│  │   ┌─────────┐  ┌─────────┐  ┌─────────┐                      │    │
│  │   │ node-1  │  │ node-2  │  │ node-N  │  (gossip: 17946)     │    │
│  │   │ :9091   │  │ :9091   │  │ :9091   │  (admin/metrics HTTP) │    │
│  │   │ /admin/*│  │ /admin/*│  │ /admin/*│  (gRPC: 18080)        │    │
│  │   │ /metrics│  │ /metrics│  │ /metrics│                       │    │
│  │   └─────────┘  └─────────┘  └─────────┘                      │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                                                                        │
│  Out of scope for this CLI:                                            │
│     • Embedded SDK (woodpecker/ package, runs inside hosts like Milvus)│
│     • K8s Operator / WoodpeckerCluster CRD (managed via kubectl)       │
│     • Jaeger/Tempo trace backend queries (use their own UI/CLI)        │
└──────────────────────────────────────────────────────────────────────┘
```

### 1.3 Data flow of a typical command

Take `wp node list` as an example:

1. Parse flags/env/config to determine the seed endpoint (e.g. `http://node-1.wp.svc:9091`)
2. `HTTP GET {seed}/admin/memberlist` → obtain N nodes `{id, addr, az, rg, state}`
3. Concurrently `HTTP GET http://{node-addr}:9091/admin/node/status` for each node
   → obtain lifecycle state / decommission progress / node self-reported info
4. CLI side merges results, sorts, renders as table/json/yaml per `-o` option
5. Non-zero exit code if and only if: seed unreachable / memberlist failed / all targeted nodes are down

**One key convention**: fan-out tolerates partial failure. When some nodes are unreachable, the CLI lists them in the output as `UNREACHABLE` state, exit code remains 0 (unless `--strict` is specified).

### 1.4 Repo layout

```
woodpecker/
├── cmd/
│   ├── main.go                 # existing server entrypoint (untouched)
│   ├── external/               # existing (untouched)
│   └── wpcli/                  # ← new: the entire CLI lives here
│       ├── README.md
│       ├── main.go             # package main, cobra root assembly + Execute()
│       ├── cmd/                # package cmd, cobra subcommands
│       │   ├── root.go
│       │   ├── node.go
│       │   ├── cluster.go
│       │   ├── logstore.go
│       │   ├── metrics.go
│       │   ├── ops.go
│       │   ├── config.go
│       │   ├── logging.go
│       │   ├── profile.go
│       │   └── version.go
│       ├── client/             # admin HTTP client, discovery, fan-out
│       │   ├── client.go
│       │   ├── discovery.go
│       │   └── fanout.go
│       ├── output/             # table / json / yaml rendering
│       │   ├── table.go
│       │   ├── json.go
│       │   └── yaml.go
│       ├── config/             # CLI's own config & contexts parsing
│       │   └── cli_config.go
│       └── internal/           # CLI-only utilities
│
├── common/http/management/     # existing admin handlers directory
│   ├── node_handler.go         # existing (append cancel-decommission)
│   ├── admin_handler.go        # existing (empty TODO)
│   ├── logstore_handler.go     # ← new: family C endpoints
│   ├── ops_handler.go          # ← new: family G op registry endpoints
│   ├── config_handler.go       # ← new: family E config endpoint
│   ├── env_handler.go          # ← new: family E env endpoint
│   └── logging_handler.go      # ← new: family E log level endpoint
│
├── common/runtime/opregistry/  # ← new: op registry (common package)
│   ├── types.go
│   ├── registry.go
│   ├── eviction.go
│   ├── observer.go
│   ├── interceptor.go
│   ├── metrics.go
│   └── registry_test.go
│
├── common/metrics/
│   ├── op.go                   # ← new: StartOp/Op abstraction
│   ├── op_observer.go          # ← new: OpObserver chain
│   └── ...                     # existing metrics files
│
├── common/version/             # ← new (if missing): build info
│   └── version.go
│
├── tests/docker/wpcli/         # ← new: local E2E tests, strictly mirrors tests/docker/monitor/
│   └── ...
│
└── Makefile                    # add `make wpcli` target
```

### 1.5 Binary distribution

- `make wpcli` → outputs `bin/wp` (statically linked, cross-platform compilation supports linux/amd64, linux/arm64, darwin/arm64)
- Docker image: ship `wp` inside the server image at `/usr/local/bin/wp`, convenient for `kubectl exec pod -- wp ...` troubleshooting
- Release artifacts: `wp-<version>-<os>-<arch>.tar.gz`, plugged into the existing GitHub Release workflow

### 1.6 Non-goals (explicit)

| Non-goal | Reason |
|---|---|
| Any direct connection to the embedded SDK process | Per user decision, SDK runtime info is observable via metrics |
| Wrapping Jaeger/Tempo trace queries | Reinventing the wheel; external tools are mature |
| Direct K8s Operator CRD management (scale/upgrade/...) | `kubectl` + operator already cover this; family F provides hybrid fallback |
| Modifying configuration files (writing ConfigMap / yaml) for persistence | Runtime config changes go through admin endpoints; persistence stays with GitOps/ConfigMap |
| Authentication / authorization | Admin port is on the cluster's internal network by default; CLI is an admin tool |

---

## 2. Command Family Booklet

Complete command catalog (35 commands):

| Family | # subcommands | One-line role |
|---|---|---|
| **A** Node lifecycle | 6 | View node / decommission node / cancel decommission |
| **B** Cluster overview | 3 | View cluster health / topology / gossip consistency |
| **C** Logstore runtime | 7 | View server writer state + intervene (force-flush/fence/compact) |
| **D** Metrics local analysis | 5 | Directly scrape `/metrics`, local windowed aggregation + scenario reports |
| **E** Config / Logging / Env / pprof | 7 | View config, change log level, view environment, capture pprof |
| **F** K8s Hybrid | 4 | Default prints kubectl commands, executes when `-x` |
| **G** In-flight op registry | 3 | View running in-process ops + trace_id |

### 2.A Node Lifecycle

#### 2.A.1 Common prerequisites

**Node addressing convention**: All family A commands resolve `node-id` → `gossip advertise host` + default admin port 9091. The admin port across all nodes in the cluster must be uniform (when inconsistent, override with `--admin-port` or context configuration).

**Server change #0**: The existing `GET /admin/memberlist` returns plain text (`GetMemberlistStatus() string`); it must be extended to support `Accept: application/json`, returning `{members: [{id, gossip_addr, service_addr, az, rg, state, incarnation, last_seen, tags}]}`.

#### 2.A.2 Subcommands

##### A.1 `wp node list`

| Aspect | Content |
|---|---|
| **Purpose** | List all server nodes in the cluster and their key state |
| **Args** | none |
| **Flags** | `--az <az>` / `--rg <rg>` / `--state <state>` / `--strict` / global |
| **Inputs** | `GET /admin/memberlist` + concurrent `GET /admin/node/status` + `GET /healthz` |
| **Output columns** | `NAME ADDR STATE AZ RG HEALTH UPTIME` (`-o wide` adds `VERSION LAST_SEEN DECOM_PROGRESS`) |
| **Endpoint** | ✅ reuse (depends on #0) |

##### A.2 `wp node show <node>`

| Aspect | Content |
|---|---|
| **Purpose** | Single-node detailed view, sectioned output |
| **Flags** | `--with-metrics` (additionally scrape `/metrics`) |
| **Inputs** | `/admin/memberlist` for addressing → `/admin/node/status` + `/healthz` (+ `/metrics` optional + decommission progress optional) |
| **Output sections** | Identity / Placement / Lifecycle / Health / Resources(if --metrics) |
| **Endpoint** | ✅ reuse |

> Implicit small server change: the `GetNodeStatus()` return struct may need to add `started_at`, `version`, `last_health_check` fields.

##### A.3 `wp node decommission <node>`

| Aspect | Content |
|---|---|
| **Purpose** | Initiate graceful decommission |
| **Flags** | `--async` (default blocking), `--timeout 30m`, `--interval 3s`, `--heartbeat-interval 15s`, `-y` |
| **Inputs** | `POST /admin/node/decommission`, in blocking mode loops `GET /admin/node/decommission/progress` |
| **Behavior** | Default blocking; prints when progress numbers change; emits a heartbeat line + hint `wp ops list <node>` when no change within `--heartbeat-interval` |
| **Endpoint** | ✅ reuse |

##### A.4 `wp node drain-status <node>`

| Aspect | Content |
|---|---|
| **Purpose** | Observe decommission progress (does not initiate the action) |
| **Flags** | `-w/--watch`, `--interval 3s`, `--timeout 30m` |
| **Inputs** | `GET /admin/node/decommission/progress` |
| **Endpoint** | ✅ reuse |

##### A.5 `wp node cancel-decommission <node>` 🔨

| Aspect | Content |
|---|---|
| **Purpose** | Cancel an in-progress decommission, restore to active |
| **Flags** | `-y` |
| **Inputs** | `POST /admin/node/decommission/cancel` (**new**) |
| **State constraints** | **Strict**: only `decommissioning → active` is allowed; `active` (idempotent return); `decommissioned` returns error |

**New server endpoint**: `POST /admin/node/decommission/cancel`
- New `NodeLifecycleManager.CancelDecommission()` method (`server/lifecycle.go`)
- handler in `common/http/management/node_handler.go`
- path constant in `common/http/router.go`: `AdminNodeDecommissionCancelPath`
- `AdminCallbacks` adds `CancelDecommission func() error` field
- After cancellation, data already uploaded to object storage is **not** restored — only the "accepts new writes" state is restored

##### A.6 `wp node restart <node>` (stub)

```
$ wp node restart node-2
wp node restart is intentionally not implemented by this CLI.

On Kubernetes (managed by Woodpecker Operator):
  kubectl rollout restart sts/<cluster-name>-server

On bare metal / systemd:
  ssh to the node and run: sudo systemctl restart woodpecker

Why: the WP CLI does not assume a specific supervisor for process restart.
Restart responsibility belongs to the orchestrator.
```

Exit code `10`. ~20 lines of cobra implementation.

#### 2.A.3 Family A server-side change summary

| Change | Phase |
|---|---|
| `/admin/memberlist` supports JSON format | 1 |
| `/admin/node/status` field fill-in | 1 |
| `POST /admin/node/decommission/cancel` new endpoint | 1 |
| `NodeLifecycleManager.CancelDecommission()` strict version | 1 |

**1 new endpoint + 3 enhancements to existing endpoints**.

### 2.B Cluster Overview

Family B is **pure aggregation**, performs no intervention, **zero new endpoints**.

#### B.1 `wp cluster info`

Merges what was originally an independent `cluster topology` command.

| Flag | Default | Meaning |
|---|---|---|
| `-o table` | default | sectioned Overview + Topology tree (combined view) |
| `-o tree` | | topology tree only |
| `-o json/yaml` | | full structured data |

(For a flat one-node-per-row view, use `wp node list`.)

**Sample output** (default):
```
Cluster Overview
  Endpoint:       http://node-1.wp.svc:9091
  Total Nodes:    5   (reachable 4, unreachable 1)

By State
  active: 3  decommissioning: 1  decommissioned: 0  unreachable: 1

Versions
  v0.1.26: 4 nodes

Topology
├── us-east-1a / default
│   ├── node-1   active           3h12m  v0.1.26
│   └── node-2   decommissioning  2h50m  v0.1.26  (remaining_procs=3)
├── us-east-1b / default
│   ├── node-3   active           3h05m  v0.1.26
│   └── node-4   UNREACHABLE      last seen 2m15s ago
└── us-east-1c / default
    └── node-5   active           1h40m  v0.1.26

Warnings
  ⚠  node-4 (us-east-1b) has been unreachable for 00:02:15
  ⚠  node-2 is decommissioning; see `wp node drain-status node-2 -w`
```

#### B.2 `wp cluster health`

**Replica-count-aware red/yellow/green health check**.

Replica count source priority:
1. **Preferred**: scrape `/metrics`, read per-log ensemble size / write quorum (align metric names during implementation)
2. **Fallback**: default N=3
3. **Override**: `--expected-replicas N` or `--per-log-expected log=n,...`

**Rating rules**:
- 🟢 **Green**: `active_nodes_total >= N` **and** every AZ has ≥ `ceil(N/2)` active nodes **and** all nodes reachable **and** `/healthz` all OK **and** versions consistent
- 🟡 **Yellow**: active nodes ≥ N but losing one would drop below threshold; or there are decommissioning nodes; or rolling upgrade (versions inconsistent); or some RG has node count == N (zero redundancy); or some log under-replicated but still ≥ majority
- 🔴 **Red**: `active_nodes_total < N` (quorum breached); or there is UNREACHABLE; or `/healthz` FAIL; or some AZ that originally had nodes now has 0; or some log dropped to `< Wq`

**Output includes a Log Replica Health section** (when metrics available):
```
Log Replica Health (default expected ≥3)
  ✅ log-catalog        expected=5  active=5/5
  ✅ log-write-ahead    expected=3  active=3/3
  🔴 log-metrics        expected=3  active=2/3   (node-4 unreachable)
```

#### B.3 `wp cluster gossip-diff`

Cross-node memberlist view comparison to find split-brain.

Output contains two sections:
- **Membership Set Agreement**: the member set seen by each node
- **Per-Member State Divergence**: differences in `incarnation / state / last_seen` for the same member as seen by different nodes (`last_seen` deviation threshold defaults to 30s, adjustable via `--last-seen-tolerance`)

Additional conditions for being judged inconsistent (besides member-set mismatch):
- Same member's `incarnation` differs by ≥ 2 across different group views
- Same member shown as `alive` in one group's view, `suspect/dead` in another

#### Family B server-side changes

**Zero**. Built entirely on family A's JSON memberlist + `/admin/node/status` + `/metrics`.

### 2.C Logstore Runtime

Family C **adds the most new endpoints**. Its role is "in-process data view + intervention operations", matching the issue's "system debug buffer / queue / dump".

**Boundary with family G**:
- Family C answers "what is the current **state** of the writer/segment/buffer/queue" (nouns, data-structure snapshots)
- Family G answers "which **operations** are currently running, where they're stuck, how long they've been running" (verbs, in-flight aliveness)

#### 2.C.1 Common prerequisite: Active Writer Registry

All new endpoints in family C require the server to first expose a `WriterRegistry` abstraction (`server/storage/writer_registry.go`, new file):

```go
type WriterRegistry interface {
    List() []WriterSnapshot
    Get(logId, segmentId int64) *WriterSnapshot
    ForceFlush(ctx context.Context, logId, segmentId int64) error
    Fence(ctx context.Context, logId, segmentId int64, reason string) error
    Compact(ctx context.Context, logId, segmentId int64) error
}
```

The `storage.Writer` interface adds `Snapshot()` and `SnapshotDetailed()` methods, implemented by both backends (`objectstorage`, `stagedstorage`) respectively.

#### 2.C.2 Subcommands

**Family C has 7 subcommands** (the `logstore dump` from the original inventory is merged into `segment-show --full -o json`, not standalone):

| Subcommand | Type | One-liner | Endpoint |
|---|---|---|---|
| `logstore segments <node>` | observe | List active segments | 🔨 `GET /admin/logstore/segments` |
| `logstore segment-show <node>` | observe | Single segment detail (`--full -o json` equivalent to dump) | 🔨 `GET /admin/logstore/segments/{log}/{seg}` |
| `logstore buffer <node>` | observe | Aggregate of all writer buffers (local aggregation of C.1 data) | reuse |
| `logstore flush-queue <node>` | observe | Flush queue depth aggregate (local aggregation of C.1 data) | reuse |
| `logstore force-flush <node>` | intervene | Force sync | 🔨 `POST /admin/logstore/flush` |
| `logstore fence <node>` | ⚠️ intervene | Force fence segment | 🔨 `POST /admin/logstore/fence` |
| `logstore compact <node>` | intervene | Force compaction | 🔨 `POST /admin/logstore/compact` |

##### C.1 `wp logstore segments <node>`

**Endpoint**: `GET /admin/logstore/segments`, supports `?log_id=&state=&writable=` filtering

**Returns JSON** (one entry per writer):
```json
{
  "segments": [{
    "log_id": 42, "segment_id": 7,
    "backend": "staged",
    "state": "active",
    "writable": true, "fenced": false, "finalized": false, "recovered": true,
    "size_bytes": 125829120, "entry_count": 48321,
    "first_entry_id": 1000000, "last_entry_id": 1048320,
    "last_submitted_flushing_entry_id": 1048299,
    "last_submitted_flushing_block_id": 42,
    "buffer_bytes": 2097152, "buffer_entries": 821,
    "flush_queue_depth": 12, "flush_queue_capacity": 960,
    "written_bytes": 125829120,
    "last_modified_ms": 1712553600000,
    "created_at_ms": 1712550000000
  }]
}
```

##### C.2 `wp logstore segment-show <node> --log X --seg Y [--full]`

**Endpoint**: `GET /admin/logstore/segments/{log_id}/{seg_id}?detailed=true` (`--full` triggers detailed)

`segment-show --full -o json` is equivalent to the `wp logstore dump` from the original inventory — sharing the same endpoint, avoiding command sprawl.

##### C.3 `wp logstore buffer <node>` / C.4 `wp logstore flush-queue <node>`

**Reuse the C.1 endpoint**, CLI-side local aggregation + top-N sorting. **No new endpoints**.

##### C.5 `wp logstore force-flush <node>`

**Endpoint**: `POST /admin/logstore/flush`, body `{"log_id": 42, "segment_id": 7}` or `{}` (all)

##### C.6 `wp logstore fence <node>` ⚠️

**High-risk intervention**. Endpoint: `POST /admin/logstore/fence`, body `{"log_id": ..., "segment_id": ..., "reason": "..."}`

`--reason` is a required flag, written to the log and fence file for after-the-fact traceability. `-y` skips confirmation (use with caution in production).

##### C.7 `wp logstore compact <node>`

**Endpoint**: `POST /admin/logstore/compact`. Precondition: segment must be finalized; the server validates this and returns 409 if not satisfied.

#### 2.C.3 Family C server-side change summary

| Change | File |
|---|---|
| `GET /admin/logstore/segments` | `common/http/management/logstore_handler.go` (new) |
| `GET /admin/logstore/segments/{log}/{seg}` | same as above |
| `POST /admin/logstore/flush` | same as above |
| `POST /admin/logstore/fence` | same as above |
| `POST /admin/logstore/compact` | same as above |
| `WriterRegistry` interface + implementation | `server/storage/writer_registry.go` (new) |
| `storage.Writer.Snapshot()` / `SnapshotDetailed()` | `server/storage/writer.go` + two backends |
| Path constants | `common/http/router.go` |
| AdminCallbacks extension | `common/http/server.go` |

**5 new endpoints**.

### 2.D Metrics Local Analysis

#### 2.D.1 Positioning

`wp metrics` is a **temporary on-the-spot fault analysis tool**. Its niche is **"things Prometheus can't see or can't see in time"**:
- **Zero latency**: directly scrape the node's `/metrics`, no Prometheus 15-30s scrape interval
- **Zero dependencies**: works in new clusters, demo environments, isolated networks, environments without Prometheus
- **Zero query language**: no PromQL to learn, just use scenario names
- **Window-of-now**: the CLI itself acts as a **temporary sampler**, collecting from **right now** for N seconds/minutes

**Explicit boundaries**:
- Family D **never queries historical data**
- Family D **never accesses any external system** (no Prometheus integration)
- Family D **does not look at client-side metrics**

#### 2.D.2 Subcommands

| Subcommand | One-liner | Mode |
|---|---|---|
| `metrics list <node>` | List which metric series this node exposes | 1 scrape |
| `metrics snapshot [<node>\|--all]` | Capture **right-now** point snapshots | 1 scrape |
| `metrics top --by <metric>` | Cross-node top-N by metric | 1 scrape × N nodes |
| `metrics watch <metric>` | Streaming real-time display | periodic scrape |
| `metrics report --scenario <name>` | Predefined scenario: window collection + joint analysis | periodic scrape over window |

##### D.4 `wp metrics watch` — streaming-only

**Streaming mode only**. Scrapes once every `--interval` (default 1s), prints line-by-line in append fashion, with `↑ ↓` trend arrows. Ctrl+C to exit.

##### D.5 `wp metrics report --scenario <name>` — dual mode

Supports two parameter forms:
- `--scenario <name>`: multi-metric joint analysis per predefined rules
- `--metric <name>`: statistical summary for a single metric (min/max/avg/p50/p99/stddev/trend)

**12 built-in scenarios**:

| Scenario | Aspect | Check |
|---|---|---|
| `hot-write` | write | severe imbalance in append rate across nodes |
| `slow-write` | write | append p99 deviates from baseline |
| `stuck-write` | write | append barely moves; client retries climbing |
| `slow-slot` | write | a slot in the quorum ensemble is dragging the whole down |
| `stuck-flush` | write | flush queue high + p99 spikes + errors growing |
| `slow-compact` | write | compact p99 deviates + errors growing |
| `fencing` | write | fence events in window exceed threshold |
| `slow-read` | read | read p99 deviates + read queue grows |
| `stuck-read` | read | read throughput ≈ 0 but pending exists |
| `read-amplification` | read | ratio of block reads / entry reads abnormal |
| `quorum-degraded` | structural | some log has active replicas < ensemble |
| `under-replication` | structural | persistently under-replicated within window |

**Scenarios are YAML-driven**, files located at `cmd/wpcli/internal/scenarios/*.yaml`. Each scenario declares metrics, rules, severity, hint commands.

**Core mechanism**: the CLI acts as a temporary in-memory time-series collector, continuously scrapes the relevant metrics within the window (default 1m), and after the window ends applies scenario rules to output findings + recommended "next-step commands".

**Scenario rule YAML example**:
```yaml
name: stuck-flush
description: Detect flush-path stalls and buildup
metrics:
  - name: woodpecker_logstore_flush_queue_depth
    type: gauge
  - name: woodpecker_logstore_flush_duration_seconds
    type: histogram
    quantile: 0.99
rules:
  - id: sustained_queue_high
    severity: red
    condition: |
      flush_queue_depth / flush_queue_capacity > 0.25
      for at least 50% of window samples
    hints:
      - "wp ops list {node} --type flush --longer-than 30s"
      - "wp logstore segment-show {node} --log {log} --seg {segment}"
```

#### 2.D.3 Family D server-side changes

**Zero**. But there is one implicit dependency: the metric series used by D.5 scenarios must exist with appropriate labels. Do a metric inventory pass during implementation; fill in any missing series.

### 2.E Config / Log Level / Env / pprof

#### 2.E.1 Subcommands (7)

| Subcommand | One-liner | Phase |
|---|---|---|
| `config show <node>` | Print the currently effective configuration | 1 |
| `config diff` | Compare config across nodes | 1 |
| `env show <node>` | Print env vars + Go runtime + host + build | 1 |
| `env diff` | Compare env across nodes | 1 |
| `logging get-level <node>` | Read the current log level | 2 |
| `logging set-level <node>` | Change the log level (no auto-revert) | 2 |
| `profile <node>` | Capture pprof | 1 |

#### 2.E.2 Key design

**E.1 no redaction**: `GET /admin/config` returns the full cfg directly. Reasoning: the CLI is an admin tool. The security caveat is documented in deployment docs (the admin port must be network-isolated).

**E.4 no auto-revert**: `logging set-level` is a one-time level change, with no server-side timer/state. Users who need "revert in 10 minutes" can use a one-line shell:
```bash
wp logging set-level node-2 --level debug && sleep 600 && wp logging set-level node-2 --level info
```

**E.6/E.7 env is a separate subcommand family**: not conflated with config. `/admin/env` returns 4 sections:
```json
{
  "env": { "HOSTNAME": "...", "WOODPECKER_NODE_NAME": "...", ... },
  "runtime": { "go_version": "go1.22.1", "gomaxprocs": 8, ... },
  "host": { "hostname": "...", "os": "linux", "kernel": "...", ... },
  "build": { "version": "v0.1.26", "commit": "2c88470", "build_time": "...", "go_version": "..." }
}
```

`env diff` has a built-in ignore set (noisy keys like `HOSTNAME / PWD / SHLVL / KUBERNETES_*_PORT_*`), append more via `--ignore <regex>`.

**E.5 profile**: pure CLI wrapper, zero server-side changes, reuses existing `/debug/pprof/*`.

#### 2.E.3 Family E server-side change summary

| Change | Phase |
|---|---|
| `GET /admin/config` new endpoint | 1 |
| `GET /admin/env` new endpoint | 1 |
| `GET /log/level` + `POST /log/level` stub fill-in (using zap AtomicLevel) | 2 |
| `common/logger/logger.go` add `GetLevel()` / `SetLevel()` | 2 |
| Linker ldflags injection of build info | 1 |

**2 new endpoints + 2 stub fill-ins**.

### 2.F K8s Hybrid

#### 2.F.1 Design core: Print + Execute dual mode

**Default mode (print-only)**: print the recommended kubectl command and suggestions, do not execute. Zero side effects, zero dependencies.

**Execute mode (`-x` / `--execute`)**: assemble the kubectl command using `--namespace / --wp-cluster / --kube-context / --kubeconfig` and shell out to execute; stdout/stderr/exit code/signal pass through.

**Auto-degradation when kubectl is absent**: if `-x` is given but kubectl can't be found in `$PATH` (and no `--kubectl <path>` override), degrade to print mode + warning.

#### 2.F.2 Shared k8s flags

| Flag | Default | Description |
|---|---|---|
| `-x / --execute` | false | Trigger execution |
| `--kubectl <path>` | `$(which kubectl)` | kubectl binary path |
| `-n / --namespace <ns>` | from cli.yaml or kubeconfig | namespace |
| `--wp-cluster <name>` | from cli.yaml | WoodpeckerCluster CR name |
| `--kube-context <ctx>` | kubeconfig current | kubectl context (does not conflict with wp `--context`) |
| `--kubeconfig <path>` | `$KUBECONFIG` or `~/.kube/config` | passthrough |

#### 2.F.3 Subcommands (4)

| Subcommand | Default mode | Execute mode |
|---|---|---|
| `k8s status` | Prints `kubectl get woodpeckercluster + describe + get pods` commands + suggestions | Actually executes and prints results |
| `k8s scale --replicas N` | Prints `kubectl patch woodpeckercluster ...` | Actually executes |
| `k8s logs <node-or-pod>` | Prints `kubectl logs ...` | Actually executes (including `-f` passthrough); node-id → pod name uses `<cluster>-server-<ordinal>` convention |
| `k8s doctor` | Pure stub (still a stub in Phase 3, real impl in a future release) | Does not support `-x` |

#### 2.F.4 Server-side changes

**Zero**. Family F is purely CLI-side + shell-out.

### 2.G In-flight Op Registry

#### 2.G.1 Positioning

**Lightweight stuck-op sentry**. Not a replacement for an in-process debugger. SREs use it to know "what ops are currently running on this node, are any taking too long". If you want to know the internals of an op — take the `trace_id` and query your own trace backend.

#### 2.G.2 Design principles (4)

1. **Bounded pool + drop-oldest**: when capacity is full, evict the oldest, keeping memory bounded
2. **Eviction is a signal**: an old op being evicted = stall signal; a young op being evicted = high throughput
3. **OpRecord is minimal and immutable**: 7 fields, written once at registration, deleted only at End (**no phase / no mutable extra**)
4. **Zero business code imports opregistry**: hooked in indirectly via `common/metrics/op.go`

#### 2.G.3 Server-side architecture

**Package structure**:

```
common/runtime/opregistry/        ← common registry
├── types.go                       ← OpType / OpRecord / Filter / Stats
├── registry.go                    ← bounded pool + drop-oldest
├── eviction.go                    ← eviction + young/old signal buckets
├── metrics.go                     ← Prometheus self-metrics
├── observer.go                    ← implements metrics.OpObserver
├── interceptor.go                 ← grpc UnaryServerInterceptor
└── registry_test.go

common/metrics/op.go               ← new file: StartOp/Op abstraction + observer chain
common/metrics/op_observer.go      ← new file: OpObserver interface
```

**OpRecord 7 fields (immutable)**:

```go
type OpRecord struct {
    OpID      string    // e.g. "fl-7d2f4a9-001"
    OpType    OpType    // append/read/flush/compact/recover/fence/meta
    TraceID   string    // OTel hex; empty = no span context
    SpanID    string    // OTel hex
    StartedAt time.Time
    LogID     int64     // 0 = N/A
    SegmentID int64     // 0 = N/A
}
```

Each record is ~100 bytes; capacity 1024 totals ~100KB.

**`metrics.Op` bridge**:

```go
type Op struct {
    opType   string
    labels   prometheus.Labels
    histo    prometheus.Observer
    start    time.Time
}

func StartOp(opType string, hist prometheus.Observer, labels prometheus.Labels) *Op
func (o *Op) End(status string)
```

Business-code usage:
```go
op := metrics.StartOp("file.flush",
    metrics.WpFileFlushLatency.WithLabelValues(...),
    prometheus.Labels{"log_id": w.logIdStr, "segment_id": w.segmentIdStr})
var status = "success"
defer func() { op.End(status) }()
// ... do work ...
if err != nil { status = "error" }
```

**OpObserver interface (only 2 hooks)**:

```go
type OpObserver interface {
    OnOpStart(op *Op) uint64
    OnOpEnd(op *Op, handle uint64, elapsed time.Duration, status string)
}

func RegisterOpObserver(o OpObserver)
```

**Eviction policy (`eviction.go`)**:
- When at capacity, take the oldest from list front and delete
- Compute age = now - StartedAt
- `age < warn_age` (default 30s) → `EvictedYoung++`, DEBUG log
- `age >= warn_age` → `EvictedOld++`, **WARN log**
- Prometheus histogram `wp_op_registry_evicted_age_seconds.Observe(age.Seconds())`
- Prometheus counter `wp_op_registry_evicted_total{signal="young|old"}.Inc()`

**The `evicted_old` counter is the core alarm metric**:
```
rate(wp_op_registry_evicted_total{signal="old"}[5m]) > 0
```
=== the cluster is stalling.

**Business code mechanical migration (~12 op-shaped histogram call sites)**:

| Location | Existing metric | New op_type |
|---|---|---|
| `server/logstore.go` × 9 methods | `WpLogStoreOperationLatency` | `logstore.<method>` |
| `server/storage/objectstorage/writer_impl.go` flush | `WpFileFlushLatency` | `file.flush` |
| `server/storage/stagedstorage/writer_impl.go` flush | `WpFileFlushLatency` | `file.flush` |
| `server/storage/disk/writer_impl.go:617` flush | `WpFileFlushLatency` | `file.flush` |
| compact / recover / fence entries × 2 backends | `WpFileCompactLatency` etc. (recovery/fence may need new metrics) | `file.compact` etc. |

Each location is ~5 lines of diff, pure mechanical replacement:

```diff
- start := time.Now()
- // ... do work ...
- if err != nil {
-     metrics.WpLogStoreOperationLatency.WithLabelValues(...).
-         Observe(float64(time.Since(start).Milliseconds()))
- }
+ op := metrics.StartOp("logstore.add_entry", metrics.WpLogStoreOperationLatency, labels)
+ var status = "success"
+ defer func() { op.End(status) }()
+ // ... do work ...
+ if err != nil { status = "error" }
```

**gRPC interceptor as backstop**:

```go
// common/runtime/opregistry/interceptor.go
func UnaryInterceptor() grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
        opType := opTypeFromGRPCMethod(info.FullMethod)
        labels := prometheus.Labels{"method": info.FullMethod, ...}
        op := metrics.StartOp(string(opType), nil, labels)
        defer func() { /* op.End(status) */ }()
        return handler(ctx, req)
    }
}
```

`cmd/main.go` only needs to attach it once when creating the grpc.Server.

**Server startup phase (`cmd/main.go`)**:
```go
opReg := opregistry.New(
    cfg.Woodpecker.Runtime.OpRegistry.Capacity,    // default 1024
    cfg.Woodpecker.Runtime.OpRegistry.WarnAge,     // default 30s
)
metrics.RegisterOpObserver(opReg)

adminCallbacks.Ops = http_management.OpsCallbacks{
    List:  opReg.List,
    Get:   opReg.Get,
    Stats: opReg.Stats,
}
```

#### 2.G.4 CLI commands (3)

##### G.1 `wp ops list <node>`

**Flags**: `--type` (multi-value) / `--log` / `--seg` / `--longer-than` / `--limit` / `--sort-by` / `--all-nodes` / `-o`

**Endpoint**: `GET /admin/runtime/ops?type=...&log_id=...&segment_id=...&longer_than_ms=...&limit=...`

**Output (single node)**:
```
$ wp ops list node-2
Node: node-2
Pool: 487/1024 used (47.6%)
Evicted: total=12,341  young=12,287  old=54  ⚠
Last evicted: 2.3s ago, age was 0.4s

OP_TYPE             OP_ID                  TRACE_ID         ELAPSED   LOG:SEG
logstore.add_entry  ap-7d2f4a9-001         7d2f4a9...       1m23s 🟡  42:7
file.compact        cp-3e91c4a-002         3e91c4a...       45s   🟡  42:5
file.flush          fl-8a1b2c3-003         8a1b2c3...       18s       17:2
... (484 more, use --limit to see more, or --longer-than 30s for stuck ops)

Hint: pick a TRACE_ID and query your trace backend for the full span tree.
```

##### G.2 `wp ops show <node> --op-id <id>`

```
$ wp ops show node-2 --op-id fl-7d2f4a9-001
Op fl-7d2f4a9-001 on node-2

Identity
  op_type:     file.flush
  op_id:       fl-7d2f4a9-001
  trace_id:    7d2fa83e1c4b9f8a7e3d6c5b4a3210ed
  span_id:     a9b8c7d6e5f43210

Timing
  started_at:  2026-04-08T14:31:55Z
  elapsed:     1m23s
  age status:  🟡 above warn age (30s)

Context
  log_id:      42
  segment_id:  7

Trace
  Use this trace_id with your trace backend:
    7d2fa83e1c4b9f8a7e3d6c5b4a3210ed

Cross-references
  wp logstore segment-show node-2 --log 42 --seg 7
  wp metrics watch woodpecker_logstore_file_flush_latency --node node-2 --labels log=42
```

##### G.3 `wp ops stats <node>`

```
$ wp ops stats node-2
Op Registry — node-2

Capacity:    1024
In Use:      487  (47.6%)
Warn Age:    30s

Eviction Totals (since process start, uptime 12d8h)
  total:        12,341
  young:        12,287   (99.6%)   age < 30s
  old:              54   (0.4%)    age ≥ 30s — STALL SIGNAL ⚠

Recent Eviction Activity (Prometheus histogram, last 1h)
   0-1s:    8,432
   1-5s:    3,201
   5-30s:     603
  30s-2m:     47   ⚠
   2m-5m:      7   🔴

Health Interpretation
  ⚠ Recent evictions had non-trivial old-bucket count (54 since boot, 47 in
    last hour landed in 30s-2m bucket and 7 in 2m-5m bucket).

    Likely meaning: heavy ops (probably flush/compact) are taking longer than
    30s to complete, accumulating in the registry, and getting bumped out by
    newer ops. This is the early warning of a write-path stall.

    Investigate with:
      wp ops list node-2 --longer-than 30s
      wp metrics report --scenario stuck-flush --window 2m --nodes node-2
```

#### 2.G.5 Family G admin endpoints

| Path | Method | Purpose |
|---|---|---|
| `/admin/runtime/ops` | GET | List ops with query filters |
| `/admin/runtime/ops/{op_id}` | GET | Get single op |
| `/admin/runtime/ops/stats` | GET | RegistryStats |

#### 2.G.6 Family G server-side change summary

**Single largest engineering effort**:

| Category | Change |
|---|---|
| New common package `common/runtime/opregistry/` | ~500 lines |
| New files `common/metrics/op.go` + `op_observer.go` | ~200 lines |
| Config `Woodpecker.Runtime.OpRegistry.{Capacity, WarnAge}` | `common/config/configuration.go` |
| `cmd/main.go` wiring (instantiate + RegisterOpObserver + admin callbacks + add interceptor to grpc.NewServer) | ~30 lines |
| Business-code mechanical migration ~12 sites | `server/logstore.go` × 9 + `server/storage/{disk,objectstorage,stagedstorage}/writer_impl.go` × 3-5 |
| New endpoints × 3 | `common/http/management/ops_handler.go` (new) |

About **1100 lines of new code + ~150 lines of mechanical migration**. **No business file imports opregistry**.

---

## 3. Cross-cutting Concerns

### 3.1 Context model (kubeconfig style)

CLI config file `~/.woodpecker/cli.yaml`:

```yaml
current-context: prod

contexts:
  prod:
    endpoint:    http://prod.wp.svc:9091
    admin_port:  9091
    timeout:     30s
    concurrency: 8
    strict:      false
    k8s:
      enabled:      true
      kubectl:      kubectl
      kubeconfig:   ~/.kube/prod
      kube_context: prod-eks
      namespace:    woodpecker-prod
      cluster:      my-woodpecker

  dev:
    endpoint: http://localhost:9091
    k8s:
      enabled:   true
      namespace: wp-dev
      cluster:   dev
      kube_context: kind-dev

defaults:
  output:    table
  no_color:  false
  page_size: 50
```

**Config-file lookup order**:
1. `--config <path>` flag
2. `$WOODPECKER_CLI_CONFIG`
3. `$XDG_CONFIG_HOME/woodpecker/cli.yaml`
4. `~/.woodpecker/cli.yaml`

### 3.2 Configuration resolution chain

```
cmdline flag  >  env var  >  current context field  >  defaults section  >  built-in hardcoded default
```

**Key flag / env / context field mapping**:

| Flag | Env Var | Context field | Default |
|---|---|---|---|
| `--context <name>` | `WOODPECKER_CONTEXT` | `current-context` | (first context) |
| `--endpoint <url>` | `WOODPECKER_ENDPOINT` | `contexts.<name>.endpoint` | (none) |
| `--admin-port <n>` | `WOODPECKER_ADMIN_PORT` | `contexts.<name>.admin_port` | `9091` |
| `--timeout <dur>` | `WOODPECKER_TIMEOUT` | `contexts.<name>.timeout` | `30s` |
| `--concurrency <n>` | — | `contexts.<name>.concurrency` | `8` |
| `--strict` | — | `contexts.<name>.strict` | `false` |
| `-o, --output <fmt>` | `WOODPECKER_OUTPUT` | `defaults.output` | `table` |
| `--no-color` | `NO_COLOR` | `defaults.no_color` | auto-detect tty |
| `-v, --verbose` | `WOODPECKER_VERBOSE` | — | `0` |

> **Note (#187):** `WOODPECKER_ENDPOINT` is implemented in
> `cmd/wpcli/cmd/resolver.go` and is the zero-config in-pod default — all server
> images set `WOODPECKER_ENDPOINT=http://localhost:9091`. Per the chain above it
> sits above the cli.yaml context, so it overrides a pod-mounted `cli.yaml`.

### 3.3 Global flags

```
Global Flags:
  --context string         CLI context name
  --endpoint string        Admin HTTP seed endpoint
  --admin-port int         Admin port for fan-out (default 9091)
  --timeout duration       Per-request timeout (default 30s)
  --concurrency int        Fan-out concurrency (default 8)
  --strict                 Treat partial fan-out failures as errors
  -o, --output string      Output format: table|json|yaml|wide
  --no-color               Disable color in output
  -v, --verbose count      Increase verbosity (-v info, -vv debug, -vvv trace)
  --help                   Show help
  --version                Show CLI version
```

### 3.4 Output format conventions

**All commands default to `-o table`**.

| Format | Meaning | Applies to |
|---|---|---|
| `table` | default. list-type as rows/columns; show/stats-type as sectioned key-value | all commands |
| `wide` | enhanced table with extra columns | list-type |
| `json` | RFC 8259 JSON | all |
| `yaml` | YAML 1.2 | all |
| `tree` | ASCII tree | only `wp cluster info` |
| `unified` | `diff -u` style | only `config diff` / `env diff` |
| `raw` | original Prometheus text format | only `metrics snapshot` |

**Severity markers**:

| Marker | ANSI |
|---|---|
| `✅` OK | green |
| `🟡` Warning | yellow |
| `🔴` Critical | red |
| `⚠` Annotation | yellow |
| `↑ ↓` Trend | (none) |
| `—` N/A | dim |

**Color disable**: via `NO_COLOR` env var, `--no-color` flag, or auto-disabled when stdout is non-tty.

**Output split**:
- **stdout** = the command's normal output (table/json/yaml)
- **stderr** = errors, warnings, debug logs, progress hints

### 3.5 Exit code table

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Network/connection error |
| 2 | Usage error |
| 3 | Target does not exist |
| 4 | State conflict |
| 5 | Wait/Watch timeout |
| 6 | Strict mode partial failure |
| 7 | User abort (confirmation answered "n") |
| 8 | Yellow finding |
| 9 | Red finding |
| 10 | Intentionally not implemented |
| 11 | Op/resource id does not exist |
| 12 | CLI configuration error |
| 13 | Precondition missing |
| 130 | SIGINT (Ctrl+C, Unix standard) |
| 100-200 | k8s execute passthrough = `100 + kubectl_exit_code` |

**Error display formats**:

Table mode (human-readable):
```
wp: <command>: <short reason>
  Detail: <one-line context>

  Hint: <suggested fix or next command>
```

JSON mode:
```json
{
  "error": {
    "code": 4,
    "command": "node decommission",
    "message": "state conflict",
    "detail": "node-2 is already in 'decommissioned' state",
    "hint": "use `wp node show node-2` to see current state"
  }
}
```

### 3.6 Network behavior

| Item | Default / Behavior |
|---|---|
| Single HTTP call timeout | 30s |
| Whole `--watch` / `--wait` | 30m |
| Fan-out concurrency | 8 |
| Retries | **No retries** (`--watch`/`--wait` is itself polling) |
| TLS | Phase 1 does not support HTTPS admin port; the user's HTTPS endpoint can be reached via `--endpoint https://...` but the CLI does not verify certificates |

### 3.7 Security model

The `wp` CLI is designed as an **admin tool**, with **no built-in authentication / authorization**. The admin HTTP port (default `:9091`) must be restricted by deployment ops via **network-layer isolation** (K8s NetworkPolicy / security groups / firewalls) so that only the cluster-internal and admin networks can reach it. The CLI does none of the following:

- Does not send Authorization headers
- Does not handle client certificates
- Does not do OAuth / OIDC / SSO
- Does not do RBAC

Anyone who can reach `:9091` is treated as admin. If your environment requires the admin port to be public-facing, deploy a reverse proxy in front of the server to perform authentication; the CLI will work transparently through it.

### 3.8 The CLI's own logging

The `-v` count flag controls the verbosity of the CLI's own internal logs (**not** the server-side log level):

| Flag | Level | Content |
|---|---|---|
| (none) | error | error messages only |
| `-v` | info | adds fan-out progress, HTTP request summaries |
| `-vv` | debug | adds URL/status/duration of each HTTP request |
| `-vvv` | trace | adds HTTP request/response bodies |

**All verbose output goes to stderr**.

### 3.9 Input conventions

**Node identifiers**: support three forms (in priority order)
1. `node_id` (from memberlist, most authoritative)
2. `host:port` (gossip advertise addr)
3. IP prefix (must match uniquely)

**Duration**: Go's standard `time.ParseDuration` format (`30s`, `5m`, `2h30m`)

**List / multi-value flags**: support `--type a --type b` or `--type a,b`

**Confirmation prompts**: intervention commands require confirmation by default; `-y / --yes` skips; **when `stdin` is non-tty, execution is refused outright (exit 2)** to avoid script misuse.

---

## 4. Test Strategy

### 4.1 Test pyramid (simplified, 2 layers)

```
        ▲
        │
        │ Layer 2: Local E2E tests (docker-compose brings up cluster)
        │   Location: tests/docker/wpcli/ (strictly mirrors tests/docker/monitor/)
        │   Trigger: developer runs ./run_wpcli_tests.sh locally
        │   Not in CI workflow, does not block PRs
        │   Coverage: end-to-end functionality of each command family against
        │             a real cluster + eviction-signal verification
        │             + wp's fan-out / intervention against a docker-compose
        │             multi-node cluster
        │   Realism: real `wp` binary against real `woodpecker server` processes +
        │            real etcd + real MinIO + real gossip / real admin HTTP
        │
        │ Layer 1: Unit tests
        │   Location: _test.go in same package as source
        │   Trigger: go test ./... (runs as part of existing repo CI)
        │   Coverage:
        │     • opregistry / metrics op chain
        │     • new admin handlers
        │     • CLI cobra commands / output renderer / config / fanout
        │     • exit-code mapping table-driven test
        │     • CancelDecommission addition in lifecycle.go
        │     • business-code metric migration regression tests
        ▼
```

**This WIP introduces no wpcli-specific CI workflow**. Unit tests run as part of the existing repo CI.

### 4.2 Unit test coverage

**New server-side code**:

| Module | Key tests |
|---|---|
| `common/runtime/opregistry/` | Register/Done lifecycle, FIFO order, eviction young/old bucketing, log level, Prometheus metrics, ListFilter, concurrent race (`go test -race`), Stats consistency |
| `common/metrics/op.go` + `op_observer.go` | StartOp/End triggers observer, histogram.Observe call, End idempotency, observer panic isolation, concurrent race |
| `common/http/management/*_handler_test.go` | for each new handler: happy path, 405, 404, 500, query param parsing |
| `server/lifecycle_test.go` additions | `TestCancelDecommission_*` three state transitions + concurrency |
| Business-code metric migration regression | one test per migration site verifying original histogram behavior unchanged |

**CLI side**:

| Module | Key tests |
|---|---|
| `cmd/wpcli/cmd/*_test.go` | each subcommand: happy + error path + flag parsing |
| `cmd/wpcli/output/*_test.go` | golden file tests, `-update` flag for one-shot regeneration |
| `cmd/wpcli/config/cli_config_test.go` | YAML parsing, context resolution, three-tier overlay, error handling |
| `cmd/wpcli/client/fanout_test.go` | all-success, partial failure default/strict, all-failure, concurrency cap, timeout, ctx cancel |
| `cmd/wpcli/internal/errors/exit_test.go` | table-driven coverage of **all** 14 exit codes |
| `cmd/wpcli/internal/k8s/executor_test.go` | print mode, kubectl absent, command assembly, exit-code passthrough, SIGINT passthrough |

### 4.3 Local E2E tests (`tests/docker/wpcli/`)

**Location**: `tests/docker/wpcli/`, **strictly mirrors the existing `tests/docker/monitor/` directory structure**. This is the **end-to-end test suite** for the `wp` CLI, aligned with a real multi-node cluster environment (a docker-compose-launched woodpecker server cluster + etcd + MinIO), needing no kubernetes and no kind.

**Why docker-compose instead of kind**:
- `tests/docker/monitor/` has already established the docker-compose pattern; wpcli reuses it directly to reduce duplication
- docker-compose starts quickly on developer machines (within 30s); kind is 2-3x slower
- The `wp` CLI does not depend on any k8s-specific capability except family F's `-x` execute mode
- Family F's `-x` mode can be verified in a docker-compose environment using a fake kubectl shim
- The real Operator + `wp` integration tests (Phase 3 family F e2e) are run manually with kind, not part of this automation

**Directory structure (strictly aligned with monitor)**:

```
tests/docker/wpcli/
├── README.md                       # usage + "Local E2E, not CI"
├── docker-compose.wpcli.yaml       # corresponds to monitor/docker-compose.monitor.yaml
│                                   #   etcd + minio + 3 woodpecker servers
│                                   #   plus a "slow backend" injection sidecar for stuck scenarios
├── wpcli_cluster.go                # corresponds to monitor/monitor_cluster.go
│                                   #   start cluster / stop cluster / wait ready / inject faults
├── wpcli_test.go                   # corresponds to monitor/monitor_test.go
│                                   #   main entry + subtest dispatch
├── run_wpcli_tests.sh              # corresponds to monitor/run_monitor_tests.sh
│                                   #   up → test → down one-shot
├── prometheus.yml                  # corresponds to monitor/prometheus.yml (for metrics scenarios)
├── grafana/                        # corresponds to monitor/grafana/ (optional, live observation)
│   └── ...
└── scenarios/                      # corresponds to patterns like monitor/rolling_restart/
    ├── node_decommission/          # family A: decommission --wait + cancel
    │   └── decommission_test.go
    ├── ops_eviction_signal/        # family G: construct stuck op to verify evicted_old signal
    │   └── eviction_signal_test.go
    ├── logstore_force_flush/       # family C: force-flush verifies BufferBytes goes to zero
    │   └── force_flush_test.go
    ├── metrics_stuck_flush/        # family D: scenario report yields a red finding under slow backend
    │   └── stuck_flush_test.go
    ├── k8s_hybrid/                 # family F: fake kubectl shim verifies command assembly + exec passthrough
    │   └── hybrid_test.go
    └── cluster_health/             # family B: cluster health changes color after a node is killed
        └── health_test.go
```

**Key alignment points**:
- **File naming**: `wpcli_cluster.go` / `wpcli_test.go` / `docker-compose.wpcli.yaml` / `run_wpcli_tests.sh` strictly correspond to monitor's matching files
- **Layering**: top-level is orchestration (compose + cluster helper + main entry); subdirectories under `scenarios/` are independent scenarios
- **Entry point**: `./run_wpcli_tests.sh` is the only run entry, same as monitor

**How to run tests**:
```bash
# Run everything one-shot
cd tests/docker/wpcli/
./run_wpcli_tests.sh

# Or control manually
docker compose -f docker-compose.wpcli.yaml up -d
go test ./... -tags=docker  # (build tag aligned with monitor)
docker compose -f docker-compose.wpcli.yaml down -v
```

**Key scenarios that must be verified** (by command family + scenario directory):

| Family | scenario directory | Must-test |
|---|---|---|
| **A** | `node_decommission/` | `node list` sees the whole cluster; `node decommission --wait` actually waits until `safe_to_terminate=true` + heartbeat outputs a line every 15s; `cancel-decommission` actually flips state back to active mid-decommission |
| **B** | `cluster_health/` | `cluster info` outputs correct numbers across 3 nodes; `cluster health` turns yellow / red after killing one container; `gossip-diff` reports inconsistent under simulated partition |
| **C** | `logstore_force_flush/` | `logstore segments` shows active segments under continuous writes; `segment-show` fields are non-empty; `buffer_bytes` goes to zero after `force-flush`; `fence` + `compact` state transitions are correct |
| **D** | `metrics_stuck_flush/` | `metrics snapshot` returns non-empty response; `metrics top --hot-flushers` returns top nodes under slow backend; `metrics report --scenario stuck-flush --window 30s` produces a critical finding + exit 9 |
| **E** | `node_decommission/` or standalone | `config show` returns full config; `env show` returns build info; `logging set/get-level` round-trip; `profile --type cpu --seconds 5` downloads a valid pprof |
| **F** | `k8s_hybrid/` | with a fake kubectl shim (a bash script pretending to be kubectl, just echoing call args) verify: print mode emits correct commands, `-x` mode actually invokes the shim, shim exit code passes through, `logs -f` SIGINT passes through |
| **G** | `ops_eviction_signal/` | `ops list` shows in-flight ops under continuous writes; **construct a stuck flush** (slow backend sidecar) so that `ops list --longer-than 30s` is non-empty, `ops stats` shows `evicted_old > 0`, `ops show --op-id` returns full fields |

**Special emphasis on family G's eviction-signal verification**: family G's core value proposition is "eviction is signal"; if the e2e tests can't verify this, the entire opregistry design has not been end-to-end proven. This scenario is mandatory and cannot be skipped.

### 4.4 Phase 3 K8s / Operator E2E (manual)

Family F's `-x` execute mode and the Operator integration (e.g. `wp k8s scale -x` actually causes the Operator to reconcile) are **not part of automated E2E**, but instead serve as **manual verification items before Phase 3 release**:

- Use `kind` to bring up a local k8s cluster
- Install Woodpecker Operator + create a WoodpeckerCluster CR
- Run manually:
  - `wp --kube-context kind-wpcli k8s status -x`
  - `wp --kube-context kind-wpcli k8s scale --replicas 5 -x` + observe Operator reconcile
  - `wp --kube-context kind-wpcli k8s logs <pod> -f -x` actually tails
- Record results in Phase 3 release notes

**Why not automated**: kind + Operator is slow to start up (2-3 minutes) and demanding on CI runner memory; runs infrequently (once before Phase 3 release); needs to rerun on every Operator upgrade. Manual verification has higher ROI.

If automation is desired in the future, reserve the path `tests/k8s/wpcli/` (peer to `tests/docker/wpcli/`), using kind-based scripts. **But this WIP spec explicitly does not commit to that path**.

### 4.5 Test coverage targets

| Module | Target |
|---|---|
| `common/runtime/opregistry/` | ≥ 90% |
| `common/metrics/op.go` + `op_observer.go` | ≥ 90% |
| `common/http/management/` new handlers | ≥ 85% |
| `server/lifecycle.go` new methods | ≥ 90% |
| `cmd/wpcli/cmd/` | ≥ 70% |
| `cmd/wpcli/output/` | ≥ 80% (golden tests) |
| `cmd/wpcli/client/` | ≥ 85% |
| `cmd/wpcli/config/` | ≥ 85% |
| `cmd/wpcli/internal/k8s/` | ≥ 75% |
| **CLI overall** | ≥ 75% |

### 4.6 Key testing techniques

- **Time mocking**: use `github.com/jonboulle/clockwork v0.2.2`; opregistry / `--watch` / `--longer-than` all go through mock clock
- **Output capture**: inject `io.Writer` instead of `os.Stdout`, easing unit testing
- **Golden files**: `output/testdata/golden/*.golden`, regenerate via `-update` flag
- **Concurrency tests**: `go test -race` is mandatory; opregistry / fan-out / observer chain are the focus

---

## 5. Phased Delivery Plan

### 5.1 Phasing principles

1. **Each phase ships end-to-end**: even if only Phase 1 is merged, the wp binary runs and is useful
2. **Start with ABI-stable parts**: Phase 1 lands the "won't churn again" infrastructure first
3. **Read-only before write**
4. **Op Registry as its own phase**: an independent phase keeps it focused for review
5. **K8s hybrid last**: core operations don't depend on it

### 5.2 Phase dependency graph

```
                       ┌──────────────────────┐
                       │  Phase 1 - Foundation │
                       │  (CLI skeleton + node basics) │
                       └──────────┬───────────┘
                                  │
                                  ▼
                       ┌──────────────────────┐
                       │  Phase 2 - Observability│
                       │  + Op Registry        │
                       └──────────┬───────────┘
                                  │
                                  ▼
                       ┌──────────────────────┐
                       │  Phase 3 - K8s + Polish│
                       └──────────────────────┘
```

### 5.3 Phase 1 — Foundation & Daily Ops

**Scope (commands)**:
- `wp ctx use / list / view`
- `wp version`
- Family A complete: `node list/show/decommission/drain-status/cancel-decommission/restart`
- Family B complete: `cluster info/health/gossip-diff`
- E.1 / E.2: `config show/diff`
- E.6 / E.7: `env show/diff`
- E.5: `profile`

**Phase 1 command total: 14 feature commands** + 4 CLI base commands (`wp ctx use/list/view`, `wp version`) = 18 cobra subcommands total

**Server-side changes**:
1. `/admin/memberlist` adds JSON mode
2. `/admin/node/status` field fill-in
3. `POST /admin/node/decommission/cancel` new endpoint
4. `NodeLifecycleManager.CancelDecommission()` strict version
5. `GET /admin/config` new endpoint
6. `GET /admin/env` new endpoint
7. Linker ldflags injection of build info

**CLI deliverables**:
- Complete `cmd/wpcli/` skeleton (cobra + client + output + config + errors)
- Makefile `make wpcli` target
- 14 subcommand implementations + unit tests
- Each command in families A/B/E with at least happy + 1 error test
- output renderer golden file tests
- Exit-code table-driven full coverage

**Acceptance criteria**:
- [ ] `make wpcli` produces the `bin/wp` static binary
- [ ] `wp --version` shows build info
- [ ] `wp ctx list / use / view` round-trip
- [ ] `wp node list / show / decommission --wait / drain-status -w / cancel-decommission` works on docker-compose
- [ ] `wp cluster info / health / gossip-diff` same
- [ ] `wp config show / diff` same
- [ ] `wp env show / diff` same
- [ ] `wp profile node-2 --type cpu --seconds 5` pulls down a valid pprof file
- [ ] Unit tests pass with coverage ≥ 75%
- [ ] All Phase 1 admin endpoint changes have handler test coverage (3 brand-new + 2 existing enhancements)

### 5.4 Phase 2 — Observability & Op Registry

**Scope (commands)**:
- Family C complete (7): `logstore segments/segment-show/buffer/flush-queue/force-flush/fence/compact` (dump merged into `segment-show --full`)
- Family D complete: `metrics list/snapshot/top/watch/report`
- E.3 / E.4: `logging get-level/set-level`
- Family G complete: `ops list/show/stats`
- Local E2E test `tests/docker/wpcli/` complete setup (mirrors `tests/docker/monitor/`)

**Phase 2 command total: 17** (cumulative 31)

**Server-side changes**:

a) **New common packages**:
- `common/runtime/opregistry/` ~500 lines
- `common/metrics/op.go` + `op_observer.go` ~200 lines

b) **Admin endpoint changes (10, of which 8 brand-new + 2 stub fill-ins)**:
- Family C 5 brand-new: `logstore/segments`, `logstore/segments/{log}/{seg}`, `logstore/flush`, `logstore/fence`, `logstore/compact`
- Family G 3 brand-new: `runtime/ops`, `runtime/ops/{id}`, `runtime/ops/stats`
- Family E 2 stub fill-ins (paths long-existing but body is TODO stub): `/log/level` GET + POST

c) **Business-code changes** (intrusion):
- `storage.Writer` interface adds `Snapshot()` / `SnapshotDetailed()`
- The two backends (objectstorage / stagedstorage) implement them
- `WriterRegistry` abstraction
- ~12 op-shaped histogram call-site mechanical migrations
- Logger AtomicLevel-ization
- gRPC interceptor injection
- `cmd/main.go` instantiates opregistry + RegisterOpObserver + admin callbacks
- Mockery regeneration

**Phase 2 total intrusion is ~520 lines of diff** distributed across ~10 existing files (see §6.3 for detailed breakdown), of which ~150 lines is mechanical migration of ~12 op-shaped histogram call sites. Each migration has a regression test.

**CLI deliverables**:
- 17 new cobra subcommands
- YAML scenario engine + 12 built-in scenario files
- Cross-command hint framework
- `tests/docker/wpcli/` complete setup

**Acceptance criteria**:
- [ ] All 17 new commands verified end-to-end via docker-compose
- [ ] **Critical acceptance**: construct a stuck flush scenario on docker-compose, run:
  - [ ] `wp ops list --longer-than 30s` can see that flush op
  - [ ] `wp ops stats` shows `evicted_old > 0`
  - [ ] `wp metrics report --scenario stuck-flush --window 1m` produces a critical finding + exit 9
- [ ] Business-side metric migration regression tests all pass
- [ ] opregistry unit-test coverage ≥ 90%
- [ ] gRPC interceptor correctly registered + deregistered on all RPCs (concurrent test)
- [ ] Existing Woodpecker main-repo tests **do not** regress due to metric migration

### 5.5 Phase 3 — K8s Integration & Polish

**Scope (commands)**:
- Family F complete: `k8s status/scale/logs/doctor`
- Release artifacts: cross-platform binary builds, Docker image embedded with wp, GitHub Release automation
- Documentation: `cmd/wpcli/README.md`, quickstart, cookbook (10 common troubleshooting recipes), configuration reference

**Phase 3 command total: 4** (cumulative 35, full spec delivered)

**Server-side changes**: **zero**

**CLI deliverables**:
- Family F 4 commands + dual-mode (print / `-x` execute)
- `cmd/wpcli/internal/k8s/` executor wrapper
- Cross-platform build: `make wpcli-release` outputs binaries for 4 platforms
- Docker image integration: `build/Dockerfile` `COPY bin/wp /usr/local/bin/wp`
- GitHub Release workflow
- Documentation: `cmd/wpcli/README.md`, `docs/wpcli/quickstart.md`, `docs/wpcli/cookbook.md`, `docs/wpcli/configuration.md`

**Acceptance criteria**:
- [ ] Family F 4 commands work in `-x` mode on dev machines with kubectl + kind installed
- [ ] Family F 4 commands print correctly + exit 10 in environments without kubectl
- [ ] `make wpcli-release` produces binaries for 4 platforms
- [ ] `wp --version` works inside the server Docker image
- [ ] README + quickstart + cookbook are written
- [ ] Spec file finalized (WIP marker removed)

### 5.6 Risk register

| Risk | Level | Impact | Mitigation |
|---|---|---|---|
| Phase 2 business metric migration breaks existing metric behavior | 🔴 high | existing monitoring/alerts break | each migration has a regression test; verify against existing grafana dashboards |
| gRPC interceptor introduces performance regression | 🟡 medium | append latency rises | benchmark `BenchmarkAppend` before/after migration; observer-chain hot path uses RWMutex optimization |
| `WriterRegistry` abstraction is not generic; the two backend implementations diverge | 🟡 medium | family C endpoint fields inconsistent across backends | during interface design, draw the snapshot field tables separately on the two writers, take the intersection for the interface |
| `tests/docker/wpcli/` won't run on ARM dev machines | 🟡 medium | M-series mac developers can't verify locally | docker-compose uses multi-arch images |
| `clockwork v0.2.2` conflicts with Woodpecker's existing dependency versions | 🟢 low | compile failure | grep go.mod during implementation to confirm; on conflict fall back to a hand-rolled Clock interface |

---

## 6. Intrusion Summary (Reviewer Checklist)

This section is a **single-page checklist** for the reviewer — it consolidates all server-side changes scattered through earlier sections into one place so reviewers can close the loop on "exactly which existing files did we touch, what did we add, what dependencies did we introduce".

### 6.1 New packages

| Phase | Package path | Line estimate |
|---|---|---|
| 2 | `common/runtime/opregistry/` | ~500 |
| 1 | `common/version/` (if missing) | ~30 |
| 1 | `cmd/wpcli/` full set | ~3000 |
| 2 | `cmd/wpcli/internal/scenarios/` | ~400 |
| 2 | `cmd/wpcli/internal/hints/` | ~150 |
| 3 | `cmd/wpcli/internal/k8s/` | ~400 |
| 2 | `tests/docker/wpcli/` | ~500 |

### 6.2 New files (in existing packages)

| Phase | File | Purpose |
|---|---|---|
| 2 | `common/metrics/op.go` | `Op` type + `StartOp` / `End` API |
| 2 | `common/metrics/op_observer.go` | `OpObserver` interface + observer chain |
| 2 | `common/metrics/op_test.go` | unit test |
| 2 | `common/http/management/logstore_handler.go` | family C 5 endpoints |
| 2 | `common/http/management/logstore_handler_test.go` | unit test |
| 2 | `common/http/management/ops_handler.go` | family G 3 endpoints |
| 2 | `common/http/management/ops_handler_test.go` | unit test |
| 1 | `common/http/management/config_handler.go` | family E GET /admin/config |
| 1 | `common/http/management/config_handler_test.go` | unit test |
| 1 | `common/http/management/env_handler.go` | family E GET /admin/env |
| 1 | `common/http/management/env_handler_test.go` | unit test |
| 2 | `common/http/management/logging_handler.go` | family E GET/POST /log/level |
| 2 | `common/http/management/logging_handler_test.go` | unit test |
| 2 | `server/storage/writer_registry.go` | `WriterRegistry` interface + wraps server-internal writer list |

### 6.3 Modified existing files (intrusion — focal point)

| Phase | File | Change | diff lines |
|---|---|---|---|
| 1 | `Makefile` | add `make wpcli` target + ldflags | ~10 |
| 1 | `cmd/main.go` | register cancel-decommission callback, config callback, env callback | ~30 |
| 2 | `cmd/main.go` | instantiate opregistry, `metrics.RegisterOpObserver`, inject ops admin callbacks, add interceptor to grpc.NewServer | ~30 |
| 1 | `common/config/configuration.go` | add `Runtime.OpRegistry.{Capacity, WarnAge}` config items | ~30 |
| 1 | `common/http/router.go` | add path constants: `AdminNodeDecommissionCancelPath`, `AdminConfigPath`, `AdminEnvPath` | ~10 |
| 2 | `common/http/router.go` | add path constants: 5 logstore + 3 ops + 2 log level | ~25 |
| 1 | `common/http/server.go` | `AdminCallbacks` struct adds fields: `CancelDecommission`, `Config`, `Env`; handler registration | ~40 |
| 2 | `common/http/server.go` | `AdminCallbacks` adds `Logstore`, `Ops`, `LogLevel` substructs; handler registration | ~50 |
| 1 | `common/http/server.go` | `/admin/memberlist` adds JSON mode | ~20 |
| 1 | `common/http/management/node_handler.go` | add `NewNodeCancelDecommissionHandler` | ~25 |
| 1 | `common/http/management/node_handler_test.go` | add cancel-decommission test cases | ~50 |
| 2 | `common/logger/logger.go` | replace static level with `zap.AtomicLevel` + expose `GetLevel()` / `SetLevel()` | ~30 |
| 1 | `server/lifecycle.go` | add strict `CancelDecommission()` method | ~30 |
| 1 | `server/lifecycle_test.go` | add `TestCancelDecommission_*` cases | ~80 |
| 1 | `server/service.go` | implement `CancelDecommission()` service method | ~20 |
| 2 | `server/logstore.go` | metric migration on all 9 methods | ~100 |
| 2 | `server/storage/objectstorage/writer_impl.go` | implement `Snapshot()` / `SnapshotDetailed()`; flush-task entry migration | ~80 |
| 2 | `server/storage/stagedstorage/writer_impl.go` | same as above | ~80 |
| 2 | `server/storage/disk/writer_impl.go` | single flush-metric migration at existing line 617 | ~10 |
| 2 | `server/storage/storage.go` | `Writer` interface adds `Snapshot()` / `SnapshotDetailed()` methods | ~10 |
| 2 | `server/storage/{objectstorage,stagedstorage,disk}/...` | wrap compact / recovery / fence entries with `metrics.StartOp` | ~50 |
| 2 | `common/metrics/service_metrics.go` | add `WpFileRecoveryLatency` / `WpFileFenceLatency` (if missing) | ~30 |

**Intrusion subtotals**:
- Phase 1: ~270 lines of diff across ~8 existing files
- Phase 2: ~520 lines of diff across ~10 existing files
- Phase 3: ~0 lines of diff (family F is purely additive)

**Highest intrusion hotspot**: `server/logstore.go` (Phase 2 ~100 lines diff), because it has 9 metric migration sites.

### 6.4 Configuration item changes

| Phase | Config path | Type | Default | Purpose |
|---|---|---|---|---|
| 2 | `woodpecker.runtime.op_registry.capacity` | int | 1024 | opregistry capacity |
| 2 | `woodpecker.runtime.op_registry.warn_age` | duration | 30s | young/old eviction boundary |

**Only 2 new config items**. Both have reasonable defaults; users are not required to set them in yaml.

### 6.5 New dependencies

| Phase | Module | Version | Purpose |
|---|---|---|---|
| 2 | `github.com/jonboulle/clockwork` | `v0.2.2` | Time mocking (for opregistry tests) |
| 1 | `github.com/spf13/cobra` | latest stable | CLI command framework |
| 1 | `github.com/spf13/pflag` | latest stable | cobra transitive dependency |
| 1 | `gopkg.in/yaml.v3` | latest stable | cli.yaml parsing (should already exist) |
| 1 | `github.com/prometheus/common/expfmt` | latest stable | metrics text format parsing (should already exist) |

Confirm during implementation by grepping `go.mod`.

### 6.6 Mockery regeneration list

**Phase 2**:
- `metrics.OpObserver` (new interface)
- `opregistry.Registry` (if interfaced)
- `storage.Writer` interface adds methods → mock regeneration
- `storage.WriterRegistry` (new interface)
- `cmd/wpcli/client.AdminClient`
- `cmd/wpcli/output.Renderer`

**Phase 3**:
- `cmd/wpcli/internal/k8s.Executor`

### 6.7 Documentation changes

| Phase | File | Change |
|---|---|---|
| 1 | `cmd/wpcli/README.md` | new (quickstart) |
| 1 | `docs/wpcli/getting-started.md` | new |
| 2 | `cmd/wpcli/README.md` | append Phase 2 command-family description |
| 2 | `docs/wpcli/observability.md` | new (explains ops command family, scenario report, stuck-op troubleshooting) |
| 3 | `cmd/wpcli/README.md` | append Phase 3 + k8s integration description |
| 3 | `docs/wpcli/cookbook.md` | new (10 common troubleshooting recipes) |
| 3 | `docs/wpcli/configuration.md` | new (cli.yaml full reference + context model) |
| 3 | `README.md` (repo root) | append wpcli intro paragraph |

### 6.8 Build / Distribution changes

| Phase | Change |
|---|---|
| 1 | `Makefile` adds `make wpcli` target, outputs `bin/wp` |
| 1 | `Makefile` adds ldflags injection of build info |
| 3 | `Makefile` adds `make wpcli-release` target, outputs binaries for 4 platforms |
| 3 | `build/Dockerfile` modified: `COPY bin/wp /usr/local/bin/wp` into the server image |
| 3 | GitHub Release workflow triggers wpcli binary upload |

### 6.9 Reviewer one-page checklist (condensed)

| Category | Phase 1 | Phase 2 | Phase 3 | Total |
|---|---|---|---|---|
| **New packages** (cmd/wpcli/* counted as 1 top-level) | 1 + common/version | 1 (opregistry) | 1 (k8s subpackage) | 3-4 top-level |
| **New files** | 4 handlers + CLI skeleton | 8 handlers/op chain | docs + Dockerfile mod | ~15 |
| **Existing-file diff** | ~270 lines in 8 files | ~520 lines in 10 files | 0 lines | ~790 lines |
| **Brand-new admin endpoints** | 3 (cancel/config/env) | 8 (5 logstore + 3 ops) | 0 | **11** |
| **Existing endpoint enhancements or stub fill-ins** | 2 (memberlist JSON, node/status fields) | 2 (/log/level GET+POST fill-ins) | 0 | 4 |
| **Business-code metric migration** | 0 | ~12 sites / ~150 lines | 0 | ~12 sites |
| **New config items** | 0 | 2 | 0 | 2 |
| **New dependencies** | cobra, pflag | clockwork v0.2.2 | 0 | 3 |
| **Newly mocked** | 0 | 6 | 1 | 7 |
| **Proto file changes** | 0 | 0 | 0 | **0** ✓ |
| **Data migration** | 0 | 0 | 0 | **0** ✓ |
| **Backward-incompatibility** | 0 | 0 | 0 | **0** ✓ |

---

## 7. Non-goals

To prevent reviewers from asking "why isn't this in the spec?", explicitly list what this spec does **not** do:

- ❌ Does not modify any client SDK (`woodpecker/` package)
- ❌ Does not modify any metadata-layer code (`meta/`)
- ❌ Does not modify etcd / minio / gossip or any dependency
- ❌ Does not modify existing Operator code (`deployments/operator/`)
- ❌ Does not modify any proto file (`proto/*.proto`)
- ❌ Does not introduce any new data persistence (besides the existing `node_state.json`)
- ❌ Does not introduce any new gRPC method
- ❌ Does not introduce any new etcd key prefix
- ❌ Does not modify the name / label set / type of any existing metric (only **adds**, never **changes**)
- ❌ Does not modify the URL or HTTP method of any existing admin endpoint (only **adds**, never **changes**)
- ❌ Does not introduce any authentication / authorization / TLS code
- ❌ Does not embed any external tool's query language (PromQL / JaegerQL)
- ❌ Does not expose any new interface to the client SDK
- ❌ Does not implement real diagnostic logic for `wp k8s doctor` (stays a stub in Phase 3)
- ❌ Does not support modification of node metadata (labels / tags read-only)
- ❌ Does not commit to multi-version CLI compatibility (from Phase 3 onward, follows the server's main version)

**Key guarantee**: the 11 items above are hard constraints on "existing users' migration cost" — any user upgrading to a wp-equipped server version **does not need** to change any etcd / minio / dashboard / monitoring configuration; server behavior is 100% backward-compatible.

---

## Appendix A: References

- Related issue: [zilliztech/woodpecker#128](https://github.com/zilliztech/woodpecker/issues/128)
- Existing admin handlers: `common/http/management/`
- Existing server lifecycle: `server/lifecycle.go`
- Existing docker E2E test pattern: `tests/docker/monitor/`
- Woodpecker Operator documentation: `docs/woodpecker_operator.md`
- Existing metrics definitions: `common/metrics/{client_metrics,service_metrics}.go`

## Appendix B: Glossary

| Term | Meaning |
|---|---|
| **wp** | Binary name of this CLI |
| **wpcli** | Go package name of this CLI (`cmd/wpcli/`) |
| **admin port** | The Woodpecker server's HTTP admin/metrics port (default 9091) |
| **fan-out** | The mode in which one CLI command concurrently calls all nodes in the cluster |
| **op** | An operation that "has started but has not yet finished" (flush task, compact, gRPC call, etc.) |
| **op registry** | The in-process in-flight op registry |
| **eviction signal** | When the op registry is full and the oldest op is evicted, classify as young / old based on whether its age exceeds warn_age; old is the stall signal |
| **warn age** | The time threshold separating young/old in eviction (default 30s) |
| **scenario** | A predefined diagnostic scenario for D.5 metrics report (defined in YAML) |
| **WriterRegistry** | The server-internal index of active writers; data source for family C endpoints |
| **OpObserver** | The interface for metrics op lifecycle hooks; opregistry implements it |
| **hybrid execute** | The dual-mode design of family F K8s commands: print kubectl command by default, execute when `-x` |
