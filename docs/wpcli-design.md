# Woodpecker CLI (`wp`) — Design Document

| 字段 | 内容 |
|---|---|
| **状态** | 🚧 WIP — 设计稿，未开始实现 |
| **分支** | `introduce_wp_cli` |
| **关联 issue** | [zilliztech/woodpecker#128](https://github.com/zilliztech/woodpecker/issues/128) |
| **最后更新** | 2026-04-09 |
| **作者** | (待填) |
| **Reviewers** | (待填) |

## 摘要

`wp` 是一个**独立的 Go 二进制 CLI 工具**，服务于 Woodpecker service mode 集群的日常运维。它的定位是 **替代 SRE 在排查 / 干预集群时反复切换 `kubectl exec` + `curl /admin` + grep 的工具组合**，把这些动作收敛到一个零依赖（除 woodpecker server admin 端口外）的命令行工具。

**它最不寻常的设计决定**是：**完全不做外部工具的轮子**。它不包 Jaeger、不包 Prometheus PromQL、不包 kubectl（除了 Phase 3 的 hybrid execute 模式作为可选便利）。它的独特价值在于暴露**只有 server 进程自己能看到**的运行时数据 —— in-flight operation 列表、in-memory writer buffer、flush queue 深度、segment runtime 状态。

工具将分 **3 个 Phase 交付**，最终交付 **35 个 feature 子命令**（7 个命令族）+ **11 个全新 admin endpoint**（外加 4 个对现有 endpoint 的增强或补实）+ **1 个新通用包**（`common/runtime/opregistry/`），不引入任何 proto 改动、数据迁移或向后不兼容。

---

## 目录

- [1. 架构总览](#1-架构总览)
- [2. 命令族分册](#2-命令族分册)
  - [2.A Node 生命周期](#2a-node-生命周期)
  - [2.B Cluster 总览](#2b-cluster-总览)
  - [2.C Logstore 运行时](#2c-logstore-运行时)
  - [2.D Metrics 局部分析](#2d-metrics-局部分析)
  - [2.E 配置 / 日志级别 / Env / pprof](#2e-配置--日志级别--env--pprof)
  - [2.F K8s Hybrid](#2f-k8s-hybrid)
  - [2.G In-flight Op 注册表](#2g-in-flight-op-注册表)
- [3. 横切关注点](#3-横切关注点)
- [4. 测试策略](#4-测试策略)
- [5. 分期落地计划](#5-分期落地计划)
- [6. 侵入项总览（Reviewer Checklist）](#6-侵入项总览reviewer-checklist)
- [7. 非目标](#7-非目标)

---

## 1. 架构总览

### 1.1 定位

`wp` 是一个独立的运维 CLI 二进制，服务于 Woodpecker service mode 集群的日常运维。它的边界由三条规则定义：

1. **只和 Woodpecker Server 节点交互**，不触碰嵌入式 SDK（client）进程
2. **不复刻外部工具生态**（Jaeger UI / Prometheus PromQL / kubectl），只暴露进程内外部工具看不到的运行时数据
3. **读取用 `GET`，干预用 `POST`**，所有动作走 server 已有或新增的 `/admin/*` HTTP endpoint；跨节点由 CLI 侧扇出

### 1.2 与现有组件的关系

```
┌──────────────────────────────────────────────────────────────────────┐
│  运维人员                                                              │
│     │                                                                  │
│     │  wp <command>                                                    │
│     ▼                                                                  │
│  ┌──────────┐    ~/.woodpecker/cli.yaml (contexts, default endpoint)  │
│  │  wp CLI  │<── $WOODPECKER_ENDPOINT / $WOODPECKER_CONTEXT            │
│  └────┬─────┘    --endpoint, --context flags                           │
│       │                                                                │
│       │ 1) seed: GET /admin/memberlist                                 │
│       │ 2) fan-out: GET /admin/node/status (每节点)                    │
│       │ 3) action:  POST /admin/node/decommission 等                   │
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
│  不在本 CLI 范围内：                                                    │
│     • 嵌入式 SDK（woodpecker/ 包，跑在 Milvus 等宿主进程里）             │
│     • K8s Operator / WoodpeckerCluster CRD（用 kubectl 管）             │
│     • Jaeger/Tempo trace 后端查询（用各自 UI/CLI）                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 1.3 典型命令的数据流

以 `wp node list` 为例：

1. 解析 flags/env/config，确定 seed endpoint（如 `http://node-1.wp.svc:9091`）
2. `HTTP GET {seed}/admin/memberlist` → 拿到 N 个节点 `{id, addr, az, rg, state}`
3. 对每个节点并发 `HTTP GET http://{node-addr}:9091/admin/node/status`
   → 拿到 lifecycle state / decommission progress / 节点自报信息
4. CLI 侧合并结果、排序、按 `-o` 选项渲染为 table/json/yaml
5. 非 0 退出码当且仅当：seed 不可达 / memberlist 失败 / 被干预的节点全挂

**一个关键约定**：fan-out 允许部分失败。部分节点不可达时，CLI 在输出里以 `UNREACHABLE` 状态列出，exit code 仍为 0（除非 `--strict` 指定）。

### 1.4 Repo 布局

```
woodpecker/
├── cmd/
│   ├── main.go                 # 现有 server 入口（不动）
│   ├── external/               # 现有（不动）
│   └── wpcli/                  # ← 新增：整个 CLI 自成一片
│       ├── README.md
│       ├── main.go             # package main，cobra root 组装 + Execute()
│       ├── cmd/                # package cmd，cobra 子命令
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
│       ├── client/             # admin HTTP client、discovery、fan-out
│       │   ├── client.go
│       │   ├── discovery.go
│       │   └── fanout.go
│       ├── output/             # table / json / yaml 渲染
│       │   ├── table.go
│       │   ├── json.go
│       │   └── yaml.go
│       ├── config/             # CLI 自身配置 & contexts 解析
│       │   └── cli_config.go
│       └── internal/           # CLI-only 工具
│
├── common/http/management/     # 现有 admin handlers 目录
│   ├── node_handler.go         # 现有（追加 cancel-decommission）
│   ├── admin_handler.go        # 现有（空 TODO）
│   ├── logstore_handler.go     # ← 新增：C 区端点
│   ├── ops_handler.go          # ← 新增：G 区 op registry 端点
│   ├── config_handler.go       # ← 新增：E 区 config 端点
│   ├── env_handler.go          # ← 新增：E 区 env 端点
│   └── logging_handler.go      # ← 新增：E 区 log level 端点
│
├── common/runtime/opregistry/  # ← 新增：op 注册表（通用包）
│   ├── types.go
│   ├── registry.go
│   ├── eviction.go
│   ├── observer.go
│   ├── interceptor.go
│   ├── metrics.go
│   └── registry_test.go
│
├── common/metrics/
│   ├── op.go                   # ← 新增：StartOp/Op 抽象
│   ├── op_observer.go          # ← 新增：OpObserver chain
│   └── ...                     # 现有 metrics 文件
│
├── common/version/             # ← 新增（如缺）：build info
│   └── version.go
│
├── tests/docker/wpcli/         # ← 新增：本地 E2E 测试，严格 mirror tests/docker/monitor/
│   └── ...
│
└── Makefile                    # 加 `make wpcli` target
```

### 1.5 二进制分发

- `make wpcli` → 输出 `bin/wp`（静态链接，跨平台编译支持 linux/amd64、linux/arm64、darwin/arm64）
- Docker image：把 `wp` 附在 server image 的 `/usr/local/bin/wp`，方便 `kubectl exec pod -- wp ...` 排障
- Release 工件：`wp-<version>-<os>-<arch>.tar.gz`，塞进现有 GitHub Release workflow

### 1.6 非目标（明确）

| 非目标 | 原因 |
|---|---|
| 对嵌入式 SDK 进程的任何直连 | 用户决定 SDK 运行时信息通过 metrics 观察即可 |
| Jaeger/Tempo trace 查询封装 | 重复造轮子，外部工具已经很成熟 |
| K8s Operator CRD 直接管理（scale/upgrade/...） | `kubectl` + operator 已经覆盖；F 族提供 hybrid 兜底 |
| 修改配置文件（写入 ConfigMap / yaml）持久化 | 运行时改配置走 admin endpoint，持久化仍走 GitOps/ConfigMap |
| 认证 / 鉴权 | Admin 端口默认是集群内部网络；CLI 是管理员工具 |

---

## 2. 命令族分册

完整命令清单（35 个）：

| 族 | 子命令数 | 一句话定位 |
|---|---|---|
| **A** Node 生命周期 | 6 | 看节点 / 下线节点 / 取消下线 |
| **B** Cluster 总览 | 3 | 看集群健康 / 拓扑 / gossip 一致性 |
| **C** Logstore 运行时 | 7 | 看 server writer 状态 + 干预（force-flush/fence/compact） |
| **D** Metrics 局部分析 | 5 | 直接 scrape `/metrics`，本地窗口聚合 + 场景报告 |
| **E** Config / Logging / Env / pprof | 7 | 看配置、改日志级别、看环境、抓 pprof |
| **F** K8s Hybrid | 4 | 默认打印 kubectl 命令，`-x` 时执行 |
| **G** In-flight Op 注册表 | 3 | 看进程内正在跑的 op + trace_id |

### 2.A Node 生命周期

#### 2.A.1 通用前提

**节点寻址约定**：所有 A 族命令通过 `node-id` → `gossip advertise host` + 默认 admin port 9091 解析。集群内所有节点的 admin port 必须统一（不一致时用 `--admin-port` 或 context 配置覆盖）。

**Server 改动 #0**：现有 `GET /admin/memberlist` 返回纯文本（`GetMemberlistStatus() string`），需扩展支持 `Accept: application/json`，返回 `{members: [{id, gossip_addr, service_addr, az, rg, state, incarnation, last_seen, tags}]}`。

#### 2.A.2 子命令

##### A.1 `wp node list`

| 维度 | 内容 |
|---|---|
| **用途** | 列出集群所有 server 节点及其关键状态 |
| **Args** | 无 |
| **Flags** | `--az <az>` / `--rg <rg>` / `--state <state>` / `--strict` / 全局 |
| **输入** | `GET /admin/memberlist` + 并发 `GET /admin/node/status` + `GET /healthz` |
| **输出列** | `NAME ADDR STATE AZ RG HEALTH UPTIME`（`-o wide` 加 `VERSION LAST_SEEN DECOM_PROGRESS`） |
| **Endpoint** | ✅ 复用（依赖 #0） |

##### A.2 `wp node show <node>`

| 维度 | 内容 |
|---|---|
| **用途** | 单节点详细视图，sectioned 输出 |
| **Flags** | `--with-metrics`（额外抓 `/metrics`） |
| **输入** | `/admin/memberlist` 寻址 → `/admin/node/status` + `/healthz`（+ `/metrics` 可选 + decommission progress 可选） |
| **输出 sections** | Identity / Placement / Lifecycle / Health / Resources(if --metrics) |
| **Endpoint** | ✅ 复用 |

> Server 隐含小改：`GetNodeStatus()` 回值结构体可能要补 `started_at`、`version`、`last_health_check` 字段。

##### A.3 `wp node decommission <node>`

| 维度 | 内容 |
|---|---|
| **用途** | 发起优雅下线 |
| **Flags** | `--async`（默认阻塞）、`--timeout 30m`、`--interval 3s`、`--heartbeat-interval 15s`、`-y` |
| **输入** | `POST /admin/node/decommission`，阻塞模式下循环 `GET /admin/node/decommission/progress` |
| **行为** | 默认阻塞；progress 数值变化时打印；`--heartbeat-interval` 内无变化时打 heartbeat 行 + 提示 `wp ops list <node>` |
| **Endpoint** | ✅ 复用 |

##### A.4 `wp node drain-status <node>`

| 维度 | 内容 |
|---|---|
| **用途** | 观察下线进度（不发起动作） |
| **Flags** | `-w/--watch`、`--interval 3s`、`--timeout 30m` |
| **输入** | `GET /admin/node/decommission/progress` |
| **Endpoint** | ✅ 复用 |

##### A.5 `wp node cancel-decommission <node>` 🔨

| 维度 | 内容 |
|---|---|
| **用途** | 取消正在进行的下线，恢复为 active |
| **Flags** | `-y` |
| **输入** | `POST /admin/node/decommission/cancel`（**新增**） |
| **状态约束** | **严格**：只允许 `decommissioning → active`；`active`（幂等返回）；`decommissioned` 报错 |

**新增 Server endpoint**：`POST /admin/node/decommission/cancel`
- `NodeLifecycleManager.CancelDecommission()` 新方法（`server/lifecycle.go`）
- handler 在 `common/http/management/node_handler.go`
- 路径常量在 `common/http/router.go`：`AdminNodeDecommissionCancelPath`
- `AdminCallbacks` 加 `CancelDecommission func() error` 字段
- 取消后**不**恢复已经被 upload 到 object storage 的数据 —— 仅恢复"接受新写入"状态

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

退出码 `10`。约 20 行 cobra 实现。

#### 2.A.3 A 族对 server 侧改动汇总

| 改动 | Phase |
|---|---|
| `/admin/memberlist` 支持 JSON 格式 | 1 |
| `/admin/node/status` 字段补齐 | 1 |
| `POST /admin/node/decommission/cancel` 新端点 | 1 |
| `NodeLifecycleManager.CancelDecommission()` 严格版 | 1 |

**1 个新 endpoint + 3 处现有增强**。

### 2.B Cluster 总览

B 族**纯聚合**，不做任何干预，**零新增 endpoint**。

#### B.1 `wp cluster info`

合并了原本独立的 `cluster topology` 命令。

| Flag | 默认 | 含义 |
|---|---|---|
| `-o table` | 默认 | sectioned Overview + Topology 树（合并视图） |
| `-o tree` | | 仅 topology 树 |
| `-o json/yaml` | | 完整结构化数据 |

（想要一行一个节点的平铺视图请用 `wp node list`。）

**输出示例**（默认）：
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

**副本数感知的红黄绿体检**。

副本数来源优先级：
1. **首选**：抓 `/metrics`，读 per-log ensemble size / write quorum（实现期对齐 metric 名字）
2. **Fallback**：默认 N=3
3. **Override**：`--expected-replicas N` 或 `--per-log-expected log=n,...`

**评级规则**：
- 🟢 **Green**：`active_nodes_total >= N` **且** 每个 AZ 都有 ≥ `ceil(N/2)` 个 active 节点 **且** 所有节点 reachable **且** `/healthz` 全 OK **且** 版本一致
- 🟡 **Yellow**：active nodes ≥ N 但失去一个就跌破阈值；或存在 decommissioning 节点；或 rolling upgrade（版本不一致）；或某 RG 节点数 == N（零冗余）；或某 log under-replicated 但仍 ≥ majority
- 🔴 **Red**：`active_nodes_total < N`（quorum 跌破）；或有 UNREACHABLE；或 `/healthz` FAIL；或某 AZ 原本有节点现在变成 0；或有 log 掉到 `< Wq`

**输出包含 Log Replica Health 段**（metrics 可用时）：
```
Log Replica Health (default expected ≥3)
  ✅ log-catalog        expected=5  active=5/5
  ✅ log-write-ahead    expected=3  active=3/3
  🔴 log-metrics        expected=3  active=2/3   (node-4 unreachable)
```

#### B.3 `wp cluster gossip-diff`

跨节点对比 memberlist 视图找分脑。

输出包含两段：
- **Membership Set Agreement**：每个节点看到的成员集合
- **Per-Member State Divergence**：同一成员在不同节点视角下的 `incarnation / state / last_seen` 差异（`last_seen` 偏差阈值默认 30s，可用 `--last-seen-tolerance` 调整）

判定为 inconsistent 的额外条件（除成员集合不一致外）：
- 同一成员在不同 group 视角下 `incarnation` 差异 ≥ 2
- 同一成员一个 group 看 `alive`，另一个看 `suspect/dead`

#### B 族 Server 改动

**零**。完全建立在 A 族的 JSON memberlist + `/admin/node/status` + `/metrics` 上。

### 2.C Logstore 运行时

C 族是**新增 endpoint 最多的一族**。它的定位是"进程内数据视图 + 干预操作"，对应 issue 里的 "system debug buffer / queue / dump"。

**和 G 族的边界**：
- C 族答"当前 writer/segment/buffer/queue 的**状态**是什么样"（名词、数据结构快照）
- G 族答"当前有哪些 **operation** 正在跑、卡在哪一步、跑了多久"（动词、in-flight 活体）

#### 2.C.1 通用前提：Active Writer Registry

C 族的所有新 endpoint 需要 server 先暴露一个 `WriterRegistry` 抽象（`server/storage/writer_registry.go`，新文件）：

```go
type WriterRegistry interface {
    List() []WriterSnapshot
    Get(logId, segmentId int64) *WriterSnapshot
    ForceFlush(ctx context.Context, logId, segmentId int64) error
    Fence(ctx context.Context, logId, segmentId int64, reason string) error
    Compact(ctx context.Context, logId, segmentId int64) error
}
```

`storage.Writer` interface 加 `Snapshot()` 和 `SnapshotDetailed()` 方法，两个 backend（`objectstorage`、`stagedstorage`）各自实现。

#### 2.C.2 子命令

**C 族共 7 个子命令**（原 inventory 里的 `logstore dump` 合并成 `segment-show --full -o json`，不独立成命令）：

| 子命令 | 类型 | 一句话 | 端点 |
|---|---|---|---|
| `logstore segments <node>` | 观察 | 列出 active segments | 🔨 `GET /admin/logstore/segments` |
| `logstore segment-show <node>` | 观察 | 单 segment 详细（`--full -o json` 等价 dump） | 🔨 `GET /admin/logstore/segments/{log}/{seg}` |
| `logstore buffer <node>` | 观察 | 所有 writer buffer 汇总（C.1 数据本地聚合） | 复用 |
| `logstore flush-queue <node>` | 观察 | flush 队列深度汇总（C.1 数据本地聚合） | 复用 |
| `logstore force-flush <node>` | 干预 | 强制 sync | 🔨 `POST /admin/logstore/flush` |
| `logstore fence <node>` | ⚠️ 干预 | 强制 fence segment | 🔨 `POST /admin/logstore/fence` |
| `logstore compact <node>` | 干预 | 强制 compaction | 🔨 `POST /admin/logstore/compact` |

##### C.1 `wp logstore segments <node>`

**Endpoint**：`GET /admin/logstore/segments`，支持 `?log_id=&state=&writable=` 过滤

**返回 JSON**（每个 writer 一条）：
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

**Endpoint**：`GET /admin/logstore/segments/{log_id}/{seg_id}?detailed=true`（`--full` 触发 detailed）

`segment-show --full -o json` 等价于原 inventory 中的 `wp logstore dump` —— 共用同一个 endpoint，避免命令膨胀。

##### C.3 `wp logstore buffer <node>` / C.4 `wp logstore flush-queue <node>`

**复用 C.1 的 endpoint**，CLI 侧本地聚合 + top-N 排序。**不新增 endpoint**。

##### C.5 `wp logstore force-flush <node>`

**Endpoint**：`POST /admin/logstore/flush`，body `{"log_id": 42, "segment_id": 7}` 或 `{}`（全部）

##### C.6 `wp logstore fence <node>` ⚠️

**高危干预**。Endpoint：`POST /admin/logstore/fence`，body `{"log_id": ..., "segment_id": ..., "reason": "..."}`

`--reason` 是必填 flag，写入日志和 fence file 便于事后追溯。`-y` 跳过确认（在生产环境慎用）。

##### C.7 `wp logstore compact <node>`

**Endpoint**：`POST /admin/logstore/compact`。前置条件：segment 必须 finalized；server 侧校验，不满足返回 409。

#### 2.C.3 C 族 Server 改动汇总

| 改动 | 文件 |
|---|---|
| `GET /admin/logstore/segments` | `common/http/management/logstore_handler.go`（新） |
| `GET /admin/logstore/segments/{log}/{seg}` | 同上 |
| `POST /admin/logstore/flush` | 同上 |
| `POST /admin/logstore/fence` | 同上 |
| `POST /admin/logstore/compact` | 同上 |
| `WriterRegistry` 接口 + 实现 | `server/storage/writer_registry.go`（新） |
| `storage.Writer.Snapshot()` / `SnapshotDetailed()` | `server/storage/writer.go` + 两个 backend |
| 路径常量 | `common/http/router.go` |
| AdminCallbacks 扩展 | `common/http/server.go` |

**5 个新 endpoint**。

### 2.D Metrics 局部分析

#### 2.D.1 定位

`wp metrics` 是一个**故障现场临时分析工具**，它的生态位是 **"Prometheus 看不到或来不及看的事"**：
- **零延迟**：直接 scrape 节点 `/metrics`，没有 Prometheus 15-30s 的抓取间隔
- **零依赖**：新集群、demo 环境、隔离网络、没装 Prometheus 都能用
- **零 query 语言**：不学 PromQL，用场景名就行
- **window-of-now**：CLI 自己作为**临时采样器**，从**此刻**往后采集 N 秒/分钟

**明确边界**：
- D 族**从不查历史数据**
- D 族**不访问任何外部系统**（无 Prometheus 集成）
- D 族**不看 client-side metrics**

#### 2.D.2 子命令

| 子命令 | 一句话 | 模式 |
|---|---|---|
| `metrics list <node>` | 列出该节点暴露了哪些 metric series | 1 次 scrape |
| `metrics snapshot [<node>\|--all]` | 抓**此刻**的点位快照 | 1 次 scrape |
| `metrics top --by <metric>` | 跨节点按指标排 top-N | 1 次 scrape × N 节点 |
| `metrics watch <metric>` | streaming 实时显示 | 周期 scrape |
| `metrics report --scenario <name>` | 预置场景：窗口采集 + 联合分析 | 周期 scrape over window |

##### D.4 `wp metrics watch` —— streaming-only

**只保留 streaming 模式**。每 `--interval`（默认 1s）scrape 一次，逐行追加打印，趋势箭头标 `↑ ↓`。Ctrl+C 退出。

##### D.5 `wp metrics report --scenario <name>` —— 双模

支持两种入参：
- `--scenario <name>`：按预置规则做多指标联合分析
- `--metric <name>`：对单个指标做统计摘要（min/max/avg/p50/p99/stddev/trend）

**12 个内置 scenario**：

| Scenario | 侧面 | 检查 |
|---|---|---|
| `hot-write` | write | 跨节点 append 速率严重失衡 |
| `slow-write` | write | append p99 偏离基线 |
| `stuck-write` | write | append 几乎不动；client 重试在涨 |
| `slow-slot` | write | quorum 内某 ensemble slot 拖累整体 |
| `stuck-flush` | write | flush queue 高位 + p99 飙升 + errors 增长 |
| `slow-compact` | write | compact p99 偏离 + errors 增长 |
| `fencing` | write | 窗口内 fence 事件超阈值 |
| `slow-read` | read | read p99 偏离 + read queue 增长 |
| `stuck-read` | read | read throughput ≈ 0 但有 pending |
| `read-amplification` | read | 块读次数 / 条目读次数 比率异常 |
| `quorum-degraded` | 结构 | 有 log 活跃副本数 < ensemble |
| `under-replication` | 结构 | 窗口内持续 under-replicated |

**Scenario 用 YAML 驱动**，文件位于 `cmd/wpcli/internal/scenarios/*.yaml`。每个 scenario 声明 metrics、rules、severity、hint commands。

**核心机制**：CLI 在内存里作为临时 time-series 采集器，窗口内（默认 1m）持续 scrape 相关 metric，结束后应用 scenario rules 输出 finding + 推荐"下一步命令"。

**Scenario rule YAML 示例**：
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

#### 2.D.3 D 族 Server 改动

**零**。但有一条隐含依赖：D.5 scenario 用到的 metric series 必须存在且有合适 label。实现期做一轮 metric inventory，缺的 series 补齐。

### 2.E 配置 / 日志级别 / Env / pprof

#### 2.E.1 子命令（7 个）

| 子命令 | 一句话 | Phase |
|---|---|---|
| `config show <node>` | 打印当前生效配置 | 1 |
| `config diff` | 跨节点对比配置 | 1 |
| `env show <node>` | 打印 env vars + Go runtime + host + build | 1 |
| `env diff` | 跨节点对比 env | 1 |
| `logging get-level <node>` | 读当前日志级别 | 2 |
| `logging set-level <node>` | 改日志级别（无 auto-revert） | 2 |
| `profile <node>` | 抓 pprof | 1 |

#### 2.E.2 关键设计

**E.1 不脱敏**：`GET /admin/config` 直接返回完整 cfg。理由：CLI 是管理员工具。安全提示写进部署文档（admin 端口必须网络隔离）。

**E.4 不做 auto-revert**：`logging set-level` 是一次性改级别，没有任何 server 端 timer/state。需要"10 分钟后回退"的用户用 shell 一行：
```bash
wp logging set-level node-2 --level debug && sleep 600 && wp logging set-level node-2 --level info
```

**E.6/E.7 env 是独立子命令族**：和 config 不混淆。`/admin/env` 返回 4 个 section：
```json
{
  "env": { "HOSTNAME": "...", "WOODPECKER_NODE_NAME": "...", ... },
  "runtime": { "go_version": "go1.22.1", "gomaxprocs": 8, ... },
  "host": { "hostname": "...", "os": "linux", "kernel": "...", ... },
  "build": { "version": "v0.1.26", "commit": "2c88470", "build_time": "...", "go_version": "..." }
}
```

`env diff` 内置忽略集（`HOSTNAME / PWD / SHLVL / KUBERNETES_*_PORT_*` 等噪声 key），可用 `--ignore <regex>` 追加。

**E.5 profile**：纯 CLI wrapper，零 server 改动，复用现有 `/debug/pprof/*`。

#### 2.E.3 E 族 Server 改动汇总

| 改动 | Phase |
|---|---|
| `GET /admin/config` 新端点 | 1 |
| `GET /admin/env` 新端点 | 1 |
| `GET /log/level` + `POST /log/level` 补实（用 zap AtomicLevel） | 2 |
| `common/logger/logger.go` 加 `GetLevel()` / `SetLevel()` | 2 |
| Linker ldflags 注入 build info | 1 |

**2 个新 endpoint + 2 个补实 stub**。

### 2.F K8s Hybrid

#### 2.F.1 设计核心：Print + Execute 双模

**默认模式（print-only）**：打印推荐的 kubectl 命令和建议，不执行。零副作用、零依赖。

**Execute 模式（`-x` / `--execute`）**：用 `--namespace / --wp-cluster / --kube-context / --kubeconfig` 拼出 kubectl 命令并 shell-out 执行；stdout/stderr/exit code/signal 透传。

**kubectl 缺席自动降级**：如果 `-x` 但 `$PATH` 找不到 kubectl（也没 `--kubectl <path>` 覆盖），降级为 print 模式 + 警告。

#### 2.F.2 共享 k8s flags

| Flag | 默认 | 说明 |
|---|---|---|
| `-x / --execute` | false | 触发执行 |
| `--kubectl <path>` | `$(which kubectl)` | kubectl 二进制路径 |
| `-n / --namespace <ns>` | 来自 cli.yaml 或 kubeconfig | namespace |
| `--wp-cluster <name>` | 来自 cli.yaml | WoodpeckerCluster CR 名字 |
| `--kube-context <ctx>` | kubeconfig current | kubectl context（不和 wp `--context` 冲突） |
| `--kubeconfig <path>` | `$KUBECONFIG` 或 `~/.kube/config` | 透传 |

#### 2.F.3 子命令（4 个）

| 子命令 | 默认模式 | Execute 模式 |
|---|---|---|
| `k8s status` | 打印 `kubectl get woodpeckercluster + describe + get pods` 命令 + 建议 | 真执行并打印结果 |
| `k8s scale --replicas N` | 打印 `kubectl patch woodpeckercluster ...` | 真执行 |
| `k8s logs <node-or-pod>` | 打印 `kubectl logs ...` | 真执行（含 `-f` 透传）；node-id → pod name 用 `<cluster>-server-<ordinal>` 约定 |
| `k8s doctor` | 纯 stub（Phase 3 也仍是 stub，未来 release 再实现） | 不支持 `-x` |

#### 2.F.4 Server 改动

**零**。F 族纯 CLI 端 + shell-out。

### 2.G In-flight Op 注册表

#### 2.G.1 定位

**轻量级 stuck op 哨兵**。不是 in-process debugger 的替代品。SRE 用它知道"现在这个节点上有哪些 op 正在跑、有没有跑得过久的"。如果想知道 op 内部细节 —— 拿 `trace_id` 去你自己的 trace 后端查。

#### 2.G.2 设计原则（4 条）

1. **Bounded pool + drop-oldest**：容量满了挤掉最老的，让内存可控
2. **驱逐即信号**：老 op 被挤掉 = stall 信号；年轻 op 被挤掉 = 高吞吐
3. **OpRecord 极简且不可变**：7 个字段，注册时一次写入，到 End 才删除（**无 phase / 无 mutable extra**）
4. **零业务代码 import opregistry**：经由 `common/metrics/op.go` 间接 hook

#### 2.G.3 服务端架构

**包结构**：

```
common/runtime/opregistry/        ← 通用 registry
├── types.go                       ← OpType / OpRecord / Filter / Stats
├── registry.go                    ← bounded pool + drop-oldest
├── eviction.go                    ← 驱逐 + young/old 信号桶
├── metrics.go                     ← Prometheus 自身指标
├── observer.go                    ← 实现 metrics.OpObserver
├── interceptor.go                 ← grpc UnaryServerInterceptor
└── registry_test.go

common/metrics/op.go               ← 新文件：StartOp/Op 抽象 + observer chain
common/metrics/op_observer.go      ← 新文件：OpObserver interface
```

**OpRecord 7 字段（不可变）**：

```go
type OpRecord struct {
    OpID      string    // e.g. "fl-7d2f4a9-001"
    OpType    OpType    // append/read/flush/compact/recover/fence/meta
    TraceID   string    // OTel hex；空 = 无 span context
    SpanID    string    // OTel hex
    StartedAt time.Time
    LogID     int64     // 0 = N/A
    SegmentID int64     // 0 = N/A
}
```

每条约 100 bytes，capacity 1024 总占用 ~100KB。

**`metrics.Op` 桥梁**：

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

业务代码用法：
```go
op := metrics.StartOp("file.flush",
    metrics.WpFileFlushLatency.WithLabelValues(...),
    prometheus.Labels{"log_id": w.logIdStr, "segment_id": w.segmentIdStr})
var status = "success"
defer func() { op.End(status) }()
// ... do work ...
if err != nil { status = "error" }
```

**OpObserver 接口（仅 2 个 hook）**：

```go
type OpObserver interface {
    OnOpStart(op *Op) uint64
    OnOpEnd(op *Op, handle uint64, elapsed time.Duration, status string)
}

func RegisterOpObserver(o OpObserver)
```

**驱逐策略（`eviction.go`）**：
- 容量满时从 list front 取最老的删除
- 计算 age = now - StartedAt
- `age < warn_age`（默认 30s）→ `EvictedYoung++`，DEBUG log
- `age >= warn_age` → `EvictedOld++`，**WARN log**
- Prometheus histogram `wp_op_registry_evicted_age_seconds.Observe(age.Seconds())`
- Prometheus counter `wp_op_registry_evicted_total{signal="young|old"}.Inc()`

**`evicted_old` counter 是核心 alarm 指标**：
```
rate(wp_op_registry_evicted_total{signal="old"}[5m]) > 0
```
=== 集群正在 stall。

**业务代码 mechanical migration（约 12 处 op-shaped histogram call sites）**：

| 位置 | 现有 metric | 新 op_type |
|---|---|---|
| `server/logstore.go` × 9 个 method | `WpLogStoreOperationLatency` | `logstore.<method>` |
| `server/storage/objectstorage/writer_impl.go` flush | `WpFileFlushLatency` | `file.flush` |
| `server/storage/stagedstorage/writer_impl.go` flush | `WpFileFlushLatency` | `file.flush` |
| `server/storage/disk/writer_impl.go:617` flush | `WpFileFlushLatency` | `file.flush` |
| compact / recover / fence 入口 × 2 backend | `WpFileCompactLatency` 等（recovery/fence 可能要新增 metric） | `file.compact` 等 |

每处 ~5 行 diff，纯机械替换：

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

**gRPC interceptor 兜底**：

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

`cmd/main.go` 在创建 grpc.Server 时挂上一处即可。

**服务端启动期（`cmd/main.go`）**：
```go
opReg := opregistry.New(
    cfg.Woodpecker.Runtime.OpRegistry.Capacity,    // 默认 1024
    cfg.Woodpecker.Runtime.OpRegistry.WarnAge,     // 默认 30s
)
metrics.RegisterOpObserver(opReg)

adminCallbacks.Ops = http_management.OpsCallbacks{
    List:  opReg.List,
    Get:   opReg.Get,
    Stats: opReg.Stats,
}
```

#### 2.G.4 CLI 命令（3 个）

##### G.1 `wp ops list <node>`

**Flags**：`--type`（多次）/ `--log` / `--seg` / `--longer-than` / `--limit` / `--sort-by` / `--all-nodes` / `-o`

**Endpoint**：`GET /admin/runtime/ops?type=...&log_id=...&segment_id=...&longer_than_ms=...&limit=...`

**输出（单节点）**：
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

#### 2.G.5 G 族 Admin Endpoints

| Path | Method | 用途 |
|---|---|---|
| `/admin/runtime/ops` | GET | List ops with query filters |
| `/admin/runtime/ops/{op_id}` | GET | Get single op |
| `/admin/runtime/ops/stats` | GET | RegistryStats |

#### 2.G.6 G 族 Server 改动汇总

**单一最大工程量**：

| 类别 | 改动 |
|---|---|
| 新通用包 `common/runtime/opregistry/` | ~500 行 |
| 新文件 `common/metrics/op.go` + `op_observer.go` | ~200 行 |
| 配置 `Woodpecker.Runtime.OpRegistry.{Capacity, WarnAge}` | `common/config/configuration.go` |
| `cmd/main.go` wiring（实例化 + RegisterOpObserver + admin callbacks + grpc.NewServer 加 interceptor） | ~30 行 |
| 业务代码 mechanical migration ~12 处 | `server/logstore.go` × 9 + `server/storage/{disk,objectstorage,stagedstorage}/writer_impl.go` × 3-5 |
| 新 endpoint × 3 | `common/http/management/ops_handler.go`（新） |

约 **1100 行新代码 + ~150 行机械迁移**。**没有任何业务文件 import opregistry**。

---

## 3. 横切关注点

### 3.1 Context 模型（kubeconfig 风格）

CLI 配置文件 `~/.woodpecker/cli.yaml`：

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

**配置文件位置探测顺序**：
1. `--config <path>` flag
2. `$WOODPECKER_CLI_CONFIG`
3. `$XDG_CONFIG_HOME/woodpecker/cli.yaml`
4. `~/.woodpecker/cli.yaml`

### 3.2 配置解析链

```
命令行 flag  >  环境变量  >  当前 context 字段  >  defaults 节  >  内置硬编码默认
```

**关键 flag / env / context 字段对照**：

| Flag | Env Var | Context 字段 | 默认 |
|---|---|---|---|
| `--context <name>` | `WOODPECKER_CONTEXT` | `current-context` | (第一个 context) |
| `--endpoint <url>` | `WOODPECKER_ENDPOINT` | `contexts.<name>.endpoint` | (无) |
| `--admin-port <n>` | `WOODPECKER_ADMIN_PORT` | `contexts.<name>.admin_port` | `9091` |
| `--timeout <dur>` | `WOODPECKER_TIMEOUT` | `contexts.<name>.timeout` | `30s` |
| `--concurrency <n>` | — | `contexts.<name>.concurrency` | `8` |
| `--strict` | — | `contexts.<name>.strict` | `false` |
| `-o, --output <fmt>` | `WOODPECKER_OUTPUT` | `defaults.output` | `table` |
| `--no-color` | `NO_COLOR` | `defaults.no_color` | 自动检测 tty |
| `-v, --verbose` | `WOODPECKER_VERBOSE` | — | `0` |

### 3.3 全局 flags

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

### 3.4 输出格式约定

**所有命令默认 `-o table`**。

| Format | 含义 | 适用 |
|---|---|---|
| `table` | 默认。list 类是行列、show/stats 类是 sectioned key-value | 全部命令 |
| `wide` | table 增强版，多几列 | list 类 |
| `json` | RFC 8259 JSON | 全部 |
| `yaml` | YAML 1.2 | 全部 |
| `tree` | ASCII 树状图 | 仅 `wp cluster info` |
| `unified` | `diff -u` 风格 | 仅 `config diff` / `env diff` |
| `raw` | 原样 Prometheus text format | 仅 `metrics snapshot` |

**严重度标记**：

| 标记 | ANSI |
|---|---|
| `✅` OK | green |
| `🟡` Warning | yellow |
| `🔴` Critical | red |
| `⚠` Annotation | yellow |
| `↑ ↓` Trend | (none) |
| `—` N/A | dim |

**颜色禁用**：`NO_COLOR` env var、`--no-color` flag、stdout 非 tty 时自动禁用。

**输出分流**：
- **stdout** = 命令的正常输出（table/json/yaml）
- **stderr** = 错误、警告、调试日志、进度提示

### 3.5 退出码总表

| Code | 含义 |
|---|---|
| 0 | 成功 |
| 1 | 网络/连接错误 |
| 2 | Usage 错误 |
| 3 | Target 不存在 |
| 4 | State 冲突 |
| 5 | Wait/Watch 超时 |
| 6 | Strict 模式部分失败 |
| 7 | 用户中止（confirmation 答 "n"） |
| 8 | Yellow finding |
| 9 | Red finding |
| 10 | 故意未实现 |
| 11 | Op/resource id 不存在 |
| 12 | CLI 配置错误 |
| 13 | 前置条件缺失 |
| 130 | SIGINT (Ctrl+C, Unix 标准) |
| 100-200 | k8s execute 透传 = `100 + kubectl_exit_code` |

**错误显示格式**：

Table mode（人类可读）：
```
wp: <command>: <short reason>
  Detail: <one-line context>

  Hint: <suggested fix or next command>
```

JSON mode：
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

### 3.6 网络行为

| 项 | 默认 / 行为 |
|---|---|
| 单 HTTP 调用超时 | 30s |
| `--watch` / `--wait` 整体 | 30m |
| Fan-out 并发度 | 8 |
| 重试 | **不重试**（`--watch`/`--wait` 本身就是轮询） |
| TLS | Phase 1 不支持 HTTPS admin port；用户的 HTTPS 端口可走 `--endpoint https://...` 但 CLI 不验证证书 |

### 3.7 安全模型

`wp` CLI 设计为**管理员工具**，**不内置任何身份认证 / 鉴权机制**。Admin HTTP 端口（默认 `:9091`）必须由部署运维通过**网络层隔离**（K8s NetworkPolicy / 安全组 / 防火墙）来限制只对集群内部和管理员网络可访问。CLI 不会做以下任何事：

- 不发送 Authorization 头
- 不处理 client 证书
- 不做 OAuth / OIDC / SSO
- 不做 RBAC

任何能够访问 `:9091` 的人都视为管理员。如果环境需要 admin 端口公网可达，请在 server 前部署反向代理做认证；CLI 通过该代理访问时，反向代理透明工作。

### 3.8 CLI 自身的日志

`-v` 计数 flag 控制 CLI 自身的内部日志详细度（**不是** server 端日志级别）：

| Flag | 级别 | 内容 |
|---|---|---|
| (无) | error | 只有错误消息 |
| `-v` | info | 加 fan-out 进度、HTTP 请求摘要 |
| `-vv` | debug | 加每个 HTTP 请求的 URL/状态码/耗时 |
| `-vvv` | trace | 加 HTTP 请求/响应 body |

**全部 verbose 输出走 stderr**。

### 3.9 输入约定

**节点标识符**：支持三种形式（按优先级）
1. `node_id`（来自 memberlist，最权威）
2. `host:port`（gossip advertise addr）
3. IP 前缀（唯一匹配）

**Duration**：Go 标准 `time.ParseDuration` 格式（`30s`、`5m`、`2h30m`）

**列表 / 多值 flag**：支持 `--type a --type b` 或 `--type a,b`

**Confirmation prompts**：干预型命令默认要求确认；`-y / --yes` 跳过；**`stdin` 非 tty 时直接拒绝执行（exit 2）**，避免脚本错用。

---

## 4. 测试策略

### 4.1 测试金字塔（简化版，2 层）

```
        ▲
        │
        │ Layer 2: 本地 E2E 测试（docker-compose 拉起集群）
        │   位置：tests/docker/wpcli/（严格 mirror tests/docker/monitor/）
        │   触发：开发者本地手动 ./run_wpcli_tests.sh
        │   不进 CI workflow，不阻塞 PR
        │   覆盖：每个命令族对真集群的端到端功能 + eviction 信号验证
        │         + wp 对 docker-compose 多节点集群的 fan-out / 干预动作
        │   真实性：真二进制 `wp` 跑真 `woodpecker server` 进程 +
        │           真 etcd + 真 MinIO + 真 gossip / 真 admin HTTP
        │
        │ Layer 1: 单元测试
        │   位置：源码同包 _test.go
        │   触发：go test ./...（随仓库现有 CI 一起跑）
        │   覆盖：
        │     • opregistry / metrics op chain
        │     • 新 admin handler
        │     • CLI cobra 命令 / output renderer / config / fanout
        │     • 退出码映射 table-driven test
        │     • lifecycle.go 的 CancelDecommission 增量
        │     • 业务代码 metric migration regression test
        ▼
```

**WIP 不引入任何 wpcli 专属的 CI workflow**。单元测试随仓库现有 CI 一起跑。

### 4.2 单元测试覆盖

**Server 侧新增代码**：

| 模块 | 关键测试 |
|---|---|
| `common/runtime/opregistry/` | Register/Done lifecycle、FIFO 顺序、eviction young/old 分桶、log level、Prometheus 指标、ListFilter、并发 race（`go test -race`）、Stats 一致性 |
| `common/metrics/op.go` + `op_observer.go` | StartOp/End 触发 observer、histogram.Observe 调用、End 幂等、observer panic 隔离、并发 race |
| `common/http/management/*_handler_test.go` | 每个新 handler：happy path、405、404、500、query param 解析 |
| `server/lifecycle_test.go` 追加 | `TestCancelDecommission_*` 三种状态转换 + 并发 |
| 业务代码 metric migration regression | 每个迁移点写一个 test 验证原 histogram 行为不变 |

**CLI 侧**：

| 模块 | 关键测试 |
|---|---|
| `cmd/wpcli/cmd/*_test.go` | 每个子命令：happy + error path + flag 解析 |
| `cmd/wpcli/output/*_test.go` | golden file 测试，`-update` flag 一键更新 |
| `cmd/wpcli/config/cli_config_test.go` | YAML 解析、context 解析、三级 overlay、错误处理 |
| `cmd/wpcli/client/fanout_test.go` | 全成功、部分失败默认/strict、全失败、并发上限、超时、ctx cancel |
| `cmd/wpcli/internal/errors/exit_test.go` | table-driven 覆盖**所有** 14 个退出码 |
| `cmd/wpcli/internal/k8s/executor_test.go` | print mode、kubectl 缺失、命令组装、exit code 透传、SIGINT 透传 |

### 4.3 本地 E2E 测试（`tests/docker/wpcli/`）

**位置**：`tests/docker/wpcli/`，**严格 mirror 现有 `tests/docker/monitor/` 的目录结构**。这是 `wp` CLI 的**端到端测试套件**，对齐真实多节点集群环境（docker-compose 拉起的 woodpecker server 集群 + etcd + MinIO），不需要 kubernetes 也不需要 kind。

**为什么选 docker-compose 而不是 kind**：
- `tests/docker/monitor/` 已经建立了 docker-compose 这套成熟 pattern，wpcli 直接复用减少重复
- docker-compose 在开发机上启动快（30s 内），kind 慢 2-3 倍
- `wp` CLI 除了 F 族的 `-x` execute 模式外，不依赖任何 k8s 特有能力
- F 族的 `-x` 模式用 fake kubectl shim 在 docker-compose 环境下就能验证
- 真实的 Operator + `wp` 集成测试（Phase 3 的 F 族 e2e）用 kind 手动跑，不进这套自动化

**目录结构（严格对齐 monitor）**：

```
tests/docker/wpcli/
├── README.md                       # 使用说明 + "Local E2E, not CI"
├── docker-compose.wpcli.yaml       # 对应 monitor/docker-compose.monitor.yaml
│                                   #   etcd + minio + 3 woodpecker server
│                                   #   外加一个"slow backend"注入 sidecar 用于 stuck 场景
├── wpcli_cluster.go                # 对应 monitor/monitor_cluster.go
│                                   #   起集群 / 停集群 / 等 ready / 注入故障
├── wpcli_test.go                   # 对应 monitor/monitor_test.go
│                                   #   主入口 + 子测试调度
├── run_wpcli_tests.sh              # 对应 monitor/run_monitor_tests.sh
│                                   #   up → test → down 一键
├── prometheus.yml                  # 对应 monitor/prometheus.yml（metrics 场景用）
├── grafana/                        # 对应 monitor/grafana/（可选，live 观察用）
│   └── ...
└── scenarios/                      # 对应 monitor/rolling_restart/ 等 pattern
    ├── node_decommission/          # A 族：decommission --wait + cancel
    │   └── decommission_test.go
    ├── ops_eviction_signal/        # G 族：构造 stuck op 验证 evicted_old 信号
    │   └── eviction_signal_test.go
    ├── logstore_force_flush/       # C 族：force-flush 验证 BufferBytes 归零
    │   └── force_flush_test.go
    ├── metrics_stuck_flush/        # D 族：scenario report 在 slow backend 下出红色 finding
    │   └── stuck_flush_test.go
    ├── k8s_hybrid/                 # F 族：fake kubectl shim 验证命令组装 + exec 透传
    │   └── hybrid_test.go
    └── cluster_health/             # B 族：挂一个节点后 cluster health 变色
        └── health_test.go
```

**关键对齐点**：
- **文件命名**：`wpcli_cluster.go` / `wpcli_test.go` / `docker-compose.wpcli.yaml` / `run_wpcli_tests.sh` 严格对应 monitor 的对应文件
- **分层**：top-level 是 orchestration（compose + cluster helper + 主入口）；scenarios/ 下的子目录是独立场景
- **入口**：`./run_wpcli_tests.sh` 是唯一运行入口，同 monitor

**测试运行方式**：
```bash
# 一键跑全部
cd tests/docker/wpcli/
./run_wpcli_tests.sh

# 或手动控制
docker compose -f docker-compose.wpcli.yaml up -d
go test ./... -tags=docker  # (build tag 对齐 monitor)
docker compose -f docker-compose.wpcli.yaml down -v
```

**必须验证的关键场景**（按命令族 + scenario 目录）：

| 族 | scenario 目录 | 必测 |
|---|---|---|
| **A** | `node_decommission/` | `node list` 看到全集群；`node decommission --wait` 真等到 `safe_to_terminate=true` + 心跳每 15s 输出一行；`cancel-decommission` 在 decommissioning 中途真把状态转回 active |
| **B** | `cluster_health/` | `cluster info` 在 3 节点上输出正确数；`cluster health` 在 kill 一个 container 后变 yellow / red；`gossip-diff` 在分区模拟下报 inconsistent |
| **C** | `logstore_force_flush/` | `logstore segments` 在持续写入下能看到 active segment；`segment-show` 字段非空；`force-flush` 后 `buffer_bytes` 归零；`fence` + `compact` 状态转换正确 |
| **D** | `metrics_stuck_flush/` | `metrics snapshot` 拿到非空响应；`metrics top --hot-flushers` 在 slow backend 下返回 top 节点；`metrics report --scenario stuck-flush --window 30s` 产出 critical finding + exit 9 |
| **E** | `node_decommission/` 或独立 | `config show` 拿到完整 config；`env show` 拿到 build info；`logging set/get-level` 闭环；`profile --type cpu --seconds 5` 下载到合法 pprof |
| **F** | `k8s_hybrid/` | 用 fake kubectl shim（一个 bash 脚本假装是 kubectl，只 echo 调用参数）验证：print mode 输出正确命令、`-x` 模式真调 shim、shim exit code 透传、`logs -f` SIGINT 透传 |
| **G** | `ops_eviction_signal/` | `ops list` 在持续写入下看到 in-flight ops；**构造 stuck flush**（slow backend sidecar）后 `ops list --longer-than 30s` 非空、`ops stats` 显示 `evicted_old > 0`、`ops show --op-id` 能拿到完整字段 |

**特别强调 G 族的 eviction 信号验证**：G 族的核心价值主张是"驱逐即信号"，如果 e2e 测试不能验证这个，整个 opregistry 的设计就没被端到端证明过。这个场景必须做，不能省。

### 4.4 Phase 3 的 K8s / Operator E2E（手动）

F 族的 `-x` execute 模式和 Operator 的集成（比如 `wp k8s scale -x` 真的让 Operator reconcile）**不进入自动化 E2E**，而是作为 **Phase 3 发布前的手动验证项**：

- 用 `kind` 起一个本地 k8s 集群
- 装 Woodpecker Operator + 创建一个 WoodpeckerCluster CR
- 手动跑：
  - `wp --kube-context kind-wpcli k8s status -x`
  - `wp --kube-context kind-wpcli k8s scale --replicas 5 -x` + 观察 Operator reconcile
  - `wp --kube-context kind-wpcli k8s logs <pod> -f -x` 真 tail
- 记录结果到 Phase 3 release notes

**为什么不自动化**：kind + Operator 的组合启动慢（2-3 分钟）且对 CI runner 内存要求高；运行频率低（Phase 3 发布前一次）；每次 Operator 升级还要重跑。手动验证的 ROI 更高。

如果未来要自动化，预留路径 `tests/k8s/wpcli/`（和 `tests/docker/wpcli/` 平级），使用 kind-based 脚本。**但本 WIP spec 明确不承诺这个路径**。

### 4.5 测试覆盖率目标

| 模块 | 目标 |
|---|---|
| `common/runtime/opregistry/` | ≥ 90% |
| `common/metrics/op.go` + `op_observer.go` | ≥ 90% |
| `common/http/management/` 新 handler | ≥ 85% |
| `server/lifecycle.go` 新方法 | ≥ 90% |
| `cmd/wpcli/cmd/` | ≥ 70% |
| `cmd/wpcli/output/` | ≥ 80%（golden 测试） |
| `cmd/wpcli/client/` | ≥ 85% |
| `cmd/wpcli/config/` | ≥ 85% |
| `cmd/wpcli/internal/k8s/` | ≥ 75% |
| **CLI 整体** | ≥ 75% |

### 4.6 关键测试技术

- **Time mocking**：用 `github.com/jonboulle/clockwork v0.2.2`，opregistry / `--watch` / `--longer-than` 全部走 mock clock
- **Output capture**：注入 `io.Writer` 而非 `os.Stdout`，便于单元测试
- **Golden files**：`output/testdata/golden/*.golden`，`-update` flag 重新生成
- **并发测试**：`go test -race` 必跑，opregistry / fan-out / observer chain 是重点

---

## 5. 分期落地计划

### 5.1 分期原则

1. **每期端到端可交付**：哪怕只 merge 了 Phase 1，wp 二进制也能跑、有用
2. **从 ABI 稳定的部分开始**：Phase 1 把"不会再大改"的基础设施先打齐
3. **Read-only 优先于 write**
4. **Op Registry 单独成 Phase**：独立 Phase 让它能聚焦评审
5. **K8s hybrid 放最后**：核心运维不依赖

### 5.2 Phase 依赖图

```
                       ┌──────────────────────┐
                       │  Phase 1 - Foundation │
                       │  (CLI 骨架 + 节点基础)  │
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

**Scope（命令）**：
- `wp ctx use / list / view`
- `wp version`
- A 族完整：`node list/show/decommission/drain-status/cancel-decommission/restart`
- B 族完整：`cluster info/health/gossip-diff`
- E.1 / E.2：`config show/diff`
- E.6 / E.7：`env show/diff`
- E.5：`profile`

**Phase 1 命令总数：14 个 feature 命令** + 4 个 CLI 基础命令（`wp ctx use/list/view`、`wp version`）= 共 18 个 cobra 子命令

**Server 改动**：
1. `/admin/memberlist` 加 JSON 模式
2. `/admin/node/status` 字段补齐
3. `POST /admin/node/decommission/cancel` 新端点
4. `NodeLifecycleManager.CancelDecommission()` 严格版
5. `GET /admin/config` 新端点
6. `GET /admin/env` 新端点
7. Linker ldflags 注入 build info

**CLI 交付**：
- `cmd/wpcli/` 完整骨架（cobra + client + output + config + errors）
- Makefile `make wpcli` target
- 14 个子命令实现 + 单元测试
- A/B/E 族每个命令至少 happy + 1 error 测试
- output renderer golden file 测试
- 退出码 table-driven 全表覆盖

**Acceptance criteria**：
- [ ] `make wpcli` 产出 `bin/wp` 静态二进制
- [ ] `wp --version` 显示 build info
- [ ] `wp ctx list / use / view` 闭环
- [ ] `wp node list / show / decommission --wait / drain-status -w / cancel-decommission` 在 docker-compose 上跑通
- [ ] `wp cluster info / health / gossip-diff` 同上
- [ ] `wp config show / diff` 同上
- [ ] `wp env show / diff` 同上
- [ ] `wp profile node-2 --type cpu --seconds 5` 拉到合法 pprof 文件
- [ ] 单元测试通过，覆盖率 ≥ 75%
- [ ] Phase 1 所有 admin endpoint 改动都有 handler test 覆盖（3 个全新 + 2 个现有增强）

### 5.4 Phase 2 — Observability & Op Registry

**Scope（命令）**：
- C 族完整（7 个）：`logstore segments/segment-show/buffer/flush-queue/force-flush/fence/compact`（dump 已合并为 `segment-show --full`）
- D 族完整：`metrics list/snapshot/top/watch/report`
- E.3 / E.4：`logging get-level/set-level`
- G 族完整：`ops list/show/stats`
- 本地 E2E 测试 `tests/docker/wpcli/` 完整 setup（mirror `tests/docker/monitor/`）

**Phase 2 命令总数：17 个**（累计 31）

**Server 改动**：

a) **新通用包**：
- `common/runtime/opregistry/` ~500 行
- `common/metrics/op.go` + `op_observer.go` ~200 行

b) **Admin endpoint 改动（10 个，其中 8 个全新 + 2 个 stub 补实）**：
- C 族 5 个全新：`logstore/segments`、`logstore/segments/{log}/{seg}`、`logstore/flush`、`logstore/fence`、`logstore/compact`
- G 族 3 个全新：`runtime/ops`、`runtime/ops/{id}`、`runtime/ops/stats`
- E 族 2 个补实（路径早已存在但 body 是 TODO stub）：`/log/level` GET + POST

c) **业务代码改动**（侵入项）：
- `storage.Writer` interface 加 `Snapshot()` / `SnapshotDetailed()`
- 两个 backend（objectstorage / stagedstorage）实现
- `WriterRegistry` 抽象
- ~12 处 op-shaped histogram call site mechanical migration
- Logger AtomicLevel 化
- gRPC interceptor 注入
- `cmd/main.go` 实例化 opregistry + RegisterOpObserver + admin callbacks
- Mockery 重新生成

**Phase 2 总侵入约 ~520 行 diff** 分布在 ~10 个现有文件（详见 §6.3 的详细拆分），其中约 150 行是 ~12 处 op-shaped histogram call site 的机械迁移。每处迁移有 regression test。

**CLI 交付**：
- 17 个新 cobra 子命令
- YAML scenario 引擎 + 12 个内置 scenario 文件
- 跨命令 hint 框架
- `tests/docker/wpcli/` 完整 setup

**Acceptance criteria**：
- [ ] 所有 17 个新命令通过 docker-compose 端到端验证
- [ ] **关键 acceptance**：在 docker-compose 上构造一个 stuck flush 场景，跑：
  - [ ] `wp ops list --longer-than 30s` 能看到那个 flush op
  - [ ] `wp ops stats` 显示 `evicted_old > 0`
  - [ ] `wp metrics report --scenario stuck-flush --window 1m` 产出 critical finding + exit 9
- [ ] 业务侧 metric migration 的 regression test 全过
- [ ] opregistry 单元测试 ≥ 90% 覆盖
- [ ] gRPC interceptor 在所有 RPC 上正确注册 + 反注册（concurrent test）
- [ ] 现有 Woodpecker 主仓库测试**不**因为 metric migration regress

### 5.5 Phase 3 — K8s Integration & Polish

**Scope（命令）**：
- F 族完整：`k8s status/scale/logs/doctor`
- 发布工件：跨平台二进制构建、Docker image 嵌入 wp、GitHub Release 自动化
- 文档：`cmd/wpcli/README.md`、quickstart、cookbook（10 个常见排障 recipe）、configuration reference

**Phase 3 命令总数：4 个**（累计 35，spec 全部交付）

**Server 改动**：**零**

**CLI 交付**：
- F 族 4 命令 + dual-mode（print / `-x` execute）
- `cmd/wpcli/internal/k8s/` executor 封装
- Cross-platform build：`make wpcli-release` 输出 4 平台二进制
- Docker image 集成：`build/Dockerfile` `COPY bin/wp /usr/local/bin/wp`
- GitHub Release workflow
- 文档：`cmd/wpcli/README.md`、`docs/wpcli/quickstart.md`、`docs/wpcli/cookbook.md`、`docs/wpcli/configuration.md`

**Acceptance criteria**：
- [ ] F 族 4 命令在装了 kubectl + kind 的开发机上 `-x` 模式能跑
- [ ] F 族 4 命令在没装 kubectl 的环境下 print 模式正常输出 + exit 10
- [ ] `make wpcli-release` 产出 4 个平台的二进制
- [ ] server Docker image 里 `wp --version` 工作
- [ ] README + quickstart + cookbook 写完
- [ ] spec 文件 finalize（去掉 WIP 标记）

### 5.6 风险登记

| 风险 | 等级 | 影响 | 缓解 |
|---|---|---|---|
| Phase 2 业务 metric 迁移破坏现有 metric 行为 | 🔴 高 | 现有监控告警断 | 每处迁移有 regression test；和现有 grafana dashboard 对比验证 |
| gRPC interceptor 引入性能 regression | 🟡 中 | append latency 上升 | benchmark `BenchmarkAppend` 在迁移前后对比；observer 链 hot path 用 RWMutex 优化 |
| `WriterRegistry` 抽象不通用，两种 backend 实现差异大 | 🟡 中 | C 族 endpoint 字段在两种 backend 上不一致 | 接口定义阶段先在两种 writer 上分别画 snapshot 字段表，取交集做 interface |
| `tests/docker/wpcli/` 在 ARM 开发机上跑不动 | 🟡 中 | M 系列 mac 开发者无法本地验证 | docker-compose 用 multi-arch image |
| `clockwork v0.2.2` 和 Woodpecker 现有依赖版本冲突 | 🟢 低 | 编译失败 | 实现期 grep go.mod 确认；如有冲突回退到自写 Clock interface |

---

## 6. 侵入项总览（Reviewer Checklist）

这一节是给 reviewer 的**单页 checklist** — 把分散在前几节的所有 server-side 改动汇总到一处，让评审者能闭环检查"我们到底碰了哪些现有文件、新建了什么、引入了什么依赖"。

### 6.1 新增包

| Phase | 包路径 | 行数估算 |
|---|---|---|
| 2 | `common/runtime/opregistry/` | ~500 |
| 1 | `common/version/`（如缺） | ~30 |
| 1 | `cmd/wpcli/` 全套 | ~3000 |
| 2 | `cmd/wpcli/internal/scenarios/` | ~400 |
| 2 | `cmd/wpcli/internal/hints/` | ~150 |
| 3 | `cmd/wpcli/internal/k8s/` | ~400 |
| 2 | `tests/docker/wpcli/` | ~500 |

### 6.2 新增文件（在已有包里）

| Phase | 文件 | 用途 |
|---|---|---|
| 2 | `common/metrics/op.go` | `Op` 类型 + `StartOp` / `End` API |
| 2 | `common/metrics/op_observer.go` | `OpObserver` interface + observer chain |
| 2 | `common/metrics/op_test.go` | 单测 |
| 2 | `common/http/management/logstore_handler.go` | C 族 5 个 endpoint |
| 2 | `common/http/management/logstore_handler_test.go` | 单测 |
| 2 | `common/http/management/ops_handler.go` | G 族 3 个 endpoint |
| 2 | `common/http/management/ops_handler_test.go` | 单测 |
| 1 | `common/http/management/config_handler.go` | E 族 GET /admin/config |
| 1 | `common/http/management/config_handler_test.go` | 单测 |
| 1 | `common/http/management/env_handler.go` | E 族 GET /admin/env |
| 1 | `common/http/management/env_handler_test.go` | 单测 |
| 2 | `common/http/management/logging_handler.go` | E 族 GET/POST /log/level |
| 2 | `common/http/management/logging_handler_test.go` | 单测 |
| 2 | `server/storage/writer_registry.go` | `WriterRegistry` 接口 + 包 server 内 writer 列表 |

### 6.3 修改的现有文件（侵入项 — 重点）

| Phase | 文件 | 改动 | diff 行数 |
|---|---|---|---|
| 1 | `Makefile` | 加 `make wpcli` target + ldflags | ~10 |
| 1 | `cmd/main.go` | 注册 cancel-decommission callback、config callback、env callback | ~30 |
| 2 | `cmd/main.go` | 实例化 opregistry、`metrics.RegisterOpObserver`、注入 ops admin callbacks、grpc.NewServer 加 interceptor | ~30 |
| 1 | `common/config/configuration.go` | 加 `Runtime.OpRegistry.{Capacity, WarnAge}` 配置项 | ~30 |
| 1 | `common/http/router.go` | 加路径常量：`AdminNodeDecommissionCancelPath`、`AdminConfigPath`、`AdminEnvPath` | ~10 |
| 2 | `common/http/router.go` | 加路径常量：5 个 logstore + 3 个 ops + 2 个 log level | ~25 |
| 1 | `common/http/server.go` | `AdminCallbacks` struct 加字段：`CancelDecommission`、`Config`、`Env`；handler 注册 | ~40 |
| 2 | `common/http/server.go` | `AdminCallbacks` 加 `Logstore`、`Ops`、`LogLevel` 子结构；handler 注册 | ~50 |
| 1 | `common/http/server.go` | `/admin/memberlist` 加 JSON 模式 | ~20 |
| 1 | `common/http/management/node_handler.go` | 加 `NewNodeCancelDecommissionHandler` | ~25 |
| 1 | `common/http/management/node_handler_test.go` | 加 cancel-decommission test 用例 | ~50 |
| 2 | `common/logger/logger.go` | 用 `zap.AtomicLevel` 替换静态 level + 暴露 `GetLevel()` / `SetLevel()` | ~30 |
| 1 | `server/lifecycle.go` | 加 `CancelDecommission()` 严格版方法 | ~30 |
| 1 | `server/lifecycle_test.go` | 加 `TestCancelDecommission_*` 用例 | ~80 |
| 1 | `server/service.go` | 实现 `CancelDecommission()` 服务方法 | ~20 |
| 2 | `server/logstore.go` | 9 个 method 全部 metric migration | ~100 |
| 2 | `server/storage/objectstorage/writer_impl.go` | 实现 `Snapshot()` / `SnapshotDetailed()`；flush task 入口 migration | ~80 |
| 2 | `server/storage/stagedstorage/writer_impl.go` | 同上 | ~80 |
| 2 | `server/storage/disk/writer_impl.go` | 现有 line 617 的 flush metric 单点迁移 | ~10 |
| 2 | `server/storage/storage.go` | `Writer` interface 加 `Snapshot()` / `SnapshotDetailed()` 方法 | ~10 |
| 2 | `server/storage/{objectstorage,stagedstorage,disk}/...` | compact / recovery / fence 入口加 `metrics.StartOp` 包装 | ~50 |
| 2 | `common/metrics/service_metrics.go` | 补 `WpFileRecoveryLatency` / `WpFileFenceLatency`（如不存在） | ~30 |

**侵入项小计**：
- Phase 1：~270 行 diff，分布在 ~8 个现有文件
- Phase 2：~520 行 diff，分布在 ~10 个现有文件
- Phase 3：~0 行 diff（F 族纯新增）

**最高侵入热点**：`server/logstore.go`（Phase 2 ~100 行 diff），因为它有 9 个 metric 迁移点。

### 6.4 配置项变更

| Phase | 配置路径 | 类型 | 默认值 | 用途 |
|---|---|---|---|---|
| 2 | `woodpecker.runtime.op_registry.capacity` | int | 1024 | opregistry 容量 |
| 2 | `woodpecker.runtime.op_registry.warn_age` | duration | 30s | 驱逐 young/old 分界 |

**仅 2 项新配置**。两项都有合理默认值，不强制用户在 yaml 里写。

### 6.5 新依赖

| Phase | 模块 | 版本 | 用途 |
|---|---|---|---|
| 2 | `github.com/jonboulle/clockwork` | `v0.2.2` | Time mocking（opregistry 测试用） |
| 1 | `github.com/spf13/cobra` | latest stable | CLI 命令框架 |
| 1 | `github.com/spf13/pflag` | latest stable | cobra 间接依赖 |
| 1 | `gopkg.in/yaml.v3` | latest stable | cli.yaml 解析（应已存在） |
| 1 | `github.com/prometheus/common/expfmt` | latest stable | metrics 文本格式解析（应已存在） |

实现期 grep `go.mod` 确认。

### 6.6 Mockery 重新生成清单

**Phase 2**：
- `metrics.OpObserver`（新接口）
- `opregistry.Registry`（如 interface 化）
- `storage.Writer` interface 增加方法 → mock 重新生成
- `storage.WriterRegistry`（新接口）
- `cmd/wpcli/client.AdminClient`
- `cmd/wpcli/output.Renderer`

**Phase 3**：
- `cmd/wpcli/internal/k8s.Executor`

### 6.7 文档变更

| Phase | 文件 | 改动 |
|---|---|---|
| 1 | `cmd/wpcli/README.md` | 新建（quickstart） |
| 1 | `docs/wpcli/getting-started.md` | 新建 |
| 2 | `cmd/wpcli/README.md` | 追加 Phase 2 命令族说明 |
| 2 | `docs/wpcli/observability.md` | 新建（解释 ops 命令族、scenario report、stuck-op 排查） |
| 3 | `cmd/wpcli/README.md` | 追加 Phase 3 + k8s 集成说明 |
| 3 | `docs/wpcli/cookbook.md` | 新建（10 个常见排障 recipe） |
| 3 | `docs/wpcli/configuration.md` | 新建（cli.yaml 完整 reference + context 模型） |
| 3 | `README.md`（仓库根） | 追加 wpcli 简介段 |

### 6.8 Build / Distribution 改动

| Phase | 改动 |
|---|---|
| 1 | `Makefile` 加 `make wpcli` target，输出 `bin/wp` |
| 1 | `Makefile` 加 ldflags 注入 build info |
| 3 | `Makefile` 加 `make wpcli-release` target，输出 4 平台二进制 |
| 3 | `build/Dockerfile` 改：在 server image 里 `COPY bin/wp /usr/local/bin/wp` |
| 3 | GitHub Release workflow 触发 wpcli 二进制上传 |

### 6.9 Reviewer 一页 checklist（精简版）

| 类别 | Phase 1 | Phase 2 | Phase 3 | 总计 |
|---|---|---|---|---|
| **新增包**（cmd/wpcli/* 算 1 个 top-level） | 1 + common/version | 1（opregistry） | 1（k8s 子包） | 3-4 个 top-level |
| **新增文件** | 4 个 handler + CLI 骨架 | 8 个 handler/op chain | 文档 + Dockerfile mod | ~15 个 |
| **现有文件 diff** | ~270 行 in 8 files | ~520 行 in 10 files | 0 行 | ~790 行 |
| **全新 admin endpoint** | 3（cancel/config/env） | 8（5 logstore + 3 ops） | 0 | **11** |
| **现有 endpoint 增强或补实** | 2（memberlist JSON、node/status 字段） | 2（/log/level GET+POST 补实） | 0 | 4 |
| **业务代码 metric migration** | 0 | ~12 处 / ~150 行 | 0 | ~12 处 |
| **新配置项** | 0 | 2 | 0 | 2 |
| **新依赖** | cobra, pflag | clockwork v0.2.2 | 0 | 3 |
| **mockery 新生成** | 0 | 6 个 | 1 个 | 7 个 |
| **proto 文件改动** | 0 | 0 | 0 | **0** ✓ |
| **数据迁移** | 0 | 0 | 0 | **0** ✓ |
| **向后不兼容** | 0 | 0 | 0 | **0** ✓ |

---

## 7. 非目标

为了让 reviewer 不会问"为什么 spec 里没有这个"，明确列出本 spec **不**做的事：

- ❌ 不修改任何 client SDK（`woodpecker/` 包）
- ❌ 不修改任何 metadata 层代码（`meta/`）
- ❌ 不修改 etcd / minio / gossip 任何依赖
- ❌ 不修改现有 Operator 代码（`deployments/operator/`）
- ❌ 不修改任何 proto 文件（`proto/*.proto`）
- ❌ 不引入任何新的数据持久化（除已有 `node_state.json`）
- ❌ 不引入任何新的 gRPC 方法
- ❌ 不引入任何新的 etcd key prefix
- ❌ 不修改任何现有 metric 的名字 / label set / type（只**增**不**改**）
- ❌ 不修改任何现有 admin endpoint 的 URL 或 HTTP method（只**增**不**改**）
- ❌ 不引入任何认证 / 鉴权 / TLS 相关代码
- ❌ 不嵌入任何外部工具的查询语言（PromQL / JaegerQL）
- ❌ 不向 client SDK 暴露任何新接口
- ❌ 不实现 `wp k8s doctor` 的真实诊断逻辑（Phase 3 仍是 stub）
- ❌ 不支持节点元数据修改（labels / tags 只读）
- ❌ 不承诺多版本 CLI 兼容（Phase 3 起跟随 server 主版本）

**关键保障**：以上 11 条是"现有用户的迁移成本"硬约束 — 任何升级到带 wp 的 server 版本的用户**不需要**改任何 etcd / minio / dashboard / monitoring 配置，server 行为 100% 向后兼容。

---

## 附录 A：参考资料

- 关联 issue：[zilliztech/woodpecker#128](https://github.com/zilliztech/woodpecker/issues/128)
- 现有 admin handlers：`common/http/management/`
- 现有 server lifecycle：`server/lifecycle.go`
- 现有 docker E2E 测试 pattern：`tests/docker/monitor/`
- Woodpecker Operator 文档：`docs/woodpecker_operator.md`
- 现有 metrics 定义：`common/metrics/{client_metrics,service_metrics}.go`

## 附录 B：术语表

| 术语 | 含义 |
|---|---|
| **wp** | 本 CLI 的二进制名 |
| **wpcli** | 本 CLI 的 Go 包名（`cmd/wpcli/`） |
| **admin port** | Woodpecker server 的 HTTP admin/metrics 端口（默认 9091） |
| **fan-out** | CLI 一次命令并发调用集群所有节点的模式 |
| **op** | 一次"已经开始但还没结束的"操作（flush task、compact、gRPC call、etc.） |
| **op registry** | 进程内的 in-flight op 注册表 |
| **eviction signal** | 当 op registry 满了驱逐最老 op 时，根据 age 是否超过 warn_age 区分 young / old，old 是 stall 信号 |
| **warn age** | 驱逐 young/old 的分界时间（默认 30s） |
| **scenario** | D.5 metrics report 的预置诊断场景（YAML 定义） |
| **WriterRegistry** | server 内部的 active writer 索引，C 族 endpoint 的数据源 |
| **OpObserver** | metrics op 生命周期 hook 的接口；opregistry 实现它 |
| **hybrid execute** | F 族 K8s 命令的双模设计：默认打印 kubectl 命令，`-x` 时执行 |
