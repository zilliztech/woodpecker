# wp — Woodpecker Operational CLI

`wp` is the Woodpecker operational CLI for service-mode clusters.
See [`docs/wpcli-design.md`](../../docs/wpcli-design.md) for the full design.

## Build

    make wpcli          # current platform
    make wpcli-release  # linux/amd64, linux/arm64, darwin/arm64, windows/amd64
    ./bin/wp version

## Quick start

See [`docs/wpcli/quickstart.md`](../../docs/wpcli/quickstart.md) for a 15-minute onboarding guide.

## Minimum config

Create `~/.woodpecker/cli.yaml`:

```yaml
current-context: local

contexts:
  local:
    endpoint: http://localhost:9091
    admin_port: 9091
```

See [`docs/wpcli/configuration.md`](../../docs/wpcli/configuration.md) for the full reference.

## Commands

### Node lifecycle
- `wp node list` — list all server nodes
- `wp node show <node>` — detailed view of one node
- `wp node decommission <node>` — graceful decommission (default blocks until safe)
- `wp node drain-status <node> [-w]` — watch decommission progress
- `wp node cancel-decommission <node>` — abort an in-progress decommission
- `wp node restart <node>` — intentionally not implemented (exit 10)

### Cluster overview
- `wp cluster info` — cluster summary + topology tree
- `wp cluster health` — red/yellow/green health check
- `wp cluster gossip-diff` — detect membership view divergence

### Config & env
- `wp config show <node>` — show resolved server config
- `wp config diff --all` — compare config across nodes
- `wp env show <node>` — env vars + Go runtime + host + build info
- `wp env diff` — compare env across nodes

### Diagnostics
- `wp profile <node> --type cpu --seconds 30` — download pprof profile

### Logstore runtime
- `wp logstore segments <node>` — list active segments
- `wp logstore segment-show <node> --log X --seg Y` — detailed segment view
- `wp logstore buffer <node>` — buffer bytes summary
- `wp logstore flush-queue <node>` — flush queue depth summary
- `wp logstore force-flush <node>` — force sync
- `wp logstore fence <node> --log X --seg Y --reason "..." -y` — force fence
- `wp logstore compact <node> --log X --seg Y` — force compaction

### Metrics analysis
- `wp metrics list <node>` — list all metric series
- `wp metrics snapshot [<node>|--all]` — point-in-time snapshot
- `wp metrics top --by <metric>` — cross-node top-N
- `wp metrics watch <metric> <node>` — real-time trend stream
- `wp metrics report --scenario <name>` — scenario-based analysis (12 built-in)

### Ops registry
- `wp ops list <node>` — in-flight operations
- `wp ops show <node> --op-id <id>` — single op detail
- `wp ops stats <node>` — registry utilization + eviction analysis

### Dynamic logging
- `wp logging get-level [<node>|--all]` — read current log level
- `wp logging set-level <node> --level <level>` — change log level

### Kubernetes
- `wp k8s status` — cluster status (print kubectl commands, `-x` to execute)
- `wp k8s scale --replicas N` — scale cluster
- `wp k8s logs <node-or-pod>` — tail pod logs
- `wp k8s doctor` — not yet implemented

### CLI contexts
- `wp ctx list` — list configured contexts
- `wp ctx use <name>` — switch active context
- `wp ctx view` — show resolved active context

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Network/connection error |
| 2 | Usage error |
| 3 | Target not found |
| 4 | State conflict |
| 5 | Wait/Watch timeout |
| 6 | Strict mode partial failure |
| 7 | User abort |
| 8 | Yellow finding |
| 9 | Red finding |
| 10 | Intentionally not implemented |
| 11 | Resource not found |
| 12 | Configuration error |
| 13 | Prerequisite missing |
| 100+ | K8s execute passthrough (100 + kubectl exit code) |

## Incident response cookbook

See [`docs/wpcli/cookbook.md`](../../docs/wpcli/cookbook.md) for 10 common recipes.
