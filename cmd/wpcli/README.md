# wp — Woodpecker Operational CLI

`wp` is the Woodpecker operational CLI for service-mode clusters.
See [`docs/wpcli-design.md`](../../docs/wpcli-design.md) for the full design.

## Build

    make wpcli
    ./bin/wp version

## Minimum config

Create `~/.woodpecker/cli.yaml`:

```yaml
current-context: local

contexts:
  local:
    endpoint: http://localhost:9091
    admin_port: 9091
```

## Phase 1 commands

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

### CLI contexts
- `wp ctx list` — list configured contexts
- `wp ctx use <name>` — switch active context
- `wp ctx view` — show resolved active context

## Exit codes

See [`docs/wpcli-design.md` §3.5](../../docs/wpcli-design.md#35-退出码总表).

## Phase 1 NOT yet implemented

The following command families are planned for Phase 2 and Phase 3:
- `wp logstore *`, `wp metrics *`, `wp ops *`, `wp logging *` — Phase 2
- `wp k8s *` — Phase 3
