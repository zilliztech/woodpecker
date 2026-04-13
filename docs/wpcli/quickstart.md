# wp CLI Quickstart

Get up and running with the Woodpecker operational CLI in 15 minutes.

## 1. Install

**From source:**
```bash
git clone https://github.com/zilliztech/woodpecker.git
cd woodpecker
make wpcli
./bin/wp version
```

**From GitHub Releases:**
Download the binary for your platform from the [Releases](https://github.com/zilliztech/woodpecker/releases) page. Rename to `wp` and add to your PATH.

## 2. Configure

Create `~/.woodpecker/cli.yaml`:

```yaml
current-context: local

contexts:
  local:
    endpoint: http://localhost:9091
    admin_port: 9091
```

For multi-cluster setups, add more contexts:

```yaml
current-context: prod

contexts:
  prod:
    endpoint: http://wp-prod-1.internal:9091
    admin_port: 9091
    concurrency: 16
    timeout: 60s
  staging:
    endpoint: http://wp-staging-1.internal:9091
    k8s:
      namespace: woodpecker
      cluster: wp-staging
```

## 3. First commands

```bash
# Check cluster status
wp cluster info

# List all nodes
wp node list

# Detailed health check
wp cluster health

# Show config of a specific node
wp config show node-1
```

## 4. Switch contexts

```bash
wp ctx list              # show all configured contexts
wp ctx use staging       # switch to staging
wp ctx view              # verify active context
wp cluster info          # now targeting staging cluster
```

## 5. Common workflows

### Decommission a node
```bash
wp node decommission node-3 --wait     # blocks until safe to terminate
wp node drain-status node-3 -w         # watch progress in real time
```

### Check for stuck flushes
```bash
wp ops list node-1 --longer-than 30000  # ops older than 30s
wp logstore flush-queue node-1          # check queue depth
wp ops stats node-1                     # look for old evictions (stall signal)
```

### Download a CPU profile
```bash
wp profile node-1 --type cpu --seconds 30 --output-file node1-cpu.pb.gz
```

### Change log level for debugging
```bash
wp logging set-level node-1 --level debug   # enable debug
# ... reproduce the issue ...
wp logging set-level node-1 --level info    # restore
```

### Run a diagnostic scenario
```bash
wp metrics report node-1 --scenario stuck-flush --window 1m
```

## 6. K8s integration

If running on Kubernetes with the Woodpecker operator:

```bash
# See what kubectl commands would run
wp k8s status --wp-cluster wp-prod -n woodpecker

# Actually execute them
wp k8s status --wp-cluster wp-prod -n woodpecker -x

# Tail logs from a pod
wp k8s logs 0 --wp-cluster wp-prod -n woodpecker -x

# Scale the cluster
wp k8s scale --replicas 5 --wp-cluster wp-prod -n woodpecker -x
```

## 7. Global flags

Every command accepts these flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint` | from cli.yaml | Admin HTTP seed endpoint |
| `--admin-port` | 9091 | Admin port |
| `--timeout` | 30s | Per-request timeout |
| `--concurrency` | 8 | Fan-out concurrency |
| `--strict` | false | Treat partial failures as errors |
| `-o, --output` | table | Output format: table, wide, json, yaml |
| `--no-color` | false | Disable color output |
| `-v` | 0 | Verbosity (-v, -vv, -vvv) |
| `--context` | current-context | CLI context name |

## Next steps

- [Configuration reference](configuration.md) — full cli.yaml documentation
- [Cookbook](cookbook.md) — 10 incident response recipes
- [Design spec](../wpcli-design.md) — architecture and rationale
