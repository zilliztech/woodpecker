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

For access from outside Kubernetes, forwarding only the seed is insufficient for
multi-node operations. Forward each replica's admin port separately and configure
`node_admin_urls` in the context, or use repeated `--node-admin-url KEY=URL` flags.
See the [external access example](configuration.md#running-outside-kubernetes) before
running quorum diagnostics. Metadata commands also need independently reachable etcd.

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
wp node decommission node-3             # blocks until safe to terminate (default)
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

### Diagnose a reader or audit a log

```bash
# Use your application's actual etcd endpoint and metadata prefix.
wp log readers my-channel --etcd etcd-a:2379 --meta-prefix by-dev/woodpecker
wp log scan my-channel --mode quick --etcd etcd-a:2379 --meta-prefix by-dev/woodpecker
wp logstore lac my-channel 7 --etcd etcd-a:2379 --meta-prefix by-dev/woodpecker
wp segment probe my-channel 7 --from-entry 50 --max-entries 100 \
  --etcd etcd-a:2379 --meta-prefix by-dev/woodpecker
wp segment inspect my-channel 7 --max-blocks 4096 \
  --etcd etcd-a:2379 --meta-prefix by-dev/woodpecker
```

Compare reader positions after at least a 30s reporting interval. Quick scan checks
structure, not payload integrity or shared object-storage data. For verified unreadable
entries, see the [skip-range recovery workflow](cookbook.md#17-recover-a-stalled-reader-with-an-explicit-skip-declaration)
for evidence gates, preview/acceptance, delayed propagation and withdrawal. A skip declaration
gives up entries; it does not repair or truncate data. For deliberate write interruption,
use the [quorum fencing workflow](cookbook.md#15-diagnose-writes-that-stopped-advancing).

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

These are root flags; some commands define a local flag with a more specific meaning
(for example, decommission `--timeout` bounds its wait):

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint` | from cli.yaml | Admin HTTP seed endpoint |
| `--node-admin-url KEY=URL` | empty | Repeatable per-node admin origin override |
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
- [Cookbook](cookbook.md) — incident workflows from diagnosis through recovery
- [Design spec](../../wip/wpcli/wpcli-design.md) — architecture and rationale
- [v0.1.46 coverage review](release-0.1.46.md) — delivered features and retained boundaries
