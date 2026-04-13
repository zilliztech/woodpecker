# wp CLI Cookbook

10 common incident-response recipes for Woodpecker operators.

---

## 1. Decommission a node safely

```bash
# Initiate graceful decommission (blocks until safe)
wp node decommission node-3 --wait

# Or watch progress separately
wp node decommission node-3
wp node drain-status node-3 -w

# If you need to abort
wp node cancel-decommission node-3
```

## 2. Investigate a stuck flush

```bash
# Check for long-running ops
wp ops list node-1 --longer-than 30000

# Look at flush queue depth
wp logstore flush-queue node-1

# Check for stall signal (old evictions > 0)
wp ops stats node-1

# Run the automated scenario
wp metrics report node-1 --scenario stuck-flush --window 1m

# Drill into a specific segment
wp logstore segment-show node-1 --log 42 --seg 7
```

## 3. Compare config across nodes

```bash
# Quick drift check
wp config diff --all

# Compare specific nodes
wp config diff node-1 node-2

# Full config dump in JSON
wp config show node-1 -o json | jq .
```

## 4. Check for gossip split-brain

```bash
# Compare memberlist views across all nodes
wp cluster gossip-diff

# If drift is detected, check individual views
wp cluster info
wp node list
```

## 5. Download a CPU profile

```bash
# 30-second CPU profile
wp profile node-1 --type cpu --seconds 30 --output-file cpu.pb.gz

# Heap profile
wp profile node-1 --type heap --output-file heap.pb.gz

# Analyze with go tool pprof
go tool pprof cpu.pb.gz
```

## 6. Change log level for debugging

```bash
# Enable debug logging on a specific node
wp logging set-level node-1 --level debug

# ... reproduce the issue ...

# Check current level
wp logging get-level --all

# Restore to info
wp logging set-level node-1 --level info
```

## 7. Find the busiest node

```bash
# Rank nodes by operation count
wp metrics top --by woodpecker_server_logstore_operations_total

# Rank by active segments
wp metrics top --by woodpecker_server_logstore_active_segments

# Watch a specific metric in real time
wp metrics watch woodpecker_server_logstore_operations_total node-1
```

## 8. Check for under-replication

```bash
# Run automated scenario
wp metrics report --scenario under-replication --window 2m

# Also check
wp metrics report --scenario quorum-degraded --window 2m

# Manual check
wp cluster health
```

## 9. Scale a K8s cluster

```bash
# See what would happen (print mode)
wp k8s scale --replicas 5 --wp-cluster wp-prod -n woodpecker

# Execute
wp k8s scale --replicas 5 --wp-cluster wp-prod -n woodpecker -x

# Verify
wp k8s status --wp-cluster wp-prod -n woodpecker -x
```

## 10. Quick health check

```bash
# Traffic-light health check
wp cluster health

# Cluster topology
wp cluster info

# All segments across a node
wp logstore segments node-1

# Op registry state
wp ops stats node-1
```

Exit code interpretation:
- **0**: all green
- **8**: yellow finding (warning, investigate)
- **9**: red finding (critical, act now)
