# wp CLI Configuration Reference

## File location

`wp` looks for `cli.yaml` in the following order (first found wins):

1. `$WOODPECKER_CLI_CONFIG` (environment variable)
2. `$XDG_CONFIG_HOME/woodpecker/cli.yaml`
3. `~/.woodpecker/cli.yaml`

## Structure

```yaml
# The context to use when --context is not specified.
current-context: prod

# Named cluster contexts.
contexts:
  prod:
    # Admin HTTP endpoint of any cluster node (seed for discovery).
    endpoint: http://wp-prod-1.internal:9091

    # Admin port used for fan-out peer discovery (default: 9091).
    admin_port: 9091

    # Per-request timeout (default: 30s).
    timeout: 60s

    # Maximum concurrent fan-out requests (default: 8).
    concurrency: 16

    # Treat partial fan-out failures as errors (default: false).
    strict: false

    # Kubernetes integration (optional).
    k8s:
      # Kubernetes namespace.
      namespace: woodpecker

      # WoodpeckerCluster CR name.
      cluster: wp-prod

      # kubectl context (not wp context).
      kube_context: prod-cluster

      # Path to kubeconfig file.
      kubeconfig: /etc/kube/config

      # Path to kubectl binary (default: $PATH lookup).
      kubectl: /usr/local/bin/kubectl

  staging:
    endpoint: http://wp-staging-1.internal:9091
    k8s:
      namespace: woodpecker-staging
      cluster: wp-staging

# Global defaults (optional).
defaults:
  # Default output format: table, wide, json, yaml.
  output: table

  # Disable color output.
  no_color: false

  # Default page size for paginated output.
  page_size: 50
```

## Context fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | (required) | Admin HTTP seed endpoint |
| `admin_port` | int | 9091 | Admin port for peer discovery |
| `timeout` | duration | 30s | Per-request timeout |
| `concurrency` | int | 8 | Fan-out concurrency |
| `strict` | bool | false | Strict fan-out mode |
| `k8s.namespace` | string | | Kubernetes namespace |
| `k8s.cluster` | string | woodpecker | WoodpeckerCluster CR name |
| `k8s.kube_context` | string | | kubectl context name |
| `k8s.kubeconfig` | string | | Path to kubeconfig |
| `k8s.kubectl` | string | kubectl | Path to kubectl binary |

## Precedence

Values are resolved in this order (highest priority first):

1. **Command-line flags** (`--endpoint`, `--timeout`, etc.)
2. **Environment variables** — `$WOODPECKER_ENDPOINT` (the admin endpoint;
   overrides the cli.yaml context); `$WOODPECKER_CLI_CONFIG` (selects which
   `cli.yaml` to load)
3. **cli.yaml context** (the active context)
4. **Hardcoded defaults** (admin_port=9091, timeout=30s, etc.)

> Server images set `WOODPECKER_ENDPOINT=http://localhost:9091`, so `wp` runs
> zero-config inside a pod. Because env beats the cli.yaml context, this also
> overrides a `cli.yaml` mounted into the pod — pass `--endpoint` to target a
> different cluster from in-pod.

## Flag-to-config mapping

| Flag | cli.yaml field | Default |
|------|---------------|---------|
| `--endpoint` | `contexts.<name>.endpoint` | (required) |
| `--admin-port` | `contexts.<name>.admin_port` | 9091 |
| `--timeout` | `contexts.<name>.timeout` | 30s |
| `--concurrency` | `contexts.<name>.concurrency` | 8 |
| `--strict` | `contexts.<name>.strict` | false |
| `--context` | `current-context` | (first context) |
| `-o, --output` | `defaults.output` | table |
| `--no-color` | `defaults.no_color` | false |
| `-n, --namespace` | `contexts.<name>.k8s.namespace` | |
| `--wp-cluster` | `contexts.<name>.k8s.cluster` | woodpecker |
| `--kube-context` | `contexts.<name>.k8s.kube_context` | |
| `--kubeconfig` | `contexts.<name>.k8s.kubeconfig` | |
| `--kubectl` | `contexts.<name>.k8s.kubectl` | kubectl |

## Examples

### Single-node development
```yaml
current-context: dev
contexts:
  dev:
    endpoint: http://localhost:9091
```

### Multi-cluster production
```yaml
current-context: prod-us
contexts:
  prod-us:
    endpoint: http://wp-us-1.internal:9091
    concurrency: 32
    timeout: 60s
  prod-eu:
    endpoint: http://wp-eu-1.internal:9091
    concurrency: 32
  staging:
    endpoint: http://wp-staging.internal:9091
    strict: true
```

### K8s-enabled
```yaml
current-context: k8s-prod
contexts:
  k8s-prod:
    endpoint: http://wp-prod-headless.woodpecker:9091
    k8s:
      namespace: woodpecker
      cluster: wp-prod
      kube_context: prod-gke
```
