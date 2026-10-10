# wp CLI Configuration Reference

## File location

`wp` looks for `cli.yaml` in the following order (first found wins):

1. `$WOODPECKER_CLI_CONFIG` (environment variable)
2. `cli.yaml` beside the resolved `wp` executable
3. `$XDG_CONFIG_HOME/woodpecker/cli.yaml`
4. `~/.woodpecker/cli.yaml`

The current working directory is not searched. A toolkit directory can carry its own
cluster context beside the binary; verify the resolved source, context and endpoint
shown on stderr for text output. JSON/YAML output suppresses those diagnostics.

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
| `node_admin_urls` | map[string]string | empty | Original node identity/address to reachable admin HTTP origin |
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
| `--node-admin-url KEY=URL` (repeatable) | `contexts.<name>.node_admin_urls` | empty |
| `--admin-port` | `contexts.<name>.admin_port` | 9091 |
| `--timeout` | `contexts.<name>.timeout` | 30s |
| `--concurrency` | `contexts.<name>.concurrency` | 8 |
| `--strict` | `contexts.<name>.strict` | false |
| `--context` | `current-context` | active context |
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

### Running outside Kubernetes

The seed `endpoint` only bootstraps discovery. Subsequent node requests use
advertised identities from memberlist or metadata, which may contain pod FQDNs
that your workstation cannot resolve. Forward each node's admin port separately
and map its original address to the corresponding reachable HTTP origin:

```bash
# Run each forward in a separate terminal.
kubectl -n woodpecker port-forward pod/wp-0 19091:9091
kubectl -n woodpecker port-forward pod/wp-1 29091:9091
kubectl -n woodpecker port-forward pod/wp-2 39091:9091
```

```yaml
current-context: external
contexts:
  external:
    endpoint: http://127.0.0.1:19091
    node_admin_urls:
      "wp-0.wp-headless.woodpecker.svc.cluster.local:18080": http://127.0.0.1:19091
      "wp-1.wp-headless.woodpecker.svc.cluster.local:18080": http://127.0.0.1:29091
      "wp-2.wp-headless.woodpecker.svc.cluster.local:18080": http://127.0.0.1:39091
```

Use the actual advertised service addresses from your cluster. To override a
mapping for one invocation, repeat `--node-admin-url KEY=URL` as needed:

```bash
wp --context external \
  --node-admin-url 'wp-0.wp-headless.woodpecker.svc.cluster.local:18080=http://127.0.0.1:49091' \
  segment probe my-log 0 --etcd 127.0.0.1:2379
```

Flags merge into the context map, overriding the same key; the last repeated
value for a key wins. For each node the lookup order is exact service address,
node ID, exact gossip address, service hostname, then gossip hostname. Mapping
values must be full `http://` or `https://` origins, with optional ports and no
credentials, path, query or fragment. IPv6 origins use brackets.

All CLI admin requests to peers, including memberlist fan-out and metadata
quorum operations (LAC, probe, inspect, scan, skip-range inspection and fence),
use these mappings. Single-node commands also support mapped identities.
Mappings change only the connection destination: node identities, metadata,
quorum validation and command-specific partial-failure/strict behavior remain
unchanged. Text output reports configured mappings on stderr.

A historical quorum address missing from memberlist can still be contacted
when its original address or hostname has an explicit mapping. Without one it
remains an unknown node. A mapped external URL is not a quorum identity and
cannot be used to bypass fence target validation. Each mapping must reach the
specific replica, rather than a load balancer that can route to any node.

The seed endpoint and metadata `--etcd` endpoints must be independently
reachable; mappings do not rewrite them or SDK/gRPC traffic and do not create
port forwards automatically.

## Metadata connections are separate

`marking`, `log readers`, `log scan`, `segment probe/inspect`, `logstore lac/fence-quorum`,
and `log skip-range` accept metadata connection flags:

```bash
wp --context external log readers my-channel \
  --etcd 127.0.0.1:2379 --meta-prefix by-dev/woodpecker
```

Set the prefix to the embedding client's `etcd.rootPath` plus `woodpecker.meta.prefix`,
not an assumed server default. LogStore nodes do not connect to etcd, so their config's
etcd section is not validated for this purpose. An incorrect prefix can successfully
connect while showing an empty keyspace.

Giving both `--etcd` and `--meta-prefix` skips admin-config discovery. Pure metadata
commands (`log readers`, skip-range `list`/`remove`, and `marking`) then need no seed;
quorum operations still need a reachable seed and individually reachable replica admin
origins. TLS and authentication overrides are `--etcd-cert`, `--etcd-key`,
`--etcd-cacert`, `--etcd-tls-min-version`, `--etcd-username`, and `--etcd-password`.
Certificate paths must be available on the machine running `wp`, rather than copied
unexamined from a server-side path.

`--timeout` controls CLI request budgets (node drain commands have local wait timeouts).
It does not set application append/send/read deadlines or skip-range refresh latency.
See the [cookbook](cookbook.md) for the difference between those budgets.
