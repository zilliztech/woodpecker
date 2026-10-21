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

`wp shell` lists configured contexts when `--context` is omitted, even when
`current-context` is set. The current context is the Enter default; choosing a
context affects only the shell session. Tenant metadata is explicitly attached
with `connect`, independently of stored service context etcd settings. See the
[shell guide](shell.md) for its session-specific connection behavior.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | (required for admin commands) | Admin HTTP seed endpoint; raw etcd commands do not need it |
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

## Etcd connections

Store etcd settings under the selected context. Use host-reachable endpoints and
certificate files local to the machine running wp:

```yaml
current-context: external
contexts:
  external:
    endpoint: http://127.0.0.1:19091
    etcd:
      endpoints: [127.0.0.1:32379]
      meta_prefix: milvus-prod/woodpecker
      # Add username/password if the deployed etcd requires authentication.
      # username: operator
      # password: YOUR_PASSWORD
      # For TLS, set these to actual local files; cert/key are needed only for mTLS.
      # tls_enabled: true
      # cacert: /path/to/ca.pem
      # cert: /path/to/client.pem
      # key: /path/to/client-key.pem
      # tls_min_version: "1.3"
```

The metadata prefix is the full deployed namespace, for example a Milvus root
path followed by `woodpecker`; it is independent of the data bucket/root path.

| Context field | CLI override | Meaning |
|---|---|---|
| `etcd.endpoints` | `--etcd` | YAML list of endpoints; flag accepts a comma-separated list |
| `etcd.meta_prefix` | `--meta-prefix` | Full Woodpecker metadata namespace |
| `etcd.username` / `etcd.password` | `--etcd-username` / `--etcd-password` | Etcd authentication |
| `etcd.cacert` | `--etcd-cacert` | Local PEM trust bundle; omitted uses system roots for TLS |
| `etcd.cert` / `etcd.key` | `--etcd-cert` / `--etcd-key` | Local client certificate/private key pair for mutual TLS |
| `etcd.tls_min_version` | `--etcd-tls-min-version` | Minimum TLS version; default 1.2 |
| `etcd.tls_enabled` | — | Explicit TLS configuration switch; absent infers TLS from certificate settings |

Connection fields resolve as **flags → context → admin discovery**. Explicit
empty flags can clear optional settings, for example `--etcd-password=`. A
certificate flag enables TLS even when the context's tls_enabled is false.
Cert/key must be supplied together; unreadable or invalid local TLS files are
configuration errors. An invalid context name is an error.

These settings apply to `marking`, `log readers/skip-range/scan`, `segment
inspect/probe`, and `logstore lac/fence-quorum`. With both endpoints and
meta_prefix supplied by flags/context, the metadata connection does not need
admin-config discovery. Commands that also query quorum members still need
their reachable admin endpoints and mappings.

`wp etcd list/get/cat` use the same connection fields but **always skip admin
discovery** and do not require meta_prefix. Input keys/prefixes are literal;
meta_prefix is never prepended. An etcd-only context can therefore omit the
admin endpoint entirely:

```yaml
current-context: raw-etcd
contexts:
  raw-etcd:
    etcd:
      endpoints: [127.0.0.1:32379]
```

```bash
wp --context raw-etcd etcd list
```

See the [raw etcd guide](../../wp-etcd-cli.md) for value encodings and
revision-pinned pagination.

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
