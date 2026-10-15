<p align="center"><img src="docs/images/logo.png" alt="Woodpecker" width="440"></p>

<h1 align="center">A Cloud-Native Write-Ahead Log Service</h1>

<p align="center">Ordered, durable logs built on object storage.<br>Deploy a replicated WAL service or embed the engine in your Go application.</p>

<p align="center">
  <a href="https://github.com/zilliztech/woodpecker/releases"><img src="https://img.shields.io/github/v/release/zilliztech/woodpecker?color=38d9f5" alt="Latest release"></a>
  <a href="https://zilliztech.github.io/woodpecker/docs.html"><img src="https://img.shields.io/badge/docs-online-8b7cff" alt="Documentation"></a>
</p>

<p align="center"><a href="https://zilliztech.github.io/woodpecker/">Website</a> · <a href="https://zilliztech.github.io/woodpecker/docs.html#quickstart">Quick Start</a> · <a href="https://zilliztech.github.io/woodpecker/docs.html#operations-cli">CLI Handbook</a> · <a href="https://zilliztech.github.io/woodpecker/docs.html#operations-scenarios">Troubleshooting</a> · <a href="https://github.com/zilliztech/woodpecker/releases">Releases</a></p>

Woodpecker provides ordered, durable logs for databases and stateful cloud applications. Service mode separates WAL processing from application compute: clients write to a quorum of LogStore nodes, active data is staged locally, and completed segments are compacted into object storage. Etcd provides metadata and coordination.

**Service mode is released, and Woodpecker is in an ongoing release cycle.** The next phase focuses on reliability, performance, operations, and observability. The current release is [v0.1.47](https://github.com/zilliztech/woodpecker/releases/tag/v0.1.47); check the release notes and integration requirements when selecting a version.

## Choose your deployment

| | Service mode | Embedded mode |
|---|---|---|
| Runs as | A dedicated LogStore cluster | A Go library inside your application |
| Write path | Quorum-based replication with local staging | In-process LogStore with the configured storage backend |
| Dependencies | LogStore nodes, etcd, object storage | Etcd and the configured storage backend |
| Fits | Applications that manage WAL infrastructure separately | Applications that prefer in-process integration |

<p align="center"><img src="docs/images/service_architecture.png" alt="Service architecture: clients access replicated LogStore nodes, with object storage and etcd" width="820"></p>

Read the [architecture guide](https://zilliztech.github.io/woodpecker/docs.html#architecture) for both modes and the [quorum guide](https://zilliztech.github.io/woodpecker/docs.html#quorum) for replication and placement.

## Get started

### Run a local service cluster

Install Git and Docker with the Compose plugin, then use the deployment template from a tagged release. This starts a local development environment with LogStore nodes, etcd, MinIO, and Jaeger; it uses local ports and development credentials.

```bash
git clone --branch v0.1.47 --depth 1 https://github.com/zilliztech/woodpecker.git
cd woodpecker/deployments
export WOODPECKER_IMAGE=docker.io/milvusdb/woodpecker:v0.1.46
docker compose up -d
docker compose ps
```

The example uses the v0.1.46 service image; v0.1.47 adds CLI and documentation updates. Select a published service image supporting your host architecture. Follow the [deployment guide](deployments/README.md) and [service quick start](https://zilliztech.github.io/woodpecker/docs.html#quickstart/service) for configuration and client connection details. Kubernetes deployment is also available through the [Woodpecker Operator](deployments/operator/README.md).

### Integrate the Go library

From your application's Go module:

```bash
go get github.com/zilliztech/woodpecker@v0.1.47
```

Use the Go version required by [go.mod](go.mod). The [SDK quick start](https://zilliztech.github.io/woodpecker/docs.html#quickstart/embedded) covers storage configuration, client creation, and log reads/writes. Embedded mode still requires etcd and its configured storage backend.

### Install the operational CLI

Download the binary for your OS and architecture from [Releases](https://github.com/zilliztech/woodpecker/releases/tag/v0.1.47). For example, on macOS with Apple Silicon:

```bash
mkdir -p "$HOME/.local/bin"
curl -fL https://github.com/zilliztech/woodpecker/releases/download/v0.1.47/wp-darwin-arm64 \
  -o "$HOME/.local/bin/wp"
chmod +x "$HOME/.local/bin/wp"
export PATH="$HOME/.local/bin:$PATH"
wp version
```

Linux binaries are `wp-linux-amd64` and `wp-linux-arm64`; Windows uses `wp-windows-amd64.exe`. To build from a checkout instead, run `make wpcli` and use `./bin/wp`. The [CLI setup guide](https://zilliztech.github.io/woodpecker/docs.html#operations-cli-local) explains `cli.yaml`, etcd connections, and node address mappings for access from outside Kubernetes.

## Operate and troubleshoot

| Goal | Tools and documentation |
|---|---|
| Check cluster and node health | `wp cluster health`, `wp node log-health` — [cluster status](https://zilliztech.github.io/woodpecker/docs.html#operations-cli-cluster) |
| Investigate slow or stalled work | `wp ops`, `wp metrics report`, `wp profile` — [runtime diagnostics](https://zilliztech.github.io/woodpecker/docs.html#operations-cli-ops) |
| Compare metadata and replica data | `wp log scan`, `wp segment inspect/probe`, `wp meta check` — [segment diagnosis](https://zilliztech.github.io/woodpecker/docs.html#operations-cli-segment) |
| Correct known metadata problems | `wp meta` preview/apply with revision checks, backup, and audit — [metadata repairs](https://zilliztech.github.io/woodpecker/docs.html#operations-cli-meta) |
| Manage read progress and retention | `wp log readers`, `wp log skip-range`, `wp log truncate` — [log operations](https://zilliztech.github.io/woodpecker/docs.html#operations-cli-log) |

Start from the symptom: [read stalls](https://zilliztech.github.io/woodpecker/docs.html#operations-scenarios-read), [write stalls](https://zilliztech.github.io/woodpecker/docs.html#operations-scenarios-write), or [data corruption and recovery](https://zilliztech.github.io/woodpecker/docs.html#operations-scenarios-corruption). Metadata consistency, physical data coverage, and application recovery are separate checks; each guide explains what its results establish.

Prometheus metrics and OpenTelemetry tracing support monitoring and diagnosis. See [monitoring](https://zilliztech.github.io/woodpecker/docs.html#monitoring) for configuration.

## Development direction

With the service architecture in place, development focuses on:

- **Reliability:** failure testing, recovery behavior, and lifecycle transitions.
- **Performance:** throughput, tail latency, and resource efficiency.
- **Operations:** diagnosis, controlled repairs, and maintenance workflows.
- **Observability:** actionable metrics, tracing, and runtime visibility.

See [open issues](https://github.com/zilliztech/woodpecker/issues) for ongoing work. Evaluate performance with the [benchmark tooling](tests/benchmark) using your own workload, hardware, durability policy, and deployment mode.

## Contribute

Bug reports, reproducible performance findings, documentation improvements, and pull requests are welcome. Check [existing issues](https://github.com/zilliztech/woodpecker/issues) before opening a new one, and see [build instructions](build/README.md) to get started.

## License

- **Server components** (`server/`): your choice of AGPLv3 or SSPLv1, as described in [server/LICENSE](server/LICENSE).
- **Other components:** [Apache License 2.0](APACHE-LICENSE-2.0.txt).

See [LICENSE.txt](LICENSE.txt) for details.
