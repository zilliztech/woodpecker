# Core deployment and optional components

A new `install` installs **Milvus + three WP services + etcd/MinIO**. Grafana,
Prometheus, Alertmanager, Jaeger and Loki are all optional. Core-only rendering
does not emit monitoring CRDs or download the monitoring chart. Trace export is
`noop` until Jaeger is selected.

The default is a command preview; add `-x` to execute resource/workload changes.
The current kubeconfig context is used unless `--context` / `CONTEXT` is supplied.
Kubernetes itself is always managed externally.

## Install the core first

```bash
python3 -m pip install -r scripts/requirements-connections.txt
kubectl config current-context  # Configure and run Kubernetes separately.
./stack.sh images --local-arm64  # Image inventory; prepare node image access externally.
./stack.sh install --local-arm64 -x
./stack.sh forward --wpcli -x
# Another terminal:
./stack.sh smoke -x
./stack.sh perf -x
```

For a fresh public-image installation, omit `--local-arm64`. Core-only perf checks
write/query/search correctness and saves request/log evidence; absent optional
metrics and Alertmanager checks are explicitly recorded as **skipped**, not
healthy. A selected monitoring service that is unreachable still fails its checks.

The Jupyter full walkthrough deliberately selects `--with all` for its render,
images and install steps. Use the CLI above for a minimal deployment.

## Add components later

`--with` accepts comma-separated names or `all`. `addons-install` is additive:
previous components stay enabled, and live core image/resource/user overrides
are preserved. Adding Loki or Grafana does not rebuild or restart the core.
Changing whether Jaeger is selected changes the shared trace configuration and
rolls Milvus/WP Pods while retaining application PVC data.

| Component | Preview command (add `-x` to execute) | Automatically included |
| --- | --- | --- |
| Grafana | `./stack.sh addons-install --with grafana` | Seven reference/E2E dashboards; datasources for selected services |
| Prometheus | `./stack.sh addons-install --with prometheus` | Prometheus Operator, infrastructure collectors, PodMonitors and reference WP rules |
| Alertmanager | `./stack.sh addons-install --with alertmanager` | Prometheus Operator; local-only receiver |
| Jaeger | `./stack.sh addons-install --with jaeger` | OTLP export configuration in Milvus and WP |
| Loki | `./stack.sh addons-install --with loki` | PVC-backed Loki and an Alloy collector DaemonSet |

Grafana can be installed without Prometheus or monitoring CRDs. Adding Prometheus
or Alertmanager later applies the pinned Operator CRDs before upgrading the
monitoring release (requires permission to manage these cluster-scoped CRDs). Alertmanager can run without
Prometheus, but needs a producer to send alerts; the example's WP alert flow uses
both. Jaeger and Loki can be queried directly without Grafana. Loki includes its
collector because a storage backend alone does not collect Pod logs.

`images --with ...` lists the required images. Prepare registry access or node
caches outside this tool, then install the selected components. For example:

```bash
./stack.sh images --local-arm64 --with loki
./stack.sh addons-install --with loki -x
# Add a UI, then metrics and alerts, at any later point:
./stack.sh addons-install --with grafana -x
./stack.sh addons-install --with prometheus,alertmanager -x
./stack.sh addons-install --with jaeger -x
./stack.sh addons-status
```

Or install the complete example initially:

```bash
./stack.sh images --local-arm64 --with all
./stack.sh install --local-arm64 --with all -x
```

`render`, `images` and `charts` use an **offline** selection: omitting `--with`
means core only. `install` reads the installed selection and preserves existing
addons, so a routine core upgrade cannot accidentally remove them. Repeat core
override files such as `--local-arm64` on core upgrades. Addon commands reuse live
core overrides and need no image flag. Values files adjust component settings;
`--with` controls whether components are installed.

Plans are generated in isolated `.local/plans/plan-*/` directories. They contain
Helm values, potentially including local credentials, and are excluded from Git
and diagnostic collection. The selected component flags are stored in the
supporting Helm release, scoped to the explicit context and namespace. The first
upgrade from the earlier always-on example discovers its existing workloads.

## Open and validate the selected services

After adding components, run `./stack.sh forward --wpcli` again. It reuses matching
existing forwards and starts only the missing ones. A forward to a replaced Pod
may exit; restart forwarding after rollout. Ctrl-C only closes forwards owned by
that invocation. No endpoint is opened for an uninstalled component.

| Service | Default local address | Check |
| --- | --- | --- |
| Milvus | http://127.0.0.1:19530 | `./stack.sh smoke` |
| Grafana | http://127.0.0.1:3000 (`admin/admin`) | `./stack.sh verify` with Prometheus |
| Prometheus | http://127.0.0.1:9099 | `./stack.sh verify` (Grafana is optional) |
| Alertmanager | http://127.0.0.1:9093 | `./stack.sh alerts` with Prometheus |
| Jaeger | http://127.0.0.1:16686 | `./stack.sh traces-smoke` |
| Loki API | http://127.0.0.1:3100 | `./stack.sh logs-smoke` |

Use `connections` for actual host ports, `--json` for machine-readable inventory,
and `--wpcli-config` for the separate service context/metadata instructions.
`acceptance` checks the core plus selected optional components. An absent addon
is skipped; a selected but broken path fails its check. The tracing report still
records the supplied image's known write-chain gap; use `TRACES_REQUIRE_E2E=1`
to fail acceptance on that partial coverage too.

See [LOGGING.md](LOGGING.md), [TRACING.md](TRACING.md), [ALERTING.md](ALERTING.md)
and [E2E-DASHBOARDS.md](E2E-DASHBOARDS.md) for interpretation and focused steps.

## Pause, remove addons, or uninstall everything

```bash
# Pause/resume this stack’s workloads; preserve releases and PVC data.
./stack.sh stop -x
./stack.sh start -x
# Remove only Loki/Alloy AND their collected log data; leave Milvus/WP alone.
./stack.sh addons-uninstall --with loki -x
# Add them again later (starts a new log history after uninstall).
./stack.sh addons-install --with loki -x
# Remove all optional components and their data, retaining the core.
./stack.sh addons-uninstall --with all -x
# Remove all stack releases AND all their PVC data, including Loki.
./stack.sh uninstall -x
```

`addons-uninstall` is destructive for the named addons' data. It removes their Pods,
updates Grafana's managed datasources, and deletes only their selected PVCs.
Removing Jaeger turns off the managed OTLP exporter before removing the collector.
Jaeger uses memory rather than a PVC, so workload restart/stop does not retain
trace history. Shared monitoring CRDs, Kubernetes and the namespace are not deleted.

`stop` saves replica counts in the namespace ConfigMap `milvus-full-stack-lifecycle`
and uses native scale/patch/wait operations. `start` restores those exact counts,
including any previous zero, and removes the pause state after successful rollouts.
It never starts Kubernetes. Install/addon changes require resuming a paused stack first.
