# Core deployment and optional components

A new `deploy` installs **Milvus + three WP services + etcd/MinIO**. Grafana,
Prometheus, Alertmanager, Jaeger and Loki are all optional. Core-only rendering
does not emit monitoring CRDs or download the monitoring chart. Trace export is
`noop` until Jaeger is selected.

## Install the core first

```bash
python3 -m pip install -r scripts/requirements-connections.txt
./stack.sh start
./stack.sh images --local-arm64
./stack.sh deploy --local-arm64
./stack.sh forward --wpcli
# Another terminal:
./stack.sh smoke
./stack.sh perf
```

For a fresh public-image installation, omit `--local-arm64`. Core-only perf checks
write/query/search correctness and saves request/log evidence; absent optional
metrics and Alertmanager checks are explicitly recorded as **skipped**, not
healthy. A selected monitoring service that is unreachable still fails its checks.

The Jupyter full walkthrough deliberately selects `--with all` for its render,
images and deploy steps. Use the CLI above for a minimal deployment.

## Add components later

`--with` accepts comma-separated names or `all`. `addons-deploy` is additive:
previous components stay enabled, and live core image/resource/user overrides
are preserved. Adding Loki or Grafana does not rebuild or restart the core.
Changing whether Jaeger is selected changes the shared trace configuration and
rolls Milvus/WP Pods while retaining application PVC data.

| Component | Install/start command | Automatically included |
| --- | --- | --- |
| Grafana | `./stack.sh addons-deploy --with grafana` | Seven reference/E2E dashboards; datasources for selected services |
| Prometheus | `./stack.sh addons-deploy --with prometheus` | Prometheus Operator, infrastructure collectors, PodMonitors and reference WP rules |
| Alertmanager | `./stack.sh addons-deploy --with alertmanager` | Prometheus Operator; local-only receiver |
| Jaeger | `./stack.sh addons-deploy --with jaeger` | OTLP export configuration in Milvus and WP |
| Loki | `./stack.sh addons-deploy --with loki` | PVC-backed Loki and an Alloy collector DaemonSet |

Grafana can be installed without Prometheus or monitoring CRDs. Adding Prometheus
or Alertmanager later applies the pinned Operator CRDs before upgrading the
monitoring release (requires permission to manage these cluster-scoped CRDs). Alertmanager can run without
Prometheus, but needs a producer to send alerts; the example's WP alert flow uses
both. Jaeger and Loki can be queried directly without Grafana. Loki includes its
collector because a storage backend alone does not collect Pod logs.

On minikube, preload any new images with the same component selection before
installing them. For example:

```bash
./stack.sh images --local-arm64 --with loki
./stack.sh addons-deploy --with loki
# Add a UI, then metrics and alerts, at any later point:
./stack.sh addons-deploy --with grafana
./stack.sh addons-deploy --with prometheus,alertmanager
./stack.sh addons-deploy --with jaeger
./stack.sh addons-status
```

Or install the complete example initially:

```bash
./stack.sh images --local-arm64 --with all
./stack.sh deploy --local-arm64 --with all
```

`render`, `images` and `charts` use an **offline** selection: omitting `--with`
means core only. `deploy` reads the installed selection and preserves existing
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

## Pause, remove addons, or clear everything

```bash
# Pause/resume the entire local cluster; preserve persistent application/log data.
./stack.sh stop
./stack.sh start
# Remove only Loki/Alloy AND their collected log data; leave Milvus/WP alone.
./stack.sh addons-clear --with loki
# Add them again later (starts a new log history after clear).
./stack.sh addons-deploy --with loki
# Remove all optional components and their data, retaining the core.
./stack.sh addons-clear --with all
# Remove all stack releases AND all their PVC data, including Loki.
./stack.sh clear
```

`addons-clear` is destructive for the named addons' data. It removes their Pods,
updates Grafana's managed datasources, and deletes only their selected PVCs.
Removing Jaeger turns off the managed OTLP exporter before removing the collector.
Jaeger uses memory rather than a PVC, so restart/stop never guarantees retained
trace history. Shared monitoring CRDs and the namespace are not deleted.
