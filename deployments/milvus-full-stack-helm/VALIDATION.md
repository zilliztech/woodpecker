# Foundation acceptance — 2026-10-25

This receipt covers the deployment foundation in PR #448. Automatic diagnosis
belongs to [#450](https://github.com/zilliztech/woodpecker/issues/450) and its
children; it is not a requirement added to this deployment PR.

## Environment

- ARM64 Kubernetes v1.33.1, prepared externally with minikube's Docker driver,
  four CPUs and 6 GiB allocated memory. The wrapper did not manage Kubernetes.
- Separate kubeconfig/context `wp-stack-448-acceptance` and disposable namespace
  `stack-448-acceptance`. The existing `wp-ops-v0146` remained stopped and the
  default kubeconfig's context selection was unchanged.
- Public `milvusdb/milvus:v3.0.3` (multi-platform repository digest
  `sha256:08747249b27c64f158db79a53ba008f89314ee240c731e585a7ec4fb7140731e`),
  three `milvusdb/woodpecker:v0.1.46` services and the pinned versions listed in
  [README.md](README.md#components-and-versions). No private Milvus image override.
- Official Milvus chart 5.0.31 and kube-prometheus-stack 67.4.0.

## Live checks

| Check | Observed result |
| --- | --- |
| Default `install` preview | A native-command guard was configured to reject writes; preview completed with no mutation attempt. Namespace metadata unchanged. |
| Core-only `install -x` | Milvus + three WP services + persistent etcd/MinIO ready; no optional observability workloads. Ten inserted vectors passed Strong query and nearest-vector search. WP server append counters were positive. |
| Grafana-only addon | Grafana ready; all original core Pod UIDs unchanged; no Operator monitoring CRDs installed. |
| Later Prometheus/AM/Jaeger/Loki | Operator CRDs installed after Grafana; all components ready. Enabling Jaeger rolled core trace configuration while retaining PVC UIDs and the original collection. |
| Dashboards | All seven provisioned dashboards, all 861 visible PromQL expressions and all 21 required E2E stage groups passed. Optional idle/version-specific gaps remain visible. |
| Logs | A real smoke collection marker was found in Proxy Pod logs and then in Loki through Alloy. No synthetic Loki push. |
| Traces | Exact tagged requests exported; Insert coverage remained `partial`, while query/search covered their expected services. No complete E2E write-trace claim. |
| Alerts | All 31 reference alerts/five recording rules loaded; offline behavior tests passed. Controlled probe fired in Prometheus, reached Alertmanager and resolved. Inhibition matched related alerts while leaving unrelated nodes visible. Local-only receiver; no email sent. |
| Bounded perf | Five seconds at two requests/second, ten rows/request: 100 acknowledged rows, zero failed requests, Strong-read/search correctness passed. Schema v2 retained raw evidence and collection gaps without WP threshold attribution. |
| Stop interrupted and retried | External test wrapper failed one native scale after several workloads had stopped. Command exited nonzero; saved original replica snapshot survived the retry. |
| Repeated stop | All stack Pods stopped, including Operator-owned Prometheus/AM and Alloy. Repeating stop retained the original snapshot. Kubernetes and unrelated resources remained. |
| Start | Exact original workload identities and replica counts restored, including Grafana's original zero. Operator CRs and Alloy scheduling restored. All PVC UIDs unchanged; Strong count of the original ten rows still correct. |
| Selected Loki uninstall | Loki/Alloy removed while core Pod UIDs and queryable data remained unchanged. |
| Full uninstall | Preview made no mutation attempt. Explicit uninstall removed stack releases/PVCs, retaining the namespace and unrelated zero-replica Deployment, ConfigMap and PVC. |

## Repeating the functional path

Prepare a disposable Kubernetes environment, image access and kubeconfig outside
the stack. Select it explicitly; do not use important data for uninstall checks.

```bash
export KUBECONFIG=/absolute/path/to/test-kubeconfig
export CONTEXT=YOUR_TEST_CONTEXT NAMESPACE=YOUR_DISPOSABLE_NAMESPACE
./stack.sh install                         # Inspect the native plan
./stack.sh install -x
# Separate terminal with the same environment:
./stack.sh forward --wpcli -x
# Back in the first terminal:
BATCHES=1 BATCH_SIZE=10 INTERVAL=0 ./stack.sh smoke -x
./stack.sh addons-install --with grafana -x
./stack.sh addons-install --with prometheus,alertmanager,jaeger,loki -x
# Reopen forwards after component additions or workload replacement.
./stack.sh forward --wpcli -x
```

In another terminal with those forwards running:

```bash
BATCHES=1 BATCH_SIZE=10 INTERVAL=0 ./stack.sh acceptance -x
PERF_DURATION=5 PERF_WARMUP=0 PERF_COOLDOWN=35 PERF_RPS=2 \
  PERF_BATCH_SIZE=10 PERF_CONCURRENCY=1 ./stack.sh perf -x
./stack.sh stop                            # Preview first
./stack.sh stop -x
./stack.sh stop -x                         # Preserve the original snapshot
./stack.sh start -x
# Reopen forwards and query the original retained collection before cleanup.
./stack.sh addons-uninstall --with loki -x
./stack.sh uninstall                       # Inspect destructive plan
./stack.sh uninstall -x                    # Deletes stack PVC data
```

The live acceptance additionally compared resource UIDs/counts, used unrelated
sentinels, exercised an original zero replica and injected a single command
failure outside the stack. Raw command logs, query coverage, workload reports and
native-command traces were retained under `.local/acceptance/`, `.local/perf/`
and `.local/evidence/`; local evidence and credentials are excluded from Git.

## Offline and website checks

All 99 Python tests passed, including the new high-WP-metric/no-attribution and
post-flush correctness regressions. ShellCheck with sourced files, support-chart
Helm lint/render and the pinned Promtool behavior suite passed. Browser checks
covered six stack pages, 66 internal links, light/dark layouts at desktop/mobile
widths, clipboard, navigation and existing Operations/Quick Start routes.

## Fix found and limits

The first tiny workload did not trigger automatic flush, so the required
`milvus_storage_op_count` series was absent. The smoke now explicitly flushes,
then checks the complete row count and nearest-vector search before reporting
success. Re-running acceptance populated the persistence stage without relaxing
the required-metric gate. Regression tests reject a failed flush or missing rows.

This is functional lifecycle/observability acceptance, not a capacity benchmark
or crash-durability test. The five-second perf run is too small for reliable tail
latency conclusions. Its missing finite mutation-latency/object-store-error
signals were retained as coverage gaps. Jaeger memory is lost on stop/restart;
no collector-outage/rotation loss, retention-aging, SMTP delivery or HA guarantee
is established. Earlier private-image performance receipts describe their own
configuration and do not establish public-image capacity.
