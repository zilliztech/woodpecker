# Local tracing with Jaeger

When Jaeger is selected, the stack deploys `jaegertracing/all-in-one:1.53.0` and exports Milvus and WP
spans over OTLP/gRPC to `full-stack-jaeger:4317`. The official Milvus chart mounts
the shared `user.yaml` into both Milvus and the three WP servers. Grafana also
provisions a `Jaeger` datasource with UID `jaeger`.

```bash
./stack.sh images --local-arm64 --with jaeger,grafana
./stack.sh install --local-arm64 -x
./stack.sh addons-install --with jaeger,grafana -x
./stack.sh forward --wpcli -x
# In another terminal:
./stack.sh connections
./stack.sh traces
./stack.sh traces-smoke -x
```

Jaeger UI: http://127.0.0.1:16686. Use `JAEGER_PORT` for a different host port;
`connections` reports an observed existing forward when it differs. OTLP ports
4317/4318 stay inside Kubernetes; local workloads call Milvus, not the collector.
In Grafana, open Explore and select **Jaeger**. The datasource queries the Jaeger
service through Grafana, so it does not require a browser-side collector address.

`traces-smoke` writes 100 vectors to a new retained collection, verifies all 100
IDs with a Strong query and checks nearest-vector search. It injects a separate
W3C `traceparent` into each REST request and fetches those exact trace IDs from
Jaeger after allowing batch export. `.local/tracing-smoke.json` contains client
latency, trace IDs, clickable links, service coverage and missing exporters.
Raw per-request spans are saved under `.local/tracing-smoke-traces/` before the
ephemeral backend evicts them. The sampled external parent ID is a correlation
anchor, not an exported client span; this test does not export a Python SDK span.

## What is connected in the supplied image

The local ARM64 image at Milvus commit `27bd28d20688ab00cdd74232a20e988fb2162aed`
and WP v0.1.46 were exercised with real requests on 2026-10-22:

| Path | Observed coverage | Limit |
| --- | --- | --- |
| User Insert | REST/Proxy and coordinator spans under the supplied trace ID | The request trace does not continue into SN/WP in this image |
| Strong Query | Proxy, coordinator, SN-hosted query/delegator and segcore | Does not imply a WP historical read was needed |
| Vector Search | Proxy, SN-hosted query/delegator and segcore | Existing spans do not split every queue/wait |
| WP servers | All three nodes export real spans independently | Exporter presence does not establish connection to a user Insert |
| Background flush and other async work | Separate spans can be inspected by service/time | Not all background tasks retain user-request context |

The verification result is **partial** when requests export spans but a required
service is missing from the same trace. It never treats five service names in the
service list as proof of an E2E write trace. Default exit status checks export and
functional correctness; strict E2E mode also fails on a coverage gap:

```bash
# Expected to fail for the observed write gap in the supplied image.
TRACES_REQUIRE_E2E=1 ./stack.sh traces-smoke -x
# Standalone equivalent:
python3 scripts/check-traces.py --smoke --require-e2e --output .local/tracing-strict.json
```

The supplied historical binaries contain async work that creates a fresh root context;
for example WP `AppendOp.Execute` uses `logger.NewIntentCtx`, which starts from
`context.Background()`. The complete write-chain break needs source-level context
and span-link investigation, including the Milvus producer/SN boundary. Installing
a collector cannot repair that propagation or add missing instrumentation. Do not
attribute all unexpanded Proxy time to one downstream operation.

For source builds with the propagation fixes, use the separate
[real full-stack acceptance guide](../../tests/integration/tracing/FULL_STACK.md)
and its [versioned local receipt](../../tests/integration/tracing/FULL_STACK_VALIDATION.md).
It exports an actual request-client span, verifies retained parent/link identities,
and compares standalone/composed `wpdiag` results. This is a stronger causal check
than the small smoke test's service coverage. It also checks the embedded WP
client's provider ownership: a default/noop client must not suppress Milvus spans.
The temporary validated images do not change the stack's default image tags or
make an older build's write coverage complete. Async batch links remain partial
serial coverage even when their exact request association is verified.

## Follow a slow request

1. Select the matching workload window in the E2E metrics dashboard.
2. Open `requests.jsonl` from a normal `stack.sh perf` run. Each insert record now
   includes `trace_id`, `trace_sampled` and, when sampled, `trace_url`. The summary
   links the five slowest sampled requests. A recorded ID does not guarantee the
   trace was exported or is still retained.
3. Open that ID in Jaeger or paste it into Grafana Explore with datasource Jaeger.
   Inspect the request's actual spans and events, preserving parent/child scope.
4. For a disconnected path, inspect WP/SN services in the same time window and
   correlate logs using trace ID, log/segment/entry identity. This is supporting
   evidence, not a verified automatic join to the user request.

Span durations can nest and run in parallel. They are wall-clock durations,
including waits inside their scope; do not add nested spans or subtract unrelated
P99 values. The current flush span still does not independently time encoding,
file Write and file Sync. Metrics, logs and traces remain complementary evidence.

## Sampling, retention and performance comparisons

Core-only defaults use exporter `noop`. Selecting Jaeger generates the following
shared Milvus/WP configuration (full sampling for the local lab):

```yaml
trace:
  exporter: otlp
  sampleFraction: 1.0
  otlp:
    endpoint: full-stack-jaeger:4317
    method: grpc
    secure: false
```

Full sampling is deliberate for small diagnostic runs. It consumes CPU, memory
and network and changes the observed system. For sustained load, reduce
`sampleFraction` in an additional Milvus values file containing the shared user
configuration, and repeat it on core deployments. The perf client
also makes a sampling decision through `PERF_TRACE_SAMPLE_FRACTION` (0..1,
default 1), carried in the W3C sampled flag. Parent-based samplers may honor this
upstream decision, so change both settings when reducing trace volume. The small
`traces-smoke` probe explicitly requests sampling to verify propagation.

```bash
PERF_TRACE_SAMPLE_FRACTION=0.1 ./stack.sh perf -x
```

For a tracing-off baseline, run `./stack.sh addons-uninstall --with jaeger` to disable
the managed exporter and remove the ephemeral collector,
and run perf with `PERF_TRACE_SAMPLE_FRACTION=0`. Hold the other parameters constant.
Disabling client sampling alone does not disable all independently rooted
background spans. This stack does not claim a measured tracing-overhead budget.

Jaeger uses bounded in-memory storage (`jaeger.maxTraces: 10000`), not a PVC.
This is a trace-count limit, not a span-count limit or a fixed memory-byte budget.
Restarts and trace eviction remove history; export JSON evidence while it exists.
`stop` preserves application PVC data but does not guarantee Jaeger memory survives
resume. `uninstall` removes the Jaeger Deployment with the supporting Helm release.
Adjust memory/trace limits for larger experiments. Jaeger's local UI is reachable
through loopback port-forward; the example does not expose it through a public
LoadBalancer or Ingress.

## Troubleshooting

- No services: verify collector readiness, shared `trace.exporter`, sampling,
  endpoint/TLS settings and exporter errors in Pod logs.
- Missing trace ID: check the sampled flag, propagation, batch-export delay and
  eviction. `traces-smoke` checks explicit IDs rather than recent background spans.
- Only Proxy in an Insert trace: inspect `coverage_gaps`; this is an observed
  propagation/instrumentation limit of the supplied image, not proof that SN/WP
  work took zero time.
- No dedicated QueryNode span: the reference topology can execute growing-data
  queries on SN's co-located query/delegator role.
- Jaeger restarts under heavy tracing: inspect memory use/limits and lower
  sampling; an in-memory collector shares this small minikube host with the stack.

References: [Jaeger v1.53.0](https://github.com/jaegertracing/jaeger/tree/v1.53.0),
[Milvus trace configuration](https://milvus.io/docs/configure_trace.md), and
this repository's `common/tracer/tracer.go` and gRPC tracing interceptors.

## What Jaeger stores and when spans can be lost

A span stores an operation name, trace/span/parent IDs, start time and duration,
service/resource attributes, status and recorded events. Related spans form a
trace. Jaeger does not store the application's vector dataset or replace Loki's
raw container logs. An event on a span is also not the entire application log.

The current path is application OpenTelemetry batch exporter → OTLP → Jaeger
collector queue/workers → in-memory trace storage → Jaeger query/UI. There is
no durable queue or external trace database in this local example. External
Jaeger storage backends are a separate deployment choice.

| Situation | Consequence | How to distinguish it |
| --- | --- | --- |
| Sampling excludes a request | Its spans are intentionally not exported | Check server/client sampling and the W3C sampled flag |
| Application exporter queue fills, export fails or the process exits before flushing | Some completed spans may never reach Jaeger | Inspect SDK/export errors and process termination; collector metrics cannot see spans it never received |
| Collector queue fills or storage cannot keep up | Spans can be refused/dropped or exports can ultimately fail | Inspect collector queue length, queue latency, dropped spans, save latency/errors and Pod resources |
| In-memory history reaches its trace limit | Older retained traces are evicted | Different from ingestion loss; export evidence while it remains available |
| Jaeger Pod restarts | In-memory history disappears | Check Pod restart/rollout history; no PVC is configured |
| Context propagation or instrumentation is missing | Child work may be a separate trace or have no span | Inspect parent IDs and known coverage gaps; absence does not prove queue loss |

**Full sampling does not guarantee complete traces.** The SDK's batch queue is
bounded and normally nonblocking; sustained export/storage overload cannot be
solved by increasing queues indefinitely. Lower sampling or improve collector/
storage capacity before relying on trace coverage during heavy load. A durable
intermediate queue/external storage can improve resilience but is not installed
by this local profile.

With optional Prometheus, the Jaeger admin metrics endpoint is scraped through
a PodMonitor. Inspect the actual exported `jaeger_collector_*` queue, dropped-span
and storage metrics; compare them over the workload window, not just their current
values. Collector zero-drop counters alone do not prove the SDK exported every
span. No span-loss fault injection or no-loss guarantee is claimed here.

The pinned 1.53 image exposes these useful series (scope queries to your namespace):

```promql
jaeger_collector_queue_length{namespace="milvus-stack"}
  / jaeger_collector_queue_capacity{namespace="milvus-stack"}
increase(jaeger_collector_spans_dropped_total{namespace="milvus-stack"}[5m])
histogram_quantile(0.99, sum by (le) (
  rate(jaeger_collector_save_latency_bucket{namespace="milvus-stack"}[5m])
))
```

The local post-install snapshot showed queue length 0, capacity 2000 and collector
dropped spans 0. This is an observation at that time, not a stress-test result or
proof that application-side export lost no spans.

Sources: [Jaeger architecture](https://www.jaegertracing.io/docs/1.76/architecture/),
[collector queues and slow storage](https://www.jaegertracing.io/docs/1.53/performance-tuning/),
[OpenTelemetry Go batch queue behavior](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/trace#WithMaxQueueSize).
