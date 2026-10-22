# Local tracing with Jaeger

The stack deploys `jaegertracing/all-in-one:1.53.0` and exports Milvus and WP
spans over OTLP/gRPC to `full-stack-jaeger:4317`. The official Milvus chart mounts
the shared `user.yaml` into both Milvus and the three WP servers. Grafana also
provisions a `Jaeger` datasource with UID `jaeger`.

```bash
./stack.sh images --local-arm64
./stack.sh deploy --local-arm64
./stack.sh forward --wpcli
# In another terminal:
./stack.sh connections
./stack.sh traces
./stack.sh traces-smoke
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
TRACES_REQUIRE_E2E=1 ./stack.sh traces-smoke
# Standalone equivalent:
python3 scripts/check-traces.py --smoke --require-e2e --output .local/tracing-strict.json
```

The existing binaries contain async work that creates a fresh root context;
for example WP `AppendOp.Execute` uses `logger.NewIntentCtx`, which starts from
`context.Background()`. The complete write-chain break needs source-level context
and span-link investigation, including the Milvus producer/SN boundary. Installing
a collector cannot repair that propagation or add missing instrumentation. Do not
attribute all unexpanded Proxy time to one downstream operation.

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

Local defaults in `values/milvus.yaml` are:

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
`sampleFraction` in the shared user configuration and redeploy. The perf client
also makes a sampling decision through `PERF_TRACE_SAMPLE_FRACTION` (0..1,
default 1), carried in the W3C sampled flag. Parent-based samplers may honor this
upstream decision, so change both settings when reducing trace volume. The small
`traces-smoke` probe explicitly requests sampling to verify propagation.

```bash
PERF_TRACE_SAMPLE_FRACTION=0.1 ./stack.sh perf
```

For a tracing-off baseline, set the shared exporter to `noop`, redeploy Milvus/WP,
and run perf with `PERF_TRACE_SAMPLE_FRACTION=0`. Hold the other parameters constant.
Disabling client sampling alone does not disable all independently rooted
background spans. This stack does not claim a measured tracing-overhead budget.

Jaeger uses bounded in-memory storage (`jaeger.maxTraces: 10000`), not a PVC.
Restarts and trace eviction remove history; export JSON evidence while it exists.
`stop` preserves application PVC data but does not guarantee Jaeger memory survives
resume. `clear` removes the Jaeger Deployment with the supporting Helm release.
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
