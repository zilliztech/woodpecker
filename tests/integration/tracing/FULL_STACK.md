# Real Milvus → Woodpecker trace acceptance

This is a bounded, explicitly opted-in test against an existing test deployment.
It sends real REST requests, exports real request-client spans, checks data
correctness, and retains actual Jaeger responses. It does not import fabricated
spans, provision Kubernetes, change the current context, install Helm releases,
or inject faults. Cluster provisioning and each Helm configuration change are
separate, visible operator steps.

The [validation receipt](FULL_STACK_VALIDATION.md) records the locally tested
image combination and its limits. These temporary images are **not published
release images**. The stack's existing default image is not upgraded by this test.
Issue #459 remains open for released, usable image/dependency combinations.

## Image prerequisites

Three independently versioned parts must agree:

1. Milvus must contain the per-message context propagation and WAL instrumentation
   from [milvus-io/milvus#50796](https://github.com/milvus-io/milvus/pull/50796).
   Milvus v3.0.3 does not contain that change. Inspect the selected source/image,
   rather than inferring compatibility from a tag's apparent age.
2. The **WP client embedded inside Milvus** must contain WP #479 (queued append
   context, batch links, gRPC stats handler) and this delivery's noop-provider fix.
   Otherwise constructing a default/noop WP client can replace Milvus's global
   OpenTelemetry provider and silently suppress its SN/client exports. A noop
   library client must inherit the host instrumentation without reconfiguring it.
3. WP service Pods need a compatible server image with tracing enabled. Updating
   those Pods does not update the client compiled into Milvus.

For a new build, use Milvus's documented build process with the native libraries
and headers matching that checkout. Select the WP revision in both Milvus's root
and `pkg` Go modules as appropriate for that revision. Rebuild the Milvus binary
and image, and record the dependency revision and runtime image ID. Do not mix
unrelated native libraries/headers to make a Go build link. The local validation
used a matched cached native build plus a Go-only upstream backport; it is a
development fixture, not a public release recipe.

The tested backend is Jaeger all-in-one **1.53.0**, with OTLP enabled. Test sampling
is 100%; batching, queue overflow, collector limits, eviction and restarts can
still produce incomplete traces. Export evidence before ephemeral history expires.

## Prepare an existing test deployment

Run from the repository root. Configure `KUBECONFIG` outside the stack and use an
explicit context and namespace in every command. Supply your own compatible image
tags below; they must already be pullable or loaded by the cluster administrator.
The fixture helper uses the stack's pinned PyYAML dependency.

```bash
make wpdiag
go build -o bin/trace-requests ./tests/integration/tracing/cmd/requests

kubectl --context TEST_CONTEXT create namespace trace-acceptance
kubectl --context TEST_CONTEXT label namespace trace-acceptance \
  diagnostics.woodpecker.io/trace-acceptance=459

python3 tests/integration/tracing/prepare_values.py \
  --milvus-image YOUR_MILVUS_REPOSITORY:YOUR_TAG \
  --wp-image YOUR_WP_REPOSITORY:YOUR_TAG \
  --profile single --output /tmp/trace-single.json

# Review the native Helm/kubectl plan first. Add -x to execute it.
deployments/milvus-full-stack-helm/stack.sh install \
  --context TEST_CONTEXT --namespace trace-acceptance --with jaeger \
  -f /tmp/trace-single.json
deployments/milvus-full-stack-helm/stack.sh install \
  --context TEST_CONTEXT --namespace trace-acceptance --with jaeger \
  -f /tmp/trace-single.json -x
```

The override preserves the base stack's shared `user.yaml`, sets explicit images,
and gives DataNode a 2 GiB limit for this development fixture. The stack enables
the trace exporter when `--with jaeger` is selected. Wait for the complete rollout,
including the WP StatefulSet, before starting acceptance. Selected Pods must be
Ready and keep the same UID, image ID and restart count throughout a scenario.

## Single-entry control

Without `--execute`, the runner prints an offline plan and creates no artifacts.
It does not even call `kubectl`.

```bash
python3 tests/integration/tracing/full_stack.py \
  --context TEST_CONTEXT --namespace trace-acceptance \
  --expect child --requests 4 --output /tmp/trace-evidence
# Add --execute to run the reviewed workload.
```

The runner owns three random loopback port-forwards for Milvus REST, Jaeger query
and OTLP/HTTP. It closes those processes on exit. The actual Go request client
creates one random `trace_accept_<uuid>` collection, inserts deterministic vectors,
verifies every acknowledged ID with a Strong query, verifies a vector search,
and drops only its own collection. Each call has an exported client span and a
W3C `traceparent`; there is no phantom unexported parent to confuse graph coverage.

Defaults are eight requests, one worker, 16 rows/request and eight dimensions.
Limits are 32 requests, eight workers, 64 rows/request and 128 dimensions. This is
an instrumentation acceptance test, not a throughput benchmark or latency SLO.
The worker has a two-minute workload deadline and 15-second HTTP deadlines.
The runner defaults to a 600-second execution budget, caps fetched trace sizes,
span counts, batch candidates and raw artifacts, and uses a separate bounded
cleanup allowance.

For non-default release/service names, use `--release`, `--jaeger-service` and
`--services path.json`. The JSON is an exact Jaeger service-name-to-role mapping,
for example `{"proxy":"proxy","streamingnode":"streamingnode",
"querynode":"querynode","milvus-woodpecker-0":"wp_server"}`. Include every
selected WP server; unmapped names do not become guessed roles.

## Coalesced batch control

Generate an override with the same image arguments and `--profile batch`. Apply
it with the same explicit `stack.sh install ... -f ... -x` command and wait for
rollout. Then run:

```bash
python3 tests/integration/tracing/full_stack.py \
  --context TEST_CONTEXT --namespace trace-acceptance --execute \
  --expect linked --requests 32 --concurrency 8 --output /tmp/trace-evidence
```

`single` sets `woodpecker.client.segmentAppend.maxBatchEntries=1`; `batch` sets
it to 1000. Concurrency can reproduce real coalescing but does not guarantee it:
linked mode fails if no exact asynchronous input link is retained.

For every Insert the runner requires either an observed parent path through
Proxy → actual SN WAL scope → embedded WP client → WP server, or an exact
`FOLLOWS_FROM(input_trace_id,input_span_id)` from a retained BatchAppendOp Execute
span whose children reach a WP server. A batch can serve several requests; it is
not attributed to one arbitrary request parent. Matching service names or nearby
timestamps are insufficient evidence.

The independent `wpdiag trace diagnose` and `wpdiag run --modules trace` must
produce equal finding semantics for the same request and retained batch trace.
Linked batch traces remain `partial_observed_path`; links are never promoted to
serial parents. A burst can also retain one verified direct branch while other
selected parents are missing. Linked mode records that minimum branch proof and
preserves the source's stricter partial whole-trace conclusion. Single-entry mode
requires the source's `observed_stages_connected` result. Neither result proves
that all spans were exported, all replica RPCs succeeded, or durability completed.

## Controlled admission delay and recovery

Use **only a disposable test deployment**. Generate `--profile admission-slow`
with the same image arguments, review the override, apply it explicitly, and
wait for rollout. It retains single-entry WP appends, sets SN's memory slowdown
threshold to 0.0001, adaptive slowdown HWM/LWM to 16kb/8kb and default burst to
128kb, with recovery threshold 0 and slowdown startup delay 0s. The recovery
threshold must be below the forced slowdown threshold; otherwise a sample can
immediately return to recovery. This exercises admission delay without allocating memory or filling disk.
These are test settings, not production recommendations.

```bash
python3 tests/integration/tracing/full_stack.py \
  --context TEST_CONTEXT --namespace trace-acceptance --execute \
  --expect child --requests 32 --concurrency 4 --rows 64 --dimension 32 \
  --output /tmp/trace-evidence
```

Check actual client latencies and `trace.slow_span` observations against the raw
span IDs. A slow observed span is evidence of elapsed time in that scope, not proof
of an exclusive queue or disk bottleneck. The fixture's intended cause also needs
the applied Helm values/runtime admission evidence; correlated elapsed durations
alone cannot establish it. If no delay is reproduced, record that coverage gap.
Restore the reviewed `single` override explicitly and repeat the normal control.
The harness does not inject or repair faults automatically.

## Evidence and failure handling

Each unique output directory contains `acceptance.json`, the workload receipt,
raw `*.trace.json`, standalone `*.source.json` and composed `*.integrated.json`.
It records runtime image IDs, Pod identities/restarts, CLI binary hashes, request
latencies, correctness, retained parent/link witnesses and coverage gaps. These
are local artifacts; `connections.private.json` and raw spans can expose internal
names or application attributes. Review and redact them before sharing.

Failed workloads remain failures even when their spans can be diagnosed. Attempted
create/insert/query/search/drop trace IDs are retained and diagnosed when available;
missing traces remain explicit gaps. The Go worker handles SIGINT/SIGTERM with a
bounded, independent cleanup context. SIGKILL, backend unavailability or a cleanup
timeout can leave its collection behind. Read its exact owned name from
`workload.json`, confirm it belongs to this run, and remove only that collection
after service recovery. Preserve the original failure receipt and record manual
recovery separately.

`--expect partial` is a negative control for earlier/incompatible write builds:
write association must remain partial and must not acquire fabricated ancestry.
Strong query/search correctness and supported read association are still required.
Do not use negative-control success as evidence that an image supports complete
write tracing.

Trace coverage cannot establish client metadata authority, crash durability,
individual file Write/Sync latency, lossless export, or an additive critical path.
Use metrics, runtime evidence and logs to corroborate a suspected cause. Do not
add/subtract aggregate quantiles or substitute missing spans with zero latency.
