# Write trace propagation — #459

The first delivery (#479) fixes the Woodpecker queued-append boundary and supplies
a real exporter/transport compatibility probe. The second delivery adds an
opted-in [real Milvus/WP acceptance runner](FULL_STACK.md), fixes a verified
noop-client override of the host's OpenTelemetry provider, and recognizes the
actual Milvus WAL scopes. It **does not close #459**: the locally validated
development images are not published supported release images. See the
[full-stack receipt](FULL_STACK_VALIDATION.md) for actual controls and limits.

The next delivery adds [read-only source/image prerequisites](RELEASE_COMPATIBILITY.md)
and a [dated published-version snapshot](RELEASE_CHECK_VALIDATION.md). It records
both Milvus module declarations, separate client/server requirements and exact
platform digests without operating a cluster or downloading layers. The checked
public candidates remain unqualified; metadata inspection does not complete #459.

## Verified boundaries and versions

| Boundary / version | Evidence | Ownership / result |
| --- | --- | --- |
| Milvus v3.0.3, commit `328df0cd2f` | `produceGrpcClient.SendProduceMessage` sends payload/properties without per-message context injection. `ProduceServer.handleProduce` calls `wal.AppendAsync(p.produceServer.Context(), ...)`, using the long-lived stream context instead of a per-message extracted context. `pkg/streaming/util/message/trace.go` is absent in this tag. | That build cannot connect the user Insert to WAL by merely installing Jaeger. Keep old-image results partial. |
| Private stack Milvus `27bd28d20688ab00cdd74232a20e988fb2162aed` / WP v0.1.46 | Retained 2026-10-22 tagged Insert `db840433644b4186b0863ff641213378` contains Proxy/mixture spans without connected SN/WP spans. Independent WP append/flush traces were retained separately. | Historical evidence, not a new reproduction or evidence from the current image. The [public-image acceptance receipt](../../../deployments/milvus-full-stack-helm/VALIDATION.md) likewise records partial Insert coverage. |
| Current Milvus source containing [milvus-io/milvus#50796](https://github.com/milvus-io/milvus/pull/50796), commit `5eebaa9ad4ee5eadd82071cecc1b839df9cf1441` | Per-message `_tc` serialization/extraction, logical/distributed/adaptor WAL spans and focused tests are already upstream. The WP WAL adaptor forwards the append context to `LogWriter.Write`. | Reuse this upstream implementation; do not duplicate it or claim that v3.0.3 contains it. A verified image/dependency combination is still required. |
| WP baseline `9cc60d40c9de3907bada2829e44ca891446028ed`, queue → replica RPC | `segmentHandleImpl.createPendingAppendOp` did not retain the caller's trace identity. `AppendOp.Execute` and `BatchAppendOp.Execute` used `logger.NewIntentCtx`, starting from `context.Background()`. | Fixed here. A propagated context reaching WP could still be detached at this independent boundary. |
| WP client `232633c`, initialization inside Milvus | Default `trace.exporter=noop` still installed a process-global SDK provider without an exporter, replacing Milvus's provider and propagator. Real Insert spans reached WP servers while their SN/client parents were not exported. | Fixed by this delivery (local image built at `e26a7e5` before rebasing): noop initialization leaves the host provider/propagator and the later enabled-initialization opportunity untouched. Both client constructors have regressions. |

Relevant Milvus sources:
[v3.0.3 producer](https://github.com/milvus-io/milvus/blob/v3.0.3/internal/streamingnode/client/handler/producer/produce_grpc_client.go),
[v3.0.3 receiver](https://github.com/milvus-io/milvus/blob/v3.0.3/internal/streamingnode/server/service/handler/producer/produce_server.go),
[upstream propagation implementation](https://github.com/milvus-io/milvus/pull/50796).
The first delivery did not start or alter the existing Kubernetes environments.
The second used an externally provisioned, isolated `wp-trace-459` context; it did
not change the default context or operate the other profiles.

The real exporter probe also reproduced an independent transport gap: the
legacy `otelgrpc.StreamClientInterceptor` ends a server-streaming client span
only when a stream operation observes EOF/error. WP consumes a final `Synced`
protocol frame and cancels/closes without another receive, so the exported
server span referred to a client span that was never ended/exported. The pool
now uses the same dependency version's recommended `otelgrpc.NewClientHandler`,
which ends spans on gRPC `stats.End`, including cancellation. It replaces both
legacy interceptors, avoiding duplicate spans; application RPC/ack handling is
unchanged. The probe now requires every local `CHILD_OF` parent to be retained.
The stats handler truthfully exports `Cancelled` when the client closes a stream
after its final acknowledgement. For an exact WP append client RPC, wpdiag keeps
the error/status evidence but reports `observed_rpc_cancellation` with unknown
impact: normal acknowledgement cleanup and genuine interruption cannot be
distinguished from that transport status alone. Other/server errors and
malformed/conflicting tags retain their existing interpretation.

## Runtime trace contract

Queued appends capture only the immutable `trace.SpanContext` before publication.
No caller context, deadline, cancellation, arbitrary context value or baggage is
retained. Execution keeps its existing independent lifetime. Existing send/ack
timeouts, retries, batch sizes, quorum rules and ordering are unchanged.

- **Single entry:** scope `AppendOp`, operation `Execute` is a child of the
  captured span. Its send and received-ack spans, remote client propagation and
  remote server span retain that trace ID. The Execute span ends after initial
  replica sends; it is not a durability/quorum latency measurement. A child may
  finish after its ended async submission parent.
- **Coalesced batch:** scope `BatchAppendOp`, operation `Execute` starts a new
  trace with links to distinct valid input `(trace_id, span_id)` identities.
  Duplicate inputs are linked once; different spans in the same trace remain
  distinct. A batch is not assigned an arbitrary request parent. Its remote
  server spans use the batch execution trace. Coalescing remains independent of
  tracing, and a batch size of one still uses the single-op path.
- Single Execute spans carry decimal-string `log_id`, `segment_id`, `entry_id`;
  batch Execute spans carry `log_id`, `segment_id` and `append.entries`. IDs do
  not establish the client's metadata namespace or Milvus channel association.
- No valid input context preserves the existing root-span behavior. Parent
  sampling decisions are retained for single entries. Batch root sampling and
  SDK link limits still apply; links omitted by limits and unsampled spans
  remain coverage gaps. Link truncation is not overridden to create unlimited
  telemetry.

The backend probe uses actual OTLP export into **Jaeger 1.53.0**. Links appear as
`FOLLOWS_FROM`; gRPC server propagation appears as `CHILD_OF`. The independent
`wpdiag trace diagnose` recognizes the exact AppendOp/BatchAppendOp Execute
scopes and qualified AddEntry/AddEntries RPC names from these real spans.
Embedded client scopes do not count as Milvus StreamingNode work merely because
the service is named StreamingNode; a generic Execute name remains unsupported.
For writes only, Proxy/SN spans with the exact `milvus.streaming.wal` scope and
operations `wal.autocommit`, `wal.txn`, `wal.dist_append`, `wal.append`, or
`wal.appendimpl` supply their respective logical/distributed/adaptor/backend WAL
boundaries. They are inclusive scopes, not disk Write/Sync timers. Conflicting
scope aliases remain unsupported. This does not extend read, consume or DDL
coverage by analogy.
The parser retains references and reports
`async_links_not_serial_children`. It does not fetch/merge external linked
traces or turn them into serial critical-path evidence. An input trace alone
does not contain the independently rooted batch trace. Operators need the
batch trace's links; automatic cross-trace discovery is a later extension.

## Repeat the isolated regression

Unit/race tests need no existing monitoring service or Kubernetes cluster:

```bash
go test -race -short ./common/logger ./common/tracer ./woodpecker/segment ./woodpecker/client
```

The focused `TestAppendTrace*` cases cover actual FIFO execution/remote gRPC,
cancelled/ended caller contexts, delayed success/error acknowledgements,
multi-request batching, link identity/deduplication, absent context, sampling
and SDK link limits. Normal runtime correctness/race suites still apply.

To verify actual export, retained links and independent-tool compatibility:

```bash
make wpdiag
docker pull jaegertracing/all-in-one:1.53.0
python3 tests/integration/tracing/append_propagation.py \
  --output /tmp/wp-append-trace-evidence
```

The harness starts only a temporary Jaeger bound to random loopback ports, runs
the actual Go queue/client/executor against a local generated-gRPC protocol
fixture, checks successful/error single-entry and mixed-request batch spans in
Jaeger, then diagnoses those exact IDs using wpdiag. It does not import fabricated
OTLP spans. The protocol fixture acknowledges frames **without storing data**;
passing does not verify Milvus, file writes/fsync, crash durability or production
sampling/export reliability. Raw traces, fixture receipts, diagnostic JSON and
the Go test log are retained only when `--output` is supplied. The temporary
Jaeger is stopped on success/failure. Its ephemeral storage is not production
retention.

## Remaining #459 acceptance

1. Record an available Milvus build/image containing the upstream propagation
   change and the released WP client fix. Record the actual WP service image
   versions separately; updating server Pods alone does not update the client
   embedded in Milvus.
2. Repeat the [locally validated controls](FULL_STACK_VALIDATION.md) on that
   released combination. In an explicitly selected test deployment, run a tagged Insert and Strong
   query/search controls. Retain actual spans and response correctness, including
   single-entry and coalesced/slow paths where applicable.
3. Verify the causal Proxy → WAL/SN → WP relationships through parents or links;
   retain partial coverage for earlier/incompatible versions and undiscovered
   cross-trace batches. Update the small stack example/compatibility receipt
   only for combinations that were actually run.
4. Distinguish propagation, instrumentation, sampling, export/retention and
   storage-timer scope. No missing-span-as-zero, lossless-export claim, quantile
   addition/subtraction or disk Write/Sync claim from aggregate flush spans.

This PR references #459 and #450. It must not close #459 based only on the
isolated protocol acceptance.
