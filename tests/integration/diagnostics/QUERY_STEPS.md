# Deployed query scheduling and execution acceptance

This [#490](https://github.com/zilliztech/woodpecker/issues/490) scenario validates
the existing `wpdiag milvus query` and integrated query stage on a real, already
Loaded collection. It complements the completed [Customized TSafe case](QUERY.md).
No new diagnostic rules, thresholds or minimum-observation policies are added.

Two bounded request profiles distinguish:

- **Execution:** serial large-NQ FLAT searches with qualified slow core and
  Sealed-segment completions, normal scheduling on that same QueryNode exporter,
  zero observed ready backlog and separate TSafe observations.
- **Scheduling:** concurrent searches with qualified slow queue completions and
  positive ready backlog throughout retained scrapes on that same exporter.
  An earlier spike, recovered queue, missing series or a timeout does not pass.

The default dataset is 65,536 deterministic 256-dimensional float32 vectors,
**64 MiB of raw vectors**, plus IDs/index/storage overhead. Its explicit FLAT/L2
index makes search effort reproducible. Results require exact vector self-match,
not a synthetic acknowledgement. These are controlled development workloads,
not production SLOs or proof of an exclusive CPU, disk or network cause.

## Prepare an existing disposable stack

Use the [stack/acceptance setup](README.md) with a dedicated namespace, no other
application traffic and a compatible Milvus/WP service deployment. All selected
core Pods must already be Ready. Prometheus must scrape actual QueryNode,
StreamingNode and Proxy metrics with `cluster="local"`, the selected namespace,
and `woodpecker_id=RELEASE`; the example uses 15-second scrapes.

The harness never installs Helm, starts/stops a cluster, changes kube context,
freezes processes, resizes Pods or changes shared configuration. Workload effort
is controlled only through owned data, NQ, concurrency and request budgets.
Provision the example's documented resources separately; an OOM/restart remains
a failed contrast, not an implicit request to resize the target.

Build from the repository root with Go 1.25+ and Python 3.9+:

```sh
make wpdiag
go -C tests/integration/diagnostics/queryclient build \
  -o "$PWD/bin/query-steps" ./steps

kubectl --context YOUR_CONTEXT label namespace YOUR_NAMESPACE \
  diagnostics.woodpecker.io/query-steps-acceptance=cli-query-steps-v1
```

The protobuf helper uses the existing isolated go-api/v3 v3.0.0 module, without
changing production dependencies or the previous TSafe client. Kubernetes access
needs namespace/core resource reads, Pod port-forwards and ConfigMap lock
create/get/delete. Milvus collection/index/data operations affect only a new
`query_accept_TIMESTAMP_NONCE` collection. No tenant etcd credentials or metadata
prefix are selected. Review the normal local image/architecture setup separately.

## Plan and execute

```sh
python3 tests/integration/diagnostics/query_steps_acceptance.py \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE
# Offline default: no commands, connections or output directory.

python3 tests/integration/diagnostics/query_steps_acceptance.py \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE \
  --output /tmp/wp-query-steps --execute
```

Override `--release`, `--prometheus-service`, `--wpdiag` and `--queryclient` as
needed. `--case execution|scheduling` selects a focused subset with its own
baseline/idle/recovery; default `all` validates both. A successful subset does not
close the complete issue by itself.

Default controls are execution NQ **512**, one in-flight request; scheduling NQ
**256**, at most **eight** in flight. Offering lasts at most 120 seconds, then
in-flight requests drain under their deadlines. Scheduling permits at most 1,024
offered requests; the actual offered/completed counts are recorded. This is a
closed-loop workload, not a promised open-loop RPS. The retained pressure interval
starts after an actual ready observation and lasts 75 seconds while offering
continues. Source qualification, rather than the priming probe, decides whether
every retained ready sample supports sustained pressure.

Dataset/NQ/duration bounds may be explicitly adjusted for the selected hardware.
If fewer than twenty relevant completions are observed, the existing source rule
stays inconclusive and the required case fails. Do not lower policy to make it
pass. The [deployed receipt](QUERY_STEPS_VALIDATION.md) records both successful
conditions and non-reproducing/failed attempts.

## Sequence and correctness

1. Check namespace UID/opt-in and all selected core Pods; record actual image IDs,
   restarts, resources and mounted ConfigMap fingerprints. Acquire the shared
   create-only `wpdiag-cli-acceptance-lock`.
2. Journal unique collection ownership before creation. Its schema description
   carries the exact run-owned name; actual collection ID is retained separately.
3. Insert bounded 512-row protobuf batches; check every returned ID/count and
   nonzero server timestamp. Flush, wait for completed persistence, build FLAT/L2
   and wait for 100% load. Record index and physical segment/node/row/state facts.
4. Read every acknowledged ID and vector bit-for-bit in bounded range queries,
   detect duplicates/extras and verify no outside-range ID exists. Prime a real
   search, then run normal NQ=1 and explicit idle controls.
5. Run the selected execution/scheduling profiles with Strong consistency, no
   future timestamp fence, topK=1 and filter `id >= 0`. Each returned query in the
   NQ batch must contain its exact expected self ID and finite near-zero L2 score.
   Loaded state is sampled while workload runs.
6. Return to normal NQ=1 requests, verify all IDs/vectors again, replay the original
   incident windows after recovery, and check wrong-instance/absent/unreachable
   source controls. Compare standalone/composed semantics, targets and unknowns;
   integrated summaries retain original child IDs and lineage.
7. Require unchanged core Pod/image/restart/resource/config identity, then drop
   only the original owned collection and release only the original lock using
   a UID precondition. Collection drop is verified; object-store/log reclamation
   timing is not asserted by this test.

The vector generator does not repeat every 251 IDs like the earlier tiny marker
fixture. A dedicated regression checks all 65,536 vectors for exact duplicates.
Query verification requests one extra row per range and verifies vector bytes;
search verification checks every NQ result, not just one marker in the batch.

## Interpret the reports

`acceptance.json` records selections, requested limits, actual windows, outcomes,
resolved evidence, binary/source hashes, user latency and cleanup. Each phase has
`*.workload.json`, sampled Loaded states, standalone `*.query.json` and integrated
`*.integrated.json`. Source observations retain exact executed Prometheus queries,
sample windows, populations, units, default policy and P95 bucket bounds.

Execution needs existing `suspected_execution_work` and
`observed_slow_segment_work` outcomes with successful client work. Its normal
queue and zero-ready controls must match the expensive core's exporter; unrelated
SN gauges cannot clear a QueryNode queue. In this deployed architecture TSafe may
be exported by the StreamingNode/delegator while sealed core work runs on a
QueryNode: those remain **independent observations**, not a fabricated same-node
or per-request join.

Scheduling needs the existing `suspected_scheduling_pressure` outcome, its exact
source queue/ready references and matching exporter identity. Executing concurrency
is observed separately and does not establish configured capacity or CPU saturation.
Completed queue latency and sampled backlog may coexist with expensive execution;
the test does not claim every concurrent request is slow exclusively because of
queueing. Query timer quantiles are never added or subtracted.

A recovered current queue does not erase the retained incident. Source absence,
wrong exporter or failed collection cannot pass the baseline gate. A failed
reproducer retains its workload receipt and best-effort source report, including
inconclusive completion counts. A current snapshot cannot reconstruct missing
runtime history. Metrics do not establish a collection/channel-to-WP-log binding.

## Bounds, cancellation and recovery

Default whole workload budget: 1,200 seconds (hard allowed range 300–1,800), plus
a separate bounded cleanup allowance. Each diagnostic has a 30-second budget;
JSON is limited to 16 MiB/file and 96 MiB/run. Host RPC workers have a 600-second
outer deadline, 45-second default per-RPC timeout (allowed 1–60), at most 16
in-flight requests, NQ at most 16,384, 1,024 offered requests and 120 seconds of
offering. Raw data vectors are sent, not copied into diagnostic artifacts.

SIGINT/TERM stops only the owned worker and runs cleanup. Parent stdin EOF also
cancels actual pending RPCs. A forced stop/timeout is a failed workload, not a slow
successful completion. Process/host loss leaves a recovery receipt:

```sh
python3 tests/integration/diagnostics/query_steps_recover.py \
  --run /tmp/wp-query-steps/EXACT_RUN_ID \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE
# Inspect the offline plan, then repeat with --execute.
```

Recovery checks original namespace UID, label, lock UID/run/profile, collection
description and recorded collection ID. It needs no Ready query node or global
inventory before removing owned data. Ambiguous lock/collection creates are
resolved only through their original ownership records. A foreign replacement,
unavailable cleanup API or unresolved identity preserves data/lock for inspection.
`manual-recovery.json` records an explicit recovery without rewriting the original
failed acceptance. An already completed cleanup is an offline no-op.

## Offline regression checks

```sh
go -C tests/integration/diagnostics/queryclient test -race ./...
python3 -m unittest discover -s tests/integration/diagnostics \
  -p 'test_query_steps_acceptance.py' -v
```

Unit CI discovers the new helper package and Python tests through the existing
acceptance checks. Protocol/counterexample fixtures complement the real deployed
receipt; they do not substitute for actual queue/execution reproduction.
