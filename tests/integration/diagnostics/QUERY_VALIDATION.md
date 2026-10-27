# Loaded query/search consistency acceptance — 2026-10-27

This [#486](https://github.com/zilliztech/woodpecker/issues/486) receipt validates
the existing query stage with real successful RPCs and metrics. The condition is
a **Customized future guarantee timestamp** on an already-loaded collection,
with paired Strong controls; it is not a Strong-query production incident or a
slow/failed storage system. See [QUERY.md](QUERY.md) for reproduction.

## Fixture and provenance

- Run `20261027T105127Z-a5dc6576`, UTC `2026-10-27T10:51:27.620088Z` to
  `2026-10-27T10:56:48.727481Z`; exit status **0**.
- Base master `d4511a259ce796a5584bdef5436d2599a7906d0a`, independent of pending
  #485. The reused `cli_acceptance.py` hash was
  `69bd7d3e1c8532bcae9db7d813d5473faf88c9e195922f39342a2f38d7fdbc9e`.
- External task-owned profile/context and namespace `wp-diag-458`, Kubernetes
  v1.33.1, four CPUs/6144 MiB; existing core services and Prometheus v3.0.1 with
  15-second scrapes. No new Helm rollout or management configuration change was
  needed. The harness used explicit contexts and did not manage cluster lifecycle.
- Official Milvus chart 5.0.31 and the existing support/monitoring charts. The
  actual Milvus image was
  `harbor-us-vdc.zilliz.cc/milvusdb/milvus:enhance-woodpecker-v0.1.46-3.0-20261012-27bd28d206-v2-arm64`,
  configuration SHA-256
  `56f66c0b875ae2a49ff54a2b1f43e81441a92d3cecc9d938c68e17434b51dea2`.
  Three WP servers used `milvusdb/woodpecker:v0.1.46`, configuration SHA-256
  `4bef58cf01716909e6b2ab2093eb06c98489d8980b9822f4a132ca8e27d2d5f2`.
  This older metrics-compatible combination does not establish full trace support.
- All selected Pod UIDs/image IDs/restart counts stayed unchanged. Pre-existing
  container restart counts after external profile startup were 1–3; none increased
  during the scenario. The mounted `milvus` ConfigMap UID/resourceVersion and data
  fingerprint stayed unchanged (data SHA-256
  `2fc303771ab68f6acfd473faa70b8a555dc4f6fe019ef988f3c0ca47964da560`).

The official protobuf test client used go-api/v3 v3.0.0 and gRPC v1.80.0 in an
isolated nested module. Go 1.25.0 built the diagnostic CLI. Exact binary hashes:

| Binary | SHA-256 |
| --- | --- |
| wpdiag | `8fa3ede7bad11f813c03b9f32a277462d5a90e17f2175870f5b4942d7bd0421e` |
| query client | `b50d09cd7b6b181aff4e55c67955aaecae58fcb4b1370641bb81928c042af71a` |

The executed Python harness hash was
`027be354727fa50526979fe64261939ee855facde0e5849c4374c0f5157399ef`.
The later Go EOF-cancellation helper extraction preserves the same behavior and
has a dedicated blocked-RPC regression; recorded executed-code hashes are retained.

## Execution and actual results

```bash
python3 tests/integration/diagnostics/query_acceptance.py \
  --context wp-diag-458 --namespace wp-diag-458 \
  --queryclient /tmp/wp-query-client-486 --output /tmp/wp-query-486-live --execute
```

The runner seeded 64 vectors, then made 24 marker batches in each active phase,
two concurrent batches scheduled over 30 seconds. Every batch acknowledged one
new ID and successfully queried/searched that ID in both target and Strong-control
requests. The future fence added 2000 ms to the **server acknowledgement** timestamp;
the client did not synthesize it from its own wall clock. There were no query/search
errors or deadline samples accepted as success.

| Phase | Target query p95 | Target search p95 | Paired Strong query/search p95 | Exact ID verification |
| --- | ---: | ---: | ---: | --- |
| Loaded baseline | 17.98 ms | 18.29 ms | 18.42 / 19.37 ms | 88 IDs and search passed |
| Idle | No workload RPCs in the selected interval | No workload RPCs | Not sampled as application latency | Existing loaded collection; observed idle completion populations |
| Customized future fence | 2188.35 ms | 2189.19 ms | 21.53 / 21.79 ms | 112 IDs and search passed |
| Normal-request recovery | 19.18 ms | 18.89 ms | 18.90 / 19.30 ms | All 136 IDs and search passed |

The 64 retained active/verification Loaded snapshots all reported Loaded and 100%
progress. The idle check also passed. This excludes the initial-Loading condition
tested separately in #484; between-sample states are not asserted.

## Metrics and conclusions

StreamingNode node 26 (`10.244.0.62:9091`) exported qualified TSafe-wait populations
for `query_type=query` and `search`. Each had **48 completions** in the selected
future-fence window, above the unchanged 20-observation minimum. For both, the
P95 histogram bounds were **2048–4096 ms**, above the unchanged **250 ms** source
policy. The stage reported `suspected_visibility_wait` for both operations.

Proxy completion and handler/result-wait observations also showed the elapsed
wait. Queue/core/loading evidence did not produce an alternative scheduling,
execution or loading pressure suspicion. These overlapping scopes were not summed
or subtracted into an exclusive network/WAL duration. Client p95 and source bucket
bounds describe different populations and are not treated as the same quantile.

Baseline and recovery had positively qualified Proxy and TSafe completions for
both operations. Idle had observed zero completion increases rather than absent
series. Missing/lazy families and collection/channel/WP-client metadata mappings
remained explicit gaps; no WP read or storage failure was inferred.

Standalone and composed diagnoses agreed for all four phases and historical replay
on rule/version, outcomes, targets, flow/stage, severity/strength, summaries and
uncertainty. Original finding/observation references resolved and summary IDs
remained original. Re-querying the fence interval after recovery retained the
visibility suspicion; current normal requests did not clear retained history.

The recovery is **removal of the requested future fence**, not repair of a broken
consumer. The explicit request control corroborates this case only; production
incidents still require requested-timestamp/frontier and consumption investigation.

## Negative controls and cleanup

- Wrong namespace selector: completed queries, exit 0, working gate rejected.
- Unreachable Prometheus: failed collection, exit 1, working gate rejected.
- The only owned collection was dropped/absence-checked and the original shared
  exclusive namespace lock was released. No existing collection or shared setting
  was changed. Raw source evidence and private connections remain local.

Six Go protocol/cancellation tests passed with the race detector, including actual
serialized custom/Strong RPCs, incorrect successful IDs, transport/deadline errors,
server-timestamp bounds/precision, parent-pipe EOF cancellation of a blocked RPC,
offline planning and receipt preservation. The Python evidence/lifecycle suite
passed 29 tests including the existing admission regressions. Nested module tidy
was unchanged. Browser/trace/Loki/persistence and production storage/durability
acceptance are outside this receipt.

Artifacts: `/tmp/wp-query-486-live/20261027T105127Z-a5dc6576/`.

| Artifact | SHA-256 |
| --- | --- |
| acceptance.json | `b67b3526d2c95f2277144848bec3e9f6082aa3a40a200d78c62fc8872934194a` |
| future.workload.json | `65ab3bb7836b2f71fac44a14979588a791d559b63696aa8fd00acfd053b84cae` |
| future.query.json | `b0be93194c36a5081d0e5d9b682f09530462e7583fac4472cf33d2d59d693f97` |
| historical_after_recovery.integrated.json | `8465c24f7891e50cb4d8f1eebeac7b6f2cda394e4ff9a9f666a5d04e24823c68` |

This completes the request-level loaded consistency-wait case. Scheduling/slow-core
faults, loaded reader stalls, persistence, real-stack WebUI/log/resilience cases
and released full-trace combinations remain separate #450/#459 follow-ups.
