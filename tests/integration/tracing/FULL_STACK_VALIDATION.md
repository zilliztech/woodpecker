# Local full-stack tracing receipt — 2026-10-27

This records real Milvus REST → SN WAL → remote WP service requests, not the
protocol-only exporter fixture. It demonstrates the propagation fixes on temporary
development images. **It does not establish a published release combination or
close #459.** Reproduction instructions are in [FULL_STACK.md](FULL_STACK.md).

## Build provenance

The dedicated ARM64 fixture used official Milvus Helm chart **5.0.31** and the
existing stack support chart, with three WP service replicas, etcd 3.5.25,
MinIO `RELEASE.2024-12-18T13-15-44Z`, and Jaeger all-in-one **1.53.0**. Only core
services and Jaeger were installed; this is not a new all-monitoring acceptance
claim. Kubernetes v1.33.1 ran in an externally created `wp-trace-459` minikube
profile (four CPUs, 6144 MiB). Docker Desktop supplied eight CPUs and about 8 GiB
total VM memory. The namespace had the explicit trace-acceptance opt-in label.
The default kubecontext stayed unset; other profiles stayed stopped.
After the recovery control, only this dedicated profile was stopped; its data and
local images were retained for inspection. The stack did not manage its lifecycle.

| Component | Source / local tag | Runtime image configuration SHA-256 |
| --- | --- | --- |
| Milvus before noop fix | Private cached base `27bd28d20688ab00cdd74232a20e988fb2162aed`, Go-only backport of Milvus #50796, embedded WP `232633c`; `wp-trace-milvus-459:local` | `50bdb702cca4c55a7b8ee87379c7d4fe47c6690c1b28e1cbc5655695a3c6ee39` |
| Milvus after noop fix | Same base/backport, embedded WP `e26a7e5d2432a072a18f5e25a5d3cbfbd6511d42`; `wp-trace-milvus-459:noop-fixed` | `d5face7624d431ca7cbc22ee338584e6ffa1bd2d2782aaa91dab48e4c4fc8374` |
| Remote WP servers, unchanged in the comparison | WP `232633c`, including merged #479; `wp-trace-server-459:local` | `f3e0fcd298101053bcc7eaf5947cda471f5462f6d46a8b87a6dc9eb23537c7d9` |

The Milvus backport is from upstream commit
`5eebaa9ad4ee5eadd82071cecc1b839df9cf1441`. The base lacked that implementation
despite its later private build date. Its matched cached native libraries were
retained; source-compatible headers included milvus-storage `f38765d` and Arrow
17 C interfaces. The ARM64 Go binary was rebuilt with Go 1.26.9 and
`dynamic,sonic` tags. SHA-256 of the resulting Milvus working diff against its base
(including local module replacements) was
`f01cddf51ffcc8117505e5e715c88f40ea60cd44b6eb3ca098d9c539424c6234`.
This is a local test fixture, not a redistributed private patch or a supported
mixed-native build recipe. Use matching source/native dependencies for new builds.

The diagnostic binary included this PR's exact `milvus.streaming.wal` catalog
entries and conflicting-scope checks. Its SHA-256 was
`89268d85ee47273121ef16296c43600fee810fe8f60e23bbc3fc5cd4a6352d70`.
The final request-client binary SHA-256 was
`a250ad7504f486743edd5beaf960c7caf5124aee8f04b3f190b2f88162511746`.
Every run recorded actual Pod UID, image/imageID, restart count and CLI hashes.
Successful scenarios had unchanged selected Pod inventories and zero restarts.

## Actual scenarios

Artifacts were retained locally under `/tmp/wp-trace-459-live/<run-id>/`.
Every successful workload checked exact Strong-query IDs and vector search, then
dropped only its random owned collection. All positive controls compared
standalone `trace diagnose` with composed `run --modules trace` findings.

| Run ID (UTC) | Control / workload | Observed result |
| --- | --- | --- |
| `20261027T072844Z-22ade444` | Before provider fix, single-entry, four Inserts / 64 rows | Data/query/search succeeded and collection dropped. Strict write association failed as expected: Proxy and WP server spans existed, but SN and embedded WP-client parents were absent. The source retained partial coverage. |
| `20261027T074429Z-e62fd4f4` | After provider fix, single-entry, four Inserts / 64 rows | All four writes plus query/search had recognized retained ancestry. Insert client latency 6.04–33.08 ms. |
| `20261027T075226Z-ffc00060` | Batch size limit 1000, 32 Inserts, concurrency eight, 512 rows | Passed: 16 Inserts had exact asynchronous links into four retained batch traces; 16 had direct parent paths. Batch-linked source coverage stayed partial. Insert latency 9.90–52.25 ms. |
| `20261027T075832Z-a1a630a4` | First attempted admission slowdown, 32 Inserts, concurrency four, 2048 rows / 32 dimensions | Workload/ancestry passed, but **slow-path reproduction did not**: Insert latency 15.33–63.05 ms, no slow write spans. Recovery threshold was still 0.8 and undid the forced slowdown. This run is not slow-path evidence. |
| `20261027T080211Z-9ec8ead4` | Corrected `admission-slow` profile, same 32 × 64-row workload, concurrency four, 32 dimensions | All writes/query/search/ancestry passed. Insert latency 13.18–4944.97 ms; 18 client requests exceeded 1000 ms. Source reports retained 72 slow **write** span findings (overlapping inclusive scopes, not independent causes). |
| `20261027T080503Z-260f552e` | Explicitly restored normal single-entry profile; same workload as slowdown | All 2048 rows, query/search and 34 diagnoses passed; Insert latency 14.17–56.01 ms and no slow write findings. Read controls can still include startup/catch-up latency; this is not an all-operation latency SLO. |

The slowdown/recovery workload contains 256 KiB of float-vector values before
IDs/protocol/JSON overhead. It does not fill the disk or allocate pressure memory.
Normal settings: memory slowdown/recovery thresholds 0.85/0.8, slowdown HWM/LWM
4mb/256kb, burst 20m, batch limit one. Slow settings: thresholds 0.0001/0,
HWM/LWM 16kb/8kb, burst 128kb, startup delay 0s, batch limit one. The applied Helm
values, image IDs and workload limits are separate from inferred span timing.

An additional post-workload metric snapshot on the same slow-profile SN node 26
reported memory thresholds 0.0001/0, slowdown HWM/LWM 16384/8192 bytes, and
`milvus_wal_rate_limit_controller_state{source="memory",state="slowdown"}=8192`
for both physical channels. This is an adjacent runtime snapshot, not a
historical Prometheus time series covering every request.

### Inspect one slow write

Trace **`d63178d538d8e9c1e30820be8841772a`** retained:

| Observed scope | Span ID | Inclusive duration |
| --- | --- | --- |
| Actual acceptance client request | `f66e44ca47d356e4` | 4944.973 ms |
| Proxy REST Insert | `2b6ab52c2de77ed7` | 4939.904 ms |
| Proxy-Insert-Execute | `665ce46337f2d1f2` | 4936.908 ms |
| `wal.autocommit` at Proxy | `b69661004994c7f1` | 12.262 ms |
| `wal.append` at SN | `e7cec53770d0f93f` | 10.729 ms |
| WP client `LogWriter/Write` inside SN | `407f658de75b7f59` | 9.806 ms |
| One remote WP AddEntry server branch | `57de6be217d1e126` | 6.853 ms |

This narrows the suspicion to work/wait before the observed WAL append, rather
than equating the whole 4.94-second request with WP append or disk flush. The
controlled values and adjacent limiter snapshot corroborate admission slowdown.
The spans do not directly time every admission queue, so the diagnostic reports
suspicious observed Proxy work without claiming a precisely measured exclusive
queue. These inclusive values must not be summed or subtracted into component
quantiles, disk timers or a guaranteed critical path.

### Inspect one batch relationship

Input trace `b84d5fc2f05ee71414813f9bcc7ef4ff`, retained input span
`e6e03d6c331fcd6c`, has Proxy → actual SN WAL → WP client ancestry. Batch trace
`64861fe58580701fbdcf10ba835292ea`, BatchAppendOp Execute span
`86d1fad0f17318e7`, links that exact pair and reaches WP server span
`6cc122a233109669` through children. Both source traces remain partial serial
coverage. Service-name presence alone would not establish this relationship.

An earlier batch run `20261027T074931Z-7b808830` passed data correctness but exposed
an overly strict acceptance assumption: one valid direct branch can coexist with
other selected RPC/async parents that are not retained. The harness now records
minimum branch proof in linked mode and preserves the source's stricter partial
whole-trace result. The source's one-root/retained-parent rules were not relaxed.

## Failed workload and recovery

Run `20261027T073939Z-c046e662` timed out during collection creation and its cleanup
attempt. The original failed receipt remains failed. WP logs reported
`local disk above hard watermark, write backpressure active`; the Docker VM
filesystem was about 88% used. That error can also represent probabilistic
backpressure between the configured soft/hard watermarks; it does **not** prove
that utilization exceeded 90%. This was environmental disk pressure, not the
intended admission fault.

Only the task-owned Go build-cache volume (about 4 GiB) was removed. No unrelated
images/volumes were pruned and no WP disk guard thresholds were weakened. The
filesystem fell to about 81% used and the same fixed image/configuration passed.
At recovery, the exact run-owned collection was still present and was explicitly
dropped; `manual-recovery.json` records has=true → false without rewriting the
original receipt. Its attempted create/drop trace IDs were no longer retained
when inspected later. This is a real missing-evidence gap, not a zero-latency or
healthy result. The runner now saves available attempted-request traces before
reporting workload failure or missing exports.

## Retained receipt hashes and checks

SHA-256 of each positive control's `acceptance.json`:

```text
single    f41cb26cf6ddcf261ced297caae405183a13eb64121bd7f7b72babb90c976096
batch     a48443c84a5e27415c67fe2427c2bbc11c9c1eaee06caf2241904bf47e272484
slow      57e221abf3e67cc30f9a6647b02901a7d422ac4f0aeb6d1b4bb366b5b2f52005
recovery  d2a559ddc36fb695e55dad7baf6f5b72e489c266379d02bd11727e5550a9667b
```

Validation also passed race tests across tracer/logger, Woodpecker, diagnostics
and the CLI, both real-client constructor regressions, HTTP request-client
propagation/error regressions, ten offline graph/plan/equivalence counterexample tests,
20 Jaeger source smoke cases, the actual queue/gRPC exporter probe's three
cases, and focused Milvus message/producer/WAL-adaptor tracing tests.
Lint passed for the changed Go packages. Website fragment generation was
idempotent. None of these tests claims file Write/Sync or crash durability.
An additional offline comparison of 112 retained standalone/composed report pairs
also matched targets, flow/stage, summaries and uncertainty, excluding fresh run IDs.

Remaining release work: publish/identify an available Milvus image with the
upstream propagation and updated embedded WP client, record the released WP
service version independently, and rerun these controls on that supported
combination. Sampling loss, exporter/retention failures, uninstrumented waits,
client metadata ownership and crash durability remain separate coverage limits.
