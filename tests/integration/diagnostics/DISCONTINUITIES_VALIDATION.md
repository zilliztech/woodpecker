# Actual restart/reset and scrape-gap receipt — 2026-10-28

Passed #493 run: **`source-20261028t113258z-ff70b508`**, UTC
**11:32:58.587827–11:35:07.057867**. Reproduction: [DISCONTINUITIES.md](DISCONTINUITIES.md).
`passed=true`, no failures, original owned resources/lock cleaned. All data and
telemetry were produced by actual WP RPCs, Prometheus scrapes and deployed
collectors. No synthetic counters, TSDB edits, interpolated samples or diagnostic
rule/policy changes were used.

## Builds and original ownership

- Diagnostic production baseline: merged **`389ad6b`** (PR #500), built with
  Go 1.25.0 on Darwin ARM64. `wpdiag` SHA-256:
  `0fd44085867a795e0cfe05417dcd8b492d15912e036abad41187f37fffccc37b`.
- Recovery worker: actual WP protobuf client with standard tracing, node build
  `CGO_ENABLED=0 GOOS=linux GOARCH=arm64`; source SHA-256
  `0675adfb91c71b37312b254947c5863d7937f63b508ae3a461f432bbdd8816c3`.
  Its additional `--verify-only` path never appends; protocol corruption tests
  reject rewritten/wrong acknowledged values.
- Disposable context/profile/namespace `wp-diag-458`, Kubernetes v1.33.1,
  Docker 28.1.1, Linux ARM64, four CPUs/6144 MiB. Namespace UID:
  `e39652ca-1bdb-4cdf-b987-f60e0f916941`.
- Original Pod UID: **`5bbf4500-407f-4d8d-a678-3dceafc18bd8`**. Its spec/desired
  configuration stayed unchanged. Only this owned fixture enables a shared Pod
  PID namespace; no host PID access, privilege or node mutation was used.
- The complete **27-item existing workload/configuration inventory was unchanged**.
  Six unselected fixture containers retained exact identities, images and zero
  restarts. WP had exactly one intended restart at the same image ID.

Actual runtime image/config SHA-256 values:

| Container | Image | Runtime digest |
| --- | --- | --- |
| WP | `milvusdb/woodpecker:v0.1.46` | `4bef58cf01716909e6b2ab2093eb06c98489d8980b9822f4a132ca8e27d2d5f2` |
| MinIO | `milvusdb/minio:RELEASE.2024-12-18T13-15-44Z` | `8ce3a2b6c83521fabbde58b4f06e5fb7f95e64bfc05bc10def264479327c407b` |
| Prometheus | `quay.io/prometheus/prometheus:v3.0.1` | `57847a717fc6727452cd1ed5b02200e3030aba04ca13cbdc536ee99402643440` |
| Jaeger | `jaegertracing/all-in-one:1.53.0` | `894a0d7326581e198bc594c2409b9d670112fdf1ca74e56bce982a23624c860e` |
| Loki | `grafana/loki:3.5.7` | `1abe5e0900c0cacd81a3d52fa103749c222ff8abba7a93af57f44ff56728cacd` |
| Alloy | `grafana/alloy:v1.10.2` | `cff945593440273c7a3c4a2ffb5298a418011ebd3e7e75d4ee1e90f8d75f3dc2` |
| Worker | `wp-source-client-493:local` | `271f48a63815208813ff9d48906c5cab706c9d2147580fd1eb5296686ca741de` |

## Real restart and reset

The inherited source acceptance first passed its actual error/span/log and
independent/composed controls. Two synced acknowledged values were read-verified;
ten empty append attempts were explicitly rejected. Three additional read-only
verification rounds produced a pre-restart success count of four.

| Fact | Before | After |
| --- | --- | --- |
| WP container | `2601869e3478ed2149cdc75447a29e8316fa8df6786cf3224cfca9b2ba11b90c` | `4ba6185a40c9e145570a5921732e029dbba12d131af9686d0b4d1bbbccb65641` |
| Actual PID / `/proc` start ticks | **7 / 208080** | **214 / 213323** |
| Container start | `11:32:59Z` | `11:33:52Z` |
| Restart count | 0 | 1 |

The original process terminated at **11:33:51Z**, exit **137**, following the
journaled KILL. Read-only verification then returned both original entry IDs and
payloads plus exact EOF. No value was appended to make recovery pass.

Retained reset interval: **11:33:39.807337–11:34:00.202109 UTC**. The same labeled
success counter population retained eight samples, including:

| Timestamp (epoch ms) | Actual counter value |
| --- | ---: |
| `1793187229582` | 4 |
| `1793187235581` | 1 |

There was no sample interval above the unchanged 30,000 ms coverage bound in
this reset window. Independent and composed diagnosis returned existing
`counter_reset_or_restart`, **inconclusive/unknown**, with original evidence
references. It did not subtract through the reset or report healthy latency.

## Real scrape gap and restoration

The second incident kept the recovered PID **214**, start ticks **213323**,
container ID and restart count **1** unchanged. The harness journaled STOP at
**11:34:09.396272 UTC**, held it for forty seconds, then CONT'd the same verified
process. Actual Prometheus reported **up=0** during the outage and **up=1** after
recovery. Both original values/EOF again passed read-only verification.

Retained gap interval: **11:34:00.306666–11:34:57.546853 UTC**. Eight raw success
counter samples included:

| Timestamp (epoch ms) | Actual counter value |
| --- | ---: |
| `1793187247582` | 3 |
| `1793187289626` | 3 |

The actual missing-sample interval was **42.044 seconds**, with no counter reset.
Both command forms retained `scrape_gap`, **inconclusive/unknown**. The unchanged
count across the outage was not interpreted as observed idle/normal operation;
there were no interpolated samples or invented events between scrapes.

Re-querying the exact earlier reset interval after both restorations preserved
its original samples, outcome and uncertainty. Fresh observations had fresh IDs;
stable owner semantics and lineage agreed. A separate post-recovery interval
**11:34:57.715222–11:35:03.868433 UTC** retained three samples at count five,
without the earlier gap/reset. Successful actual reads establish current
recovery; that small population does not establish a latency SLO or clear history.

## Failures, verification and cleanup

Earlier run **`source-20261028t112917z-ba71e7be`** stays failed and cleaned.
Its PID-1 KILL command returned zero but the process/container never changed;
the 90-second restart gate correctly failed. The final fixture explicitly uses
an ordinary identified process within its own Pod PID namespace. No threshold,
source data or required acceptance condition was relaxed.

The initial full race audit also encountered the existing Prometheus invalid-JSON
test's 20 ms timeout while the VM/builds were active: it observed timeout rather
than the expected invalid-body code. The final complete audit passed after the
VM was stopped, without editing that test, timeout or production behavior.

Final verification passed: all **129 Python acceptance-tool regressions**,
native/source/stage/integrated/CLI/WebUI Go race checks, source-client protocol
checks, isolated Milvus protocol module race tests and **eight actual-report
replay checks**. Replay mutations (identity/time/deployment/conflict/dangling
lineage/current-history/duplicate) are explicitly offline counterexamples, not
new producer events. Repository Go lint, syntax, documentation generation and
diff checks passed. No production diagnosis code or thresholds changed.

All source-fixture resource UIDs/settings and original lock ownership were
verified before cleanup. Independent Kubernetes reads confirmed no owned Pod
or acceptance lock remained. The external operator restored the dedicated
profile's original **Stopped** state; scripts performed no cluster lifecycle
actions. Raw artifacts (approximately **2.66 MB**, below 96 MiB) remain local at
`/tmp/wp-discontinuity-493-live/<run-id>/`; private configuration is not committed.

This establishes the representative MinIO-backed restart and scrape gap, not
staged-disk, full SDK quorum or power-loss durability. Complete export/retention,
every failure timing and full Milvus trace release remain outside this receipt.
The [final audit](FINAL_AUDIT.md) records the original #458 gate and retained
limits. #459/#450 remain open independently of this delivery.

## Artifact fingerprints

| Local raw artifact | SHA-256 |
| --- | --- |
| `acceptance.json` | `a9f0378f02527200631ba9a8a3b76d8497531f21f37ef085585a95219b354c90` |
| `fixture.json` | `15ba3296bbd1139e6636b3a6735fc72ac63be7ae23ac14d1e99fc61a1aefe39f` |
| `actual-reset.prometheus.json` | `f54e59bf2e0c5671cec2871e9d27c51e9eae3b2b76a520d2ea1f05864b4b5ed8` |
| `actual-reset.integrated.json` | `6bd5beedced737edd5817d10ba227e7cc620c2c2b323a8cba3f39eee7af1cef9` |
| `actual-gap.prometheus.json` | `9cbf5f64310450d5c91f28defafa90dcce182730c6e29e68691f0aa53e4ad775` |
| `reset-after-recovery.prometheus.json` | `0286f1d4bd1c74b5f17dd7a088f14d98febcac52f4571c897ff6f24d022b89ae` |
| `current-after-recovery.prometheus.json` | `27e44494dee8c601f45c1ef3587ae958d7b3f0ee7b15113531d75e1c2b1eb779` |
| `restart-recovery.requests.json` | `b0470141c7ef6dd15f58d9c43c89935f80ce3653c3e0e45d67afeec46b8ef729` |
| `gap-recovery.requests.json` | `26e1e167046a157f6f6950f2715841c1ffec188568e103bd710303985ea95c02` |
| `paused-exporter-up.json` | `e47d95af15673812c36487127b9987be248c0a60d9dfea60de6f45af5a72ae66` |
| `recovered-exporter-up.json` | `e0ea5f1e75c40f465378ca889df4e6275f4124ae5484605fe863770557a7d1ad` |
