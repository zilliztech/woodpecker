# Native WP deployed acceptance — 2026-10-28

This [#489](https://github.com/zilliztech/woodpecker/issues/489) receipt exercises
real acknowledged append/read work and the existing native/adapter/integrated
contracts. [NATIVE.md](NATIVE.md) describes reproduction and recovery.

## Fixture, scope and provenance

- Accepted run: `native-20261028t021940z-49825a53`, UTC
  `2026-10-28T02:19:40.401819Z` to `02:21:38.469298Z`, exit **0**, `passed=true`,
  no failures, `owned_fixture_cleaned=true`, `gate_restore_required=false`.
- Base master `e48504f` (merged #487), independent of pending #494/#495.
  No production source, dependencies, diagnostic rules, thresholds or default
  minimum observations were changed.
- Existing externally managed context/namespace `wp-diag-458`, Kubernetes v1.33.1,
  Linux arm64, Docker 28.1.1; the profile has four CPUs/6144 MiB. Its existing stack
  resources were not selected or mutated. This task externally resumed the
  previously stopped profile and stopped it again after cleanup; the harness
  itself has no cluster-lifecycle or Helm actions.
- A newly created Pod ran one released WP server with `storage.type=minio`, its
  own MinIO, the transport gate and Prometheus. This is a **MinIO-backed WP gRPC
  service**, not the default staged WAL disk/replica path. RPCs use the repository's
  actual WP protobuf, not a mock service or synthetic counter/registry injection.
- Original namespace UID `e39652ca-1bdb-4cdf-b987-f60e0f916941`; lock UID
  `9370e95e-b956-47ec-b30c-82768fcdf08b`; fixture ConfigMap UID
  `a6cc3bf2-b88e-4e8a-a3c9-5244255da17c`; Pod UID
  `9652d575-674a-4012-a6b1-26b4d894175e`.
- The Pod spec SHA-256 remained
  `290e0d2ed5157ab972294c7255075965117ee984b5e893b7ff54200268aeb036`.
  All four original container IDs/images stayed identical, with zero restarts.
  Fixture volumes were new emptyDirs; no PVC or client etcd namespace was used.

Actual images and configuration image IDs:

| Component | Image | SHA-256 image ID |
| --- | --- | --- |
| WP | `milvusdb/woodpecker:v0.1.46` | `4bef58cf01716909e6b2ab2093eb06c98489d8980b9822f4a132ca8e27d2d5f2` |
| MinIO | `milvusdb/minio:RELEASE.2024-12-18T13-15-44Z` | `8ce3a2b6c83521fabbde58b4f06e5fb7f95e64bfc05bc10def264479327c407b` |
| Prometheus | `quay.io/prometheus/prometheus:v3.0.1` | `57847a717fc6727452cd1ed5b02200e3030aba04ca13cbdc536ee99402643440` |
| Test gate | `wp-native-fixture-489:local` | `494f0a21744c3fbc3d18495de9748dd1c20f4cc5580ef858fdf17c5a487293f7` |

The gate is a scratch image of the acceptance helper, cross-built for Linux arm64.
It forwards real HTTP to MinIO and temporarily holds only run-owned object keys.
It does not modify server code, Admin responses, metrics, entries or registry
records. Host binaries were built with Go 1.25.0; Python 3.9 ran the harness.
The later repository-required gofumpt formatting of the helper changed no runtime
logic; exact executed binary hashes below remain authoritative.

## Execution and results

```sh
python3 tests/integration/diagnostics/native_acceptance.py \
  --context wp-diag-458 --namespace wp-diag-458 \
  --gate-image wp-native-fixture-489:local \
  --worker /tmp/wp-native-fixture-489 --output /tmp/wp-native-489-live --execute
```

| Control/case | Actual result |
| --- | --- |
| Before work | Lazy completion counter family absent; snapshot did not establish a stall |
| Working writes/reads | Real `Buffered` then `Synced` acknowledgements; exact read identities and bytes |
| Idle | Focused read did not report a persistent stall |
| Caught-up tail | Actual entry-not-found/EOF polling; expanded `read_activity` reported `eof_observed` |
| Wrong node | Standalone adapter failed (exit 1); real native target selection failed; no successful empty-health result |
| Pending write | 20 distinct real `logstore.add_entry` operations, identical IDs/start times across all four rounds; existing `write_stall` emitted 20 `suspected_stall` findings |
| Pending read | 20 distinct real `logstore.get_batch_entries` operations, identical IDs/start times across all four rounds; existing `read_stall` emitted 20 `suspected_stall` findings |
| Snapshot during each fault | `inconclusive`; no sustained-progress assertion |
| Expanded pending read | Reader catch-up expectation not invented in the absence of explicit client metadata |
| Restoration | Explicit release before lease expiry; zero automatic releases; gate drained; all 40 pending RPCs completed successfully |
| Recovery | Focused current diagnosis had no persistent-stall suspicion; all 112 distinct acknowledged entries verified exactly |
| Historical replay | Both actual completion-latency incidents remained; recovered native child separately labeled `current_context`/snapshot |

The write historical window was `02:20:07.482249Z`–`02:20:30.562653Z`;
the read window was `02:20:40.723698Z`–`02:21:10.154067Z`. Each contained
**20 actual slow successful completions** after the blocked requests were
released. The write worker's observed latency range was approximately
**18,962–18,970 ms**; the read worker's range was **25,053–25,178 ms**.
These durations intentionally include controlled waiting and client transport;
they are not normal deployment performance or exclusive storage service time.

Both default-policy source results were `suspected_high_latency`, with P95
**at least 512 ms**, above the unchanged 250 ms WP policy. The server histogram
overflows its final finite bucket, so it supplies no finite P95 upper bound here.
Do not substitute the client duration for an exact histogram P95 or attribute the
whole interval to disk. The actual retained rates were 20/22 seconds (write) and
20/28 seconds (read), including scrape boundaries. The write window also retained
another idle log population, and admission metrics were absent/inconclusive for
this backend; neither was rewritten as a healthy or slow disk verdict.

Only one HTTP read had to reach the gate while the other real reads waited on
reader initialization; all twenty top-level operations remained pending. The
cumulative gate held-request counts were 20 after write and 21 after read, not
forty independent storage requests. Duplicate views were not promoted into
independent corroboration.

Every focused adapter and integrated child was checked against the **exact real
native subprocess response it imported**, captured without stdout/exit changes.
Original finding/observation IDs, source IDs, timestamps, targets, rule versions,
support/contradiction and unknowns matched; integrated summaries retained the
original child IDs. The native incidents remain in their original files after
recovery. The current snapshot was not backdated or used to erase retained
Prometheus facts. No native runtime history was reconstructed from a fresh sample.

## Integrity, cleanup and explicit gaps

The distinct acknowledged total was 84 normal write-log entries, 20 faulted
write-log entries and eight read-log seed entries: **112**. Every segment was read
back with exact entry IDs, segment IDs and bytes; the verifier requested an extra
entry to detect an unexpected addition. The twenty reads of the same four entries
do not count as eighty distinct writes. A service `Synced` acknowledgement is not
a power-loss/restart durability guarantee or an SDK quorum assertion.

UID-guarded cleanup removed the original Pod, then fixture ConfigMap, then lock.
All ephemeral object/metric/local data was removed; namespace opt-in remains.
An independent `kubectl get` confirmed no run-labelled resources or acceptance
lock remained. The dedicated profile was then returned to its initial stopped
state. Existing services, PVCs, Helm values and default kube context were unchanged.
The accepted run retained about **1.97 MB** of local evidence, below its 96 MiB
budget; raw/private connection files and Pod logs are not committed.

Expanded rules requiring client metadata, leased reader cadence, inline quorum
or staged disk admission remain explicit gaps. The standalone adapter continues
to import only focused `write_stall`/`read_stall`; no expanded-rule import is
claimed. Counter/ops collection is non-atomic and sampled, not continuous.
Registry and coverage qualifications remain in the original native reports.
Normally blocking SDK-tail semantics, absent APIs, incompatible builds,
registry truncation/eviction, resets and interrupted lifecycle are supplemented
by existing semantic tests and the new offline lifecycle tests; they are not
counted as additional deployed fault cases. No Milvus request-to-WP-log mapping,
trace/Loki corroboration or WebUI coverage is claimed.

## Retained attempts and reproducible failures

The earlier attempts were retained under `/tmp/wp-native-489-live/` and are not
counted as successful deployed acceptance:

| Run suffix / UTC start | Outcome and correction |
| --- | --- |
| `021244z-fed408c8` | No seed: HTTP Ready but memberlist empty; failed before data/fault; owned fixture cleaned |
| `021350z-ef7face1` | Explicit environment identity alone did not create membership; failed before data/fault; owned fixture cleaned |
| `021420z-ec360ec7` | Self seed with empty advertise address could not resolve; failed before data/fault; owned fixture cleaned |
| `021534z-0717ea18` | Captured bounded failure context confirmed membership startup's empty-advertise failure; owned fixture cleaned |
| `021625z-8c57048b` | First full successful run; then added explicit artifact-budget and gate-drain/fallback-cleanup checks |
| `021940z-49825a53` | Final complete run with those checks; passed and cleaned |

No failed attempt lowered diagnostic thresholds or manufactured expected work.
The startup fix uses an explicit self seed and loopback advertise addresses in
this isolated fixture. It does not change production WP startup behavior.

## Hashes and verification

| Executed binary/source | SHA-256 |
| --- | --- |
| wp | `a5cd227aada242da318e49be28000e9ac256b970e9be58fc4f6f2e2029ab647f` |
| wpdiag | `b5d7c0da69b46c0e202b82960cd2ee0bbf9748e7c05c9ed024f9e87d683ae54b` |
| Host RPC worker | `c4f4ae63c3abd6d004b7eaae9237167dd6050ba1e156de4dbd0e483b14beafb2` |
| native_acceptance.py | `52c45d1da21eac02ea76d4a0934181196aa40564481bbcc840601fe7aebccaea` |
| native_fixture.py | `49f20c65a13152fa75b7cb8053cf2b526dec00bb36329ad6ffa0726c99c5d109` |
| native_capture.py | `4909b21e38ed0967882e23da37a7a5280a7f1d214c622f6bbddc1a997119a7a5` |

| Raw local artifact | SHA-256 |
| --- | --- |
| acceptance.json | `8f14af5b32553a11d3bb5bb0122a816eb7f4b9146b17cfee4bda8fad729af935` |
| fixture.json | `2e1f567404dc72682637f4cedde7043fb547a75df9d1544ab41d1911e3f863c1` |
| write-native.json | `382feefeec0a1c79519a491e39e9db3c21e59b7f35887a3decaf3999daf86333` |
| read-native.json | `af57c6fbf598626a02af2ffea0bfa46101cdfb54db7b8812f09e99f7fbb94eb5` |
| write-historical-current.json | `06774655d10353d5ed83e15f12e484a24c4f0888b16e89ab12d126c59f176d3d` |
| read-historical-current.json | `370f6be361733f444730bccd98b7176c43bd466329f34de3b3498aa47887a2ca` |

Offline validation includes six Go race tests for acknowledgement/payload/EOF
protocols, request bounds, cancellation and gate release, plus Python provenance,
offline-plan, ambiguous-create, ownership, UID deletion and explicit-recovery
regressions. The entire deployed-acceptance Python suite passed. Those fixtures
are separate from the actual Kubernetes execution documented above.
