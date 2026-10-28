# Deployed persistence acceptance receipt — 2026-10-28 UTC

This is the actual selected-build receipt for [#488](https://github.com/zilliztech/woodpecker/issues/488),
not a synthetic Prometheus fixture. It covers normal sync completions, observed
idle, a known in-place object-store CPU quota and normal-workload recovery.
Other #458 acceptance children and released full-trace combinations remain open.

## Result and reproduction

Run `20261028T004636Z-c8f2fb37` started at `00:46:36.376557Z` and finished at
`00:53:09.655156Z`. It exited 0 with `passed=true`, no failures, exact-ID/vector
search correctness, restored resources, and owned collection/lock cleanup.

```sh
python3 tests/integration/diagnostics/persistence_acceptance.py \
  --context wp-diag-458 --namespace wp-diag-458 \
  --queryclient /tmp/wp-persistence-client-488 \
  --output /tmp/wp-persistence-488-live --execute
```

Use [PERSISTENCE.md](PERSISTENCE.md) to build binaries, provision an appropriate
disposable target and inspect the offline plan. Local paths above identify this
observation, not a portable connection. No diagnostic policy override was used.

## Selected environment and provenance

- Source base: merged `e48504f` (PR #487). Production dependency files and
  diagnosis rules were unchanged. The official-protocol helper remains in its
  existing isolated nested Go module.
- Kubernetes v1.33.1, Docker driver/cgroup v2, dedicated `wp-diag-458` profile,
  four CPUs / 6144 MiB; Helm Milvus chart 5.0.31 and Prometheus 3.0.1 with
  15-second scrapes. Cluster lifecycle was an external operator action.
- Milvus developer ARM64 image
  `harbor-us-vdc.zilliz.cc/milvusdb/milvus:enhance-woodpecker-v0.1.46-3.0-20261012-27bd28d206-v2-arm64`,
  image ID `56f66c0b875ae2a49ff54a2b1f43e81441a92d3cecc9d938c68e17434b51dea2`.
  The inspected Milvus source was `27bd28d20688ab00cdd74232a20e988fb2162aed`.
- WP service v0.1.46, image ID
  `4bef58cf01716909e6b2ab2093eb06c98489d8980b9822f4a132ca8e27d2d5f2`.
- MinIO `milvusdb/minio:RELEASE.2024-12-18T13-15-44Z`, image ID
  `8ce3a2b6c83521fabbde58b4f06e5fb7f95e64bfc05bc10def264479327c407b`.
- Same storage Pod `b3d9d65f-43a0-474b-b77f-de51a22e7d57`, restart count 0;
  same controller `50a10940-916d-4816-acb2-d4ce41dda6a9` and Bound claim
  `2eba3486-598e-4ff4-96f9-4e7be890ff36` throughout the measured run.
- Memory remained limit 512 MiB / request 128 MiB. The fixture baseline was
  CPU limit 1 / request 100m. Only the original Pod's CPU request/limit changed
  to 25m, then back to the exact baseline. Kernel `cpu.max` verified
  `100000 100000` → `2500 100000` → `100000 100000`.
- Mounted Milvus ConfigMap data SHA-256 remained
  `2fc303771ab68f6acfd473faa70b8a555dc4f6fe019ef988f3c0ca47964da560`.
  Milvus/WP Pod UIDs, image IDs and pre-existing restart counts stayed identical
  before/after; nonzero pre-existing counts were not hidden or reset.
- The management API masks the MinIO address/port. Mounted file precedence and
  Service/controller/Pod bindings were verified; unreadable effective overrides
  remain a gap. The preparation parser was PyYAML 6.0.2 in this recorded run;
  the guide uses the example's pinned preparation dependency.

This establishes this build/profile only. An unavailable/changed metric family,
unsupported cgroup interface or incompatible image remains a coverage failure;
this is not certification of every public Milvus release.

## Actual workload and observed contrast

The owned collection used two shards and 12 explicit partitions. Each active
phase inserted 24 × 16 rows, read each batch's marker with Strong Query/Search,
and submitted two real gRPC Flush requests with at least the native quota gap.
Each seal returned 24 actual segment IDs. A separate completed REST flush and
all-ID query/search followed. Default flush rate limits were preserved.

| Phase | User Insert P95 | Query / Search P95 | Qualified sync completions | Sync P95 histogram bounds | Exact IDs after completed flush |
| --- | --- | --- | --- | --- | --- |
| Baseline | 58.54 ms | 26.01 / 14.57 ms | 48 | 111.63–244.95 ms | 448 |
| Idle | No application traffic | No application traffic | Positively observed zero increase | No latency distribution inferred | Existing data retained |
| CPU-limited storage | 185.59 ms | 113.36 / 13.83 ms | 48 | 27.34–60.00 s | 832 |
| Normal recovery | 29.86 ms | 20.93 / 12.97 ms | 48 | 111.63–244.95 ms | 1216 |

The initial 64 rows plus three 384-row phases total 1216 acknowledged IDs.
Their logical float-vector payload is about 76 KiB, excluding protocol,
storage-format, index and metadata overhead. No request timed out in this run.
The two Flush acknowledgements per phase took about 10–20 ms; they are sealing
responses, not storage-completion measurements. The final completed-flush waits
were about 0.54–0.56 seconds after recovery/quiescence.

The quota was verified applied at `00:50:03.170916Z` and restored at
`00:51:07.863125Z`. The slow metric window was
`00:49:45.918338Z`–`00:51:35.630085Z`, deliberately including completed work after
resource recovery. Its actual pending samples were `0,0,24,29,24,15,0`, peak 29.
This includes queued **and executing** work and does not establish sustained
queue saturation. Scrapes do not prove continuous state between samples.

At the unchanged 1000 ms policy and 20-completion minimum, the existing assessor
returned `observed_slow_sync_execution`, `observed_slow_save_completions` and
`observed_slow_storage_put_completions`. Recovery qualified within-policy
execution completions. The histogram population was the same actual StreamingNode
exporter (`10.244.0.68:9091`, node 29); no request/tenant identity was invented.

The user requests remained comparatively fast while background persistence
slowed. Inclusive sync execution includes upload and callbacks; its P95 is not
client latency, isolated object-store CPU time, disk/fsync time or an additive
piece of another percentile. Checkpoint/seen observations and the separate WP
maintenance branch retained their original meanings and mapping gaps. An
exported WP admission warning also remained visible in baseline/recovery; passing
this selected sync contrast is not an all-component health verdict.

## Reuse, historical controls and restoration

Every phase's standalone `milvus persistence` and integrated `run --flow write
--stages persistence --sources prometheus` reports matched findings, targets,
uncertainty and original source/observation lineage. The historical slow interval
still produced the slow-completion finding after normal recovery.

Absent namespace and wrong exporter controls completed with no qualified working
population; the unreachable backend failed with exit 1. None could pass the
working gate. Completion status alone was not treated as healthy.

Kubernetes's resource-status CPU fields remained cached during a verified quota
change. The harness retained those fields and independently read the actual
container `cpu.max`; a desired-state patch alone was not accepted as enforcement.
The final exact resource object, original Pod/image and PVC identities were
verified before the owned collection was dropped and the original lock released.

The example initially had no MinIO CPU limit. A separate operator preparation
recorded the original template/resources and applied an explicit 1-CPU fixture
baseline. That preparation rolled the Pod **before** measurement; teardown
restored the original unlimited template through its normal rollout **after**
measurement and lock release. This is distinct from the no-restart CPU contrast.
The dedicated profile is returned to its original stopped state after evidence
collection; the harness itself performs no cluster lifecycle operations.

## Earlier failed attempts are retained

| Local run suffix / time | Failure and disposition |
| --- | --- |
| `20261027T144502Z-9de50c0b` | Management route absent on SN; failed preflight, no data/fault, lock released. The coordinator owns that route. |
| `20261027T144823Z-331f74db` | Address/port masked, not a readable effective binding; failed preflight, no fault, lock released. Mounted binding checks and explicit runtime gap replaced the unsupported assumption. |
| `20261027T145100Z-6d96d359` | Per-batch flush exceeded the native per-collection quota. Two additional acknowledged marker inserts remain in its raw failed RPC receipt; the complete-phase count was not a full count of those partial acknowledgements. Owned data/lock were cleaned. No quota or diagnostic threshold was relaxed. |
| `20261027T220248Z-12187651` | Sixteen shards exceeded this deployment's two physical channels. Creation failed; unique-name cleanup and lock release completed. The final workload uses owned partitions on two shards. |
| `20261027T222244Z-fb15d296` | Actual full object-store outage blocked the first WAL Insert until its eight-second deadline. It did not reproduce the intended successful persistence-completion contrast. Original replicas/claims were restored and owned data/lock cleaned. Timed-out mutation commit remained uncertain; no data-integrity success was inferred. |
| `20261027T223346Z-d9018b3f` | Cached CPU status caused an overly strict convergence failure; Kubernetes then rejected removing a CPU limit newly added by resize. Separate UID-preconditioned operator recovery recreated only the original storage Pod from the unchanged template, verified original resources/PVCs, cleaned owned data/lock and preserved the failed receipt. `manual-recovery.json` records that intervention. Unlimited baselines are now rejected before mutation. |
| `20261027T225650Z-5e29222c` | Explicit limit made restoration reversible, but the entire CPU status allocation remained cached. This failed attempt restored baseline resources/data/lock. Bounded no-data control probes confirmed actual kernel quota enforcement; the final harness verifies cgroup state. |

These attempts are not converted into passes. Separate control probes are not
counted as deployed workload acceptance or independent source confirmations.

## Artifact fingerprints and checks

Original raw artifacts remain local under
`/tmp/wp-persistence-488-live/20261028T004636Z-c8f2fb37/`; private connections and
raw operational data are not committed. The executed Python source hashes match
the delivered harness/helpers, with no later runtime logic relabeled as tested.

| Artifact | SHA-256 |
| --- | --- |
| `acceptance.json` | `d86da56a3a695c50d798a4532a5d8e57085ea9167df2e430cfb32ed8a10c13d3` |
| `storage-restoration.json` | `26dfe717f0766ebef0cc163608a69ba5f0cc4dd5070d77e5812976217c68cfe4` |
| `slow_storage.persistence.json` | `1de7c8d7b239df37d6b83ac25cd27da17cd481228ba77f727147771f5c761f75` |
| `historical_after_recovery.integrated.json` | `60436209081a293a9ba658ebb9dd641b677887e0c0ffb3aac3e102069953e2e2` |
| `slow_storage.workload.json` | `303d3966ea21af498224ab6d24479dc5fb95dbc0c0eae1aeee23471abe8a2246` |
| Executed `wpdiag` | `f38a2faa9be8c6fa6e5d0a63918dcab854e2412edd07f060cd356c4199840778` |
| Executed protocol client | `1dd506f6d7ddf6aecf3834656522456f78ebc7772fe0c76fd1154f0a305b8a7f` |
| `persistence_acceptance.py` | `832bd42823cca9244e96369e0bf84bfbae223ea4760b785db8af7543ceae6a1c` |
| `persistence_fault.py` | `7652cf0f93e91dc5c5dea84ed773790e4b27cd2ca16cc25c77b31a728accc329` |

Eleven Go protocol/cancellation tests passed with `-race`; 63 Python evidence,
transport and lifecycle tests passed, including 18 new persistence cases.
Nested-module `go mod tidy -diff` was unchanged. Website generation and anchor
checks were idempotent. Those offline tests remain distinct from this deployed
receipt. No crash-durability, complete tracing, native WP fault or WebUI
acceptance is claimed by this child.

After the deployed run, the helper's help clarified the profile-specific bounds
and an extreme-start-ID overflow guard was added. That rejected-input regression
is covered by offline tests; the executed binary/hash above is preserved and is
not relabeled as containing that later guard. Normal owned-range RPC behavior
and the executed Python harness are unchanged.
