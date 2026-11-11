# Deployed consume startup / visibility acceptance — 2026-10-27

This is the focused [#484](https://github.com/zilliztech/woodpecker/issues/484)
follow-up to the first CLI admission subset of #458. It validates **new scanner
startup and initial collection visibility**, not an already-loaded reader stall,
generic query performance, disk I/O or the complete diagnosis program.
See [CONSUME.md](CONSUME.md) for preparation, execution and explicit recovery.

## Selected build and fixture

- Run: `20261027T090037Z-3f43a050`, UTC
  `2026-10-27T09:00:37.184070Z` to `2026-10-27T09:06:14.452542Z`.
- Diagnostic source base: master `d4511a259ce796a5584bdef5436d2599a7906d0a`
  (merged #483), Go 1.25.0 / Darwin ARM64. No diagnostic Go rules or thresholds
  changed for this scenario.
- Externally resumed, task-owned minikube profile/context `wp-diag-458`,
  namespace `wp-diag-458`, Kubernetes v1.33.1, four CPUs / 6144 MiB.
  Harness and Helm commands named the context explicitly; the harness did not
  manage Kubernetes lifecycle or change the current context.
- Official Milvus chart 5.0.31, release revision 4; full-stack-support 0.1.0,
  revision 4; kube-prometheus-stack 67.4.0, revision 5; Prometheus v3.0.1 with
  15-second scrapes. Only core services and Prometheus were required.
- The previous admission test's dedicated deployment was reused after external
  startup and an explicit Helm baseline rollout. Old startup etcd connectivity
  retries finished before all selected Pods became Ready and measurement began.
  The baseline preserved the existing values and added explicit
  `streaming.walScanner.pauseConsumption: false`; DataNode limit remained 2 GiB.

| Component | Actual image | Runtime image configuration SHA-256 |
| --- | --- | --- |
| Milvus roles | `harbor-us-vdc.zilliz.cc/milvusdb/milvus:enhance-woodpecker-v0.1.46-3.0-20261012-27bd28d206-v2-arm64` | `56f66c0b875ae2a49ff54a2b1f43e81441a92d3cecc9d938c68e17434b51dea2` |
| Three WP service Pods | `milvusdb/woodpecker:v0.1.46` | `4bef58cf01716909e6b2ab2093eb06c98489d8980b9822f4a132ca8e27d2d5f2` |

The runtime images were the older metrics-compatible combination, not the custom
full-trace images from #483. This scenario does not need that propagation fix or
claim that these images support complete write tracing. The Helm chart's default
appVersion is not substituted for the actual overridden runtime image.

Diagnostic binary SHA-256:
`4752596f30725c214aa972b0c9c18a992032cc7febe13f0534949e3fd2629c38`.
The runtime receipt also recorded exact Pod UIDs, image IDs, restart counts and
harness hashes. Selected Pod inventories were identical before/after and every
selected container had zero restarts during the scenario.

## Command and workload

```bash
python3 tests/integration/diagnostics/consume_acceptance.py \
  --context wp-diag-458 --namespace wp-diag-458 \
  --output /tmp/wp-consume-484-live --execute
```

Exit status **0**. Defaults were four Insert requests/second, 16 rows of 16
dimensions/request, 30-second measurement phases, 16-second scrape padding,
eight-second setting propagation allowance and four-second query probe deadline.
Each working phase acknowledged 120 requests / 1920 rows. There were also two
16-row priming Inserts, for 5792 total acknowledged rows across two owned
collections. These are correctness/observability loads, not capacity benchmarks.

The authoritative REST-described collection IDs were
`470047948178401951` (existing working control) and `470047948178402906` (new
paused subscription). The Prometheus working-traffic gate required actual
MsgPack counter increase for the matching collection ID. It did not substitute
global exporter activity, a WP service context or guessed reader metadata.

| Phase | Inserts / rows during measurement | New/selected collection Strong probes | Other evidence |
| --- | --- | --- | --- |
| Baseline | 120 / 1920 | 51 successful; p95 180.54 ms | Exact 1936 IDs including prime and vector search passed; positive owned-collection consumption |
| Intentional idle | 0 / 0 | 58 successful; p95 29.58 ms | Same exact 1936 IDs/search; no scanner/processing pressure hypothesis |
| New scanner startup paused | 120 / 1920 | Seven attempts reached the client deadline, 4003.53–4010.55 ms | All seven existing-collection control queries succeeded; new collection remained `LoadStateLoading`, progress 0 |
| Recovery | 120 / 1920 | 51 successful; p95 169.45 ms | Exact 3856 IDs in the new collection and vector search passed; positive owned-collection consumption |

The seven timeout durations are **censored failed-attempt durations**. They are
not completed query latency quantiles or a measurement of an exclusive four-second
scanner queue. Strong reads during the fault did not return incorrect successful
responses. This is not a claim that a previously loaded reader was stopped.

## Source evidence and interpretation

The only configuration fault was `streaming.walScanner.pauseConsumption=true`.
Original false/FileSource provenance was journaled before mutation; the control
was read back as an EtcdSource override, then restored to false/FileSource with
the override removed. Consumers bypassing this startup gate were not reclassified
as paused. Existing-collection queries supplied the paired working control.

Retained source facts on StreamingNode node 18, exporter `10.244.0.44:9091`:

| Phase | Physical channel `dml_0` pause | Physical channel `dml_1` pause | Pending-queue bytes |
| --- | --- | --- | --- |
| Baseline | All retained samples 0 | All retained samples 0 | Both populations 0 |
| Idle | All retained samples 0 | All retained samples 0 | Both populations 0 |
| Startup pause | All retained samples 0 | All retained samples 1 | Both populations 0 |
| Recovery | All retained samples 0 | All retained samples 0 | Both populations 0 |

The actual full labels were `by-dev-rootcoord-dml_0` and
`by-dev-rootcoord-dml_1`; the metric is shared by scanners on a channel. These
facts do not identify an individual WP reader or establish a channel-to-WP-client
metadata/log mapping.

The stage returned `scanner_pause_state_observed`, and **did not** manufacture
`suspected_scanner_downstream_backpressure` from pause alone or
`suspected_consumer_processing_pressure` without qualifying evidence. Zero
pending work at a startup gate differs from blocked delivery of accumulated
downstream work. This distinction is part of the positive acceptance result,
not a reason to lower thresholds until a warning appears.

Standalone and composed results matched for baseline, idle, pause, recovery and
historical replay, including rule/version, outcome, impact/evidence strength,
flow/stage, target, summary and uncertainty. Source/observation references resolved
and integrated summary finding IDs retained their original identities. Historical
replay after recovery preserved the original pause interval rather than using
the current unpaused state to erase it.

Lazy/missing remote-only or WP retrieval populations remained explicit gaps.
Idle still permits TimeTick/control MsgPacks; their activity does not prove fresh
payload demand, per-row visibility or a stalled reader. The independent consume
command queried Prometheus only; management/workload evidence belongs to the
separate acceptance experiment.

## Negative controls, failures and restoration

- Wrong namespace selector: status `completed`, exit 0, no matching consumption
  facts, working-traffic gate rejected.
- Unreachable Prometheus: status `failed`, exit 1, working-traffic gate rejected.
- Original false/FileSource pause and override absence were restored. Both exact
  owned collection names were dropped/absence-checked and the original exclusive
  namespace lock was released. No existing collection was reused or deleted.
- First full attempt `20261027T085125Z-795c1de9` remains failed. The actual query
  deadlines occurred, but the prototype compared an exception type string with
  `TimeoutError`; Python 3.9 reported `socket.timeout` as `timeout`. It classified
  seven actual timeouts as generic failures. Structured sanitized HTTP error kinds
  now distinguish socket/builtin/wrapped timeouts from connection refusal and
  other failures; the acceptance gate was not widened to accept arbitrary errors.
  Configuration/data/lock cleanup still succeeded. A separate historical inspection
  retained its actual pause=1/pending=0 facts without rewriting the failed receipt.
- An earlier small exploratory probe attempted a configuration GET on the SN's
  management port; that route was unavailable on this build. It restored the
  applied pause and removed its owned data. The final runner uses the coordinator's
  supported API and requires actual source pause evidence after propagation.

After successful restoration/evidence collection, the external operator returned
only the task-owned profile to its original stopped state. Test data had already
been removed; receipts and retained local images were preserved.

The final additional unpaused two-scrape and strict pause-bound checks were also
evaluated against all five retained real source/child report pairs in
`post-run-contract-checks.json`; all passed. The in-process run used harness hash
`a8879e0ac6c8d18b2ec0fac40f35b6c09d30e91076ee5a2d7b241bfabc4e60cb`.
The strengthened post-run checker used
`1c6a5c5d26bcecc2a959a2d141ed66b8d47f3fca6a71cb1546d9510bf0d7d645`.
Idempotent already-absent cleanup and those extra gates have dedicated regressions;
the original receipt is preserved with its actual executed-code provenance.
The later baseline-journal extension is covered by explicit pre-fault and
post-fault process-loss recovery fixtures; the recorded deployment run predates
that extension.

## Regressions and artifact fingerprints

The combined HTTP/evidence/lifecycle suite passed **34 tests**, including the
existing admission controls. It covers exact override restoration, wrong target,
large collection IDs, missing/idle/foreign populations, timeout classification,
external changes, wrong lock ownership, explicit crash recovery, already-absent
owned data, failed artifact writes and failed-restoration lock retention. It uses
local fixtures, not Kubernetes fault injection in CI. Website generation was
idempotent; CLI build and whitespace checks passed.

Artifacts remain local under `/tmp/wp-consume-484-live/20261027T090037Z-3f43a050/`.
Do not share private connections/raw labels without review.

| Artifact | SHA-256 |
| --- | --- |
| `acceptance.json` | `705fa7cf20996c418a10e0d2d2d90730299bc8545136be409e2f6367a58df6a7` |
| `restoration.json` | `ce99f8a2f25d5727882f933e341ae34ff31456936d8ae9c5d7366f0f1b6b31d1` |
| `startup_paused.consume.json` | `668588926208db88b6e76d3d33e98dda55caf358122f72609d3bc3d1445a6e96` |
| `post-run-contract-checks.json` | `164052121acc25e842fa767c76e8f130c85523f1a7b744cd21c9ef7d48586add` |

This completes the focused startup/initial-visibility acceptance outcome. Loaded
reader stalls, broader query/persistence performance, real-stack WebUI, log
corroboration, restart/reset/scrape-gap scenarios and released full-trace image
acceptance remain separate work under #450/#459 and further follow-ups.
