# v0.1.46 operational documentation coverage

Review date: 2026-10-10. This is a review of **current master**, including merged PR #413,
against [milestone v0.1.46](https://github.com/zilliztech/woodpecker/milestone/25).
It is not a claim that every milestone issue is implemented or that a release tag has shipped.
Issue descriptions include original proposals; the merged code and closing decisions determine
what operators can actually do.

The website [operations guide](../../operations.html) covers the diagnostic decisions and
recovery boundaries. The [cookbook](cookbook.md) supplies executable command templates;
[configuration](configuration.md) explains discovery and external addressing, and the
[quickstart](quickstart.md) introduces the main paths.

## Delivered operational paths

| Issues | Current behavior | Documentation |
| --- | --- | --- |
| [#361](https://github.com/zilliztech/woodpecker/issues/361) | Partial views are reported, no replies fails applicable commands, explicit node addresses work, healthz/log-health have commands, and CLI config can travel beside the binary. | Operations setup, health and script outcomes; cookbook 13–14; configuration discovery order. |
| [#360](https://github.com/zilliztech/woodpecker/issues/360) | Refuse unusable discovered etcd endpoints; explicit endpoint/prefix and TLS/auth overrides remain available. | Operations metadata setup; cookbook 11 and 14; configuration metadata access. |
| [#368](https://github.com/zilliztech/woodpecker/issues/368) | Config/env diff distinguishes unreachable from identical; no replies is an error. | Operations drift workflow; cookbook 19. |
| [#369](https://github.com/zilliztech/woodpecker/issues/369) | `logstore lac` assembles per-node durable positions for Active segments and uses metadata ends for settled segments. | Operations writer/LAC section; cookbook 15–16. |
| [#357](https://github.com/zilliztech/woodpecker/issues/357) | Client auditor rolls non-empty idle Active segments using the existing policy; a new empty Active segment has no local data file. | Operations drain workflow; cookbook 1. |
| [#378](https://github.com/zilliztech/woodpecker/issues/378) | Bounded connecting/sending/selection and quorum reads, plus rolling away from failed replicas; not a 2s end-to-end promise. | Operations writer timeout explanation; cookbook 15. |
| [#380](https://github.com/zilliztech/woodpecker/issues/380) | Finalize batches index/footer writes; completion still contributes latency and is distinct from sending. | Operations writer/compaction workflow; cookbook 15 and 18. |
| [#387](https://github.com/zilliztech/woodpecker/issues/387) | `fence-quorum` validates original members and requires `wq-aq+1`; previews do not execute, partial executions can change some nodes. | Operations deliberate-fence steps; cookbook 15. |
| [#389](https://github.com/zilliztech/woodpecker/issues/389) | `log readers` reads leased checkpoints, report ages and open/current positions; comparisons need reporting cadence. | Operations reader workflow; cookbook 16. |
| [#391](https://github.com/zilliztech/woodpecker/issues/391) | `segment probe` reads each replica; subsequent `segment inspect` (PR #394) surveys blocks beyond readable failures when structure permits. | Operations probe/inspect interpretation; cookbook 16. |
| [#397](https://github.com/zilliztech/woodpecker/issues/397) | Whole-log quick/raw scan reconciles metadata with replica coverage, distinguishing established loss, unknown and compacted shared storage. | Operations whole-log workflow; cookbook 16. |
| [#395](https://github.com/zilliztech/woodpecker/issues/395) | Peers can readmit a abruptly killed node at a new address; observing convergence is still required. | Operations node replacement steps; cookbook 1. |
| [#400](https://github.com/zilliztech/woodpecker/issues/400), [#401](https://github.com/zilliztech/woodpecker/issues/401) | `log skip-range` declarations have a quorum evidence gate and CAS; stalled readers consult the record on reporting ticks and can advance. This is accepted loss, not repair, unconditional skipping, or immediate propagation. | Operations skip-range recovery and withdrawal; cookbook 17; CLI README. |
| [#408](https://github.com/zilliztech/woodpecker/issues/408) | Full-scan writer recovery logs stops on incomplete/undecodable/unexpected records, with location and discarded bytes. | Operations recovery/compaction boundary; cookbook 18. |
| [#410](https://github.com/zilliztech/woodpecker/issues/410) | E2E coverage pins Active/Completed corruption, surviving replicas, reader skip recovery and damaged-segment truncation. No new repair command. | Operations corruption state table; cookbook 18. |
| [#412](https://github.com/zilliztech/woodpecker/issues/412) | Shared node admin URL mappings cover quorum, memberlist and single-node targets, including explicitly mapped historical members. | Operations external access; configuration mapping precedence; cookbook 14. |
| [#362](https://github.com/zilliztech/woodpecker/issues/362) | This documentation issue is addressed by PR #363; the guide retains actual unsupported capabilities rather than assuming them complete. | Operations guide and companion CLI guides. |

## Closed issues that do not mean a new capability shipped

| Issue | Actual disposition | Documentation consequence |
| --- | --- | --- |
| [#371](https://github.com/zilliztech/woodpecker/issues/371) | Closed as not planned; PR #373 was not merged. Submitted-position/oldest-queue-age and auditor-outcome metrics did not land. | Retain the client-side visibility boundary. Use existing confirmed frontiers, server op ages and auditor summary logs. Do not invent metric names or a command exposing another process's queue. |
| [#358](https://github.com/zilliztech/woodpecker/issues/358) | Closed as not planned; neither terminal checksum errors nor a new checksum counter shipped. Transient tail failures must continue retrying. | Use reader positions, quorum LAC and targeted surveys. No counter/background scrub is claimed. |
| [#359](https://github.com/zilliztech/woodpecker/issues/359) | The initial unbounded-durability premise was withdrawn; both append phases already had bounds. Later send/read/connect improvements are documented from the actual code. | Separate per-phase bounds from total latency; do not claim the broader #229 audit is complete. |

## Still-open capabilities

| Issue | Boundary to preserve |
| --- | --- |
| [#254](https://github.com/zilliztech/woodpecker/issues/254) | No general `wp meta show/diff/repair`; marking confirmation and skip declarations only edit their specific records. |
| [#229](https://github.com/zilliztech/woodpecker/issues/229) | The systematic coherent-timeout audit remains open. Current send/read budgets do not establish deadlines for every control-plane chain. |
| [#327](https://github.com/zilliztech/woodpecker/issues/327) | Completed segments can still wait for an unavailable copy required to cover their end; restore availability before treating a silent replica as proven lost. |
| [#280](https://github.com/zilliztech/woodpecker/issues/280) | Quorum metadata representation changes are still proposed. Mappings do not rewrite stored identities or infer new membership. |
| [#277](https://github.com/zilliztech/woodpecker/issues/277) | Use-after-close reader enforcement is not delivered. Follow the application's close/reopen lifecycle; withdrawal does not rewind a reader. |

These states are a dated review, not a substitute for the current issue tracker.

## Regression and infrastructure changes

| Issues / PRs | Operational effect |
| --- | --- |
| [#405](https://github.com/zilliztech/woodpecker/issues/405), [#406](https://github.com/zilliztech/woodpecker/pull/406) | A finalized staged replica does not answer EOF below its footer LAC. Diagnose coverage against the confirmed/completed end, not a transient stale reader LAC. |
| [#403](https://github.com/zilliztech/woodpecker/issues/403), [#404](https://github.com/zilliztech/woodpecker/pull/404) | Stability assertions account for the cost of a silent completion replica. Test budgets are not CLI or append SLA settings. |
| [#376](https://github.com/zilliztech/woodpecker/issues/376) | CI MinIO/mc image-source repair; no new operator command. |

All 31 items returned by the milestone review are accounted for above. The scope also includes
merged `segment inspect` behavior even though its own issue is outside that milestone listing.
