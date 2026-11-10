# Atomic WAL check inventory

This is the implemented catalog, not a claim of complete WAL observability.
The runtime catalog is authoritative for policies and formulas. Source-owned metric
rules/inspection live in `diagnostics/evidence`; new checks never call legacy
cross-check corroboration. The 52-point prototype inventory remains documented
in `diagnostics/wal/EVIDENCE.md` for migration history.

- 39 Woodpecker Metrics/resource definitions.
- 16 Trace definitions: independent duration/error checks for eight instrumented phases.
- `scenario all`: all 55 unique checks.
- Current metadata uses existing `wp meta`; Loki is not in the new catalog.

## Parameter contract

`requires.parameters` lists mandatory parameters, `optional_parameters` lists
supported optional filters, and `conditional_parameters` names the source setting
needed to enable a filter. The CLI engine and WebUI share this contract. Scope
fields unused by the selected checks are stripped from reusable bindings.

Every check requires an incident window and its own source. Node ID is mandatory
for S05, S06 and H01–H03. Reader name is supported by R04 Metrics; filesystem path
is supported by S06. Server log-level Metrics support Log namespace and Log ID.
Trace identities
require configured `identity_tags`; Request ID requires `request_id_tag`. None of
these 55 checks consumes client etcd, bucket or storage root path.

## Checks

| ID | Source | Name | Step | Input | Initial criterion |
| --- | --- | --- | --- | --- | --- |
| W01 | metrics | writer-latency | writer | woodpecker_client_writer_operation_latency | Abnormal: P99 lower bound ≥ 100 ms in consecutive blocks covering ≥ 3 min, each with ≥ 100 completions. |
| W02 | metrics | writer-errors | writer | woodpecker_client_writer_operation_latency | Abnormal: recorded error increase > 0. |
| W03 | metrics | writer-throughput | writer | woodpecker_client_writer_bytes_written | Context only; no workload baseline or throughput threshold is assumed. |
| W04 | metrics | append-submissions | client-append | woodpecker_client_append_requests_total | Context only; no workload baseline or throughput threshold is assumed. |
| W05 | metrics | append-pending | client-append | woodpecker_client_segment_handle_pending_append_ops | Growth sustained for at least 3 min; submission activity and a write stall are assessed separately. |
| W06 | metrics | client-append-latency | client-append | woodpecker_client_append_latency | Abnormal: P99 lower bound ≥ 100 ms in consecutive blocks covering ≥ 3 min, each with ≥ 100 completions. |
| W07 | metrics | writer-progress | writer | woodpecker_client_write_frontier_segment, woodpecker_client_write_frontier_entry | Report paired-position advancement as context; no stall or readable-backlog verdict. |
| W08 | metrics | replica-append-errors | client-append | woodpecker_client_replica_append_total | Abnormal: recorded error increase > 0. |
| S01 | metrics | server-append-errors | server-append | woodpecker_server_logstore_operations_total | Abnormal: recorded error increase > 0. |
| S02 | metrics | server-buffer-wait | server-buffer | woodpecker_server_buffer_wait_latency | Abnormal: P99 lower bound ≥ 100 ms in consecutive blocks covering ≥ 3 min, each with ≥ 100 completions. |
| S03 | metrics | server-flush-latency | server-flush | woodpecker_server_file_flush_latency | Abnormal: P99 lower bound ≥ 100 ms in consecutive blocks covering ≥ 3 min, each with ≥ 100 completions. |
| S04 | metrics | server-flush-throughput | server-flush | woodpecker_server_flush_bytes_written | Context only; no workload baseline or throughput threshold is assumed. |
| S05 | metrics | disk-rejections | server-disk | woodpecker_server_logstore_write_rejected_total | Abnormal: recorded error increase > 0. |
| S06 | metrics | server-disk-watermark | server-disk | woodpecker_server_logstore_disk_usage_ratio, woodpecker_server_logstore_disk_watermark_soft, woodpecker_server_logstore_disk_watermark_hard | Abnormal: usage peak ≥ soft watermark. |
| S07 | metrics | server-read-latency | server-read | woodpecker_server_logstore_operation_latency | Abnormal: P99 lower bound ≥ 100 ms in consecutive blocks covering ≥ 3 min, each with ≥ 100 completions. |
| S08 | metrics | server-read-errors | server-read | woodpecker_server_logstore_operations_total | Abnormal: recorded error increase > 0. |
| R01 | metrics | reader-throughput | reader | woodpecker_client_reader_bytes_read | Context only; no workload baseline or throughput threshold is assumed. |
| R02 | metrics | reader-iteration-latency | reader | woodpecker_client_reader_operation_latency | Abnormal: P99 lower bound ≥ 100 ms in consecutive blocks covering ≥ 3 min, each with ≥ 100 completions. |
| R03 | metrics | reader-errors | reader | woodpecker_client_reader_operation_latency | Abnormal: recorded error increase > 0. |
| R04 | metrics | reader-progress | reader | woodpecker_client_read_frontier_segment, woodpecker_client_read_frontier_entry | Report paired-position advancement as context; no stall or readable-backlog verdict. |
| R06 | metrics | direct-read-latency | direct-read | woodpecker_client_direct_read_latency | Abnormal: P99 lower bound ≥ 100 ms in consecutive blocks covering ≥ 3 min, each with ≥ 100 completions. |
| R07 | metrics | direct-read-errors | direct-read | woodpecker_client_direct_read_requests_total | Abnormal: recorded error increase > 0. |
| R08 | metrics | quorum-read-errors | replica-read | woodpecker_client_quorum_read_total | Abnormal: recorded error increase > 0. |
| R09 | metrics | read-failovers | replica-read | woodpecker_client_read_failover_total | Report observed failovers as context; a failover alone does not establish a failed read. |
| R10 | metrics | read-branch-activity | replica-read | woodpecker_client_quorum_read_total, woodpecker_client_direct_read_requests_total | Positive increase establishes recorded branch activity. Missing series do not establish an unused branch. |
| B01 | metrics | segment-rolls | maintenance | woodpecker_client_segment_rolled_total | Context only; no error or slowness verdict from this count. |
| B02 | metrics | compaction-errors | maintenance | woodpecker_client_segment_compaction_failures_total | Abnormal: recorded error increase > 0. |
| B03 | metrics | compaction-progress | maintenance | woodpecker_client_compaction_frontier_segment, woodpecker_client_compaction_frontier_entry | Report paired-position advancement as context; no stall or readable-backlog verdict. |
| H01 | metrics | cpu-utilization | resources | woodpecker_server_system_cpu_usage | Abnormal: that interval lasts ≥ 3 min. |
| H02 | metrics | io-wait-rate | resources | woodpecker_server_system_io_wait | Context only, in cumulative seconds / wall second. No percentage threshold. |
| H03 | metrics | memory-utilization | resources | woodpecker_server_system_memory_usage_ratio | Abnormal: that interval lasts ≥ 3 min. |
| OR01 | metrics | reader-open-success | open-reader | woodpecker_client_log_handle_operations_total | Context only; no error or slowness verdict from this count. |
| OR02 | metrics | reader-open-errors | open-reader | woodpecker_client_log_handle_operations_total | Abnormal: recorded error increase > 0. |
| OR03 | metrics | reader-open-latency | open-reader | woodpecker_client_log_handle_operation_latency | Abnormal: P99 lower bound ≥ 100 ms in consecutive blocks covering ≥ 3 min, each with ≥ 100 completions. |
| OR04 | metrics | reader-open-error-latency | open-reader | woodpecker_client_log_handle_operation_latency | Abnormal: P99 lower bound ≥ 100 ms in consecutive blocks covering ≥ 3 min, each with ≥ 100 completions. |
| OW01 | metrics | writer-open-success | open-writer | woodpecker_client_log_handle_operations_total | Context only; no error or slowness verdict from this count. |
| OW02 | metrics | writer-lock-errors | open-writer | woodpecker_client_log_handle_operations_total | Abnormal: recorded error increase > 0. |
| OW03 | metrics | writer-fencing-errors | open-writer | woodpecker_client_log_handle_operations_total | Abnormal: recorded error increase > 0. |
| OW04 | metrics | writer-open-error-latency | open-writer | woodpecker_client_log_handle_operation_latency | Abnormal: P99 lower bound ≥ 100 ms in consecutive blocks covering ≥ 3 min, each with ≥ 100 completions. |
| TW01 | trace | writer-operation-duration | writer | Write, WriteAsync | Recorded span duration > 1000 ms |
| TW02 | trace | writer-operation-errors | writer | Write, WriteAsync | At least one matching span has an error flag; application outcome is not inferred. |
| TA01 | trace | client-append-duration | client-append | Execute | Recorded span duration > 1000 ms |
| TA02 | trace | client-append-errors | client-append | Execute | At least one matching span has an error flag; application outcome is not inferred. |
| TS01 | trace | server-append-duration | server-append | AddEntry, woodpecker.proto.logstore.LogStore/AddEntry, woodpecker.proto.logstore.LogStore/AddEntries | Recorded span duration > 1000 ms |
| TS02 | trace | server-append-errors | server-append | AddEntry, woodpecker.proto.logstore.LogStore/AddEntry, woodpecker.proto.logstore.LogStore/AddEntries | At least one matching span has an error flag; application outcome is not inferred. |
| TF01 | trace | server-flush-duration | server-flush | processFlushTask | Recorded span duration > 1000 ms |
| TF02 | trace | server-flush-errors | server-flush | processFlushTask | At least one matching span has an error flag; application outcome is not inferred. |
| TR01 | trace | reader-operation-duration | reader | ReadNext | Recorded span duration > 1000 ms |
| TR02 | trace | reader-operation-errors | reader | ReadNext | At least one matching span has an error flag; application outcome is not inferred. |
| TB01 | trace | server-read-duration | server-read | GetBatchEntriesAdv, woodpecker.proto.logstore.LogStore/GetBatchEntriesAdv | Recorded span duration > 1000 ms |
| TB02 | trace | server-read-errors | server-read | GetBatchEntriesAdv, woodpecker.proto.logstore.LogStore/GetBatchEntriesAdv | At least one matching span has an error flag; application outcome is not inferred. |
| TO01 | trace | open-reader-duration | open-reader | OpenLogReader | Recorded span duration > 1000 ms |
| TO02 | trace | open-reader-errors | open-reader | OpenLogReader | At least one matching span has an error flag; application outcome is not inferred. |
| TP01 | trace | open-writer-duration | open-writer | OpenLogWriter, OpenInternalLogWriter | Recorded span duration > 1000 ms |
| TP02 | trace | open-writer-errors | open-writer | OpenLogWriter, OpenInternalLogWriter | At least one matching span has an error flag; application outcome is not inferred. |

## Scenario manifests

The same manifest feeds CLI, Skills discovery and WebUI. No data-dependent
verdict is added during grouping; individual check results stay unchanged.

| Scenario | Check IDs | Coverage notes |
| --- | --- | --- |
| slow-reads | S07, S08, R01, R02, R03, R06, R07, R08, R09, R10, H01, H02, TR01, TR02, TB01, TB02 |  |
| slow-writes | W01, W02, W03, W04, W05, W06, W08, S01, S02, S03, S04, S05, S06, H01, H02, H03, TW01, TW02, TA01, TA02, TS01, TS02, TF01, TF02 |  |
| stuck-reads | S08, R01, R03, R04, R07, R08, R09, R10, TR01, TR02, TB01, TB02 | Historical readable backlog and expected demand are not established by these position/throughput checks. |
| stuck-writes | W02, W04, W05, W07, W08, S03, S05, S06, B01, B02, B03, TW01, TW02, TA01, TA02, TF01, TF02 |  |
| open-reader | OR01, OR02, OR03, OR04, TO01, TO02 |  |
| open-writer | OW01, OW02, OW03, OW04, TP01, TP02 |  |
| all | W01, W02, W03, W04, W05, W06, W07, W08, S01, S02, S03, S04, S05, S06, S07, S08, R01, R02, R03, R04, R06, R07, R08, R09, R10, B01, B02, B03, H01, H02, H03, OR01, OR02, OR03, OR04, OW01, OW02, OW03, OW04, TW01, TW02, TA01, TA02, TS01, TS02, TF01, TF02, TR01, TR02, TB01, TB02, TO01, TO02, TP01, TP02 |  |

## Intentional gaps and boundaries

- Historical readable backlog and expected workload are not reconstructed from current metadata or an unchanged position.
- Pending growth is a trend observation; it does not require or imply submitted demand or a write stall.
- Metrics and Trace timers need not measure the same interval. Reader metrics reset per iteration, while ReadNext spans include tail waits.
- Trace service scope does not identify a Metrics log unless emitted target/request tags actually establish it.
- Duration/error checks only cover exported sampled spans; cancellation/error semantics may be unresolved.
- Upload, cleanup/recovery correctness and filesystem occupancy attribution do not have a complete validated diagnostic path here. Existing flush/compaction/roll observations are context, not proof of those tasks' correctness.
- Host I/O-wait has no matching CPU denominator for a utilization percentage; it stays context.
- No metadata watchers or automatic repair. All Milvus-owned diagnosis and Loki analysis are outside the product scope.
