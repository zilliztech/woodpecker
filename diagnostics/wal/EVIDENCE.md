# Evidence implementation audit

The 52 proposed points are an inventory, not a count of validated automated
checks. This audit is for the problem-oriented WAL workbench (#503).

- **reuse**: call an existing read-only wpcli view/check; preserve its contract.
- **adapt**: reuse the existing Prometheus client, auth, scope, bounded raw-matrix
  parser and quality checks; apply a new versioned WAL problem rule.
- **defer**: no automatic decision in this workbench; retain an explicit Unknown
  and direct operators to independent tools instead of inventing a correlation.

New code consists of the catalog planner, P99/sustained/position/resource rules,
problem report API/UI, and optional supplied-findings Agent adapter. The legacy
P95 and source/stage CLI contracts remain unchanged.

| Point | Step | Decision | Owner / actual evidence |
| --- | --- | --- | --- |
| W01 | Writer operation p99 | adapt | woodpecker_client_writer_operation_latency |
| W02 | Failed Writer operations | adapt | woodpecker_client_writer_operation_latency |
| W03 | Successful Writer byte rate | adapt | woodpecker_client_writer_bytes_written |
| W04 | Submitted append entry rate | adapt | woodpecker_client_append_requests_total |
| W05 | Pending append growth | adapt | woodpecker_client_segment_handle_pending_append_ops |
| W06 | Append completion p99 | adapt | woodpecker_client_append_latency |
| W07 | Acknowledged write-position progress | adapt | woodpecker_client_write_frontier_segment, woodpecker_client_write_frontier_entry |
| W08 | Failed replica append outcomes | adapt | woodpecker_client_replica_append_total |
| S01 | Failed AddEntry method calls | adapt | woodpecker_server_logstore_operations_total |
| S02 | Buffer notification p99 | adapt | woodpecker_server_buffer_wait_latency |
| S03 | Flush execution p99 | adapt | woodpecker_server_file_flush_latency |
| S04 | Completed Flush byte rate | adapt | woodpecker_server_flush_bytes_written |
| S05 | Disk-protection write rejections | adapt | woodpecker_server_logstore_write_rejected_total |
| S06 | WAL disk usage vs configured watermarks | adapt | woodpecker_server_logstore_disk_usage_ratio, woodpecker_server_logstore_disk_watermark_soft, woodpecker_server_logstore_disk_watermark_hard |
| S07 | Server batch-read p99 | adapt | woodpecker_server_logstore_operation_latency |
| S08 | Failed server batch-read calls | adapt | woodpecker_server_logstore_operations_total |
| R01 | Delivered Reader byte rate | adapt | woodpecker_client_reader_bytes_read |
| R02 | Reader loop-iteration p99 | adapt | woodpecker_client_reader_operation_latency |
| R03 | Failed read_next observations | adapt | woodpecker_client_reader_operation_latency |
| R04 | Reader position progress | adapt | woodpecker_client_read_frontier_segment, woodpecker_client_read_frontier_entry |
| R05 | Readable backlog at the incident time | defer | Time-aligned readable-tail capture + Reader position |
| R06 | Direct object-read p99 | adapt | woodpecker_client_direct_read_latency |
| R07 | Failed direct object reads | adapt | woodpecker_client_direct_read_requests_total |
| R08 | Failed quorum-read candidate outcomes | adapt | woodpecker_client_quorum_read_total |
| R09 | Read failover events | adapt | woodpecker_client_read_failover_total |
| R10 | Observed read-branch activity | adapt | woodpecker_client_quorum_read_total, woodpecker_client_direct_read_requests_total |
| B01 | Completed segment rolls | adapt | woodpecker_client_segment_rolled_total |
| B02 | Compaction failures | adapt | woodpecker_client_segment_compaction_failures_total |
| B03 | Compaction frontier progress | adapt | woodpecker_client_compaction_frontier_segment, woodpecker_client_compaction_frontier_entry |
| H01 | Node CPU usage | adapt | woodpecker_server_system_cpu_usage |
| H02 | Node I/O-wait activity | adapt | woodpecker_server_system_io_wait |
| H03 | Node memory usage ratio | adapt | woodpecker_server_system_memory_usage_ratio |
| OR01 | Successful Reader opens | adapt | woodpecker_client_log_handle_operations_total |
| OR02 | Failed Reader opens | adapt | woodpecker_client_log_handle_operations_total |
| OR03 | Successful Open Reader p99 | adapt | woodpecker_client_log_handle_operation_latency |
| OR04 | Failed Open Reader p99 | adapt | woodpecker_client_log_handle_operation_latency |
| OR05 | Matching Reader open error | defer | Application error / matching log event |
| OW01 | Successful Writer opens | adapt | woodpecker_client_log_handle_operations_total |
| OW02 | Writer-lock acquisition failures | adapt | woodpecker_client_log_handle_operations_total |
| OW03 | Writer fencing failures | adapt | woodpecker_client_log_handle_operations_total |
| OW04 | Open Writer error timing | adapt | woodpecker_client_log_handle_operation_latency |
| OW05 | Matching Writer open error | defer | Application error / matching log event |
| M01 | Log metadata snapshot | reuse | Existing read-only wpcli command |
| M02 | Segment metadata snapshot | reuse | Existing read-only wpcli command |
| M03 | Quorum metadata snapshot | reuse | Existing read-only wpcli command |
| M04 | Existing wpcli meta-check findings | reuse | Existing read-only wpcli command |
| T01 | Correlated WP write span | defer | Trace span with matching log / request identity |
| T02 | Correlated WP read span | defer | Trace span with matching log / request identity |
| T03 | Correlated open-operation span | defer | Trace span with matching log / request identity |
| MV01 | Milvus producer reservation delay | adapt | milvus_streaming_produce_rate_limit_delay_seconds |
| MV02 | Milvus WAL implementation append p99 | adapt | milvus_wal_impls_append_message_duration_seconds |
| MV03 | Milvus Scanner pending delivery bytes | adapt | milvus_wal_scanner_pending_queue_bytes |

The runtime UI exposes each point's actual metric names, label scope,
preconditions, rule, native units, source references and limits. Targets and
source identity are explicit. Deferred trace, log and readable-tail points never
return synthetic observations or passed checks.

Milvus evidence is opt-in with an explicit channel mapping. A metric lacking
the required association remains Unknown. Current metadata views cannot clear
a historical incident. No general Search/Query or data-file persistence assessor
is added to this new workbench.

H02 was corrected during deployed-source validation: its raw metric is cumulative
seconds, not a percentage. It reports a qualified interval rate as Context; the
GOMAXPROCS gauge cannot establish a host CPU denominator for a utilization rule.
