#!/usr/bin/env python3
"""Generate ordered operational dashboards and their stage coverage contract."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / "charts/full-stack-support/dashboards"
SCOPE = 'cluster="$cluster",namespace="$namespace",woodpecker_id="$woodpecker_id"'
CHANNEL = 'channel_name=~"${channel:regex}"'
LOG = 'log_ns=~"${log_ns:regex}",log_id=~"${log_id:regex}"'
DS = {"type": "prometheus", "uid": "${datasource}"}
LINKS = [("Write / Append", "milvus-e2e-write"), ("WAL Read / Consume", "milvus-e2e-consume"),
         ("Flush / Checkpoint", "milvus-e2e-flush"), ("Search / Query", "milvus-e2e-query"),
         ("Milvus reference", "milvus-full-stack"), ("WP client reference", "woodpecker-client-k8s"),
         ("WP server reference", "woodpecker-server-k8s")]


def metric(name, extra=""):
    return name + "{" + SCOPE + ("," + extra if extra else "") + "}"


def rate(name, labels="pod", extra=""):
    return f"sum by ({labels}) (rate({metric(name, extra)}[$__rate_interval]))"


def gauge(name, labels="pod", extra="", aggregate="max"):
    return f"{aggregate} by ({labels}) ({metric(name, extra)})"


def line(title, expression, unit="short", legend="{{pod}}", description=""):
    return {"title": title, "unit": unit, "description": description,
            "targets": [{"expr": expression, "legendFormat": legend}]}


def latency(title, name, labels="pod", extra="", seconds=False):
    selector = metric(name + "_bucket", extra)
    suffix = " · ".join("{{" + key + "}}" for key in labels.split(","))
    return {"title": title, "unit": "ms", "description":
            "P50/P95/P99 of observed operations; no observations produce no data/NaN. "
            "Check the stage's observation rate. Quantiles from different stages cannot be added or subtracted. "
            "Values above the largest finite histogram bucket cannot be resolved accurately.",
            "targets": [{"expr": f"histogram_quantile({q}, sum by (le,{labels}) (rate({selector}[$__rate_interval])))"
                         + (" * 1000" if seconds else ""), "legendFormat": f"{label} · {suffix}"}
                        for q, label in ((.50, "P50"), (.95, "P95"), (.99, "P99"))]}


def probe(name, extra="", required=False):
    return {"metric": name, "expression": metric(name, extra), "required": required}


def stage(key, title, guidance, plots, probes):
    return {"key": key, "title": title, "guidance": guidance, "plots": plots, "probes": probes}


def delta(left, right, labels="pod,channel_name", left_extra=CHANNEL, right_extra=CHANNEL):
    # Timestamp gauges are exported as Unix seconds. Match identities before
    # subtraction; never subtract maxima belonging to unrelated channels/Pods.
    return (f"clamp_min({gauge(left, labels, left_extra)} - on ({labels}) "
            f"{gauge(right, labels, right_extra)}, 0)")


def definitions():
    proxy = 'component="proxy"'
    sn = 'component="streamingnode",' + CHANNEL
    consumer = 'component=~"streamingnode|querynode"'
    mutation = proxy + ',msg_type=~"insert|upsert|delete"'
    write_fn = proxy + ',function_name=~"Insert|Upsert|Delete"'
    query_fn = proxy + ',function_name=~"Search|Query|HybridSearch"'
    wp = 'component="woodpecker",' + LOG
    client = 'component!="woodpecker",' + LOG
    c_labels = "pod,log_id"
    n_labels = "pod,node_id,log_id"
    ch_labels = "pod,channel_name"
    read_note = ("A tailing scanner can read the SN write-ahead memory buffer without WP RPCs. "
                 "A cold/catchup reader may read an active segment from WP services or a compacted segment directly from object storage. "
                 "Zero WP read traffic alone is not a fault. EOF/end_of_file is normal at the tail.")
    write = [
        stage("proxy", "1. Proxy — admission, preparation and request completion",
              "Start here for slow/failed inserts. Compare successful mutation latency with completed request status and queue depth. "
              "If the queue grows while downstream latency stays stable, inspect Proxy resources and admission. "
              "If produce latency rises too, continue to stage 2. REST/network/client time is outside this successful-mutation timer.", [
                  line("Write request completions by status", rate("milvus_proxy_req_count", "pod,function_name,status", write_fn), "ops", "{{pod}} {{function_name}} {{status}}",
                       "Use status=total for total completions; success/fail are classifications, not additional requests to add to total."),
                  latency("Successful mutation latency", "milvus_proxy_mutation_latency", "pod,msg_type", mutation),
                  line("Proxy DML queue depth", gauge("milvus_proxy_queue_task_num", "pod,task_state", proxy + ',queue_type="dml"'), legend="{{pod}} {{task_state}}"),
                  line("Inserted rows / second", rate("milvus_proxy_insert_vectors_count", extra=proxy), "ops", description="Successful rows/second. Compare mutation latency with request completion rates; internal WAL message counts include control traffic."),
                  latency("Proxy timestamp allocation", "milvus_proxy_apply_timestamp_latency", extra=proxy),
                  latency("Proxy primary-key allocation", "milvus_proxy_apply_pk_latency", extra=proxy),
              ], [probe("milvus_proxy_mutation_latency_count", mutation, True), probe("milvus_proxy_req_count", write_fn, True)]),
        stage("streaming-client", "2. Proxy Streaming Client — route and send to StreamingNode",
              "Compare produce latency/status by channel. Unavailable producers suggest routing, ownership or reconnect problems. "
              "When producers are available but produce latency grows, inspect SN in-flight work and WAL append in stage 3.", [
                  latency("Produce latency", "milvus_streaming_produce_success_duration_seconds", ch_labels, proxy + "," + CHANNEL, True),
                  line("Produce completions by status", rate("milvus_streaming_produce_total", "pod,channel_name,status", proxy + "," + CHANNEL), "ops", "{{pod}} {{channel_name}} {{status}}"),
                  line("Resuming producers by availability", gauge("milvus_streaming_resuming_producer_total", "pod,channel_name,status", proxy + "," + CHANNEL), legend="{{pod}} {{channel_name}} {{status}}"),
                  line("SN in-flight produce requests", gauge("milvus_streamingnode_produce_inflight_total", ch_labels, sn), legend="{{pod}} {{channel_name}}"),
              ], [probe("milvus_streaming_produce_total", proxy + "," + CHANNEL, True), probe("milvus_streaming_resuming_producer_total", proxy + "," + CHANNEL)]),
        stage("wal", "3. StreamingNode WAL — interceptors and backend append",
              "Compare full WAL append, backend append and each interceptor. Overlapping timer scopes are not an additive latency breakdown. "
              "Inspect retries and per-channel confirmation progress. Backend latency rising points to stage 4; growing interceptor time points to the named interceptor.", [
                  latency("Full WAL append by result", "milvus_wal_append_message_duration_seconds", "pod,channel_name,status", sn, True),
                  latency("WAL backend append by result", "milvus_wal_impls_append_message_duration_seconds", "pod,channel_name,status", sn, True),
                  latency("Before-append interceptors", "milvus_wal_interceptor_before_append_duration_seconds", "pod,interceptor_name", sn, True),
                  line("WAL append outcomes / second", rate("milvus_wal_append_message_total", "pod,channel_name,message_type,status", sn), "ops", "{{channel_name}} {{message_type}} {{status}}"),
                  latency("After-append interceptors", "milvus_wal_interceptor_after_append_duration_seconds", "pod,interceptor_name", sn, True),
                  line("Backend retries / second", rate("milvus_wal_impls_append_message_retry_total", ch_labels, sn), "ops", "{{pod}} {{channel_name}}"),
              ], [probe("milvus_wal_append_message_duration_seconds_count", sn, True), probe("milvus_wal_impls_append_message_duration_seconds_count", sn, True)]),
        stage("wp-client", "4. WP Client inside Milvus — pending appends and replica results",
              "The WP client runs inside SN; it is not a separate Pod. Compare pending appends and writer outcomes with per-log append latency. "
              "Replica counters are attempts, not acknowledged Milvus rows. For failing/slow replicas continue to stage 5; use wp meta log list and wp node list to resolve identities.", [
                  latency("WP append latency", "woodpecker_client_append_latency", c_labels, client),
                  line("Pending append operations", gauge("woodpecker_client_segment_handle_pending_append_ops", c_labels, client, "sum"), legend="{{pod}} log={{log_id}}"),
                  line("Writer outcomes / second", rate("woodpecker_client_writer_operation_latency_count", "pod,log_id,status", client + ',operation="write"'), "ops", "{{pod}} log={{log_id}} {{status}}"),
                  line("Replica append outcomes / second", rate("woodpecker_client_replica_append_total", "pod,log_id,status", client), "ops", "{{pod}} log={{log_id}} {{status}}"),
              ], [probe("woodpecker_client_append_requests_total", client, True), probe("woodpecker_client_segment_handle_pending_append_ops", client, True)]),
        stage("wp-server", "5. WP services — append, buffer wait and local disk flush",
              "Compare individual nodes rather than only a cluster-wide quantile. High buffer wait plus flush time suggests local disk/sync pressure. "
              "Check scheduler queues and node resources before changing concurrency. wp ops, wp node log-health and component logs provide the next evidence.", [
                  latency("Server append operation latency", "woodpecker_server_logstore_operation_latency", n_labels, wp + ',operation=~"add_entry|add_entry_batch"'),
                  latency("Buffer wait before acknowledgement", "woodpecker_server_buffer_wait_latency", n_labels, wp),
                  latency("Local WAL file flush latency", "woodpecker_server_file_flush_latency", n_labels, wp),
                  line("Append operation outcomes / second", rate("woodpecker_server_logstore_operations_total", "pod,node_id,status", wp + ',operation=~"add_entry|add_entry_batch"'), "ops", "{{pod}} {{node_id}} {{status}}"),
              ], [probe("woodpecker_server_logstore_operations_total", wp + ',operation=~"add_entry|add_entry_batch"', True), probe("woodpecker_server_file_flush_latency_count", wp)]),
        stage("pressure", "6. Admission and resource pressure — explain sustained slowdown",
              "Disk admission state is 0=normal, 1=warn, 2=blocked. Compare actual disk use with exported watermarks in the WP reference dashboard. "
              "Check the Flush/Checkpoint dashboard for Milvus buffer pressure; WP object-store compaction is a separate background path, not every append's synchronous storage step.", [
                  line("WP write backpressure state", gauge("woodpecker_server_logstore_write_backpressure_state", "pod,node_id", 'component="woodpecker"'), legend="{{pod}} {{node_id}}"),
                  line("Local WAL disk usage", gauge("woodpecker_server_logstore_disk_usage_ratio", "pod,node_id,path", 'component="woodpecker"'), "percentunit", "{{pod}} {{path}}"),
                  line("Sync scheduler waiting tasks", gauge("woodpecker_server_sync_scheduler_waiting", "pod,node_id", 'component="woodpecker"'), legend="{{pod}} {{node_id}}"),
                  line("Allocated minus confirmed TimeTick", delta("milvus_wal_last_allocated_time_tick", "milvus_wal_last_confirmed_time_tick"), "s", "{{pod}} {{channel_name}}",
                       "Unix-second timestamp difference for the same Pod/channel, not a per-request latency or entry count."),
              ], [probe("woodpecker_server_logstore_write_backpressure_state", 'component="woodpecker"', True), probe("milvus_wal_last_confirmed_time_tick", sn)]),
    ]
    consume = [
        stage("source", "1. Establish the read mode — memory tailing or WAL catchup",
              read_note + " Compare scanner modes and WP read sources before interpreting an idle backend. For a true durability test, use a separate controlled restart/recovery scenario.", [
                  line("Scanners by mode", gauge("milvus_wal_scanner_total", "pod,channel_name,scanner_model", sn, "sum"), legend="{{channel_name}} {{scanner_model}}"),
                  line("SN write-ahead buffer bytes", gauge("milvus_wal_write_ahead_buffer_size_bytes", ch_labels, sn), "bytes", "{{pod}} {{channel_name}}"),
                  line("WP active-segment quorum read bytes / second", rate("woodpecker_client_quorum_read_bytes_total", c_labels, client), "Bps", "{{pod}} log={{log_id}}"),
                  line("WP compacted-segment direct read outcomes", rate("woodpecker_client_direct_read_requests_total", "pod,log_id,status", client), "ops", "{{pod}} log={{log_id}} {{status}}"),
              ], [probe("milvus_wal_scanner_total", sn, True), probe("woodpecker_client_quorum_read_bytes_total", client), probe("woodpecker_client_direct_read_requests_total", client)]),
        stage("reader", "2. Historical WAL read — WP Reader, services or object storage",
              "Follow this group when catchup reads are active. Compare read_next success/error and backend read latency. "
              "A blocking read_next includes waiting for new data; a large duration alone does not prove a slow disk. EOF/end_of_file at the tail is an expected outcome. "
              "Per-reader read-frontier metrics are not exported by the example's WP v0.1.46; inspect wp log readers when needed.", [
                  latency("WP reader read_next by result", "woodpecker_client_reader_operation_latency", "pod,log_id,status", client + ',operation="read_next"'),
                  line("Reader read_next outcomes / second", rate("woodpecker_client_reader_operation_latency_count", "pod,log_id,status", client + ',operation="read_next"'), "ops", "{{pod}} log={{log_id}} {{status}}"),
                  latency("WP service read batch latency", "woodpecker_server_read_batch_latency", n_labels, wp),
                  latency("Compacted-segment direct read latency", "woodpecker_client_direct_read_latency", c_labels, client),
              ], [probe("woodpecker_client_reader_operation_latency_count", client + ',operation="read_next"'), probe("woodpecker_server_read_batch_latency_count", wp), probe("woodpecker_client_direct_read_latency_count", client)]),
        stage("scanner", "3. SN Scanner — scan, filter, ordering and transaction buffers",
              "Compare scanned and passed messages by message type and mode. Filtering TimeTicks/control messages is expected, so those rates are not a loss ratio. "
              "Growing pending/transaction buffers or a paused scanner warrant inspection of downstream processing and transaction completion. Continue to stages 4–5.", [
                  line("Scanned messages / second", rate("milvus_wal_scan_message_total", "pod,channel_name,scanner_model,message_type", sn), "ops", "{{channel_name}} {{scanner_model}} {{message_type}}"),
                  line("Passed messages / second", rate("milvus_wal_scan_pass_message_total", "pod,channel_name,scanner_model,message_type", sn), "ops", "{{channel_name}} {{scanner_model}} {{message_type}}"),
                  line("Scanner pending bytes", gauge("milvus_wal_scanner_pending_queue_bytes", ch_labels, sn, "sum"), "bytes", "{{pod}} {{channel_name}}"),
                  line("Scanner consumption paused", gauge("milvus_wal_scanner_pause_consumption", ch_labels, sn), legend="{{pod}} {{channel_name}}"),
                  line("Transaction buffer bytes", gauge("milvus_wal_scanner_txn_buf_bytes", ch_labels, sn, "sum"), "bytes", "{{pod}} {{channel_name}}"),
                  line("TimeTick ordering buffer bytes", gauge("milvus_wal_scanner_time_tick_buf_bytes", ch_labels, sn, "sum"), "bytes", "{{pod}} {{channel_name}}"),
              ], [probe("milvus_wal_scan_message_total", sn, True), probe("milvus_wal_scanner_pending_queue_bytes", sn, True)]),
        stage("subscription", "4. Subscription and dispatch — local and remote access",
              "Consumer client metrics belong to the process that subscribes, which can be SN itself. A local consumer bypasses the remote gRPC handler. "
              "Missing remote-only server series is expected for a local subscription; unavailable consumers indicate a reconnect/routing problem.", [
                  line("Consumers by access model", gauge("milvus_streaming_consumer_total", "pod,channel_name,access_model", CHANNEL, "sum"), legend="{{pod}} {{channel_name}} {{access_model}}"),
                  line("Resuming consumers by availability", gauge("milvus_streaming_resuming_consumer_total", "pod,channel_name,status", CHANNEL, "sum"), legend="{{pod}} {{channel_name}} {{status}}"),
                  line("Delivered bytes / second", rate("milvus_streaming_consume_bytes_sum", ch_labels, CHANNEL), "Bps", "{{pod}} {{channel_name}}"),
                  line("Remote server in-flight consume bodies", gauge("milvus_streamingnode_consume_inflight_total", ch_labels, sn), description="Remote handler only; no data is expected when all subscribers use access_model=local.", legend="{{pod}} {{channel_name}}"),
              ], [probe("milvus_streaming_consumer_total", CHANNEL, True), probe("milvus_streaming_consume_bytes_count", CHANNEL, True), probe("milvus_streamingnode_consume_inflight_total", sn)]),
        stage("visibility", "5. Consumer processing and visibility — include the SN delegator",
              "The querynode-named metrics can be emitted by StreamingNode's co-located delegator; legends preserve the actual component/Pod. "
              "Compare consumption lag, pending processing and insert/delete latency. Rising TSafe wait links this dashboard to Search/Query. "
              "Use a Strong-consistency workload to verify application visibility; these metrics do not measure per-row insert-to-visible latency.", [
                  line("Consumer TimeTick lag", gauge("milvus_querynode_consume_tt_lag_ms", "pod,component,collection_id", consumer), "ms", "{{pod}} {{component}} collection={{collection_id}}"),
                  line("Messages waiting for processing", gauge("milvus_querynode_wait_processing_msg_count", "pod,component,msg_type", consumer, "sum"), legend="{{pod}} {{component}} {{msg_type}}"),
                  latency("Insert/delete processing latency", "milvus_querynode_process_insert_or_delete_latency", "pod,component,msg_type", consumer),
                  latency("Search/query wait for TSafe", "milvus_querynode_sq_wait_tsafe_latency", "pod,component,query_type", consumer),
              ], [probe("milvus_querynode_consume_tt_lag_ms", consumer, True), probe("milvus_querynode_process_insert_or_delete_latency_count", consumer)]),
    ]
    flush = [
        stage("buffer", "1. Growing data — buffering and flush pressure",
              "Start here when writes/consumption succeed but persistence lags. Growing bytes are a gauge, even when a metric ends in _total. "
              "Compare pressure with configured watermarks before attributing admission slowdown to WP disk.", [
                  line("Growing segment bytes", gauge("milvus_wal_growing_segment_bytes", "pod,channel_name,lv", sn, "sum"), "bytes", "{{pod}} {{channel_name}} {{lv}}"),
                  line("Node flush pressure bytes", gauge("milvus_wal_growing_segment_flush_pressure_bytes", extra='component="streamingnode"'), "bytes"),
                  line("Growing data high watermark", gauge("milvus_wal_growing_segment_hwm_bytes", extra='component="streamingnode"'), "bytes"),
                  line("Growing data low watermark", gauge("milvus_wal_growing_segment_lwm_bytes", extra='component="streamingnode"'), "bytes"),
              ], [probe("milvus_wal_growing_segment_flush_pressure_bytes", 'component="streamingnode"'), probe("milvus_wal_growing_segment_bytes", sn)]),
        stage("schedule", "2. Flusher — state and queued sync work",
              "Check state before queue latency: an in_recovery flusher may be replaying. Pending tasks include queued AND in-flight work, not just queue length. "
              "Dispatcher metrics are version-specific; missing series on another image require a coverage check, not a zero substitution.", [
                  line("Flusher state", gauge("milvus_wal_flusher_info", "pod,channel_name,state", sn), legend="{{pod}} {{channel_name}} {{state}}"),
                  line("Pending sync tasks (queued + executing)", gauge("milvus_wal_flusher_sync_dispatcher_pending_tasks", extra='component="streamingnode"')),
                  latency("Sync dispatcher queue latency", "milvus_wal_flusher_sync_dispatcher_queue_duration_seconds", extra='component="streamingnode"', seconds=True),
                  line("Submitted sync tasks / second", rate("milvus_wal_flusher_sync_dispatcher_task_total", extra='component="streamingnode"'), "ops"),
              ], [probe("milvus_wal_flusher_info", sn, True), probe("milvus_wal_flusher_sync_dispatcher_pending_tasks", 'component="streamingnode"')]),
        stage("persist", "3. Milvus persistence — execute, upload and complete flush",
              "Compare sync execution, save latency and storage operations. Datanode-named flush metrics may be emitted by SN; keep component labels. "
              "Storage operations are per Milvus process; they are not identical to WP's background object-store compaction.", [
                  latency("Sync execution including upload/callbacks", "milvus_wal_flusher_sync_dispatcher_execute_duration_seconds", extra='component="streamingnode"', seconds=True),
                  latency("Save flushed data latency", "milvus_datanode_save_latency", "pod,component", 'component=~"streamingnode|datanode"'),
                  line("Flushed data bytes / second", rate("milvus_datanode_flushed_data_size", "pod,component", 'component=~"streamingnode|datanode"'), "Bps", "{{pod}} {{component}}"),
                  line("Milvus storage operation outcomes", rate("milvus_storage_op_count", "pod,persistent_data_op_type,status", 'component="streamingnode"'), "ops", "{{pod}} {{persistent_data_op_type}} {{status}}",
                       "status=total and success/fail are separate classifications; do not sum them as independent operations."),
              ], [probe("milvus_storage_op_count", 'component="streamingnode"', True), probe("milvus_datanode_save_latency_count", 'component=~"streamingnode|datanode"')]),
        stage("checkpoint", "4. Checkpoint and recovery — compare the right progress markers",
              "Flusher 'seen' TimeTick is not a durable checkpoint. Compare confirmation/consumption progress with the persisted recovery snapshot and DataCoord checkpoint. "
                       "Snapshot/checkpoint age can grow while idle: investigate alongside fresh writes, flush activity and advance over time; no automatic red/green threshold is applied. "
                       "DataCoord checkpoint labels may name virtual channels; that panel intentionally shows all channels in the selected deployment.", [
                  line("Confirmed minus flusher-seen TimeTick", delta("milvus_wal_last_confirmed_time_tick", "milvus_wal_flusher_time_tick"), "s", "{{pod}} {{channel_name}}"),
                  line("In-memory minus persisted recovery TimeTick", delta("milvus_wal_recovery_in_mem_time_tick", "milvus_wal_recovery_persisted_time_tick", left_extra=sn, right_extra=sn), "s", "{{pod}} {{channel_name}}"),
                  line("DataCoord checkpoint age (all channels)", f"clamp_min(time() - {gauge('milvus_datacoord_channel_checkpoint_unix_seconds', 'pod,channel_name')}, 0)", "s", "{{pod}} {{channel_name}}"),
                  line("Recovery state", gauge("milvus_wal_recovery_info", "pod,channel_name,state", sn), legend="{{pod}} {{channel_name}} {{state}}"),
              ], [probe("milvus_wal_flusher_time_tick", sn, True), probe("milvus_wal_recovery_persisted_time_tick", sn), probe("milvus_datacoord_channel_checkpoint_unix_seconds")]),
        stage("wp-background", "5. Separate WP background path — offload and local disk pressure",
              "This is a parallel WP maintenance path, not the next serial Milvus flush step. Slow compaction/offload can retain local WAL files and eventually create disk admission pressure. "
              "Compare object-store operation status, compaction latency and local disk use; expected stat_object not_found is shown as a status, not automatically counted as an error.", [
                  latency("WP compaction latency", "woodpecker_server_file_compaction_latency", n_labels, wp),
                  latency("WP object-storage operation latency", "woodpecker_server_object_storage_operation_latency", "pod,node_id,operation", wp),
                  line("WP object-storage outcomes", rate("woodpecker_server_object_storage_operations_total", "pod,node_id,operation,status", wp), "ops", "{{pod}} {{operation}} {{status}}"),
                  line("WP local disk usage", gauge("woodpecker_server_logstore_disk_usage_ratio", "pod,node_id,path", 'component="woodpecker"'), "percentunit", "{{pod}} {{path}}"),
              ], [probe("woodpecker_server_logstore_disk_usage_ratio", 'component="woodpecker"', True), probe("woodpecker_server_file_compaction_latency_count", wp)]),
    ]
    query = [
        stage("proxy-query", "1. Proxy — Search/Query request completion",
              "Start from request status, observation count and successful latency. Strong consistency can wait for ingestion visibility; "
              "use stage 2 and the WAL Read/Consume dashboard when TSafe dominates. These server timers do not include the SDK/network round trip.", [
                  line("Search/query completions by status", rate("milvus_proxy_req_count", "pod,function_name,status", query_fn), "ops", "{{pod}} {{function_name}} {{status}}"),
                  latency("Successful Proxy search/query latency", "milvus_proxy_sq_latency", "pod,query_type", proxy),
                  line("Proxy search/query latency samples / second", rate("milvus_proxy_sq_latency_count", "pod,query_type", proxy), "ops", "{{pod}} {{query_type}}"),
                  line("Proxy DQL queue depth", gauge("milvus_proxy_queue_task_num", "pod,task_state", proxy + ',queue_type="dql"'), legend="{{pod}} {{task_state}}"),
              ], [probe("milvus_proxy_sq_latency_count", proxy, True), probe("milvus_proxy_req_count", query_fn, True)]),
        stage("tsafe", "2. Delegator — wait for visibility and route work",
              "TSafe wait means the required consistency timestamp is not yet serviceable; compare consumption lag instead of immediately blaming vector execution. "
              "Querynode-prefixed metrics can originate on SN. The actual Pod/component remains in each legend.", [
                  latency("Wait for TSafe", "milvus_querynode_sq_wait_tsafe_latency", "pod,component,query_type", consumer),
                  line("Consumer TimeTick lag", gauge("milvus_querynode_consume_tt_lag_ms", "pod,component,collection_id", consumer), "ms", "{{pod}} {{component}} collection={{collection_id}}"),
                  latency("Search/query handler latency", "milvus_querynode_sq_req_latency", "pod,component,query_type", consumer),
                  line("Handler outcomes", rate("milvus_querynode_sq_req_count", "pod,component,query_type,status", consumer), "ops", "{{pod}} {{component}} {{query_type}} {{status}}"),
              ], [probe("milvus_querynode_sq_wait_tsafe_latency_count", consumer, True), probe("milvus_querynode_sq_req_latency_count", consumer, True)]),
        stage("query-queue", "3. Execution scheduling — queue versus available concurrency",
              "Rising queue delay with a growing ready queue suggests scheduling/resource pressure. Compare execution time in stage 4. "
              "Scheduler families depend on the Milvus image and which node executes the work; missing series are reported as coverage gaps.", [
                  latency("Search/query queue latency", "milvus_querynode_sq_queue_latency", "pod,component,query_type", consumer),
                  line("Ready read tasks", gauge("milvus_querynode_read_task_ready_len", "pod,component", consumer, "sum"), legend="{{pod}} {{component}}"),
                  line("Executing read-task concurrency", gauge("milvus_querynode_read_task_concurrency", "pod,component", consumer, "sum"), legend="{{pod}} {{component}}"),
                  latency("Read-task scheduler queue duration", "milvus_querynode_read_task_queue_duration", "pod,component,outcome", consumer),
              ], [probe("milvus_querynode_sq_queue_latency_count", consumer), probe("milvus_querynode_read_task_ready_len", consumer)]),
        stage("query-execution", "4. Segment execution — core work and loading",
              "Compare core/segment execution and segment loading. Growing and sealed work can run in different processes; preserve component and segment-state labels. "
              "Loading a segment is a different timer population from an individual search, so do not add their quantiles.", [
                  latency("Segcore search/query latency", "milvus_querynode_sq_core_latency", "pod,component,query_type", consumer),
                  latency("Per-segment execution latency", "milvus_querynode_sq_segment_latency", "pod,component,query_type,segment_state", consumer),
                  latency("Segment loading latency", "milvus_querynode_load_segment_latency", "pod,component", consumer),
                  line("Search/query execution observations / second", rate("milvus_querynode_sq_core_latency_count", "pod,component,query_type", consumer), "ops", "{{pod}} {{component}} {{query_type}}"),
              ], [probe("milvus_querynode_sq_core_latency_count", consumer), probe("milvus_querynode_sq_segment_latency_count", consumer)]),
        stage("query-return", "5. Result return — downstream reduction and Proxy merge/decode",
              "If execution is stable but result waiting grows, inspect fan-out, routing and the slowest responding node. "
              "Proxy wait includes downstream work and transport; it is not a pure network latency. Large reductions/decode can reflect result size or fan-out.", [
                  latency("Proxy wait for results", "milvus_proxy_sq_wait_result_latency", "pod,query_type", proxy),
                  latency("Downstream result reduction", "milvus_querynode_sq_reduce_latency", "pod,component,query_type", consumer),
                  latency("Proxy merge/reduce results", "milvus_proxy_sq_reduce_result_latency", "pod,query_type", proxy),
                  latency("Proxy decode results", "milvus_proxy_sq_decode_result_latency", "pod,query_type", proxy),
              ], [probe("milvus_proxy_sq_wait_result_latency_count", proxy, True), probe("milvus_proxy_sq_reduce_result_latency_count", proxy), probe("milvus_proxy_sq_decode_result_latency_count", proxy)]),
    ]
    return [("write", "E2E Write / Append", "Proxy → Streaming Client → SN WAL → WP Client → WP services / disk", write),
            ("consume", "E2E WAL Read / Consume", "Memory or historical WAL → SN Scanner → subscription → processing / visibility", consume),
            ("flush", "E2E Flush / Checkpoint", "Growing data → flush scheduling → persistence → checkpoint; separate WP offload path", flush),
            ("query", "E2E Search / Query", "Proxy → Delegator / TSafe → scheduling → segment execution → result return", query)]


def variables():
    def variable(name, query, current, multi=False):
        return {"name": name, "label": name.replace("_", " "), "type": "query", "datasource": DS,
                "query": {"query": query, "refId": "StandardVariableQuery"}, "definition": query,
                "refresh": 1, "sort": 1, "multi": multi, "includeAll": multi,
                "current": {"text": "All" if multi else current, "value": "$__all" if multi else current},
                **({"allValue": ".*"} if multi else {}), "options": []}
    return [{"name": "datasource", "type": "datasource", "query": "prometheus",
             "current": {"text": "Prometheus", "value": "prometheus"}},
            variable("cluster", 'label_values(up{woodpecker_id!=""}, cluster)', "local"),
            variable("namespace", 'label_values(up{cluster="$cluster",woodpecker_id!=""}, namespace)', "milvus-stack"),
            variable("woodpecker_id", 'label_values(up{cluster="$cluster",namespace="$namespace",woodpecker_id!=""}, woodpecker_id)', "milvus"),
            variable("channel", "label_values(" + metric("milvus_wal_info") + ", channel_name)", ".*", True),
            variable("log_ns", "label_values(" + metric("woodpecker_client_append_requests_total") + ", log_ns)", ".*", True),
            variable("log_id", "label_values(" + metric("woodpecker_client_append_requests_total", 'log_ns=~"${log_ns:regex}"') + ", log_id)", ".*", True)]


def navigation():
    # Include filter values only between E2E pages. The reference dashboards have
    # different variable meanings (notably instance), so do not retarget them.
    return [{"title": title, "type": "link", "url": "/d/" + uid, "keepTime": True,
             "includeVars": uid.startswith("milvus-e2e-"), "targetBlank": False} for title, uid in LINKS]


def build_dashboard(key, title, flow, stages):
    panels = []
    y = 0
    def add(panel, x=0, width=24, height=4):
        panel.update(id=len(panels) + 1, gridPos={"x": x, "y": y, "w": width, "h": height})
        panels.append(panel)
    add({"type": "text", "title": title + " — read from top to bottom", "options": {"mode": "markdown", "content":
         "**" + flow + "**\n\nSelect a time window containing the workload. Channel filters apply to Milvus channel metrics; "
         "log filters apply to WP metrics. They are independent identities: use wpcli metadata to map them; "
         "there is no automatic channel-to-log or collection-to-log join. Legends identify the actual process.\n\n"
         "**No data ≠ healthy zero.** Lazy/error/remote-only metrics may be absent; `./stack.sh verify` records each stage's coverage. "
         "Timers overlap and quantiles are not additive. Read each step's guidance before interpreting an idle signal."}}, height=7)
    y += 7
    for item in stages:
        add({"type": "row", "title": item["title"], "collapsed": False, "panels": []}, height=1)
        y += 1
        add({"type": "text", "title": "How to use this step", "options": {"mode": "markdown", "content": item["guidance"]}}, height=4)
        y += 4
        for index, spec in enumerate(item["plots"]):
            add({"type": "timeseries", "title": spec["title"], "description": spec["description"], "datasource": DS,
                 "targets": [{"refId": chr(65 + i), "datasource": DS, "range": True, **target}
                             for i, target in enumerate(spec["targets"])],
                 "fieldConfig": {"defaults": {"unit": spec["unit"], "min": 0,
                                 "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 5,
                                            "spanNulls": False, "showPoints": "never"}}, "overrides": []},
                 "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}},
                x=(index % 2) * 12, width=12, height=8)
            if index % 2 or index == len(item["plots"]) - 1:
                y += 8
    return {"id": None, "uid": "milvus-e2e-" + key, "title": "Milvus + WP — " + title,
            "description": flow + ". Ordered operational steps; see E2E-DASHBOARDS.md for metric scope and coverage.",
            "schemaVersion": 39, "version": 1, "editable": True, "timezone": "browser", "refresh": "15s",
            "time": {"from": "now-30m", "to": "now"}, "tags": ["milvus", "woodpecker", "e2e", "operations"],
            "links": navigation(), "templating": {"list": variables()}, "panels": panels}


def generate():
    DEST.mkdir(parents=True, exist_ok=True)
    catalog = {"schema_version": 1, "milvus_reference_commit": "27bd28d20688ab00cdd74232a20e988fb2162aed",
               "woodpecker_reference_image": "milvusdb/woodpecker:v0.1.46", "dashboards": {}}
    for key, title, flow, stages in definitions():
        dashboard = build_dashboard(key, title, flow, stages)
        (DEST / ("e2e-" + key + ".json")).write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n")
        catalog["dashboards"][dashboard["uid"]] = {"title": title, "flow": flow, "stages": [
            {k: item[k] for k in ("key", "title", "guidance", "probes")} for item in stages]}
    # Reference imports and generated E2E pages share navigation without needing
    # the original production Milvus export to regenerate these four dashboards.
    for path in DEST.glob("*.json"):
        dashboard = json.loads(path.read_text())
        dashboard["links"] = navigation()
        path.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n")
    (ROOT / "e2e-dashboard-catalog.json").write_text(json.dumps(catalog, indent=2, ensure_ascii=False) + "\n")
    provenance_path = ROOT / "dashboards-sources.json"
    provenance = json.loads(provenance_path.read_text())
    provenance["e2e"] = {"generator": "scripts/e2e_dashboards.py", "guide": "E2E-DASHBOARDS.md",
                         "milvus_reference_commit": catalog["milvus_reference_commit"],
                         "woodpecker_reference_image": catalog["woodpecker_reference_image"],
                         "dashboards": list(catalog["dashboards"])}
    provenance_path.write_text(json.dumps(provenance, indent=2, ensure_ascii=False) + "\n")
    print("Generated four E2E dashboards, stage coverage catalog and shared navigation")


if __name__ == "__main__":
    generate()
