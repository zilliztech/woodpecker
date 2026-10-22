# E2E operational dashboards

Four focused dashboards follow processing stages from top to bottom. Start with
the symptom, read the guidance above each group, then use the component reference
dashboards and wpcli for deeper inspection. Grafana provisions all seven dashboards
from the support chart; the new pages appear after a normal `stack.sh deploy`.

| Symptom | Dashboard | Default local URL |
| --- | --- | --- |
| Writes are slow or fail | E2E Write / Append | http://127.0.0.1:3000/d/milvus-e2e-write |
| Consumption falls behind or new data is not visible | E2E WAL Read / Consume | http://127.0.0.1:3000/d/milvus-e2e-consume |
| Consumption works but persistence/checkpoints lag | E2E Flush / Checkpoint | http://127.0.0.1:3000/d/milvus-e2e-flush |
| Search/query requests are slow | E2E Search / Query | http://127.0.0.1:3000/d/milvus-e2e-query |

Keep `./stack.sh forward` or `./stack.sh forward --wpcli` running. Use
`./stack.sh connections` for actual ports when the defaults differ. Grafana's local
example credentials are `admin / admin`.

## Choose a chain and follow its steps

### Write / Append

1. **Proxy:** write completions by status, successful mutation latency, quota
   write-rate reduction, process RSS relative to container memory limits, DML
   queue, inserted rows, timestamp allocation and primary-key preparation.
2. **Streaming Client:** produce latency/status, producer availability and SN
   in-flight produce requests, payload/producer byte rates and quota admission
   delay, split by physical channel where available.
3. **SN WAL:** full append versus backend append, before/after interceptors,
   append outcomes and backend retry rate.
4. **WP client inside Milvus:** append latency, pending appends, writer outcomes
   and replica-append outcomes, split by log identity.
5. **WP services:** append-operation latency/outcomes, buffer wait and local file
   flush, preserving individual Pod/node identities.
6. **Pressure:** disk admission state, local disk use, scheduler waiting tasks
   and allocated-minus-confirmed TimeTick for the same SN/channel.

This separates Proxy preparation, producer routing, WAL interception and backend
work. Slow local flush and buffer wait call for per-node disk/CPU/sync inspection;
the reference WP dashboard contains further resources and configured watermarks.
WP background compaction/upload is not a synchronous step of every append.

For `rate=0` rejections, inspect stages 1–2 before blaming the storage backend.
The reference QuotaCenter exports `milvus_rootcoord_rate_limit_ratio` as **one
minus the allowed-rate factor**: zero means no reduction and one means full
reduction. It does not identify the reason. Correlate it with memory, consume lag
and QuotaCenter logs. StreamingNode hosts a query/delegator role, so a QueryNode
memory-protection log can refer to the SN process. The RSS/limit graph preserves
Pod/container identity but is not the exact hardware-metric input used by quota
decisions; check configured low/high watermarks and the log's used/total memory.
Long streaming quota-admission delays can explain multi-second writes while WAL
and WP append remain much faster. In the reference image, observed streaming
rate-limit rejections incremented Proxy `status=retry`, even with client retries
disabled; inspect that series alongside `fail` and the client's error report.

### WAL Read / Consume

1. **Source/mode:** catchup versus tailing scanners, SN write-ahead memory buffer,
   active-segment quorum read traffic and compacted-segment direct reads.
2. **Historical WAL reader:** WP `read_next` outcomes/latency, service read-batch
   latency and direct object-storage read latency.
3. **SN scanner:** scanned/passed messages by type/mode, pending bytes,
   consumption pause, transaction buffer and TimeTick ordering buffer.
4. **Subscription:** local/remote consumers, availability, delivered bytes and
   remote-handler in-flight work where that path is active.
5. **Processing/visibility:** consume TimeTick lag, pending processing,
   insert/delete processing latency and search/query TSafe wait.

A live tailing consumer may receive data entirely from the SN memory buffer:
zero WP read traffic is then expected. Catchup may use a WP service or read a
compacted segment directly from object storage. Normal tail EOF is displayed as
an outcome, not classified as an error. A blocking `read_next` can include waiting
for new data, so its latency is not a pure storage timer.

In the validated Milvus image, several `milvus_querynode_*` processing metrics
are emitted by StreamingNode's co-located delegator. Queries include both actual
`streamingnode` and `querynode` components and preserve them in legends; selecting
only QueryNode Pods would hide the active consume path. A local subscription
bypasses the remote consumer handler, whose lazy metric families may be absent.

The example WP v0.1.46 does not export the newer per-reader read-frontier metrics.
Use `wp log readers` and metadata/log inspection for those identities and positions.
No dashboard invents per-reader backlog or subtracts entry IDs across segments.

### Flush / Checkpoint

1. **Growing data:** bytes, flush-pressure decisions and configured watermarks.
2. **Scheduling:** flusher state, pending sync work, dispatcher queue latency
   and submitted task rate. Pending includes queued plus in-flight tasks.
3. **Milvus persistence:** sync execution including storage upload/callbacks,
   saving data, flushed bytes and storage operation outcomes.
4. **Progress:** confirmed versus flusher-seen TimeTick, in-memory versus persisted
   recovery TimeTick, DataCoord checkpoint age and recovery state.
5. **Separate WP background path:** compaction, object-storage operation latency
   and outcomes, and local disk usage. This is a parallel maintenance path.

Flusher-seen progress is not a durable checkpoint. Persisted recovery metadata and
the DataCoord checkpoint represent different boundaries. Checkpoint/snapshot age
can grow during idle periods; correlate it with fresh writes and progress over
time. DataCoord may label virtual channels, so its panel intentionally shows all
channels in the deployment rather than applying the physical-channel filter.

### Search / Query

1. **Proxy:** completions/status, successful request latency, observation rate
   and DQL queue.
2. **Delegator/TSafe:** visibility wait, consumption lag, handler latency/outcomes.
3. **Scheduling:** queue latency, ready tasks, execution concurrency and scheduler
   queue duration.
4. **Segment work:** segcore and per-segment execution, loading latency and
   execution observation rate, retaining actual component labels.
5. **Return:** Proxy wait for results, downstream reduction, Proxy merge/reduce
   and decoding.

TSafe wait links query symptoms to the consume dashboard. Proxy result wait includes
downstream work and transport, rather than measuring only the network. Loading and
individual searches are different timer populations.

## Filters, units and interpretation

- Cluster, namespace and deployment identity scope all queries. Physical channel
  filters apply to channel metrics; WP `log_ns`/`log_id` filters apply to WP metrics.
  They are separate identities, not a verified join. Resolve the channel/log
  relationship through deployed configuration and wpcli metadata before isolating
  both. There is no automatic collection-to-WP-log filter.
- Dashboard links preserve time. Links between E2E pages also preserve their shared
  filters; reference dashboards use their own variable definitions.
- Histograms show P50/P95/P99. Milvus streaming seconds are converted to milliseconds;
  WP and existing Proxy/QueryNode millisecond histograms remain in milliseconds.
  Overlapping timers and different populations make quantiles non-additive.
- A quantile with no observations is unavailable/NaN, not healthy zero. Sparse
  histograms and overflow above their largest finite bucket limit interpretation.
  Compare the plotted throughput/outcomes with latency, and use the stage coverage
  report before interpreting an empty group.
- `milvus_wal_*_time_tick` gauges in the reference image are Unix seconds. Progress
  differences match Pod/channel identities before subtraction. They are time gaps,
  not per-request latency or outstanding entry counts.
- A metric suffix such as `_total` does not guarantee counter semantics: scanner
  counts and several growing-data metrics are gauges. Requests with `status=total`
  and their success/failure classifications must not be summed as extra requests.
- EOF and expected object-store `stat_object` not-found statuses are displayed
  separately. An absent lazy error counter does not establish an error-free system.

## Verification and repeatable evidence

```bash
./stack.sh smoke
# Allow at least two 15-second scrapes after writing.
./stack.sh verify
./stack.sh perf
```

`verify` checks all seven provisioned dashboards and their visible PromQL. It also
checks the required metric families in **each E2E processing stage**, using the
generated `e2e-dashboard-catalog.json`. A generic UP panel cannot substitute for a
missing WAL or consume-stage family. The report under `.local/verification.json`
lists each stage as `observed`, `partial` or `missing-required`, with missing
optional families and required failures separately. Presence is not proof of
active traffic, acceptable latency or application correctness.

Optional families can be lazy, idle, local/remote-specific or image-specific;
rejected queries always fail verification. The supplied ARM64 image was used to
check names/labels, including local subscriptions and SN-hosted querynode metrics.
Other Milvus/WP builds require rechecking their exported families and semantics.

The local ARM64 validation on 2026-10-21 inserted 6,000 vectors, verified Strong
queries and vector search after every batch, then flushed the test collection and
confirmed all 6,000 rows with a Strong query. All 853 visible expressions across
seven dashboards and the required families in 21 E2E stages passed verification.
Browser checks scrolled all four E2E pages and observed numeric frames with no
query/runtime errors. The local-only consumer handler and idle growing-segment
families were recorded as optional coverage gaps, not converted into healthy zeros.

A follow-up local test targeted 20 MB/s of raw float32 vectors for 60 seconds
with concurrent Strong queries and vector searches. With the existing small
resource limits, acknowledged throughput was about 8.65 MB/s (535 MB), all 255
write batches succeeded, and final Strong count matched all 290,816 acknowledged
rows including warmup. Query/search checks passed, but insert P99 was 5.36 s and
search P99 was 4.90 s. An earlier run stopped after ten rate-limit rejections.
QuotaCenter logs recorded the SN-hosted delegator at about 90% of its 1 GiB memory
limit; quota-admission delay reached seconds while WP disk admission stayed
normal. This evidence motivated the quota, memory, byte-rate and admission-delay
graphs above. It is a diagnostic example, not a 20 MB/s capacity claim or a
durability/recovery test. Short rejections can fall between scrapes; client traces
and component logs remain necessary. The updated seven dashboards contain 861
visible expressions.

Performance reports and the notebook link to the four E2E pages and three
reference dashboards. Run-window links make it easier to compare requests,
metrics, wpcli state and logs. The smoke/performance workloads verify live
insert/query/search behavior; a controlled restart/recovery test is still required
to establish behavior across faults. The dashboards correlate evidence; they do
not provide distributed request traces or a measured per-row insert-to-visible SLI.

## Sources and maintenance

The authored definitions and guidance are in `scripts/e2e_dashboards.py`:

```bash
python3 scripts/e2e_dashboards.py
python3 tests/dashboards.test.py
```

Generation is deterministic and updates the four checked-in JSON files, stage
catalog and navigation on all seven dashboards. `scripts/sync-dashboards.py` also
regenerates these assets after importing reference dashboards. Deployment does not
need the external production dashboard export or Milvus source checkout.

Metric semantics were checked against the running exporters and the supplied image's
[Milvus source commit](https://github.com/milvus-io/milvus/tree/27bd28d20688ab00cdd74232a20e988fb2162aed),
particularly `pkg/metrics/{proxy,querynode,datanode,streaming_service}_metrics.go`,
`pkg/streaming/walimpls/impls/wp/`, and WAL/flusher metric observers under
`internal/streamingnode/server/`. WP definitions are in this repository's
`common/metrics/client_metrics.go` and `service_metrics.go`. Keep this version
scope and the coverage contract current when changing images or adding metrics.
