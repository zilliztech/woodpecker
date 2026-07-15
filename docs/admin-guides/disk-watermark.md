# WAL Disk Watermarks: Sizing, Warning, and Write Backpressure

In service mode each LogStore node stages WAL data on a local disk/PVC
(`woodpecker.storage.rootPath`) before it is uploaded/compacted to object storage.
If that disk fills, appends fail with `no space left on device` and clients see
opaque timeouts. Woodpecker monitors the WAL disk and applies two watermarks
(`woodpecker.logstore.diskWatermarkPolicy`):

| Watermark | Default | Behaviour |
|---|---|---|
| Soft (`softThresholdRatio`) | 0.80 | Rate-limited WARN log + `backpressure_state=1`. Writes still accepted. |
| Hard (`hardThresholdRatio` **or** `minFreeBytes`) | 0.90 / 1GB | New appends rejected with a **retriable** error (`logstore local disk above hard watermark`, code 2007). Clients treat it as a transient rate-limit and retry. |

The usage ratio is `used/(used+free)` with `free` = space available to the process
(df semantics — on ext4 this excludes root-reserved blocks). Sampling runs every
`sampleInterval` (default 10s); recovery clears the gate on the next sample after
usage drops below the thresholds. Reads, segment fence/complete/compact/clean are
never gated — those operations free disk.

## Metrics to alert on

- `woodpecker_server_logstore_disk_usage_ratio{node_id,path}` — alert at >= 0.80
- `woodpecker_server_logstore_disk_free_bytes{node_id,path}` — alert near `minFreeBytes`
- `woodpecker_server_logstore_write_backpressure_state{node_id}` — 0 normal / 1 warn / 2 blocked
- `woodpecker_server_logstore_write_rejected_total{node_id,reason="disk_pressure"}` — rejection rate

## Sizing the PVC / tuning retention

WAL disk demand ≈ peak ingest rate × `woodpecker.logstore.retentionPolicy.ttl`
(default 72h), plus compaction headroom. If nodes sit above the soft watermark in
steady state, either:

1. **Expand the PVC** (preferred for sustained higher ingest), or
2. **Lower `woodpecker.logstore.retentionPolicy.ttl`** so truncated segments are
   reclaimed sooner (data already compacted to object storage does not need long
   local retention), or
3. Reduce ingest (upstream rate limiting).

`hardThresholdRatio: 1.0` together with `minFreeBytes: 0` turns blocking off
(warn-only); `enabled: false` disables watermark monitoring entirely.
