# WAL Disk Watermarks: Sizing, Warning, and Write Backpressure

In service mode each LogStore node stages WAL data on a local disk/PVC
(`woodpecker.storage.rootPath`) before it is uploaded/compacted to object storage.
If that disk fills, appends fail with `no space left on device` and clients see
opaque timeouts. Woodpecker monitors the WAL disk and applies three stages
(`woodpecker.logstore.diskWatermarkPolicy`):

| Stage | Range | Default | Behaviour |
|---|---|---|---|
| Soft (`softThresholdRatio`) | `ratio == softThresholdRatio` boundary | 0.80 | Rate-limited WARN log + `backpressure_state=1`. Writes still accepted. |
| Throttle | `softThresholdRatio < ratio < hardThresholdRatio` | ramps 0%→100% | New appends are rejected with a **retriable** error with a probability that ramps linearly from 0% at soft to 100% at hard (`logstore_write_reject_probability`). Each append is an independent coin flip, so writers slow down proportionally to disk pressure instead of hitting a hard cliff. Controlled by `throttleEnabled` (default `true`); when `false`, no rejections happen until the hard watermark. |
| Hard (`hardThresholdRatio` **or** `minFreeBytes`) | `ratio >= hardThresholdRatio` or `free <= minFreeBytes` | 0.90 / 1GB | New appends always rejected with a **retriable** error (`logstore local disk above hard watermark`, code 2007). Clients treat it as a transient rate-limit and retry. Applies regardless of `throttleEnabled`. |

The usage ratio is `used/(used+free)` with `free` = space available to the process
(df semantics — on ext4 this excludes root-reserved blocks). Sampling runs every
`sampleInterval` (default 10s); recovery clears the gate on the next sample after
usage drops below the thresholds. Reads, segment fence/complete/compact/clean are
never gated — those operations free disk.

## Config notes

- `throttleEnabled` (default `true`) turns the probabilistic throttle band on or
  off. When `true`, appends between soft and hard are rejected with probability
  `(ratio - soft) / (hard - soft)`. When `false`, the policy behaves as a
  two-stage warn/block gate: nothing is rejected until the hard watermark (or
  the `minFreeBytes` floor) is reached.
- If `hardThresholdRatio <= softThresholdRatio` the throttle band is empty (no
  ramp is computed); only the hard rule can reject.

## Metrics to alert on

- `woodpecker_server_logstore_disk_usage_ratio{node_id,path}` — alert at >= 0.80
- `woodpecker_server_logstore_disk_free_bytes{node_id,path}` — alert near `minFreeBytes`
- `woodpecker_server_logstore_write_backpressure_state{node_id}` — 0 normal / 1 warn / 2 blocked
- `woodpecker_server_logstore_write_rejected_total{node_id,reason="disk_pressure"}` — rejection rate
- `woodpecker_server_logstore_write_reject_probability{node_id}` — current reject probability (0..1) applied by the throttle stage

## Sizing the PVC / tuning retention

WAL disk demand ≈ peak ingest rate × `woodpecker.logstore.retentionPolicy.ttl`
(default 72h), plus compaction headroom. If nodes sit above the soft watermark in
steady state, either:

1. **Expand the PVC** (preferred for sustained higher ingest), or
2. **Lower `woodpecker.logstore.retentionPolicy.ttl`** so truncated segments are
   reclaimed sooner (data already compacted to object storage does not need long
   local retention), or
3. Reduce ingest (upstream rate limiting).

To disable write rejection entirely and keep only the warn stage, set
`throttleEnabled: false`, `hardThresholdRatio: 1.0`, and `minFreeBytes: 0`;
`enabled: false` disables watermark monitoring entirely.
