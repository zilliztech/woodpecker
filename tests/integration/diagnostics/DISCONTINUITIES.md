# Actual restart/reset and scrape-gap acceptance (#493)

This scenario extends the existing [source acceptance](SOURCES.md) with an actual
restart and a bounded exporter outage. No counters, scrape samples, logs or spans
are synthesized. The existing diagnostic policy is unchanged. A counter reset
must be visible as an actual decreasing retained counter; a gap must exceed the
owner's existing 30-second coverage limit without a second reset. Both must be
inconclusive/unknown, rather than healthy or a fabricated latency verdict.

## Prepare and plan

Use an existing disposable context/namespace and the seven source-fixture images
from [SOURCES.md](SOURCES.md). Build the compatible read-only recovery worker for
the node architecture (use `GOARCH=amd64` for x86-64 nodes):

```sh
make wpdiag
mkdir -p .local/discontinuity-client
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -o .local/discontinuity-client/source-client ./tests/integration/diagnostics/sourceclient
docker build --platform linux/arm64 \
  -f tests/integration/diagnostics/sourceclient/Dockerfile \
  -t wp-source-client-493:local .local/discontinuity-client
```

Make that image available using the cluster's normal registry/image-loading
procedure. Cluster lifecycle/image loading is an external preparation step; the
scripts never start/stop a cluster or change the current Kubernetes context.

```sh
python3 tests/integration/diagnostics/discontinuity_acceptance.py \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE
```

The default prints an offline plan with no network, subprocess or artifact write.
Execution requires the explicit disposable-namespace opt-in and creates the same
exclusive lock and run-owned resources used by #491/#492:

```sh
kubectl --context YOUR_CONTEXT label namespace YOUR_NAMESPACE \
  diagnostics.woodpecker.io/source-acceptance=cli-source-v1

python3 tests/integration/diagnostics/discontinuity_acceptance.py \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE \
  --output /tmp/wp-discontinuity-493 --execute
```

## What actually changes

Only this newly created fixture Pod uses `shareProcessNamespace: true`. It does
not use host PID access, host mounts, privileged containers or node commands.
The original #491/#492 fixture defaults remain unchanged. The container's WP
process is therefore not the PID-namespace init, which can ignore STOP/KILL sent
from inside that namespace.

Before every signal, verify namespace UID, original lock/Pod UID, labels, owner
references and unchanged desired configuration. Identify exactly one WP binary
under `/proc`, verify its executable, node/cluster arguments, non-init PID and
process start ticks, and bind that process to the original container identity.
Journal before exec; recheck PID start ticks/executable in the signaling command.
Never infer success from an exit code alone or signal a replacement process.

| Phase | Required evidence |
| --- | --- |
| Existing source checks | Actual acknowledged values, rejection events, retained traces/logs, genuine association and selected/unsupported source controls from #491. |
| Warm reads | Verify both acknowledged values/EOF without appending; build an actual pre-restart read counter population. |
| Restart | KILL only owned WP; same Pod/spec/image, exactly one new WP container/process, recorded termination/start times, all six other containers unchanged. |
| Reset diagnosis | Read-verify original values, retain a decreasing success counter, no masking scrape gap; standalone and composed reports preserve `counter_reset_or_restart`, original lineage and unknown verdict. |
| Scrape outage | STOP that same recovered WP for 40 seconds. Real Prometheus observes `up=0`; no TSDB edits or response mocks. |
| Restoration | CONT the exact same PID/start/container identity; verify readiness, acknowledged payloads and `up=1`. No second restart. |
| Gap diagnosis | Actual raw sample interval exceeds 30 seconds with no reset; preserve `scrape_gap`, original timestamps and inconclusive findings. No interpolation or healthy-zero substitution. |
| Historical/current | Re-query the exact reset interval after recovery; original samples/findings remain. A separate current interval and actual successful reads do not clear the historical incident. |

The worker's `--verify-only` path issues only the actual read/EOF calls and checks
both original payloads. It cannot rewrite the acknowledged values. This selected
MinIO-backed service restart does not establish disk/fsync, full SDK quorum or
power-loss durability.

## Bounds, failures and recovery

Whole work budget: 360–900 seconds, default 600. Existing source startup/response/
artifact limits remain in effect; restart readiness is bounded to 90 seconds and
the pause to 40 seconds of work. Restoration uses separately bounded calls even
after the work deadline. Normal failure/interrupt attempts CONT on the same
original process before the shared UID-safe fixture cleanup. Ambiguous exec is
journaled as ambiguous and reconciled by actual process/scrape evidence.

SIGKILL can prevent local restoration. The existing recovery tool validates the
original resource journal and deletes only that owned fixture, including any
paused process. It preserves the original failed acceptance receipt and refuses
foreign resources or a replacement lock:

```sh
python3 tests/integration/diagnostics/source_recover.py \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE \
  --receipt /tmp/wp-discontinuity-493/RUN/fixture.json
# Inspect first, then repeat with --execute.
```

Inspect private local `acceptance.json`/`fixture.json`, action/process journals,
read receipts, actual `up` responses and independent/composed historical/current
reports. Do not publish raw connection/configuration data. No shared PVC, Helm
release, etcd setting or existing workload is changed. Owned emptyDir histories
are deleted by cleanup; incomplete sampling/retention is not lossless telemetry.

The [actual receipt](DISCONTINUITIES_VALIDATION.md) retains failed attempts,
builds and artifact fingerprints. The [final audit](FINAL_AUDIT.md) maps every
original #458 dimension to deployed and/or explicitly labeled fixture evidence.
Its review/merge gate is separate from #459's published full-trace images.

Offline checks are collected automatically by Diagnostics CI's Harness job:

```sh
python3 -m unittest discover -s tests/integration/diagnostics -p 'test_*.py' -v
go test -race ./tests/integration/diagnostics/sourceclient
```
