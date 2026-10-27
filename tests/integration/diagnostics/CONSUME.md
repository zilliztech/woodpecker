# Real scanner-startup consumption and initial visibility acceptance

[#484](https://github.com/zilliztech/woodpecker/issues/484) follows the first
CLI admission acceptance subset of #458. This scenario runs the existing
`wpdiag milvus consume` and integrated CLI against an existing service-mode
Milvus/Woodpecker test deployment. It requires Prometheus, not Jaeger, Loki,
Grafana, native reader metadata or WebUI. Complete trace propagation is not a
prerequisite. See [CONSUME_VALIDATION.md](CONSUME_VALIDATION.md) for actual evidence.

## What this fault does

The Milvus build must support the native refreshable setting
`streaming.walScanner.pauseConsumption` and the management configuration API.
In the tested implementation, `scannerAdaptorImpl.waitUntilStartConsumption`
checks the setting **once at scanner startup**. It does not suspend already
running readers. Consumers with `IgnorePauseConsumption` bypass it.

The harness first establishes a working collection. After enabling pause, it
creates a second, uniquely owned collection so its new subscription encounters
the startup gate. Acknowledged writes to the second collection can succeed while
its initial load/visibility is blocked. Its Strong queries should hit the bounded
client deadline, while the existing collection remains queryable. After restoring
the original setting, the second collection must expose every acknowledged ID
and support vector search. No Pod restart, process signal, disk fill or memory
pressure is injected.

This is a **cold subscription / initial-loading** case, not a benchmark for an
already-loaded reader, normal query scheduling or disk I/O. A timed-out query is
a censored attempt, not a completed four-second query-latency sample. HTTP errors,
connection refusal and incorrect successful responses cannot satisfy the timeout
gate. Baseline/idle/recovery queries must return the expected acknowledged IDs.

## Prepare an existing disposable deployment

Use a namespace dedicated to this test, with no unrelated clients. The pause
setting affects new scanners across the selected Milvus deployment. Kubernetes
provisioning, Helm installation and component sizing are explicit operator steps;
the runner never creates/starts/stops/deletes a cluster or changes current context.

From the repository root, build the current CLI and prepare a baseline:

```bash
make wpdiag
# Install the stack's pinned Python dependencies in your usual environment.
python3 -m pip install -r deployments/milvus-full-stack-helm/scripts/requirements-connections.txt

python3 tests/integration/diagnostics/prepare_values.py --profile consume \
  --output /tmp/consume-baseline.json

deployments/milvus-full-stack-helm/stack.sh install \
  --context YOUR_CONTEXT --namespace consume-acceptance --with prometheus \
  -f /tmp/consume-baseline.json \
  -f tests/integration/diagnostics/values/acceptance.yaml
# Review the native Helm/kubectl plan, then repeat with -x.
```

Choose a compatible Milvus image through Helm values; use `--local-arm64` only
when the documented local development image is available. The receipt records
the actually tested combination rather than assuming every image implements
these APIs and metric contracts. For a custom shared `user.yaml`, pass your
Milvus values file to `prepare_values.py --base-values` so its other settings are
preserved. `--profile consume` adds only the explicit false pause baseline;
the helper's default admission profile remains unchanged.

An explicit `FileSource` baseline is necessary on builds that cannot expose
implicit default provenance. The runner accepts a baseline false value from
FileSource or an existing EtcdSource override and restores that exact provenance,
including original override absence. It refuses injection without this evidence.
The acceptance fixture sets DataNode memory to 2 GiB; account for the full
deployment's memory and wait for all rollouts to finish before measurement.

The default Prometheus service is `monitoring-kube-prometheus-prometheus`, with
15-second scrapes and the stack's exact labels `namespace`, `cluster="local"`,
`woodpecker_id="milvus"`. Use `--release` and `--prometheus-service` for alternate
release/service names. This first scenario uses loopback port-forwards and the
example's unauthenticated REST/management APIs; production authentication is
outside its scope. Prometheus query volume and source series are explicitly
bounded; the fixture raises the real multi-node series limit to 2,500.

Opt in only the disposable namespace:

```bash
kubectl --context YOUR_CONTEXT label namespace consume-acceptance \
  diagnostics.woodpecker.io/consume-acceptance=cli-consume-v1
```

## Plan and execute

```bash
# Offline plan: no subprocess, connection, artifact or mutation.
python3 tests/integration/diagnostics/consume_acceptance.py \
  --context YOUR_CONTEXT --namespace consume-acceptance

# Explicit bounded execution.
python3 tests/integration/diagnostics/consume_acceptance.py \
  --context YOUR_CONTEXT --namespace consume-acceptance \
  --output /tmp/consume-evidence --execute
```

The runner creates the same create-only namespace ConfigMap lock used by
admission acceptance, preventing cooperating runs from mutating shared settings
concurrently. It owns three loopback forwards and at most two random
`consume_accept_<run-id>_<baseline|paused>` collections. Existing names are checked
before creation and never reused/deleted. Pod UIDs, readiness, image IDs and
restart counts must remain unchanged during this non-restart scenario.

Default measurement phases last 30 seconds, with 16-second scrape padding and
eight seconds for pause-setting propagation. Working phases offer four serial
Insert requests per second, 16 rows of 16 dimensions/request (about 4 KiB/s of
logical float-vector values before IDs/JSON). Idle sends no Inserts; TimeTick
and other control traffic can still produce scanner/MsgPack activity. Every
query targets an ID acknowledged before that attempt. The writer stops on any
failed or ambiguous Insert rather than reusing possibly committed IDs.

HTTP probe deadlines default to four seconds. Exact-ID verification/search have
separate bounded calls. The execution budget defaults to 720 seconds and cleanup
has independent bounded calls. Workload sizes, duration, request rate and query
deadlines are bounded; each collection fits the exact 16,000-ID query limit.
Artifacts are limited to 16 MiB per JSON file and a 96 MiB raw-artifact budget;
the small final receipt is written separately. These are fixture limits, not production tuning advice.

## Gates and interpretation

| Phase | Workload evidence | Diagnostic expectation |
| --- | --- | --- |
| Working baseline | All Inserts acknowledged; exact Strong-query IDs and vector search; live Strong probes succeed | Actual MsgPack counter increase for the REST-described collection ID; retained unpaused state; no pre-existing pressure hypothesis |
| Idle | No Inserts; existing acknowledged IDs stay queryable | Retained unpaused state and no manufactured scanner/processing-pressure diagnosis; background/control activity is allowed |
| New scanner paused | Acknowledged Inserts to the new collection; bounded Strong-query timeouts; existing-collection Strong probes succeed; new collection remains Loading | At least one actual channel has continuously sampled pause=1; startup pause without pending work must not become downstream-processing or WP-storage pressure |
| Recovery | Exact ID set and vector search in the new collection; concurrent writes/Strong probes succeed | Positive collection MsgPack activity and at least two retained unpaused samples on every observed channel |
| Historical replay | Query the original incident interval after successful recovery | Original pause evidence and standalone/composed conclusions remain; current recovery does not rewrite history |
| Missing evidence | Wrong namespace selector / unreachable backend | Missing facts fail the working gate; backend failure is not a completed healthy result |

Standalone and composed stage findings must agree on rule/version, outcome,
impact/evidence strength, flow/stage, target, summary and uncertainty. Fresh run
UUIDs can differ. Original source and observation references must resolve, and
the integrated summary must retain original finding IDs. Repeated views of one
observation are not independent confirmations.

The pause gauge is shared by scanners on a physical channel, not a per-reader
registry. The module intentionally reports `scanner_pause_state_observed` when
pause=1 but pending bytes=0. It does not claim downstream backpressure without
the source-owned pending-work evidence. The workload/configuration experiment
supplies external corroboration for this controlled startup case; the stage
does not fetch management controls or guess a specific reader's desired frontier.
No channel-to-WP-client-metadata/log mapping is invented. Lazy, missing or unused
remote/WP retrieval series remain gaps, including normal local/memory-tail paths.

## Inspect evidence and recover after interruption

Each unique output directory contains:

- `acceptance.json`: builds, selection/limits, phases, correctness and restoration.
- `<phase>.requests.json` and `all-requests.json`: acknowledged ID ranges, exact
  probe targets, successes/errors and attempt durations. Successful query latency
  and censored timeout-attempt duration distributions are separate.
- `<phase>.consume.json` / `.integrated.json`: actual source samples, executed
  queries, stage findings, identities and uncertainty; also historical replay.
- `restoration.json` / `fault-applied.json`: original configuration provenance,
  applied setting and restoration readback. The baseline journal exists before
  the first owned collection, covering process loss during baseline/idle too.
  `wpdiag.private.json` is local-only.

SIGINT/SIGTERM run bounded restoration before owned-data cleanup and lock release.
An artifact-write failure cannot skip restoration. External changes to the pause
value/provenance cause refusal instead of being overwritten; unresolved config or
data cleanup retains the lock. SIGKILL/power loss cannot run cleanup. Inspect the
original receipt and journal, then use the explicit recovery helper:

```bash
python3 tests/integration/diagnostics/consume_recover.py --run PATH_TO_RUN \
  --context YOUR_CONTEXT --namespace consume-acceptance
# Review the exact configuration and owned collection plan, then add --execute.
```

Recovery verifies the original target, coordinator/lock identities and registered
control before restoration. It drops only the two allowed names from that run,
checks absence, and records `manual-recovery.json` without rewriting a failed
acceptance receipt into a pass. Missing/changed identity or external values need
manual investigation. The configuration API exposes value/source rather than
an atomic compare-and-swap revision; the exclusive lock coordinates test runners,
not arbitrary concurrent administrator edits.

Review raw reports before sharing internal labels/connections. The scenario does
not claim complete query/persistence/native/WebUI acceptance, sustained reader
stall coverage, disk Write/Sync timing, crash durability or lossless telemetry.

```bash
# CI runs these HTTP/lifecycle/evidence fixtures without Kubernetes or faults.
python3 -m unittest discover -s tests/integration/diagnostics -p 'test_*.py' -v
```
