# Deployed diagnostic CLI acceptance

This is the first deployed scenario for [#458](https://github.com/zilliztech/woodpecker/issues/458):
**normal writes and strong queries → controlled admission slowdown → recovery**.
It tests the existing independent `wpdiag milvus append` / `milvus query` commands
and the integrated `wpdiag run` CLI. No WebUI is required.

The harness reuses the stack's bounded REST workload and the existing source/stage
rules. It does not add diagnostic rules, change their thresholds, or turn the Helm
wrapper into a diagnosis engine. Diagnostics remain read-only. Only this explicitly
opted-in harness creates test data and temporarily changes four Milvus controls.

## Prepare a disposable deployment

Use an existing Kubernetes context and a **dedicated namespace with no other
workloads or clients**. The fault controls affect the selected Milvus deployment,
not just the test collection. The scripts never create/start/stop/delete a
Kubernetes cluster or change the current context. Provision that separately.

From the repository root:

```sh
make wpcli wpdiag
python3 -m pip install PyYAML  # also required by the existing stack planner
python3 tests/integration/diagnostics/prepare_values.py \
  --output deployments/milvus-full-stack-helm/.local/admission-baseline.json

deployments/milvus-full-stack-helm/stack.sh install \
  --context YOUR_CONTEXT --namespace diag-acceptance --with prometheus \
  -f deployments/milvus-full-stack-helm/.local/admission-baseline.json
# Inspect the native Helm/kubectl plan, then repeat with -x to install.
```

Use the stack's `--local-arm64` option only when the documented local development
image is available; otherwise select a compatible Milvus build using Helm values.
The [stack guide](../../../deployments/milvus-full-stack-helm/README.md) covers
images, resources and component selection. Grafana, Loki, Jaeger and Alertmanager
are not prerequisites for this metrics/runtime scenario.

The baseline helper merges explicit original defaults (0.85, 4mb, 256kb, 20m)
into the example's existing `extraConfigFiles.user.yaml`, preserving its other
settings. When supplying custom values, pass their file to `--base-values`.
Some Milvus builds cannot report the provenance of implicit built-in defaults.
The harness refuses injection unless all four controls can be read and restored;
the explicit Helm baseline makes that requirement reproducible.

The selected release must have one Ready mix coordinator, a reachable REST
service, service-mode Woodpecker Pods, and Prometheus scraping the stack labels
`namespace`, `cluster="local"`, `woodpecker_id="milvus"` every 15 seconds.
The first profile uses local port-forwards and assumes authentication is disabled
on this disposable stack. Authenticated production deployments are outside its
scope. All commands name the context and namespace explicitly.

After verifying the selected namespace is disposable, opt it in:

```sh
kubectl --context YOUR_CONTEXT label namespace diag-acceptance \
  diagnostics.woodpecker.io/acceptance=cli-admission-v1 --overwrite
```

## Plan, execute and inspect

```sh
# Offline plan: no commands, HTTP requests, files or cluster mutations.
python3 tests/integration/diagnostics/cli_acceptance.py \
  --context YOUR_CONTEXT --namespace diag-acceptance --wp bin/wp

# Explicit execution. Exit 0 means every gate passed; any failed gate exits 1.
python3 tests/integration/diagnostics/cli_acceptance.py \
  --context YOUR_CONTEXT --namespace diag-acceptance --wp bin/wp --execute
```

Defaults: three 45-second write phases, each with 30-second settling, 20-second
bounded priming writes to materialize lazy histograms, and 16-second scrape
padding before/after each phase, 8 insert requests/second, four workers, 256 rows
of 128 float dimensions per request. Offered logical vector load is about 1 MiB/s;
achieved throughput is reported separately. Strong queries run concurrently about
twice per second. Each phase checks the cumulative acknowledged row count, sampled
IDs and a vector search. This is live correctness, **not crash durability**.

`--duration`, `--prime-duration`, `--settle`, `--scrape-wait`, `--rps`, `--concurrency`, `--batch-size`,
`--dimension`, `--max-requests`, `--request-timeout` and `--total-budget` have
explicit bounds. Logical vector payload is capped at 512 KiB/request and 256 MiB
across all phases. Default workload/diagnostic budget is 720 seconds; cleanup has
separate bounded calls so an expired workload deadline cannot prevent restoration.
REST JSON, transport and port-forward overhead are included in client latency.
The driver is bounded rather than a calibrated capacity benchmark.

The fault lowers the node-memory slowdown threshold to 0.0001, adaptive slowdown
high/low watermarks to 512kb/256kb, and default burst to 1mb. This deliberately
activates the producer's protective rate guard **without exhausting memory**.
It exercises `milvus_streaming_produce_rate_limit_delay_seconds` and the existing
`suspected_quota_admission_wait` finding. It does not inject slow disk, an outage,
or collection quota rejections. These settings are reproduction controls, not
production tuning advice. Propagation/adaptation takes time; a non-reproducing
fault fails acceptance rather than being reported as success.

The gates require:

- Successful writes, concurrent strong reads, exact acknowledged row counts,
  sampled IDs and vector search in every phase.
- Positively observed Proxy write/query completions. Missing, idle or
  inconclusive series cannot pass this gate. Other gaps remain in the raw report.
- A non-slow admission baseline, admission-wait suspicion during the fault, and
  no persisting admission suspicion after exact configuration restoration.
- Client insert p95 at least 1.5× baseline during the fault; recovered p95 at most
  0.8× the fault p95. These are acceptance contrast controls, not SLOs.
- Identical rule/version/outcome/severity/strength for standalone append diagnosis
  and its composed child over the same retained interval. Finding and observation
  references must resolve; the integrated summary retains original child IDs.
- With `--wp`, re-query the fault's historical interval after recovery, alongside
  an explicitly labeled **current snapshot**. The historical suspicion must remain.
  Omit `--wp` for metrics-only acceptance; the receipt records that coverage gap.

Histograms describe different exporter populations; their quantiles cannot be
subtracted to allocate an individual request's latency. No time-valid Milvus
client-metadata mapping is invented from a WP service context.

## Artifacts and failure recovery

Each run writes a unique directory under `tests/integration/diagnostics/.local/`
(override with `--output`). Keep `acceptance.json`, `workload.json`,
`requests.jsonl`, phase `*.reads.json`, `*.append.json`, `*.query.json`,
`*.integrated.json`, `capabilities.json` and `restoration.json` together. They
record image IDs/Pod identities, binary hashes, exact windows, workload limits,
user latency/throughput/correctness, source observations and derived findings.
The Prometheus series budget is explicitly 2,500 for this small multi-node fixture.
Artifact/input/output limits still apply; exceeding them fails the run.

`*.private.json` are transient local connection files, not portable reports.
Treat the complete output directory as private, review artifacts before sharing,
and do not commit it. No Secrets or Pod environment variables are collected.

A create-only `wpdiag-cli-acceptance-lock` ConfigMap serializes opted-in harnesses
within the namespace; it is not a distributed lock against operators or arbitrary
clients. Do not modify the deployment during a run. The restoration journal is
written **before** mutation. Cleanup runs on errors and Ctrl+C/SIGTERM, preserves
original etcd values **or override absence**, and verifies exact value/source
readback before releasing the owned lock. The unique owned collection is dropped
by default; `--keep-collection` retains it. Existing collections are never reused.

SIGKILL, power loss and host failure cannot run cleanup. If a lock remains, inspect
the original receipt and journal before starting another run. The explicit recovery
helper plans locally by default, then validates the opt-in namespace, original lock
UID/run ID, coordinator identity and current controls before restoring:

```sh
python3 tests/integration/diagnostics/recover.py --run PATH_TO_RUN \
  --context YOUR_CONTEXT --namespace diag-acceptance
# Review, then repeat with --execute.
```

It refuses unrelated target identities or externally changed controls. A failed
restoration retains the lock and unresolved journal for manual investigation.
The helper does not delete test data: inspect the named `diag_accept_*` collection
and drop only that original owned collection separately if desired. With changed
coordinator identity or an unavailable management API, restore the four journaled
values/override absences through Milvus management operations after operator review;
do not blindly remove the lock. The helper records a separate recovery receipt and
does not rewrite a failed acceptance result into a pass.

## Automated checks and remaining coverage

```sh
python3 -m unittest discover -s tests/integration/diagnostics -p 'test_*.py' -v
```

CI runs these cluster-free lifecycle/lineage/negative controls with local HTTP
fixtures. The deployed scenario is manual and explicitly opted in; CI does not
provision a cluster or inject faults. See [VALIDATION.md](VALIDATION.md) for a real
deployment receipt and its limitations.

This PR is a subset of #458. WebUI interaction/cancellation, trace and Loki
combinations, other stages, identity conflicts and restart/reset/source-gap
acceptance remain in the parent validation matrix. It does not close #458 or
establish complete Milvus-to-WP trace propagation (#459).
