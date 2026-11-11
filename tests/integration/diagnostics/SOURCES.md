# Deployed source and corroboration acceptance (#491)

This representative acceptance runs the **real WP server and MinIO backend**,
collects its unmodified stdout through deployed Alloy's Kubernetes log source,
retains actual RPC spans in Jaeger, and invokes the independent Prometheus,
trace and Loki commands before selected integrated combinations. It never pushes
synthetic logs or imports fixture spans. The [receipt](SOURCES_VALIDATION.md)
separates application evidence, negative selections and replay counterexamples.

The workload acknowledges two small entries, explicitly rejects ten separately
tagged empty-append attempts, verifies both exact payloads, and checks the exact
end-of-file status. Each rejected attempt has its own trace identity. The receipt
counts associated and unresolved identities separately; one event never becomes
multiple independent confirmations. A zero-association run fails the gate.
An input-validation rejection is a real application error, **not an injected
storage outage**. Recovery correctness comes from acknowledgements and reads;
no-match in Loki remains inconclusive. Small completion populations do not
establish a latency SLO. No production thresholds or rule catalogs are changed.

## Prepare the owned worker

Use an existing, disposable Kubernetes namespace with sufficient memory for
the fixture (about 2 GiB of container limits, plus existing workloads). The
harness neither manages minikube nor changes the current Kubernetes context.
It creates only an exclusive acceptance lock and random owned names for a
Pod, ConfigMap, ServiceAccount, Role and RoleBinding. All fixture data uses
bounded emptyDirs; no existing PVC or Helm release is changed.

Build for the **node** architecture, not necessarily the laptop architecture:

```sh
make wpdiag
mkdir -p .local/source-client-491
# Use GOARCH=amd64 for x86-64 nodes.
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -o .local/source-client-491/source-client ./tests/integration/diagnostics/sourceclient
docker build --platform linux/arm64 -f tests/integration/diagnostics/sourceclient/Dockerfile \
  -t wp-source-client-491:local .local/source-client-491
```

Make the seven images listed by the offline plan available to the selected
cluster using its normal registry/image-loading procedure. For an operator's
own existing minikube profile, `minikube -p YOUR_PROFILE image load
wp-source-client-491:local --daemon` is one external preparation option. The
scripts do not run it. Supply a matching platform when building the image.

The standard gRPC instrumented client runs **inside the fixture Pod**. Its
client RPC spans describe actual calls, not invented enclosing work. Using the
node clock avoids comparing laptop timestamps with VM timestamps for very short
RPCs. Even with a common clock, CRI stdout timestamps can lag producer events:
the associator must still require actual span/event overlap. It has no added
clock-skew tolerance or timestamp rewriting to make an association pass.

## Plan, run, inspect

```sh
python3 tests/integration/diagnostics/source_acceptance.py \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE

kubectl --context YOUR_CONTEXT label namespace YOUR_NAMESPACE \
  diagnostics.woodpecker.io/source-acceptance=cli-source-v1

python3 tests/integration/diagnostics/source_acceptance.py \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE \
  --output /tmp/wp-source-491 --execute
```

Planning performs no network access, workload creation or artifact writes.
Execution requires the explicit namespace label, records its UID, acquires the
same create-only lock as the other deployed scenarios and journals before each
create. Cleanup verifies namespace/resource UIDs, labels, ownership and desired
configuration before deleting with UID preconditions. Foreign changes preserve
the unresolved lock. Do not run multiple acceptance scenarios in one namespace.

Artifacts are local investigation data: exact container image IDs/restarts,
the owned collector/sampling/retention configuration, actual request receipts,
bounded producer lines and raw retained traces, independent/composed reports,
current metric snapshots, checks and cleanup status. Files can contain internal
labels/error text; review them before sharing. A successful HTTP query or CLI
exit is execution success, not a healthy verdict.

The selected combinations are WP write/read with all three sources, E2E
write/read with trace+Loki, plus Milvus write/read **no-selected-producer**
controls. A WP-only deployment cannot prove a Milvus query path or connected
Proxy → SN → WP propagation. E2E relationship gaps must remain visible. The
separate [#459 tracing acceptance](../tracing/FULL_STACK.md) owns published
Milvus/embedded-WP/server combinations; this scenario does not depend on them.
The existing admission, consume and query scenarios retain their real Milvus
metrics proof. This is a selected source matrix, not every Cartesian pairing.

Controls also cover an unrelated actual request in the same incident window,
empty retained search, an exact missing trace returning Jaeger HTTP 404,
post-rejection no-match, a one-line Loki budget, interrupted live collection,
and selected versus unselected unreachable backends. HTTP 404 currently remains
an explicit failed lookup; empty successful search remains inconclusive. Neither
means healthy. Successful pagination cannot establish collector completeness;
idle traffic, unknown formats, ingestion lag and retention remain explicit gaps.
Current metric snapshots are labeled separately from historical evidence and
are never joined to the individual error request.

## Replay identities without altering the backends

After a genuine association, run the pure production summarizer against its
retained integrated report, then deliberately mutate **copies** as counterexamples:

```sh
WP_SOURCE_ACCEPTANCE_REPORT=/tmp/wp-source-491/RUN/wp-write.integrated.json \
  go test -race -v ./tests/integration/diagnostics/sourcecheck
```

Wrong identity, outside-span time, other deployment, conflicting module identity,
dangling lineage and historical/current mixing must withhold association.
Replaying an identical report keeps the same association and finding references;
it does not create independent confirmations. Prometheus/native evidence is not
joined without authoritative mapping. These mutations are offline regression
fixtures, never application events or data pushed into Loki/Jaeger.

## Bounds, cancellation and interrupted recovery

Execution has a 240–900 second work budget (480 by default), 180-second startup
deadline, fourteen RPCs with 20-second request deadlines and a 90-second worker bound.
Artifacts are bounded to 16 MiB per file and 96 MiB per run, reserving final
failure/cleanup journals. The existing namespace workload/configuration inventory
and fixture process identities must stay unchanged throughout capture.
Cleanup uses separately bounded calls even if the work budget has expired.
Reads verify only two small values; the rejected append writes none. Normal
cleanup runs on failure or SIGINT/SIGTERM. SIGKILL requires the original journal:

```sh
python3 tests/integration/diagnostics/source_recover.py \
  --context YOUR_CONTEXT --namespace YOUR_NAMESPACE \
  --receipt /tmp/wp-source-491/RUN/fixture.json
# Inspect, then repeat with --execute to delete only original owned resources.
```

Recovery leaves the original failed `acceptance.json` unchanged. Each run has a
fresh ID; failed receipts are never overwritten. Jaeger uses bounded ephemeral
memory (10,000 traces); Loki filesystem data, collector position and Prometheus's
one-hour TSDB are emptyDirs. Cleanup intentionally removes this owned history.
Sampling is 1 for this controlled fixture; neither sampling nor readiness proves
lossless export/ingestion, stable retention, disk/fsync or crash durability.

Offline checks need no Kubernetes or backend; client protocol counterexamples
need a loopback socket:

```sh
go test -race ./tests/integration/diagnostics/sourceclient
python3 -m unittest discover -s tests/integration/diagnostics -p 'test_*.py' -v
go test -race ./diagnostics/jaeger ./diagnostics/loki ./diagnostics/integrated
```
