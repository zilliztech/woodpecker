# Loaded query/search consistency-wait acceptance

This focused [#486](https://github.com/zilliztech/woodpecker/issues/486) scenario
validates the existing `wpdiag milvus query` and integrated query-stage commands
against a real, already-loaded collection. It works from the merged #482 helpers
without #485, Jaeger, Loki, WebUI or native WP metadata. See
[QUERY_VALIDATION.md](QUERY_VALIDATION.md) for the actual deployment receipt.

The request control is an explicit **Customized guarantee timestamp** slightly
ahead of a fresh server-issued Insert acknowledgement timestamp. Milvus retains
that fence and waits for TSafe; normal Strong requests on the same acknowledged
row are paired controls. This exercises actual visibility-wait instrumentation
without pausing consumers, changing configuration, filling disk or imposing CPU
pressure. It is a controlled request, not a Strong-query production incident or
evidence that the deployment/WP storage is unhealthy.

## Build the isolated test client

From the repository root:

```bash
make wpdiag
go -C tests/integration/diagnostics/queryclient build -o ../../../../bin/query-requests .
```

`query-requests` is a test helper, not another diagnosis command. Its nested Go
module pins official Milvus go-api/v3 protobufs and gRPC dependencies separately
from production modules. It sends actual Insert, Query and Search RPCs, validates
transport/status/IDs and records uint64 timestamps as decimal strings. A timestamp
offset adds milliseconds to the 18-bit-logical Hybrid Timestamp physical part;
it does not depend on the load-generator clock. The inspected REST interface does
not expose this guarantee timestamp. Strong requests may be optimized differently
and are never labeled as the artificial future-fence requests.

## Prepare an existing disposable stack

Provision Kubernetes separately and select a dedicated namespace with no unrelated
clients. The harness never starts/stops a cluster, changes the current context,
installs Helm releases or writes management configuration. Use the existing stack
wrapper to plan/install core services and Prometheus, with images supporting this
protocol and the query metric catalog:

```bash
deployments/milvus-full-stack-helm/stack.sh install \
  --context YOUR_CONTEXT --namespace query-acceptance --with prometheus \
  -f tests/integration/diagnostics/values/acceptance.yaml
# Review, then add -x to execute the native Helm/kubectl operations.
kubectl --context YOUR_CONTEXT label namespace query-acceptance \
  diagnostics.woodpecker.io/query-acceptance=cli-query-consistency-v1
```

Supply compatible image overrides; use the stack's `--local-arm64` only when its
documented development image is available. The receipt records the tested image
combination; it does not imply all chart default images support every API/metric.
Unlike the admission or consume startup fault, this scenario needs no special
configuration baseline or dynamic management API. The acceptance sizing gives
DataNode 2 GiB; budget enough resources for the whole stack and finish rollouts
before measurement.

The first profile assumes unauthenticated disposable Milvus REST/gRPC services,
15-second Prometheus scrapes and the stack's exact labels `namespace`,
`cluster="local"`, `woodpecker_id="milvus"`. Alternative release/service names
use `--release` and `--prometheus-service`. It owns loopback forwards for the
Milvus HTTP/gRPC service and Prometheus. The source-series budget is explicitly
2500 for the real multi-node deployment; source policy thresholds and minimum
completion counts are unchanged.

## Plan, execute and interpret

```bash
# Offline plan: no backend calls, commands or artifacts.
python3 tests/integration/diagnostics/query_acceptance.py \
  --context YOUR_CONTEXT --namespace query-acceptance

# Explicit execution.
python3 tests/integration/diagnostics/query_acceptance.py \
  --context YOUR_CONTEXT --namespace query-acceptance \
  --output /tmp/query-evidence --execute
```

| Phase | Requests and correctness | Required source evidence |
| --- | --- | --- |
| Loaded baseline | Seed 64 vectors; 24 marker batches with paired Strong query/search, exact IDs and search | Qualified successful query **and** search populations at Proxy and TSafe scopes |
| Idle | No application reads/writes during the selected interval; collection remains loaded | Observed idle query/search populations, not absent series as healthy zeros |
| Future fence | Each acknowledged marker supplies a server timestamp; concurrent Customized query/search use `ack + 2000ms`, paired Strong controls use the same row | `suspected_visibility_wait` for both operations at unchanged thresholds; no unrelated scheduling/core/loading pressure used as the cause |
| Recovery | Stop requesting future fences; normal Strong requests and exact full ID set/search | Normal completion/TSafe populations; no deployment configuration is repaired |
| Historical replay | Re-query the original future-fence interval after normal requests recover | Same retained visibility suspicion and standalone/composed conclusions |
| Missing evidence | Wrong namespace selector / unreachable backend | Failed working gate; backend failure stays distinct from completed empty collection |

Each batch inserts one new marker and sends four reads (query/search, each target
and Strong control), so the default active phase makes 120 RPCs and acknowledges
24 new rows. Defaults are two concurrent batches scheduled over 30 seconds,
16-second scrape padding, 2000 ms fence offset and eight-second RPC deadlines.
The data footprint is deliberately small; this is not a capacity benchmark.

`--batches` (20–48), `--parallel` (1–4), `--duration` (10–60 seconds),
`--future-ms` (1000–5000), `--scrape-wait` and `--budget` have explicit bounds.
The default total execution budget is 720 seconds. Cleanup has independent
bounded calls. Raw JSON is capped at 16 MiB/file and a 96 MiB raw-artifact budget,
with the small final receipt written separately.

The runner checks Loaded/progress=100 before/after requests and samples it during
the active client phases. These samples do not prove every between-sample state.
Any observed Loading/unloaded state, wrong ID, failed RPC or deadline error fails
acceptance instead of becoming a successful slow-query sample. The selected Pod
UIDs/images/restarts and mounted ConfigMap fingerprints must remain unchanged;
arbitrary runtime configuration overrides are not collected.

Client p95 and source histogram p95 bounds describe different populations. Do not
add/subtract them into an exclusive queue, network or WAL duration. Handler/result
waiting can overlap TSafe waiting. Actual request input corroborates this controlled
case; production diagnosis remains a suspicion that needs requested consistency,
timestamp/frontier and consumption evidence. No collection/channel/WP-client
metadata/log association is guessed. A query can be served without a WP read.

## Evidence, cancellation and explicit recovery

Each unique output directory records `acceptance.json`, exact workloads and
timestamps in `<phase>.workload.json`, Loaded snapshots in `*.loaded.json`,
standalone `*.query.json`, composed `*.integrated.json`, negative controls and
private local connection configuration. It records binary hashes, original
finding/observation lineage, deployment identities and mounted configuration
fingerprints. Repeated views of one observation are not independent confirmations.
Review internal labels/connections before sharing raw artifacts.

The runner uses the same create-only namespace lock as admission/consume acceptance
and creates only one unique `query_accept_<run-id>` collection. The owned name is
journaled before creation, including ambiguous creation failures. It never reuses
an existing name or drops a name rejected by preflight. Only owned data is dropped
and absence-checked; failed cleanup retains the lock. SIGINT/SIGTERM clean up with
bounded calls. An owned stdin pipe cancels the Go client's context if its parent
vanishes, and the client also has a 180-second hard context budget. Each request
has a separate deadline. The client never creates/recreates collections.

After process loss, stop/inspect the original run before explicit cleanup:

```bash
python3 tests/integration/diagnostics/query_recover.py --run PATH_TO_RUN \
  --context YOUR_CONTEXT --namespace query-acceptance
# Review the exact owned-data/lock plan, then add --execute.
```

Recovery verifies profile, original target/name, opt-in and lock UID/run identity;
it performs no configuration writes. It writes `manual-recovery.json` and preserves
the original failed receipt. Unknown ownership or a changed lock fails closed.
Unresolved lock creation before ownership was recorded needs manual inspection.

## Checks

```bash
go -C tests/integration/diagnostics/queryclient test -race ./...
python3 -m unittest discover -s tests/integration/diagnostics -p 'test_*.py' -v
```

CI runs protocol/evidence/lifecycle fixtures without Kubernetes fault injection.
They do not replace the retained deployed receipt. Loaded-reader stalls,
CPU/scheduling or slow-execution faults, persistence/WebUI/log/resilience cases,
and released full-trace image acceptance remain separate work under #450/#459.
