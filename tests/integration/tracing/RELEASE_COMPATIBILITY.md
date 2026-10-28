# Source and published-image prerequisites for #459

Run this read-only check before provisioning a new full-stack tracing candidate.
It records explicit source refs as immutable commits, the WP requirements in
both Milvus Go modules, known tracing-commit ancestry and registry platform
digests. The [dated snapshot](RELEASE_CHECK_VALIDATION.md) records why the checked
public versions are not yet qualified. #459 remains open.

Three separately versioned parts matter:

1. Milvus producer/SN per-message propagation, originally Milvus PR #50796.
2. The WP **client compiled into Milvus**, including #479's queued-context/batch
   links and RPC instrumentation, plus #483's preservation of the host provider.
3. A compatible WP server with tracing enabled. Released v0.1.46 is the existing
   independently observed server trace-export baseline. #479 is a client-path
   change; do not require client-only commits on the server or assume a server
   upgrade updates Milvus's embedded client. The exact pairing still needs live
   acceptance.

## Inspect and execute

Requires Python 3 and, for online execution only, authenticated `gh` plus Docker
Buildx registry inspection. Normal credential setup stays outside the tool.
No Go build, cluster, image-layer download or executable/image run is needed.

```sh
python3 tests/integration/tracing/release_check.py \
  --milvus-ref v3.0.4 --wp-server-ref v0.1.46 \
  --milvus-image milvusdb/milvus:v3.0.4 \
  --wp-image milvusdb/woodpecker:v0.1.46 \
  --platform linux/arm64
# Review the offline plan, then repeat with --execute.
```

Supply the actual source ref and published image/digest of a candidate, not a
tag guessed from its apparent age. Use `linux/amd64` for x86-64. Registry tags
are recorded with both index and selected platform digests. Ambiguous platform
entries and single-manifest images lacking a separately verified platform stay
unverified; the tool does not guess. It never changes the stack's defaults.

Default planning performs no network, subprocess or artifact write. Execution
writes a unique private folder under `/tmp/wp-trace-release-459/`, with public
source-file snapshots, registry manifests and `release-check.json`. Limits are
24 metadata calls, 40 seconds per call, 180 seconds total by default (60–360
allowed), 2 MiB per JSON artifact and 16 MiB per run. Raw backend/auth errors
are withheld, not copied into reports. No Kubernetes command is issued.

| Exit / report status | Meaning |
| --- | --- |
| 0 / offline plan | Nothing was queried or executed. |
| 0 / `requires_runtime_verification` | All four known source-ancestry prerequisites and image-platform metadata were observed. Runtime image/source binding and real tracing are still unverified. |
| 3 / `source_prerequisites_not_met` | A declared source candidate predates a required commit. This is a source prerequisite failure, not a claim that a live request was tested or every possible custom image is incompatible. |
| 1 / `inconclusive` | Unavailable metadata, divergent/unreviewed backports, mismatched modules, unsupported declarations or unverified image/platform. |

The report always leaves `full_trace_acceptance_complete=false` and
`runtime_source_image_binding_verified=false`. A successful metadata check
alone does not complete #459.

## Provenance limits

The module reader deliberately qualifies only one literal unreplaced WP
requirement per module. Different root/pkg requirements, replacements,
exclusions, duplicate declarations and unknown syntax remain unverified. This
is **not a Go MVS resolver**: transitive requirements, workspaces, alternate
modfiles or build flags can change the effective binary dependency even when
the two declarations agree. Verify the actual embedded build information and
runtime digest before trusting a candidate.

WP pseudo-versions resolve through their commit suffix and must match the
recorded commit timestamp. This resolves ancestry, not canonical module-graph
or release certification. A public trace-compatible client revision need not
force publication of a new server tag; server and client remain independent.

An included original commit does not rule out later semantic reverts. A
divergent release branch does not prove that an equivalent cherry-pick is
absent. Review the selected source/backport and run its focused propagation
tests; do not rewrite divergent ancestry into a fabricated positive or negative
runtime verdict.

After source/image provenance is established, run the existing
[full-stack controls](FULL_STACK.md): single-entry parent ancestry, exact batch
links, observed slowdown and explicit recovery, plus Strong query/search and
data correctness. Save actual Pod image IDs, raw retained span/link witnesses
and standalone/composed findings. Those are the live gate. The earlier
[local development receipt](FULL_STACK_VALIDATION.md) remains valid local
evidence and does not become a published-image receipt.

The offline counterexamples join the existing Trace Export Integration job:

```sh
python3 -m unittest discover -s tests/integration/tracing -p 'test_*.py' -v
```

They cover module ambiguity/overrides, source/server separation, pseudo-version
timestamps, stale/unknown ancestry, exact platform digests, metadata failure,
budgets and the distinction between metadata and actual full-trace acceptance.
