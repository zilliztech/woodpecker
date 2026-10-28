# Published tracing prerequisite snapshot — 2026-10-28

This is a **read-only source/registry prerequisite check**, not a new deployed
Milvus tracing receipt. Both checked candidates remain
`source_prerequisites_not_met` (exit 3). #459 remains open; no supported published
full-trace combination has been established by these checks.

The [checker guide](RELEASE_COMPATIBILITY.md) explains reproduction and limits.
The earlier [local development-image receipt](FULL_STACK_VALIDATION.md) remains
separate evidence. No Kubernetes context, release, Pod or cluster was changed;
no image layers were downloaded or binaries/images executed in this work.

## Actual checked refs and requirements

The stable GitHub release APIs returned Milvus **v3.0.4** (published
2026-10-26 06:53:52 UTC) and WP **v0.1.47** (2026-10-14 14:23:52 UTC). Version
strings alone were not treated as compatibility evidence. The actual source
refs resolved to:

| Source ref | Immutable commit | Observation |
| --- | --- | --- |
| Milvus v3.0.4 | `a6125c53333f59f596fcfb4940b8e73818e36739` | Both root and `pkg` module files declare WP v0.1.46. |
| WP v0.1.46 | `c45123a65de000be22d5d981e649da5c6e73e5c4` | Declared embedded-client candidate and independently observed server trace baseline. |
| WP v0.1.47 | `0ef6e94b69aa6fae80494915b8f8835668a01553` | Newer source release, still before the two client fixes below. |

Inspect the exact
[root module](https://github.com/milvus-io/milvus/blob/a6125c53333f59f596fcfb4940b8e73818e36739/go.mod)
and [pkg module](https://github.com/milvus-io/milvus/blob/a6125c53333f59f596fcfb4940b8e73818e36739/pkg/go.mod).
These are declared requirements, not a reconstructed effective Go MVS graph or
proof of the dependency actually compiled into an arbitrary image.

| Boundary | Known implementation/baseline | Recorded ancestry / disposition |
| --- | --- | --- |
| Milvus per-message WAL propagation | Milvus #50796, `5eebaa9ad4ee5eadd82071cecc1b839df9cf1441` | v3.0.4 is **diverged**; release-branch equivalence requires review. Divergence alone is not proof that an equivalent backport is absent. |
| Embedded WP queue, batch link and RPC handling | WP #479, `c7e4185da64ea5bcac3f988ace4c10b819644383` | Declared v0.1.46 candidate is behind this commit; requirement not established. |
| Embedded WP preserves Milvus host provider | WP #483, `d4511a259ce796a5584bdef5436d2599a7906d0a` | Declared v0.1.46 candidate is behind this commit; requirement not established. |
| WP service trace baseline | WP v0.1.46, `c45123a65de000be22d5d981e649da5c6e73e5c4` | v0.1.46 identical, v0.1.47 ahead. Client-only #479/#483 changes are not imposed on the server. Actual version pairing still requires live acceptance. |

WP v0.1.47 is also behind #479/#483. Direct inspection of its tagged
`common/tracer/tracer.go` retained the old noop initialization path, which
installs a provider rather than preserving the host's instrumentation. A server
tag change cannot upgrade the client compiled into Milvus.

## Actual registry metadata

Public Docker Hub `milvusdb/woodpecker` returned **14 tags**, no next page,
with v0.1.46 the latest versioned entry by last-updated ordering. It had no
v0.1.47 entry; direct Buildx inspection of that exact tag returned **not found**.
This is the public repository observation, not a statement about private images
or every registry. A GitHub release does not itself establish a public image.

| Public image | Index digest | Selected linux/arm64 manifest |
| --- | --- | --- |
| `milvusdb/milvus:v3.0.4` | `sha256:af49657e3a5dd6b613bf1adcccab8f601221ee05cbf0aa9e8a50935d5fe80dec` | `sha256:dd2e8a80c0001302d655f80cc58cd8c3ea121afbbfd1b9ec0f40454129e0c740` |
| `milvusdb/woodpecker:v0.1.46` | `sha256:008b303879a6c7c71167c00c0743bf9f469ba5afdf5a51672316840e1e376b13` | `sha256:938ba3108ca565cf66962787b20d50fbd9fbbf3e6bdad1c222fd2a3be4a1fc28` |
| `milvusdb/woodpecker:v0.1.47` | Metadata unavailable at checked public tag | No platform digest qualified |

Both available indexes also advertise Linux AMD64; this execution selected
ARM64. Index/platform-manifest digests are different objects from the image
configuration IDs recorded by some container runtimes; none is silently
substituted for another. Actual binary/source/digest binding remains unverified.

## Actual runs and verification

Both final executions used checker source SHA-256
`2a085ae516bdfe5560f4c0e72cb085f8e05da9c79b3d0b50433349a59fd485eb`.
They read public metadata through bounded `gh api` and Buildx calls only.

| Run / UTC interval | Selection | Result |
| --- | --- | --- |
| `20261028T152859Z-0b3c5021`, 15:28:59.149463–15:29:15.643530 | Milvus v3.0.4 source/image; WP v0.1.46 source/image | Exit 3; two available ARM64 manifests, old declared embedded client, Milvus backport unreviewed. Ten metadata calls. |
| `20261028T152859Z-0566c031`, 15:28:59.149418–15:29:15.295796 | Same Milvus; WP v0.1.47 source/image | Exit 3; same old embedded-client declarations, server baseline satisfied but selected public server image unavailable. Eleven metadata calls. |

Every report retains `full_trace_acceptance_complete=false` and
`runtime_source_image_binding_verified=false`. No single/batch/slow/recovery
workload was run against these public candidates. An old declared dependency
does not rule out an operator's separately rebuilt/replaced binary; that needs
actual build provenance. No runtime failure or successful full tracing is
fabricated from metadata alone.

All **23 offline tracing tests** passed (ten existing graph/acceptance tests and
thirteen new prerequisite counterexamples). They cover overrides/exclusions,
different module declarations, pseudo-version timestamps, independent client
and server requirements, diverged ancestry, platform ambiguity, missing metadata,
budgets and the non-equivalence of preflight and live acceptance. They run in
the existing Trace Export Integration workflow; no new online registry/GitHub
check is forced into ordinary CI.

## Remaining #459 gate

1. Identify a pullable Milvus image with the per-message propagation (original
   commit or reviewed equivalent) and an actual embedded WP client containing
   both #479/#483. A traceable client revision/pseudo-version can be reviewed;
   publishing a new server tag is not an invented prerequisite.
2. Record actual binary dependency/build provenance, image/platform digests and
   the separately selected server configuration/version.
3. Execute the existing full-stack single, batch-link, supported slowdown and
   explicit recovery controls, plus Strong query/search/data correctness.
4. Retain original span/link identities, standalone/composed diagnoses and
   sampling/export/retention/instrumentation gaps. Only that accepted result
   can satisfy #459. #450 remains open for this gate.

Raw local artifacts remain under `/tmp/wp-trace-release-459-live/<run-id>/`.
Reviewed fingerprints:

| Artifact | SHA-256 |
| --- | --- |
| v0.1.46 candidate `release-check.json` | `34c884a2282c9389d6c1bc812699db1ee15eed04a8a29041a4c0254c5dbdd815` |
| v0.1.47 candidate `release-check.json` | `4f1583f11338f2b19ffcb3386bd0fdcb09859ac5bf51c72b7f1562801fcc07ec` |
| Both runs `milvus-root-go.mod.json` | `b0f26d9619c3812d82e4b4bb069e0a896367f3c41c4a217005d9441d57ff99a3` |
| Both runs `milvus-pkg-go.mod.json` | `0c08c0ddf396f68ca056e3ed27ee7914fadfcd526de4cd9680ea048431f427db` |
| Both runs `milvus-manifest.json` | `d7d9a4041544be51bd637a8fe99d8a2df4c8f249175a73cef978f8a280e5731d` |
| Available v0.1.46 `wp_server-manifest.json` | `3d7db103f086aa31bcf4cb245c6ba745306d4bd90bc6be84681401f898dfe517` |
| Separate reviewed public tag catalog | `e311237dcf069cd3078258335476bcc95992fe7c25292c2e9457959a33cdaa22` |
