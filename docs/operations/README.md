# Operations documentation maintenance

Both documents are native content pages in the existing Docs shell:

- `docs.html#operations-cli`: overview and searchable operation lookup.
- Sixteen topic pages, defined in `cli-categories.json`, each with one clear
  operational purpose. Most correspond to exactly one command group; only
  local CLI setup (ctx/completion/version) and cluster/node status combine groups.
- `docs.html#operations-scenarios`: choose a troubleshooting path.
- `docs.html#operations-scenarios-read`: read stall.
- `docs.html#operations-scenarios-write`: write stall.
- `docs.html#operations-scenarios-corruption`: corruption diagnosis and recovery.

Their sidebar, navigation, typography, code blocks and light/dark palette come
from `docs.html`. There is no iframe, dynamic fetch or separate site layout.
Each reference page includes a right-hand command navigation rail.
Deep links use `#operations-cli-log/log-scan` or `#operations-scenarios-corruption/metadata`.
Legacy operations.html and operations/cli.html / scenarios.html redirect into
the corresponding Docs content and preserve section links.

To regenerate from the built CLI:

```bash
go build -o bin/wp ./cmd/wpcli
python3 docs/operations/generate_cli.py --binary ./bin/wp
```

The generator executes only --help, never a cluster operation. Update cli-categories.json when adding or moving a command group. Update authored
examples and interpretation in cli-examples.json; missing examples fail the
build. Edit scenario content in _scenario-read.html / _scenario-write.html /
_scenario-corruption.html, the chooser in _scenarios.html, shared example
variables in _workflow-connection.html, CLI setup in _setup.html, raw etcd workflow in _etcd.html,
metadata repair in _meta.html, or the overview in _overview.html. Then run
the same generator.
The _cli.html overview and _cli-*.html category fragments are generated. embed_operations.py namespaces fragment
IDs and links, adds standard Docs code/table markup, and updates the marked
Operations content region of docs.html without touching other content pages.

Commit source edits together with generated docs.html. Examples are labelled
illustrative; captured lab evidence is explicitly distinguished. Preserve the
scope differences between metadata consistency, local block coverage, object
reads and application progress. Update the release-scope note when the local
CLI additions become part of a published release.

Verify switching between Overview, CLI Handbook and Troubleshooting stays
within docs.html. Check light/dark mode, search, section links, mobile layout
and legacy redirects.

The current handbook covers the operations registered on master, including typed
metadata inspection/repair and log truncation. It lists 20 top-level commands/groups
and 72 executable operations across 16 topics; an operation such as `meta log show`
is not a separate top-level command. Update examples and category configuration
together when adding CLI capabilities. The generator fails if an operation lacks
an authored example or topic.
