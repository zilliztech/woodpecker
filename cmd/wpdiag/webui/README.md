# Local Woodpecker operations workspace

Start with `wp webui --config /path/to/cli.yaml`. The loopback-only workspace
presents Diagnostics, Operations and Command Explorer over the same wp CLI
checks and native commands. It owns no collection or diagnosis rules.

See [the user guide](../../../docs/admin-guides/wpcli/diagnostics.md).
The old `wpdiag serve` and combined Milvus/Loki UI are retired. Existing source
receipts are historical, not supported execution surfaces.

To regenerate the embedded command directory, build wp and run:

```bash
python3 cmd/wpdiag/webui/generate_command_catalog.py --binary ./bin/wp
```

Only help is invoked; no source query or operational command is run. Commit the
resulting catalog so a clean checkout builds without a generation bootstrap.
