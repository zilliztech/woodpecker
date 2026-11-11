# Retired standalone diagnostic executable

`wpdiag` is retired. The current Woodpecker-only product is part of `wp`:

```bash
make wpcli
bin/wp diag catalog
bin/wp diag metrics check writer-latency --window 15m
bin/wp diag scenario slow-writes --window 15m --node-id woodpecker-0
bin/wp report export diagnosis.json --packet
bin/wp webui --config /path/to/cli.yaml
```

See [diagnostics and operations](../../docs/admin-guides/wpcli/diagnostics.md)
for connections, rules, incident windows, scope and migration. The legacy
executable refuses source/stage/integrated/UI execution and performs no queries.
Milvus and Loki diagnosis are outside the supported product. Historical source
modules, tests and receipts remain available for reference and reuse; they do
not expose additional supported executable capabilities.
