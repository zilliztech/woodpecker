# Stack examples documentation

This section introduces the purpose of the Milvus + Woodpecker stack, then connects
Helm deployment, monitoring/alerts, wpcli operations and repeatable E2E experiments.
All pages are native content in `docs.html`, under `#stack-examples` and its topic
routes. The existing Docs navigation, theme, code-copy behavior and mobile layout
are shared; no iframe, client fetch or separate website is required.

Edit the `_*.html` source fragments, then regenerate:

```bash
python3 docs/stack-examples/generate.py
```

Commit both sources and generated `docs/docs.html`. The generator changes only
the marked `STACK EXAMPLES NAV` and `STACK EXAMPLES CONTENT` regions, so regenerating
the Operations or Quick Start sections preserves these pages. Styling is in
`docs/assets/stack-examples.css`; the shared Docs router handles section links
such as `#stack-examples-wpcli/scopes`.

Keep commands and defaults aligned with `deployments/milvus-full-stack-helm/`.
Explain the WP service context separately from each Milvus instance's WP-client
metadata connection. Distinguish checks available now from future fault/recovery,
optimization and autonomous-agent examples. New E2E chapters should describe their
starting state, trigger, workload, expected signals, investigation, recovery and
verification, with evidence a reader can reproduce.

Before publishing, regenerate twice to check stability and verify routes, section
links, code copying, existing Docs navigation, light/dark themes and mobile layout.
Only rendered Docs content is published; source/generator files in this directory
are excluded by `docs/_config.yml`.
