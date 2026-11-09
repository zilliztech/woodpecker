---
name: wp-wal-metadata
description: Inspect current Woodpecker WP-client log, segment and quorum metadata and existing consistency checks using wp meta or the wp webui Metadata view. Does not add dynamic diagnosis or repairs.
---

Resolve the exact client authority: etcd endpoints, full metadata prefix and log
name. The WP service context is a different connection; never derive the client
prefix from the service's etcd namespace. Clarify ambiguous authorities rather
than reading another instance.

Use existing read-only commands, discovering required parameters with `wp meta
--help` and the selected subcommand's help:

```bash
wp meta log show --etcd <endpoints> --meta-prefix <client-prefix> --output json -- <log-name>
wp meta segment list --etcd <endpoints> --meta-prefix <client-prefix> --output json -- <log-name>
wp meta quorum list --etcd <endpoints> --meta-prefix <client-prefix> --output json
wp meta check --etcd <endpoints> --meta-prefix <client-prefix> --output json -- <log-name>
```

Alternatively start `wp webui`, choose a WP context, explicitly attach the client
metadata authority, and open its existing snapshot/check views. Switching context
detaches metadata; browser choices do not change global cli.yaml.

These are current snapshots. Metadata views provide context, not validation of
all constraints. A passing `meta check` covers its existing checks only. Report
actual violations and observation time; source failure is not an inconsistency.
Current metadata cannot reconstruct a past Metrics/Trace incident. There is no
metadata scenario under the new historical `wp diag` command.

Do not expand into watchers, dynamic metadata diagnosis, repair, fencing,
deletion or rebalancing. Ordinary inspection requires no model or WebUI.
