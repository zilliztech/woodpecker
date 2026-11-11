# Interactive wp shell

See also the native [Interactive shell handbook](../../docs.html#operations-cli-shell)
for the connect parameter table, command availability by connection state, and
copyable examples in the Docs layout with section navigation.

The shell keeps two independent targets visible: a Woodpecker **service context**
and an optional **tenant metadata connection**. Multiple tenants can share one
service while using different etcd clusters, metadata prefixes, and storage
locations. Start with a source build that includes `wp shell` (`make wpcli`);
check `wp shell --help` when using a released binary.

## Configure service access

Create `~/.woodpecker/cli.yaml`, or use `WOODPECKER_CLI_CONFIG` to choose a file.
The normal [configuration search order](configuration.md#file-location) applies.
For access from outside Kubernetes, forward each replica's admin port and map
the identities advertised by that deployment:

```yaml
current-context: wp-prod
contexts:
  wp-prod:
    endpoint: http://127.0.0.1:19091
    timeout: 10s
    node_admin_urls:
      node-0: http://127.0.0.1:19091
      node-1: http://127.0.0.1:19092
      node-2: http://127.0.0.1:19093
  wp-staging:
    endpoint: http://127.0.0.1:29091
```

Replace the example identities with the actual node IDs or advertised addresses.
An etcd-only context cannot serve as a shell's service context; it needs an Admin
HTTP endpoint. Existing standalone raw etcd commands still support etcd-only
contexts.

## Choose a context

```console
$ wp shell
Contexts from /Users/example/.woodpecker/cli.yaml:
  1) wp-prod  [current]
  2) wp-staging
Select context by number or name [1]:
Service context: wp-prod
Config file:     /Users/example/.woodpecker/cli.yaml
Admin endpoint:  http://127.0.0.1:19091
Node mappings:   3
Tenant metadata: not configured
wp[context=wp-prod | meta=disconnected]>
```

The selector always appears when `--context` is omitted, including with one
configured context or an existing `current-context`. Names are sorted. Enter a
number or exact name; Enter accepts a valid current context. Without a valid
current context, an explicit choice is required. Invalid input prompts again.
Ctrl+C or EOF cancels selection without connecting to a cluster.

To skip selection, run `wp shell --context wp-prod`. Unknown contexts fail
explicitly. Selection never rewrites `cli.yaml`. A missing/invalid configuration
reports its source and how to configure service access.

The shell targets the selected context even if `WOODPECKER_ENDPOINT` is set by a
server image. An explicit startup `--endpoint` or `--node-admin-url` overrides the
initial service configuration; `use` loads the next context's own connection
settings. Standalone `wp` endpoint precedence remains unchanged.

## Operate on the service and attach a tenant

Commands inside the shell use their existing names without a leading `wp`:

```console
wp[context=wp-prod | meta=disconnected]> node list
wp[context=wp-prod | meta=disconnected]> cluster health
wp[context=wp-prod | meta=disconnected]> meta log list
Prerequisite: no tenant metadata connection configured. Connect with: ...
wp[context=wp-prod | meta=disconnected]> connect tenant-a --etcd 127.0.0.1:32379 --meta-prefix tenant-a/woodpecker --bucket tenant-a-bucket --root-path milvus/wp
wp[context=wp-prod | meta=tenant-a]> meta log list
wp[context=wp-prod | meta=tenant-a]> log readers application-log
wp[context=wp-prod | meta=tenant-a]> log scan application-log --mode raw
wp[context=wp-prod | meta=tenant-a]> segment inspect application-log 0
wp[context=wp-prod | meta=tenant-a]> status
```

`connect` validates a bounded etcd read in the candidate metadata prefix before
replacing the active tenant. It accepts `--etcd-username`, `--etcd-password`,
`--etcd-cert`, `--etcd-key`, `--etcd-cacert`, and `--etcd-tls-min-version`, using
the existing TLS/auth implementation and local certificate paths. Credentials
belong in these options rather than endpoint URLs.

The tenant name is a session alias. The endpoints and complete `--meta-prefix`
select the metadata; `--bucket` and `--root-path` select the physical storage
instance. The metadata prefix and storage root are independent values.

Attaching metadata alone is supported: omit both storage options. Metadata and
raw etcd commands then work, while `log scan`, `segment inspect`, and `segment
probe` require a bucket/root pair before making requests. Supply it on that
command or reconnect with the pair. Explicit command options override inherited
values for that invocation; they do not change the session. Attached storage
defaults also filter `instance data` and `node log-health`, whose existing root
option is named `--root`.

No tenant is attached on entry, even if `cli.yaml` contains etcd settings. Shell
metadata commands require an explicit attachment and never fall back to service
metadata discovery. A failed connection reports whether etcd/TLS/authentication
failed and retains the previous tenant. A later outage retains the configured
target and reports its failure; it does not silently detach or switch tenants.

## Inspect and switch the session

| Command | Behavior |
| --- | --- |
| `status` | Show selected service, configuration source, tenant, etcd endpoints, metadata prefix, bucket and storage root; redact credentials; make no network requests. |
| `status --check` | Check Admin and tenant etcd separately with bounded requests; skip etcd when detached. Failure retains the selected configuration. |
| `connect <tenant> --etcd ... --meta-prefix ...` | Validate and replace all tenant settings, including credentials, TLS and storage. Omitted values never carry over from the previous tenant. |
| `disconnect` | Clear the entire tenant attachment while keeping the service context. |
| `use <context>` | Switch the service context for this session and clear the tenant after a successful switch. Invalid contexts leave both targets unchanged. |
| `help`, `help <command>`, `<command> --help` | Show existing command help and shell command help. |
| `exit` / `quit` | Leave the shell. |

```console
wp[context=wp-prod | meta=tenant-a]> connect tenant-b --etcd 127.0.0.1:42379 --meta-prefix tenant-b/woodpecker
wp[context=wp-prod | meta=tenant-b]> status --check
wp[context=wp-prod | meta=tenant-b]> disconnect
wp[context=wp-prod | meta=disconnected]> use wp-staging
wp[context=wp-staging | meta=disconnected]> exit
```

The prompt names configured targets rather than continuously checking their
health. `meta=disconnected` means no tenant is configured. Separate shells retain
independent service/tenant settings. Use the session's `use` command to change
services; per-command `--context` cannot retarget the session. Existing `wp ctx
use` (or `ctx use` in the shell) remains a persistent change to the original
configuration file and does not retarget the running session.

## Editing, cancellation, and automation

Tab completes commands, subcommands, flags and names after `use`. Arrow keys
recall session history. History stays in memory; `connect` and lines containing
credential options or endpoint URLs are excluded. No history file is written.
Quoted arguments and escaped spaces are supported; OS shell expansion, pipes,
and redirection are not evaluated.

Ctrl+C clears the input line or cancels the active operation while retaining the
session. EOF at an empty prompt exits. Every existing mutation still requires
its own preview/confirmation, revision, reason and backup as applicable. Flags
such as `--apply` and `--yes` never carry into the next command. Cancellation does
not undo a request already accepted by a server; inspect the target afterward.

For piped input, explicitly select a context:

```bash
printf 'status\nnode list\nexit\n' | wp shell --context wp-prod
```

Piped commands produce output without prompts. Errors are reported and later
lines continue; the shell exits nonzero if any command failed. Normal
single-command `wp` calls retain their existing behavior and exit codes.

Operational commands run in isolated instances of the same `wp` executable.
Each receives a private temporary `cli.yaml` containing the current session
settings (directory mode 0700, file mode 0600), removed when that command exits.
Its source diagnostic can therefore name this temporary file; `status` always
shows the original configuration source. The shell does not modify the original
file except when explicitly invoking persistent `ctx use`.
