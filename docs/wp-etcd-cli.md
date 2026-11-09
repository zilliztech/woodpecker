# Explore raw etcd keys

`wp etcd` provides read-only access to raw etcd keys. It needs an etcd endpoint,
but neither a Woodpecker admin endpoint nor a metadata prefix. Keys are used
literally; a context's `meta_prefix` is never prepended.

## Discover a metadata namespace

```bash
# Omit the prefix to list keys across the store (100 keys per page by default).
wp etcd list --etcd 127.0.0.1:32379

# Narrow the search once you recognize a namespace.
wp etcd list milvus-prod/woodpecker/ --etcd 127.0.0.1:32379

# Inspect one exact key, including revisions, lease and value encoding.
wp etcd get milvus-prod/woodpecker/logs/my-channel --etcd 127.0.0.1:32379

# Print just the value. Binary values default to base64.
wp etcd cat milvus-prod/woodpecker/logs/my-channel --etcd 127.0.0.1:32379

# Save exact bytes, without an added newline.
wp etcd cat milvus-prod/woodpecker/logs/my-channel --etcd 127.0.0.1:32379 --encoding raw > log-meta.pb
```

`list /` only searches keys beginning with `/`; omit the argument to search the
whole store. `get` defaults to JSON and supports `-o yaml`. `list` defaults to a
table and also supports JSON/YAML. `cat` prints only the value, independently of
the output format flag. Value encodings are `auto`, `text`, `base64`, and `hex`;
`cat` also supports `raw`. Auto encoding chooses base64 for binary values.

Woodpecker metadata values are protobuf; raw get/cat display their encoded bytes,
not schema-decoded fields. Typed metadata inspection/repair is tracked separately
in [issue #254](https://github.com/zilliztech/woodpecker/issues/254).
These raw commands do not modify etcd.

## Supply explicit etcd parameters

Current source builds require an explicit `--etcd` endpoint or an explicitly
selected `--client-parameters-file`. Raw commands do not use server-context etcd
settings or Admin discovery. They do not require a metadata prefix and never
prepend one. Authentication/TLS paths are host-local request settings.

```bash
wp etcd list --etcd http://127.0.0.1:32379
wp etcd get /tenant/example --etcd http://127.0.0.1:32379
```

See the [connection guide](admin-guides/wpcli/configuration.md#etcd-connections)
for private parameter files and authentication. Named client-meta profiles and
implicit context metadata authority are retired in the current source build.

## Read additional pages

```bash
wp etcd list milvus-prod/ --etcd 127.0.0.1:32379 --limit 20 -o json
```

If `more` is true, pass the response's `next_key_base64` to `--after-key-base64`
and its `snapshot_revision` to `--revision`, retaining the same prefix. This
cursor preserves even non-UTF-8 keys. For ordinary text keys, `next_key` with
`--after-key` is also supported; the two cursor flags are mutually exclusive. Repeat until
`more` is false. This pins all pages to one snapshot even when keys change.
`count` reports the number of matching keys remaining from that page's start;
`keys` contains at most `--limit` records. Reads fail if the requested revision
has already been compacted. `get` and `cat` also accept `--revision`.
