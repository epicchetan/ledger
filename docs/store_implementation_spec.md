# Ledger Store Implementation Spec

## Purpose

`store` is Ledger's generic object registry and local object layer.

It is not the runtime data plane, a feed runner, a projection system, or a
market-domain crate. It owns durable object registration and local materializing
of bytes so the rest of Ledger can say:

```text
register this file as an object
list objects by role/kind/id prefix
hydrate this object into a local path
delete this exact object
inspect or prune the local object
validate descriptor/local/remote consistency
```

The important boundary is simple:

```text
store owns bytes and descriptors
feeds read hydrated object paths
runtime owns active cells and scheduling
projections own their own state shape
Lens/API expose object exploration, not market-specific processing
```

## Crate

```text
crates/store
```

The Rust package and library crate are both named `store`.

Public API concepts:

```text
Store
R2Store
StoreObjectId
StoreObjectRole
StoreObjectDescriptor
RegisterFileRequest
HydratedObject
ObjectFilter
DeleteObjectReport
LocalStoreStatus
LocalStorePruneReport
StoreValidationReport
RemoteStore
R2RemoteStore
```

There is intentionally no old raw migration API. Store registration starts from
a local file path, uploads it to the current R2 layout, mirrors a descriptor, and
records the local object entry.

## Object Model

An object is content-addressed by SHA-256:

```text
sha256-<64 lowercase hex characters>
```

Descriptors are JSON records that carry the generic metadata needed by API,
Lens, feeds, and future library code:

```text
id
role
kind
file_name
content_sha256
size_bytes
format
media_type
remote
local
lineage
metadata_json
created_at_ns
updated_at_ns
last_accessed_at_ns
```

`role` is intentionally small:

```text
raw
artifact
```

`kind` and `metadata_json` are caller-owned. For example, Databento raw files can
use `kind = databento.dbn.zst`; future ODTE captures or runtime artifacts can use
their own values without changing `store`.

## Local Layout

Default root:

```text
<LEDGER_DATA_DIR>/store
```

With the default `LEDGER_DATA_DIR=data`, this is:

```text
data/store
```

Layout:

```text
data/store/
  registry/
    objects/
      <shard>/
        sha256-<hash>.json
  local/
    objects/
      sha256-<hash>/
        <file-name>
  tmp/
    put/
    hydrate/
```

The local object area is allowed to evict objects by least-recently-used descriptor
metadata when `LEDGER_STORE_LOCAL_MAX_BYTES` is exceeded. The default cap is
20 GiB.

## R2 Layout

Store writes new remote objects under a namespaced layout:

```text
store/objects/sha256/<shard>/<hash>/<file-name>
store/registry/objects/sha256/<shard>/<hash>.json
```

Examples:

```text
store/objects/sha256/af/af0a480d.../raw.dbn.zst
store/registry/objects/sha256/af/af0a480d....json
```

The descriptor mirror exists so R2 can be inspected independently of the local
registry. The local registry remains the source of truth for local API and CLI
operations.

## API Surface

The API exposes object exploration only:

```text
GET    /health
GET    /store/objects?role=raw&kind=databento.dbn.zst&id_prefix=sha256-af
GET    /store/objects/:id
DELETE /store/objects/:id
```

Delete removes the exact descriptor, local file, R2 object, and R2 descriptor
mirror when present. It does not cascade to related objects.

## CLI Surface

The CLI stays a thin adapter:

```text
ledger store list
ledger store show --id <object-id>
ledger store import-file --path <path> --role raw --kind <kind>
ledger store hydrate --id <object-id>
ledger store delete --id <object-id>
ledger store local-status
ledger store local-prune
ledger store validate [--id <object-id>] [--verify-remote]
```

Commands parse arguments, construct `R2Store`, and print JSON. Behavior belongs
inside `store`.

## Validation

Validation is intentionally about store integrity only:

```text
descriptor can be decoded
local object file exists when descriptor says it exists
local object size/hash matches descriptor
remote object exists when remote verification is requested
remote size/hash matches descriptor when available
```

It does not validate market semantics, feed correctness, bars, or projection
results.

## Tests

Store tests should cover:

```text
object id parsing
role parsing
descriptor registry round trip
file registration uploads object and descriptor mirror
local object path uses data/store
R2 key path uses store/
hydrate restores a missing local object
delete removes descriptor, local, object, and descriptor mirror
local prune evicts least recently used local copies
```

## Success Criteria

This phase is complete when:

```text
workspace builds with crates/store
API and Lens use /store/objects
CLI uses ledger store ...
default local data lives under data/store
new R2 keys live under store/
no migration-only store API remains
retired crates are not referenced
cargo test --workspace passes
```
