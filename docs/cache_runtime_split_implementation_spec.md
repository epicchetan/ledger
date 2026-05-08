# Cache / Runtime Split Implementation Spec

This spec covers the next structural cleanup for the generic runtime work:

```text
crates/cache
crates/runtime
```

It replaces the current single `crates/ledger-runtime` crate shape. The goal is
not to add feed execution yet. The goal is to separate the standalone active
state primitive from the scheduler/projection contract that uses it.

## 1. Conceptual Model

The current `ledger-runtime` crate contains two different ideas:

```text
cache / data plane
  active typed cells
  ownership checks
  read/write primitives
  changed-key effects

runtime
  external write queue
  projection trait and context
  projection dependency index
  projection queue
  deterministic run loop
```

Those are related, but they are not the same layer.

The cache should be useful by itself:

```text
register typed cells
read typed cells
mutate typed cells
enforce writer ownership
return changed keys
```

It should not know what a projection is. It should not know what a feed is. It
should not know about scheduling.

The runtime should build on cache:

```text
accept feed/external write batches
apply those batches to cache
collect changed keys
schedule projections
run projections through ProjectionContext
collect projection changed keys
schedule downstream projections
```

Concrete feeds and concrete projections still belong above these generic crates,
in the Ledger application layer or feature-specific crates.

## 2. Target Crate Shape

Workspace members:

```text
crates/cache
crates/runtime
crates/store
crates/api
crates/cli
```

Dependency direction:

```text
cache
  no dependency on runtime
  no dependency on store
  no dependency on api/cli/lens

runtime
  depends on cache
  no dependency on store
  no dependency on api/cli/lens

ledger application later
  depends on cache, runtime, store
  owns concrete feeds/projections/domain payloads
```

This keeps the hot state primitive independently testable and keeps the runtime
scheduler generic.

## 3. `cache` Crate

Create:

```text
crates/cache
```

Package name:

```toml
name = "cache"
```

### Public Responsibility

`cache` owns active runtime state:

```text
Key
CellOwner
CellDescriptor
CellKind
ValueKey<T>
ArrayKey<T>
Cache
CacheError
WriteEffects
```

Rename the current public `DataPlane` vocabulary:

```text
DataPlane      -> Cache
DataPlaneError -> CacheError
```

The old phrase "data plane" can remain in docs as historical context, but public
Rust API should use `Cache`.

### Ownership Model

The current implementation uses:

```rust
CellOwner::Runtime
CellOwner::Origin(String)
CellOwner::Projection(String)
```

After the split, `cache` should not need to know about "origin" or
"projection". Use an opaque owner id instead:

```rust
pub struct CellOwner(Key);
```

or an equivalent private-string newtype with the same validation rules as `Key`.

Recommended owner ids:

```text
feed.databento.es_mbo
feed.live.es_mbo
projection.bars_1m
projection.gamma_heatmap
```

`cache` only checks equality:

```text
writer owner == registered cell owner
```

It does not enforce that `feed.databento.es_mbo` only writes
`feed.databento.es_mbo.*` keys. Prefix discipline is a convention owned by the
application/runtime layer.

Suggested API:

```rust
impl CellOwner {
    pub fn new(value: impl Into<String>) -> Result<Self, CacheError>;
    pub fn as_str(&self) -> &str;
}
```

Do not add reserved constructors such as `CellOwner::runtime()` in `cache`.
`cache` should not have built-in knowledge of runtime, feeds, or projections. If
any higher layer needs a specific owner string later, that layer can create it
through `CellOwner::new(...)`.

### Cell Keys

Keep the current `Key` behavior:

```text
lowercase dotted paths
ascii lowercase letters, digits, and underscores per segment
```

Examples:

```text
feed.databento.es_mbo.status
feed.databento.es_mbo.batches
projection.bars_1m.current
projection.bars_1m.completed
runtime.error
```

The old `origin.*` examples should be renamed in tests and docs to `feed.*`.

### Cell Kinds

Keep the two current primitives:

```text
Value<T>
Array<T>
```

The cache must not introduce:

```text
schemas
streams
cursors
feed lifecycle
projection lifecycle
runtime scheduler
persistence
store/object semantics
websocket/frame semantics
```

### File Layout

Move from the current `ledger-runtime` source tree:

```text
crates/ledger-runtime/src/key.rs        -> crates/cache/src/key.rs
crates/ledger-runtime/src/cell.rs       -> crates/cache/src/cell.rs
crates/ledger-runtime/src/data_plane.rs -> crates/cache/src/cache.rs
crates/ledger-runtime/src/error.rs      -> crates/cache/src/error.rs
```

Create:

```text
crates/cache/src/lib.rs
```

Export:

```rust
pub use cache::Cache;
pub use cell::{ArrayKey, CellDescriptor, CellKind, CellOwner, ValueKey, WriteEffects};
pub use error::CacheError;
pub use key::Key;
```

The module name `cache::cache` is awkward. Prefer:

```text
src/state.rs
```

or:

```text
src/cache.rs
```

If using `src/cache.rs`, public usage still reads cleanly:

```rust
use cache::Cache;
```

## 4. `runtime` Crate

Rename:

```text
crates/ledger-runtime -> crates/runtime
```

Package name:

```toml
name = "runtime"
```

`runtime` depends on `cache`:

```toml
[dependencies]
cache = { path = "../cache" }
thiserror.workspace = true
```

### Public Responsibility

`runtime` owns scheduler and projection execution contracts:

```text
ProjectionId
ProjectionDescriptor
Projection trait
ProjectionContext
ProjectionError
ExternalWriteBatch
Runtime
RuntimeError
RuntimeStep
RunStats
```

The `Projection` trait belongs here because it is the runtime execution
contract:

```text
how dependencies are declared
how projection work is called
what context is passed to projections
how projection errors are wrapped
how changed keys are collected for downstream scheduling
```

Concrete projections do not belong here.

### Projection Ownership

`ProjectionId` should produce a cache owner id without requiring cache to know
about projections:

```rust
impl ProjectionId {
    pub fn owner(&self) -> CellOwner {
        CellOwner::new(self.as_str()).expect("projection id already validates as owner")
    }
}
```

Projection ids should remain key-like:

```text
projection.bars_1m
projection.gamma_heatmap
```

### ProjectionContext

`ProjectionContext` stays in `runtime`.

It wraps cache access with runtime meaning:

```text
read input cells from cache
write projection-owned output cells to cache
record changed keys
return WriteEffects to Runtime
```

Conceptually:

```rust
pub struct ProjectionContext<'a> {
    cache: &'a Cache,
    projection_id: ProjectionId,
    effects: WriteEffects,
}
```

This type should not move into `cache`, because it knows about projection ids and
runtime scheduling effects.

### ExternalWriteBatch

Keep `ExternalWriteBatch` in `runtime`.

It is not a universal cache primitive. It is the runtime's queued input format:

```text
external caller/feed creates batch
runtime queues batch
runtime applies batch to cache
runtime schedules projections from changed keys
```

The writer is a `cache::CellOwner`:

```rust
let mut batch = ExternalWriteBatch::new(CellOwner::new("feed.databento.es_mbo")?);
```

The runtime does not know whether that owner is a replay feed, live feed,
journal feed, or test caller.

### Runtime Scheduler

Keep the current scheduler behavior:

```text
external writes are FIFO
all queued external write batches apply before one projection run
projection queue is FIFO
projection queue dedupes while queued
changed keys dedupe while preserving first-seen order
projection writes schedule downstream projections
run_until_idle has a max step guard
```

Rename fields and docs from `data_plane` to `cache`:

```text
Runtime::new(data_plane) -> Runtime::new(cache)
Runtime::data_plane()    -> Runtime::cache()
```

If a temporary compatibility method helps tests during the move, keep it private
or remove it before finishing the task.

### File Layout

Move into `crates/runtime`:

```text
src/projection.rs
src/runtime/mod.rs
src/runtime/external_write.rs
src/runtime/dependency.rs
src/runtime/projection_registry.rs
src/runtime/projection_queue.rs
```

Public `src/lib.rs` should export:

```rust
pub use projection::{
    Projection, ProjectionContext, ProjectionDescriptor, ProjectionError, ProjectionId,
};
pub use runtime::{ExternalWriteBatch, RunStats, Runtime, RuntimeError, RuntimeStep};
```

It should re-export cache types only if ergonomics demand it. Prefer explicit
imports in callers:

```rust
use cache::{Cache, CellOwner, Key};
use runtime::{Runtime, Projection};
```

Avoid making `runtime` a facade for all cache APIs. The point of the split is
that cache stands alone.

## 5. Tests

Split the existing tests by responsibility.

Move to `crates/cache/tests`:

```text
key.rs
data_plane.rs -> cache.rs
```

Rename test helpers and examples:

```text
origin_owner() -> feed_owner()
origin.es_mbo.* -> feed.databento.es_mbo.*
DataPlane -> Cache
DataPlaneError -> CacheError
```

Move to `crates/runtime/tests`:

```text
projection.rs
external_write.rs
runtime_scheduler.rs
```

Runtime tests should import cache explicitly:

```rust
use cache::{Cache, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use runtime::{ExternalWriteBatch, Projection, Runtime};
```

Expected test coverage after the split:

```text
cache:
  key validation
  owner validation
  typed value registration/read/write
  typed array registration/read/write
  public descriptor reads
  changed-key effects
  clone/shared-cell behavior

runtime:
  external batch operation ordering
  external batch owner mismatch propagation
  projection registration/dedupe
  dependency scheduling
  projection context read/write behavior
  downstream scheduling from projection writes
  run_until_idle guard
```

## 6. Workspace And Import Updates

Update root `Cargo.toml`:

```toml
[workspace]
members = [
    "crates/cache",
    "crates/runtime",
    "crates/store",
    "crates/api",
    "crates/cli",
]
```

Remove:

```text
crates/ledger-runtime
```

Update any references:

```text
ledger-runtime -> runtime
DataPlane      -> Cache
DataPlaneError -> CacheError
Origin         -> feed/external owner naming
origin.* keys  -> feed.* keys
```

Search patterns before finishing:

```text
ledger-runtime
ledger_runtime
DataPlane
DataPlaneError
data_plane
CellOwner::Origin
origin.es_mbo
origin.
```

Some old proposal text may intentionally mention "data plane" conceptually. New
implementation docs should prefer "cache" when describing the crate/API.

## 7. Documentation Updates

Update or supersede:

```text
docs/ledger_runtime_data_plane_implementation_spec.md
docs/ledger_runtime_scheduler_implementation_spec.md
docs/ledger_runtime_simplified_data_plane_proposal.md
```

Recommended approach:

```text
keep old specs as implementation history
add a short note at the top saying the code has since split into cache/runtime
make this spec the active rename/split spec
update the proposal's crate responsibility section after implementation
```

Do not spend time rewriting every historical section before code validates.

## 8. Implementation Sequence

1. Create `crates/cache`.
2. Move key/cell/cache/error modules and cache tests into it.
3. Rename public types:

```text
DataPlane -> Cache
DataPlaneError -> CacheError
CellOwner::Origin -> opaque CellOwner ids
```

4. Rename `crates/ledger-runtime` to `crates/runtime`.
5. Add `cache = { path = "../cache" }` to `runtime`.
6. Update runtime projection/context/external-write code to import cache types.
7. Rename runtime methods from `data_plane` language to `cache` language.
8. Move tests to the appropriate crates and update imports/examples.
9. Update workspace members and lockfile.
10. Run formatting and tests.
11. Sweep stale names and docs.

## 9. Validation

Required commands:

```bash
cargo fmt --all
cargo test -p cache
cargo test -p runtime
cargo test --workspace
git diff --check
```

Expected result:

```text
cache tests pass independently
runtime tests pass against cache dependency
workspace compiles without crates/ledger-runtime
no public Rust API exposes DataPlane naming
no code uses CellOwner::Origin
```

## 10. Non-Goals

Do not add these during the split:

```text
generic Feed trait
feed lifecycle
async runtime/task model
store integration
API runtime routes
Lens runtime views
frame bus
persistence
domain payload types
concrete replay/live/0DTE feeds
concrete projections
```

Those come after the cache/runtime boundary is validated.

## 11. Success Criteria

This split is complete when:

```text
cache can be tested without runtime
runtime owns the projection trait/context/scheduler
runtime depends on cache, not the other way around
external/feed writes enter runtime as ExternalWriteBatch
projection writes flow through ProjectionContext
changed cache keys still drive projection scheduling
all old ledger-runtime naming is removed from active code
```

At that point we can spec the first feed trait and first concrete feed with a
clean dependency graph.
