# Ledger Runtime Data Plane MVP Spec

This spec is scoped only to the DataPlane slice of the future
`ledger-runtime` crate.

It does not specify the scheduler, projection execution, replay feed controller,
FrameBus, websocket protocol, or Lens integration. Those should be specified
after this slice is implemented and validated independently.

## 1. Conceptual Model

The DataPlane is bounded shared runtime state.

It is not the runtime scheduler. It is not a replay controller. It is not a
projection framework. It does not know Ledger-specific payloads like ES MBO
batches, candles, heatmap cells, or order-book events.

The DataPlane owns only these mechanics:

```text
named cells
cell ownership
cell kind: Value or Array
typed registration
read/write locks
owner-checked mutations
public read metadata
changed-key effects
```

The Ledger application, feeds, and projections own the payload types:

```text
OriginStatus
EsMboBatch
Candles1mBars
HeatmapCell
ProjectionPrivateState
```

So the intended split is:

```text
ledger-runtime:
  "I have a named Value<T> cell and Array<T> cell. I can protect and mutate them."

ledger application:
  "This specific Value<T> is origin status. This Array<T> is ES MBO batches.
   This projection reads those cells and writes these other cells."
```

## 2. Primitive Cells

The first DataPlane has two primitive cell kinds.

### Value Cell

A Value cell holds one optional current value.

```text
Value<T>
```

Use it for latest/current state:

```text
origin status
runtime error
current chart output
projection private state
latest metrics snapshot
```

Required operations:

```text
read
set
clear
update through a closure
```

### Array Cell

An Array cell holds an ordered mutable collection.

```text
Array<T>
```

Use it for ordered collections:

```text
source batches
completed bars
heatmap cells
trades
projection-owned rows
```

Required operations:

```text
read all
read range
replace all
push
insert
replace range
remove range
clear
update through a closure
```

This is intentionally not called an append stream. Some cells will be push-only
by convention, but the primitive itself should support general array mutation.
For example, a future heatmap projection may need range replacement or arbitrary
insertion.

## 3. Important Non-Goals

Do not add these to the DataPlane MVP:

```text
schemas
runtime-owned cursors
append-stream primitive
projection scheduler
dependency graph
async task runner
websocket protocol
FrameBus
domain payload enums
generic JSON payloads on the hot path
durable persistence
cross-cell transactions
cross-cell snapshot isolation
```

Projection cursors are projection state, not DataPlane mechanics. If
`candles_1m` wants to remember that it processed the first 10,000 source
batches, it can store that in a projection-owned Value cell. The runtime does
not need to know what that cursor means.

## 4. Crate Scope

Add a new workspace member:

```text
crates/ledger-runtime
```

Initial files:

```text
crates/ledger-runtime/src/lib.rs
crates/ledger-runtime/src/key.rs
crates/ledger-runtime/src/cell.rs
crates/ledger-runtime/src/data_plane.rs
crates/ledger-runtime/src/error.rs
crates/ledger-runtime/src/projection.rs
crates/ledger-runtime/tests/key.rs
crates/ledger-runtime/tests/data_plane.rs
crates/ledger-runtime/tests/projection.rs
```

Suggested dependencies:

```toml
[dependencies]
thiserror.workspace = true
```

No dependency on:

```text
ledger-domain
ledger-replay
ledger
api
store
ingest
```

This keeps the DataPlane independently testable.

## 5. Public Types

### Key

Cells are addressed by stable string keys.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(String);

impl Key {
    pub fn new(value: impl Into<String>) -> Result<Self, DataPlaneError>;
    pub fn as_str(&self) -> &str;
}

impl std::fmt::Display for Key {
    // write the inner key path
}
```

MVP key validation:

```text
lowercase dotted path
segments contain ascii lowercase letters, digits, and underscores
examples:
  origin.es_mbo.status
  origin.es_mbo.batches
  projection.candles_1m.bars
  projection.heatmap.cells
```

The inner string is private. Invalid keys should be unrepresentable outside the
crate because callers must use `Key::new`.

### Owner

Every cell has an owner. The DataPlane checks that a writer matches the owner
before allowing mutation.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CellOwner {
    Runtime,
    Origin(String),
    Projection(String),
}
```

Examples:

```text
Runtime
Origin("origin.es_mbo")
Projection("projection.candles_1m")
Projection("projection.heatmap")
```

These owner strings are still generic runtime identifiers. They do not introduce
domain payload types into `ledger-runtime`. For the DataPlane MVP, owner strings
are opaque identifiers and only equality is checked. The DataPlane does not
enforce that `Origin("origin.es_mbo")` only writes keys under
`origin.es_mbo.*`; that is a convention for the caller or later runtime layer.

### Cell Kind

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellKind {
    Value,
    Array,
}
```

### Descriptor

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellDescriptor {
    pub key: Key,
    pub owner: CellOwner,
    pub kind: CellKind,
    pub public_read: bool,
}
```

Descriptors are not schemas. They describe runtime mechanics and generic access
metadata, not payload shape. `public_read` is only metadata. The DataPlane does
not need Lens-specific read methods; callers can inspect the descriptor and
decide how to enforce read policy.

## 6. Typed Handles

The DataPlane should be generic over payload types without knowing what those
payloads are.

Use typed handles:

```rust
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct ValueKey<T> {
    key: Key,
    _marker: PhantomData<fn() -> T>,
}

#[derive(Debug, Clone)]
pub struct ArrayKey<T> {
    key: Key,
    _marker: PhantomData<fn() -> T>,
}
```

The handle says:

```text
this key is a Value<T>
this key is an Array<T>
```

`ledger-runtime` does not need to know what `T` is.

The handle constructors should not be public. Registration returns handles, and
callers pass those handles around. The handles may expose `key()` for descriptor
lookup and effects comparison:

```rust
impl<T> ValueKey<T> {
    pub fn key(&self) -> &Key;
}

impl<T> ArrayKey<T> {
    pub fn key(&self) -> &Key;
}
```

Avoid a public `ValueKey::new(Key)` or `ArrayKey::new(Key)` constructor in the
MVP. Reconstructing handles from strings weakens the bounded environment.

Example outside the runtime crate:

```rust
struct OriginStatus { /* ledger-owned */ }
struct EsMboBatch { /* ledger-owned */ }
struct Candles1mBars { /* ledger-owned */ }

let status_key: ValueKey<OriginStatus> = data_plane.register_value(...)?;
let batches_key: ArrayKey<EsMboBatch> = data_plane.register_array(...)?;
let candles_key: ValueKey<Candles1mBars> = data_plane.register_value(...)?;
```

Internally, the DataPlane can store cells type-erased with `Any`. That is an
implementation detail. A `TypeId` check or downcast is acceptable as one cheap
per-operation safety check. It is not schema validation and it must not walk
payload records.

## 7. Registration API

The DataPlane starts empty.

```rust
impl DataPlane {
    pub fn new() -> Self;

    pub fn register_value<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Option<T>,
    ) -> Result<ValueKey<T>, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn register_array<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Vec<T>,
    ) -> Result<ArrayKey<T>, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;
}
```

Registration validation:

```text
descriptor.key is already validated because Key is only constructible through Key::new
key must not already exist in the DataPlane
register_value requires descriptor.kind == Value
register_array requires descriptor.kind == Array
payload type must be Send + Sync + 'static
```

Registration returns the typed handle. Code should pass these handles around
instead of reconstructing keys from strings.

Registration stores the descriptor and storage together in the same cell map:

```text
DataPlane
  cells: Key -> Cell

Cell
  descriptor: CellDescriptor
  storage: ValueStorage<T> or ArrayStorage<T>
```

There is no separate descriptor registry in the MVP. `describe(key)` reads the
descriptor from the registered cell.

## 8. Read API

The MVP read API clones data out of the cell and releases the lock before
returning.

```rust
impl DataPlane {
    pub fn describe(&self, key: &Key) -> Result<CellDescriptor, DataPlaneError>;

    pub fn read_value<T>(&self, key: &ValueKey<T>) -> Result<Option<T>, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn read_array<T>(&self, key: &ArrayKey<T>) -> Result<Vec<T>, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn read_array_range<T>(
        &self,
        key: &ArrayKey<T>,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<T>, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;
}
```

Cloning is acceptable for the MVP because this slice is about correctness and
boundaries. If profiling later shows large clone costs, add `Arc<T>` payloads,
snapshot handles, or range-specific serializers. Do not optimize before the
runtime shape is proven.

## 9. Mutation API

Every mutation takes a writer owner and returns changed-key effects.

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteEffects {
    pub changed_keys: Vec<Key>,
}
```

The DataPlane does not compare old and new payloads. `changed_keys` means "keys
that had a successful write operation committed." Every successful mutation
returns exactly the target key once. Later scheduler and gateway code will
decide how to use those keys for dependency resolution or outbound
notifications. Callers should avoid issuing no-op writes if they do not want
downstream work to run.

### Value Mutations

```rust
impl DataPlane {
    pub fn set_value<T>(
        &self,
        writer: &CellOwner,
        key: &ValueKey<T>,
        value: T,
    ) -> Result<WriteEffects, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn clear_value<T>(
        &self,
        writer: &CellOwner,
        key: &ValueKey<T>,
    ) -> Result<WriteEffects, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn update_value<T, R>(
        &self,
        writer: &CellOwner,
        key: &ValueKey<T>,
        update: impl FnOnce(&mut Option<T>) -> R,
    ) -> Result<(R, WriteEffects), DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;
}
```

### Array Mutations

```rust
impl DataPlane {
    pub fn replace_array<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        items: Vec<T>,
    ) -> Result<WriteEffects, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn push_array<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        items: Vec<T>,
    ) -> Result<WriteEffects, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn insert_array<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        index: usize,
        items: Vec<T>,
    ) -> Result<WriteEffects, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn replace_array_range<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        range: std::ops::Range<usize>,
        items: Vec<T>,
    ) -> Result<WriteEffects, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn remove_array_range<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        range: std::ops::Range<usize>,
    ) -> Result<WriteEffects, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn clear_array<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
    ) -> Result<WriteEffects, DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn update_array<T, R>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        update: impl FnOnce(&mut Vec<T>) -> R,
    ) -> Result<(R, WriteEffects), DataPlaneError>
    where
        T: Clone + Send + Sync + 'static;
}
```

The explicit methods cover common cases. The closure method gives callers
full control without adding a new DataPlane abstraction every time a caller
needs a slightly different mutation.

The closure methods run while the cell write lock is held. Callers should keep
closures small and compute expensive replacement values before calling into the
DataPlane.

## 10. Mutation Rules

Every mutation must validate:

```text
key exists
typed handle matches registered cell type
cell kind matches operation
writer owner equals descriptor.owner
array index/range is valid
```

Array bounds:

```text
range.start <= range.end
range.end <= current array length
insert index <= current array length
empty item lists and empty ranges are allowed
successful empty mutations still return the target key in changed_keys
```

The DataPlane does not validate payload business logic:

```text
no candle timestamp checks
no ES MBO event checks
no heatmap shape checks
no projection cursor checks
no schema checks
```

Those belong to the feed, projection, or application code that owns the payload.

## 11. Locking Model

The DataPlane is synchronous. It does not need async methods for the MVP.

Use normal locks:

```rust
#[derive(Clone)]
pub struct DataPlane {
    inner: Arc<DataPlaneInner>,
}

struct DataPlaneInner {
    cells: RwLock<HashMap<Key, Arc<Cell>>>,
}

struct Cell {
    descriptor: CellDescriptor,
    storage: RwLock<Box<dyn Any + Send + Sync>>,
}

struct ValueStorage<T> {
    value: Option<T>,
}

struct ArrayStorage<T> {
    items: Vec<T>,
}
```

Read flow:

```text
lookup cell
clone Arc<Cell>
release map lock
lock cell for read
downcast to expected ValueStorage<T> or ArrayStorage<T>
clone requested data
release cell lock
return data
```

Write flow:

```text
lookup cell
clone Arc<Cell>
release map lock
validate owner/kind from descriptor
lock cell for write
downcast storage to expected ValueStorage<T> or ArrayStorage<T>
validate array index/range against current storage when applicable
mutate
release cell lock
return WriteEffects
```

Do not hold a lock while doing any future async work. Later runtime tasks can
use channels and `.await`, but they should call the DataPlane synchronously and
release locks before awaiting websocket sends, timers, or feed IO.

If using `std::sync::RwLock`, lock poisoning must become a `DataPlaneError`.
The MVP should not panic on poisoned locks during normal API calls.

## 12. Error Model

Use a specific error enum.

```rust
#[derive(Debug, thiserror::Error)]
pub enum DataPlaneError {
    #[error("invalid key `{0}`")]
    InvalidKey(String),

    #[error("cell `{0}` already exists")]
    DuplicateCell(Key),

    #[error("cell `{0}` does not exist")]
    MissingCell(Key),

    #[error("cell `{key}` expected kind {expected:?}, found {found:?}")]
    WrongCellKind {
        key: Key,
        expected: CellKind,
        found: CellKind,
    },

    #[error("writer {writer:?} cannot mutate cell `{key}` owned by {owner:?}")]
    OwnerMismatch {
        key: Key,
        writer: CellOwner,
        owner: CellOwner,
    },

    #[error("cell `{0}` payload type mismatch")]
    TypeMismatch(Key),

    #[error("array range is out of bounds for cell `{key}`")]
    ArrayRangeOutOfBounds {
        key: Key,
    },

    #[error("data-plane registry lock poisoned")]
    RegistryLockPoisoned,

    #[error("lock poisoned while accessing cell `{key}`")]
    CellLockPoisoned {
        key: Key,
    },
}
```

This crate should not use `anyhow` internally for expected DataPlane failures.

## 13. Example Registration

This example should live in tests or crate docs with local dummy types, not
Ledger domain types.

```rust
#[derive(Clone, Debug, PartialEq)]
struct OriginStatus {
    playing: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct SourceBatch {
    seq: u64,
}

let data_plane = DataPlane::new();

let origin = CellOwner::Origin("origin.es_mbo".to_string());

let status_key = data_plane.register_value::<OriginStatus>(
    CellDescriptor {
        key: Key::new("origin.es_mbo.status")?,
        owner: origin.clone(),
        kind: CellKind::Value,
        public_read: true,
    },
    None,
)?;

let batches_key = data_plane.register_array::<SourceBatch>(
    CellDescriptor {
        key: Key::new("origin.es_mbo.batches")?,
        owner: origin.clone(),
        kind: CellKind::Array,
        public_read: false,
    },
    Vec::new(),
)?;

data_plane.set_value(&origin, &status_key, OriginStatus { playing: true })?;
data_plane.push_array(&origin, &batches_key, vec![SourceBatch { seq: 1 }])?;
```

## 14. Independent Test Plan

These tests validate the DataPlane without scheduler, projections, replay, API,
or Lens.

### Key And Registration Tests

```text
key_accepts_lowercase_dotted_paths
key_rejects_empty_path
key_rejects_uppercase
key_rejects_empty_segments
register_value_returns_typed_handle
register_array_returns_typed_handle
register_rejects_duplicate_key
register_value_rejects_array_descriptor_kind
register_array_rejects_value_descriptor_kind
describe_returns_registered_descriptor
descriptor_public_read_round_trips
describe_missing_key_errors
```

### Ownership Tests

```text
owner_can_set_owned_value
owner_can_mutate_owned_array
runtime_owner_cannot_mutate_origin_cell
origin_owner_cannot_mutate_projection_cell
projection_owner_cannot_mutate_origin_cell
owner_mismatch_does_not_change_cell
```

### Value Cell Tests

```text
read_value_starts_as_initial_value
set_value_replaces_current_value
clear_value_sets_none
update_value_can_modify_existing_value
update_value_can_initialize_none
value_mutation_returns_changed_key
```

### Array Cell Tests

```text
read_array_starts_as_initial_items
read_array_range_returns_slice
read_array_range_rejects_out_of_bounds_range
replace_array_replaces_all_items
push_array_appends_items
insert_array_inserts_at_index
insert_array_rejects_out_of_bounds_index
replace_array_range_replaces_range
replace_array_range_rejects_out_of_bounds_range
remove_array_range_removes_range
remove_array_range_rejects_out_of_bounds_range
clear_array_removes_all_items
update_array_can_perform_custom_mutation
array_mutation_returns_changed_key
```

### Public API Tests

```text
typed_handles_expose_registered_key
```

Typed handle constructors are crate-private. Integration tests should validate
the public API shape; impossible forged-handle states do not need normal runtime
tests. A later compile-fail test harness such as `trybuild` can lock this down
if it becomes important.

### Concurrency Tests

```text
clone_data_plane_shares_same_cells
parallel_writes_to_different_cells_do_not_deadlock
parallel_reads_can_observe_registered_cells
write_releases_lock_before_next_read
```

Keep concurrency tests simple. The goal is to catch obvious lock mistakes, not
to prove a formal memory model.

## 15. Validation Criteria

This slice is complete when:

```text
crates/ledger-runtime exists
it compiles without depending on ledger-domain or ledger
DataPlane supports Value<T> cells
DataPlane supports Array<T> cells
registration returns typed handles
mutations enforce owner checks
mutations return changed keys
all independent DataPlane tests pass
no scheduler/projection/replay code has been introduced
```

After this is validated, the next spec should cover:

```text
scheduler-owned dependency index
queue projection id when dependency key changes
projection execution environment
origin feed controller boundary
FrameBus changed-key consumption
first candles_1m projection using projection-owned state
```
