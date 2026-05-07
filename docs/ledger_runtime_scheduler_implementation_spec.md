# Ledger Runtime Scheduler MVP Spec

This spec covers the remaining generic runtime work for the
`ledger-runtime` crate after the DataPlane MVP.

It builds on:

```text
docs/ledger_runtime_data_plane_implementation_spec.md
```

The goal is to complete a small, verifiable runtime that can:

```text
accept external data-plane write batches
apply those writes through the DataPlane
track changed keys
queue projections whose dependencies changed
call each projection's run function
feed projection changed keys back into dependency scheduling
drain deterministically in tests
```

The runtime must stay generic. It must not know Ledger payload types like ES MBO
batches, replay status, candles, heatmap cells, order-book state, or Lens frames.

Scheduler correctness only applies to writes that pass through the runtime:

```text
Runtime::submit_external_writes
Projection::run through ProjectionContext
```

Direct `DataPlane` mutations are still possible because the DataPlane is a
standalone primitive, but they bypass dependency scheduling. The Ledger
application should treat direct DataPlane mutation as setup/test-only once a
runtime is active.

## 1. Conceptual Model

The DataPlane is shared typed state.

The Runtime is the coordinator around that state.

```text
external feed / caller
  submits external write batch

Runtime
  applies external writes to DataPlane
  sees changed keys
  queues dependent projections
  runs queued projections
  sees projection changed keys
  queues downstream projections

Projection
  owns its computation
  reads/writes through ProjectionContext
  returns success or failure
```

External writes are not domain objects in `ledger-runtime`.

They are simply queued DataPlane mutations submitted from outside the runtime.
In the Ledger application those writes will usually target keys like:

```text
origin.es_mbo.status
origin.es_mbo.batches
```

But `ledger-runtime` should not enforce an `origin.*` key prefix. The generic
write safety rule is still DataPlane ownership:

```text
the writer CellOwner must match the registered cell owner
```

Projection execution is also a black box to the runtime. The runtime only calls:

```rust
projection.run(&mut ctx)
```

The runtime does not inspect what the projection reads, computes, or writes.

## 2. Important Non-Goals

Do not add these in this slice:

```text
async runtime requirement
tokio dependency
background task/thread API
FrameBus
Lens websocket delivery
durable journal
runtime history persistence
origin-specific protocol
replay controls
projection business logic
projection cursor abstraction
schema validation
stream primitive
cross-cell transactions
automatic cycle detection
```

The MVP should be synchronous and deterministic. The Ledger application can run
it in a task or thread later.

## 3. Runtime Invariants

The implementation should preserve these invariants:

```text
external writes are applied in FIFO order
writes inside one external batch are applied in the order they were added
projection scheduling requests are deduped while queued
projection queue order is FIFO
changed keys are deduped while preserving first-seen order
every committed changed key is passed through dependency scheduling
external writes get priority before each projection run
projection computation is opaque to the runtime
```

External writes should not be deduped or coalesced in the MVP. They represent
actual requested mutations. Coalescing can be added later if profiling shows the
feed is too chatty.

Projection scheduling requests should be deduped because "this projection needs
to run" is a state, not a payload.

## 4. Crate Scope

Add the runtime scheduler files to the existing `ledger-runtime` crate:

```text
crates/ledger-runtime/src/runtime/mod.rs
crates/ledger-runtime/src/runtime/external_write.rs
crates/ledger-runtime/src/runtime/dependency.rs
crates/ledger-runtime/src/runtime/projection_registry.rs
crates/ledger-runtime/src/runtime/projection_queue.rs
crates/ledger-runtime/tests/external_write.rs
crates/ledger-runtime/tests/runtime_scheduler.rs
```

Small modules are preferred. Avoid putting all scheduler logic into one large
source file.

Update `src/lib.rs` to export the public runtime API:

```rust
pub use runtime::{ExternalWriteBatch, RunStats, Runtime, RuntimeError, RuntimeStep};
```

Internal helper types can stay crate-private unless tests or callers need them.

## 5. External Write Batch

An external write batch is a queued set of typed DataPlane mutations.

Conceptually:

```text
ExternalWriteBatch
  writer: CellOwner
  writes: Vec<typed write operation>
```

Example from the future Ledger application:

```rust
let mut batch = ExternalWriteBatch::new(CellOwner::Origin("origin.es_mbo".to_string()));
batch.push_array(&batches_key, batches);
batch.set_value(&status_key, status);
runtime.submit_external_writes(batch);
```

The runtime does not know the payload types of `batches` or `status`.

### Public API

```rust
pub struct ExternalWriteBatch {
    // private
}

impl ExternalWriteBatch {
    pub fn new(writer: CellOwner) -> Self;
    pub fn writer(&self) -> &CellOwner;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;

    pub fn set_value<T>(&mut self, key: &ValueKey<T>, value: T) -> &mut Self
    where
        T: Clone + Send + Sync + 'static;

    pub fn clear_value<T>(&mut self, key: &ValueKey<T>) -> &mut Self
    where
        T: Clone + Send + Sync + 'static;

    pub fn replace_array<T>(&mut self, key: &ArrayKey<T>, items: Vec<T>) -> &mut Self
    where
        T: Clone + Send + Sync + 'static;

    pub fn push_array<T>(&mut self, key: &ArrayKey<T>, items: Vec<T>) -> &mut Self
    where
        T: Clone + Send + Sync + 'static;

    pub fn insert_array<T>(
        &mut self,
        key: &ArrayKey<T>,
        index: usize,
        items: Vec<T>,
    ) -> &mut Self
    where
        T: Clone + Send + Sync + 'static;

    pub fn replace_array_range<T>(
        &mut self,
        key: &ArrayKey<T>,
        range: std::ops::Range<usize>,
        items: Vec<T>,
    ) -> &mut Self
    where
        T: Clone + Send + Sync + 'static;

    pub fn remove_array_range<T>(
        &mut self,
        key: &ArrayKey<T>,
        range: std::ops::Range<usize>,
    ) -> &mut Self
    where
        T: Clone + Send + Sync + 'static;

    pub fn clear_array<T>(&mut self, key: &ArrayKey<T>) -> &mut Self
    where
        T: Clone + Send + Sync + 'static;
}
```

Do not add update-closure operations to `ExternalWriteBatch` in the MVP. External
callers should compute the desired values before submitting the batch. If a
future caller needs queued custom mutations, add them later with tests.

### Internal Representation

The batch needs type erasure because different writes can have different payload
types.

Suggested internal shape:

```rust
pub struct ExternalWriteBatch {
    writer: CellOwner,
    operations: Vec<Box<dyn ExternalWriteOperation>>,
}

trait ExternalWriteOperation: Send {
    fn apply(
        self: Box<Self>,
        ctx: &mut ExternalWriteContext<'_>,
    ) -> Result<(), RuntimeError>;
}
```

Each builder method pushes a typed operation:

```rust
struct SetValue<T> {
    key: ValueKey<T>,
    value: T,
}

struct PushArray<T> {
    key: ArrayKey<T>,
    items: Vec<T>,
}
```

The typed operation calls the matching `ExternalWriteContext` method when the
runtime applies the batch.

## 6. External Write Context

External writes should not receive direct DataPlane access. They should write
through an `ExternalWriteContext` so changed keys are collected consistently.

`ExternalWriteContext` should be crate-private in the MVP. Callers build
`ExternalWriteBatch` values; only the runtime constructs the context while
applying a batch.

```rust
pub(crate) struct ExternalWriteContext<'a> {
    // private
}

impl<'a> ExternalWriteContext<'a> {
    pub(crate) fn writer(&self) -> &CellOwner;
    pub(crate) fn changed_keys(&self) -> &[Key];
    pub(crate) fn into_effects(self) -> WriteEffects;

    pub(crate) fn set_value<T>(&mut self, key: &ValueKey<T>, value: T) -> Result<(), RuntimeError>
    where
        T: Clone + Send + Sync + 'static;

    pub(crate) fn clear_value<T>(&mut self, key: &ValueKey<T>) -> Result<(), RuntimeError>
    where
        T: Clone + Send + Sync + 'static;

    pub(crate) fn replace_array<T>(
        &mut self,
        key: &ArrayKey<T>,
        items: Vec<T>,
    ) -> Result<(), RuntimeError>
    where
        T: Clone + Send + Sync + 'static;

    pub(crate) fn push_array<T>(
        &mut self,
        key: &ArrayKey<T>,
        items: Vec<T>,
    ) -> Result<(), RuntimeError>
    where
        T: Clone + Send + Sync + 'static;

    pub(crate) fn insert_array<T>(
        &mut self,
        key: &ArrayKey<T>,
        index: usize,
        items: Vec<T>,
    ) -> Result<(), RuntimeError>
    where
        T: Clone + Send + Sync + 'static;

    pub(crate) fn replace_array_range<T>(
        &mut self,
        key: &ArrayKey<T>,
        range: std::ops::Range<usize>,
        items: Vec<T>,
    ) -> Result<(), RuntimeError>
    where
        T: Clone + Send + Sync + 'static;

    pub(crate) fn remove_array_range<T>(
        &mut self,
        key: &ArrayKey<T>,
        range: std::ops::Range<usize>,
    ) -> Result<(), RuntimeError>
    where
        T: Clone + Send + Sync + 'static;

    pub(crate) fn clear_array<T>(&mut self, key: &ArrayKey<T>) -> Result<(), RuntimeError>
    where
        T: Clone + Send + Sync + 'static;
}
```

The context should:

```text
call DataPlane mutation methods using the batch writer CellOwner
record returned WriteEffects
dedupe changed keys while preserving first-seen order
```

It should not expose read methods in the MVP. External write batches are input
commits, not compute tasks.

## 7. Projection Registry

The runtime needs a registry from `ProjectionId` to projection instance.

```rust
struct ProjectionRegistry {
    projections: HashMap<ProjectionId, Box<dyn Projection>>,
}
```

Suggested API:

```rust
impl ProjectionRegistry {
    fn new() -> Self;

    fn register(
        &mut self,
        projection: Box<dyn Projection>,
    ) -> Result<(), RuntimeError>;

    fn contains(&self, id: &ProjectionId) -> bool;

    fn get_mut(
        &mut self,
        id: &ProjectionId,
    ) -> Result<&mut Box<dyn Projection>, RuntimeError>;
}
```

Registration rules:

```text
projection id must be unique
dependencies may be empty
duplicate dependency keys in one descriptor should be deduped before indexing
registration should preserve projection order for dependency fanout
the runtime clones id and dependencies at registration time
```

Projection descriptors are treated as immutable after registration. If a
projection mutates internal descriptor data later, the dependency index is not
updated and behavior is unsupported.

No removal API is required in the MVP.

## 8. Dependency Index

The dependency index maps changed keys to projections that should run.

```rust
struct DependencyIndex {
    dependents: HashMap<Key, Vec<ProjectionId>>,
}
```

Registration:

```text
for each dependency key in projection.descriptor().dependencies:
  dependency_index[dependency].push(projection_id)
```

Lookup:

```rust
fn dependents_for(&self, key: &Key) -> &[ProjectionId];
```

Ordering:

```text
dependency fanout order should match projection registration order
```

That makes tests deterministic and keeps scheduling behavior easy to reason
about.

## 9. Projection Queue

The projection queue is keyed and duplicate-resistant.

```rust
struct ProjectionQueue {
    order: VecDeque<ProjectionId>,
    queued: HashSet<ProjectionId>,
}
```

Behavior:

```text
enqueue_once(id):
  if id is already queued:
    return false
  push id to the back of order
  add id to queued
  return true

pop_next():
  pop id from front of order
  remove id from queued
  return id
```

Important detail:

```text
remove the projection id from queued before running it
```

This allows a projection to be queued again from changes produced by its own run
or by downstream cycles. The runtime does not detect cycles in the MVP, so drain
calls need a max-step limit.

## 10. Runtime Type

The main runtime owns the scheduler state and the DataPlane handle.

```rust
pub struct Runtime {
    data_plane: DataPlane,
    projections: ProjectionRegistry,
    dependencies: DependencyIndex,
    external_writes: VecDeque<ExternalWriteBatch>,
    projection_queue: ProjectionQueue,
}
```

Public API:

```rust
impl Runtime {
    pub fn new(data_plane: DataPlane) -> Self;

    pub fn data_plane(&self) -> &DataPlane;

    pub fn register_projection<P>(&mut self, projection: P) -> Result<(), RuntimeError>
    where
        P: Projection + 'static;

    pub fn submit_external_writes(&mut self, batch: ExternalWriteBatch);

    pub fn queue_projection(
        &mut self,
        id: &ProjectionId,
    ) -> Result<bool, RuntimeError>;

    pub fn is_idle(&self) -> bool;

    pub fn run_once(&mut self) -> Result<RuntimeStep, RuntimeError>;

    pub fn run_until_idle(
        &mut self,
        max_steps: usize,
    ) -> Result<RunStats, RuntimeError>;
}
```

`data_plane()` is for registration, reads, descriptor inspection, and tests. A
caller can technically mutate through the returned DataPlane because the
DataPlane is an independently usable primitive. Such writes bypass scheduling
and are outside runtime correctness guarantees.

`register_projection` should:

```text
read id and dependencies from the projection descriptor
reject duplicate projection id
store projection in registry
add dependency index entries
```

`submit_external_writes` should:

```text
push non-empty batches into external_writes
ignore empty batches
```

`queue_projection` should:

```text
error if the projection id is unknown
delegate to ProjectionQueue::enqueue_once
return whether the projection was newly queued
```

## 11. Step Semantics

`run_once` is one deterministic scheduler step.

It should:

```text
1. drain a snapshot of currently queued external write batches
2. apply each batch in FIFO order
3. pass all committed changed keys through dependency scheduling
4. run at most one queued projection
5. pass projection committed changed keys through dependency scheduling
6. return a RuntimeStep summary
```

External writes get priority because every `run_once` drains external writes
before a projection run.

Use a snapshot count for external write draining:

```text
let external_count = external_writes.len() at run_once start
process at most external_count batches
```

The synchronous MVP will not receive new external writes during a `run_once`
unless a future API allows callbacks. The snapshot rule should still be
specified because it prevents future background wrappers from accidentally
starving projections with an unbounded external drain.

If there are no external writes and no queued projections, `run_once` returns an
idle step.

If an external batch fails, `run_once` should:

```text
consume the failed batch
keep writes that committed before the failure
schedule dependents for those committed changed keys
leave later external batches queued
not run a projection in that step
return the error
```

If a projection fails, `run_once` should:

```text
keep writes that committed before the failure
schedule dependents for those committed changed keys
not run another projection in that step
return RuntimeError::Projection with the failing projection id
```

Because the projection id is removed from the queue before execution, it is not
automatically retried after failure. It can only be queued again if dependency
scheduling schedules it again.

### RuntimeStep

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStep {
    pub external_write_batches_applied: usize,
    pub projection_run: Option<ProjectionId>,
    pub changed_keys: Vec<Key>,
    pub scheduled_projections: Vec<ProjectionId>,
    pub idle_after: bool,
}
```

`changed_keys` should contain committed changed keys from both external writes
and the projection run, deduped in first-seen order.

`scheduled_projections` should contain projections newly queued during the step,
also in first-seen order. This is useful for tests and later observability.

`idle_after` means the runtime has no queued external writes and no queued
projections after this step completes. It can be `true` on a step that did real
work.

`RuntimeStep.changed_keys` is observability only. Scheduling must use phase-local
effects immediately after external writes and immediately after the projection
run. Do not wait until the end of the step and then schedule once from the
deduped summary.

## 12. External Write Execution

For each external write batch:

```text
create ExternalWriteContext(data_plane, batch.writer)
apply each operation in order
collect context changed keys
schedule dependents for committed changed keys
```

No cross-cell transaction exists. If operation 1 succeeds and operation 2 fails,
operation 1 remains committed.

The runtime should still schedule dependents for changes committed before the
failure is returned.

After a failed external batch, later batches that were already queued should
remain queued for a future `run_once`. The failing batch is consumed because its
operations may have partially committed and retrying the same batch would repeat
those writes.

Operations after the failing operation in the same batch are not applied.

Implementation pattern:

```rust
let mut ctx = ExternalWriteContext::new(&self.data_plane, writer);
let result = batch.apply(&mut ctx);
let effects = ctx.into_effects();
self.schedule_changed_keys(&effects.changed_keys);
result?;
```

The exact helper names can differ, but the behavior should remain.

## 13. Projection Execution

For one queued projection:

```text
pop ProjectionId from ProjectionQueue
look up projection in ProjectionRegistry
create ProjectionContext(data_plane, projection_id)
call projection.run(&mut ctx)
collect context changed keys
schedule dependents for committed changed keys
return projection result
```

The runtime must not inspect projection internals.

No projection write batch type is needed. `ProjectionContext` already records
DataPlane writes.

No rollback exists. If a projection writes one key and then returns an error,
that write remains committed. The runtime should still pass already-committed
changed keys through dependency scheduling before returning the error.

The failed projection is not automatically reinserted into the queue. If its
committed writes changed keys that map back to itself, normal dependency
scheduling may queue it again; otherwise it stays unqueued.

Implementation pattern:

```rust
let mut ctx = ProjectionContext::new(&self.data_plane, projection_id.clone());
let result = projection.run(&mut ctx);
let effects = ctx.into_effects();
self.schedule_changed_keys(&effects.changed_keys);
result?;
```

Projection authors should compute first and write near the end if they want to
avoid partial writes.

## 14. Dependency Scheduling

Scheduling changed keys:

```text
for changed_key in changed_keys:
  for projection_id in dependency_index.dependents_for(changed_key):
    projection_queue.enqueue_once(projection_id)
```

Rules:

```text
unknown changed keys are ignored
known dependents are queued in dependency index order
already queued projections are skipped
newly queued projection ids are recorded in RuntimeStep
scheduling happens immediately per phase, not from RuntimeStep.changed_keys
```

This is the entire dependency model for the MVP.

No cursors, layers, priorities, or graph traversal rules are needed. If a
projection needs a cursor, it owns that cursor in its own DataPlane state.

## 15. Run Semantics

`run_until_idle(max_steps)` repeatedly calls `run_once`.

```text
steps = 0
while !runtime.is_idle():
  if steps == max_steps:
    return RunLimitExceeded
  run_once()
  steps += 1
return RunStats
```

Use a max-step limit because cyclic dependencies can be valid but accidentally
infinite:

```text
projection.a depends on projection.b.output
projection.b depends on projection.a.output
```

The runtime should not try to solve cycles in the MVP.

### RunStats

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunStats {
    pub steps: usize,
    pub external_write_batches_applied: usize,
    pub projections_run: usize,
    pub changed_keys: Vec<Key>,
}
```

`changed_keys` should be deduped in first-seen order across all steps.

## 16. Error Model

Add a runtime-level error type.

```rust
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    #[error("duplicate projection `{0}`")]
    DuplicateProjection(ProjectionId),

    #[error("missing projection `{0}`")]
    MissingProjection(ProjectionId),

    #[error("data plane error: {0}")]
    DataPlane(#[from] DataPlaneError),

    #[error("projection `{id}` failed: {source}")]
    Projection {
        id: ProjectionId,
        source: ProjectionError,
    },

    #[error("run limit exceeded after {max_steps} steps")]
    RunLimitExceeded {
        max_steps: usize,
    },
}
```

`RuntimeError::DataPlane` should be used for external write failures.

`RuntimeError::Projection` should wrap projection failures and include the
projection id that failed.

If a projection id is queued but not registered, return `MissingProjection`.
That should only happen through a bug or a manual queue call with an unknown id.

## 17. Locking and Async

Keep this runtime synchronous for the MVP.

Do not add async methods to `Runtime`, `ExternalWriteBatch`, or projection
scheduling. The current DataPlane uses normal locks and synchronous methods.

The later Ledger application can choose how to run this:

```text
single-threaded test run
background std::thread loop
tokio spawn_blocking wrapper
explicit run_once calls from an app loop
```

That outer runner is not part of this spec.

## 18. Public API Example

Example shape for tests and future Ledger integration:

```rust
let data_plane = DataPlane::new();
let mut runtime = Runtime::new(data_plane.clone());

let origin_owner = CellOwner::Origin("origin.es_mbo".to_string());
let projection_id = ProjectionId::new("projection.candles_1m")?;

let batches_key = data_plane.register_array(
    CellDescriptor {
        key: Key::new("origin.es_mbo.batches")?,
        owner: origin_owner.clone(),
        kind: CellKind::Array,
        public_read: false,
    },
    Vec::<EsMboBatch>::new(),
)?;

let bars_key = data_plane.register_array(
    CellDescriptor {
        key: Key::new("projection.candles_1m.bars")?,
        owner: projection_id.owner(),
        kind: CellKind::Array,
        public_read: true,
    },
    Vec::<Bar>::new(),
)?;

runtime.register_projection(CandlesProjection::new(
    projection_id,
    vec![batches_key.key().clone()],
    batches_key.clone(),
    bars_key.clone(),
))?;

let mut writes = ExternalWriteBatch::new(origin_owner);
writes.push_array(&batches_key, incoming_batches);

runtime.submit_external_writes(writes);
runtime.run_until_idle(100)?;
```

The runtime does not know what `EsMboBatch`, `Bar`, or `CandlesProjection` do.

## 19. Test Plan

Add integration tests under `crates/ledger-runtime/tests`.

### External Write Tests

Verify:

```text
external batch set_value writes through DataPlane
external batch clear_value clears through DataPlane
external batch push_array preserves item order
external batch replace_array replaces existing items
external batch insert_array validates bounds through DataPlane
external batch replace_array_range validates bounds through DataPlane
external batch remove_array_range validates bounds through DataPlane
external batch clear_array clears through DataPlane
external batch records each changed key once
external batch preserves operation order
external batch fails on owner mismatch
external batch schedules committed changes even if a later operation fails
empty external batch is ignored by Runtime::submit_external_writes
failed external batch stops the run_once before projection execution
later external batches remain queued after a failed batch
```

### Projection Registry Tests

Verify:

```text
register_projection stores projection by id
register_projection rejects duplicate id
register_projection indexes dependency keys
duplicate dependencies in one descriptor are indexed once
empty dependencies are allowed
queue_projection rejects unknown projection id
```

### Projection Queue Tests

Verify:

```text
enqueue once queues projection once
duplicate enqueue returns false while queued
pop removes queued state
after pop the same projection can be queued again
FIFO order is preserved
```

These can be unit tests inside integration files if the queue remains public
only through Runtime behavior.

### Runtime Scheduler Tests

Verify:

```text
external write changed key queues dependent projection
projection run is called exactly once for duplicate dependency changes
projection writes queue downstream projection
downstream projections run in registration order
external writes are applied before projection runs
external write batches are FIFO
one run_once runs at most one projection
run_once returns idle when no work exists
RuntimeStep.idle_after is true after the last work item drains
run_until_idle drains external writes and projections
run_until_idle returns RunLimitExceeded for a dependency cycle
projection errors are wrapped with projection id
projection committed writes still schedule dependents before error returns
external write committed writes still schedule dependents before error returns
phase-local scheduling allows a popped projection to requeue itself
direct DataPlane writes do not schedule projections unless submitted through runtime
```

### Generic Payload Tests

Use small fake payloads in tests:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
struct SourceBatch {
    seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Bars {
    count: usize,
}
```

Do not introduce Ledger domain crates into `ledger-runtime` tests.

## 20. Implementation Order

Recommended order:

```text
1. add RuntimeError
2. add ExternalWriteBatch and ExternalWriteContext
3. add tests for external write application
4. add ProjectionQueue
5. add ProjectionRegistry and DependencyIndex
6. add Runtime with submit/register/queue_projection
7. add Runtime::run_once
8. add Runtime::run_until_idle
9. add scheduler integration tests
10. run cargo fmt and cargo test -p ledger-runtime
11. run cargo test --workspace
```

Keep each step independently testable.

## 21. Success Criteria

This slice is complete when:

```text
ledger-runtime has no dependency on Ledger domain crates
external write batches can mutate typed DataPlane cells
changed keys from external writes queue dependent projections
registered projections run through Projection::run
changed keys from projections queue downstream projections
projection queue dedupes while preserving FIFO order
runtime run behavior is deterministic
projection execution remains a black box
cargo test -p ledger-runtime passes
cargo test --workspace passes
```
