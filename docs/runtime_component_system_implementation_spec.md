# Runtime Component System Implementation Spec

## Purpose

This spec defines the next refactor of the `runtime` crate.

The goal is to replace the current projection-specific scheduler with a generic
runtime component system before any new `ledger` crate work begins.

The runtime should own execution:

```text
runtime
  owns component lifecycle
  owns process spawning
  owns task scheduling
  owns cache write application
  owns dependency scheduling
  owns shutdown/cancellation
  owns runtime diagnostics

cache
  owns active typed state cells

store
  owns durable objects and generated artifacts
```

Domain crates built later can compose concrete components and install them into
runtime. That future composition is out of scope for this spec.

The important boundary:

```text
Domain layer decides what components exist.
Runtime decides how components run.
Cache/store hold state.
```

Runtime should not learn what ES, DBN, MBO, replay, play, pause, speed, order
books, studies, Lens, sessions, or trading mean.

## Motivation

The current repo was created quickly and has no production compatibility
requirements. The existing `Projection` API is not a stable boundary. It should
be removed rather than adapted.

The runtime needs a generic execution model for two kinds of compute:

```text
Process
  long-running async component
  examples: replay feed, live adapter, websocket source, timer loop

Task
  finite scheduled component
  examples: order-book projection, study calculation, chart series builder,
            cache validator
```

Both are runtime components. They differ in execution shape:

```text
process:
  runtime starts it
  it loops until shutdown or failure
  it emits writes whenever source data or time says to

task:
  runtime schedules it from dependency changes or explicit queue requests
  it runs a finite step
  it emits writes while running
  it returns
```

The runtime should expose processes and tasks, not feeds and projections. Feed,
projection, study, and simulator remain domain-layer names for concrete
components.

## Scope

Implement the first runtime component system inside:

```text
crates/runtime
```

In scope:

```text
component ids and descriptors
runtime process trait
runtime task trait
shared component write context
cloneable external write sink
runtime-owned async worker
runtime-owned process spawning
runtime-owned task scheduling
dependency index from cache keys to tasks
task queue with dedupe
dynamic install of processes and tasks
async preparation that does not block the control loop
runtime shutdown of owned processes
projection API removal
```

Out of scope:

```text
creating a ledger crate
concrete ES replay feed
concrete DBN artifact preparation
concrete playback controller
API session registry
trade domain composition
Lens websocket frames
parallel task execution
task cancellation
runtime plugin ABI
loading new Rust code from dynamic libraries
WASM plugins
durable component persistence
cross-process components
```

Concrete runtime tests should use synthetic process/task components.

## Crate Dependencies

Update `crates/runtime/Cargo.toml`:

```toml
[dependencies]
anyhow.workspace = true
async-trait.workspace = true
cache = { path = "../cache" }
thiserror.workspace = true
tokio.workspace = true
```

`serde` and `serde_json` are not required for this phase. Component command and
query payload schemas can wait until a domain/API layer needs them.

## Module Layout

Recommended layout:

```text
crates/runtime/src/
  lib.rs
  component.rs
  error.rs
  handle.rs
  process.rs
  task.rs
  worker.rs
  runtime/
    dependency.rs
    external_write.rs
    mod.rs
    task_queue.rs
    task_registry.rs
```

Remove or rename the projection-specific files:

```text
projection.rs
runtime/projection_queue.rs
runtime/projection_registry.rs
```

There is no compatibility layer requirement.

## Naming

Use `component` as the generic runtime primitive.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ComponentId(cache::Key);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentKind {
    Process,
    Task,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentDescriptor {
    pub id: ComponentId,
    pub kind: ComponentKind,
}
```

Tasks that react to cache changes need dependency metadata:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDescriptor {
    pub component: ComponentDescriptor,
    pub dependencies: Vec<cache::Key>,
}
```

Processes do not declare cache-key dependencies to the scheduler. They wake
from source IO, timers, playback state, shutdown, or future component commands.

## Projection Replacement

Delete the old projection API:

```rust
pub trait Projection: Send {
    fn descriptor(&self) -> &ProjectionDescriptor;
    fn run(&mut self, ctx: &mut ProjectionContext<'_>) -> Result<(), ProjectionError>;
}
```

Replace names directly:

```text
ProjectionId          -> ComponentId
ProjectionDescriptor  -> TaskDescriptor
ProjectionContext     -> TaskContext
ProjectionError       -> ComponentError
Projection            -> RuntimeTask
ProjectionQueue       -> TaskQueue
ProjectionRegistry    -> TaskRegistry
```

The semantic target:

```text
domain projection/study -> RuntimeTask
domain feed/source      -> RuntimeProcess
```

Runtime owns the generic execution types. Domain crates own the vocabulary on
top.

## Component Traits

Runtime should support two execution traits.

### RuntimeProcess

Processes are long-running async components.

```rust
#[async_trait]
pub trait RuntimeProcess: Send + 'static {
    fn descriptor(&self) -> &ComponentDescriptor;

    async fn prepare(&mut self, ctx: ProcessPrepareContext) -> Result<(), ComponentError> {
        let _ = ctx;
        Ok(())
    }

    async fn run(self: Box<Self>, ctx: ProcessContext) -> Result<(), ComponentError>;
}
```

Rules:

```text
runtime owns process prepare and spawn
prepare must not block the runtime control loop
run loops until shutdown, completion, or failure
process writes go through ProcessContext::submit
```

Examples later:

```text
ES replay feed
live market adapter
journal action receiver
periodic source
```

### RuntimeTask

Tasks are finite scheduled components.

```rust
#[async_trait]
pub trait RuntimeTask: Send + 'static {
    fn descriptor(&self) -> &TaskDescriptor;

    async fn prepare(&mut self, ctx: TaskPrepareContext) -> Result<(), ComponentError> {
        let _ = ctx;
        Ok(())
    }

    async fn run_once(&mut self, ctx: TaskContext) -> Result<(), ComponentError>;
}
```

Rules:

```text
runtime owns task prepare and registration
prepare should be lightweight in the first implementation
run_once is finite
run_once should be quick
run_once may submit zero, one, or many write batches
each submit is a real commit point
tasks should submit internally consistent batches
tasks should store progress/cursors in cache or store
long-running work belongs in a RuntimeProcess
```

Examples later:

```text
order-book projection
candle/study calculation
chart frame builder
active cache validator
```

The first version should assume tasks are quick compute. Do not add task
cancellation or a separate task executor until a concrete task needs it. If a
component needs to do long-running work, it should be a process.

## State Model

State should remain explicit.

```text
cache
  active typed state
  process status
  task cursors
  latest values
  arrays of emitted source batches
  public state observed by API/Lens later

store
  durable raw objects
  durable artifacts
  optional persisted checkpoints later

component struct
  id
  typed keys
  configuration
  domain-provided handles
  transient runtime state while running
```

A domain projection implemented as a runtime task does not need hidden state
inside the Rust object. Its durable and inspectable state should live in
cache/store.

Example future cell names:

```text
feed.databento.es_replay.batches
projection.book.cursor
projection.book.snapshot
projection.book.stats
```

The runtime should not care what those keys mean.

## Shared Write Path

All active component writes should enter runtime through the same write path.

```text
process component
  let mut batch = ctx.batch()
  fill batch
  ctx.submit(batch).await

task component
  let mut batch = ctx.batch()
  fill batch
  ctx.submit(batch).await
```

`submit()` is the commit point:

```text
runtime receives ExternalWriteBatch
runtime applies writes to cache
runtime records changed keys
runtime schedules dependent tasks
```

There is no special "task returns all writes at the end" API. A task can submit
multiple times during `run_once` when that makes the implementation clearer.

The only discipline required from component authors:

```text
each submitted batch should be internally consistent
```

For example, a task should write cursor and snapshot together if downstream
tasks expect them to move together.

Direct `Cache` mutation remains useful for setup and tests, but active
components should not mutate cache directly because that bypasses scheduling.

## External Write Sink

The write sink is the generic async ingress into the runtime.

```rust
#[derive(Clone)]
pub struct ExternalWriteSink {
    tx: tokio::sync::mpsc::Sender<ExternalWriteBatch>,
}

impl ExternalWriteSink {
    pub async fn submit(&self, batch: ExternalWriteBatch) -> Result<(), RuntimeError>;
}
```

The sink is not feed-specific. Runtime should treat every submitted batch as an
external component write. Processes and external callers use this sink.
Scheduler-inline tasks use the same write semantics through `ComponentWriteContext`
without round-tripping through the mpsc channel.

## Shared Write Context

Processes and tasks should share the same write helper.

```rust
#[derive(Clone)]
pub struct ComponentWriteContext {
    component_id: ComponentId,
    submitter: ComponentWriteSubmitter,
}

impl ComponentWriteContext {
    pub fn component_id(&self) -> &ComponentId;
    pub fn owner(&self) -> cache::CellOwner;
    pub fn batch(&self) -> ExternalWriteBatch;

    pub async fn submit(
        &self,
        batch: ExternalWriteBatch,
    ) -> Result<(), ComponentError>;
}
```

`batch()` creates:

```rust
ExternalWriteBatch::new(self.owner())
```

That keeps owner mechanics out of component implementations.

`ComponentWriteSubmitter` is a runtime implementation detail. For processes it
can be backed by `ExternalWriteSink`. For tasks running inline on the scheduler,
it should apply the batch through the scheduler's local write path so
`submit()` is still the immediate commit point and does not deadlock by sending
back into the same control loop.

## Contexts

### Prepare Contexts

Preparation may need to register cells, validate current cache state, hydrate
metadata, or run expensive setup supplied by a concrete component.

Preparation must not run on the runtime control loop.

```rust
pub struct ProcessPrepareContext {
    cache: cache::Cache,
    writes: ComponentWriteContext,
    shutdown: ShutdownReceiver,
}

pub struct TaskPrepareContext {
    cache: cache::Cache,
    writes: ComponentWriteContext,
}
```

If a concrete component needs store access, object storage, playback state, or
domain configuration, the component struct should carry those handles. Runtime
does not know what they are.

### Process Context

```rust
pub struct ProcessContext {
    cache: cache::Cache,
    writes: ComponentWriteContext,
    shutdown: ShutdownReceiver,
}
```

Process context helpers:

```rust
impl ProcessContext {
    pub fn component_id(&self) -> &ComponentId;
    pub fn owner(&self) -> cache::CellOwner;
    pub fn cache(&self) -> &cache::Cache;
    pub fn shutdown(&self) -> &ShutdownReceiver;
    pub fn batch(&self) -> ExternalWriteBatch;
    pub async fn submit(&self, batch: ExternalWriteBatch) -> Result<(), ComponentError>;
}
```

### Task Context

```rust
pub struct TaskContext {
    cache: cache::Cache,
    writes: ComponentWriteContext,
    wake: TaskWake,
}
```

Task context helpers:

```rust
impl TaskContext {
    pub fn component_id(&self) -> &ComponentId;
    pub fn owner(&self) -> cache::CellOwner;
    pub fn cache(&self) -> &cache::Cache;
    pub fn wake(&self) -> &TaskWake;
    pub fn batch(&self) -> ExternalWriteBatch;
    pub async fn submit(&self, batch: ExternalWriteBatch) -> Result<(), ComponentError>;
}
```

`TaskWake` should include why the task ran:

```rust
pub enum TaskWake {
    DependencyChanged,
    Manual,
    Rerun,
}
```

Exact changed-key details can be added later if a concrete task needs them.

## Runtime Worker

Add an async worker that owns the runtime control loop.

```rust
pub struct RuntimeWorker {
    runtime: Runtime,
    commands: RuntimeCommandReceiver,
    writes: ExternalWriteReceiver,
    processes: ProcessRegistry,
    tasks: TaskRegistry,
    task_queue: TaskQueue,
    shutdown: ShutdownController,
}
```

Public constructor shape:

```rust
impl RuntimeWorker {
    pub fn new(cache: cache::Cache) -> (Self, RuntimeHandle);

    pub async fn run(self) -> Result<RuntimeExit, RuntimeError>;
}
```

Handle shape:

```rust
#[derive(Clone)]
pub struct RuntimeHandle {
    commands: RuntimeCommandSender,
    writes: ExternalWriteSink,
    cache: cache::Cache,
}
```

Handle responsibilities:

```rust
impl RuntimeHandle {
    pub fn cache(&self) -> &cache::Cache;
    pub fn external_write_sink(&self) -> ExternalWriteSink;

    pub async fn install_process<P>(&self, process: P) -> Result<ComponentHandle, RuntimeError>
    where
        P: RuntimeProcess;

    pub async fn install_task<T>(&self, task: T) -> Result<ComponentHandle, RuntimeError>
    where
        T: RuntimeTask;

    pub async fn queue_task(&self, id: &ComponentId) -> Result<bool, RuntimeError>;
    pub async fn stop_process(&self, id: &ComponentId) -> Result<(), RuntimeError>;
    pub async fn shutdown(self) -> Result<RuntimeExit, RuntimeError>;
}
```

The handle is the control surface for callers. Callers should not own or drive
the worker loop directly.

## Runtime Commands

Runtime commands are control-plane messages. External writes are data-plane
messages and should keep using `ExternalWriteSink`.

Initial command set:

```rust
pub enum RuntimeCommand {
    InstallProcess {
        process: Box<dyn RuntimeProcess>,
        reply: oneshot::Sender<Result<ComponentHandle, RuntimeError>>,
    },

    InstallTask {
        task: Box<dyn RuntimeTask>,
        reply: oneshot::Sender<Result<ComponentHandle, RuntimeError>>,
    },

    QueueTask {
        id: ComponentId,
        reply: oneshot::Sender<Result<bool, RuntimeError>>,
    },

    StopProcess {
        id: ComponentId,
        reply: oneshot::Sender<Result<(), RuntimeError>>,
    },

    ComponentStatus {
        id: ComponentId,
        reply: oneshot::Sender<Result<ComponentStatus, RuntimeError>>,
    },

    ListComponents {
        reply: oneshot::Sender<RuntimeSnapshot>,
    },

    Drain {
        max_steps: usize,
        reply: oneshot::Sender<Result<RunStats, RuntimeError>>,
    },

    Shutdown {
        reply: oneshot::Sender<Result<RuntimeExit, RuntimeError>>,
    },
}
```

No domain commands belong here in this phase.

## Control Loop And Process Executor

The runtime needs to stay responsive while processes prepare and run.

Do not run expensive process preparation on the control loop. Task preparation
and task execution should be lightweight in the first implementation. If a task
becomes expensive, redesign it as a process or add a dedicated task executor in
a later spec.

Recommended shape:

```text
runtime control loop
  receives commands
  receives ExternalWriteBatch values
  applies cache writes serially
  schedules dependent tasks
  runs quick finite tasks
  starts/stops process components
  records diagnostics
  remains responsive

prepare executor
  runs process prepare futures
  reports ready/failed back to the control loop

process executor
  owns long-running process run futures
  processes submit writes through ExternalWriteSink
```

The control loop is still the only place that applies cache writes and releases
dependency scheduling. This first cut intentionally does not add a task
executor. Scheduled tasks should be fast enough to run inline with a step
budget. Add a task executor only after there is a concrete slow task and clear
ordering requirements.

## Write Application And Scheduling

When runtime accepts an `ExternalWriteBatch` through either the external sink or
a scheduler-local task submit:

```text
1. apply the batch to cache
2. collect changed keys
3. find dependent tasks
4. enqueue each dependent task once
5. run queued tasks within the scheduler step budget
```

This is true whether the batch came from:

```text
process context submit
task context submit
test/manual external write sink
```

`submit()` commits immediately. If a task submits three coherent batches during
one `run_once`, each batch is applied and may schedule dependents. The task
queue dedupes repeated schedules.

## Task Queue Semantics

Tasks need only simple scheduling state in the first implementation.

Recommended states:

```text
Ready
Queued
Running
Failed
```

Rules:

```text
dependency change while Ready:
  enqueue task

dependency change while Queued:
  keep one queue entry

dependency change while Running:
  enqueue one rerun after the current run returns

task finishes with no queued rerun:
  move to Ready

task fails:
  move to Failed and record error
```

This prevents duplicate concurrent runs of the same task. Tasks are expected to
be quick; advanced cancellation, pausing, and worker-pool scheduling should be
added only when needed.

## Dynamic Installation

Runtime should support installing processes and tasks after startup.

Process installation should accept ownership quickly and perform preparation
asynchronously. Task installation should stay simple and lightweight until a
real task requires heavier setup.

Process install:

```text
1. receive boxed RuntimeProcess through RuntimeHandle
2. reject duplicate component id
3. register component as Preparing
4. spawn process.prepare(...) outside the control loop
5. reply with ComponentHandle once ownership is accepted
6. when prepare succeeds, spawn process.run(...)
7. when prepare fails, mark component Failed
```

Task install:

```text
1. receive boxed RuntimeTask through RuntimeHandle
2. reject duplicate component id
3. call task.prepare(...) on the control path
4. register dependencies
5. mark Ready
6. reply with ComponentHandle
```

If task preparation becomes expensive, move it to async preparation later. Do
not design that complexity before there is a task that needs it.

The handle can later expose:

```rust
pub async fn wait_ready(&self) -> Result<(), RuntimeError>;
```

That is useful for process readiness in CLI validation, but not required in the
first cut.

## Component Handles

Runtime should return a handle for installed components.

```rust
#[derive(Clone)]
pub struct ComponentHandle {
    id: ComponentId,
    kind: ComponentKind,
    runtime: RuntimeHandle,
}
```

Initial methods:

```rust
impl ComponentHandle {
    pub fn id(&self) -> &ComponentId;
    pub fn kind(&self) -> ComponentKind;
    pub async fn status(&self) -> Result<ComponentStatus, RuntimeError>;
}
```

Do not add task stop/shutdown in this phase. Processes can be stopped because
they are long-running. Tasks are finite scheduled compute and should return
quickly. Do not add a large command/query API in this phase. Future component
command and query support should be message-based, not direct method calls on
running component objects.

## Error Types

Runtime errors should preserve component context.

```rust
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("duplicate component `{0}`")]
    DuplicateComponent(ComponentId),

    #[error("missing component `{0}`")]
    MissingComponent(ComponentId),

    #[error("component `{id}` failed: {source}")]
    Component {
        id: ComponentId,
        source: ComponentError,
    },

    #[error("runtime command channel closed")]
    CommandChannelClosed,

    #[error("external write ingress closed")]
    ExternalWriteIngressClosed,

    #[error("run limit exceeded after {max_steps} steps")]
    RunLimitExceeded { max_steps: usize },

    #[error(transparent)]
    Cache(#[from] cache::CacheError),
}
```

Component errors can use `anyhow` internally:

```rust
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ComponentError(#[from] anyhow::Error);
```

Concrete domain errors can be converted into `ComponentError` later at the
runtime boundary.

## Runtime Invariants

The runtime should guarantee:

```text
component ids are unique
active processes are spawned by runtime
finite tasks are scheduled by runtime
process and task writes use the same write application path
external writes are applied FIFO
task dependencies are scheduled from changed cache keys
queued tasks are deduped
running tasks are not run concurrently with themselves
dependency changes during a running task queue one rerun
task writes can schedule downstream tasks
shutdown reaches all runtime-owned processes
runtime has no domain-specific market/replay knowledge
```

The runtime should not guarantee yet:

```text
parallel task execution
task cancellation
exactly-once durable component execution
durable component state
domain command schema validation
plugin ABI stability
cross-process supervision
```

## Tests

Unit tests:

```text
ComponentId validates through cache::Key
installing duplicate process id is rejected
installing duplicate task id is rejected
ComponentWriteContext::batch uses component owner
ExternalWriteSink submits batches into worker
worker applies process-submitted writes
worker applies task-submitted writes
changed keys schedule dependent tasks
tasks are deduped while queued
running task that receives another dependency change is requeued once
task writes schedule downstream tasks
worker shutdown stops spawned processes
failing process returns component id in diagnostics
failing task returns component id in diagnostics
run budget prevents infinite self-scheduling drain
```

Integration tests:

```text
long-running synthetic process emits several ExternalWriteBatch values
runtime worker applies them and runs dependent task
task cursor stored in cache advances across repeated process writes
task can submit multiple coherent batches from one quick run_once
shutdown exits with no leaked process handles
```

Workspace validation:

```text
cargo fmt --all
cargo test --workspace
```

## Implementation Phases

### Phase 1: Rename Projection Scheduler To Tasks

```text
delete Projection / ProjectionDescriptor / ProjectionContext / ProjectionError
add ComponentId / ComponentDescriptor / ComponentKind
add RuntimeTask / TaskDescriptor / TaskContext
rename projection queue and registry to task queue and registry
rewrite scheduler tests around RuntimeTask
```

### Phase 2: Shared Write Sink And Context

```text
add ExternalWriteSink
add ComponentWriteContext
make TaskContext use batch()/submit()
route task writes through the same ExternalWriteBatch application path
```

### Phase 3: Runtime Worker And Control Plane

```text
add RuntimeWorker
add RuntimeHandle
add RuntimeCommand
worker owns cache write application and task scheduling
worker receives external writes over mpsc
worker remains responsive while work is pending
```

### Phase 4: Processes

```text
add RuntimeProcess
add ProcessPrepareContext
add ProcessContext
runtime installs and spawns process components
process components submit writes through ComponentWriteContext
shutdown stops runtime-owned processes
```

### Phase 5: Scheduler Polish

```text
install process as Preparing
run process prepare outside the control loop
register task dependencies during task install
run quick finite tasks inline with the scheduler
implement rerun-on-change-while-running behavior
keep task cancellation out of scope
```

## Success Criteria

This runtime phase is complete when:

```text
old projection API is gone
runtime exposes Process and Task component types
runtime owns process spawning
runtime owns finite task scheduling
runtime exposes a cloneable RuntimeHandle
runtime exposes a cloneable ExternalWriteSink
processes and tasks share batch()/submit() write semantics
submit commits writes immediately through runtime
task writes can schedule downstream tasks
runtime can dynamically install process components
runtime can dynamically install task components
shutdown stops runtime-owned processes
deterministic task scheduler behavior is tested
runtime still has no ES/replay/domain knowledge
cargo test --workspace passes
```

## Future Extensions

These should be left for later specs:

```text
component command/query envelopes
typed domain wrappers around component commands
parallel task executor
task priority and fairness classes
component restart policies
durable component checkpoints
component metrics stream
API session registry
WASM or process-isolated plugins
native dynamic library plugins
```