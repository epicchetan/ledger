# Atomic Runtime Snapshots and Projection Delivery — Implementation Spec

Status: implemented and host-validated; final on-device feel-check pending

Date: 2026-07-10

Baseline: `a8e030a` (`Add replay chart and reload-safe session resume`)

## 1. Purpose

Build the missing boundary between Ledger's exact projection state and the
visual frames consumed by Lens.

The implementation has two inseparable foundations:

1. **Atomic runtime snapshots.** The runtime worker remains the single owner of
   cache mutation and serves typed, multi-cell read transactions between
   runtime steps. A projection delivery reader can therefore read status,
   arrays, live values, and cursors as one committed state without retrying
   around torn multi-key reads.
2. **Projection-owned delivery.** Each installed projection supplies both its
   deterministic `RuntimeTask` and a read-only delivery source describing how
   to extract frames from its cells, what delivery semantics its payload has,
   its useful wall-clock cadence, and how it behaves during seek. A separate
   delivery executor samples dirty projections without pacing or blocking the
   projection runtime.

The viewer participates through a validated subscription protocol. It reports
schema support, requested cadence, activity, and the last **server-issued
position it actually applied**. Ledger uses that only to optimize suffixes and
resumption. Ledger's cache remains authoritative; any uncertainty falls back to
an authoritative snapshot.

The target separation is:

```text
projection state cadence   exact server computation; as fast/often as required
projection frame cadence   server wall-clock delivery policy; coalesced
Lens render cadence         browser/device paint cadence; last-mile coalesced
```

An explicit seek is not visual playback. The runtime catches up at full speed,
delivery suppresses intermediate projection frames, and the viewer receives one
authoritative projection update when the requested projections converge.

## 2. Why This Is the Next Boundary

The current pipeline is correct enough to expose the missing layer:

```text
EsReplayFeed RuntimeProcess
  -> feed cells
  -> BarsTask
  -> bars/live/status cells
  -> one Remux watcher per bars status cell
  -> barsFrame for every watcher wake the transport can service
  -> BarsAccumulator publishes every accepted frame
  -> BarsLayer applies every publish to lightweight-charts
```

This has four consequences:

- watcher coalescing is opportunistic, not an FPS policy;
- a seek's projection convergence is painted as a fast replay;
- Remux reads several cache cells independently and can observe a mixed
  multi-cell snapshot;
- the server serializes frames without knowing what an individual viewer has
  successfully applied.

The cache already uses per-cell locks and generation watches. The runtime
already serializes writes and schedules dependents from changed keys. The
missing feature is not a second cache or a parallel writer: it is an
owner-executed snapshot path plus a concurrent read-only publisher.

This spec supersedes these earlier temporary decisions:

- `docs/ledger_projection_system_implementation_spec.md`: frame policies and
  UI coalescing were explicitly deferred.
- `docs/ledger_session_transport_implementation_spec.md`: direct status-cell
  watchers were the initial transport and rate limiting was out of scope.
- `docs/lens_replay_chart_implementation_spec.md`: the statement that catch-up
  should render as a fast fill is superseded. Projection data may converge
  incrementally, but explicit seek presentation snaps to the final converged
  state.

The long-range design remains consistent with `docs/vision.md`: exact
computation, trader visibility, and UI frame coalescing are separate concerns.

## 3. Host Performance Assumptions

The current host is:

```text
CPU       AMD Ryzen 7 9700X
cores     8 physical / 16 hardware threads
max clock approximately 5.6 GHz
L3        32 MiB, one NUMA node
memory    62 GiB
storage   mirrored NVMe
```

This favors a simple execution layout:

```text
runtime/cache owner       one serial commit domain; strong single-core path
projection delivery       one independent worker per active session
stdio/network             asynchronous I/O, outside the runtime commit path
heavy future compute      immutable input jobs on a bounded worker pool
```

Ordinary projection tasks do not become concurrent cache writers. The machine
has ample cores for delivery, serialization, artifact loading, Remux, and
future pure compute without weakening deterministic state mutation.

No CPU pinning, NUMA policy, or general parallel DAG executor is part of this
implementation.

## 4. Terminology

```text
runtime owner
  The RuntimeWorker's serial commit domain. It applies external write batches,
  runs one task step at a time, schedules changed-key dependents, and executes
  atomic snapshot requests.

committed snapshot
  A typed read of one or more cells that cannot interleave with a runtime write
  batch or RuntimeTask::run_once. It may still represent an intentionally
  lagging projection graph state.

converged snapshot
  A committed snapshot where the selected projections have processed the
  required feed cursor/generation.

projection delivery source
  Projection-specific, read-only code that owns delivery cursor state and knows
  how to extract an owned frame from that projection's cache cells.

available head
  The latest authoritative position currently materialized in projection
  cells.

sent head
  The latest position placed into one viewer subscription's ordered delivery
  stream.

acknowledged head
  The latest server-issued position a viewer says it parsed, validated, and
  committed to its accumulator.

consumer instance
  One live JavaScript context. A native WebView reload creates a new consumer
  instance even when it restores the same Ledger session and Remux tab.

session generation
  Server-issued incarnation preventing frames or resume positions from an old
  session/process/seek generation from aliasing current state.
```

## 5. Load-Bearing Invariants

1. **One mutation owner.** Active runtime cells are mutated only by the runtime
   owner through local task commits or the external write ingress.
2. **Cache and scheduler stay together.** The cache does not become a separate
   actor from the scheduler; changed-key effects and task scheduling remain in
   the same commit domain.
3. **Atomic is not converged.** A snapshot can atomically observe a feed cursor
   ahead of a projection status. That is projection lag, not a torn read.
4. **Submitted batches are coherent.** Components that require several cells
   to move atomically write them in one batch. Separate external batches are
   separate commits.
5. **Snapshot readers are short.** Owner-executed readers clone only the small
   owned/`Arc` data required for delivery. They never serialize, await, perform
   network I/O, or run expensive projection computation.
6. **Delivery never paces compute.** A slow viewer, full output queue, or low
   requested FPS may coalesce or stop frames but cannot slow the feed or
   projection task graph except for the bounded time required to capture a
   snapshot.
7. **Server truth is authoritative.** Viewer positions are validated
   optimization hints. They never change projection cells, feed state, clock
   state, or convergence decisions.
8. **Every delta names its base.** A viewer applies a delta only when its local
   applied position equals the frame's required base.
9. **Every projection can snapshot.** Unknown, stale, invalid, or unretained
   positions recover through an authoritative projection snapshot.
10. **Delivery semantics are projection-defined.** Newest-wins is used only
    where valid. Ordered streams preserve and deduplicate every logical item.
11. **Reload does not inherit memory.** A new JS consumer reports `have: null`
    unless it actually restored validated projection state from durable client
    storage (not implemented here).
12. **Seek publishes the target, not the work.** Explicit seek catch-up may be
    observed through progress metadata, but intermediate projection data is not
    painted as playback.

## 6. Target Architecture

```text
                         one serial commit domain
                  +--------------------------------+
clock/feed writes ->| RuntimeWorker                  |
                    |  Cache owner                   |
                    |  RuntimeTask scheduling        |
                    |  atomic snapshot requests      |
                    +---------------+----------------+
                                    |
                           owned/Arc snapshots
                                    |
                    +---------------v----------------+
                    | ProjectionDeliveryExecutor     |
                    |  dirty set                     |
                    |  per-source wall deadlines     |
                    |  seek barrier                  |
                    |  per-consumer positions        |
                    |  semantic coalescing           |
                    +---------------+----------------+
                                    |
                            typed delivery frames
                                    |
                    +---------------v----------------+
                    | Ledger Remux transport         |
                    |  subscription RPCs             |
                    |  targeted notifications        |
                    |  JSON serialization / stdio    |
                    +---------------+----------------+
                                    |
                    +---------------v----------------+
                    | Lens projection client         |
                    |  schema parser                 |
                    |  accumulator                   |
                    |  applied-head acknowledgments  |
                    |  chart layer / renderer        |
                    +--------------------------------+
```

The delivery executor owns no market or projection truth. If it dies, a new
executor can reconstruct delivery from cache plus active subscription state or
force snapshots.

## 7. Part A — Cache Capabilities and Atomic Runtime Snapshots

### 7.1 Do Not Split Cache and Runtime Threads

A cache-only actor would force every task read/write across a channel and then
send changed-key effects back to the scheduler. The runtime would pay channel
and ordering costs in the hottest path and recreate a distributed transaction
between two threads.

Instead, `RuntimeWorker` is the logical cache owner. It may run as its current
Tokio task; correctness does not require OS-thread affinity. A dedicated
runtime thread can be introduced later without changing snapshot callers.

### 7.2 Separate Owner and Reader Capabilities

Today `Cache` is cloneable and exposes both reads and owner-checked mutations.
`LedgerSessionHandle::cache()` consequently makes active-session ownership a
convention.

Introduce a read-only capability:

```rust
#[derive(Clone)]
pub struct CacheReader {
    inner: Arc<CacheInner>,
}

pub struct Cache {
    inner: Arc<CacheInner>,
}

impl Cache {
    pub fn reader(&self) -> CacheReader;

    // registration and owner-checked mutations remain on Cache
}

impl CacheReader {
    pub fn describe(&self, key: &Key) -> Result<CellDescriptor, CacheError>;
    pub fn watch_key(&self, key: &Key) -> Result<CellWatch, CacheError>;
    pub fn read_value<T>(&self, key: &ValueKey<T>) -> Result<Option<T>, CacheError>;
    pub fn read_array<T>(&self, key: &ArrayKey<T>) -> Result<Vec<T>, CacheError>;
    pub fn read_array_range<T>(...) -> Result<Vec<T>, CacheError>;
}
```

Rules:

- `RuntimeWorker::new(cache)` consumes the mutable/registration capability.
- `RuntimeHandle`, `ProcessContext`, and public session handles expose only a
  `CacheReader` or narrower watch/read methods.
- tasks write through `TaskContext`; processes write through
  `ExternalWriteSink`.
- setup code and cache unit tests may retain a `Cache` before runtime start.
- active production code must not retain a mutable `Cache` clone after handing
  it to `RuntimeWorker`.

Per-cell locks remain in this phase. Single-cell read/watch users such as the
feed clock observer continue to use `CacheReader`. Removing locks is neither
required for atomic snapshots nor safe while read-only observers run on other
threads.

### 7.3 Snapshot API

Add an owner-executed typed snapshot API on `RuntimeHandle`:

```rust
pub async fn snapshot<R, F>(&self, read: F) -> Result<R, RuntimeError>
where
    R: Send + 'static,
    F: FnOnce(&CacheReadView<'_>) -> Result<R, CacheError> + Send + 'static;
```

`CacheReadView` exposes the same typed read operations as `CacheReader` but no
watch registration and no mutation.

The generic closure is type-erased inside the worker command:

```rust
struct SnapshotRequest {
    read: BoxedSnapshotRead,
    reply: oneshot::Sender<Result<Box<dyn Any + Send>, RuntimeError>>,
}
```

The public generic method performs the type-safe downcast internally. A caller
never handles `Any`.

Alternative implementation shapes are acceptable if they preserve:

- typed inputs and output at the public API;
- an immutable read view;
- execution on the runtime owner;
- no interleaving with writes or task steps;
- cancellation and shutdown behavior.

### 7.4 Snapshot Commit Boundary

A snapshot is served only:

```text
after the current ExternalWriteBatch has fully applied
after the current RuntimeTask::run_once has returned
before the next write batch or task step begins
```

Even if a task calls `ctx.submit()` several times during one `run_once`, an
external snapshot cannot run halfway through that task step.

A process submitting several external batches creates several commits. A
snapshot may validly observe between them. Components must group cells that
must be atomic into one external batch.

### 7.5 Worker Integration and Fairness

Add a bounded snapshot request channel distinct from external writes and
lifecycle commands. The worker services it while idle and between bounded task
steps.

Conceptually:

```rust
async fn step_until_idle(&mut self) -> Result<(), RuntimeError> {
    while !self.runtime.is_idle() {
        self.pump_external_writes();
        self.serve_snapshot_requests(SNAPSHOT_BUDGET_PER_BOUNDARY);
        self.runtime.run_once().await?;
        tokio::task::yield_now().await;
    }
    self.serve_snapshot_requests(SNAPSHOT_BUDGET_WHILE_IDLE);
    Ok(())
}
```

Requirements:

- reads cannot starve writes or tasks;
- a `WakeAgain` rebuild chain still serves reads between chunks;
- request cancellation drops the reply and may skip work when not yet started;
- shutdown rejects outstanding/new requests deterministically;
- a snapshot panic is contained and reported as a runtime error rather than
  killing the worker;
- snapshot queue wait and execution duration are measured.

The exact budgets are implementation-tuned constants, not wire contracts.

### 7.6 Committed Versus Converged

The first API provides committed snapshots. Do not block the runtime owner
waiting for projection convergence inside a read closure.

If a caller needs convergence, it observes versions/watches asynchronously and
then requests a committed snapshot. The delivery executor's seek barrier owns
this orchestration.

Possible future convenience vocabulary:

```rust
pub enum SnapshotConsistency {
    Committed,
    AtLeast(ProjectionPosition),
    Converged(Vec<ProjectionKey>),
}
```

Only `Committed` is required now.

### 7.7 Snapshot Performance Discipline

Owner-executed code may:

- read scalar/value cells;
- clone `Arc<T>` snapshots;
- clone a small append-only suffix;
- assemble a small typed struct.

It may not:

- serialize JSON;
- block or await;
- perform network/filesystem I/O;
- fold raw market batches;
- build a full visualization from scratch;
- clone a complete large history when a suffix or `Arc` is possible.

Instrument slow snapshot execution with projection/spec identity. Start with
measurement rather than a hard microsecond failure threshold.

### 7.8 Large-State Direction

For replace-latest state such as depth, projections should store immutable
snapshots as `Arc<T>` so owner snapshots clone only an `Arc`.

For large append-only state, a future cache representation may use immutable
chunks:

```text
Vec<Arc<[T]>>
```

That permits zero-copy range snapshots across threads. It is not required for
the initial one-minute bars delivery and must be justified by profiling.

## 8. Part B — Projection-Owned Delivery Definitions

### 8.1 Installation Contract

Projection installation should return computation and delivery together:

```rust
pub struct InstalledProjection {
    pub spec: ProjectionSpec,
    pub task: Box<dyn RuntimeTask>,
    pub delivery: Box<dyn ProjectionDeliverySource>,
}
```

The current `LedgerSessionBuilder::bars()` can remain a convenience method, but
internally it creates:

```text
BarsTask             owns computation and cell writes
BarsDeliverySource   owns publication reads and delivery cursor state
```

They share typed cell handles, never mutable projection state.

The builder retains all delivery sources and passes them to the session-owned
delivery executor when starting the session.

### 8.2 Delivery Descriptor

Each delivery source declares:

```rust
pub struct ProjectionDeliveryDescriptor {
    pub spec: String,
    pub kind: &'static str,
    pub schema_version: u16,
    pub semantics: ProjectionDeliverySemantics,
    pub default_max_fps: NonZeroU16,
    pub max_useful_fps: NonZeroU16,
    pub seek_policy: SeekDeliveryPolicy,
}

pub enum ProjectionDeliverySemantics {
    ReplaceLatest,
    AppendOrdered,
    PatchByKey,
    Snapshot,
}

pub enum SeekDeliveryPolicy {
    EmitIntermediate,
    FinalSnapshotOnly,
}
```

Bars may use a more precise documented semantic such as append-ordered
completed bars plus replace-latest live bar, while still mapping to one bars
frame envelope.

Projection declarations define the useful/default cadence. A viewer may ask
for a lower cadence but cannot request a cadence above the projection's maximum
or weaken ordered delivery semantics.

```text
effective FPS = min(projection max useful FPS, viewer requested FPS)
```

Initial candidate defaults (tune by measurement, not product contract):

```text
bars live state     10 FPS default, 20 FPS maximum
depth snapshot      20 FPS default, 30 FPS maximum
trade envelopes     10 FPS envelope cadence; all ordered items retained
model/score         emit accepted results, capped near 5 FPS
```

### 8.3 Delivery Source Trait

The delivery executor owns each mutable source on its own thread:

```rust
pub trait ProjectionDeliverySource: Send + 'static {
    fn descriptor(&self) -> &ProjectionDeliveryDescriptor;
    fn watch_keys(&self) -> &[Key];

    fn collect_request(
        &mut self,
        reason: DeliveryReason,
        consumer: &ConsumerProjectionState,
    ) -> BoxedProjectionSnapshotRequest;

    fn commit_sent(
        &mut self,
        consumer_id: &ConsumerId,
        frame: &ProjectionFrame,
    );
}
```

The exact type-erasure mechanism may differ. The load-bearing responsibilities
are:

- define which cache keys mark this source dirty;
- define a short owner-executed atomic read;
- return owned or `Arc`-backed typed frame data;
- validate viewer/server delivery positions;
- construct a suffix, replace, patch set, or snapshot according to semantics;
- advance sent state only after the frame enters the outbound stream;
- retain enough authoritative source state or history to recover, otherwise
  request a snapshot.

Do not serialize to JSON inside the snapshot closure.

### 8.4 Typed Frames

Keep Ledger frames typed. Do not reduce all projections to arbitrary JSON in
the projection crate.

Initial shape:

```rust
pub enum ProjectionFrame {
    Bars(BarsDeliveryFrame),
}
```

The enum grows with real projection kinds. Remux maps typed variants to wire
DTOs. This preserves Rust validation while allowing a common delivery
executor.

Every frame has a common envelope:

```rust
pub struct ProjectionFrameEnvelope<TPosition, TPayload> {
    pub session_generation: u64,
    pub spec: String,
    pub schema_version: u16,
    pub frame_sequence: u64,
    pub base: Option<TPosition>,
    pub head: TPosition,
    pub operation: ProjectionFrameOperation,
    pub payload: TPayload,
}
```

`base: None` means an authoritative snapshot/reset. A delta requires an exact
base match in the viewer.

### 8.5 Projection Positions

Positions are server-issued and projection-specific but must include common
generation/version stamps.

Bars position example:

```rust
pub struct BarsDeliveryPosition {
    pub session_generation: u64,
    pub epoch: u64,
    pub projection_revision: u64,
    pub processed_batches: usize,
    pub completed_bars: usize,
}
```

The current `BarsStatus` does not contain a dedicated revision. Initially,
`(epoch, processed_batches, completed_bars)` is a monotonic source version.
Adding a projection-owned revision is preferred when live-state updates can
change without advancing those fields.

The server validates a viewer-reported position:

```text
same active session generation
same projection spec and schema version
known epoch/generation
position not beyond authoritative head
position compatible with retained/reconstructable history
```

Failure selects a snapshot; it never mutates server truth.

### 8.6 Bars Delivery Reader

`BarsDeliverySource` atomically reads:

```text
BarsStatus
completed bars range required by this consumer
live bar
feed cursor or convergence stamp when required
```

Same epoch and valid extent:

```text
read bars[consumer.completedBars .. status.completedBars]
carry latest live bar
base = consumer position
head = current bars position
operation = append/patch
```

New epoch, unknown position, or invalid extent:

```text
read bars[0 .. status.completedBars]
carry latest live bar
base = null
head = current bars position
operation = snapshot/reset
```

One-minute bars are the only active Lens projection in the first integration.
The design must remain correct for one-second bars without requiring them to be
kept warm when unused.

## 9. Part C — Delivery Executor

### 9.1 One Worker per Session, Not per Projection

Start one delivery executor for the active Ledger session. It owns all delivery
sources and consumer subscription state. Do not spawn an OS thread per
projection.

An explicit current-thread Tokio runtime on a dedicated OS thread is a good
initial implementation because it supports cell watches, cadence timers,
channels, leases, and shutdown without competing with the cache owner. Running
it as an isolated Tokio task is acceptable if tests prove the same scheduling
and blocking properties.

### 9.2 Sample Before Reading

Cell watches only mark a projection dirty:

```text
status generation changes
  -> dirty[spec] = true
```

The expensive atomic read occurs only when:

- a consumer's wall-clock delivery deadline is due;
- a seek barrier completes;
- a subscription needs an initial snapshot/suffix;
- a resync is requested;
- a terminal/final state must flush immediately.

Never read on every cache change and then discard frames at the FPS boundary.

### 9.3 Executor Loop

Conceptual loop:

```rust
loop {
    tokio::select! {
        Some(change) = dirty_rx.recv() => mark_dirty(change),
        Some(command) = command_rx.recv() => handle_command(command),
        _ = next_deadline() => collect_due_dirty_sources().await,
        _ = lease_deadline() => expire_consumers(),
        _ = shutdown.changed() => break,
    }
}
```

Commands include:

```text
subscribe consumer
acknowledge applied positions
set consumer active/inactive and cadence caps
request resync
begin seek barrier
cancel/replace seek barrier
close consumer/session
```

### 9.4 Output Backpressure

Delivery output is bounded. The executor must not wait indefinitely on a slow
stdio/WebSocket consumer.

Per semantic:

```text
ReplaceLatest   replace an unsent pending state with the newest state
PatchByKey      merge latest pending patch per stable key
AppendOrdered   combine unseen ordered items; never silently drop them
Snapshot        retain newest requested snapshot; regeneration is allowed
```

Sent state advances only when a frame is successfully admitted to the ordered
outbound stream. A disconnected consumer is expired/resynced, not allowed to
retain unbounded pending memory.

### 9.5 Serialization

Serialization runs after the owner snapshot returns and after cache locks have
been released.

The initial protocol remains JSON. Avoid the current extra materialization
path when practical:

```text
typed frame -> serde_json::Value -> String
```

Prefer serializing a typed JSON-RPC notification directly to an owned string or
byte buffer on the delivery/I/O side. Binary transport is a non-goal until
measurements demonstrate that JSON is the limiting factor.

## 10. Part D — Seek Barrier

### 10.1 Why FPS Is Not Enough

Limiting seek output to 20 FPS still presents projection catch-up as a short
fake replay. Explicit seeking should present the requested target once the
server has materialized it.

### 10.2 Barrier State

Before submitting a seek clock write, session control tells delivery:

```rust
Seeking {
    expected_clock_revision: u64,
    target_session_ns: u64,
    required_specs: Vec<String>,
}
```

This must happen before the new clock state can be observed. If submitting the
seek fails, the barrier is cancelled.

The expected revision prevents the pre-seek converged state from satisfying
the barrier before the queued clock write applies.

### 10.3 Completion Predicate

The barrier completes only after an atomic snapshot shows:

```text
clock.revision >= expected_clock_revision
feed.status.clock.revision >= expected_clock_revision
feed.status.cursor == feed.cursor
feed.cursor.catchingUp == false
for every required projection:
  projection.epoch == feed.cursor.epoch
  projection.processedBatches == feed.cursor.batchIdx
```

Projection kinds may add stronger convergence rules.

The feed clock revision is a causal acknowledgment, not presentation
metadata. A clock write and the feed's reaction are concurrent tasks; the new
clock can therefore become visible while the old cursor/projection pair still
looks converged. The feed publishes its unchanged cursor/status after it has
observed a clock revision even when a seek lands between batches, and the
barrier requires that acknowledgment before accepting convergence.

### 10.4 Barrier Presentation

While seeking:

- clock and cursor/progress metadata may continue;
- projection dirty flags accumulate;
- ordinary projection frame deadlines do not collect or emit intermediate
  projection state for `FinalSnapshotOnly` sources;
- controls remain responsive;
- a newer seek replaces the older expected revision/target;
- pause/play/speed changes do not themselves create seek barriers.

On completion:

- collect every required dirty projection exactly once from one committed
  post-convergence state per projection;
- emit immediately, ignoring the ordinary FPS deadline;
- forward progress/head metadata showing convergence;
- return to ordinary cadence.

Forward seek can emit a validated suffix when the consumer's base remains
usable. Backward seek/new epoch emits a reset snapshot.

## 11. Part E — Viewer Subscription and Accuracy Protocol

### 11.1 Viewer State Is an Optimization Hint

The viewer communicates:

- which schema versions it can parse;
- its requested FPS cap;
- whether it is active/visible;
- its last **server-issued applied position**, if the current JS context still
  holds that exact accumulator state.

The viewer does not report arbitrary counts as truth. Ledger validates all
positions and chooses suffix or snapshot.

### 11.2 Consumer Instance

Each Lens JavaScript bootstrap generates a random `consumerInstanceId`.

```text
same WebView suspended/resumed   same consumer instance; may report `have`
native WebView reload            new consumer instance; empty state => null
SPA tab/view switch retaining accumulator
                                 same only if the accumulator truly remains
```

The exact replay `sessionId` in the Remux route restores the server session; it
does not imply the new consumer has projection data.

### 11.3 Subscribe RPC

Proposed method:

```text
remux/ledger/session/projections/subscribe
```

Request:

```json
{
  "sessionId": "session-4",
  "consumerInstanceId": "random-js-context-id",
  "projections": [
    {
      "spec": "bars:1m",
      "schemaVersions": [1],
      "requestedMaxFps": 15,
      "have": null
    }
  ]
}
```

Response:

```json
{
  "subscriptionId": "opaque-capability",
  "sessionGeneration": 7,
  "leaseMs": 30000,
  "projections": [
    {
      "spec": "bars:1m",
      "schemaVersion": 1,
      "semantics": "appendOrderedWithLatestLive",
      "effectiveMaxFps": 15,
      "resume": "snapshot"
    }
  ]
}
```

Subscription starts only after Lens has installed the matching parser and
accumulator. Unknown/unsupported projection kinds remain headless or cause a
typed subscription rejection; they never send frames the viewer cannot parse.

### 11.4 Frame Notification

Proposed common notification:

```text
remux/ledger/session/projections/frame
```

Envelope:

```json
{
  "subscriptionId": "opaque-capability",
  "sessionGeneration": 7,
  "spec": "bars:1m",
  "schemaVersion": 1,
  "frameSequence": 843,
  "base": {
    "epoch": 1,
    "projectionRevision": 19231,
    "processedBatches": 882010,
    "completedBars": 500
  },
  "head": {
    "epoch": 1,
    "projectionRevision": 19720,
    "processedBatches": 883019,
    "completedBars": 507
  },
  "operation": "append",
  "kind": "bars",
  "payload": {
    "bars": [],
    "live": {}
  }
}
```

The viewer applies a delta only when:

```text
subscription/session generation match
schema version is supported
frame sequence is monotonic
frame.base exactly equals local applied position
projection-specific payload validation succeeds
```

`base: null` is an authoritative snapshot/reset. After an atomic accumulator
commit, the local applied position becomes `head`.

### 11.5 Acknowledgment RPC

Proposed method:

```text
remux/ledger/session/projections/ack
```

An acknowledgment means parsed, validated, and committed to the accumulator;
it does not wait for canvas paint.

```json
{
  "subscriptionId": "opaque-capability",
  "applied": [
    {
      "spec": "bars:1m",
      "head": {
        "epoch": 1,
        "projectionRevision": 19720,
        "processedBatches": 883019,
        "completedBars": 507
      }
    }
  ]
}
```

Lens coalesces acknowledgments:

- at most once every approximately 500–1000 ms during steady playback;
- after a large snapshot;
- after a seek-final frame;
- before suspension when the lifecycle event permits;
- never one RPC per visual frame by default.

Server acceptance requires:

```text
active opaque subscription capability
monotonic advance from previous acknowledgment
position no newer than a frame issued to this subscription
current session generation/spec/schema
projection-specific valid position
```

Invalid acknowledgment cannot corrupt state; it is rejected or causes a
snapshot resync.

### 11.6 Demand/Visibility RPC

Proposed method:

```text
remux/ledger/session/projections/demand
```

```json
{
  "subscriptionId": "opaque-capability",
  "active": false,
  "requestedMaxFps": 5
}
```

When inactive:

- projection computation continues;
- authoritative cache state advances;
- full frames stop or reduce according to policy;
- lightweight head/lease traffic may continue;
- resumption uses the viewer's applied position or a snapshot.

Unload/unsubscribe is best effort. Leases reclaim abandoned consumers because
mobile WebView destruction cannot be trusted to send cleanup.

### 11.7 Resync RPC

Proposed method:

```text
remux/ledger/session/projections/resync
```

Lens sends its current applied position and a reason such as:

```text
base_mismatch
malformed_frame
unsupported_operation
resume_after_disconnect
consumer_state_lost
```

Ledger validates the position and responds with a suffix or authoritative
snapshot. The existing `session/bars { from }` pull can remain as a diagnostic
and transitional fallback, but the subscription protocol becomes the normal
stream recovery path.

### 11.8 Server Per-Consumer State

For each projection subscription:

```text
acknowledgedHead
sentHead
availableHead
dirty
effectiveMaxFps
lastFrameAt
schemaVersion
active
leaseDeadline
```

`sentHead` advances on outbound admission. `acknowledgedHead` advances only on
validated viewer acknowledgment. `availableHead` is read from authoritative
projection cells.

For bars, Ledger can regenerate a suffix from the cached array and need not
retain every prior frame. Ordered projections without reconstructable cache
history must retain an ordered delivery log until acknowledgments advance or
fall back to an authoritative snapshot/history pull.

## 12. Part F — Staleness and Progress

Do not reduce staleness to one timestamp or boolean.

### 12.1 Projection Lag

Server projection is behind feed truth:

```text
projection.processedBatches < feed.cursor.batchIdx
```

This is expected during seek/rebuild or expensive computation.

### 12.2 Delivery Lag

Viewer applied head is behind the authoritative projection head:

```text
viewer.appliedRevision < projection.availableRevision
```

Small delivery lag is intentional under FPS coalescing.

### 12.3 Connection Uncertainty

The viewer cannot establish the current server head because the connection,
subscription, or lease is unhealthy.

### 12.4 Head Watermark

Provide a lightweight watermark, piggybacked on existing feed notifications or
sent at a low cadence while active:

```json
{
  "subscriptionId": "opaque-capability",
  "feed": {
    "epoch": 1,
    "feedSeq": 883019,
    "batchIdx": 883019,
    "catchingUp": false
  },
  "projections": [
    {
      "spec": "bars:1m",
      "epoch": 1,
      "projectionRevision": 19720,
      "processedBatches": 883019
    }
  ]
}
```

Lens derives UI state:

```text
current
projection_catching_up
delivery_pending
viewer_lagging
resyncing
disconnected_unknown
```

Do not use server/client wall-clock comparison for correctness; clocks may
differ. Monotonic server generations, revisions, positions, and feed cursors
are authoritative. A paused session is current indefinitely when advertised
and applied heads match.

## 13. Part G — Remux Client Scoping

### 13.1 Current Limitation

Remux currently broadcasts extension notifications to all connected clients.
The native host adds top-level `remuxContext` (`tabId`, `resourceKey`) to
requests, and the Remux runtime knows the originating `WsClient`, but generic
extension routing forwards only method/params to the stdio extension.

A viewer-generated `consumerInstanceId` tagged onto globally broadcast frames
can prove the Ledger protocol, but it still wastes transport and requires every
viewer to filter other consumers' frames.

### 13.2 Required Remux Extension

Add client-scoped extension subscriptions/notifications:

```text
WebView request
  -> Remux binds an opaque origin to WsClient + tab/WebView context
  -> extension receives opaque origin metadata for subscribe
  -> extension returns/uses an opaque subscription capability
  -> targeted extension notification names the origin/subscription
  -> Remux sends only to the originating downstream client/tab
```

Do not expose mutable `WsClient` internals to extensions. The extension needs
only an opaque consumer origin and a way to emit a targeted notification.

Remux already has client-scoped RPC machinery for built-in subscriptions; the
implementation should extend that pattern rather than invent a second socket.

### 13.3 Staged Compatibility

Allowed staging:

1. implement Ledger subscription semantics with tagged global broadcast under
   the current single-user/single-active-session assumptions;
2. add Remux targeted extension delivery;
3. switch Ledger frames to targeted delivery without changing frame payloads.

The final acceptance criteria require targeted delivery before claiming
multi-viewer optimization complete.

## 14. Part H — Lens Projection Clients

### 14.1 Registry

Evolve the existing chart-layer registry into a schema-versioned projection
client registry:

```ts
interface ProjectionClient<TFrame, TSnapshot, TPosition> {
  kind: string
  schemaVersion: number
  parseFrame(value: unknown): TFrame | null
  createAccumulator(spec: string): ProjectionAccumulator<
    TFrame,
    TSnapshot,
    TPosition
  >
  createLayer?(context: ProjectionLayerContext<TSnapshot>): ChartLayer
}
```

Server projections advertise kind/schema; Lens subscribes only after a
matching client is installed. Server projection code never ships executable JS
to Lens.

### 14.2 Accumulator Contract

An accumulator:

- owns the local applied position;
- validates common envelope and projection payload;
- applies snapshot/delta atomically;
- exposes a stable `getSnapshot()`;
- returns the newly applied server head for acknowledgment;
- reports base mismatch without mutating state;
- remains idempotent under duplicate/repeated frames.

Current bars monotonic/epoch/gap guards remain useful but move under the common
base/head protocol.

### 14.3 Rendering

Projection delivery already bounds server frame cadence. Lens still coalesces
multiple deliveries into one `requestAnimationFrame` paint as defense in depth.

Data accumulation is never gated by React rendering. Acknowledgment follows
accumulator commit, not canvas paint.

Explicit seek receives one final projection frame from the server. Lens may
show cursor/progress syncing state while waiting but must not animate partial
bars as playback.

### 14.4 Reload and Resume

```text
same live JS context after temporary suspension
  report actual local applied position; suffix may resume

new WebView after native reload
  exact session route attaches server session
  new consumer instance reports have: null
  authoritative projection snapshot hydrates chart
```

Durable IndexedDB projection caching is a future feature. Until then, no route
or session token implies projection data possession.

## 15. Future Off-Thread Projection Compute

Atomic snapshots create a safe path for heavy computation without parallel
cache writers:

```text
1. runtime captures immutable typed inputs atomically
2. bounded compute pool processes inputs off-thread
3. result returns with session generation + input position
4. runtime owner validates acceptance policy
5. runtime commits accepted output atomically
6. changed keys schedule downstream tasks
```

Future execution policies:

```text
CoreSync       cheap, exact, required immediately; stays on runtime owner
InlineSync     runs on owner only when wake policy is due
AsyncLatest    heavy replace-latest work; obsolete input/result may drop
AsyncOrdered   heavy ordered work; commits in input order
Offline        artifact production outside active replay
```

Bars remains inline until profiling proves otherwise. Moving a cheap
incremental fold off-thread would add input copying, scheduling, ordering, and
commit overhead with little benefit.

This spec defines generation/position stamps so the first genuinely heavy
projection can add snapshot/compute/commit without changing delivery or cache
ownership. A general multicore DAG scheduler is not implemented now.

## 16. Failure Handling

```text
snapshot request rejected/shutdown
  delivery marks consumer uncertain and retries/resyncs; runtime truth remains

snapshot reader error
  projection-specific delivery error; no sent-head advance

delivery executor failure
  session reports stream failure, restarts executor or requires resubscribe;
  projection cache remains authoritative

outbound queue full
  semantic coalescing or consumer expiration; never block runtime compute

schema mismatch
  typed subscription error; do not send undecodable payload

base mismatch
  viewer rejects frame without mutation and requests resync

invalid acknowledgment
  reject/ignore and force snapshot if necessary; never alter truth

lease expiration
  discard consumer delivery state; session/projections continue according to
  session ownership

extension/runtime restart
  ephemeral session generation disappears; route attach misses and normal open
  creates a new session
```

## 17. Observability

Add counters/timings before tuning constants:

### Runtime snapshots

```text
requests
cancelled/rejected
queue wait duration
execution duration
slow-read warnings by projection/spec
bytes/items cloned when source can report them
```

### Projection delivery

```text
dirty notifications
atomic collects
dirty changes coalesced per collect
frames by kind/spec/operation
payload bytes
frames suppressed during seek
seek barrier duration
outbound backpressure/coalescing
snapshot vs suffix decisions and reasons
```

### Consumers

```text
active subscriptions
effective FPS
sent-to-acked distance
available-to-acked distance
ack cadence
resync count/reasons
lease expirations
schema rejections
```

Expose diagnostics through logs/status first. A user-facing performance panel is
not required.

## 18. Implementation Plan

### Phase 1 — Cache ownership capability

Files primarily under `crates/cache` and `crates/runtime`.

1. Add `CacheReader`/`CacheReadView` and consume the mutable cache into the
   runtime worker.
2. Migrate `RuntimeHandle`, task/process contexts, Ledger session handles, CLI,
   and Remux to read-only capability exposure.
3. Preserve direct owner APIs for setup/cache tests.
4. Prove active writes still flow only through task-local or external batches.

Deliverable: mutation ownership is explicit; behavior otherwise unchanged.

### Phase 2 — Atomic snapshot command

Files primarily:

```text
crates/runtime/src/handle.rs
crates/runtime/src/worker.rs
crates/runtime/src/runtime/mod.rs
crates/runtime/src/snapshot.rs (new, if useful)
```

1. Add bounded snapshot request/reply channel.
2. Add typed `RuntimeHandle::snapshot` API.
3. Execute reads at task/write boundaries with fairness and panic containment.
4. Add metrics and shutdown/cancellation behavior.
5. Convert Remux status and bars pull multi-cell reads to atomic snapshots as
   the first production users.

Deliverable: no retry is required for a coherent bars/status/live read.

### Phase 3 — Projection delivery definitions

Files primarily under `crates/ledger/src/projection` and
`crates/ledger/src/session.rs`.

1. Introduce typed delivery descriptors, positions, frames, and source trait.
2. Make bars installation produce `BarsTask + BarsDeliverySource`.
3. Add one session-owned delivery executor with dirty watches and timers.
4. Add bounded semantic output queue and delivery metrics.
5. Exercise delivery internally without changing the existing Lens wire yet.

Deliverable: bars frames can be sampled from atomic cache state at bounded FPS
without direct Remux cell reads.

### Phase 4 — Seek barrier

1. Bind seek controls to an expected clock revision before submitting the
   clock write.
2. Suppress `FinalSnapshotOnly` projection collection during seek.
3. Complete on atomic feed/projection convergence.
4. Emit one final suffix or reset snapshot.
5. Handle rapid seek replacement and controls during convergence.

Deliverable: seeking never presents intermediate bars as fast playback.

### Phase 5 — Ledger subscription protocol

Files primarily under `crates/remux/src/session.rs` plus Lens replay API/types.

1. Add subscribe/frame/ack/demand/resync protocol.
2. Add consumer instances, opaque subscription IDs, leases, and positions.
3. Replace direct bars watcher with delivery executor frames.
4. Retain current pull snapshot as fallback/diagnostic.
5. Add head watermarks and explicit staleness derivation.

Deliverable: one viewer can resume by validated suffix or snapshot and Ledger
knows its acknowledged applied head.

### Phase 6 — Lens projection client registry

1. Generalize parser/accumulator/layer registration by kind/schema version.
2. Subscribe only after client readiness.
3. Validate base/head and coalesce acknowledgments.
4. Send activity/cadence demand from Remux host visibility.
5. Resync on mismatch/resume; treat new WebView state as empty.
6. Coalesce last-mile chart paints with `requestAnimationFrame`.

Deliverable: accurate, observable, projection-specific viewer delivery.

### Phase 7 — Remux targeted extension streams

Changes live in the Remux repository and are coordinated from this spec.

1. Forward an opaque origin for client-scoped extension subscription requests.
2. Support targeted extension notifications through stdio supervision and WS.
3. Bind origin to downstream client/tab lifecycle.
4. Route Ledger projection frames only to their subscriber.
5. Remove transitional globally broadcast tagged frames.

Deliverable: multi-viewer delivery is correct and transport-efficient.

### Phase 8 — Profiling and policy tuning

1. Run full-day real-data seek/playback profiles.
2. Measure runtime step time, snapshot wait/execute time, clone volume,
   serialization time, wire bytes, viewer apply time, and ack lag.
3. Tune FPS defaults and queue/budget sizes from evidence.
4. Introduce `Arc`/chunked state only where measurements justify it.

## 19. Test Plan

### Cache/runtime tests

- snapshot of two values written together never observes mismatched values
  across many concurrent requests;
- snapshot cannot run midway through one `RuntimeTask::run_once`, including a
  task that submits more than one local batch;
- separate external batches are observable as separate commits;
- snapshots are served during a long `WakeAgain` rebuild chain;
- bounded snapshot servicing cannot starve writes/tasks;
- snapshot cancellation and runtime shutdown do not leak waiters;
- snapshot reader panic does not kill the runtime worker;
- public runtime/session handles cannot mutate cache directly;
- existing scheduling, ownership, feed, and projection tests remain green.

### Bars delivery tests

- initial subscription with `have: null` emits one authoritative snapshot;
- valid same-epoch position emits exactly the missing completed suffix plus
  latest live bar;
- invalid extent, old generation, or changed epoch emits reset snapshot;
- many dirty writes inside one FPS interval cause one atomic collect/frame;
- frame base/head positions are monotonic and validate against cache state;
- sent head advances only after outbound admission;
- backpressure coalesces live state without losing completed bars;
- a 20k/full-day seek performs no per-chunk presentation reads and emits one
  final projection update;
- backward seek emits one new-epoch reset;
- rapid seeks publish only the newest target;
- pause/speed/play during catch-up remains responsive.

### Subscription tests

- unsupported schema is rejected before frames are sent;
- valid viewer `have` resumes with suffix;
- viewer position beyond head or from another generation snapshots;
- frame base mismatch is detected and resync returns correct state;
- duplicate/out-of-order acknowledgment cannot regress acknowledged head;
- acknowledgment beyond sent head is rejected;
- inactive consumer receives no unnecessary full frames and resumes correctly;
- expired lease discards consumer state without closing session truth;
- two consumers with different applied positions receive correct independent
  bases/suffixes;
- one slow consumer cannot pace runtime or another consumer.

### Lens tests/checks

- parser rejects malformed common envelope and malformed projection payload;
- accumulator mutation is atomic with applied-head advance;
- delta with wrong base does not mutate data;
- duplicate frame is idempotent;
- acknowledgments occur after accumulator commit and are coalesced;
- native reload subscribes with `have: null` and hydrates a snapshot;
- suspension/resume with retained state uses validated suffix;
- explicit seek shows progress then one final chart update, not fast fill;
- typecheck, lint, and production build pass.

### Real-data validation

- existing deterministic bars totals/checksums remain identical;
- cache truth with delivery enabled equals truth with delivery disabled;
- explicit seek output converges to the same pull snapshot;
- frame count respects effective FPS during timed playback within scheduling
  tolerance;
- delivery frame count is independent of feed batch count during seek;
- record performance metrics on the validated full-day raw before/after.

## 20. Acceptance Criteria

1. Runtime/cache mutation remains one deterministic commit domain.
2. Production multi-cell projection reads use owner-executed atomic snapshots;
   no status/read/status retry is required for coherence.
3. Snapshot requests are served during bounded rebuild chains without starving
   compute or controls.
4. Projection tasks define matching typed delivery sources and descriptors.
5. Cell changes mark delivery dirty; expensive reads occur only at a delivery
   deadline, subscribe/resync, terminal flush, or seek completion.
6. Explicit seek produces one final projection update per required projection
   and never animates catch-up bars.
7. Projection state and final deterministic output are identical with delivery
   disabled, slow, fast, disconnected, or backpressured.
8. Every delta carries a required base and produced head; viewer mismatch
   recovers through snapshot.
9. Viewer acknowledgment is monotonic, validated, coalesced, and never trusted
   as projection truth.
10. Viewer can distinguish projection lag, delivery lag, resync, and connection
    uncertainty from server-issued positions/watermarks.
11. A reloaded empty WebView never reuses an old consumer's applied position.
12. Slow/inactive viewers do not pace runtime computation or create unbounded
    queues.
13. Remux ultimately targets projection frames to the subscribing client/tab.
14. Workspace tests, rustfmt, relevant clippy checks, Lens typecheck/lint/build,
    and real-data validation pass.

## 21. Explicit Non-Goals

- cache-only thread separate from the runtime scheduler;
- concurrent cache writers or a general multicore projection DAG;
- offloading the bars projection without profiling evidence;
- removing all per-cell locks in the first phase;
- dynamic JavaScript supplied by server projections;
- durable IndexedDB projection caches;
- exactly-once network delivery;
- retaining every visual frame forever;
- binary frame transport before JSON profiling;
- multiple active Ledger sessions unless separately specified;
- checkpointed session survival across extension/runtime process restart;
- trader-visibility latency simulation (related but separate from wall-clock
  projection delivery cadence).

## 22. Deferred Extensions

```text
async projection compute
  snapshot/compute/commit with generation-gated latest/ordered policies

chunked immutable arrays
  Arc-backed zero-copy delivery for large append-only histories

projection registry/manifests
  dynamic shared dependency graph after a real second/derived projection

durable viewer cache
  validated reuse of applied positions across native reloads

multiple sessions
  session-keyed delivery executors and targeted subscription registries

trader visibility
  replay-time market-data delay/coalescing feeding projections, separate from
  delivery wall-clock FPS

binary protocol
  only if typed JSON serialization/wire cost is measured as limiting
```

## 23. Implementation Handoff Checklist

Before beginning each phase:

- re-read this spec's invariants and the prior phase's tests;
- preserve unrelated/user changes in the worktree;
- update the spec when an implementation constraint changes a contract;
- keep atomic consistency distinct from projection convergence;
- keep server truth distinct from viewer acknowledgment;
- measure before introducing additional threads, copies, or storage forms.

At each phase boundary record:

```text
files changed
wire changes
new invariants/tests
performance measurements
remaining transitional behavior
whether the next phase is still correctly scoped
```

The intended end state is deliberately simple: one deterministic runtime/cache
owner, one concurrent read-only delivery executor, projection-defined typed
delivery, and viewers that can prove exactly which server-issued state they
have applied.

## 24. Implementation Record

Implementation date: 2026-07-10

Ledger baseline before this implementation:

```text
98fcd7c Document atomic snapshots and projection delivery
```

Remux baseline before the client-scoping implementation:

```text
89d1226 Fix Codex transcript tables and message replay
```

All changes remain uncommitted pending review.

### 24.1 Phase 1 — Cache Ownership Capability

Implemented:

- `CacheReader` is the cloneable active-session read/watch capability.
- `CacheReadView` is the immutable owner-snapshot capability and exposes no
  watch registration or mutation.
- `RuntimeHandle`, task/process contexts, `LedgerSessionHandle`, CLI helpers,
  and Remux now expose `CacheReader`, not `Cache`.
- `LedgerSessionBuilder::cache()` is explicitly setup-only and exists so cells
  for generic tasks can be registered before runtime ownership begins.
- Runtime-local and external write batches remain the only active production
  mutation paths.

Primary files:

```text
crates/cache/src/cache.rs
crates/runtime/src/{handle,process,task,write,runtime/mod}.rs
crates/ledger/src/session.rs
```

Wire changes: none.

Remaining intentional behavior: `Cache` remains cloneable for standalone cache
tests and setup code. Active production handles do not retain or return it.

### 24.2 Phase 2 — Atomic Snapshot Command

Implemented:

- a dedicated bounded snapshot request channel, separate from runtime commands
  and external writes;
- typed `RuntimeHandle::snapshot` with internal type erasure/downcast;
- snapshot execution only between external write batches/task steps;
- bounded servicing during `WakeAgain` chains and idle operation;
- cancellation detection, deterministic shutdown rejection, panic containment,
  and queue/execution metrics;
- atomic attach clock/cursor, session status, and bars pull reads in Remux.
- CLI convergence checks and final multi-projection summaries also execute as
  owner snapshots; the final summary reads only the first/last feed batches
  instead of cloning the full feed history.

Tests prove:

- two cells written in one external batch are never observed mismatched;
- a snapshot cannot run between multiple local submits in one task step;
- snapshots are served before a long rebuild chain converges;
- a panicking reader does not kill the runtime;
- snapshot ingress rejects after shutdown.

Primary files:

```text
crates/runtime/src/snapshot.rs
crates/runtime/src/{handle,worker,error}.rs
crates/runtime/tests/worker.rs
crates/remux/src/session.rs
```

Wire changes: none.

### 24.3 Phase 3 — Projection-Owned Delivery

Implemented:

- `InstalledProjection { spec, task, delivery }` installation;
- projection-owned descriptors, semantics, schema versions, useful/default
  FPS, seek policy, typed positions, typed frames, and typed payloads;
- `BarsTask + BarsDeliverySource` installed together from one builder call;
- one session-owned delivery executor with bounded command/change/output
  channels;
- cache watches that only mark consumer state dirty;
- atomic bars reads only at subscribe/resync, a due wall-clock deadline, or
  seek completion;
- seek convergence is sampled at the delivery tick rather than once per cache
  notification, and the tick is prioritized ahead of dirty-change draining so
  sustained compute cannot starve presentation cadence;
- reconstructable append suffixes plus replace-latest live bars;
- bounded `try_send` admission and sent-head advancement only after admission;
- capacity is checked before an owner snapshot so a known-full output queue
  does not repeatedly clone projection history, and retry preserves the exact
  initial/resync/seek-final reason;
- delivery metrics for dirty/coalesced changes, atomic collects, frame kinds,
  seek barriers, backpressure, leases, subscriptions, resyncs, and schema
  rejection.

The executor is an isolated Tokio task on the multithreaded runtime rather than
an explicitly pinned OS thread. This is one of the allowed implementation
shapes in Section 9.1: it owns no cache mutation, never serializes inside a
snapshot closure, and cannot pace runtime computation.

Primary files:

```text
crates/ledger/src/projection/delivery.rs
crates/ledger/src/projection/{mod,bars}.rs
crates/ledger/src/session.rs
crates/ledger/tests/projection_delivery.rs
```

Wire changes: none during the internal phase.

### 24.4 Phase 4 — Seek Barrier

Implemented:

- every explicit seek allocates its expected clock revision and installs the
  delivery barrier before submitting the clock write;
- session controls are serialized and wait until their exact clock revision is
  committed, preventing rapid controls from deriving duplicate revisions;
- `FinalSnapshotOnly` projection frames are suppressed during convergence;
- the completion snapshot validates clock revision, feed `catching_up`, feed
  clock acknowledgment, feed epoch/cursor, and every required projection
  processed cursor;
- completion emits one immediate `seekFinal` suffix or reset snapshot;
- backward seek/new epoch resets; forward seek uses a validated suffix.

Test coverage verifies that the first projection frame observed after seek is
the final converged frame and that delivery atomic collects do not scale with
rebuild chunks.

The implementation also closes the clock/feed observation race explicitly:
`EsReplayStatus.clock.revision` must acknowledge the seek revision (with its
status cursor equal to the cursor cell) before the old converged state can
satisfy the barrier. Inactive consumers remain dirty across the barrier and
receive no seek-final frame until demand becomes active again.

Subscription tests additionally cover idempotent/latest acknowledgments,
rejected regressing acknowledgments, invalid-hint snapshot resync, inactive
seek suppression/resume, and lease expiration without session shutdown.

### 24.5 Phase 5 — Ledger Subscription Protocol

Implemented RPCs:

```text
remux/ledger/session/projections/subscribe
remux/ledger/session/projections/ack
remux/ledger/session/projections/demand
remux/ledger/session/projections/resync
```

Implemented notifications:

```text
remux/ledger/session/projections/frame
remux/ledger/session/projections/watermark
```

Implemented protocol state:

- random consumer instance supplied by Lens;
- opaque subscription capability;
- session generation;
- schema negotiation and effective FPS;
- exact base/head positions;
- monotonic frame sequence;
- sent, acknowledged, and available heads;
- a bounded issued-head log used to reject acknowledgments that were not
  issued;
- activity/demand and renewable 30-second lease;
- lease cleanup propagated to Remux origin bookkeeping, with expiration events
  retained across a temporarily full frame queue;
- authoritative snapshot fallback for invalid generation/epoch/extent;
- distinct feed/projection watermarks.

The existing `session/bars` pull remains as a diagnostic/fallback. The direct
`barsFrame` status-cell watcher and its legacy push notification were removed.
Projection-backed sessions also stop the legacy chunk-rate feed notification;
their feed cursor is carried by the one-second delivery watermark. The direct
feed stream remains only for headless/legacy sessions without a delivery
executor.

### 24.6 Phase 6 — Lens Projection Clients

Implemented:

- schema-versioned projection client registry;
- defensive common-envelope and bars payload parser;
- atomic accumulator base/head application;
- no mutation on base mismatch or malformed frames;
- idempotent duplicate handling;
- acknowledgment after accumulator commit, coalesced to 750 ms in steady
  playback and immediate after snapshot/resync/seek-final;
- demand renewal and document visibility updates;
- host-resume resync with resubscribe fallback when a lease expired;
- a new WebView subscribing with `have: null`;
- retained same-context state reporting only its actual applied position;
- watermark-derived `current`, `projection_catching_up`, `delivery_pending`,
  `viewer_lagging`, `resyncing`, and `disconnected_unknown` states;
- the action bar consumes those protocol states directly with distinct labels;
  the removed legacy `processedBatches != cursor.batchIdx` heuristic could
  misclassify a projection ahead of a one-second watermark as lagging;
- viewer-only lag is shown only after 500 ms, while authoritative projection
  catch-up, resync, initial delivery, and connection uncertainty are immediate;
- Lens ignores the legacy chunk-rate feed notification during replay and uses
  the delivery watermark cursor instead, so seek progress cannot drive React
  at projection-compute cadence;
- last-mile chart application coalesced with `requestAnimationFrame`.

Primary files:

```text
lens/src/features/replay/projection-client.ts
lens/src/features/replay/accumulator.ts
lens/src/features/replay/{api,types,use-replay-session}.ts
lens/src/features/replay/chart/bars-layer.ts
```

The reload path now attaches the exact session and lets the delivery protocol
hydrate one server snapshot. It no longer pulls and re-applies every historical
bar as an independent UI update.

### 24.7 Phase 7 — Remux Targeted Extension Streams

Implemented in `/home/ubuntu/remux`:

- Remux identifies requests that actually route to an extension;
- the WS layer binds a stable opaque origin to the connected socket plus its
  top-level `remuxContext`;
- `_remuxOrigin` is injected only into extension object params;
- Ledger stores the origin against the opaque projection subscription;
- later ack/demand/resync calls must present the same Remux-injected origin,
  preventing another socket/context from controlling a guessed subscription;
- Ledger emits top-level `remuxTarget { origin }` on projection frames and
  watermarks;
- the extension supervisor strips `remuxTarget` and calls a targeted runtime
  delivery hook instead of broadcast;
- `WsServer::send_to_origin` sends only to the owning downstream context;
- socket teardown destroys all origin capabilities.

Tests cover stable same-context origins, isolation from a second socket,
extension routing metadata stripping, and Ledger preservation of the origin on
projection output.

Direct in-process Ledger tests have no Remux origin and intentionally exercise
the compatibility broadcast fallback. Production viewer subscriptions are
targeted.

### 24.8 Phase 8 — Profiling and Policy Validation

Host/data:

```text
market day       2026-03-10
feed batches     18,858,570
one-minute bars  1,379 completed + one live
build profile    Rust debug (comparison only, not a release benchmark)
requested FPS    20
```

No delivery subscriber:

```text
elapsed          47.56 s
user             48.46 s
system           3.33 s
maximum RSS      6,989,496 KiB
```

20 FPS delivery subscriber:

```text
elapsed                       44.76 s
user                          44.17 s
system                        2.46 s
maximum RSS                   6,956,124 KiB
dirty notifications           1,102
coalesced dirty notifications 1,101
atomic projection collects    2
frames admitted/received      2 / 2
snapshot / suffix frames      1 / 1
frames suppressed during seek 47
outbound backpressure         0
seek barriers completed       1
measured convergence duration 2,542,118,217 ns
```

Both runs produced the same authoritative result:

```text
feed sequence     18,858,570
completed bars    1,379
volume            1,811,043
trade count       704,860
first bar         1773093600000000000
last/live bar     1773176340000000000
```

Conclusion: on this host, delivery did not materially pace projection compute.
1,102 dirty notifications became two presentation reads/frames: the initial
empty snapshot and the single final state after high-speed convergence. This
validates dirty coalescing, tick-sampled seek convergence, and
`FinalSnapshotOnly`; delivery performed no per-rebuild-chunk bars reads. The
initial 10 FPS/default and 20 FPS maximum remain appropriate pending on-device
paint/wire measurements.

The CLI supports repeatable profiling with:

```text
ledger session run-es-replay \
  --raw-id sha256-2f935d7277b7210f7a5f93f240b04f1b37a4db55064bfa3af380a7fca4f4dbac \
  --realtime --speed 1000000000 \
  --projection bars:1m --delivery-fps 20
```

Use `--realtime` for this comparison. The CLI's default step mode deliberately
issues one explicit seek per next feed timestamp and is a control-path stress
test, not a throughput benchmark.

### 24.9 Remaining Validation, Not Remaining Architecture

Before calling the UX final, perform the original device feel-check:

1. play at 5x, seek mid-session, reload the native viewer;
2. verify route attach preserves clock mode/speed/time and bars hydrate in one
   authoritative snapshot;
3. leave it playing while the screen is closed, return/reload, and verify feed
   and bars progressed server-side;
4. seek across a large range and verify only progress metadata moves before one
   final chart snap;
5. back to days and re-enter the same day, verifying intentional fresh start.

No architectural phase is deferred by that check. Binary transport, durable
viewer caches, async heavy projection compute, chunked immutable arrays, and
multiple active sessions remain the explicit non-goals/deferred extensions in
Sections 21–22.
