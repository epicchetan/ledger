# Ledger Feed System Implementation Spec

## Purpose

Run the ES replay feed as a live emitter: a session clock in a cache cell,
an `EsReplayFeed` runtime process that emits MBO exchange batches as they
become due at session time, and a session wrapper the CLI (and later remux)
drives.

This is the third phase of the feed build. Both prerequisites have LANDED:

```text
1. docs/cache_watch_implementation_spec.md — LANDED
   Cache::watch_key(&Key) -> Result<CellWatch, CacheError>
   CellWatch::changed().await   parks until the cell's generation advances;
                                coalescing; cancel-safe in select!
   CellWatch::generation()      latest generation visible to this watch
   errors: MissingCell on unregistered key; WatchClosed after cache drop

2. docs/es_data_management_implementation_spec.md — LANDED
   crates/ledger feed module (feed/es_replay/): DBN -> validated
   es_mbo_event_store artifact, market_day stamping, day catalog,
   CLI `es days` / `es prepare` / `es fetch`, proven against real raws
```

This phase adds the process half on top of the module half. The DBN
pipeline, codec, artifact rules, market-day contract, and Legacy Reference
live in the data-management spec and are not repeated here.

The boundary:

```text
store    durable raw files and generated artifacts
cache    active typed cells + change watches
runtime  external write application, task scheduling, process lifecycle
         UNCHANGED by this spec
ledger   session clock state, feed process, session wiring
```

The correctness constraints live in the base crates (cell ownership, write
application, scheduling, lifecycle, watches); the app layer stays free.
Feeds decide for themselves how to react to what they observe.

## Scope

```text
session clock as a cell:
  ClockState anchors in a session-owned `session.clock` value cell
  written only on transitions by the session owner through external writes
  play / pause / set_speed / seek_to (forward AND backward)

ES replay feed process:
  RuntimeProcess implementation over the prepared artifact
  feed-owned pacing via clock-cell watches and exact wall deadlines
  regression (backward seek) handling with an epoch contract

session wrapper:
  builder + handle owning the RuntimeWorker join and clock transitions

CLI validation:
  headless run; deterministic step mode with no wall sleeps and no polling
```

Out of scope:

```text
execution simulation, order book, book validation
bars and other task components
market-day catalog / days list / acquisition (delivered by the
  data-management phase, alongside the Lens Days screen)
Lens chart/session UI, websocket frames, user-facing seek UI
multiple concurrent sessions
live market adapters, 0DTE feeds
feed checkpoint persistence
memory-mapped artifact reads
```

## Crate Additions

`crates/ledger` gains dependencies and modules. `async-trait` is currently a
dev-dependency only — promote it (the `RuntimeProcess` impl needs it in the
lib):

```toml
[dependencies]
async-trait.workspace = true          # promoted from [dev-dependencies]
cache = { path = "../cache" }
runtime = { path = "../runtime" }

[dev-dependencies]
store = { path = "../store", features = ["test-util"] }   # see Tests
```

```text
crates/ledger/src/
  clock.rs             ClockState, ClockMode, ClockSnapshot, pure helpers
  session.rs           LedgerSessionBuilder, LedgerSessionHandle
  feed/es_replay/
    feed.rs            EsReplayFeed (RuntimeProcess implementation)
    cells.rs           EsReplayCells + process payload types

crates/ledger/tests/
  clock.rs
  es_replay_feed.rs
  session.rs
  support/mod.rs       shared fixtures (see Tests)
```

`lib.rs` adds `pub mod clock;` and `pub mod session;`.
`feed/es_replay/mod.rs` adds `pub mod cells;` / `pub mod feed;` and glob
re-exports them like the existing submodules.

## Feed Module vs Feed Process

Recap of the split (defined in the data-management spec):

```text
feed module     free functions over &Store<S>; no session, no runtime
feed process    the emitter instantiated into a session (this spec)
```

Feeds do not accept methods, commands, or RPC. Control is cell state: the
session owner writes the clock cell, and feeds converge on what they
observe. The rule:

```text
processes emit domain data
control is cell state written by the session owner
observation is cells: watch to wake, read to know
lifecycle is runtime commands
queries and jobs are module functions
```

## Core Flow

```text
1. CLI/transport asks Ledger to start ES replay for a raw store object id.
2. Ledger creates Cache.
3. Ledger registers the session.clock cell (session owner) and the ES
   replay feed cells (feed owner).
4. Ledger spawns RuntimeWorker::run, keeping the RuntimeHandle and worker join.
5. Ledger publishes the initial ClockState (paused at session time 0)
   through the external write path.
6. Ledger installs EsReplayFeed through RuntimeHandle::install_process.
7. Ledger installs task components later when they exist.
8. EsReplayFeed::prepare loads the artifact via the module's prepare
   (building it if this raw was never prepared; reusing the existing
   artifact if it was).
9. The runtime marks the feed Running and spawns EsReplayFeed::run.
10. EsReplayFeed::run publishes initial cursor/status, then parks on the
    clock cell watch.
11. The session owner writes clock transitions (seek_to first batch ts,
    then play or step).
12. The feed wakes on each clock write, emits every batch due at session
    time, and publishes cursor/status with each emission.
13. The worker applies feed writes as they arrive and runs task components
    to idle; cursor watchers (step driver, transport) wake in turn.
```

## Session Clock

Session time is a shared mapping owned by the session, not by feeds. The
clock is **state in a cell**, not an object with an API and not a ticker:

```text
cell:   session.clock          Value<ClockState>
owner:  session                (the session owner's CellOwner)
reads:  public
writes: only the session owner, only on transitions
```

Nothing ever ticks it. `ClockState` holds anchors; current time is computed
by whoever reads it:

```text
paused:  now = anchor_session_ns
running: now = anchor_session_ns + (wall_now - anchor_wall) * speed
```

Transitions are pushed (one cell write re-anchoring the mapping), time is
pulled (computed on read). Between transitions the clock has zero activity.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClockMode {
    Paused,
    Running,
}

/// Cell payload. Cells are in-process typed values, so the wall anchor is
/// a plain Instant; ClockState is deliberately NOT serializable.
#[derive(Debug, Clone, PartialEq)]
pub struct ClockState {
    pub mode: ClockMode,
    pub speed: f64,
    pub anchor_session_ns: u64,
    pub anchor_wall: std::time::Instant,
    pub revision: u64,
}

/// Derived, serializable view for status cells, CLI output, and transport.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClockSnapshot {
    pub mode: ClockMode,
    pub speed: f64,
    pub session_now_ns: u64,
    pub revision: u64,
}
```

`clock.rs` provides pure helpers, no machinery:

```rust
impl ClockState {
    pub fn initial() -> Self;                       // paused at 0, speed 1.0
    pub fn now_ns(&self) -> u64;
    pub fn snapshot(&self) -> ClockSnapshot;
    /// Wall instant at which session time reaches target, if running.
    pub fn wall_deadline(&self, target_ns: u64) -> Option<std::time::Instant>;
    pub fn play(&self) -> ClockState;               // re-anchor transitions
    pub fn pause(&self) -> ClockState;
    pub fn with_speed(&self, speed: f64) -> Result<ClockState, LedgerError>;
    pub fn seek_to(&self, session_ns: u64) -> ClockState;
}
```

Rules:

```text
initial state: paused at session time 0, speed 1.0
speed must be finite and greater than 0 (validated by the writer)
every transition re-anchors and increments revision
seek_to may move session time forward OR backward — regression is legal
  state; what it means is each feed's own business
the clock never reads feed data and never waits for feeds
```

Because the cell is owned by the session owner, "feeds never write clock
state" is not a discipline rule — the cache's ownership enforcement makes a
feed write to `session.clock` a hard error. The constraint lives in the
base crate.

Single-writer discipline: only the session handle performs transitions, and
it performs them serially, so read-modify-write races on the clock cell do
not arise.

Missing-cell rule: a feed that reads `session.clock` before the initial
publish treats `None` as `ClockState::initial()`.

Clock transitions and data writes flow through the same FIFO external-write
path, so components and watchers observe time changes and data changes in
one consistent order.

Drivers are just write patterns, not machinery:

```text
realtime playback     play/pause/set_speed transitions; session time then
                      advances continuously by definition (no writes)
deterministic step    seek_to(next_due_ts) repeatedly; no wall sleeps
live (future)         a driver keeps anchors pinned to wall time; the feed
                      contract is unchanged
```

## Feed Processes

Feeds are runtime processes. The component runtime already owns the
execution model — `RuntimeProcess` with a prepare/run lifecycle,
`ProcessContext` with owner-attributed writes and cache reads, runtime-owned
shutdown, statuses, and joins. Ledger must not define a second feed worker
abstraction (no `LedgerFeed` trait, no `FeedContext`, no ledger-owned
shutdown or join plumbing for feeds).

```text
runtime owns
  install_process / stop_process / component_status
  ComponentStatus: Preparing -> Running -> Completed / Failed
  ProcessContext: batch()/submit(), read_value/read_array, cache(), shutdown()
  per-process join handling and worker shutdown

cache owns
  cell ownership enforcement (who may write which cell)
  change watches (how a parked process wakes)

ledger owns
  concrete feed structs implementing RuntimeProcess
  the domain dependencies each feed carries as fields
  each feed's own pacing, catch-up, and regression behavior
```

A feed is a struct implementing `runtime::RuntimeProcess` that carries its
domain dependencies as fields:

```rust
pub struct EsReplayFeed<S>
where
    S: store::RemoteStore + 'static,
{
    descriptor: runtime::ComponentDescriptor,
    raw_object_id: store::StoreObjectId,
    store: Arc<store::Store<S>>,
    clock_key: cache::ValueKey<ClockState>,
    cells: EsReplayCells,
}
```

There is no clock channel and no clock receiver: the feed reads the clock
cell through `ProcessContext` like any other cell and parks on
`ctx.cache().watch_key(...)` when it has nothing to do.

Run-loop setup, once at the top of `run`:

```rust
let mut shutdown = ctx.shutdown().clone();   // changed() needs &mut; clone
                                             // the receiver instead of
                                             // borrowing ctx mutably
let mut clock_watch = ctx.cache().watch_key(self.clock_key.key())?;
```

`CellWatch` is an owned subscription — it borrows nothing from the cache —
and `prepare(&mut self)` stashes its results (artifact descriptor, decoded
`EsMboEventStore`) in `Option` fields on the feed struct; `run` treats a
missing prepare result as `ComponentError::Message`.

For the first feed:

```text
component id: feed.databento.es_replay
kind:         ComponentKind::Process
owner:        ComponentId::owner(), also used when registering the feed cells
```

Lifecycle mapping:

```text
RuntimeProcess::prepare
  prepare_es_replay_artifact(&store, &raw_object_id, false, None)
    force is always false from the session path: raw ids are
    content-addressed, so an existing valid artifact is reused
    (short-circuit), anything else is rebuilt — the same day-lifecycle
    behavior es prepare / es fetch already rely on. The module call also
    repairs the raw's market_day metadata and offloads the raw's local
    copy; the feed inherits those side effects deliberately.
  then read_event_store_file(&artifact.path) to decode EsMboEventStore
    into memory (whole-artifact in-memory decode is the accepted cost for
    this phase; mmap/indexed reads are out of scope)
  runtime reports ComponentStatus::Preparing while this runs

RuntimeProcess::run
  signature: async fn run(self: Box<Self>, mut ctx: ProcessContext)
  publish initial cursor/status, then the clock-observing emission loop
  the process exits only on shutdown (see End Behavior — a feed that has
  emitted everything parks rather than completing, so a backward seek can
  revive it)

domain errors
  LedgerError maps into ComponentError::Message at the trait boundary;
  cache errors ride the existing #[from] into ComponentError::Cache
  the runtime surfaces them as ComponentStatus::Failed
```

`ProcessContext::batch()` already creates owner-attributed
`ExternalWriteBatch` values and `submit` routes them through the runtime
ingress, so ledger adds no write plumbing of its own.

## Runtime Boundary

The runtime is an actor and is **unchanged by this spec**. `Runtime` is the
synchronous core (cache write application plus task scheduling);
`RuntimeWorker` wraps it in a single event loop selecting over a command
channel, the external-write channel, and internal lifecycle events;
`RuntimeHandle` is the cloneable client.

The handle bundles the planes of the system:

```text
control      commands over mpsc with oneshot replies — lifecycle only
             install_process / install_task / stop_process /
             component_status / drain / shutdown
data in      a clone of the ExternalWriteSink
data out     a clone of the Cache: direct reads plus watch_key
```

Load-bearing worker behavior: when a write batch arrives, the worker
immediately applies it and runs task components to idle. Applying the write
bumps cell generations, which wakes parked watchers. Nobody polls anything,
anywhere:

```text
driver writes clock cell -> worker applies -> feed's clock watch wakes ->
feed emits -> worker applies -> cursor generation bumps -> driver's cursor
watch wakes
```

Every hop is a direct scheduler wakeup.

Synchronization rule: lifecycle flows through commands, data through
writes, observation through cells. Synchronization is a form of
observation, so it rides cell watches — never the command channel. `Drain`
only runs work already queued inside the runtime; it cannot see an emission
a feed has not submitted yet, so it is never a barrier.

Feed processes submit through their `ProcessContext`; non-process writers
(the session owner's clock transitions, future transports) use
`RuntimeHandle::external_write_sink()`. Do not add a second ingress in
`ledger`, and do not implement a second scheduler.

## Feed Batch Semantics

The runtime does not semantically combine feed batches. It applies queued
writes in FIFO order and records changed keys. The chosen cache primitive
determines what components see:

```text
Value<T>   current state; writes overwrite       clock, cursor, status
Array<T>   ordered collection; pushes accumulate, batches
           owner may replace/truncate/clear
```

For ES replay, delivered exchange batches use the array cell
`feed.databento.es_replay.batches`. Components that consume it own their
own progress cursor in component state, and reset it when the feed's
`epoch` changes (see Regression). The runtime should not introduce a
generic feed cursor abstraction.

## Session Shape

The session owner is `CellOwner::new("session")`; the clock cell key is
`session.clock` (Value, public read). Note the write-attribution model:
`ExternalWriteSink` is a plain channel with no owner — attribution lives on
each batch via `ExternalWriteBatch::new(writer: CellOwner)`. "Session-owner
writes" means the handle constructs its batches with the session owner.

```rust
pub struct LedgerSessionBuilder<S>
where
    S: store::RemoteStore + 'static,
{
    store: Arc<store::Store<S>>,
    cache: cache::Cache,
    clock_key: cache::ValueKey<ClockState>,
    feeds: Vec<Box<dyn runtime::RuntimeProcess>>,
}

impl<S> LedgerSessionBuilder<S>
where
    S: store::RemoteStore + 'static,
{
    /// Creates the cache and registers session.clock under the session owner.
    pub fn new(store: Arc<store::Store<S>>) -> Result<Self, LedgerError>;

    /// Registers the ES replay cells and queues the feed. Returns the typed
    /// cells so the caller (CLI driver, tests, transport) can watch and read
    /// them; the feed keeps its own clone.
    pub fn es_replay(
        &mut self,
        raw_object_id: store::StoreObjectId,
    ) -> Result<EsReplayCells, LedgerError>;

    pub async fn start(self) -> Result<LedgerSessionHandle, LedgerError>;
}
```

`start` responsibilities, in order:

```text
RuntimeWorker::new(cache) -> (worker, RuntimeHandle)
tokio::spawn(worker.run()), keeping the JoinHandle
publish the initial ClockState through the external write path
install queued feeds through RuntimeHandle::install_boxed_process
install task components when available (none in this phase)
return session handle
```

Handle:

```rust
pub struct LedgerSessionHandle {
    runtime: runtime::RuntimeHandle,
    worker: JoinHandle<Result<(), runtime::RuntimeError>>,
    session_owner: cache::CellOwner,
    clock_key: cache::ValueKey<ClockState>,
    writes: runtime::ExternalWriteSink,
}

impl LedgerSessionHandle {
    pub fn cache(&self) -> &cache::Cache;      // via runtime.cache()
    pub fn runtime(&self) -> &runtime::RuntimeHandle;
    pub fn clock_snapshot(&self) -> Result<ClockSnapshot, LedgerError>;

    pub async fn play(&self) -> Result<(), LedgerError>;
    pub async fn pause(&self) -> Result<(), LedgerError>;
    pub async fn set_speed(&self, speed: f64) -> Result<(), LedgerError>;
    pub async fn seek_to(&self, session_ns: u64) -> Result<(), LedgerError>;

    pub async fn shutdown(self) -> Result<(), LedgerError>;
}
```

The transition methods are thin: read the current clock cell (`None` before
the initial publish applies means `ClockState::initial()`), apply the
matching pure `ClockState` helper, submit one `set_value` write in a batch
attributed to the session owner:

```rust
let mut batch = runtime::ExternalWriteBatch::new(self.session_owner.clone());
batch.set_value(&self.clock_key, next);
self.writes.submit(batch).await?;
```

They are the only clock writers in the system.

`shutdown()` first awaits `RuntimeHandle::shutdown()` — the worker signals
every process, waits for them to stop, and applies final writes before
replying — and then awaits the worker JoinHandle, which is where
worker-level errors and panics surface (a worker that dies mid-run drops
the shutdown reply; only the join holds the real error). Ledger keeps no
per-feed shutdown channels or joins — the worker owns those. The one join
the session owns is the RuntimeWorker task.

No session registry is required in this phase. The CLI creates one session,
runs it, prints a report, and shuts it down. The future remux transport
holds one `LedgerSessionHandle` in its state (one active session, per the
vision).

## Process Payload Types

Live in `feed/es_replay/cells.rs` alongside `EsReplayCells` — these are
process payloads (epochs, feed sequence numbers, clock snapshots), not
market-domain data, so `market/es_mbo.rs` stays pure domain.
`EsReplayStatus` embeds `ClockSnapshot` from `crate::clock`, so no `Eq`
derive on it (f64 speed):

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsMboFeedBatch {
    pub feed_seq: u64,
    pub batch_idx: usize,
    pub ts_event_ns: UnixNanos,
    pub source_first_ts_ns: UnixNanos,
    pub source_last_ts_ns: UnixNanos,
    pub events: Vec<EsMboEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsReplayCursor {
    pub epoch: u64,
    pub feed_seq: u64,
    pub batch_idx: usize,
    pub total_batches: usize,
    pub ts_event_ns: Option<UnixNanos>,
    pub next_ts_event_ns: Option<UnixNanos>,
    pub ended: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EsReplayStatus {
    pub raw_object_id: String,
    pub artifact_object_id: Option<String>,
    pub clock: ClockSnapshot,
    pub cursor: EsReplayCursor,
}
```

Field semantics, exactly:

```text
EsMboFeedBatch.feed_seq       this emission's feed sequence number
EsMboFeedBatch.batch_idx      artifact batch index (position in the
                              event store's batch list)
EsMboFeedBatch.events         the events in the artifact batch span
source_first/last_ts_ns       ts_event_ns of the span's first/last event

EsReplayCursor.epoch          starts at 0; +1 on every regression rebuild
EsReplayCursor.feed_seq       count of emissions so far; MONOTONIC across
                              regressions (never reset — drivers wait for
                              it to advance past their last seen value)
EsReplayCursor.batch_idx      next unemitted artifact batch index; this is
                              what regression truncation resets
EsReplayCursor.total_batches  artifact batch count, constant
EsReplayCursor.ts_event_ns    last emitted batch ts; None before any
EsReplayCursor.next_ts_event_ns  ts of batch at batch_idx; None past end
EsReplayCursor.ended          batch_idx == total_batches

initial cursor: epoch 0, feed_seq 0, batch_idx 0, ts None,
next Some(first batch ts), ended false (true only for an empty artifact)
```

## ES Replay Cells

```rust
pub struct EsReplayCells {
    pub batches: cache::ArrayKey<EsMboFeedBatch>,
    pub cursor: cache::ValueKey<EsReplayCursor>,
    pub status: cache::ValueKey<EsReplayStatus>,
}

impl EsReplayCells {
    /// Registers all three cells under the feed owner. Cloneable: the feed
    /// keeps one clone, the builder hands another to the caller.
    pub fn register(cache: &cache::Cache) -> Result<Self, LedgerError>;
}
```

```text
owner:  feed.databento.es_replay
keys:   feed.databento.es_replay.batches   public read: false
        feed.databento.es_replay.cursor    public read: true
        feed.databento.es_replay.status    public read: true
```

`batches` is the dependency key for components that need the replay MBO
stream. `cursor` and `status` are the public observability surface — the
step driver, CLI, and future transport wake on their watches and read them
to synchronize and report.

## ES Replay Feed Behavior

Internal state built during prepare:

```text
artifact descriptor/path
decoded EsMboEventStore
epoch
next unemitted batch index
feed_seq
```

`RuntimeProcess::run` — the feed owns its pacing; there is no generic
waiting helper in this phase (extract one when a second feed wants it):

```text
1. publish initial status/cursor through one ExternalWriteBatch
2. loop:
   a. read the clock cell (None -> ClockState::initial()) and compute now
   b. if session time regressed below the last emitted batch timestamp:
      rebuild (see Regression), then continue
   c. emit every batch with ts_event_ns <= now not yet emitted, in order,
      one ExternalWriteBatch each, publishing cursor/status with each
   d. decide how to wait:
      all batches emitted -> park on clock watch + shutdown
      paused              -> park on clock watch + shutdown
      running             -> select! {
                               sleep_until(clock.wall_deadline(next_ts)),
                               clock watch changed,
                               shutdown changed,
                             }
3. exit only on shutdown
```

Notes:

```text
the wall deadline targets the feed's own next event exactly — there is no
tick grid and no periodic wakeup; a paused or finished feed is parked
indefinitely at zero cost

catch-up: if a clock write jumps time past several batches, step (c) emits
them all as an ordered burst
```

Regression (backward seek) — feed-owned adaptation, epoch contract for
consumers:

```text
when now < last emitted batch timestamp:
  truncate the batches array to batches with ts_event_ns <= now
    (cache replace_array/remove_array_range — owner-only, already exists)
  reset the internal next-unemitted index to match
  increment epoch
  clear ended if set
  publish cursor/status

consumers of the batches array keep their own progress cursors; when
cursor.epoch changes they reset progress and re-read
```

End behavior:

```text
after emitting the final batch:
  set cursor.ended = true, next_ts_event_ns = None, publish final status
  park on the clock watch — do NOT return
  a later backward seek revives emission (regression path clears ended)
  the process returns only on shutdown, so its terminal ComponentStatus is
  Stopped, not Completed
```

One emitted exchange batch is one `ExternalWriteBatch`:

```rust
let mut batch = ctx.batch();
batch
    .push_array(&self.cells.batches, vec![feed_batch])
    .set_value(&self.cells.cursor, cursor)
    .set_value(&self.cells.status, status);
ctx.submit(batch).await?;
```

The feed never calls cache setters directly and never writes the clock cell
(the cache's ownership enforcement makes the latter a hard error).

## CLI Surface

Extend `crates/cli` (package `ledger-cli`, binary `ledger`): the `Command`
enum gains `Session(SessionCommand)` alongside `Store`/`Es`, following the
existing clap Args/Subcommand pattern. The store is built the same way the
other commands build it (`R2Store::from_env(&cli.data_dir)`), wrapped in an
`Arc` for the session builder. Artifact preparation already exists as
`es prepare`; the session command prepares implicitly through the feed's
lifecycle — which short-circuits to the existing artifact for a prepared
day, so running a ready day does no heavy work beyond hydrate + decode.

```text
ledger session run-es-replay --raw-id <store-object-id> [--batches N] [--realtime] [--speed X]
```

`--batches` defaults to running to the end (`cursor.ended`); `--speed`
implies nothing on its own — it only applies to `--realtime` (default 1.0).
Default mode: deterministic step.

```text
construct R2Store
create Ledger ES replay session (clock paused at 0)
readiness: watch the cursor cell; wait until the feed publishes its initial
  cursor (tokio::time::timeout-bounded; 60s covers a cold prepare's
  hydrate + decode; on timeout, report the feed's component_status)
loop until N batches are emitted or cursor.ended:
  target = cursor.next_ts_event_ns
  seek_to(target)
  progress: await the cursor watch until feed_seq advances past the last
    seen value or cursor.ended becomes true (bounded by a short timeout)
shutdown
print JSON summary
```

Step synchronization rides cell watches, never Drain and never polling. The
chain is direct wakeups end to end: driver writes clock -> worker applies ->
feed's clock watch wakes -> feed emits -> worker applies -> cursor
generation bumps -> driver's cursor watch wakes. Determinism means an
identical emitted batch sequence and identical cell states at each step
boundary.

`--realtime [--speed X]`:

```text
readiness as above, then seek_to the first batch timestamp
set_speed(X), play
await cursor watch until N batches are emitted or cursor.ended
pause/shutdown
print JSON summary
```

Suggested output:

```json
{
  "raw_object_id": "sha256-...",
  "artifact_object_id": "sha256-...",
  "artifact_reused": true,
  "mode": "step",
  "batches_requested": 10,
  "batches_emitted": 10,
  "first_ts_event_ns": "1773235800000000000",
  "last_ts_event_ns": "1773235801000000000",
  "ended": false,
  "epoch": 0,
  "feed_seq": 10
}
```

(All fields come from the final cursor/status cells and the prepare
metadata — there is no cumulative runtime counter to report, and the
driver never calls Drain.)

The first validation can run with zero task components. The point is to
prove:

```text
clock-cell-driven emission works
step runs are deterministic with no sleeps and no polling
backward seek truncates, bumps epoch, and revives an ended feed
runtime accepts feed writes
cache cells reflect cursor/status/latest batch
```

## Transport Surface

Transport work follows CLI validation. The session surface is exposed later
through `ledger-remux` JSON-RPC methods:

```text
remux/ledger/session/start
remux/ledger/session/play
remux/ledger/session/pause
remux/ledger/session/speed
remux/ledger/session/seek
remux/ledger/session/status
remux/ledger/session/stop
```

Handlers never run on feed tasks and never await feeds: clock methods are
one cell write through the session handle, status is cell reads, start/stop
build or shut down the one registered session. Cell watches later enable
push-style status updates instead of transport polling. Module functions
(days, prepare-as-job) are separate methods that need no session at all.

Do not implement these before the headless crate and CLI are validated.

## Error Types

Extend the data-phase `LedgerError`:

```rust
    #[error(transparent)]
    Cache(#[from] cache::CacheError),

    #[error(transparent)]
    Runtime(#[from] runtime::RuntimeError),

    #[error("invalid clock speed `{0}`")]
    InvalidClockSpeed(f64),
```

There is no backward-seek error — regression is legal state. There is no
`FeedError`. Feed processes return `runtime::ComponentError` from `prepare`
and `run`; domain failures map into `ComponentError::Message`. The runtime
surfaces them as `ComponentStatus::Failed` and `RuntimeError::Component`.

## Tests

Fixtures — feed and session tests must never decode real DBN:

```text
store crate gains a `test-util` feature:
  move the in-memory `TestRemote` from crates/store/tests/store.rs into
  crates/store/src/test_util.rs as `MemoryRemote`, exposed via
  `#[cfg(feature = "test-util")] pub mod test_util;`
  store's own tests keep using it through a self dev-dependency:
  [dev-dependencies] store = { path = ".", features = ["test-util"] }
  behavior is a pure move + rename; store tests must pass unchanged

crates/ledger/tests/support/mod.rs:
  store fixture: Store::open(tempdir, config, MemoryRemote) as in the
  store tests
  fabricate_prepared_day(store, events) -> (raw_object_id, artifact
  descriptor):
    register a small dummy file as the raw
      (role Raw, kind RAW_DATABENTO_DBN_ZST_KIND — its bytes are never
      decoded because prepare short-circuits to the artifact)
    build EsMboEventStore { events, batches: build_batches(&events) },
      encode_event_store, write to a temp file, register_file with
      role Artifact, kind ES_MBO_EVENT_STORE_KIND,
      file_name ES_MBO_EVENT_STORE_FILE_NAME, lineage [raw_object_id],
      and metadata_json matching the module's artifact_metadata shape:
      { "artifact": "es_mbo_event_store",
        "version": ES_MBO_EVENT_STORE_VERSION,
        "raw_object_id": "<raw id>", "market_day": "YYYY-MM-DD",
        "event_count": N, "batch_count": M,
        "first_ts_event_ns": "<u64 as string>",
        "last_ts_event_ns": "<u64 as string>" }
    the feed's prepare then takes the reuse path: hydrate + decode only
  synthetic event timestamps are unconstrained (market-day bounds
  validation runs only on the build-from-DBN path); use small nanosecond
  values so seek targets are easy to write in tests
```

Clock unit tests:

```text
with_speed rejects zero, negative, NaN, and infinite speeds
play/pause/with_speed/seek_to re-anchor and increment revision
seek_to accepts backward targets
now_ns is anchor math: paused returns anchor; running extrapolates
wall_deadline returns None when paused, exact instant when running
```

Feed tests:

```text
install_process reports Preparing during artifact prepare, then Running
feed treats a missing clock cell as paused at 0 and emits nothing
feed emits nothing while paused before its data
feed wakes on a clock cell write without polling
feed emits an ordered catch-up burst when a seek jumps past several batches
feed emits batches as session time crosses their timestamps in realtime
feed updates cursor and status after each emission
backward seek truncates batches, resets index, bumps epoch, clears ended
feed parks after the final batch (ended=true) and a backward seek revives it
feed writes are attributed to the feed component owner
feed cannot write the clock cell (cache ownership rejects it)
```

Session and integration tests:

```text
feed-submitted writes enter runtime through the generic external-write path
task component depending on batches runs when the batches key changes
Ledger session does not implement component scheduling policy
RuntimeHandle::shutdown stops the feed process cleanly
session shutdown awaits the worker join and surfaces worker errors
step driver synchronizes on cursor watches, never on Drain, never by polling
```

CLI validation:

```text
cargo run -p ledger-cli -- session run-es-replay --raw-id <id> --batches 10
```

Workspace validation:

```text
cargo fmt --all
cargo test --workspace
```

## Success Criteria

```text
runtime is unchanged; cache and runtime stay free of feeds/clocks/DBN/ES
feeds are runtime processes; ledger keeps no per-feed shutdown or join plumbing
the session owns exactly one join: the RuntimeWorker task
the clock is a session-owned cell; feeds structurally cannot write it
feeds own their pacing; no generic waiting helper exists in this phase
no polling anywhere: feeds and drivers park on cell watches
ES replay feed emits one exchange batch per ExternalWriteBatch
backward seek truncates, bumps epoch, and revives an ended feed
deterministic step runs emit identical batch sequences with no wall sleeps
CLI can run a headless ES replay feed for N batches
cargo test --workspace passes
```

## Future Extensions

```text
seek UI              user-facing controls come with the Lens session surface
shared pacing helper extract from the ES feed when a second feed wants it
session from Days    "open validated day as a Session" action on the Lens
                     Days screen, resolving day -> raw -> artifact
artifact indexing    split event bytes / batch index, mmap, partial loads
simulator and book   later layer consuming ES MBO batches (legacy designs
                     preserved in the data-management spec)
bars and studies     task components over the batches cell
live feed            same feed contract; live driver pins anchors to wall time
session registry     session handles by id behind remux methods
transport push       cell watches let remux push status instead of polling
```
