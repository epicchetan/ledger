# Ledger Feed System Implementation Spec

## Purpose

This spec defines the first implementation of Ledger feeds after the
`cache`/`runtime` split.

The goal is to introduce a new `ledger` crate that owns concrete feed workers,
shared playback controls, ES replay startup preparation, and application
composition around the generic `runtime` crate.

The important boundary:

```text
store
  durable raw files and generated artifacts

cache
  active typed cells

runtime
  applies queued external write batches
  schedules projections from changed keys

ledger
  concrete feeds
  concrete market payloads
  playback controls
  session wiring
  feed-owned startup preparation
```

`runtime` should not learn what replay, play, pause, speed, DBN, CME, ES, or MBO
mean. It should continue to receive `ExternalWriteBatch` values and run
projections from changed cache keys.

## Scope

Implement the first `ledger` crate with:

```text
shared playback state:
  play
  pause
  playback speed

feed worker infrastructure:
  async feed tasks
  runtime-owned write ingress
  shutdown signaling

ES replay feed:
  hydrate a raw Databento DBN object from store
  lazily build or reuse feed-owned replay artifacts
  emit ES MBO exchange batches according to playback state
  write feed state through ExternalWriteBatch

CLI validation:
  run a headless ES replay session from a store raw object
  optionally prepare artifacts only
  print JSON summaries for agentic validation
```

Out of scope for this phase:

```text
execution simulation
order book reconstruction
book validation
Lens chart/session UI
websocket frames
seeking
per-feed command enums
multiple concurrently controlled sessions in API
live market adapters
0DTE feeds
feed checkpoint persistence
memory-mapped artifact reads
```

The previous replay system had useful DBN preparation logic. This phase keeps
the useful part, but moves ownership into the ES replay feed startup path.

## Crate

Add:

```text
crates/ledger
```

Package and library crate name:

```text
ledger
```

Workspace update:

```toml
[workspace]
members = [
    "crates/cache",
    "crates/runtime",
    "crates/store",
    "crates/ledger",
    "crates/api",
    "crates/cli",
]
```

Dependencies:

```toml
[dependencies]
anyhow.workspace = true
async-trait.workspace = true
cache = { path = "../cache" }
runtime = { path = "../runtime" }
serde.workspace = true
serde_json.workspace = true
store = { path = "../store" }
thiserror.workspace = true
tokio.workspace = true
dbn.workspace = true

[dev-dependencies]
tempfile.workspace = true
```

Add workspace dependency:

```toml
dbn = "0.55"
```

## Module Layout

Recommended layout:

```text
crates/ledger/src/
  lib.rs
  error.rs
  playback.rs
  session.rs
  feed/
    mod.rs
    context.rs
    es_replay/
      mod.rs
      artifact.rs
      cells.rs
      codec.rs
      dbn.rs
      feed.rs
  market/
    mod.rs
    es_mbo.rs
```

Test layout:

```text
crates/ledger/tests/
  artifact_codec.rs
  es_replay_prepare.rs
  es_replay_feed.rs
  playback.rs
  session.rs
```

## Core Flow

The headless ES replay session should behave like this:

```text
1. CLI/API asks Ledger to start ES replay for a raw store object id.
2. Ledger creates Cache.
3. Ledger registers ES replay feed cells.
4. Ledger creates Runtime over that Cache.
5. Ledger registers projections later when they exist.
6. Ledger creates shared PlaybackController.
7. Ledger starts runtime scheduling through runtime APIs.
8. Ledger starts EsReplayFeed worker.
9. EsReplayFeed hydrates the raw object through store.
10. EsReplayFeed finds or builds its event-store artifact.
11. EsReplayFeed opens the artifact.
12. Playback starts or remains paused depending on configuration.
13. EsReplayFeed emits ExternalWriteBatch values according to its own pacing.
14. Runtime receives feed writes through its generic external-write path.
15. Runtime applies queued writes, schedules projections, and drains projection work.
```

Ledger does not need to protect projections from overwritten value
cells. Cell semantics should be chosen correctly by the feed and projections.

If a feed wants every emitted exchange batch to be available to projections, it
should write an array cell. If a feed writes a value cell, overwrite behavior is
the intended behavior of that cell.

## Feed Batch Semantics

The current runtime has an internal external-write queue and applies all queued
external writes before running one projection. That remains acceptable for the
feed system.

The runtime does not semantically combine feed batches. It applies queued writes
in FIFO order and records changed keys. The chosen cache primitive determines
what projections see:

```text
Value<T>
  current state
  repeated writes overwrite previous values
  appropriate for status, cursor, latest quote, current mode

Array<T>
  ordered collection
  pushed writes accumulate
  appropriate when projections may need every emitted item
```

For ES replay, delivered exchange batches should use an array:

```text
feed.databento.es_replay.batches
```

Projections that consume this array own their own progress cursor in projection
state. The runtime should not introduce a generic feed cursor abstraction.

The ES replay feed can still publish value cells for current state:

```text
feed.databento.es_replay.cursor
feed.databento.es_replay.status
```

## Playback Model

Playback is shared Ledger session state, not a runtime concept.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlaybackMode {
    Playing,
    Paused,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaybackState {
    pub mode: PlaybackMode,
    pub speed: f64,
    pub revision: u64,
}
```

Rules:

```text
default mode: paused
default speed: 1.0
speed must be finite and greater than 0
every state update increments revision
```

Use `tokio::sync::watch`:

```rust
pub struct PlaybackController {
    tx: watch::Sender<PlaybackState>,
}

#[derive(Clone)]
pub struct PlaybackReceiver {
    rx: watch::Receiver<PlaybackState>,
}
```

Controller methods:

```rust
impl PlaybackController {
    pub fn state(&self) -> PlaybackState;
    pub fn subscribe(&self) -> PlaybackReceiver;
    pub fn play(&self) -> Result<(), LedgerError>;
    pub fn pause(&self) -> Result<(), LedgerError>;
    pub fn set_speed(&self, speed: f64) -> Result<(), LedgerError>;
}
```

Feed behavior:

```text
paused:
  do not emit exchange batches

playing:
  emit the next exchange batch when source-time pacing says it is due

speed changed:
  recompute remaining sleep against the latest playback state

shutdown:
  exit cleanly
```

The first batch after play can emit immediately. Later batches are paced from
the previous emitted source timestamp:

```text
wall_delay = source_timestamp_delta / speed
```

For example:

```text
source delta: 100 ms
speed: 1.0
wall delay: 100 ms

source delta: 100 ms
speed: 10.0
wall delay: 10 ms
```

The feed should wait in a way that can react to playback changes and shutdown.
Use `tokio::select!` around sleep, playback `changed()`, and shutdown.

## Feed Infrastructure

Feeds are Ledger workers. They are not runtime projections and they do not write
the cache directly.

The first generic feed trait can live in `ledger`:

```rust
#[async_trait]
pub trait LedgerFeed<S>: Send + 'static
where
    S: store::RemoteStore + 'static,
{
    fn id(&self) -> &FeedId;

    async fn run(self: Box<Self>, ctx: FeedContext<S>) -> Result<(), FeedError>;
}
```

`FeedId` should be a thin validated id:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeedId(cache::Key);
```

For the first feed:

```text
feed id: feed.databento.es_replay
owner:   CellOwner::new(feed_id.as_str())
```

`FeedContext`:

```rust
pub struct FeedContext<S>
where
    S: store::RemoteStore + 'static,
{
    feed_id: FeedId,
    store: Arc<store::Store<S>>,
    cache: FeedCacheReader,
    writes: runtime::ExternalWriteSink,
    playback: PlaybackReceiver,
    shutdown: ShutdownReceiver,
}
```

Public methods:

```rust
impl<S> FeedContext<S>
where
    S: store::RemoteStore + 'static,
{
    pub fn feed_id(&self) -> &FeedId;
    pub fn owner(&self) -> cache::CellOwner;
    pub fn store(&self) -> &store::Store<S>;
    pub fn playback(&self) -> &PlaybackReceiver;
    pub fn shutdown(&self) -> &ShutdownReceiver;

    pub fn batch(&self) -> runtime::ExternalWriteBatch;

    pub async fn submit(
        &self,
        batch: runtime::ExternalWriteBatch,
    ) -> Result<(), FeedError>;

    pub fn read_value<T>(
        &self,
        key: &cache::ValueKey<T>,
    ) -> Result<Option<T>, FeedError>
    where
        T: Clone + Send + Sync + 'static;

    pub fn read_array<T>(
        &self,
        key: &cache::ArrayKey<T>,
    ) -> Result<Vec<T>, FeedError>
    where
        T: Clone + Send + Sync + 'static;
}
```

`FeedCacheReader` should wrap `Cache` and expose reads only. Do not hand feeds a
raw `Cache` unless an implementation detail makes it unavoidable.

`FeedContext::batch()` creates:

```rust
ExternalWriteBatch::new(self.owner())
```

That keeps `CellOwner` mechanics out of concrete feed code.

## Runtime Boundary

`runtime` owns external-write queueing and projection scheduling. Ledger should
not implement a second scheduler.

The current runtime already has the core synchronous boundary:

```rust
runtime.submit_external_writes(batch);
runtime.run_once();
runtime.run_until_idle(max_steps);
```

If async feed tasks need a send handle, add the generic ingress to the `runtime`
crate, not to `ledger`:

```rust
pub struct RuntimeWorker {
    runtime: Runtime,
    writes: ExternalWriteReceiver,
}

#[derive(Clone)]
pub struct ExternalWriteSink {
    // runtime-owned ingress handle
}
```

Generic behavior:

```text
receive external writes
submit them into Runtime
run the scheduler
publish diagnostics if needed
```

This remains domain-agnostic. The runtime worker should not know about feeds,
playback, ES, DBN, or replay. From runtime's perspective, a feed write is just
an `ExternalWriteBatch`.

For the first CLI validation, it is acceptable to use the existing synchronous
runtime directly if that is simpler. The important boundary is that scheduler
policy belongs to `runtime`, while feed source behavior belongs to `ledger`.

## Session Shape

`ledger` should expose a builder plus a handle.

```rust
pub struct LedgerSessionBuilder<S>
where
    S: store::RemoteStore + 'static,
{
    store: Arc<store::Store<S>>,
    cache: cache::Cache,
    runtime: runtime::Runtime,
    playback: PlaybackController,
    feeds: Vec<Box<dyn LedgerFeed<S>>>,
}
```

Builder responsibilities:

```text
create cache
register feed cells
create runtime
register projections when available
add feeds
start runtime-owned worker or use runtime's synchronous loop
spawn feed tasks
return session handle
```

Handle:

```rust
pub struct LedgerSessionHandle {
    playback: PlaybackController,
    cache: cache::Cache,
    shutdown: ShutdownController,
    join_handles: Vec<JoinHandle<Result<(), LedgerError>>>,
}
```

Handle methods:

```rust
impl LedgerSessionHandle {
    pub fn cache(&self) -> &cache::Cache;
    pub fn playback_state(&self) -> PlaybackState;
    pub fn play(&self) -> Result<(), LedgerError>;
    pub fn pause(&self) -> Result<(), LedgerError>;
    pub fn set_speed(&self, speed: f64) -> Result<(), LedgerError>;
    pub async fn shutdown(self) -> Result<(), LedgerError>;
}
```

No API-level session registry is required in this first crate implementation.
The CLI can create one session, run it, print a report, then shut it down.

## ES MBO Market Types

Add the first concrete market payloads inside `ledger`, not `cache` or
`runtime`.

```rust
pub type UnixNanos = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceTicks(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookAction {
    Add,
    Modify,
    Cancel,
    Clear,
    Trade,
    Fill,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsMboEvent {
    pub ts_event_ns: UnixNanos,
    pub ts_recv_ns: UnixNanos,
    pub sequence: u64,
    pub action: BookAction,
    pub side: Option<BookSide>,
    pub price_ticks: Option<PriceTicks>,
    pub size: u32,
    pub order_id: u64,
    pub flags: u8,
    pub is_last: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsMboBatchSpan {
    pub start_idx: usize,
    pub end_idx: usize,
    pub ts_event_ns: UnixNanos,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsMboEventStore {
    pub events: Vec<EsMboEvent>,
    pub batches: Vec<EsMboBatchSpan>,
}

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
    pub playback: PlaybackState,
    pub cursor: EsReplayCursor,
}
```

This is intentionally not a revived `domain` crate. These are concrete Ledger
feed payloads.

## ES Replay Cells

Add a helper that registers the cells used by the first feed.

```rust
pub struct EsReplayCells {
    pub batches: cache::ArrayKey<EsMboFeedBatch>,
    pub cursor: cache::ValueKey<EsReplayCursor>,
    pub status: cache::ValueKey<EsReplayStatus>,
}
```

Cell descriptors:

```text
owner:
  feed.databento.es_replay

keys:
  feed.databento.es_replay.batches
  feed.databento.es_replay.cursor
  feed.databento.es_replay.status
```

Recommended public-read flags:

```text
batches: false
cursor: true
status: true
```

`batches` is the dependency key for projections that need the replay MBO stream.
Those projections should keep their own consumed index if they only want newly
emitted batches.

`cursor` and `status` are useful for CLI/API visibility.

## ES Replay Artifact

The old implementation wrote separate artifacts:

```text
events.v1.bin
batches.v1.bin
trades.v1.bin
book_check.v1.json
```

The new first version should write one feed-owned artifact:

```text
kind:      ledger.es_mbo_event_store.v1
role:      artifact
file_name: es-mbo-event-store.v1.bin
lineage:   [<raw object id>]
```

This artifact contains:

```text
normalized ES MBO events
exchange batch spans
source metadata in descriptor metadata_json
```

Do not include:

```text
trade index
book check
execution state
visibility frames
projection outputs
```

Those can return behind later feeds/projections/simulators.

Artifact metadata:

```json
{
  "artifact": "es_mbo_event_store",
  "version": 1,
  "raw_object_id": "sha256-...",
  "event_count": 123,
  "batch_count": 45,
  "first_ts_event_ns": "1773235800000000000",
  "last_ts_event_ns": "1773239400000000000"
}
```

Store lookup:

```text
list role=artifact kind=ledger.es_mbo_event_store.v1
filter descriptors where lineage contains raw object id
filter metadata_json.version == 1
use the descriptor if present
otherwise build artifact
```

If multiple matching descriptors exist, pick the newest valid descriptor by
`updated_at_ns` and log/report the ambiguity in the prepare summary. This should
not normally happen because the artifact bytes should be deterministic for the
same raw DBN.

## Artifact Codec

Use a fixed little-endian binary codec. Do not add `bincode` or a schema system.

Magic:

```text
LEDGER_ES_MBO_EVENT_STORE_V1
```

Suggested layout:

```text
u32 magic_len
[u8; magic_len] magic
u32 version
u64 event_count
u64 batch_count

events:
  repeated event_count times:
    u64 ts_event_ns
    u64 ts_recv_ns
    u64 sequence
    u8  action
    u8  side
    i64 price_ticks_or_i64_max
    u32 size
    u64 order_id
    u8  flags
    u8  is_last

batches:
  repeated batch_count times:
    u64 start_idx
    u64 end_idx
    u64 ts_event_ns
```

Decode validation:

```text
magic matches
version matches
all batch spans are in bounds
start_idx <= end_idx
batches are non-overlapping and ordered
no trailing corrupt record data
```

Keep the codec colocated with the ES replay feed. It is not a generic store
codec.

## DBN Preparation

`EsReplayFeed` startup calls an artifact preparation helper:

```rust
pub async fn prepare_es_replay_artifact<S>(
    store: &store::Store<S>,
    raw_object_id: &store::StoreObjectId,
) -> Result<EsReplayArtifactDescriptor, LedgerError>
where
    S: store::RemoteStore + 'static;
```

Preparation behavior:

```text
1. find matching artifact descriptor
2. if found, hydrate artifact and return it
3. hydrate raw DBN object
4. decode raw DBN MBO records
5. normalize into EsMboEvent
6. build exchange batch spans from is_last
7. encode EsMboEventStore artifact
8. register artifact through store with role=artifact
9. hydrate or use local artifact path
10. return descriptor/path/summary
```

Databento normalization should port the old useful logic:

```text
MboMsg action:
  A -> Add
  M -> Modify
  C -> Cancel
  R -> Clear
  T -> Trade
  F -> Fill
  N -> None

MboMsg side:
  B -> Bid
  A -> Ask
  N -> None

price:
  UNDEF_PRICE -> None
  otherwise require ES fixed price alignment to 0.25 tick
```

Constants:

```rust
pub const ES_TICK_SIZE_FIXED_PRICE: i64 = 250_000_000;
```

Batch building:

```text
start a batch at current start index
close the batch when event.is_last is true
if EOF leaves a partial batch, close it as a trailing batch
batch timestamp is the closing event timestamp
```

The preparation step should be idempotent. Running it twice for the same raw
object should reuse the existing artifact unless the artifact is missing or
invalid.

## ES Replay Feed

Constructor shape:

```rust
pub struct EsReplayFeed {
    id: FeedId,
    raw_object_id: store::StoreObjectId,
    cells: EsReplayCells,
    initial_playback: PlaybackState,
}
```

Runtime state:

```text
artifact descriptor/path
decoded EsMboEventStore
current batch_idx
feed_seq
last emitted source timestamp
ended flag
```

Run behavior:

```text
1. prepare/open artifact
2. decode artifact into EsMboEventStore
3. publish initial status/cursor through one ExternalWriteBatch
4. wait for playback to be playing
5. emit next exchange batch when due
6. after each emitted batch, push to batches and publish cursor/status
7. stop when ended or shutdown is requested
```

One emitted exchange batch can be represented as one `ExternalWriteBatch`:

```rust
let mut batch = ctx.batch();
batch
    .push_array(&self.cells.batches, vec![feed_batch])
    .set_value(&self.cells.cursor, cursor)
    .set_value(&self.cells.status, status);
ctx.submit(batch).await?;
```

End behavior:

```text
set cursor.ended = true
set next_ts_event_ns = None
publish final status
return Ok
```

The feed should not call `cache.set_value` directly.

## CLI Surface

Extend `crates/cli` with a `session` command group that uses `ledger`.

Commands:

```text
ledger session prepare-es-replay --raw-id <store-object-id>
ledger session run-es-replay --raw-id <store-object-id> [--batches N] [--speed X]
```

`prepare-es-replay`:

```text
construct R2Store
call prepare_es_replay_artifact
print JSON summary
```

`run-es-replay`:

```text
construct R2Store
create Ledger ES replay session
set playback speed
play
wait until N batches are emitted or feed ends
pause/shutdown
print JSON summary
```

Suggested output:

```json
{
  "raw_object_id": "sha256-...",
  "artifact_object_id": "sha256-...",
  "artifact_reused": true,
  "batches_requested": 10,
  "batches_emitted": 10,
  "first_ts_event_ns": "1773235800000000000",
  "last_ts_event_ns": "1773235801000000000",
  "ended": false,
  "runtime_batches_applied": 10,
  "runtime_projections_run": 0
}
```

The first validation can run with zero projections. The point is to prove:

```text
store hydration works
artifact preparation works
feed playback emits batches
runtime accepts feed writes
cache cells reflect cursor/status/latest batch
```

## API Surface

API work can follow after CLI validation. The crate should be shaped so API can
later expose:

```text
POST /sessions/es-replay
POST /sessions/:id/play
POST /sessions/:id/pause
POST /sessions/:id/speed
GET  /sessions/:id/status
DELETE /sessions/:id
```

Do not implement these before the headless crate and CLI are validated unless
the next task explicitly asks for API integration.

## Error Types

`ledger` should have a crate error that preserves source context:

```rust
#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error(transparent)]
    Store(#[from] anyhow::Error),

    #[error(transparent)]
    Cache(#[from] cache::CacheError),

    #[error(transparent)]
    Runtime(#[from] runtime::RuntimeError),

    #[error("feed `{id}` failed: {source}")]
    Feed {
        id: FeedId,
        source: FeedError,
    },

    #[error("invalid playback speed `{0}`")]
    InvalidPlaybackSpeed(f64),

    #[error("run limit exceeded while processing feed batch")]
    RuntimeRunLimitExceeded,
}
```

`FeedError`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    #[error(transparent)]
    Store(#[from] anyhow::Error),

    #[error(transparent)]
    Cache(#[from] cache::CacheError),

    #[error("runtime external write ingress closed")]
    RuntimeIngressClosed,

    #[error("invalid DBN record: {0}")]
    InvalidDbnRecord(String),

    #[error("invalid artifact: {0}")]
    InvalidArtifact(String),
}
```

The exact variants can change during implementation. The important part is that
feed errors include enough context to debug artifact and source issues.

## Tests

Unit tests:

```text
playback rejects zero, negative, NaN, and infinite speeds
playback increments revision on play/pause/speed changes
artifact codec round trips synthetic EsMboEventStore
artifact codec rejects wrong magic
artifact codec rejects out-of-bounds batch spans
batch builder closes trailing partial batch
DBN normalization maps action/side/undefined price correctly where testable
```

Feed tests:

```text
prepare reuses existing artifact for same raw lineage
prepare registers artifact with role=artifact and kind=ledger.es_mbo_event_store.v1
feed starts paused and emits no batches
feed emits first batch after play
feed updates cursor and status after each batch
feed marks ended at the end of artifact
feed submit uses feed owner
```

Runtime integration tests:

```text
feed-submitted writes enter runtime through the generic external-write path
projection depending on batches runs when the batches key changes
Ledger session does not implement projection scheduling policy
shutdown stops feed tasks cleanly
```

CLI validation:

```text
cargo run -p cli -- session prepare-es-replay --raw-id <id>
cargo run -p cli -- session run-es-replay --raw-id <id> --batches 10 --speed 100
```

Workspace validation:

```text
cargo fmt --all
cargo test --workspace
cargo run -p cli -- store list --role raw --kind databento.dbn.zst
```

## Success Criteria

This phase is complete when:

```text
crates/ledger exists and builds
runtime remains generic and does not mention feeds/playback/DBN/ES
cache remains generic and does not mention feeds/playback/DBN/ES
ES replay feed can prepare or reuse an artifact from a raw store object
ES replay feed emits one exchange batch per ExternalWriteBatch
feed writes enter runtime through the generic external-write path
play/pause/speed affect feed emission without changing runtime
CLI can prepare an ES replay artifact
CLI can run a headless ES replay feed for N batches
cargo test --workspace passes
```

## Future Extensions

These should not be built in the first pass, but the design should leave room
for them:

```text
seek:
  add target timestamp to PlaybackState or add a session-level command path
  feed maps timestamp to batch_idx using artifact batch spans

step:
  feed emits exactly one batch while otherwise paused

artifact indexing:
  split event bytes and batch index
  mmap event store
  avoid loading full day into memory

execution simulator:
  separate feed/projection/simulator layer that consumes ES MBO batches

live feed:
  same runtime write path
  different source loop and no replay artifact

API session registry:
  store session handles by id
  route playback controls to PlaybackController

Lens:
  use session status and future projection outputs
  do not make Data Center responsible for preparing replay
```
