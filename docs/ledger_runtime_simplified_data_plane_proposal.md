# Ledger Runtime - External Feed Typed KV Data Plane Proposal

This proposal is about replacing Ledger's active replay/session/projection
runtime. It is not a rewrite of the Data Center product.

Keep the existing data ownership system:

```text
SQLite catalog
R2/object-store integration
market-day status
raw-data ingest
DBN preprocessing
batch/trade replay artifacts
book-check validation
job tracking
cache/staging/delete controls
Data Center API routes
Lens Data Center page
```

The major runtime idea:

```text
Feeds are outside the runtime.
The runtime accepts origin write requests from feeds.
The data plane is a typed concurrent KV store.
Projections own data-plane keys.
Some projection-owned keys are readable by Lens and/or streamed to Lens.
The FrameBus is a separate task that reads allowed data-plane keys.
```

This keeps replay, live feeds, runtime compute, and Lens delivery separated
without forcing a heavy graph runtime.

## 1. Target Shape

The first useful result should be:

```text
Data Center prepares local ES MBO data.
An external replay feed opens that data.
The replay feed owns play/pause/speed/latency and emits origin write requests.
The runtime commits those origin writes into the data plane.
The scheduler marks projections dirty when their inputs change.
candles_1m writes projection-owned bar state into the data plane.
FrameBus streams selected changed keys to Lens.
Lens can later request reads against allowed data-plane keys.
```

No seek, live mode, execution simulation, visibility simulation, journal context,
durable runtime history, or hot order-book runtime is required for this first
version.

This shape is friendly to live mode later:

```text
Replay feed -> OriginWriteRequest
Live feed   -> OriginWriteRequest
```

The runtime does not need to know whether an origin is replay or live as long as
the feed emits the same origin write contract.

## 2. System Boundaries

There are three separate systems:

```text
Feed / replay system
  owns timing, controls, source reading, and batch release

Runtime system
  owns data-plane writes, dependency scheduling, and projection execution

FrameBus / Gateway
  owns Lens output cadence, websocket delivery, and read requests
```

Conceptually:

```text
Lens controls
  -> ReplayFeedController
  -> feed emits origin write requests
  -> Runtime commits origin writes
  -> Scheduler runs projections
  -> DataPlane stores projection-owned keys
  -> FrameBus streams stream_to_lens keys and serves readable_by_lens queries
  -> Lens renders and can query
```

The runtime is not a playback controller. It is an ingestion and projection
processor. The FrameBus is not the scheduler. It is a separate read-side task
that reads public data-plane keys.

## 3. Concurrency Model

The runtime and feeds can run concurrently.

```text
ReplayFeedController task/thread
  owns replay clock and source cursor
  sends origin write requests to Runtime

Runtime task/thread
  owns scheduler state
  writes the DataPlane
  runs projections

FrameBus/Gateway task/thread
  reads allowed DataPlane keys
  sends websocket frames
  handles future Lens read requests
```

The data plane is shared, but it is not protected by one giant lock. It should be
a typed KV store with per-key locking:

```text
origin.es_mbo.batches          has its own lock
origin.es_mbo.status           has its own lock
projection.candles_1m.bars     has its own lock
runtime.error                  has its own lock
```

This allows the FrameBus to read `projection.candles_1m.bars` while the runtime
is appending to `origin.es_mbo.batches`, because those are different keys.

Concurrency rules:

```text
External feeds do not mutate the DataPlane directly.
Runtime scheduler is the main writer for origin/projection keys.
FrameBus streaming may read keys marked stream_to_lens.
FrameBus client read requests may read keys marked readable_by_lens.
Projection compute should not hold locks while doing heavy work.
Projection commits should lock affected keys in stable sorted order.
Streamed keys should be self-contained enough for Lens to render them directly.
```

Per-key locks give enough concurrency for the MVP without requiring a complex
snapshot system.

## 4. Typed KV Data Plane

The data plane is a typed concurrent KV store.

```rust
pub struct DataPlane {
    keys: RwLock<HashMap<Key, Arc<KeyCell>>>,
}

pub struct KeyCell {
    descriptor: KeyDescriptor,
    value: RwLock<KeyValue>,
}
```

The descriptor defines ownership, schema, read access, and streaming behavior:

```rust
pub struct KeyDescriptor {
    key: Key,
    owner: KeyOwner,
    layer: KeyLayer,
    value_kind: ValueKind,
    schema: SchemaId,
    readable_by_lens: bool,
    stream_to_lens: bool,
}
```

The value can be a latest-value slot or an append stream:

```rust
pub enum KeyValue {
    Value(RuntimeValue),
    AppendStream(AppendStream),
}
```

The data plane API should stay small:

```text
describe(key)
read_value(key)
read_stream_since(key, cursor)
read_stream_range(key, range)
commit_origin_writes(origin, write_batch)
commit_projection_writes(projection, write_batch)
commit_runtime_writes(write_batch)
```

Raw value writes and stream appends should be internal commit primitives, not
public mutation paths. External feeds use `commit_origin_writes`. Projections use
`commit_projection_writes`. Scheduler/runtime diagnostics use
`commit_runtime_writes`. That keeps ownership validation and `WriteEffects`
consistent for every mutation.

Every mutation returns effects:

```rust
pub struct WriteEffects {
    changed_keys: Vec<Key>,
    changed_stream_keys: Vec<Key>,
}
```

`changed_keys` is for dependency scheduling.

`changed_stream_keys` is for FrameBus/Gateway delivery. It is the subset of
changed keys whose descriptor has `stream_to_lens = true`.

The data plane does not know projection dependency rules. It only stores data,
validates ownership, applies writes, and reports what changed.

## 5. Projection-Owned Keys

There should not be a mandatory split between "state keys" and "Lens output
keys."

A projection owns keys. The projection decides what it stores.

Some owned keys can be marked:

```text
readable_by_lens:
  Lens may request this key on demand.

stream_to_lens:
  FrameBus should push this key when it changes.
```

For the first candles projection, one key may be enough:

```text
projection.candles_1m.bars
  owner: projection.candles_1m
  value_kind: value
  schema: candles_1m_bars.v1
  readable_by_lens: true
  stream_to_lens: true
```

The value can contain both historical and live state:

```json
{
  "completed": [],
  "live": {
    "start_ts_ns": "1760000000000000000",
    "open": 5123.25,
    "high": 5124.0,
    "low": 5122.75,
    "close": 5123.5,
    "volume": 143,
    "final": false
  }
}
```

If that becomes too large later, the projection can split storage by access
pattern:

```text
projection.candles_1m.live
  readable_by_lens: true
  stream_to_lens: true

projection.candles_1m.completed
  readable_by_lens: true
  stream_to_lens: false or true depending on UI need
  value_kind: append_stream
```

That split should be a projection design choice, not a runtime rule.

## 6. Origin Write Requests

Everything entering the runtime from an external feed should cross one simple
boundary: an origin write request.

```rust
pub enum RuntimeAction {
    OriginWrite(OriginWriteRequest),
    Close,
}

pub struct OriginWriteRequest {
    origin: OriginId,
    writes: Vec<DataPlaneWrite>,
}

pub enum DataPlaneWrite {
    Set {
        key: Key,
        value: RuntimeValue,
    },
    Append {
        key: Key,
        records: Vec<RuntimeRecord>,
    },
}
```

This replaces separate runtime concepts like "ingest origin batches" and
"origin status update." Batches, feed status, replay clock, source metadata, and
latency state are all just writes to origin-owned data-plane keys.

Example:

```text
OriginWriteRequest(origin.es_mbo)
  Append origin.es_mbo.batches with due source batches
  Set origin.es_mbo.status to latest playback/source state
```

The runtime does not need to understand what the status means. It only needs to
validate and commit the write request.

Validation rules:

```text
the origin id must be registered
every written key must be owned by that origin
MVP origin keys should live under origin.<id>.*
the write kind must match the key descriptor's value_kind
the value must match the key descriptor's schema
```

No playback controls are runtime actions:

```text
Play/Pause/Speed/Latency -> feed/replay controller
Feed output              -> OriginWriteRequest
```

If a feed is paused, no new origin write requests arrive. After pending work
drains, the runtime goes idle.

FrameBus requests are separate from runtime actions. They are read-side gateway
work against the data plane.

## 7. Keyed Coalescing Work Queue

The scheduler owns a keyed coalescing work queue.

External feeds run on separate threads/tasks and send origin write requests.
They do not have the data plane. When those requests enter the runtime, the
scheduler should not blindly append every request as a new FIFO item. It should
fold origin writes by origin and by key.

The queue contains keys for work that must happen. The payload for that work is
accumulated by key. A key appears in the queue at most once.

```rust
enum WorkKey {
    ApplyOriginWrites(OriginId),
    RunProjection(ProjectionId),
    Close,
}
```

Conceptually:

```text
RuntimeScheduler
  origin_write_order: VecDeque<OriginId>
  projection_order_by_layer: layer -> VecDeque<ProjectionId>

  queued_origin_writes: set<OriginId>
  queued_projections: set<ProjectionId>

  pending_origin_writes: OriginId -> CoalescedOriginWrites

  dependency_index: Key -> ProjectionId list
```

`CoalescedOriginWrites` should preserve append data and collapse latest-value
data:

```text
CoalescedOriginWrites
  appends: Key -> Vec<RuntimeRecord>
  sets: Key -> RuntimeValue
```

Add-time behavior:

```text
add OriginWriteRequest(origin, writes):
  validate every write belongs to origin
  for write in writes:
    if write is Append:
      pending_origin_writes[origin].appends[key].extend(records)
    if write is Set:
      pending_origin_writes[origin].sets[key] = value
  if origin not in queued_origin_writes:
    queued_origin_writes.insert(origin)
    origin_write_order.push_back(origin)

mark_projection_dirty(projection):
  if projection not in queued_projections:
    queued_projections.insert(projection)
    projection_order_by_layer[projection.layer].push_back(projection)
```

Duplicate projection dirties are cheap no-ops. More source records for an
origin append into the existing pending buffer instead of creating more queue
entries. Repeated origin status/state writes collapse to the latest value for
that key.

The scheduler, not the data plane, owns dependencies:

```text
DataPlane says:
  changed_keys = [origin.es_mbo.batches]

Scheduler decides:
  origin.es_mbo.batches wakes candles_1m
  mark_projection_dirty(candles_1m)
```

Origin write lanes should normally run before projection lanes, but not as an
unbounded drain. A scheduler turn should process a bounded snapshot of currently
queued origin write keys, then allow dirty projections to run by hierarchy
layer. New origin writes that arrive during that turn remain queued and continue
coalescing, but they do not starve already-dirty projections.

A simple MVP fairness rule:

```text
1. take origin_write_order.len() at the start of the turn
2. process at most that many ApplyOriginWrites keys
3. run eligible RunProjection keys by layer
4. repeat
```

This still applies source records before projection compute in the common case,
while preventing a fast replay feed from keeping `candles_1m` permanently dirty
but never executed.

When no keyed work or pending buffers exist, the runtime scheduler is idle.

## 8. Keyed Work Execution

Lane 1 processes origin write work keys:

```text
Process WorkKey::ApplyOriginWrites(origin.es_mbo):
  remove origin.es_mbo from queued_origin_writes
  take pending_origin_writes[origin.es_mbo]
  DataPlane.commit_origin_writes(origin.es_mbo, coalesced_writes)
  receive WriteEffects
  dependency lookup marks projections dirty from changed_keys
  notify FrameBus/Gateway about changed_stream_keys
```

For example, one coalesced commit may:

```text
Append origin.es_mbo.batches with source records 100..140
Set origin.es_mbo.status to the latest feed status snapshot
return changed_keys = [origin.es_mbo.batches, origin.es_mbo.status]
```

The scheduler does not need hard-coded behavior for status, replay clocks, or
batches. Those are just origin-owned keys. For MVP, most origin keys should not
be `stream_to_lens`; if an origin key is marked `stream_to_lens`, the same
`changed_stream_keys` notification path applies.

Lane 2 processes projection work keys by hierarchy layer:

```text
Process WorkKey::RunProjection(candles_1m):
  remove candles_1m from queued_projections
  read/copy needed inputs from DataPlane
  release locks
  run candles_1m
  receive ProjectionWriteBatch
  DataPlane.commit_projection_writes(candles_1m, write_batch)
  receive WriteEffects with changed keys and changed stream keys
  mark downstream projections dirty from changed_keys
  notify FrameBus/Gateway about changed_stream_keys
```

The scheduler's critical rule:

```text
Every data-plane write returns WriteEffects.
Every WriteEffects.changed_keys pass through dependency lookup.
Dependency lookup marks projections dirty.
Dirty projection sets prevent duplicate projection runs.
```

## 9. Add-Time Coalescing

The scheduler should not assume the runtime is always caught up.

When compatible work arrives, it should be grouped immediately rather than
queued repeatedly.

Example incoming actions:

```text
OriginWriteRequest append origin.es_mbo.batches batch_100
OriginWriteRequest append origin.es_mbo.batches batch_101
OriginWriteRequest append origin.es_mbo.batches batch_102
OriginWriteRequest set origin.es_mbo.status replay_time=10:00:01.000
OriginWriteRequest set origin.es_mbo.status replay_time=10:00:01.016
```

Bad behavior:

```text
append batch_100 -> run candles_1m
append batch_101 -> run candles_1m
append batch_102 -> run candles_1m
```

Preferred behavior:

```text
append batches 100..102 from one pending origin buffer
mark candles_1m dirty once
run candles_1m once over all new stream entries
```

Algorithmically:

```text
schedule OriginWriteRequest(origin, writes):
  for Append(key, records):
    pending_origin_writes[origin].appends[key].extend(records)
  for Set(key, value):
    pending_origin_writes[origin].sets[key] = value
  push ApplyOriginWrites(origin) only if not already queued

schedule projection dirty(projection):
  push RunProjection(projection) only if not already queued
```

Coalescing rules:

```text
append writes are never dropped
append writes preserve feed order per origin and per key
set writes collapse to the latest value for the same origin/key
origin write requests collapse to one queued ApplyOriginWrites key per origin
projection dirty requests collapse to one queued projection key
Close is a barrier and should not be merged past
```

`Set` writes are latest-state writes. They should be used for values like
current feed status, latest replay clock, or source metadata. If a value must be
replayed in exact sequence with source batches, model it as an append stream or
include it in the appended batch records instead of sending it as a coalesced
`Set`.

The critical replay policy:

```text
Preserve every source batch.
Compute projections over every source batch.
Allow Lens to skip intermediate visual states.
```

## 10. Projection Execution Contract

A projection gets a read context:

```text
read_value(key)
read_stream_since(stream_key, cursor)
current_offset(stream_key)
cursor_for(stream_key)
```

It returns a write batch:

```text
write_value(key, value)
append_stream(key, entries)
advance_cursor(stream_key, offset)
register_or_update_key_descriptor(key, descriptor)
```

The data plane commits origin write requests, projection write batches, and
runtime diagnostic writes:

```text
validate ownership
validate hierarchy rules
lock affected keys in stable sorted order
apply value writes
apply stream appends
advance projection cursors
return WriteEffects
```

If a projection fails, no partial writes should be applied. For MVP, write
`runtime.error` through `commit_runtime_writes` and stop scheduling new
projection work until the runtime is reopened or reset.

Projection cursors should live in the data plane because cursor advancement must
commit with projection writes. The scheduler knows which streams a projection
declares, but cursor values are committed runtime state.

## 11. FrameBus / Gateway

FrameBus is a separate task from the runtime scheduler.

It should not own projection computation. It should not mutate origin or
projection state. It can read data-plane keys whose descriptors permit Lens
access.

Responsibilities:

```text
track websocket subscribers
track changed stream_to_lens keys
coalesce output delivery to the UI cadence
read latest values from the data plane using per-key read locks
send runtime_frame messages to Lens
later: handle client read requests against readable_by_lens keys
```

Runtime data-plane commits return `changed_stream_keys` whenever a changed key
is marked `stream_to_lens`. For the MVP, projection commits are the expected
source of streamed keys, but origin and runtime diagnostic commits can use the
same path if their descriptors opt in. The runtime can notify FrameBus over a
channel:

```text
Runtime -> FrameBus:
  ChangedStreamKeys([projection.candles_1m.bars])
```

FrameBus keeps its own dirty set:

```text
FrameBus
  dirty_stream_keys: ordered set<Key>
  flush_pending: bool
  subscriber_cursors: SubscriberId -> OutputCursor
```

FrameBus behavior:

```text
on ChangedStreamKeys(keys):
  dirty_stream_keys.extend(keys)
  schedule at most one flush for the next 60 fps slot

on flush:
  for key in dirty_stream_keys:
    if descriptor.stream_to_lens:
      read latest value under that key's read lock
      clone payload or Arc payload
  clear delivered dirty keys
  send one coalesced frame to websocket tasks
```

If `dirty_stream_keys` is empty, FrameBus does nothing. A paused feed therefore
does not force runtime or FrameBus work unless some other output changes.

Future Lens read requests can use the same gateway:

```json
{
  "type": "read_key",
  "key": "projection.candles_1m.bars"
}
```

For local use, this does not need a large security model. It does need basic
mechanics:

```text
key must exist
key descriptor must have readable_by_lens = true
stream reads must be cursor/range bounded
large reads should have limits
read locks should be held only long enough to clone/copy the response payload
```

## 12. Locking And Consistency

Per-key locks are enough for the first runtime, but the rules need to be clear.

```text
Reads:
  acquire read lock for one key
  clone/copy the value or requested stream slice
  release lock before websocket send or heavy work

Writes:
  compute outside locks where possible
  acquire write locks for affected keys
  use stable sorted key order for multi-key commits
  apply writes
  release locks quickly
```

For MVP, consistency is key-level:

```text
FrameBus reads one self-contained streamed key and gets a consistent value for
that key.
```

If a client reads several keys at once, it may not get a single atomic snapshot
across all keys unless a future snapshot/epoch mechanism is added. That is fine
for the first local runtime.

This is why streamed keys should be self-contained for their UI job. For
`candles_1m`, `projection.candles_1m.bars` can include the live candle and
whatever completed bars Lens needs for the current view.

## 13. Codebase Impact

This section is audited against the current repo shape.

Keep:

| Area | Decision |
| --- | --- |
| `crates/domain` | Keep shared pure domain types. Add runtime key/action/frame/candle types if useful. |
| `crates/book` | Keep. It is used by ingest book-check artifact generation and by `ledger-replay`; it also remains the basis for Data Center validation. Do not use it in the first hot `candles_1m` runtime. |
| `crates/store` | Keep SQLite, R2/object storage, staging, jobs, status, delete behavior, and the active replay artifact cache contract unless intentionally replaced. |
| `crates/ingest` | Keep Databento/raw DBN ingestion, DBN preprocessing, normalized MBO artifacts, batch/trade indexes, and book-check artifact generation. |
| `crates/ledger` | Keep as the application core for Data Center lifecycle, validation composition, ingest orchestration, deletion, artifact hydration, and the new runtime boundary. Replace only the active `Session` runtime portion. |
| `crates/api` Data Center routes | Keep lifecycle routes. Migrate `/sessions/ws` because it is active runtime transport, not a Data Center route. |
| `crates/cli` data commands | Keep status/prepare/validate/data commands. Session/projection replay commands need a compatibility decision. |
| Lens Data Center page | Keep. Migrate the Open Replay/runtime entry point to the new runtime socket/feed controller. |

Replace or defer in the active runtime path:

| Area | Decision |
| --- | --- |
| current `Session` runtime | Replace with a runtime that commits external origin write requests and schedules projections. |
| current projection graph/runtime | Replace with scheduler-owned dependency index, keyed coalescing work queue, and projection write batches. |
| current `Session` use of `ledger-replay::ReplayFeed`/`ReplaySimulator` | Move replay playback outside the runtime. The replay/feed layer emits origin write requests into the runtime. |
| execution simulation | Defer for MVP. Existing execution APIs/tests must be removed, hidden, or preserved separately until intentionally replaced. |
| visibility model | Defer for MVP. Feed latency simulation in the external replay feed is enough for first candle playback. |
| seek/live mode | Defer for MVP. Existing seek commands need a migration or temporary removal decision. |
| history/context/journal | Defer. |

Important dependency caveat:

```text
Do not delete crates/replay as an early cleanup.
```

Today `ledger` has a normal dependency on `ledger-replay`; validation uses
`ReplaySimulator` probes; active sessions use `ReplayFeed`; API session
websocket tests and protocol code depend on replay/session types; CLI
session/projection commands go through the current session runtime. Removing the
crate requires replacing those surfaces, not just removing a workspace member.

There are no crate-level feature flags today. Dependencies are mostly
unconditional, so runtime replacement should be staged carefully:

```text
1. Preserve Data Center and validation.
2. Introduce the external replay/feed controller and new runtime beside the current session path.
3. Migrate Lens runtime opening and `/sessions/ws`.
4. Migrate or remove CLI session/projection commands.
5. Remove old session/projection/replay code only after preserved behavior is accounted for.
```

## 14. External Feed And Replay Layer

The replay feed is outside the runtime.

For MVP:

```text
ReplayFeedController
  opens prepared ES MBO data
  owns play/pause/speed
  owns simulated feed latency
  owns replay clock
  owns source cursor
  emits origin write requests
```

Clock concepts:

```text
wall clock:
  real local time used by async timers

replay clock:
  feed-owned replay time that advances while feed playback is running

source time:
  timestamp from prepared ES MBO data

delivery time:
  source time plus simulated feed latency
```

Feed invariant:

```text
Emit all batches whose delivery_time <= replay_time.
Preserve source/feed order.
Send batches in groups.
Do not mutate the runtime data plane directly.
```

The feed sends origin writes:

```text
RuntimeAction::OriginWrite(OriginWriteRequest {
  origin: origin.es_mbo,
  writes: [
    Append key=origin.es_mbo.batches records=batches
  ]
})
```

It may include status snapshots in the same request or in separate requests:

```text
RuntimeAction::OriginWrite(OriginWriteRequest {
  origin: origin.es_mbo,
  writes: [
    Set key=origin.es_mbo.status value=playing / paused / ended / source metadata / replay_time
  ]
})
```

Status writes are optional for projections. They are useful for frame envelopes
and UI state, but they should not become the way the runtime controls playback.

The important part is that both examples use the same runtime boundary:

```text
origin-owned write request in
typed data-plane effects out
```

## 15. Origin Keys

The first data-plane origin keys can be:

```text
origin.es_mbo.status
origin.es_mbo.batches
```

The first runtime diagnostic key can be:

```text
runtime.error
  owner: runtime
  layer: runtime diagnostics
```

`runtime.error` is not an origin key. The scheduler/runtime owns it and writes it
through the runtime diagnostic commit path.

Default Lens access for origin keys should be conservative:

```text
origin.es_mbo.batches
  readable_by_lens: false
  stream_to_lens: false

origin.es_mbo.status
  readable_by_lens: true if Lens needs it
  stream_to_lens: false by default
```

The old idea of `runtime.playback`, `runtime.speed`, and
`runtime.feed_latency_ms` as runtime-owned controls should be removed from the
runtime core. Those are feed/replay-controller concerns.

If Lens needs to display current playback state, the feed can publish it as
origin status:

```text
origin.es_mbo.status
  playback: paused
  replay_time_ns: ...
  speed: ...
  feed_latency_ms: ...
```

That status is ingested data from the source controller, not a command surface
owned by the runtime.

The runtime should treat these as ordinary origin-owned keys:

```text
Append origin.es_mbo.batches wakes projections that depend on that stream.
Set origin.es_mbo.status only wakes projections that explicitly depend on status.
```

## 16. Scheduler-Owned Dependencies

Projection dependencies live in the scheduler, derived from projection
declarations.

Projection declaration:

```text
projection id
layer
dirty inputs
context reads
owned key set or prefix
run function
```

Key distinction:

```text
dirty input:
  changing this key marks the projection dirty

context read:
  the projection may read this key when it runs, but changes to this key do not
  necessarily mark the projection dirty

owned state read:
  private state for the owning projection; never marks the projection dirty by
  itself
```

Key ownership and layer are defined by `KeyDescriptor`. A projection can use an
owned key prefix as an MVP convention, but descriptors are the authority.

For `candles_1m`:

```text
candles_1m
  dirty inputs:
    origin.es_mbo.batches
  context reads:
    origin.es_mbo.status
  owned key set or prefix:
    projection.candles_1m.*
```

This avoids running candle computation merely because feed status changed. The
candle projection runs when new source batches arrive.

The scheduler builds:

```text
dependency_index:
  origin.es_mbo.batches -> candles_1m
```

Then execution is direct:

```text
OriginWriteRequest appends origin.es_mbo.batches.
DataPlane returns changed_keys = [origin.es_mbo.batches].
Scheduler marks candles_1m dirty.
```

## 17. Hierarchy Rules

For now, cycle prevention should be structural.

Layers are defined by `KeyDescriptor.layer`. Prefixes below are examples, not
the source of truth:

```text
0 origin inputs          origin.*
1 base projections       projection.base.*
2 market views           projection.candles_1m.*
3 runtime diagnostics    runtime.error
```

Lens streaming is not a separate data layer. It is key metadata:

```text
readable_by_lens
stream_to_lens
```

Rules:

```text
origin write requests write origin.* keys
projections may read origin keys
projections may read lower-layer projection keys
projections may read their own private state
projections write only keys they own by descriptor
FrameBus streaming may read keys marked stream_to_lens
FrameBus client read requests may read keys marked readable_by_lens
FrameBus does not write projection state
runtime diagnostics do not drive projection dependencies by default
```

The hierarchy allows multiple inputs.

Example future projection:

```text
projection.some_study
  dirty inputs:
    projection.candles_1m.bars
    projection.base.trades
  context reads:
    origin.es_mbo.status
```

What it does not allow:

```text
candles_1m marks itself dirty from its own write
base projection depends on market-view output
FrameBus writes projection state
```

That is enough cycle prevention for this first runtime.

## 18. First Projection: `candles_1m`

Declaration:

```text
candles_1m
  layer: market view
  dirty inputs:
    origin.es_mbo.batches
  context reads:
    origin.es_mbo.status
  owns key set or prefix:
    projection.candles_1m.*
```

Owned key for MVP:

```text
projection.candles_1m.bars
  value_kind: value
  schema: candles_1m_bars.v1
  readable_by_lens: true
  stream_to_lens: true
```

Behavior:

```text
read origin.es_mbo.batches since its committed cursor
extract trade/fill-like MBO events needed for candles
update completed/live candles in projection.candles_1m.bars
advance cursor for origin.es_mbo.batches
```

MVP payload example:

```json
{
  "schema": "candles_1m_bars.v1",
  "symbol": "ES",
  "completed": [],
  "live": {
    "start_ts_ns": "1760000000000000000",
    "open": 5123.25,
    "high": 5124.0,
    "low": 5122.75,
    "close": 5123.5,
    "volume": 143,
    "final": false
  }
}
```

## 19. End-To-End MVP Flow

```text
Lens sends play to the replay/feed controller.
ReplayFeedController updates its own playback state.

ReplayFeedController advances its replay clock.
ReplayFeedController collects due source batches.
ReplayFeedController sends an OriginWriteRequest to the runtime.

RuntimeScheduler merges writes into pending_origin_writes[origin.es_mbo].
RuntimeScheduler pushes WorkKey::ApplyOriginWrites(origin.es_mbo) once.
Lane 1 processes that key and commits all coalesced origin writes through DataPlane.
DataPlane returns changed key origin.es_mbo.batches.
Scheduler dependency index marks candles_1m dirty and pushes WorkKey::RunProjection(candles_1m) once.

Lane 2 processes WorkKey::RunProjection(candles_1m).
candles_1m reads new origin stream entries from DataPlane.
candles_1m returns a ProjectionWriteBatch.
DataPlane atomically commits projection.candles_1m.bars and cursor advance.
DataPlane returns projection.candles_1m.bars as a changed stream key.

Runtime notifies FrameBus about changed stream key projection.candles_1m.bars.
FrameBus waits for the next 60 fps slot.
FrameBus reads projection.candles_1m.bars through its key read lock.
FrameBus sends one runtime_frame to Lens.
Lens renders candles.

If the feed pauses, no new origin write requests arrive.
After pending runtime work is drained, the runtime is idle.
If no stream_to_lens key changed, FrameBus has nothing to flush.
```

Short version:

```text
Feeds own timing and controls.
Runtime accepts origin write requests.
Data-plane writes return effects.
The scheduler maps effects to keyed projection work.
FrameBus reads changed stream_to_lens keys from the typed KV.
```

## 20. Success Criteria

The refactor is successful when:

```text
Data Center API routes still work
Lens Data Center page still works
book-check validation remains available
prepared local ES MBO data can open an external replay feed
play/pause/speed/feed-latency belong to the replay/feed controller
ReplayFeedController sends batched OriginWriteRequest actions
runtime has no playback loop of its own
paused feed means runtime naturally drains and goes idle
DataPlane is a typed KV with per-key locks
DataPlane commits origin-owned writes and returns WriteEffects
Scheduler maps changed keys to one dirty candles_1m work key
candles_1m writes projection.candles_1m.bars
projection.candles_1m.bars is readable_by_lens and stream_to_lens
FrameBus runs separately from the scheduler and reads allowed keys
Lens renders candles_1m without the old active Session projection runtime
```

The desired result is a simpler hot runtime that can ingest replay now and live
feeds later, without giving up Ledger's existing data ownership and validation
foundation.
