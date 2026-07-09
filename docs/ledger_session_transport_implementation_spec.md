# Ledger Session Transport Implementation Spec

## Purpose

Expose ledger sessions over the remux transport so a viewer can open a
prepared ES day as a replay session, control playback, and receive
projection output as push frames. This is the bridge between the Rust
session (feeds + projections writing cache cells in the `ledger-remux`
process) and Lens (a static web viewer that can only speak JSON-RPC).

This is the fifth phase of the feed build. All prerequisites have LANDED:

```text
1. docs/remux_extension_implementation_spec.md — LANDED
   ledger-remux stdio JSON-RPC binary, method dispatch, notification
   broadcast (jobs/progress, jobs/finished), Lens as a remux viewer

2. docs/ledger_feed_system_implementation_spec.md — LANDED
   LedgerSessionBuilder / LedgerSessionHandle, session clock,
   EsReplayFeed with seek regression epochs

3. docs/ledger_projection_system_implementation_spec.md — LANDED
   bars projection: completed-bars array cell + live-bar value cell +
   status cell, canonical trade-print policy, --projection CLI
```

How the pieces talk (nothing new is invented here — this is the
architecture the remux conversion settled):

```text
Lens (browser / phone)
  @remux/viewer-kit ipc: requestIpc + subscribeIpcEvents
        │  websocket
remux runtime
  process supervision, viewer asset serving, notification fan-out
        │  stdio JSON-RPC (line-delimited)
ledger-remux (crates/remux)
  owns the LedgerSession in-process: cache, runtime worker, feed,
  projection tasks — and translates cache cell changes into JSON-RPC
  notifications via cache watches
```

Lens never touches the cache. The cache is an in-process Rust structure;
the subscription surface is `Cache::watch_key` reified over the wire:
one watcher task per notification stream reads the latest cell state and
broadcasts a frame. Projections do not describe how to render themselves
— there is no render-contract registry in this phase. The projection
spec string ("bars:1m") is the shared vocabulary: the transport pushes
typed bar frames for `bars:*` projections, and the viewer knows how to
render the "bars" kind. A registry becomes worth building when there is
more than one projection kind (vision.md §7); nothing here blocks it.

The boundary:

```text
store       durable objects                                UNCHANGED
cache       typed cells + watches                          UNCHANGED
runtime     scheduling, lifecycle                          UNCHANGED
ledger      session, feed, projections                     clock_key accessor only
remux       session methods, frame watchers                THIS SPEC
lens        viewer UI                                      NOT THIS SPEC
```

The one ledger change is `LedgerSessionHandle::clock_key()` — exposing
the session clock cell key so the clock stream can be a cache watcher
like every other push stream. No new crates. No new
dependencies (`crates/remux/Cargo.toml` already depends on `ledger`,
`store`, `tokio`, `serde`, `serde_json`). Lens work is the next phase;
this phase is validated headlessly, exactly like the CLI phase was.

## Scope

```text
session lifecycle over RPC:
  open a prepared day as a replay session (single active session),
  close it, replace it, query a full status snapshot

playback control over RPC:
  play / pause / speed / seek — thin adapters over LedgerSessionHandle

push frames (notifications):
  clock changes, feed cursor progress, and per-projection bar frames
  (append-only deltas keyed by feed epoch, full resync on regression)

pull backfill:
  a bars read method so a late-joining client can fetch completed bars
  it missed before subscribing, using the same frame shape
```

Out of scope:

```text
Lens UI (chart, playback controls, session screen) — next phase
multiple concurrent sessions, per-client subscription state
render-contract registry / projection self-description
frame policies, rate limiting, UI coalescing beyond watch coalescing
dom/bbo/tick-bar projections (transport generalizes when they exist)
auth (remux runtime owns client identity; the extension trusts it)
```

## Design Principles

```text
the session lives in ledger-remux    one process owns cache + worker +
                                     feed + tasks; RPC methods touch it
                                     only through LedgerSessionHandle
watchers, not polling                every push stream is a tokio task
                                     in a watch_key loop: changed() ->
                                     read latest -> send frame
coalescing is free                   cell watches are generation-based;
                                     while a watcher is sending, any
                                     number of writes collapse into one
                                     wake — frame rate is bounded by
                                     send rate, never by write rate
frames are deltas                    completed bars are append-only per
                                     epoch (the projection contract), so
                                     a frame carries only new bars; an
                                     epoch change forces a full frame
notifications are broadcast          remux fans out to every connected
                                     client; there is no per-client
                                     subscribe — an open session streams
one active session                   opening replaces the previous
                                     session (single user, many screens);
                                     session ids exist so stale clients
                                     fail loudly, not so sessions coexist
ns values are strings                u64 nanoseconds ride JSON as strings
                                     (existing DTO convention); counts,
                                     prices (i64 ticks), and speed are
                                     plain numbers
```

## Protocol Surface

All methods follow the existing `remux/ledger/*` dispatch in
`crates/remux/src/methods.rs`; all params/results are camelCase DTOs.

### Methods

```text
remux/ledger/session/open
  params  { rawId: string, projections?: string[] }   e.g. ["bars:1m"]
  result  { sessionId: string, rawId: string,
            projections: [{ spec: string }],          canonical specs
            replaced: string | null }                 prior session id

  Parses every projection spec via ProjectionSpec::parse (invalid ->
  invalidParams; duplicate canonicals -> invalidParams). Verifies the
  raw object exists (objectNotFound otherwise). Shuts down and replaces
  any existing session (broadcasting session/closed first). Builds
  LedgerSessionBuilder -> es_replay(rawId) -> bars(...) per spec ->
  start(). Returns immediately: artifact prepare/decode happens inside
  the feed process; readiness is observable through cursor frames and
  session/status. Session ids are "session-1", "session-2", ... —
  monotonic per process, like job ids.

remux/ledger/session/close
  params  { sessionId: string }
  result  { closed: true }
  Aborts the watcher tasks, shuts the session down, broadcasts
  session/closed with reason "closed".

remux/ledger/session/status
  params  { sessionId: string }
  result  {
    sessionId: string, rawId: string,
    clock: ClockSnapshotDto,
    feed: { componentStatus: string,          e.g. "running", "failed"
            status: EsReplayStatusDto | null, feed status cell
            cursor: EsReplayCursorDto | null },
    projections: [{ spec: string, status: BarsStatusDto | null,
                    completedBars: number, liveBar: boolean }]
  }
  Pull snapshot for late joiners and for diagnosing a feed that failed
  before producing cells (componentStatus comes from
  RuntimeHandle::component_status of the feed component; Failed(reason)
  serializes as "failed: <reason>").

remux/ledger/session/play    params { sessionId }            result { ok: true }
remux/ledger/session/pause   params { sessionId }            result { ok: true }
remux/ledger/session/speed   params { sessionId, speed: number }  result { ok: true }
remux/ledger/session/seek    params { sessionId, sessionNs: string } result { ok: true }
  Thin adapters over LedgerSessionHandle::{play, pause, set_speed,
  seek_to}. sessionNs is a u64-as-string. Handle errors (e.g. invalid
  speed) map to domain errors.

remux/ledger/session/bars
  params  { sessionId: string, spec: string, from?: number }
  result  BarsFrameDto (below) with bars from index `from` (default 0)
  Pull-side backfill. Same consistency recipe and same shape as the
  push frame, so the client has one decode path.
```

Every method with a `sessionId` returns a domain error
`"unknown session <id>"` when it does not match the active session —
a stale client must resync, not silently drive a new session.

### Notifications

```text
remux/ledger/session/closed
  { sessionId: string, reason: "closed" | "replaced" }

remux/ledger/session/clock
  { sessionId: string, clock: ClockSnapshotDto }
  Watcher on the session clock cell. Fires on play/pause/speed/seek.

remux/ledger/session/feed
  { sessionId: string, cursor: EsReplayCursorDto }
  Watcher on the feed cursor cell. Watch coalescing bounds the rate:
  in step bursts or fast replay, intermediate cursors collapse.

remux/ledger/session/barsFrame
  BarsFrameDto — one watcher per projection on its status cell.
```

### DTOs

```text
ClockSnapshotDto      mirror of ledger::clock::ClockSnapshot, camelCase,
                      ns fields as strings
EsReplayCursorDto     { epoch: number, feedSeq: number, batchIdx: number,
                        totalBatches: number, tsEventNs: string | null,
                        nextTsEventNs: string | null, catchingUp: boolean,
                        ended: boolean }
                      catchingUp is true while feed catch-up output is
                      still converging after an already-due backlog.
EsReplayStatusDto     { rawObjectId, artifactObjectId, clock, cursor }
BarsStatusDto         { spec, epoch: number, processedBatches: number,
                        completedBars: number, lastTsEventNs: string | null }
BarDto                { intervalStartNs: string, open: number, high: number,
                        low: number, close: number,     i64 price ticks
                        volume: number, buyVolume: number,
                        sellVolume: number, tradeCount: number,
                        firstTsEventNs: string, lastTsEventNs: string }

BarsFrameDto {
  sessionId: string,
  spec: string,               canonical, e.g. "bars:1m"
  epoch: number,              feed epoch these bars belong to
  from: number,               index of bars[0] in the completed array
  bars: BarDto[],             completed bars from `from`
  total: number,              completed count after this frame
  live: BarDto | null,
  status: BarsStatusDto
}
```

Client contract: a frame is contiguous if `epoch` matches the client's
epoch and `from == client.count`. On mismatch (missed frames, joined
late, or feed regression) the client refetches with the pull method
from 0 — regression means previously received bars are invalid, full
resync is the correct and cheap response (one trading day of bars).

## Frame Watchers

`crates/remux/src/session.rs` (new module). The active session:

```rust
struct ActiveSession {
    id: String,
    raw_id: StoreObjectId,
    handle: LedgerSessionHandle,
    feed: EsReplayCells,
    projections: Vec<(String /* canonical */, BarsCells)>,
    watchers: Vec<tokio::task::JoinHandle<()>>,
}
```

held as `Mutex<Option<ActiveSession>>` (a tokio or std mutex — the
critical sections are tiny; follow whichever the implementation finds
cleaner, but never hold it across an `.await` on session shutdown:
`take()` the session out, release, then shut down).

Watcher tasks hold a `Cache` clone (Cache is `Clone`) and the
`OutboundSender` clone — the same ingredients the job notification
tasks already use. Shape of every watcher:

```text
let mut watch = cache.watch_key(key)?;
loop {
    read latest state; if it changed vs last sent, send notification;
    watch.changed().await — exit the task when it errors (cache gone)
}
```

The bars watcher's read recipe, for frames that are internally
consistent while the projection task is writing concurrently:

```text
1. status = read_value(status cell)         the anchor
2. if status.epoch != sent_epoch:
       sent_epoch = status.epoch; sent_count = 0     full resync frame
3. if status.completed_bars > sent_count:
       bars = read_array_range(bars cell,
                               sent_count .. status.completed_bars)
   else bars = []
4. live = read_value(live cell)
5. send frame { epoch: status.epoch, from: sent_count, bars,
                total: status.completed_bars, live, status }
   sent_count = status.completed_bars
```

Reading the range bounded by `status.completed_bars` — never by the
array's current length — keeps the frame anchored to one projection
run even if a newer run has already appended more bars (the next wake
delivers them). If a regression lands between steps 1 and 3 the range
read can fail (array shorter than the anchor); treat any read error in
a watcher as "skip this wake" — the projection rewrites the status
cell in the same regression rebuild, so another wake is already
guaranteed and the next pass resyncs from the new epoch. Do not tear
down the stream on a read race.

The live bar can change without any completed bar being added, and the
status cell is written on every state-changing projection run — which
is why the status cell (not the bars array) is the watch key: one wake
source covers completed, live, and status changes.

The pull method `session/bars` uses the same recipe with
`sent_count = from` and no state.

## Session Lifecycle

```text
open:
  parse specs -> verify raw exists -> lock slot
  if occupied: take, abort watchers, shutdown (log shutdown errors,
               do not fail the open), broadcast closed{replaced}
  build + start session -> spawn watchers -> store ActiveSession
  (builder work after start() is only watcher spawning — no awaits
   between start and slot store that could race a concurrent open;
   the slot lock is held across the swap)

close:
  lock slot, take if id matches (else unknown-session error), release
  abort watchers -> shutdown -> broadcast closed{closed}
  shutdown errors map to a domain error in the close result, but the
  slot is already empty either way — a failed shutdown must not leave
  a zombie claiming the slot

process exit (SIGTERM path in rpc.rs):
  no special handling — the runtime worker and watcher tasks die with
  the process; sessions are ephemeral by design (nothing durable lives
  in the cache; the store is untouched)
```

`LedgerSessionHandle::shutdown` consumes the handle — the
`Mutex<Option<...>>::take()` pattern is load-bearing, not stylistic.

## Error Mapping

No new error codes. Existing constructors in `crates/remux/src/error.rs`:

```text
invalid projection spec / duplicate spec     RpcError::invalid_params
unknown raw object                           RpcError::object_not_found
raw id malformed                             RpcError::invalid_object_id
unknown session id                           RpcError::domain
LedgerError from build/start/control         RpcError::domain(to_string)
```

Feed failures after start (bad artifact, decode error) are not RPC
errors — they surface as `componentStatus: "failed: <reason>"` in
`session/status`, exactly like the runtime surfaces them everywhere
else. A client that sees a cursor stall pulls status.

## Tests

`crates/remux` is a binary crate; tests live as unit tests in the new
`src/session.rs` (and `methods.rs` for dispatch), following the
existing `methods.rs` test module pattern (TestRemote in-memory store,
`LedgerRemux::new` with a captured `mpsc` output channel — captured
notifications are asserted directly from the receiver).

Test support: a `#[cfg(test)]` fabrication helper mirroring
`crates/ledger/tests/support/mod.rs::fabricate_prepared_day` and its
`trade(...)` event builder — both are built on public `ledger` /
`store` APIs, so the mirror is mechanical (do not add a testkit
feature to the ledger crate for this; revisit when a third crate needs
the fixture).

Scenarios (drive `handle(Request)` directly; synchronize on captured
notifications and `session/status` pulls with timeout-bounded waits —
never sleeps):

```text
open returns canonical specs and session-1; invalid spec, duplicate
  spec, malformed raw id, unknown raw id all fail with the mapped
  errors and do not create a session
open + seek: clock notification observed after seek; feed cursor
  frames observed; barsFrame stream delivers bars matching a direct
  cache read (same fixture as the projection tests: interval 100ns,
  hand-computable buckets)
frame contract: sequential seeks produce contiguous frames
  (frame.from == previous total, epochs equal); concatenated frame
  bars equal the full array
regression: backward seek produces a frame with the new epoch and
  from == 0; concatenating from that frame reproduces the truncated
  state; a following forward seek streams the re-emitted bars
pull backfill: session/bars with from=0 returns everything the frames
  delivered; from=k returns the suffix; unknown spec errors
control methods: play/pause/speed/seek mutate the clock cell (assert
  via clock snapshots); bad speed maps to a domain error
lifecycle: second open replaces the first (closed{replaced} observed,
  old session id becomes unknown); close broadcasts closed{closed};
  control after close -> unknown session error
status: reflects feed componentStatus, cursor, and per-projection
  status; correct both before any seek and after ended
```

Workspace validation:

```text
cargo fmt --all
cargo test --workspace
```

## Real-Data Validation

Headless stdio round-trip against the prepared 2026-03-12 day, from
the repo root (the binary reads `.env` / `LEDGER_DATA_DIR` exactly like
the CLI):

```text
1. run ledger-remux; write protocol lines on stdin:
   open  { rawId: sha256-f54b79d5…, projections: ["bars:1s","bars:1m"] }
   seek  { sessionNs: "1773266736362343529" }     the exact last_ts of
                                                  the CLI 20k-batch run
2. read stdout until both projections' barsFrames report
   status.processedBatches == cursor.batchIdx with cursor.tsEventNs ==
   the seek target, then close.
```

Acceptance: summing the streamed frames per projection reproduces the
CLI validation exactly — bars:1s: 274 completed bars, bars:1m: 5,
both with volume 2365 and tradeCount 1125, live bar present on both.
Event-time bucketing makes this a strict cross-transport determinism
check: the CLI drove by batch count, the transport drives by seek
target, and the bars must be identical.

A disposable driver script under the scratch directory (not committed)
is fine for this; the committed artifact is the test suite.

## Success Criteria

```text
store, cache, and runtime crates are unchanged; ledger only gains the
  clock_key accessor
Lens can be built against this surface with no further backend work:
  open -> frames -> control -> close covers the whole viewer loop
no polling anywhere: every push stream is a cell watch; frame rate is
  bounded by watch coalescing, not write rate
frames are deltas with an explicit resync contract (epoch + from);
  a client that misses any number of frames recovers with one pull
regression is a protocol event (new epoch, from 0), not a corruption
single active session with replace semantics; stale session ids fail
cargo test --workspace passes
real-data frames reproduce the CLI projection summary exactly
```

## Future Extensions

```text
multiple sessions        session registry keyed by id; open stops
                         replacing; frames already carry sessionId
render contracts         projection descriptors (kind, cell shapes,
                         render hints) once a second kind exists
generic cell frames      subscribe to arbitrary public_read cells by
                         key — the bars watcher generalizes
frame policies           server-side min-interval / viewport filtering
                         when phone bandwidth demands it
session persistence      reopen last session on extension restart
live feeds               the same surface serves a live databento feed
                         when one exists; only open() params change
```
