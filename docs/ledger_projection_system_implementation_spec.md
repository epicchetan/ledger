# Ledger Projection System Implementation Spec

## Purpose

Introduce projections: typed, session-scoped computations that fold feed
output into render-ready cells. This phase delivers the minimal projection
contract plus one base projection — time bars over the ES replay feed —
computed as a `RuntimeTask` that wakes on the feed's batches cell and
writes bars into its own cells.

This is the fourth phase of the feed build. All prerequisites have LANDED:

```text
1. docs/cache_watch_implementation_spec.md — LANDED
   Cache::watch_key / CellWatch::changed / generation

2. docs/es_data_management_implementation_spec.md — LANDED
   DBN -> validated es_mbo_event_store artifact + day catalog + CLI

3. docs/ledger_feed_system_implementation_spec.md — LANDED
   session clock cell, EsReplayFeed runtime process, LedgerSessionBuilder /
   LedgerSessionHandle, `ledger session run-es-replay` step/realtime CLI
```

The vision (docs/vision.md §6–§8) describes a full projection graph with
derived studies, wake policies, and frame coalescing. This phase builds
none of that machinery. It proves the load-bearing contract with the
smallest honest slice: a projection is a runtime task, dependency-scheduled
off feed cells, owning its output cells, correct under seek regression.
The graph formalizes later around this shape; nothing here should need a
rewrite when it does.

The boundary:

```text
store    durable raw files and generated artifacts        UNCHANGED
cache    active typed cells + change watches              UNCHANGED
runtime  write application, task scheduling, lifecycle    UNCHANGED
ledger   canonical trade-print policy (market/), the bars
         projection (projection/), session wiring, CLI
```

No new crates and no new dependencies. `crates/ledger/Cargo.toml`,
`crates/store`, `crates/cache`, and `crates/runtime` are untouched by this
spec (the CLI crate already depends on everything it needs).

## Scope

```text
canonical trade-print policy:
  which EsMboEvents count as chart volume, as a market/ function

projection contract (minimal):
  ProjectionSpec string grammar ("bars:1m") + parse + canonical form
  a projection is a RuntimeTask depending on feed cell keys, writing
  cells owned by its own component id

bars base projection:
  time bars (OHLC, volume, aggressor buy/sell volume, trade count)
  completed-bars array cell + live-bar value cell + status value cell
  incremental folding; full rebuild on feed epoch change (regression)

session wiring:
  LedgerSessionBuilder::bars registers cells and queues the task
  tasks install before feed processes

CLI validation:
  repeatable --projection flag on `session run-es-replay`
  projection summaries in the JSON output, deterministic in step mode
```

Out of scope:

```text
projection dependency graph, derived/composite studies
tick bars (bars:200t), dom/bbo/depth projections, session_vwap
wake policies, frame policies, UI frame coalescing
projection artifact caching (offline computation, cache-by-dataset)
order book construction, execution simulation
transport (remux session/projection RPCs), Lens rendering
projection subscription protocol
multiple feeds per session, multiple sessions
```

## Design Principles

```text
completed bars are immutable    the bars array only grows, except on feed
                                regression; consumers may cache by index
one live bar                    the in-progress bucket lives in a separate
                                value cell, rewritten in place
event time, not session time    bars bucket on ts_event_ns; the session
                                clock decides WHEN batches arrive, never
                                WHAT bar they land in — replay at any
                                speed produces identical bars
projections never write feed    cell ownership enforces it structurally
cells                           (projection component id != feed owner)
rebuild on regression           correctness first; incremental undo is a
                                future optimization
tasks, not processes            projections have no pacing and no joins;
                                the runtime's dependency scheduler is the
                                only wake source
```

## Canonical Trade-Print Policy

vision.md §5 requires an explicit policy for what counts as canonical
chart volume. Databento MBO carries two trade-shaped actions:

```text
Trade  the aggressor print: one record per match, size = matched quantity,
       side = aggressor side (None when unattributed)
Fill   per-resting-order fill records mirroring the same match
```

Counting both double-counts volume. The canonical policy: **`Trade`
records with a price and nonzero size are prints; everything else is
not.** `Fill` is preserved in the event store for microstructure work but
never contributes to bars, volume profile, or delta.

Add to `crates/ledger/src/market/es_mbo.rs`:

```rust
/// A canonical trade print per the ledger trade-print policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradePrint {
    pub ts_event_ns: UnixNanos,
    pub price_ticks: PriceTicks,
    pub size: u32,
    /// Aggressor side as reported by the venue; `None` when unattributed.
    pub aggressor: Option<BookSide>,
}

/// Canonical trade-print policy: `Trade` actions with a price and nonzero
/// size count as chart volume; `Fill` mirrors the resting side of the same
/// match and is excluded to avoid double-counting.
pub fn canonical_trade_print(event: &EsMboEvent) -> Option<TradePrint>
```

The function returns `None` for any action other than `Trade`, for
`price_ticks: None`, and for `size == 0`. `aggressor` maps straight from
`event.side`.

## Projection Spec Grammar

`crates/ledger/src/projection/mod.rs` (new module, declared in `lib.rs` as
`pub mod projection;` with `bars` as a submodule):

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionSpec {
    Bars(BarsParams),
}

impl ProjectionSpec {
    /// Parse a projection spec string, e.g. "bars:1m".
    pub fn parse(spec: &str) -> Result<Self, LedgerError>;

    /// Canonical form, e.g. "bars:1m". Parsing the canonical form yields
    /// an equal spec.
    pub fn canonical(&self) -> String;
}
```

Grammar for this phase:

```text
spec       := "bars:" interval
interval   := digits unit          digits >= 1, value > 0
unit       := "s" | "m" | "h"
```

Anything else — unknown projection name, missing/empty interval, zero
value, unknown unit, non-digit characters — is
`LedgerError::InvalidProjectionSpec`. Parsing is case-sensitive and
lowercase-only (cell keys are lowercase; do not silently normalize case).

Canonicalization reduces the interval to the largest exact unit so
differently spelled equal intervals collide onto the same projection
instead of registering twice:

```text
"bars:60s"  -> canonical "bars:1m"   (interval_ns divisible by 60s)
"bars:120m" -> canonical "bars:2h"
"bars:90s"  -> canonical "bars:90s"  (not divisible by 60s)
```

`BarsParams` lives in `projection/bars.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BarsParams {
    /// Bar interval in nanoseconds. Always > 0.
    pub interval_ns: u64,
}
```

The component id derives from the canonical spec by replacing `:` with a
dot path: `"bars:1m"` -> component id `projection.bars.1m`. Interval
strings like `1m`, `90s`, `2h` are valid key segments (lowercase ASCII +
digits). Cell keys append a final segment to the component id.

## Cells

`crates/ledger/src/projection/bars.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bar {
    /// Bucket start: ts_event_ns - (ts_event_ns % interval_ns).
    pub interval_start_ns: UnixNanos,
    pub open: PriceTicks,
    pub high: PriceTicks,
    pub low: PriceTicks,
    pub close: PriceTicks,
    /// Total canonical print volume, attributed or not.
    pub volume: u64,
    /// Volume where the aggressor was a buyer (BookSide::Bid).
    pub buy_volume: u64,
    /// Volume where the aggressor was a seller (BookSide::Ask).
    pub sell_volume: u64,
    pub trade_count: u64,
    pub first_ts_event_ns: UnixNanos,
    pub last_ts_event_ns: UnixNanos,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarsStatus {
    /// Canonical spec, e.g. "bars:1m".
    pub spec: String,
    /// Feed epoch this projection state reflects.
    pub epoch: u64,
    /// Feed batches folded in; equals cursor.batch_idx when caught up.
    pub processed_batches: usize,
    pub completed_bars: usize,
    /// Timestamp of the last folded print (not batch).
    pub last_ts_event_ns: Option<UnixNanos>,
}

#[derive(Debug, Clone)]
pub struct BarsCells {
    pub bars: ArrayKey<Bar>,
    pub live: ValueKey<Bar>,
    pub status: ValueKey<BarsStatus>,
}
```

Registration mirrors `EsReplayCells::register`. For canonical spec
`bars:1m` the cells are:

```text
projection.bars.1m.bars     Array<Bar>        public_read: true
projection.bars.1m.live     Value<Bar>        public_read: true
projection.bars.1m.status   Value<BarsStatus> public_read: true
```

All three are owned by `CellOwner::new("projection.bars.1m")` — exactly
the owner the task's `ctx.owner()` produces from its `ComponentId`, and
structurally distinct from both the feed owner and the session owner. All
initial values are empty/`None`.

Cell-key and owner construction can fail only on invalid keys, which the
canonicalization rules out; use the same
`descriptor(key, owner, kind, public_read)` helper shape as
`feed/es_replay/cells.rs`.

Semantics:

```text
bars    completed bars only, ordered by interval_start_ns, strictly
        increasing, gaps allowed (empty buckets produce no bar)
live    the in-progress bucket's aggregate; None until the first print
        after start or rebuild; NOT moved to `bars` when the feed ends —
        end-of-data does not close a bar, a later print in a newer bucket
        does (a backward seek can always revive the feed)
status  written by prepare() once, then on every run that changed state;
        drivers synchronize on it (see Driver Synchronization)
```

## The Bars Task

`crates/ledger/src/projection/bars.rs`:

```rust
pub struct BarsTask {
    descriptor: TaskDescriptor,
    params: BarsParams,
    spec: String,              // canonical
    feed: EsReplayCells,       // read-only: batches + cursor
    cells: BarsCells,
    // fold state
    epoch: u64,
    processed_batches: usize,
    completed_bars: usize,
    live: Option<Bar>,
    last_print_ts: Option<UnixNanos>,
}
```

The descriptor:

```rust
TaskDescriptor::new(component_id, vec![feed.batches.key().clone()])
```

Depending on the batches key alone is sufficient: forward emission pushes
to the batches array, and regression replaces it — both change that key,
and the feed writes batches + cursor + status atomically in one
`ExternalWriteBatch`, so whenever the task runs, the cursor it reads is
consistent with the array it reads (the worker applies writes and runs
tasks on one thread; nothing interleaves inside a drain step).

`prepare(ctx: TaskPrepareContext)` publishes the initial status so
watchers can distinguish "installed, nothing processed" from "absent":

```rust
let mut batch = ctx.batch();
batch.set_value(&self.cells.status, self.status_snapshot());
ctx.submit(batch).await?;
```

`run_once(ctx: TaskContext<'_>)`:

```text
1. cursor = ctx.read_value(&self.feed.cursor)?
   None -> return Ok(()) (feed has not published; nothing to fold)

2. if cursor.epoch != self.epoch
   or cursor.batch_idx < self.processed_batches:
     REBUILD:
       batches = ctx.read_array(&self.feed.batches)?
       reset fold state; epoch = cursor.epoch
       fold every batch in order
       one write batch:
         replace_array(bars, completed)
         set_value(live) or clear_value(live)
         set_value(status)

3. else if cursor.batch_idx > self.processed_batches:
     INCREMENTAL:
       new = ctx.read_array_range(
           &self.feed.batches,
           self.processed_batches..cursor.batch_idx)?
       fold each batch in order
       one write batch:
         push_array(bars, newly completed bars) when nonempty
         set_value(live) when the live bar changed
         set_value(status)

4. else: return Ok(()) with no writes (spurious wake; do not dirty cells)
```

The invariant `cursor.batch_idx == batches.len()` holds by feed
construction (asserted in the feed tests); the rebuild path nonetheless
trusts the array it actually read and sets
`processed_batches = batches.len()`.

Folding a batch:

```text
for event in batch.events:
  print = canonical_trade_print(&event) else continue
  bucket = print.ts_event_ns - (print.ts_event_ns % params.interval_ns)
  match live:
    None                       -> live = new bar from print at bucket
    Some(bar) if bucket == bar.interval_start_ns
                               -> fold print into bar
    Some(bar) if bucket >  bar.interval_start_ns
                               -> completed.push(bar);
                                  live = new bar from print at bucket
    Some(bar) /* bucket < */   -> fold print into bar (defensive only:
                                  event stores are ts-ordered; never
                                  rewrite completed bars)
fold print into bar:
  high = max, low = min, volume += size, trade_count += 1
  buy_volume  += size when aggressor == Some(BookSide::Bid)
  sell_volume += size when aggressor == Some(BookSide::Ask)
  close = print price; last_ts_event_ns = print ts
  (defensive out-of-order fold: update close/last_ts only when
   print.ts_event_ns >= bar.last_ts_event_ns)
```

Failures (`read_value`/`read_array*`/`submit` errors) map into
`ComponentError` via `?` — the runtime already surfaces task failures as
`ComponentStatus::Failed`. There is no projection-specific error channel.

### Why rebuild-on-regression is correct and cheap enough

Regression bumps the feed epoch and truncates the batches array; batches
the projection already folded may be gone, and folded state cannot be
incrementally unwound without per-bucket undo logs. Rebuilding from the
truncated array is deterministic and, for one trading day of bars, small
(hundreds of bars, one array replace). Replay determinism guarantees a
rebuild followed by re-emission reproduces exactly the bars a
never-regressed run would produce — the tests pin this.

## Session Wiring

`crates/ledger/src/session.rs` changes:

```rust
pub struct LedgerSessionBuilder<S> {
    // existing fields...
    tasks: Vec<Box<dyn RuntimeTask>>,      // NEW, initialized empty
}

impl<S> LedgerSessionBuilder<S> {
    /// Register a time-bars projection over an es_replay feed's cells.
    pub fn bars(
        &mut self,
        feed: &EsReplayCells,
        params: BarsParams,
    ) -> Result<BarsCells, LedgerError>
}
```

`bars` registers the three cells against `self.cache`, constructs the
`BarsTask`, pushes it into `self.tasks`, and returns the cells clone —
the same shape as `es_replay`. Registering the same canonical spec twice
fails naturally on duplicate cell registration; surface that error, do not
pre-check.

`start()` gains one step, ordered so no feed emission can precede task
registration (a task installed after a write to its dependency never sees
that write):

```text
1. spawn RuntimeWorker            (unchanged)
2. publish initial ClockState     (unchanged)
3. for task in self.tasks: runtime.install_boxed_task(task).await?   NEW
4. for feed in self.feeds: runtime.install_boxed_process(feed).await?
```

`LedgerSessionHandle` is unchanged. Projections are driven entirely by
the scheduler; the handle has no projection methods.

## Driver Synchronization

Feed writes and projection folds are decoupled: the cursor advancing does
not mean bars are current. Drivers that need quiescent projections (the
CLI summary, tests) synchronize on the status cell:

```text
caught_up(status, cursor) :=
  status.epoch == cursor.epoch
  && status.processed_batches == cursor.batch_idx
```

The idiom is the existing watch loop: `cache.watch_key(status key)`, read,
check predicate, `changed().await`, repeat — timeout-bounded, no polling.
Because the worker drains the task queue in the same loop that applies
feed writes, catch-up is prompt; the predicate exists for correctness,
not because lag is expected.

## Error Types

Extend `LedgerError`:

```rust
    #[error("invalid projection spec `{spec}`: {reason}")]
    InvalidProjectionSpec { spec: String, reason: String },
```

No other variants. Task-time failures are `ComponentError`, surfaced by
the runtime as component status, exactly like feed failures.

## CLI

`crates/cli/src/main.rs` — extend `SessionRunEsReplayArgs`:

```rust
    /// Projections to compute over the feed, e.g. `bars:1m`. Repeatable.
    #[arg(long = "projection")]
    projections: Vec<String>,
```

Flow changes in `run_session_command` / the es-replay runner:

```text
1. parse all specs up front via ProjectionSpec::parse — fail before any
   store or session work; also reject duplicate canonical specs with a
   clear anyhow error (nicer than the raw duplicate-cell error)
2. after builder.es_replay(...), call builder.bars(&cells, params) per
   spec; keep Vec<(String /* canonical */, BarsCells)>
3. run the step/realtime drive unchanged
4. after the drive completes (target reached or feed ended), read the
   final cursor, then for each projection wait for caught_up(status,
   cursor) on a status watch, bounded by SESSION_STEP_TIMEOUT; on
   timeout, report the projection spec and both counters in the error
5. extend the summary
```

Summary additions (`SessionRunEsReplaySummary`):

```rust
    #[serde(skip_serializing_if = "Vec::is_empty")]
    projections: Vec<SessionProjectionSummary>,
```

```rust
#[derive(Serialize)]
struct SessionProjectionSummary {
    spec: String,
    completed_bars: usize,
    live_bar: bool,
    /// Totals across completed bars plus the live bar.
    volume: u64,
    trade_count: u64,
    /// interval_start_ns of the first completed bar (or live bar if none
    /// completed), u64-as-string like the other ns fields.
    first_bar_start_ns: Option<String>,
    /// interval_start_ns of the live bar if present, else the last
    /// completed bar.
    last_bar_start_ns: Option<String>,
}
```

Built by reading the bars array and live cell after catch-up. With no
`--projection` flags the summary output is byte-identical to today's.

## Tests

No new fixtures beyond one helper. `crates/ledger/tests/support/mod.rs`
gains:

```rust
pub fn trade(
    ts_event_ns: u64,
    sequence: u64,
    price_ticks: i64,
    size: u32,
    aggressor: Option<BookSide>,
) -> EsMboEvent
```

— action `Trade`, `is_last: true` (one batch per event, matching the
existing `event` helper), other fields as in `event`.

Unit tests (`projection` module or a small `tests/projection_spec.rs`):

```text
parse accepts bars:1s / bars:5m / bars:2h / bars:90s
parse canonicalizes bars:60s -> bars:1m, bars:120m -> bars:2h
parse rejects: "bars", "bars:", "bars:0s", "bars:1x", "bars:m",
  "foo:1m", "BARS:1M" -> InvalidProjectionSpec
canonical_trade_print: Trade with price+size -> Some with aggressor
  mapped; Fill / Add / Cancel / Clear / None -> None; Trade with
  price None -> None; Trade with size 0 -> None
```

Integration tests (`crates/ledger/tests/projection_bars.rs`) run the real
session — `LedgerSessionBuilder` + `es_replay` + `bars` + `start`, driven
by `handle.seek_to`, synchronized via status/cursor watches with the
existing timeout-bounded wait idiom (never `Drain`, never sleeps):

```text
prepare publishes initial status (epoch 0, processed 0) before any seek
bars aggregate OHLC / volume / buy+sell volume / trade_count correctly
  across bucket boundaries; empty buckets produce no bar (gap preserved)
non-print events (Add/Cancel) and Fill events contribute nothing
unattributed prints (aggressor None) count toward volume and
  trade_count but neither buy_volume nor sell_volume
live bar: last bucket sits in the live cell, not the array; feed end
  does not promote it
completed bars are immutable: sequential forward seeks only append
  (capture the array between seeks and assert prefix equality)
incremental folding: several sequential seeks produce bars identical to
  one single seek over the same events (fresh session, same fixture)
regression: backward seek rebuilds — bars truncate to the kept batches,
  status.epoch matches the bumped cursor epoch; a following forward
  seek reproduces exactly the pre-regression bars
two projections (bars:1s and bars:1m) coexist on one feed and both
  catch up
registering the same canonical spec twice errors
projection cells are owned by the projection component: a session-owner
  write batch to the bars array is rejected by the runtime
```

Timestamps in fixtures are synthetic small nanosecond values (as in the
feed tests); choose interval params like `BarsParams { interval_ns: 100 }`
so bucket boundaries are easy to write by hand — `BarsParams` is public
and not restricted to parseable intervals.

CLI validation against the real prepared day:

```text
cargo run -p ledger-cli -- session run-es-replay \
  --raw-id sha256-f54b79d586575e910da514dfbf5891a7727d017ddfbca7ec73b898361ca1dee5 \
  --batches 20000 --projection bars:1s --projection bars:1m
```

Acceptance: both projections report nonzero volume and equal trade_count;
bars:1s has completed bars (20k batches spans several seconds of event
time at the open); a second identical run prints an identical summary
(step-mode determinism now extends through projections).

Workspace validation:

```text
cargo fmt --all
cargo test --workspace
```

## Success Criteria

```text
cache, runtime, and store crates are unchanged
projections are runtime tasks: no pacing, no joins, no polling; the
  dependency scheduler is the only wake source
projection cells are owned by the projection component; feeds and the
  session owner structurally cannot write them, and vice versa
bars are event-time: identical bars at any clock speed and across any
  seek pattern, including regression
completed-bars array mutates only by append, except regression rebuild
Fill never contributes to volume; the policy is one named function in
  market/
drivers synchronize on the status cell predicate, never on Drain
CLI without --projection is byte-identical to the previous phase
cargo test --workspace passes
```

## Future Extensions

```text
projection registry      dynamic install/uninstall on a running session,
                         session handles listing projections by spec
dependency graph         projections over projections (derived studies),
                         shared batch-feature projections
tick bars                bars:200t — same cells, count-based bucketing
more base projections    bbo, dom:N, trade_stream, session_vwap
incremental regression   per-bucket undo instead of full rebuild, when
                         bars arrays get large enough to matter
projection artifacts     offline computation cached by
                         dataset + spec + version
transport subscriptions  remux methods + cell-watch push frames to Lens,
                         alongside the session RPCs
frame policies           UI coalescing separated from exact computation
```
