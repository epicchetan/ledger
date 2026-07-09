# Replay Cleanup Pass — Implementation Spec

A performance and conformance pass on the replay pipeline, to land between
lens Phase 1 (transport + page, done) and Phase 2 (the chart). Two goals:

1. **Performance/correctness of seek and catch-up.** Today a forward seek
   of 20k batches produces 20,000 single-batch cache transactions and
   ~3,300 UI frames; a backward seek clones the entire materialized batch
   array three times (feed regress once, each bars projection once); and a
   bars rebuild runs as one unbounded task step that blocks control-write
   application until it finishes. Catch-up must become chunked, regress
   must become a truncation, rebuilds must become bounded steps that
   interleave with control writes, and the UI must be able to *know* the
   feed is catching up instead of inferring it.
2. **Lens conformance.** The replay page invented a top nav; lens's design
   language is: the bottom ActionBar is the action area, content above it,
   no self-made navs. The transport controls move onto the ActionBar
   surface.

Everything in this spec preserves the wire protocol except one addition
(`catchingUp` on the feed cursor) and preserves replay determinism: the
real-data validation (raw `sha256-f54b79d5…`, seek
`1773266736362343529` → 274/5 completed bars, volume 2365, tradeCount
1125, live present) must reproduce exactly.

## Diagnosis, with line references

- `crates/ledger/src/feed/es_replay/feed.rs:133` — the catch-up loop emits
  one submit per due batch (`emit` at feed.rs:248: `push_array` of a
  single batch + cursor + status). Each submit wakes the worker, runs both
  bars tasks, rewrites their status cells, and wakes three remux watchers.
- `feed.rs:107`+`feed.rs:133` — the clock is read once per outer loop
  pass; the inner emit loop runs to the captured `now` without re-reading
  it. A *backward seek* issued mid-catch-up is not honored until the pass
  completes. (Note: a *pause* mid-catch-up is safe today — pause freezes
  the clock at a monotonically later point, so it can never trip the
  regress path; it just stops the chase after the pass.)
- `feed.rs:228` (`regress`) — clones the whole batches array
  (`read_array`), filters by time, `replace_array`s it back. O(all) for
  any backward seek.
- `crates/ledger/src/projection/bars.rs:206` — on epoch change the bars
  task `read_array`s the *entire* feed batch array (a full clone of every
  event, per projection) and refolds it inside a single `run_once`.
- `crates/runtime/src/worker.rs:56` — the worker applies external writes
  and runs tasks via `run_until_idle` inline in its select loop. While a
  rebuild runs, newly arriving external writes (including clock controls)
  sit unread in the channel. Controls return `{ok:true}` (they only
  enqueue) but apply late — the UI's clock stream stalls behind the
  rebuild.
- `worker.rs:21` — `DEFAULT_RUN_STEP_BUDGET = 1024` turns into
  `RunLimitExceeded` (an error, runtime/mod.rs:199) if one drain exceeds
  it. This makes the budget a hard constraint on any bounded-rebuild
  design: a chunked rebuild of a full day (~16.9M batches / chunk) is
  thousands of self-requeued steps, so the worker loop change below is
  REQUIRED for the bars change, not merely nice for latency.

## Invariants to preserve

- Determinism: same raw + same seek target ⇒ identical bars/status/cursor
  state, regardless of chunk sizes. Chunking may change *when* cells are
  written, never *what* they converge to.
- The frame protocol (epoch reset / contiguous `from` / `total` / `live`)
  is unchanged. Lens accumulators need no changes for correctness.
- Cell ownership: only the feed writes `feed.*`, only each projection
  writes its own cells. No new cross-writers.
- FIFO of the external write channel (single mpsc, worker.rs:37): all of a
  component's earlier submits apply before its later ones. The regress
  redesign leans on this — documented below.

## Change 1 — runtime: bounded task steps + a responsive worker loop

`crates/runtime/src/{task.rs,runtime/mod.rs,worker.rs}`.

**1a. `TaskOutcome`.** `RuntimeTask::run_once` changes signature:

```rust
pub enum TaskOutcome {
    /// Work complete; sleep until a dependency changes.
    Idle,
    /// More work pending; re-queue this task after other queued work.
    WakeAgain,
}

async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<TaskOutcome, ComponentError>;
```

After a task returns `WakeAgain`, the runtime `enqueue_once`s it with a
new `TaskWake::SelfRequested` variant. `enqueue_once` already dedupes, so
a dependency change arriving mid-chain doesn't double-queue. All existing
`run_once` impls (BarsTask and the runtime crate's test tasks) return
`Ok(TaskOutcome::Idle)` where they returned `Ok(())`.

**1b. Worker pumping loop.** The worker's write-arm (worker.rs:62) and
QueueTask arm replace `run_until_idle(budget)` with a stepping loop that
stays responsive:

```rust
async fn step_until_idle(&mut self) -> Result<(), RuntimeError> {
    while !self.runtime.is_idle() {
        // New control writes land at the next step boundary instead of
        // waiting out an entire rebuild chain.
        while let Ok(batch) = self.writes.try_recv() {
            self.runtime.submit_external_writes(batch);
        }
        self.runtime.run_once().await?;
        // One task step can be CPU-heavy (a rebuild chunk); yield so
        // same-thread tokio tasks (cache watchers) are never starved.
        tokio::task::yield_now().await;
    }
    Ok(())
}
```

Because `Runtime::run_once` (runtime/mod.rs:144) applies **all** pending
external writes before running one task, a clock write pumped at a step
boundary is applied — and its cache watch fires — before the next rebuild
chunk runs. That is the whole unblocking mechanism.

The step-budget error disappears from this path (a self-requeueing task
makes step counts legitimately unbounded); `run_until_idle(max_steps)`
stays as-is for the `Drain { max_steps }` command so its callers keep
their explicit-budget semantics. `DEFAULT_RUN_STEP_BUDGET` remains only
for Drain-style callers.

The commands arm is intentionally NOT pumped mid-chain: a `status` pull
during a rebuild waits until idle. Acceptable — the clock/frame streams
are the UI's live channel; `status` is a debugging pull.

**Tests (runtime crate):**
- A task that returns `WakeAgain` N times runs exactly N+1 steps and then
  idles; a dependency change mid-chain doesn't duplicate it in the queue.
- Interleaving: install a `WakeAgain`-chaining task plus a value cell;
  submit an external write to that cell mid-chain from another tokio task;
  assert the write's watch fires before the chain completes (bounded by
  one chunk, not the whole chain).
- No `RunLimitExceeded` from the write arm for chains longer than the old
  budget.

## Change 2 — feed: chunked catch-up, truncating regress, `catchingUp`

`crates/ledger/src/feed/es_replay/{feed.rs,cells.rs}` (cursor type lives
with the cells/cursor definitions).

**2a. Cursor gains the flag.**

```rust
pub struct EsReplayCursor {
    ...
    /// True while the feed is emitting a backlog of already-due batches
    /// (after a forward seek, or when playback outruns pacing). Consumers
    /// should treat projection output as converging, not current.
    pub catching_up: bool,
}
```

**2b. Chunked emission.** Replace the per-batch emit loop (feed.rs:133)
with chunk assembly:

```rust
const CATCHUP_CHUNK_BATCHES: usize = 1024; // ~one write per chunk; tune freely

// Inside the outer loop, replacing the while-let emit loop:
let mut chunk = Vec::new();
while let Some(span) = event_store.batches.get(state.next_idx + chunk.len()) {
    if span.ts_event_ns > now || chunk.len() == CATCHUP_CHUNK_BATCHES {
        break;
    }
    chunk.push(feed_batch(...));           // events slice clone, as today
}
if !chunk.is_empty() {
    let more_due = next span (after chunk) exists && its ts_event_ns <= now;
    state.advance_by(chunk);               // next_idx, feed_seq, last_emitted_ts
    state.catching_up = more_due;
    // ONE submit: push_array(all chunk batches) + cursor + status.
    emit_chunk(...).await?;
    continue;                              // outer loop re-reads the clock
}
```

Properties this must satisfy:
- One cache transaction per chunk (batches + cursor + status in one
  `ExternalWriteBatch`), so the 20k-batch seek becomes ~20 transactions.
- `continue` after every chunk: the clock is re-read between chunks, so a
  backward seek or pause issued mid-catch-up takes effect within one chunk
  instead of after the entire pass.
- `catching_up` is true iff more due batches remain after this chunk. The
  final (possibly partial) chunk that reaches `now` writes it false. The
  regress path writes it false. Steady paced playback (one batch due at a
  time) never sets it.
- `feed_seq` increments per batch (not per chunk) so its meaning is
  unchanged.

**2c. Regress by truncation.** Replace clone-filter-replace with a
computed `remove_array_range`:

```rust
fn regress(&mut self, now: UnixNanos, event_store: &EsMboEventStore) -> Range<usize> {
    // All of this feed's emits are FIFO-ordered ahead of this write on the
    // single external-write channel, so at apply time the cache batches
    // array is exactly event_store.batches[0..self.next_idx]. The cut can
    // therefore be computed from the immutable event store: partition
    // point of ts_event_ns <= now within 0..next_idx (spans are
    // ts-ordered).
    let cut = event_store.batches[..self.next_idx]
        .partition_point(|span| span.ts_event_ns <= now);
    let removed = cut..self.next_idx;
    self.next_idx = cut;
    self.last_emitted_ts = (cut > 0).then(|| event_store.batches[cut - 1].ts_event_ns);
    self.epoch += 1;
    self.catching_up = false;
    removed
}
```

The submit becomes `remove_array_range(batches, removed)` + cursor +
status — O(dropped) and no `read_array` clone at all. The FIFO argument
above replaces the old comment about dropped in-flight emissions; keep it
in the code. `remove_array_range` validates the range at apply time, so a
broken invariant fails the component loudly (visible in
`componentStatus`) rather than silently corrupting — that is the desired
behavior.

**Tests (ledger crate, feed):**
- Forward seek across many batches produces ⌈n/chunk⌉ batch-array writes
  (observable via a cache watch counter), cursor converges to the same
  state as today, `catching_up` observed true then false.
- Backward seek: batches array equals the emitted prefix ≤ now; epoch
  bumped; cursor/status consistent; `catching_up` false.
- Mid-catch-up backward seek (write the clock between chunks using a
  paused/stepped clock fixture): the feed regresses without completing the
  stale pass.
- Determinism: chunked vs CHUNK=1 emission converge to identical
  bars/cursor state through a downstream bars projection.

## Change 3 — bars: bounded rebuild, ranged reads only

`crates/ledger/src/projection/bars.rs`.

The task becomes a small state machine. `run_once` dispatch:

```rust
const REBUILD_CHUNK_BATCHES: usize = 4096; // fold cost ~linear in events; keep a step in the low ms

enum Fold {
    /// Steady state: fold cursor advances incrementally (unchanged path).
    Incremental,
    /// Refolding after an epoch change: target epoch + next batch index.
    Rebuilding { epoch: u64, fold_idx: usize },
}
```

- **Epoch change or cursor regression detected** (cursor.epoch ≠ ours, or
  batch_idx < processed): enter `Rebuilding { epoch: cursor.epoch,
  fold_idx: 0 }`, reset fold state, and in the FIRST chunk's submit
  include `replace_array(bars, first_chunk_bars)` (+ live + status with
  the new epoch), so downstream sees the new epoch atomically with the
  reset. Subsequent chunks `push_array`. Return `WakeAgain` while
  batches remain below the rebuild target.
- **Each rebuilding step**: re-read the cursor first. If the epoch moved
  again, restart the rebuild against the new epoch (drop progress). The
  rebuild target is the *current* cursor.batch_idx read that step — the
  feed may still be appending; folding to the live target means the
  rebuild ends caught-up, and the normal incremental path takes over.
- **Reads**: only `read_array_range(feed.batches, fold_idx..chunk_end)`.
  The full-array `read_array` in the rebuild path is deleted; nothing in
  the projection reads the whole batch array anymore.
- **Status** is written every step (it already is), so
  `processed_batches` climbs visibly during a rebuild and the client's
  lag detection keeps working.
- The incremental path is unchanged apart from returning
  `TaskOutcome::Idle`.

**Tests (ledger crate, bars):**
- A rebuild over >2 chunks yields identical bars to a single-shot rebuild
  (chunk size 1 vs large, property-style on generated prints).
- Epoch moves again mid-rebuild → final state reflects the newest epoch
  only.
- Existing bars tests updated for the `TaskOutcome` signature; totals and
  live-bar semantics untouched.

## Change 4 — remux: expose `catchingUp`; no other protocol change

`crates/remux/src/session.rs`.

- `EsReplayCursorDto` gains `catching_up: bool` → `catchingUp`. It rides
  the existing `session/feed` notification and `status` pull; no new
  notifications, no frame-shape change.
- The bars watcher needs no change: chunked emission means it wakes per
  chunk, and its range-read already coalesces whatever completed since
  the last send into one frame.
- Update the spec doc(s): transport spec's cursor table gains the field.

**Tests (remux crate):**
- Cursor DTO passthrough (`catchingUp` present, correct casing).
- A forward-seek scenario asserts the barsFrame notification count for the
  seek is bounded (≪ batch count; assert against a generous ceiling tied
  to chunk math, not an exact figure, to stay robust).
- The 300-seek clock regression test and all existing tests stay green.

**Validation script** (`transport_validate.py`): assert per-projection
frame counts for the 20k-batch catch-up are < 500 (vs ~3,300 today);
assert `catchingUp` was observed `true` at least once and is `false` at
convergence; all existing totals assertions unchanged.

## Change 5 — lens: syncing semantics + ActionBar conformance

`lens/src/features/replay/`.

**5a. Wire + syncing.** `Cursor` type and parser gain `catchingUp:
boolean`. Syncing derivation becomes:

```
syncing = cursor.catchingUp
       || (any projection's processedBatches !== cursor.batchIdx, sustained > 250 ms)
```

The debounce kills the flicker risk during normal playback (status and
cursor arrive as separate notifications and briefly disagree);
`catchingUp` is authoritative and undebounced.

**5b. Presentation contract for catch-up** (applies to the Phase-1 cards
now; binding on Phase-2 chart layers): while `syncing`, the *data layer
keeps ingesting* (accumulators stay current — correctness machinery is
untouched) but the presentation layer does not animate intermediate
state: the chart layer (Phase 2) buffers and applies one snapshot when
syncing clears; the Phase-1 cards show their syncing pill and may keep
counters ticking (counters are honest progress; bars racing by are not).
The clock readout and scrubber are NOT gated: the clock cell is the
authority and jumps to the seek target immediately — only derived
projection output lags. Controls are never disabled by syncing.

**5c. ActionBar restyle.** The page loses its invented nav and adopts the
house pattern (reference: `day-action-bar.tsx`):

- **Delete the top header entirely.** Content area = projection cards
  (Phase 2: the chart) + the ended/failure banner. Nothing else.
- **One `ActionBar` pinned at the bottom** (the same
  `left`/`right`/`status` component days uses), with the days-style
  layout: `left` is a flex-col holding an upward panel above the pinned
  button row.
  - **Panel (always expanded while a session is up):** row 1 — the seek
    scrubber, full width. Row 2 — clock readout (ET time, mode, speed) on
    the left; market day + symbol on the right. The panel is bar surface,
    not page content, matching how days' selection panel rides the bar.
  - **Pinned row:** back-to-days `ActionButton` (ArrowLeft — the bar's
    leftmost pinned control, like days' pinned core buttons), play/pause
    `ActionButton` with `tone="primary"`, then the speed presets as
    compact buttons. Non-icon controls are allowed on the bar when they
    are styled to it — presets and slider qualify; keep them visually
    subordinate to the two ActionButtons.
  - **`status` slot:** feed progress (`batch 12,345 / 16,949,117`), the
    syncing indicator, and the `ended` marker. This is the bar's native
    status surface; use it instead of a floating footer row.
- Phase/ended/error handling is unchanged in behavior (toast + exit on
  open failure; banner + disabled transport on `closed`), but the banner
  renders in content and the disabling applies to the bar's controls.
- `useReplaySession`, `accumulator`, `api`, and `types` are untouched
  except the `Cursor` field; this is a presentation-layer rework.

**Verification:** `npm run typecheck && npm run lint && npm run build`
clean; prettier applied.

## Acceptance criteria (the pass is done when all hold)

1. Real-data validation reproduces the CLI totals exactly (274/5 bars,
   volume 2365, tradeCount 1125, live present, cursor at batch 20000) —
   with per-projection frame counts < 500 and `catchingUp` observed
   true → false.
2. A control write issued during a full-day-scale rebuild is applied and
   its clock notification delivered before the rebuild completes
   (runtime interleaving test proves the mechanism; a remux-level test
   proves it end to end: open → large forward seek → immediately set
   speed → the speed clock notification arrives before both projections
   converge).
3. Backward seek does not clone the batch array anywhere (code-level
   review criterion: no `read_array` of `feed.batches` remains outside
   tests).
4. No `RunLimitExceeded` is reachable from the worker's write arm.
5. Workspace tests green; remux crate tests green including the two new
   scenarios; lens typecheck/lint/build green.
6. The replay page renders no top nav; all controls live on the
   ActionBar; visual layout follows 5c.

## Non-goals (explicit backlog)

- **Prefix-preserving backward seek** (bars keeping its completed prefix
  and the frame protocol gaining a truncate op so the wire doesn't resend
  the full array): real win at end-of-day scale, but it changes the frame
  contract — design it with Phase-2 chart experience in hand.
- **Remux-side frame suppression during catch-up**: chunking reduces the
  flood ~40×; suppress-and-snapshot at the watcher adds protocol nuance
  for a payload the bridge can already carry. Measure after this pass.
- **Mixed-epoch one-frame window** (transport reviewer Finding 2, needs
  atomic multi-cell reads in cache) — unchanged status, pre-existing.
- Multi-worker runtime, projection dependency graphs, anything Phase 2.

## Implementation order

Single dependency chain, one worktree:

1. runtime (`TaskOutcome`, `TaskWake::SelfRequested`, worker stepping
   loop) — everything else builds on it.
2. ledger feed (chunking, truncating regress, `catching_up`) and bars
   (bounded rebuild) — can proceed in parallel after 1.
3. remux (DTO field, new tests, validation script update).
4. lens (cursor field + syncing debounce + ActionBar restyle).

Steps 1–3 are one codex-implement task (spec'd Rust with existing test
patterns); step 4 stays with the main session / frontend delegate (UI
taste on the bar layout). The real-data validation runs after step 3 and
again after step 4's build.
