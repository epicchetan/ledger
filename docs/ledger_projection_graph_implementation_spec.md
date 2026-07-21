# Static Ledger Projection Graph — Implementation Spec

Status: implemented and host-validated; on-device Gate A feel-check pending

Date: 2026-07-20

Baseline: `531997e` (`Publish immutable viewer bundle`)

## 1. Purpose

Formalize Ledger projections and their dependencies without introducing a
dynamic study system that the product does not use yet.

This pass has one user-facing compatibility gate: **Lens continues to request
and render `bars:1m`, and the refactored pipeline must prove that one-minute
bars are unchanged before additional interval capability is accepted.**

The implementation introduces:

1. a static, session-scoped projection plan resolved before playback starts;
2. one internal canonical-trade-print projection shared by every requested
   trade-derived projection;
3. an explicit, typed projection installation contract;
4. bars instances that consume the canonical trade-print stream instead of each
   rescanning ES MBO feed batches; and
5. headless proof that more than one time-bars interval can coexist in one
   session while sharing the single canonical-print dependency.

The target pipeline is:

```text
EsReplayFeed
  -> feed.databento.es_replay.batches
  -> CanonicalTradePrintsTask
  -> projection.trade_prints.prints/status
       -> BarsTask(bars:1m)
       -> BarsTask(bars:5m)        headless capability proof only
       -> future trade-derived projections
  -> projection delivery
  -> Remux
  -> Lens bars:v1 accumulator
  -> existing one-minute candlestick chart
```

The canonical-print node is the only shared base introduced in this pass.
There is no hidden one-second bars base and no automatic roll-up tree.

## 2. Relationship to Existing Specifications

All existing correctness work remains authoritative:

```text
docs/ledger_feed_system_implementation_spec.md
  server clock, ES replay feed, feed cursor/epoch, regression

docs/ledger_projection_system_implementation_spec.md
  canonical trade-print policy, current bars semantics and cells

docs/atomic_snapshot_projection_delivery_implementation_spec.md
  runtime ownership, atomic snapshots, delivery cadence, subscriptions,
  seek-final convergence, acknowledgments and resync

docs/ledger_session_transport_implementation_spec.md
  session open/control/status transport

docs/lens_replay_chart_implementation_spec.md
docs/lens_frontend_cleanup_implementation_spec.md
  Lens bars:v1 accumulation and chart behavior
```

This specification supersedes only these earlier structural choices:

- `BarsTask` no longer reads `EsMboFeedBatch` directly;
- projection dependencies are resolved explicitly rather than installed by
  ad hoc `builder.bars(...)` calls; and
- canonical trade-print extraction becomes an internal projection output
  instead of repeated math inside every bars task.

It does not supersede bar math, delivery semantics, or Lens rendering.

## 3. Decisions

These decisions are fixed for this pass:

1. **Projection graphs are immutable for a running session.** Public
   projections are supplied at session open and remain installed until session
   shutdown.
2. **Canonical trade prints are the single shared base.** Every bars instance
   folds the compact print stream directly.
3. **No time-bar roll-up hierarchy.** `bars:5m` does not depend on `bars:1s`.
4. **No dynamic runtime changes.** All cells are registered before the runtime
   takes ownership, and all tasks are installed before the feed starts.
5. **Projection definitions are compiled Rust modules.** There is no dynamic
   plugin loading, arbitrary JSON computation, or runtime factory discovery.
6. **Logical graph resolution belongs to Ledger.** Runtime dependencies remain
   ordinary typed cache keys.
7. **One canonical projection key means one computation per session.** Multiple
   receivers share that computation and retain independent delivery state.
8. **Computation and delivery remain separate.** Tasks write exact cache state;
   delivery sources sample committed state at viewer cadence.
9. **Lens remains on `bars:1m` and bars schema version 1.** No interval picker,
   tick chart, or frame-schema change is included.
10. **One-minute parity is the first gate.** Multi-interval proof follows only
    after the existing one-minute pipeline passes host and device validation.

## 4. Scope

### 4.1 In scope

```text
projection terminology and typed installation contract
static dependency expansion and deterministic install order
internal canonical-trade-print cells/task/status
exactly one canonical-print node per session when required
refactor time bars to consume the canonical-print stream
preserve current seconds/minutes/hours spec grammar and canonicalization
simultaneous headless bars:1m + bars:5m session proof
existing projection delivery for every public bars instance
CLI and Remux migration to the static projection plan
one-minute Lens compatibility and on-device validation
tests for forward play, pause, speed, seek, reload and regression
```

### 4.2 Explicitly out of scope

```text
adding/removing projections after session start
dynamic cache-cell registration
dynamic RuntimeTask installation for projections
task suspension, uninstall or reference-counted compute lifetime
changing attach identity or session replacement behavior
multiple active Ledger sessions
multiple Lens tabs or panels
Lens interval selection
tick/trade-count bars (`bars:200t`)
volume bars, range bars or other new bar completion rules
hidden bars:1s aggregation base
automatic dependency planning based on cost
projection artifacts persisted across sessions or days
generic study plugins or dynamically loaded projection code
bars wire schema version 2
removing the legacy session/bars diagnostic RPC
runtime, cache or store crate architecture changes
```

Tick bars are deliberately deferred, not rejected. When there is an active
consumer, they can be another parameterization of bars over the same canonical
print stream. They must not require a second shared base.

## 5. Terminology

```text
projection definition
  Compiled Ledger code describing one projection kind, its accepted
  parameters, dependencies, output cells, RuntimeTask and optional delivery
  source. `bars` is a definition.

projection instance
  One canonical parameterization installed into one session. `bars:1m` and
  `bars:5m` are different instances of the bars definition.

internal projection
  A graph node installed as a dependency but not requested or exposed as a
  viewer projection. Canonical trade prints are internal in this pass.

public projection
  A projection named in session open/attach identity and available to Remux
  projection subscriptions. Bars instances are public.

projection key
  The canonical, session-local identity used for dependency deduplication and
  component/cell naming.

projection output
  Typed cache-cell handles made available to dependent projection installers.

projection plan
  The immutable, deduplicated, topologically ordered set of internal and public
  nodes installed before a session starts.

receiver
  A projection delivery subscription. Receivers own delivery positions and
  cadence; they never own projection computation.

lineage
  The upstream feed epoch and processed feed-batch extent represented by a
  projection's committed output.
```

## 6. Load-Bearing Invariants

1. A session installs at most one node for each canonical projection key.
2. A public projection is never installed before all of its declared
   dependencies have installed successfully.
3. Canonical trade-print extraction occurs once per feed batch per session,
   regardless of the number of requested bars instances.
4. Canonical prints preserve their order from the ES replay feed.
5. Canonical print status advances its source lineage even when processed feed
   batches contain no prints.
6. The committed print count always equals the print-array extent:

   ```text
   trade_print_status.print_count == trade_prints.len()
   ```

7. A bars instance advances `processed_batches` to the canonical source
   lineage only after folding through the status's committed `print_count`.
8. Therefore a current bars status retains the existing convergence contract:

   ```text
   bars_status.epoch == feed_cursor.epoch
   bars_status.processed_batches == feed_cursor.batch_idx
   ```

9. A task writes only cells owned by its own component id.
10. Completed bars remain append-only within one epoch; regression may replace
    or truncate them during deterministic rebuild.
11. Related array/live/status changes commit in one task write batch.
12. Internal dependency nodes do not appear in public session projection lists,
    attach identity, projection subscription responses or Lens state.
13. Delivery cannot pace canonical-print or bars computation.
14. Refactoring the input source must not change one-minute bar values,
    boundaries, gaps, live-bar behavior, positions or frame schema.

## 7. Static Graph Model

### 7.1 Public specs remain user-facing

`ProjectionSpec` remains the parser-facing public enum:

```rust
pub enum ProjectionSpec {
    Bars(BarsParams),
}
```

The existing grammar remains unchanged:

```text
bars:<positive integer><s|m|h>
```

Canonicalization remains unchanged:

```text
bars:60s  -> bars:1m
bars:120m -> bars:2h
bars:90s  -> bars:90s
```

The implementation must not add a `t` unit in this pass.

### 7.2 Internal node identity

Add a Ledger-internal node enum. The exact names may differ, but the distinction
between public specs and internal nodes must remain:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ProjectionNodeSpec {
    CanonicalTradePrints,
    Bars(BarsParams),
}
```

Canonical node keys:

```text
CanonicalTradePrints -> projection.trade_prints
Bars(1m)              -> projection.bars.1m
Bars(5m)              -> projection.bars.5m
```

The internal trade-print node is not parseable through `ProjectionSpec::parse`.

### 7.3 Declared dependencies

Dependency declarations are explicit and parameter-independent for the first
graph:

```text
CanonicalTradePrints -> ES replay feed root
Bars(any time params) -> CanonicalTradePrints
```

The ES replay feed is a session input, not a projection node.

Conceptual API:

```rust
impl ProjectionNodeSpec {
    fn dependencies(&self) -> Vec<ProjectionNodeSpec> {
        match self {
            Self::CanonicalTradePrints => Vec::new(),
            Self::Bars(_) => vec![Self::CanonicalTradePrints],
        }
    }
}
```

### 7.4 Resolution

`ProjectionPlan::resolve` accepts already parsed public specs and performs:

1. canonical identity validation;
2. recursive dependency expansion;
3. internal-node deduplication;
4. cycle detection using `Visiting` / `Installed` states; and
5. deterministic topological ordering.

Examples:

```text
requested []
plan      []

requested [bars:1m]
plan      [CanonicalTradePrints, bars:1m]

requested [bars:1m, bars:5m]
plan      [CanonicalTradePrints, bars:1m, bars:5m]
```

Public duplicate handling remains at the current API boundary. For example,
`bars:60s` plus `bars:1m` is rejected as a duplicate canonical public spec.
Regardless of where that validation occurs, the planner must never install two
nodes with the same canonical key.

Public projection order in session responses remains request order. Internal
topological order is not exposed as API ordering.

### 7.5 No general-purpose registry

Do not introduce dynamic factory registration. Installation is an explicit
compiled match over the small known enum:

```rust
match node {
    ProjectionNodeSpec::CanonicalTradePrints => { /* trade_prints module */ }
    ProjectionNodeSpec::Bars(params) => { /* bars module */ }
}
```

This match is the catalog for this phase. It grows only when a real projection
is implemented.

## 8. Typed Installation Contract

### 8.1 Typed outputs

Dependent installers receive typed cache handles, not arbitrary JSON or string
key lookups:

```rust
pub(crate) enum ProjectionOutput {
    TradePrints(TradePrintCells),
    Bars(BarsCells),
}
```

Accessors return typed errors on a mismatched graph edge:

```rust
impl ProjectionOutput {
    fn trade_prints(&self) -> Result<&TradePrintCells, LedgerError>;
    fn bars(&self) -> Result<&BarsCells, LedgerError>;
}
```

The planner or installer retains outputs by `ProjectionNodeSpec` so a
downstream node can resolve its declared input.

### 8.2 Installation result

Generalize the current bars-only `InstalledProjection` shape so an internal
projection may have no viewer delivery source:

```rust
pub(crate) struct InstalledProjectionNode {
    pub node: ProjectionNodeSpec,
    pub output: ProjectionOutput,
    pub task: Box<dyn RuntimeTask>,
    pub delivery: Option<Box<dyn ProjectionDeliverySource>>,
}
```

Rules:

```text
CanonicalTradePrints
  task       yes
  output     TradePrintCells
  delivery   none
  public     no

Bars
  task       yes
  output     BarsCells
  delivery   BarsDeliverySource
  public     yes
```

The installation abstraction belongs in Ledger's projection module, not the
runtime crate and not the Remux adapter.

### 8.3 Session builder API

Replace adapter-side repeated `builder.bars(...)` orchestration with one
session-builder operation over the complete requested set. Conceptual shape:

```rust
pub fn projections(
    &mut self,
    feed: &EsReplayCells,
    requested: &[ProjectionSpec],
) -> Result<Vec<SessionProjectionOutput>, LedgerError>;
```

The result contains public specs and typed public outputs in request order so
CLI and Remux can retain their existing bars status/read behavior. Keep this
adapter-facing type separate from the internal dependency-output enum:

```rust
pub enum SessionProjectionOutput {
    Bars {
        canonical_spec: String,
        cells: BarsCells,
    },
}
```

Internally the builder:

1. resolves the static plan;
2. registers and installs nodes in topological order;
3. appends every node task to its task list;
4. appends only non-`None` delivery sources to delivery;
5. records only public canonical specs for session identity and seek barriers;
6. returns only public outputs to the caller.

The old public `builder.bars(feed, params)` convenience should be removed or
made a thin test-only/private wrapper through the planner. Production callers
must not bypass dependency resolution.

## 9. Canonical Trade-Print Projection

### 9.1 Module

Add:

```text
crates/ledger/src/projection/trade_prints.rs
```

The existing market policy remains in:

```text
crates/ledger/src/market/es_mbo.rs
```

The projection calls `canonical_trade_print`; it does not redefine Trade/Fill
semantics.

### 9.2 Component and cells

```text
component id: projection.trade_prints
owner:        projection.trade_prints

projection.trade_prints.prints
  Array<TradePrint>
  public_read: false

projection.trade_prints.status
  Value<TradePrintStatus>
  public_read: false
```

Proposed types:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradePrintStatus {
    pub epoch: u64,
    /// Source feed batches examined, including no-print batches.
    pub processed_batches: usize,
    /// Committed extent of `TradePrintCells::prints`.
    pub print_count: usize,
    /// Monotonic commit revision within this task instance.
    pub revision: u64,
    pub last_ts_event_ns: Option<UnixNanos>,
}

#[derive(Debug, Clone)]
pub struct TradePrintCells {
    pub prints: ArrayKey<TradePrint>,
    pub status: ValueKey<TradePrintStatus>,
}
```

The cells are cache-only. Do not add serialization traits or a public wire DTO
unless an implemented consumer requires them.

### 9.3 Separate domain data from feed chunking

The output is a flat ordered `Array<TradePrint>`, not one derived object per ES
feed batch. Feed batching is an ingestion/chunking concern; canonical prints
are the reusable market-domain data.

Benefits:

- downstream tasks read only the compact print suffix they have not folded;
- feed batches containing no prints add no empty objects to cache;
- future trade-count bars can count the same ordered print array directly; and
- other projections do not inherit an irrelevant ES feed batching model.

Source lineage remains explicit in `TradePrintStatus.processed_batches`. A
status commit may advance that field without increasing `print_count` when the
new source batches contain no canonical prints.

### 9.4 Task dependency and forward processing

`CanonicalTradePrintsTask` depends on the ES replay feed batches key and reads
the feed cursor to determine the committed target extent.

For each unprocessed `EsMboFeedBatch`:

1. preserve event order;
2. call `canonical_trade_print` for every event;
3. append every returned print to the ordered output suffix; and
4. advance source lineage even when no prints were returned.

The output suffix, `print_count`, source `processed_batches` and revision commit
atomically. `print_count` must equal the resulting array extent.

Process new input in bounded chunks. Start with the existing bars rebuild chunk
size (`4096` batches) unless real-data profiling demonstrates a reason to tune
it separately.

### 9.5 Regression and rebuild

When either condition is true:

```text
feed_cursor.epoch != task epoch
feed_cursor.batch_idx < processed_batches
```

the task resets and deterministically rebuilds the print stream from feed-batch
index zero through the committed target extent.

The first rebuild chunk replaces the trade-print array; subsequent chunks
append. Status advances with every committed chunk. `TaskOutcome::WakeAgain`
continues bounded rebuild without blocking snapshot service indefinitely.

The task must never expose a `print_count` beyond the committed output array.

### 9.6 Internal-only output

Canonical prints have no `ProjectionDeliverySource` in this pass. They do not
appear in:

```text
session open result
session attach identity
session status projections list
projection subscription negotiation
projection watermarks
Lens accumulators
```

Their value is shared server computation and typed input for other projections.

## 10. Bars Refactor

### 10.1 Input change only

Change `BarsTask` from:

```text
EsReplayCells -> scan EsMboEvent -> canonical_trade_print -> fold Bar
```

to:

```text
TradePrintCells -> iterate TradePrint -> fold Bar
```

The task descriptor depends on `projection.trade_prints.status`. `BarsTask`
retains an internal `processed_prints` cursor. On each wake it reads the source
status and then reads exactly:

```text
projection.trade_prints.prints[processed_prints .. status.print_count]
```

Because the print suffix and status are committed atomically, a task wake
observes a coherent source extent. After folding through `status.print_count`,
the bars task stamps its public `processed_batches` from the source status. If
source batches advanced without producing a print, bars still advances lineage
and revision without changing completed or live bars.

On epoch change or `status.print_count < processed_prints`, bars resets and
rebuilds from the beginning of the current canonical print array. Bounded
rebuild state tracks print extent internally; it reports the source feed-batch
lineage only after folding through the corresponding committed print count.

### 10.2 Preserved bars contract

The following must not change:

- `BarsParams { interval_ns }` in this pass;
- seconds/minutes/hours parsing and canonicalization;
- UTC epoch bucket alignment;
- `Bar` fields and integer price ticks;
- open/high/low/close and aggressor-volume math;
- gaps for intervals with no prints;
- one live, uncompleted final bar;
- immutable completed bars within an epoch;
- current defensive handling of an older event-time bucket;
- `BarsCells` names and ownership;
- `BarsStatus` JSON shape and field meanings;
- `bars` delivery kind, schema version 1 and frame semantics;
- delivery positions and base/head validation;
- snapshot/suffix/resync/seek-final behavior.

### 10.3 Status lineage

`BarsStatus.processed_batches` continues to mean source ES replay feed batches
represented. Its value is copied from `TradePrintStatus.processed_batches`
only after bars has folded through the source status's `print_count`.

This is required to preserve:

```text
BarsDeliveryPosition.processed_batches
projection watermarks
seek barrier equality with feed cursor
Lens BarsPosition parsing and validation
```

Do not rename the field or change the wire schema.

### 10.4 Multiple time instances

Each requested canonical bars spec installs its own:

```text
BarsCells
BarsTask
BarsDeliverySource
```

All instances receive the same `TradePrintCells` dependency.

For:

```text
requested [bars:1m, bars:5m]
```

the component set must contain exactly:

```text
projection.trade_prints
projection.bars.1m
projection.bars.5m
```

There must not be one canonical-print task per bars interval.

## 11. Runtime and Cache Boundary

No changes are required in `crates/runtime` or `crates/cache`.

The existing capabilities are sufficient:

```text
Cache registration before RuntimeWorker ownership
TaskDescriptor cache-key dependencies
dependency-wake deduplication
atomic component write batches
TaskOutcome::WakeAgain bounded catch-up
owner-executed atomic snapshots
```

All projection cells are registered while `LedgerSessionBuilder` still owns the
setup cache. Tasks are installed in topological order before the ES replay feed
process starts.

Do not add:

```text
Runtime projection concepts
dynamic registration commands
task dependency introspection APIs
task uninstall
generic DAG execution
```

Cycle detection and typed dependency errors are Ledger planner concerns.

## 12. Delivery and Seek Convergence

### 12.1 Delivery sources

Only public bars nodes contribute delivery sources. The current session-owned
delivery executor remains one worker containing a source keyed by each public
canonical spec.

No delivery source is created for canonical trade prints.

### 12.2 Transitive convergence

The current seek barrier may continue to require only public bars specs:

```text
feed cursor current at target
bars status epoch == feed epoch
bars status processed_batches == feed cursor.batch_idx
```

A bars task cannot reach that extent until its canonical-print dependency has
published and the bars task has folded the matching output. Bars convergence is
therefore transitive proof that the internal dependency converged.

Do not add the internal trade-print node to public watermarks or seek requests.

### 12.3 Delivery payload compatibility

The bars delivery payload remains:

```text
completed bars suffix or snapshot
latest live bar
BarsStatus
```

Lens must continue to parse `kind = "bars"`, `schemaVersion = 1`. No payload
field, operation, position or reason changes are allowed in this pass.

## 13. Remux, CLI and Lens Integration

### 13.1 Remux

Remux continues to parse and validate public `ProjectionSpec` values. Instead
of matching each spec and calling `builder.bars(...)`, it passes the complete
parsed public set to the builder's projection-plan installation method.

Remux retains typed public `BarsCells` for current status and the diagnostic
bars read. It never stores or exposes `TradePrintCells`.

Open and attach continue to compare only the canonical public projection set.
For the current Lens request that remains:

```json
["bars:1m"]
```

### 13.2 CLI

The replay CLI uses the same planner as Remux. It must not retain a parallel
manual projection installer.

CLI output continues to summarize only requested public projections. Internal
canonical print state may be covered by tests or debug metrics but is not added
to the stable command JSON in this phase.

### 13.3 Lens

Lens remains intentionally unchanged in product behavior:

```ts
const REPLAY_SPECS = ["bars:1m"]
```

No interval UI, new accumulator, tick coordinate or chart-layer behavior is
introduced.

The implementation may add a narrow regression assertion that the constant is
still `bars:1m`, but it should not generalize Lens state ahead of an active UI
need.

## 14. One-Minute Parity Gate

The implementation is split by a mandatory gate.

### Gate A — refactored one-minute bars

Before accepting the multi-interval proof, all of the following must pass with
only `bars:1m` requested:

1. Existing bars unit/integration tests.
2. Existing projection delivery tests.
3. Existing session open, attach, status, seek and reload tests.
4. A direct parity test comparing one-minute bars from:

   ```text
   reference: raw fixture events -> canonical_trade_print -> existing fold math
   actual:    feed -> canonical print stream -> refactored BarsTask
   ```

5. Exact equality for every `Bar` field, completed-bar count and live bar.
6. Exact equality after forward playback and backward seek rebuild.
7. Unchanged `BarsStatus` and delivery position semantics.
8. Lens typecheck, lint, test/build checks currently used by the repository.
9. Host real-data replay at pause, 1x and 5x.
10. Device reload while playing at 5x, confirming:

    ```text
    same session and chart route
    same server-side clock/speed/mode
    candles filled immediately from authoritative projection delivery
    playback continues progressing after reload
    no seek rendered as fast replay
    no persistent syncing spinner after convergence
    ```

If Gate A fails, stop and fix one-minute parity. Do not explain a changed candle
as expected fallout from the graph refactor.

### Gate B — additional interval capability

Only after Gate A passes, prove the static graph can install at least two time
intervals in one headless session:

```text
bars:1m
bars:5m
```

Gate B does not change Lens. It validates server capability only.

## 15. Test Plan

### 15.1 Projection planning tests

- no public specs resolve to an empty plan;
- one bars spec resolves canonical prints before bars;
- two bars specs resolve one canonical-print node and two bars nodes;
- plan order is deterministic;
- canonical duplicate public specs are rejected;
- the same internal dependency is never installed twice;
- a synthetic cycle is rejected if the resolver is implemented generically;
- internal trade prints cannot be parsed as a public projection spec;
- returned public outputs preserve request order and exclude internal nodes.

### 15.2 Canonical trade-print task tests

- Trade with valid price and nonzero size is preserved;
- Fill is excluded;
- missing-price and zero-size Trade are excluded;
- print order matches source event order;
- aggressor side, timestamp, price ticks and size are unchanged;
- output contains only canonical prints and no batch wrapper objects;
- a no-print input batch advances source lineage without growing the array;
- `status.print_count` equals output array length;
- incremental forward input reads only the new source range;
- several feed batches in one write produce the corresponding ordered suffix;
- epoch regression replaces stale output;
- backward target extent truncates/rebuilds correctly;
- bounded rebuild yields and eventually converges;
- final status extent equals feed cursor extent.

### 15.3 One-minute bars parity tests

- exact one-minute bucket boundaries;
- exact OHLC values;
- exact total/buy/sell volume and trade count;
- exact first/last event timestamps;
- gaps remain gaps;
- final bar remains live;
- no-print source batches advance lineage without changing bars;
- live-only revisions still advance delivery revision;
- completed bars remain append-only within one epoch;
- regression produces the same state as fresh replay to the target;
- replay speed does not change the result;
- full-day result matches a pure reference fold over canonical prints.

### 15.4 Multi-interval tests

- `bars:1m` and `bars:5m` share identical `TradePrintCells` handles;
- runtime component listing contains one trade-print task and two bars tasks;
- both bars statuses converge to the same feed epoch/batch extent;
- each interval has correct independent completed/live bar state;
- subscribing to each spec returns its own valid bars:v1 stream;
- two receivers of the same spec do not create another runtime task;
- seek waits for both public requested projections when both are installed;
- attach succeeds with the same public set and rejects a changed public set;
- session responses never include `projection.trade_prints`.

### 15.5 Delivery regression tests

- initial one-minute subscription emits an authoritative snapshot;
- suffix delivery still starts at the acknowledged completed-bar extent;
- live-only changes produce valid append envelopes with zero completed bars;
- resync remains authoritative;
- seek emits only the final converged projection snapshot;
- canonical-print task revisions never emit viewer frames;
- one receiver's head/FPS/lease remains independent of another receiver.

### 15.6 Real-data validation

Use an existing prepared ES market day and record:

```text
feed batch count
canonical print count
completed/live one-minute bar count
projection convergence time on initial open
projection convergence time on mid-session forward seek
projection convergence time on backward seek
runtime task steps by component
delivery frames and snapshots
```

Compare the one-minute bar output against the pre-refactor reference, not just
visual similarity. Investigate any material performance regression for the
single active one-minute Lens projection before accepting the phase.

## 16. Failure Handling

```text
invalid public projection spec
  existing typed invalid-params error before session replacement

duplicate canonical public spec
  existing typed duplicate error; no session starts

dependency cycle
  Ledger projection-plan error before cells/tasks install

missing or wrong typed dependency output
  Ledger installation error naming the node and required output kind

duplicate node key/cell registration
  installation error; never silently create a second computation

canonical print task failure
  runtime component failure; dependent bars cannot falsely report convergence

bars task failure
  existing runtime/delivery failure behavior; no sent-head advance

one-minute parity mismatch
  implementation gate failure; do not proceed to multi-interval acceptance
```

Plan resolution should finish before session replacement/destructive open work
where practical, preserving the current rule that malformed projection input
does not tear down a valid active session.

## 17. Observability

Use existing runtime and delivery metrics. Add only projection-specific counters
that help validate the new shared work:

```text
canonical source batches processed
canonical prints emitted
canonical no-print source batches processed
canonical rebuild chunks
bars input prints processed by spec
bars prints folded by spec
```

Counters may be test/debug metrics and do not need to become stable Remux DTOs.
Do not add per-print logging.

The critical proof is structural and measurable:

```text
number of bars instances may grow
canonical MBO scan task count remains one
```

## 18. Implementation Sequence

### Phase 1 — Freeze one-minute reference behavior

1. Add or strengthen a pure fixture/reference fold for current `bars:1m`.
2. Record exact completed and live outputs for representative edge cases and a
   prepared real day.
3. Confirm current host checks before changing the input pipeline.

Deliverable: executable parity oracle.

### Phase 2 — Projection plan and installation contract

1. Add internal node identity and dependency declarations.
2. Add static plan resolution, deduplication and topological ordering.
3. Add typed projection outputs.
4. Generalize installed nodes to optional delivery sources.
5. Add builder installation of a complete requested set.

Deliverable: static graph can be inspected/tested before feed start.

### Phase 3 — Canonical trade-print projection

1. Add flat print cells and source-lineage status types.
2. Add incremental and rebuild task behavior.
3. Install it automatically for any requested bars instance.
4. Verify ordered print extent and source-lineage invariants, including
   no-print source batches.

Deliverable: one shared internal canonical print stream.

### Phase 4 — Refactor bars input

1. Replace `EsReplayCells` input with `TradePrintCells`.
2. Remove repeated `canonical_trade_print` calls from `BarsTask`.
3. Preserve bars cells, status, delivery and wire types exactly.
4. Migrate Ledger tests through the graph installer.

Deliverable: `bars:1m` computed through the new dependency graph.

### Phase 5 — Gate A validation

1. Run one-minute parity tests.
2. Run all Rust formatting, lint and tests.
3. Run Lens typecheck, lint, tests/build.
4. Run host real-data validation.
5. Perform the device replay/reload/seek feel-check.

Deliverable: explicit evidence that one-minute behavior is unchanged.

### Phase 6 — Multi-interval server proof

1. Install `bars:1m` and `bars:5m` together in a headless session test.
2. Prove one canonical-print task is shared.
3. Prove independent bar states and delivery sources converge.
4. Keep Lens requesting only `bars:1m`.

Deliverable: capability for more time intervals without product UI expansion.

### Phase 7 — Adapter cleanup and documentation

1. Migrate Remux and CLI to the shared builder plan.
2. Remove production paths that manually install bars dependencies.
3. Update this document's implementation record with deviations and measured
   validation results.

Deliverable: one projection installation path across Ledger adapters.

## 19. Required Verification Commands

Use repository-standard commands discovered from the workspace during
implementation. At minimum, complete the equivalent of:

```text
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings

Lens package install/check policy already used by the repository:
  typecheck
  lint
  tests, when present
  production build
```

Do not assume passing unit tests is sufficient; Remux session tests and Lens
build validation are part of Gate A.

## 20. Acceptance Criteria

The phase is complete only when:

1. Ledger has a documented static projection definition/instance/dependency
   contract.
2. A fixed public projection set resolves before session start into a
   deterministic, deduplicated graph.
3. Canonical trade prints are materialized once per session and remain internal.
4. Bars tasks consume the canonical print stream and never scan raw MBO events.
5. `bars:1m` produces byte-for-byte/equality-equivalent `Bar` values and the
   same live/completed semantics as the reference pipeline.
6. Bars delivery remains schema version 1 and Lens code remains on
   `REPLAY_SPECS = ["bars:1m"]`.
7. Existing play, pause, speed, seek, reload, attach, resync and delivery
   behavior passes.
8. A session can install `bars:1m` and `bars:5m` simultaneously.
9. Those two bars instances share exactly one canonical-print task/output.
10. Public session APIs list only `bars:1m` and `bars:5m`, never the internal
    dependency.
11. Runtime and cache crates require no projection-specific changes.
12. Host and device validation find no material one-minute usability or
    convergence regression.

## 21. Deferred Extensions

Add only when demanded by product work:

```text
trade-count bars
  Extend bars parameters with Trades(count), consume CanonicalTradePrints
  directly, and define a non-colliding horizontal bar identity before Lens
  renders them.

other trade-derived projections
  Declare CanonicalTradePrints as a dependency and install through the same
  static plan.

derived projection dependencies
  A real projection may declare bars:1m or another typed output as a dependency.
  Add the edge only for an implemented consumer.

shared aggregate projections
  Introduce only after two active projections need the same aggregation or
  profiling proves repeated compact-print folding is material.

dynamic session graphs
  Requires owner-executed cell registration, initial wake for late tasks,
  delivery-source registration and lifecycle semantics. Not assumed here.

multiple active sessions
  Separate session graph instances; immutable store artifacts may still be
  shared, live cache truth may not.

projection artifacts
  Persist by dataset + canonical key + compute version after real history or
  expensive studies create a need.
```

## 22. Implementation Handoff Checklist

Before editing code:

- re-read this spec and the atomic delivery invariants;
- confirm the worktree and preserve unrelated changes;
- freeze current one-minute expected outputs;
- identify every `builder.bars(...)` production/test caller;
- confirm Lens still requests only `bars:1m`.

During implementation:

- keep graph planning in Ledger;
- keep canonical trade policy in `market/es_mbo.rs`;
- preserve ordered prints and separate source lineage from print extent;
- keep internal outputs out of public session identity;
- run Gate A before claiming multi-interval completion;
- update this spec if an implementation constraint changes a contract.

Before handoff:

- run the complete Rust and Lens checks;
- record one-minute parity evidence;
- record the two-interval component graph evidence;
- perform the device reload/seek validation;
- leave changes uncommitted unless explicitly asked to commit.

## 23. Implementation Record

Implementation date: 2026-07-20

All changes remain uncommitted pending review.

### 23.1 Static graph and installation contract

Implemented in Ledger:

- `ProjectionNodeSpec` distinguishes the internal canonical-trade node from
  public parameterized bars instances;
- `ProjectionPlan::resolve` expands dependencies, rejects duplicate canonical
  public specs, detects cycles, deduplicates nodes and produces deterministic
  dependency-first order;
- typed `ProjectionOutput` values connect installers without string-key or JSON
  lookups;
- `InstalledProjectionNode` carries a task, typed output and optional delivery
  source;
- `LedgerSessionBuilder::projections` installs the complete immutable graph
  once and returns only public outputs in request order; and
- the old production `builder.bars(...)` installation path was removed.

No runtime or cache architecture changes were required. All nodes still
register before `RuntimeWorker` takes cache ownership, tasks install in plan
order, and the existing runtime dependency scheduler executes their cache-key
edges.

### 23.2 Canonical trade-print projection

Added `crates/ledger/src/projection/trade_prints.rs` with:

```text
projection.trade_prints.prints  Array<TradePrint>        private
projection.trade_prints.status  Value<TradePrintStatus>  private
```

The task scans each new ES feed batch once, applies the existing
`canonical_trade_print` policy, preserves print order and atomically commits the
flat print suffix with source lineage and exact print extent. Forward work and
regression rebuilds are bounded to 4,096 source batches per task step.
No-print batches advance lineage/revision without adding array entries.
Epoch or source-extent regression replaces and deterministically rebuilds the
stream. The node has no delivery source and remains absent from every public
session identity and viewer protocol.

Tests cover accepted and rejected event forms, order, no-print lineage,
`print_count == prints.len()`, private cell descriptors, incremental work,
multi-chunk rebuild and epoch replacement.

### 23.3 Bars input refactor and compatibility

`BarsTask` now depends on the canonical print status and folds only the unseen
flat print suffix. It retains an internal `processed_prints` cursor while
copying source ES batch lineage into the unchanged public
`BarsStatus.processed_batches` field only after folding the corresponding
committed print extent.

The bars cells, owner ids, bar math, gap behavior, live/completed split,
revision semantics, delivery schema version 1, positions, seek barriers and
snapshot/suffix behavior are unchanged. Backward rebuilds are bounded to 4,096
prints per task step; intermediate rebuild status does not claim source lineage
that has not yet been folded.

Gate A includes an independent raw-event reference fold for `bars:1m`. The
graph result matches every completed/live `Bar` field at the end of the
fixture, after a backward seek, and after replaying forward again. Existing
forward, chunked catch-up, regression, epoch-replacement and delivery tests
also remain green.

### 23.4 Multi-interval proof

The headless capability proof requests `bars:1m` and `bars:5m` together and
verifies:

```text
one projection.trade_prints task/cell set
independent projection.bars.1m cells/task/delivery source
independent projection.bars.5m cells/task/delivery source
both statuses converge to the same feed epoch/batch extent
subscription negotiation preserves public request order
each source emits its own bars:v1 initial and seek-final frame
internal print cells are private and absent from subscriptions
```

CLI and Remux now pass the complete parsed public set through the same builder
planner. Lens remains unchanged at `REPLAY_SPECS = ["bars:1m"]`.

### 23.5 Validation

Host checks completed:

```text
cargo fmt --all -- --check                                  pass
cargo test --workspace                                      pass (231 tests)
cargo clippy --workspace --all-targets --all-features
  -- -D warnings                                            pass
Lens typecheck                                              pass
Lens lint                                                   pass
Lens tests                                                  pass (11 tests)
Lens production build                                       pass
```

Rust 1.96 enabled several new strict Clippy lints in unchanged workspace code.
The implementation pass applied only the suggested semantics-preserving
compatibility cleanup: sort-key forms, a redundant conversion removal,
`is_multiple_of`, small test-only type aliases, an explicit intentional
argument-count allowance, and suppression of per-integration-test support
helpers that are necessarily unused by some test binaries.

Real-data Gate A used the existing prepared ES market day for 2026-03-10:

```text
feed batches                    18,858,570
canonical prints / trade count     704,860
bars:1m completed                     1,379
bars:1m live                              1
total volume                      1,811,043
first bar start         1773093600000000000
last/live bar start     1773176340000000000
seek barrier duration              2.656 s
delivery frames                          1
outbound backpressure                    0
```

These one-minute totals and endpoints exactly match the pre-refactor recorded
baseline. The prior recorded seek-barrier sample was 2.542 seconds; the new
debug run was about 4.5% slower. That small single-sample delta does not show a
material regression on its own. Delivery coalesced 4,606 bars-status dirty
notifications into one authoritative frame.

The same real day also passed with `bars:1m` and `bars:5m` installed together:

```text
bars:1m  1,379 completed + live, volume 1,811,043, trades 704,860
bars:5m    275 completed + live, volume 1,811,043, trades 704,860
```

### 23.6 Remaining device validation

The architecture and host implementation are complete. The on-device Gate A
feel-check remains manual:

1. play `bars:1m` at 5x and seek mid-session;
2. reload the native viewer and confirm the same route/session, clock mode,
   speed and time resume with an immediately hydrated chart;
3. leave playback running while the screen is closed, return/reload and confirm
   server-side progression;
4. perform a large seek and confirm only the final converged bars snapshot is
   painted; and
5. return to days and re-enter the market day to confirm intentional fresh
   session behavior.

No multi-interval Lens UI or tick-bar behavior was introduced.
