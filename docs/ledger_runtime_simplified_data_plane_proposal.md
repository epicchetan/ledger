# Ledger Runtime Alignment Proposal

This document is the high-level alignment note for the new Ledger runtime path.
It is intentionally conceptual. Detailed implementation requirements live in
the narrower specs:

```text
docs/store_implementation_spec.md
docs/ledger_feed_system_implementation_spec.md
```

This document describes the shape we are moving toward and the responsibility
boundaries we should keep in mind while replacing the old replay/session system.

## 1. Current Direction

Ledger should move away from the old replay-specific runtime.

The runtime should be generic:

```text
feeds write source state
projections write derived state
the runtime schedules work from changed keys
the data plane stores typed cells
read/output layers observe public state
```

The runtime should not know about ES, MBO, candles, 0DTE levels, order books,
execution simulation, validation reports, or Lens chart payloads.

Those concepts belong in concrete feeds, concrete projections, or application
composition above the generic runtime.

The generic core is:

```text
DataPlane
  named typed cells
  value cells
  array cells
  cell ownership
  changed-key effects

Runtime
  queued external write batches
  projection registry
  dependency index
  duplicate-safe projection queue
  deterministic run loop

Projection
  declares dependency keys
  receives a ProjectionContext
  reads/writes typed cells through that context
```

The next major concept is feeds.

A feed should be another generic producer around the data plane. A feed does not
need to be a replay feed. It can be a replay market-data feed, live feed, 0DTE
levels feed, file-backed context feed, journal/user-action feed, or anything
else that can produce data-plane writes.

## 2. What Exists Now

`crates/ledger-runtime` currently contains a small synchronous generic runtime.

Implemented:

```text
Key
CellOwner
CellDescriptor
ValueKey<T>
ArrayKey<T>
DataPlane
WriteEffects
Projection trait
ProjectionContext
ExternalWriteBatch
Runtime
RuntimeStep
RunStats
```

The data plane has two primitive cell kinds:

```text
Value<T>
  optional current value
  read / set / clear / update

Array<T>
  ordered mutable collection
  read all / read range / replace / push / insert / replace range /
  remove range / clear / update
```

Array is intentionally not an append-only stream primitive. Some source arrays
will be append-only by convention. Other arrays, like heatmap rows or editable
projection state, may need inserts, removals, or range replacement.

The data plane does not have:

```text
schemas
stream cursors
notify keys
Lens-specific read APIs
cross-cell transactions
durable persistence
domain payload enums
```

The runtime currently accepts external write batches. These batches are just
typed data-plane mutations submitted from outside the runtime. They are not
special "origin" domain objects.

The current owner enum still calls external source ownership
`CellOwner::Origin(...)`. That is a generic external-writer owner today, not a
commitment that the long-term concept must be called "origin" instead of
"feed".

The scheduler behavior is intentionally small:

```text
apply queued external writes in FIFO order
collect changed keys
queue projections whose dependencies changed
run at most one queued projection per run_once
dedupe projections while queued
pass projection changed keys back into dependency scheduling
stop after a caller-provided run_until_idle limit
```

This gives us a testable generic coordinator without introducing async,
websockets, persistence, replay controls, or domain logic.

## 3. Important Runtime Rule

The runtime only sees changes that pass through it.

```text
Runtime::submit_external_writes(...)
Projection::run(...) through ProjectionContext
```

Direct `DataPlane` mutation is still possible because the data plane is a useful
standalone primitive. Once a runtime is active, direct mutation should be treated
as setup/test-only, because it bypasses dependency scheduling.

## 4. Feeds

A feed is a source adapter that produces data-plane writes.

Conceptually:

```text
Feed
  owns source-specific state
  owns source-specific cursors
  may use storage/files/network/model state
  converts source changes into ExternalWriteBatch values
  submits those batches to the runtime
```

The feed should own its own cursor. The runtime should not invent a generic
cursor abstraction. If a replay feed needs a file offset, event index, or source
timestamp, that is feed state. If a 0DTE feed needs model version or snapshot
time, that is feed state.

The same principle already applies to projections: if a projection needs to
remember how far it processed an input array, that cursor is projection-owned
state, not runtime state.

The runtime boundary stays simple:

```text
feed does source work
feed builds an ExternalWriteBatch
runtime applies the batch
changed keys wake dependent projections
```

Possible future feed examples:

```text
ReplayMarketDataFeed
  reads historical source files
  writes origin market-data cells

LiveMarketDataFeed
  reads a live adapter
  writes the same or compatible market-data cells

ZeroDteFeed
  reads options/model inputs
  writes level or heatmap cells

JournalFeed
  receives user/journal actions
  writes context cells
```

The generic feed trait should be specified separately before implementation.
The first version should avoid forcing async into `ledger-runtime`; an
application layer can run feeds and runtime steps in tasks or threads.

## 5. Source Preparation Versus Runtime Delivery

Do not collapse all source work into one method called "ingest".

There are two different concerns:

```text
prepare / load / hydrate
  uses files, catalog metadata, object storage, network, caches, decoding

advance / poll / pump
  produces data-plane writes for the runtime
```

For replay, preparation might locate or register raw files. Runtime delivery
might read from those files and write source batches into the data plane.

For 0DTE, preparation might locate a chain snapshot or model input. Runtime
delivery might write level arrays or heatmap cells.

The feed can use the file/object layer, but the file/object layer should not
know feed semantics.

## 6. Data Center Direction

Data Center should become more of a Ledger file/object explorer.

For the next cleanup, it should not be responsible for proving replay
readiness, running old validation, or presenting a trader trust score.

The simpler Data Center shape is:

```text
registered raw files
file/object metadata
hashes
sizes
source labels
paths / remote keys
job history
delete/cache/staging controls where still useful
```

No preprocessing should be required just to show a Data Center row.

Preprocessing, decoding, replay delivery, and domain interpretation should move
behind concrete feeds. A raw file can exist before any feed knows how to use it.

That makes the UI more generic:

```text
Data Center:
  what files/objects does Ledger know about?

Feed runtime:
  which feed can use which objects and what cells does it write?

Lens renderers:
  which public cells or frames does this view know how to render?
```

## 7. Retired Replay, Book, Ingest, And Validation Path

The old replay system is no longer the target architecture and its crates have
been removed from the active workspace.

Concepts that should stay retired unless they return behind the new feed/runtime
boundaries:

```text
session-specific replay runtime
book-check artifact generation
replay readiness/trust status as a product concept
Data Center validation actions and validation UI
session-specific websocket surface
CLI commands that depend on session-specific runtime behavior
```

The useful part to preserve is not the old replay simulator. The useful part is
the idea that Ledger can register durable source objects and later let a feed
interpret them.

The likely near-term data lifecycle becomes:

```text
register raw file/object
show it in Data Center
feed opens it when a runtime session needs it
feed writes generic data-plane cells
projections derive from those cells
Lens renders public state
```

If preprocessing artifacts are needed later, they should be introduced as feed
owned or feed requested artifacts, not as a global Data Center requirement.

## 8. Crate Responsibility Direction

The old crate rationale is changing.

Current names may change, but the responsibility split should stay clear:

```text
generic data plane
  keys, typed cells, ownership, changed-key effects

generic runtime
  external writes, projection dependencies, projection queue, run loop

file/object layer
  local/remote object registration
  paths
  hashes
  metadata
  raw file explorer behavior

Ledger application crate
  concrete feeds
  concrete projections
  market-specific payload types where needed
  app composition

API / websocket layer
  generic object exploration routes
  runtime/session control routes
  frame/read delivery

Lens
  UI shell
  object explorer
  feed-specific controls
  projection-specific renderers
```

The old `domain` crate should not become the generic data plane. If market
domain types are no longer broadly shared, they can move into the concrete
Ledger application/feed/projection code that needs them.

Repurposing `domain` as infrastructure would keep old ambiguity alive. A clean
generic crate boundary is easier to reason about.

## 9. Naming Notes

Some names are now settling:

```text
store
  Ledger's generic object registry and local cache

ledger-plane
  generic data-plane crate

ledger-cells
  more literal name for typed cells and ownership

ledger-state
  generic runtime state, clear but broad

ledger-runtime
  scheduler and orchestration around the data plane
```

`ledger-stash` is possible, but it sounds less central than the role this layer
plays. The data plane is not hidden storage; it is the shared state surface that
feeds, projections, reads, and possibly persistence all meet around.

For now, the implemented code lives in `crates/ledger-runtime`. A split should
only happen when it removes confusion or simplifies dependencies.

## 10. Persistence Direction

The data plane should not become the file/object store.

The data plane should stay fast and bounded:

```text
registered typed cells
current runtime state
owner-checked mutation
changed-key effects
```

Persistence can be added around it later:

```text
snapshot selected cells
journal selected writes
hydrate selected cells from object metadata
persist feed state
persist projection state
```

That should be an adapter or composition layer, not a reason for every data
plane write to know about storage.

The important split:

```text
file/object layer:
  durable bytes and metadata

data plane:
  active runtime cells

feed:
  interpretation of durable bytes into runtime writes

projection:
  interpretation of runtime inputs into derived runtime state
```

## 11. API And Frame Delivery

The API layer should not globally understand every market concept.

It can expose generic surfaces:

```text
object explorer
registered feeds
runtime/session controls
public cell descriptors
public reads
frame streams
job history
```

Concrete route payloads can still exist when useful, but the center should not
require API DTOs for every domain object. A Lens renderer can know how to render
bars, heatmaps, levels, or other projection outputs based on the projection/feed
contract.

Frame delivery should remain a read/output concern:

```text
runtime changes keys
output layer observes or receives changed-key summaries
output layer reads public cells
output layer serializes frames for Lens
```

The data plane should not know about websockets.

## 12. First Generic Runtime Flow

The first feed/runtime/projection loop should look like this:

```text
1. App creates DataPlane.
2. App registers feed-owned source cells.
3. App registers projection-owned derived cells.
4. App registers projections with dependency keys.
5. Feed prepares or opens its source.
6. Feed produces an ExternalWriteBatch.
7. Runtime applies the external write batch.
8. Runtime schedules projections whose dependency keys changed.
9. Runtime runs queued projections.
10. Projections write derived cells through ProjectionContext.
11. Runtime schedules downstream projections from changed keys.
12. Output/API layer reads public cells or changed-key summaries.
```

The runtime does not care what the cells contain.

Example cell names are conventions, not runtime rules:

```text
origin.market.batches
origin.market.status
projection.bars.current
projection.bars.completed
projection.gamma.heatmap
runtime.error
```

## 13. What Should Stay Flexible

Do not over-specify these yet:

```text
feed trait shape
feed lifecycle names
async strategy
API websocket protocol
frame format
data-plane persistence
crate renames
Lens renderer registration
raw file metadata schema
old crate deletion sequencing
```

The implementation will keep changing as the first real feed is built.

The invariant to protect is simpler:

```text
feeds and projections write typed cells
changed cells schedule dependent projections
runtime stays generic
storage stays separate from active state
Lens/API do not force global market-domain types
```

## 14. Near-Term Plan

The current cleanup direction:

```text
1. Keep Data Center as a store object explorer.
2. Keep API and CLI thin over store.
3. Specify the generic feed trait and first concrete feed.
4. Connect the first feed to ledger-runtime through ExternalWriteBatch.
5. Add concrete market/feed/projection types outside ledger-runtime.
6. Add runtime read/stream delivery only after the feed boundary is validated.
```

This plan can be adjusted as implementation exposes better boundaries.

## 15. Success Criteria

The new architecture is working when:

```text
Data Center can show registered raw files without replay validation.
The old replay/book/validation path is not required for the app to compile.
Concrete feeds can use file/object metadata without making the runtime
  domain-specific.
Feeds submit typed external write batches.
Runtime schedules projections from changed keys.
Projections store all private cursor/state they need.
API/Lens can read or stream public runtime state without global market DTOs.
The same runtime can support replay, live, 0DTE, and future context feeds.
```

The goal is not to preserve the old replay abstraction. The goal is a small,
generic state and scheduling core that concrete Ledger features can build on.
