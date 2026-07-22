# Zero-Origin Replay Rebuild and Projection Reconfiguration — Implementation Spec

Status: implemented; native device feel-check and real-data profiling pending

Date: 2026-07-21

Baseline: `9ff865b` (`Implement projection graph and delivery recovery`)

> Follow-on behavior (2026-07-22): the mutable rebuild path now supports
> tick-count bars through `bars:<n>t` without a new runtime or delivery schema.
> The tick-bar exclusions below describe this phase's original boundary. See
> `ledger_tick_bars_implementation_spec.md`.

## 1. Purpose

Unify explicit seek and projection-set changes behind one deterministic Ledger
operation:

```text
rebuild(target session time, desired public projection set)
```

Every rebuild starts a fresh replay epoch at source position zero, installs the
desired projection graph before source data is released, and replays forward as
fast as the server can compute until it reaches the session clock.

Projection delivery remains independent. It does not wait for convergence and
does not receive one event per cache write. It observes dirty projection
sources, samples their latest committed state at projection-defined cadence,
and uses the existing epoch/base protocol to send an authoritative snapshot
whenever the prior receiver position is no longer valid.

The resulting product model is:

```text
explicit seek to T
  = rebuild(T, current projection set)

change bars:1m -> bars:15m at current time T
  = rebuild(T, [bars:15m])
```

There is no projection catch-up mode, seek loading mode, shadow runtime,
projection history artifact, or final-snapshot-only delivery path.

This pass establishes the server and Lens lifecycle needed to change supported
time-bar projections in a running session. Checkpointing is deliberately
deferred until zero-origin behavior and performance have been measured.

## 2. Target Pipeline

```text
Store ES event-store artifact (complete market day; trusted feed access only)
  -> ES replay feed starts at source position zero for each rebuild epoch
  -> feed cache contains only the prefix released through ClockState::now_ns()
  -> canonical trade prints consume released feed batches
  -> desired bars projection consumes released canonical prints
  -> projection cache commits exact intermediate state
  -> delivery marks the source dirty and samples at bounded FPS
  -> Remux sends only receiver-targeted frames/watermarks
  -> Lens atomically applies snapshot/append frames
```

The Store artifact contains the complete day. Projection code never receives
the Store, artifact path, decoded full-day event store, or any other
future-bearing capability. Only the trusted replay feed owns that input.

## 3. Relationship to Existing Specifications

The following specifications remain authoritative except where this document
explicitly supersedes them:

```text
docs/ledger_feed_system_implementation_spec.md
  clock-driven ES replay, cursor lineage and feed cells

docs/ledger_projection_system_implementation_spec.md
  canonical print and bar math, projection-owned cache cells

docs/ledger_projection_graph_implementation_spec.md
  compiled projection definitions, dependency resolution and typed outputs

docs/atomic_snapshot_projection_delivery_implementation_spec.md
  owner snapshots, bounded delivery cadence, base/head validation,
  acknowledgements, demand, resync, leases, backpressure and targeted streams

docs/ledger_session_transport_implementation_spec.md
  session identity and control transport

docs/lens_replay_chart_implementation_spec.md
docs/lens_frontend_cleanup_implementation_spec.md
  bars accumulator, chart layer and persisted viewport behavior
```

### 3.1 Decisions superseded by this specification

This document supersedes these current choices:

1. A running session's projection graph is no longer immutable.
2. Projection specs are no longer part of immutable attach identity.
3. A forward seek no longer reuses the active feed/projection prefix.
4. A backward seek no longer truncates to the target and rebuilds projections
   over the retained prefix.
5. Every explicit seek, including a same-position seek, starts a new epoch at
   source position zero.
6. Bars delivery no longer uses `FinalSnapshotOnly` during seek.
7. Delivery no longer owns `SeekBarrier`, `begin_seek`, `cancel_seek`, or
   `SeekFinal` frame behavior.
8. Lens no longer presents normal server rebuild or sampled viewer lag as
   `processing`, `loading chart`, or `catching up` action-bar states.
9. Lens no longer hard-codes `REPLAY_SPECS = ["bars:1m"]` as immutable for the
   lifetime of the hook.

### 3.2 Contracts preserved

This pass does not change:

- canonical trade-print inclusion policy;
- time-bar OHLCV/trade-count math;
- `bars:v1` payload fields;
- cache owner checks;
- atomic owner-executed projection snapshots;
- projection-specific delivery FPS;
- dirty notification coalescing;
- bounded delivery output and backpressure behavior;
- receiver base/head validation and authoritative snapshot fallback;
- acknowledgement, demand, resync and lease semantics;
- Remux origin targeting;
- clock mode/speed persistence across explicit seek;
- reload attachment to the same live server session; or
- persisted Lens chart viewport policy.

## 4. Fixed Decisions

1. **One rebuild primitive.** Seek and projection replacement call the same
   Ledger operation with different arguments.
2. **Zero-origin epochs.** Every accepted rebuild increments the replay epoch,
   clears released source/projection state and starts at feed batch zero.
3. **Desired graph before source release.** All nodes in the requested graph
   are registered and installed before the feed resumes the new epoch.
4. **No projection access to future data.** Projections consume only their
   typed cache dependencies. Only the feed can read the full Store artifact.
5. **Normal Runtime execution.** Feed and projection components run their usual
   bounded task steps. There is no special catch-up scheduler or projection
   execution mode.
6. **Normal delivery during rebuild.** Delivery samples intermediate committed
   states at normal cadence. It may emit zero, one, or several frames while a
   rebuild runs.
7. **No compute-driven UI state.** A live chart does not show a spinner because
   the server feed or a projection is rebuilding.
8. **Epochs invalidate stale delivery state.** The first admitted frame in a
   new epoch is an authoritative snapshot when the receiver's old base fails
   validation. Later frames append normally.
9. **Projection changes replace the exact public set.** The server resolves the
   requested set, including internal dependencies, and removes nodes not
   required by that set. Compatible nodes shared by the old and desired plans
   remain installed but reset to empty state through the new feed epoch.
10. **Same-set requests are idempotent.** Setting the already-active canonical
    public set does not rebuild. An explicit seek always rebuilds, even when
    its target equals the current clock position.
11. **Checkpointing is deferred.** No hourly snapshots, copy-on-write cache
    chunks, projection checkpoints or persisted projection history are added.
12. **No projection Store artifacts.** Store remains responsible for raw and
    prepared replay artifacts in this pass.
13. **Current time-bar wire schema remains version 1.** The implementation proof
    changes among supported time intervals without adding a new bars schema.
14. **Tick bars remain a projection-kind follow-on.** The rebuild mechanism
    must accept them later without redesign, but this pass does not invent a
    tick-bar chart coordinate or expose unavailable tick choices in Lens.

## 5. Scope

### 5.1 In scope

```text
one zero-origin Ledger rebuild operation
zero-origin forward, backward and same-position explicit seeks
mutable session public projection set
dependency-aware graph reconciliation before replay release
dynamic Runtime task/cell removal and installation
dynamic delivery-source replacement
normal cadence delivery throughout rebuild
epoch-driven authoritative snapshot reset
bounded normal backlog processing for canonical prints and bars
Remux projection-set RPC
read-only attach returning the current mutable projection set
Lens time-bar selection proof (1m, 5m, 15m, 1h)
Lens accumulator/subscription replacement without compute loading UI
reload preservation of the server-selected projection
headless parity, delivery, transport and device validation
```

### 5.2 Explicitly out of scope

```text
cache checkpoints of any cadence
Store-backed canonical-print or projection artifacts
projection-specific history loaders
temporary/shadow projection runtimes
parallel or multi-core projection compute
multiple simultaneous Ledger sessions
multiple Lens chart tabs or panels
tick-count bars and bars:<n>t grammar
volume, range or other bar completion policies
cross-day or long-term bar history
dynamic plugin loading or user-authored code
changing projection FPS based on rebuild throughput
server progress jobs for seek or projection replacement
compute-driven Lens loading overlays/spinners
```

## 6. Terminology

```text
rebuild
  One serialized session operation that chooses a target time and exact public
  projection set, starts a new epoch at source position zero, and releases the
  feed to replay forward.

rebuild epoch
  The feed cursor epoch shared transitively by canonical prints, public
  projections and delivery positions. A new value invalidates old lineage.

desired public set
  Canonical public ProjectionSpec values requested for the session after a
  rebuild. Internal dependencies are resolved by ProjectionPlan.

zero-origin
  Feed state begins with batch_idx = 0 and empty released arrays. Projection
  state begins empty and observes new-epoch dependency data in source order.

released prefix
  The source batches/prints already made visible in active cache cells through
  the current server clock. It contains no later source records.

graph reconciliation
  Retention of compatible desired nodes, removal of obsolete nodes and
  installation of missing nodes while the feed is held at zero. Retained
  reducers still clear through the new feed epoch; no projection history is
  retained across the rebuild.

presentation cadence
  Projection delivery's wall-clock sampling rate. It is independent of feed
  and projection computation throughput.
```

## 7. Load-Bearing Invariants

### 7.1 Causality and source visibility

1. Within an active session, only the trusted `EsReplayFeed` process may own
   the complete decoded ES event-store artifact. Preparation/maintenance code
   may create or validate that artifact, but no projection receives it.
2. A Runtime projection receives no Store handle or event-store path.
3. In rebuild epoch `E`, `feed.batches` is exactly a prefix of the prepared
   event store beginning at batch zero.
4. Every batch in that prefix is due at the `ClockState::now_ns()` observed by
   the trusted feed when it was released.
5. Before the feed releases any batch for `E`, every desired projection node is
   installed with empty `E` state.
6. No task or feed write from an older epoch may mutate new-epoch state after
   the zero reset commits.
7. Projection dependencies are processed only through typed cache keys and
   their committed status lineage.

### 7.2 Graph and projection correctness

8. The active public set is unique by canonical projection spec.
9. `ProjectionPlan::resolve` remains the sole dependency expansion and
   deterministic ordering authority.
10. A graph node is installed only after all declared dependencies exist.
11. Graph reconciliation installs at most one node per canonical node key.
12. A node removed from the desired graph has no Runtime task, registered
    owned cells, delivery source or public Remux entry after reconciliation.
13. A compatible node retained across graph reconciliation reaches empty epoch
    `E` state before the feed releases source batch zero.
14. Canonical print status can claim batch `N` only after every batch in
    `feed.batches[0..N]` has been inspected.
15. Bars status can claim a canonical source extent only after every print in
    that extent has been folded.
16. Every normal task step processes a bounded dependency suffix and yields
    with `WakeAgain` when more work remains.
17. Time bars produced by rebuild are byte-for-byte/field-for-field equivalent
    to opening a fresh session with the same raw, target and projection spec.

### 7.3 Computation/delivery separation

18. Cache mutation rate never directly determines frame rate.
19. One or many dirty notifications before a delivery deadline may produce one
    atomic collect of the latest state.
20. Delivery never waits for feed/projection convergence before it may collect.
21. Delivery never serializes inside Runtime task execution or cache mutation.
22. An old receiver base from epoch `E-1` is invalid against an `E` status and
    selects an authoritative snapshot.
23. After an `E` snapshot is accepted, monotonic `E` states may use append
    delivery exactly as during ordinary playback.
24. Output backpressure may delay/coalesce frames but cannot pace Runtime
    computation or mutate projection truth.
25. The viewer may observe rapidly growing intermediate bar prefixes or only a
    final prefix; both must converge to the same result.
26. When graph topology changes, delivery source observation is suspended only
    across the short cache-schema reconciliation boundary. An unchanged-graph
    seek does not suspend delivery. Suspension has no target, progress or
    convergence condition and ends before the feed releases batch zero.

### 7.4 Session and UI behavior

27. Rebuild preserves the pre-operation clock mode and speed.
28. A running clock may continue advancing while the zero-origin replay chases
    it; no implicit user-visible pause is introduced.
29. A paused clock remains a fixed rebuild target.
30. A newer serialized rebuild supersedes all older epoch work.
31. Lens renders only the currently selected projection spec.
32. Selecting a different interval discards the old accumulator and subscribes
    to the new spec with `have: null`.
33. Normal rebuild progress does not disable playback controls and does not
    create a `processing`, `loading chart`, or `catching up` caption.
34. Transport disconnect and explicit delivery resync remain distinguishable
    from normal server computation.

## 8. Current Behavior and Required Change

| Area | Baseline behavior | Required behavior |
| --- | --- | --- |
| Forward seek | Feed retains current prefix and emits only the suffix | New epoch clears to zero and replays the whole due prefix |
| Backward seek | Feed truncates to target; projections rebuild over retained prefix | New epoch clears to zero and all desired nodes observe replay from batch zero |
| Same-position seek | Clock revision changes but source prefix remains | Explicit seek creates a new zero-origin epoch |
| Projection graph | Built once before Runtime ownership | Reconciled inside the active session under serialized control; shared compatible nodes remain installed but reset to the new epoch |
| Bars new backlog | Incremental path may fold the entire unseen suffix in one step | All unseen suffix work is bounded per Runtime step |
| Delivery seek policy | Bars are `FinalSnapshotOnly`; executor suppresses until a barrier converges | Dirty/cadence collection continues normally |
| Frame reason | Explicit `seekFinal` frame | Snapshot/append arrives as `initial` or `cadence`; epoch determines reset |
| Attach identity | Session id/raw id/exact projection set must all match | Session id/raw id identify the session; response reports its current graph |
| Lens projection set | Immutable module constant | Server-backed selected time-bar spec can change |
| Lens rebuild status | Watermarks surface processing/loading/catching-up labels | Normal compute progress is not a UI health state |

## 9. Unified Ledger Session Operation

The implementation should expose one internal method, with public controls as
thin adapters:

```rust
pub(crate) enum RebuildReason {
    Seek,
    ProjectionSetChanged,
}

pub(crate) struct RebuildRequest {
    pub target_session_ns: u64,
    pub projections: Vec<ProjectionSpec>,
    pub reason: RebuildReason,
}

impl LedgerSessionHandle {
    async fn rebuild(&self, request: RebuildRequest) -> Result<RebuildResult, LedgerError>;

    pub async fn seek_to(&self, session_ns: u64) -> Result<(), LedgerError>;

    pub async fn set_projections(
        &self,
        projections: Vec<ProjectionSpec>,
    ) -> Result<SetProjectionsResult, LedgerError>;
}
```

Adapter behavior:

```text
seek_to(T)
  current public set = G
  rebuild({ target: T, projections: G, reason: Seek })

set_projections(G2)
  if canonical(G2) == current public set: return unchanged
  T = current ClockState::now_ns()
  rebuild({ target: T, projections: G2, reason: ProjectionSetChanged })
```

`rebuild` is serialized by the existing session control mutex with play,
pause, speed, seek, projection changes and shutdown entry. Projection parsing
and `ProjectionPlan::resolve` occur before active state is mutated.

The operation returns after:

1. the new clock/rebuild epoch is committed;
2. the feed is held and reset to zero for that epoch;
3. the desired graph and delivery sources are installed;
4. the feed is released to compute normally.

It does **not** wait for the feed or projections to reach the target. Delivery
and status expose committed progress without an operation-level loading job.

## 10. Rebuild Sequence

### 10.1 Phase A — prepare without mutation

Under session control:

1. Parse and canonicalize every requested public spec.
2. Reject duplicates and resolve the complete `ProjectionPlan`.
3. Capture the current `ClockState`.
4. Construct the next clock with the requested target while preserving mode
   and speed.
5. Allocate `next_epoch = current_feed_epoch + 1` using saturating/checked
   behavior consistent with existing counters.
6. Pre-validate component ids, cache keys and delivery descriptors so expected
   graph errors occur before the active graph is touched.

For an idempotent `set_projections`, return before allocating an epoch.

### 10.2 Phase B — hold the trusted feed

The feed requires an internal control handle separate from its cache output:

```rust
enum EsReplayFeedCommand {
    Hold {
        reply: oneshot::Sender<Result<(), ComponentError>>,
    },
    Reset {
        epoch: u64,
        reply: oneshot::Sender<Result<(), ComponentError>>,
    },
    Release {
        epoch: u64,
        reply: oneshot::Sender<Result<(), ComponentError>>,
    },
}
```

`Hold` establishes a source-write boundary:

- the feed stops producing new batches;
- all already-submitted writes from that single feed producer remain ordered
  before the subsequent reset write;
- the session waits for the hold acknowledgement before replacing graph state.

Feed command polling must occur between every `CATCHUP_CHUNK_BATCHES` emission,
not only while the feed is idle, so a long replay cannot starve a new seek.

An unchanged-graph seek proceeds directly to Phase C. Delivery remains active
and observes the epoch reset through its existing source watches.

### 10.3 Phase C — commit clock and zero feed state

While the feed is held:

1. Commit the next `ClockState`.
2. Reset `FeedState` to:

   ```text
   epoch = next_epoch
   next_idx = 0
   feed_seq = 0
   last_emitted_ts = None
   catching_up = whether at least one source batch is already due
   ```

3. Atomically commit the feed-owned cache reset:

   ```text
   feed.batches = []
   feed.cursor = epoch E, batch_idx 0, feed_seq 0
   feed.status = same cursor + new clock snapshot
   ```

4. The reset acknowledgement completes only after the new-epoch cursor is
   observable in Runtime-owned cache state and tasks woken by that reset have
   reached the Runtime's next idle/task boundary. Any node that will be retained
   by the desired plan must then expose empty epoch `E` status.

The explicit feed command replaces inference through
`FeedState::regressed(now)`. Rebuild happens because the session requested a
new epoch, not because the feed noticed that wall/session time moved backward.

### 10.4 Phase D — reconcile the projection graph

Phase A computes deterministic `retained`, `obsolete` and `missing` node sets
by canonical node key plus exact definition/dependency compatibility.

If topology and public outputs are unchanged, skip this phase entirely. The
existing tasks/cells/sources already hold empty epoch `E` state from Phase C.

If the graph changes, with the feed still held at empty epoch `E`:

1. Suspend delivery source observation and wait for any already-started collect
   to finish. This prevents a snapshot from capturing keys while those keys are
   unregistered or re-created.
2. In one owner-executed Runtime schema transaction:
   - remove obsolete tasks in reverse dependency order;
   - unregister obsolete owned cache cells;
   - register cells for missing nodes;
   - install missing tasks in deterministic dependency-first order; and
   - initialize every missing status cell as empty epoch `E` state.
3. Atomically publish the reconciled `InstalledProjectionGraph` record.
4. Replace the delivery source map, create fresh watches and resume observation.

The source suspension is a cache-schema safety boundary only. It does not know
the target time, clock revision or projection progress, and it has no
convergence condition. It ends before source batch zero is released, so no
cache mutation belonging to the actual new replay is suppressed.

Compatible shared nodes, such as canonical trade prints, remain installed.
They retain their task/cell identity, not their prior data: Phase C has already
cleared their state through epoch `E`. No projection task is manually queued
against old source state. Once the feed is released, new-epoch dependency
changes schedule the graph normally.

### 10.5 Phase E — release computation

After graph and delivery sources exist:

1. Release the feed for epoch `E`.
2. The feed emits due batches from index zero in bounded chunks.
3. Canonical prints and bars wake through normal dependency changes.
4. Every component yields between bounded chunks.
5. The session returns success to the caller after release is accepted.

If the clock is paused, the due prefix has a fixed end. If it is running, the
feed recalculates `ClockState::now_ns()` as it works and may continue into newly
due source batches. This preserves the existing rule that seek does not change
play/pause mode or speed.

## 11. Runtime and Cache Mutation Primitives

The active `RuntimeWorker` remains the sole mutation/scheduling owner. Do not
expose `Cache` through `LedgerSessionHandle` or permit direct post-start schema
mutation from Remux/Lens code.

### 11.1 Runtime task removal

Add an owner-executed Runtime command that removes a task only between task
steps:

```rust
pub async fn remove_task(&self, id: &ComponentId) -> Result<bool, RuntimeError>;
```

Removal must:

- reject a process id with `WrongComponentKind`;
- remove queued/rerun state from `TaskQueue`;
- remove every key edge from `DependencyIndex`;
- remove the task from `TaskRegistry`;
- make a later component installation with the same id legal; and
- never interrupt a task halfway through `run_once`.

The dependency index needs a reverse task-to-keys record or equivalent so
removal cannot leave stale scheduler edges.

### 11.2 Runtime-owned cache schema reconciliation

Provide a narrow owner-executed registration/unregistration capability. The
API may be expressed as a registrar transaction rather than generic mutation,
but it must support typed `Value<T>`/`Array<T>` registration and owner-scoped
cell removal.

Required guarantees:

- cell registration/unregistration runs between task steps and snapshots;
- all keys are prevalidated before an installation becomes public;
- partial registration rolls back on error;
- unregister verifies the expected owner;
- existing watches close when their old cell is removed;
- the same key may be registered again after old watches are detached; and
- snapshots never run halfway through a graph schema transaction.

Feed cells and `session.clock` remain registered for the session lifetime.
Only obsolete/missing projection-owned cells change during graph
reconciliation. Compatible retained cells remain registered and are cleared by
the rebuild epoch.

### 11.3 Initial projection state

Task `prepare` currently submits status writes through the external write
channel. Rebuild installation should instead make fresh empty status state part
of the owner schema/installation transaction, or explicitly wait until all
prepare writes commit before sources become visible.

Delivery must never collect a registered source whose status cell is absent.

## 12. Projection Graph Ownership

Replace fixed `Vec` fields on `LedgerSessionHandle` with a session-owned graph
record protected by the existing serialized control path:

```rust
struct InstalledProjectionGraph {
    public_specs: Vec<String>,
    nodes: Vec<InstalledGraphNode>,
    public_outputs: Vec<SessionProjectionOutput>,
}

struct InstalledGraphNode {
    node: ProjectionNodeSpec,
    component_id: ComponentId,
    owned_keys: Vec<Key>,
    output: ProjectionOutput,
}
```

Delivery sources move into the delivery executor and are replaced through its
command channel. The graph record retains enough identity for task/cell
removal and Remux status/pull lookup.

Reconciliation compares the installed record with the resolved logical plan.
A node is retainable only when its canonical key, definition version,
parameters, dependency keys and output descriptors are identical. Otherwise it
is removed and reinstalled; interval equality alone is not sufficient.

`ProjectionPlan` remains a logical plan. Installation must be usable both
before Runtime start and through the active Runtime registrar without creating
two projection-definition implementations.

## 13. Bounded Normal Projection Work

There is no catch-up mode, but normal task execution must tolerate an
arbitrarily large dependency backlog without monopolizing the Runtime owner.

### 13.1 Canonical prints

The existing 4,096-feed-batch bound remains. Every run:

```text
chunk_end = min(source batch count, processed batches + 4096)
inspect that range
commit print suffix/status
WakeAgain when chunk_end < source batch count
```

### 13.2 Bars

Apply the existing 4,096-print bound to the ordinary incremental path, not
only epoch regression rebuilds. A fresh bars task in epoch `E` starts at print
zero and follows the same loop as any other backlog.

While processing a partial source extent, `BarsStatus.processed_batches` must
not claim the dependency's final batch lineage. It advances to the canonical
source lineage only after all prints represented by that status are folded.

The exact internal structure (`Fold::Rebuilding` or one unified cursor loop) is
an implementation choice. Externally there is one task behavior, not a
separate catch-up state.

### 13.3 Scheduler fairness

Between chunks, Runtime must continue to service:

- feed hold/reset commands and external writes;
- newer session controls;
- atomic snapshot requests;
- other queued projection tasks; and
- Tokio task scheduling needed by delivery/watchers.

## 14. Projection Delivery Cleanup

### 14.1 Remove seek awareness

Delete or retire:

```text
SeekDeliveryPolicy
ProjectionDeliveryDescriptor.seek_policy
ProjectionFrameReason::SeekFinal
ProjectionDeliveryHandle::begin_seek
ProjectionDeliveryHandle::cancel_seek
DeliveryCommand::{BeginSeek, CancelSeek}
SeekBarrier
complete_seek_if_ready
frames_suppressed_during_seek metrics
seek_barriers_completed metrics
total_seek_barrier_ns metrics
```

Seek/rebuild correctness no longer depends on a delivery convergence barrier.

### 14.2 Preserve cadence and reset behavior

Keep existing source dirty watches, cadence deadlines and `collect` logic.

For bars, an old receiver position validates only when:

```text
session generation matches
epoch matches
projection revision/processed batches/completed bars are not ahead
```

After rebuild increments the epoch, validation fails naturally and collection
returns:

```text
base = None
operation = Snapshot
payload = latest committed new-epoch bars/live/status
```

The snapshot may be empty, partial or already converged. Subsequent new-epoch
collects use append delivery when their base validates.

### 14.3 Dynamic source replacement

A graph-topology change needs a two-command source-schema handoff:

```rust
pub async fn suspend_sources(&self) -> Result<(), ProjectionDeliveryError>;

pub async fn replace_sources(
    &self,
    sources: Vec<Box<dyn ProjectionDeliverySource>>,
) -> Result<(), ProjectionDeliveryError>;
```

Requirements:

- `suspend_sources` aborts source watches and acknowledges only after any
  in-flight collect has finished;
- suspension retains receiver/origin positions until the replacement set is
  known;
- suspension does not inspect epoch, target time or projection status and has
  no convergence condition;
- reject duplicate specs before mutation;
- `replace_sources` is valid only from the suspended state, installs fresh
  watches and resumes cadence before reporting success;
- retain consumer projection state for specs still present, marking it dirty;
- remove consumer projection state for removed specs;
- refuse future ack/resync entries for removed specs;
- include only current sources in watermarks; and
- permit a source key/spec to be reused after graph reconciliation.

If graph reconciliation fails after suspension, the session enters its terminal
failure path; delivery is not resumed against a partial schema. This handoff
replaces cache source handles only. It does not suppress or collapse the actual
new-epoch projection computation described in Section 10.5.

Add explicit unsubscribe so Lens does not wait for a lease when replacing its
single selected spec:

```text
remux/ledger/session/projections/unsubscribe
```

Unsubscribe is idempotent for a valid session/origin/spec tuple so a client may
clean up its old subscription after the server has already removed that spec.
Lease expiry remains crash/disconnect cleanup.

### 14.4 Frame reasons

Keep:

```text
Initial
Cadence
Resync
```

An epoch-reset snapshot generated on a normal cadence remains `Cadence`; reset
semantics come from `base = None` and the new head epoch, not a seek-specific
reason string.

## 15. Remux Session Protocol

### 15.1 Set projection set

Add:

```text
remux/ledger/session/projections/set
```

Request:

```json
{
  "sessionId": "session-7",
  "projections": ["bars:15m"]
}
```

Response after graph installation/feed release:

```json
{
  "sessionId": "session-7",
  "epoch": 4,
  "projections": [{ "spec": "bars:15m" }],
  "changed": true
}
```

An equal canonical set returns `changed: false` and does not bump epoch.
Invalid specs, duplicates or dependency-plan failures are rejected before the
active graph changes.

The RPC replaces the exact session public set. It is not a per-receiver demand
or delivery subscription operation.

### 15.2 Seek

The existing request remains:

```text
remux/ledger/session/seek { sessionId, sessionNs }
```

It calls zero-origin rebuild with the current public set. The success response
means the new epoch was reset and released, not that projection computation is
finished.

### 15.3 Attach

Change attach identity from:

```text
session id + raw id + exact requested projection set
```

to:

```text
session id + raw id
```

The request becomes:

```json
{
  "sessionId": "session-7",
  "rawId": "sha256-..."
}
```

Remove `projections` from the Lens attach request and from server-side identity
matching. If an older caller still sends that field, it must not influence the
attach decision.

Attach remains strictly read-only and returns the current server projection
set, clock snapshot and feed cursor. A reloaded Lens adopts that projection
set; it does not reset a correctly attached server session merely because a
stale client constant differs.

Malformed input remains an RPC error. Missing/stale session identity remains a
typed attach miss.

### 15.4 Status and legacy bars pull

Attach, subscribe, `session/status` and diagnostic `session/bars` resolve
against the current mutable graph record rather than the open-time
`Vec<ActiveProjection>`.

`session/bars` remains a diagnostic/compatibility pull; it is not part of Lens
live-session open, attach, seek or interval-change hydration. A projection
subscription with `have: null` is the single authoritative full-snapshot path.

During the short schema reconciliation boundary these reads, along with
projection ack/demand/resync routing, must serialize behind the session
lifecycle/control lock or use one atomically published graph record. They must
observe one coherent pre/post graph view, never half of a graph reconciliation.

## 16. Lens Behavior

### 16.1 Selected projection

Replace module constants `ACTIVE_BAR_SPEC` and `REPLAY_SPECS` with one selected
server-backed time-bar spec.

Initial fresh open defaults to:

```text
bars:1m
```

Attach adopts the returned current projection set. For this single-chart pass,
Lens expects exactly one public bars projection.

Supported selection proof:

```text
bars:1m
bars:5m
bars:15m
bars:1h
```

The action-bar control belongs immediately left of playback speed, matching the
planned interval menu location. Do not expose the tick column until tick-bar
projection and chart-coordinate semantics exist.

Keep the top-level replay-session lifecycle keyed only by raw/route session
identity. The selected projection must not become a dependency that tears down
that lifecycle effect: changing an interval must never run `closeSession`, open
a replacement session or return Lens to the date selector.

### 16.2 Selection transaction

On selection of a different spec:

1. Call `session/projections/set` with the new single-spec set.
2. Create a new `BarsAccumulator` for that spec.
3. Explicitly unsubscribe the old projection subscription.
4. Subscribe to the new spec with `have: null`.
5. Replace the chart layer/title with the selected spec.
6. Apply normal snapshot/append frames as they arrive.

The chart must never label old one-minute data as fifteen-minute data. It may
be briefly empty or show rapidly growing new-epoch fifteen-minute bars.

Do not wait for server convergence before swapping. Do not display the old
interval as a fallback after the user has selected the new interval.

Controls remain enabled. A second selection is serialized client-side until
the preceding set RPC establishes its graph; the latest user choice is then
applied.

### 16.3 Delivery state cleanup

Normal watermark differences are diagnostics, not action-bar activity.

Remove compute-driven captions:

```text
projection_catching_up -> processing
delivery_pending       -> loading chart (during live rebuild)
viewer_lagging         -> catching up
```

Initial application establishment may retain its existing page-level phase
until the first usable projection frame. Once live, only actual transport or
delivery recovery may surface activity:

```text
resyncing
disconnected_unknown / reconnecting
```

Do not infer a server problem from `cursor.catchingUp` or a projection head
temporarily trailing the feed during zero-origin replay.

### 16.4 Accumulator changes

- Remove `seekFinal` parsing and immediate-ack behavior.
- Continue immediate acknowledgement for authoritative snapshots and resync.
- Continue rejecting a delta whose base does not equal the applied position.
- Accept new-epoch authoritative snapshots and replace all prior bars/live
  state atomically.
- Keep requestAnimationFrame last-mile chart coalescing.

### 16.5 Reload

The exact native route/session id remains the reload capability. On attach:

- seed clock and cursor from the returned snapshot;
- create accumulators from the server-returned projection set;
- subscribe with `have: null` after a native reload, because in-memory applied
  positions do not survive the renderer process;
- rely on that subscription to collect one authoritative snapshot from the
  current server cache, including all completed bars and live state;
- do not call `fetchSessionBars(spec, 0)` in parallel with delivery hydration;
- never issue `projections/set` merely to satisfy an old hard-coded constant.

If reload occurs during rebuild, Lens receives whichever committed new-epoch
state exists and continues through normal cadence frames. Subscribing marks the
source dirty, so a paused session still produces the requested initial snapshot
without waiting for another clock or projection revision.

## 17. Failure and Concurrency Semantics

### 17.1 Failures before mutation

These leave the current graph/epoch untouched:

- invalid projection syntax;
- duplicate canonical specs;
- dependency-cycle/plan error;
- invalid component/cache descriptors; and
- unsupported public projection kind.

### 17.2 Failures after reset begins

Graph installation should prevalidate expected errors. An unexpected Runtime,
cache or feed failure after zero reset or schema reconciliation begins is a
session failure, not a silent rollback to state from the prior epoch. Remux
reports the domain error; the session status identifies the failed component;
the viewer follows its existing terminal/error path.

Do not replay old cache data as a rollback because it belongs to a superseded
epoch and may be after the new target.

### 17.3 Newer controls

Session controls are serialized. Once a rebuild RPC returns after release, a
new seek/projection change may begin before the prior epoch reaches the clock.
The newer operation:

- holds the feed at the next bounded command point;
- allocates a newer epoch;
- resets source/graph state again; and
- makes all older positions invalid.

No obsolete task survives reconciliation, and every compatible retained task
is already in the newer empty epoch before release. No older feed producer
exists; the single feed process handles commands in order.

### 17.4 Delivery and disconnection

A disconnected viewer does not pause computation. Projection delivery demand,
lease expiry and targeted Remux routing remain presentation concerns. Reload
attaches to the active server epoch and subscribes from the state it actually
has.

## 18. Observability

Keep metrics that validate the compute/delivery boundary:

```text
rebuilds by reason (seek / projection_set_changed)
zero-origin feed batches emitted per epoch
rebuild epoch duration to first batch and to feed-current (diagnostic only)
projection task steps and maximum step duration
canonical batches/prints processed per step
bars prints processed per step
dirty and coalesced dirty notifications
atomic projection collects
snapshot vs suffix frames
frames admitted/received
outbound backpressure
delivery queue depth
```

Remove seek-suppression/barrier metrics listed in Section 14.1.

Metrics do not create a new public progress protocol or Lens loading state.

## 19. Implementation Phases

### Phase 1 — Runtime graph mutation primitives

Primary files:

```text
crates/cache/src/{cache,error}.rs
crates/runtime/src/{handle,worker,error}.rs
crates/runtime/src/runtime/{mod,task_registry,task_queue,dependency}.rs
crates/runtime/tests/{runtime_scheduler,worker}.rs
crates/cache/tests/cache.rs
```

Work:

1. Add owner-executed task removal.
2. Remove task queue/dependency edges completely.
3. Add owner-scoped cache schema registration/unregistration transaction.
4. Prove same component/key can be reinstalled after removal.
5. Prove snapshots cannot observe half a schema transaction.

Deliverable: active Runtime can replace a projection component safely while
feed/session lifetime cells remain intact.

### Phase 2 — Explicit zero-origin feed reset

Primary files:

```text
crates/ledger/src/feed/es_replay/feed.rs
crates/ledger/src/feed/es_replay/cells.rs
crates/ledger/src/session.rs
crates/ledger/tests/{es_replay_feed,session}.rs
```

Work:

1. Add feed hold/reset/release control handle.
2. Service feed commands between every emission chunk.
3. Replace inferred `regress` seek handling with explicit epoch reset.
4. Clear feed arrays/cursor/status to batch zero on all explicit seeks.
5. Preserve clock mode/speed and allow running-clock pursuit.
6. Test forward, backward and same-position zero-origin behavior.

Deliverable: `seek_to` always releases a fresh feed prefix from source zero.

### Phase 3 — Delivery source handoff and seek ignorance

Primary files:

```text
crates/ledger/src/projection/delivery.rs
crates/ledger/tests/projection_delivery.rs
```

Work:

1. Remove seek policy/barrier/final-frame machinery.
2. Add source suspension/replacement and watcher lifecycle.
3. Add explicit unsubscribe.
4. Preserve consumers for unchanged specs across ordinary seek.
5. Prove epoch reset selects snapshot, then normal append.
6. Prove many zero-origin writes remain bounded by delivery cadence/coalescing.

Deliverable: delivery can safely hand off cache source handles, is unaware of
seek/rebuild cause and remains independent of Runtime throughput.

### Phase 4 — Mutable projection graph and unified rebuild

Primary files:

```text
crates/ledger/src/projection/{mod,trade_prints,bars}.rs
crates/ledger/src/session.rs
crates/ledger/src/error.rs
crates/ledger/tests/{projection_bars,session,support/mod}.rs
```

Work:

1. Refactor installers to work through setup and active Runtime registrars.
2. Add `InstalledProjectionGraph` ownership record.
3. Reconcile retained/obsolete/missing nodes at held feed epoch zero using the
   Phase 3 source handoff only when topology changes.
4. Initialize new graph status cells with the rebuild epoch.
5. Bound normal bars incremental work to 4,096 prints.
6. Implement `rebuild`, `seek_to` adapter and idempotent
   `set_projections` adapter.
7. Prove 1m -> 15m -> 1m rebuild parity.

Deliverable: projection replacement is the same zero-origin replay operation
as seek.

### Phase 5 — Remux mutable session transport

Primary files:

```text
crates/remux/src/{methods,session}.rs
```

Work:

1. Add `session/projections/set` and unsubscribe methods.
2. Make ActiveSession projection lookup mutable/coherent.
3. Change attach matching to session id + raw id and remove requested
   projections from the attach contract.
4. Return current projection graph/epoch from set, attach and status.
5. Serialize graph-sensitive reads/routing across source-schema
   reconciliation.
6. Route dynamic delivery sources to existing targeted origins.
7. Update all DTO/frame reason conversions.

Deliverable: one active Remux session can seek or replace its time-bar spec
without closing/reopening.

### Phase 6 — Lens time-bar selection and UI cleanup

Primary files:

```text
lens/src/features/replay/{api,types,accumulator,projection-client}.ts
lens/src/features/replay/use-replay-session.ts
lens/src/features/replay/replay.tsx
lens/src/features/replay/replay-action-bar.tsx
lens/src/features/replay/chart/{layers,bars-layer,time}.ts
```

Work:

1. Replace immutable active spec constants with current server-backed spec.
2. Add the time-bar preset control left of playback speed.
3. Reconcile graph, unsubscribe old and subscribe new without waiting for
   convergence.
4. Reset chart accumulator/layer/title to the new spec.
5. Split session lifetime from mutable projection selection so an interval
   change cannot close/reopen or navigate.
6. Remove attach-time `session/bars` hydration; use one `have: null` delivery
   snapshot.
7. Remove seek-final protocol handling.
8. Remove compute/rebuild activity captions while retaining real
   reconnect/resync feedback.
9. Make reload adopt the server projection set.

Deliverable: selecting 1m/5m/15m/1h visibly rebuilds the chosen bars through
normal delivery with no compute loading UI.

### Phase 7 — Validation and documentation reconciliation

Primary files:

```text
README.md
docs/ledger_feed_system_implementation_spec.md
docs/atomic_snapshot_projection_delivery_implementation_spec.md
docs/ledger_projection_graph_implementation_spec.md
docs/lens_replay_chart_implementation_spec.md
```

Work:

1. Record superseded behavior without rewriting historical implementation
   records.
2. Run complete Rust/Lens validation.
3. Profile full-day zero-origin rebuild with and without delivery subscriber.
4. Complete native device feel-check.
5. Append implementation record and measured results to this document.

## 20. Test Matrix

### 20.1 Cache and Runtime

- remove a ready task and reinstall the same component id;
- remove a queued task and prove it never runs;
- dependency writes after removal do not enqueue the old task;
- task removal never occurs halfway through a task step;
- owner mismatch rejects cell unregistration;
- removed cell watches close;
- same typed key can be registered after removal;
- failed multi-cell registration leaves no partial keys;
- atomic snapshot sees complete old or complete new schema, never a partial
  graph transaction.

### 20.2 Feed and clock

- forward seek clears batches to zero before new-epoch growth;
- backward seek clears batches to zero rather than retaining a target prefix;
- same-position explicit seek increments epoch and clears to zero;
- paused seek stops exactly at target;
- running seek preserves running mode/speed and continues pursuing clock time;
- hold interrupts a multi-chunk replay at its next chunk boundary;
- no old-epoch feed write lands after reset acknowledgement;
- repeated seek makes the newest epoch authoritative.

### 20.3 Projection graph

- fresh 1m open equals zero-origin seek to the same target;
- fresh 15m open equals 1m -> 15m projection replacement at the same target;
- an unchanged-graph seek retains canonical/bars task and cell identities while
  clearing their data to the new epoch;
- 1m -> 15m removes the 1m task/cells/source;
- 1m -> 15m retains canonical task/cell identity but clears and recomputes its
  print data from source zero;
- 15m -> 1m reuses component/key names only after old removal;
- canonical prints install before the chosen bars node;
- all desired statuses share feed epoch;
- no desired task runs before its dependency publishes the new epoch;
- canonical work never exceeds 4,096 batches per step;
- bars work never exceeds 4,096 prints per step in both rebuild and ordinary
  backlog paths;
- invalid/duplicate set request leaves the existing graph unchanged;
- equal canonical set is a no-op.

### 20.4 Delivery

- an old-epoch base produces a new-epoch snapshot;
- snapshot may contain zero, partial or final bars;
- next valid head produces append;
- multiple dirty writes before a deadline coalesce;
- delivery frame count remains presentation-bounded during a full rebuild;
- delivery does not contain seek barrier/suppression behavior;
- an unchanged-graph seek does not suspend or replace delivery sources;
- source suspension waits for an in-flight collect before cache keys are
  removed;
- no collect is attempted while projection source keys are absent;
- source replacement resumes cadence and marks retained specs dirty before
  feed release;
- removing a source removes it from watermarks and future subscriptions;
- replacing a source with the same spec attaches a new watch safely;
- explicit unsubscribe removes consumer/origin state;
- ack/resync for removed specs are rejected;
- bounded output backpressure never stalls Runtime computation.

### 20.5 Remux

- set 1m -> 15m returns changed/new epoch/current graph;
- equal set returns unchanged/same epoch;
- seek retains current mutable public set;
- attach without a requested projection list succeeds by session id/raw id
  after the graph changed;
- a legacy attach `projections` field does not influence identity;
- attach returns current spec, clock and cursor during rebuild;
- stale session id/raw mismatch remains an attach miss;
- status and legacy bars pull resolve only current graph specs;
- targeted frames remain isolated by Remux origin after source replacement;
- unsubscribe origin validation matches ack/demand/resync validation.

### 20.6 Lens

- fresh entry defaults to `bars:1m`;
- selecting 15m calls set and subscribes with `have: null`;
- selecting an interval does not run session cleanup, close the session, open a
  new id or navigate to days;
- old 1m accumulator/layer/title are removed immediately;
- empty/partial/final 15m snapshots render without a loading caption;
- new-epoch snapshot atomically replaces old bars/live state;
- normal `cursor.catchingUp` does not show processing activity;
- normal watermark lag does not show viewer catching-up activity;
- base mismatch still requests resync;
- transport loss still shows reconnecting;
- reload adopts server-selected 15m and does not reset it to 1m;
- reload issues no full `session/bars` pull and applies one authoritative
  delivery snapshot before append frames;
- a paused reload still receives that snapshot after subscribing;
- exit to days still closes the session and fresh re-entry defaults to 1m;
- viewport persistence remains independent of interval rebuild churn.

### 20.7 Real-data parity and performance

Using the prepared ES day already used for the static graph gate:

- full-day 1m fresh-open digest equals full-day explicit seek digest;
- full-day 15m fresh-open digest equals 1m -> 15m replacement digest;
- total volume and trade count match canonical prints for every interval;
- no frame contains a batch/print/bar beyond its feed cursor epoch/extent;
- compare delivery subscriber vs no-subscriber compute time;
- record dirty notifications, coalescing, collects, frames and backpressure;
- verify frame count remains small relative to Runtime/cache commits;
- record maximum Runtime task-step duration after normal bars bounding.

No fixed wall-time gate is set before measurement. Correctness and cadence
independence are mandatory; checkpointing is considered only after the
zero-origin cost is observed on this machine.

## 21. Device Feel-Check

1. Start `bars:1m`, play at 5x and seek backward several hours.
2. Verify the chart resets and rapidly rebuilds through normal frames without
   a processing/loading/catching-up spinner.
3. Seek forward and verify the same zero-origin behavior.
4. While paused mid-session, switch 1m -> 15m and verify the chart becomes
   fifteen-minute data only, then rapidly fills to the paused target.
5. While playing at 5x, switch 15m -> 5m and verify playback mode/speed remain
   running at 5x while bars rebuild.
6. Trigger another seek during a long rebuild and verify the newer epoch wins.
7. Reload during rebuild and verify attach returns the same session, selected
   interval, clock mode/speed/time and current partial projection state.
8. Close the screen while playing, reload later and verify the server replay
   progressed independently of viewer delivery.
9. Exit to days and re-enter, verifying the deliberate fresh 1m start.

## 22. Acceptance Criteria

This phase is complete only when all of the following are true:

1. Every explicit seek resets feed/projection lineage to a new epoch at source
   position zero.
2. Forward, backward and same-position seek use the same implementation path.
3. Replacing the public projection set invokes that same rebuild path at the
   current server time.
4. The desired graph is installed before the new epoch releases source batch
   zero.
5. Projection code has no Store/full-day artifact capability.
6. Obsolete graph tasks/cells/sources are actually removed, retained nodes are
   empty in the new epoch, and removed ids/keys can be reused safely.
7. Canonical/bar computation is bounded per Runtime step.
8. Delivery contains no seek-specific suppression or convergence barrier.
9. Delivery stays bounded by presentation cadence while Runtime runs at full
   speed.
10. Epoch invalidation produces authoritative snapshot reset followed by valid
    append frames.
11. Lens changes among supported time-bar specs without showing old-spec data
    under a new label.
12. Lens exposes no normal rebuild/catch-up loading state once the replay is
    live.
13. Reload preserves the active server projection set and replay state using a
    single authoritative delivery snapshot, without a parallel full bars pull.
14. Projection selection does not close/reopen the session or navigate away.
15. Complete Rust and Lens checks pass.
16. Real-data fresh-open/seek/projection-replacement parity passes.
17. Checkpointing and projection Store artifacts remain absent.

## 23. Deferred Checkpoint Note

If measured zero-origin replay later requires acceleration, the next design
may introduce controller-owned atomic cache checkpoints. That work must retain
these invariants:

- a checkpoint is usable only at or before the requested target;
- projections cannot enumerate/access checkpoints directly;
- checkpoint graph/version compatibility is explicit;
- arrays use structural sharing rather than hourly full copies; and
- restoring a checkpoint re-enters the same forward replay/delivery contracts.

No implementation hooks, schemas or placeholder abstractions for checkpoints
are required by this phase.

## 24. Implementation Record

Implemented on 2026-07-21 and committed after review.

### 24.1 Runtime and cache

- Added owner-checked cache unregistration and an atomic `CacheSchemaBatch`.
  A batch stages heterogeneous value/array registrations plus owned removals,
  validates the complete old/new key set, then swaps the registry under one
  write lock. Failed validation publishes no partial keys and removed watches
  close before a reused key receives fresh watchers.
- Added Runtime task removal across registry, ready/rerun queue and reverse
  dependency edges. A removed id can be installed again.
- Added `RuntimeHandle::reconcile_tasks`, executed as one worker command between
  task steps and snapshot requests. Its narrow `CacheSchema` stages the schema
  batch; task ids/kinds are prevalidated before commit, obsolete tasks are
  removed and new tasks are installed before the command returns.
- Added tests for failed multi-cell staging, atomic multi-cell replacement,
  old-watch closure, snapshot exclusion during a schema transaction, complete
  queue/dependency removal and id/key reuse.

### 24.2 Feed, graph and delivery

- Added explicit replay-feed `hold`, `reset` and `release` commands. Reset is
  legal only while held, increments epoch and atomically clears feed batches,
  cursor and status to source position zero before acknowledging.
- `LedgerSessionHandle::seek_to` now always uses the zero-origin rebuild path,
  including forward and same-position seeks, while preserving clock mode and
  speed.
- Projection installers now accept setup-time `Cache` or active Runtime schema
  registration and initialize status in the requested epoch.
- Added a session-owned installed graph record and idempotent
  `set_projections`. Reconciliation retains compatible dependencies, removes
  obsolete tasks/cells in reverse dependency order, installs missing nodes in
  dependency order, replaces delivery sources, then releases the feed.
- Canonical print and bars incremental work are bounded at 4,096 source items
  per normal task step.
- Removed delivery seek policy, barriers, suppression, `SeekFinal` and their
  metrics. Added safe source suspension/replacement and explicit unsubscribe.
  Receiver acknowledgement lineage now resets on every admitted authoritative
  snapshot, including a normal-cadence snapshot caused by epoch invalidation.
- A delivery executor is created for every replay feed, including an initially
  empty graph, so the first projection set can be installed without replacing
  the session.

### 24.3 Remux and Lens

- Added `remux/ledger/session/projections/set` and
  `remux/ledger/session/projections/unsubscribe`.
- Attach identity is now exactly session id plus raw id. A legacy
  `projections` request field is accepted but ignored, and attach returns the
  current server graph, clock and cursor.
- Status, diagnostic bars pull and subscription lookup use the mutable active
  graph. Graph-sensitive Remux reads are serialized across reconciliation.
- Explicit unsubscribe preserves only a per-session origin tombstone after
  removing the active consumer/origin entry. This is the minimal state needed
  to make retries idempotent without allowing another Remux origin to claim an
  old subscription id.
- Lens fresh-open defaults to `bars:1m`; reload adopts the projection set from
  attach. The old projection list is no longer stored as client identity.
- Added the 1m/5m/15m/1h bars menu immediately left of speed. Selection is
  serialized and latest-choice-wins: set exact server graph, create fresh
  accumulators, unsubscribe the prior receiver, subscribe with `have: null`,
  then accept ordinary snapshot/append frames. It never reruns the top-level
  session lifecycle.
- Removed the Lens full-bars pull API and all `seekFinal` parsing. Normal feed
  rebuilding, delivery pending and sampled viewer lag remain diagnostics and
  no longer produce action-bar activity; only resync and transport recovery do.

### 24.4 Validation completed

```text
Rust workspace tests: 242 passed
Rust rustfmt:          clean
Rust clippy:           clean with -D warnings
Lens tests:            16 passed across 5 files
Lens typecheck:        passed
Lens lint:             passed
Lens production build: passed
```

The Rust matrix includes forward/backward/same-position zero-origin epochs,
1m-like -> alternate interval -> original key reuse, empty -> first graph,
delivery epoch snapshot/reset/acknowledgement, mutable Remux attach/status/pull,
canonical duplicate rejection and idempotent origin-validated unsubscribe.
Lens adds an accumulator regression test proving a cadence snapshot in a new
epoch atomically replaces old bars and that `seekFinal` is rejected.

### 24.5 Validation still pending

- No prepared real ES artifact exists in this workspace, so the full-day 1m
  and 15m parity digests and subscriber/no-subscriber timing measurements were
  not fabricated from synthetic data. Run them when an installed day is
  available.
- Delivery metrics are exercised for bounded/coalesced behavior, but the
  full-day frame/collect/backpressure numbers have not yet been recorded.
- The native device feel-check in Section 21 remains pending. In particular,
  verify interval replacement while paused and at 5x, reload during rebuild,
  background progression while the viewer is closed, and deliberate fresh 1m
  state after exit to days.
