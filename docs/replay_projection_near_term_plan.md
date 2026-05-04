# Replay Projection Near-Term Implementation Plan

**Companion documents:**

```text
README.md
docs/vision.md
docs/study_graph_vision.md
docs/study_graph_phased_implementation.md
```

**Status:** Focused near-term implementation plan. The older phased study-graph plan remains useful as a long-range design reference, but it is too ambitious for the next product steps.

## 1. Purpose

Ledger now has the first real projection spine:

```text
ReplayDataset
  -> Session
  -> ReplaySimulator
  -> ReplayStepResult
  -> SessionTick
  -> ProjectionRuntime
  -> ProjectionFrame
```

The next goal is not to rush advanced L3 studies, async projection workers, journal integration, or model/gamma scaffolding. The next goal is to make the base replay/projection loop useful, deterministic, and visible through CLI first, then API/WebSocket and Lens.

The product priority is:

```text
validated data
  -> active Session with replay feed
  -> base projections
  -> CLI validation
  -> Session WebSocket transport
  -> first Lens replay surface
```

This document picks up after the completed projection contract/runtime/replay-integration work and replaces the old near-term sequence. It keeps the implementation focused on base projections, deterministic Session behavior, and a thin WebSocket transport before Lens rendering.

## 2. Current Implementation Audit

### 2.1 Completed and Worth Keeping

The following pieces are aligned with the vision and should remain:

```text
ledger-domain projection contracts
  ProjectionSpec, ProjectionKey, ProjectionManifest, ProjectionFrame,
  deterministic params hash, frame stamps, wake policies, and frame policies.

ledger projection runtime skeleton
  ProjectionRegistry, ProjectionFactory, ProjectionNode, ProjectionRuntime,
  lazy subscription, dependency resolution, cycle detection, node sharing,
  topological advancement, reset generation, metrics.

Session integration
  Session owns ProjectionRuntime.
  ReplayFeed wraps ReplaySimulator.
  Ledger converts feed batches to SessionTick.
  Session advance/clock/seek reports can include ProjectionFrames.

CLI session run and clock run
  Headless validation path exists and can exercise real ReplayDataset loading,
  replay cache hydration, deterministic stepping, and fake-clock playback.

API Session WebSocket
  `GET /sessions/ws` opens one feed-driven Session per socket, subscribes
  projections, advances/plays/seeks the Session, and returns canonical
  ProjectionFrames without recomputing projection payloads in the API.
```

This is the right boundary:

```text
ledger-replay = deterministic replay mechanics
ledger        = session orchestration and projection runtime
ledger-api    = HTTP/WebSocket transport
ledger-cli    = headless validation
lens          = rendering and workflow later
```

### 2.2 What Was Too Optimistic

The old phase plan moved too quickly into:

```text
advanced L3 studies
batch feature spines
async latest-wins projection workers
offline artifacts
journal/training memory
levels/gamma/0DTE
model studies
checkpointing
live convergence
```

Those are still directionally important, but they should not drive the immediate implementation. We do not yet have enough product pressure from Lens replay usage to know the exact shapes.

### 2.3 Domain Type Audit

The projection DTOs are broader than the immediate product needs. That was useful while shaping the vision, but it is now better to tighten the contract before API/Lens start depending on it.

Contract surface needed now:

```text
ProjectionId
ProjectionVersion
ProjectionSpec
ProjectionKey
ProjectionManifest
ProjectionOutputSchema
ProjectionFrame
ProjectionFrameStamp
ProjectionFrameOp
ProjectionWakePolicy::{EveryTick, OnEventMask}
ProjectionWakeEventMask
ProjectionDeliverySemantics::{ReplaceLatest, Append, PatchByKey, Snapshot}
ProjectionFramePolicy::{EmitEveryUpdate, EmitOnChange, EmitOnWindowClose}
SourceView::{ExchangeTruth, TraderVisibility, ExecutionSimulation}
TemporalPolicy::Causal
DependencyDecl
DependencyParams
```

Remove the fields/enums that only describe future capabilities:

```text
ProjectionExecutionType
ProjectionLagPolicy
ProjectionCachePolicy
TrustTier
ProjectionCostHint
ExpectedCost
MemoryClass
VisualizationHint
VisualizationSurface
ValidationDecl
ValidationKind
```

Remove optimistic enum variants and re-add them when a real feature needs them:

```text
ProjectionKind::{DerivedStudy, CompositeStudy, ModelStudy, Journal}
ProjectionUpdateMode::{Offline, OnlineAndOffline}
SourceView::{ExternalAsOf, JournalContext}
TemporalPolicy::{AsOf, Hindsight, ReviewOnly}
```

Also review these extra variants during cleanup because they are not required for the base replay loop:

```text
ProjectionKind::{Visual, Diagnostic}
ProjectionWakePolicy::{OnIntervalNs, Manual}
ProjectionFramePolicy::{CoalesceNs, Manual}
ProjectionFrameOp::{Delete, Heartbeat}
```

Contract cleanup rule:

```text
If a concept only explains future behavior, remove it.
Remove variants that do not serve the current feature.
Keep the projection frame/spec/manifest shape stable enough for CLI validation.
Re-add vocabulary only when a feature needs it and tests prove the semantics.
Do not let API/Lens depend on concepts that are not implemented.
```

If a removed concept returns later, it should be introduced as part of that feature's implementation plan, not preloaded into this one.

## 3. Revised Phase Map

### Already Done

```text
Phase A  Data Center and replay data control plane
Phase B  Active Session controller and replay cache
Phase C  Projection contracts, runtime skeleton, and SessionTick integration
```

### Near-Term Product Path

```text
Phase 1  Base replay projections and contract cleanup
Phase 2  Deterministic Session clock CLI validation
Phase 3  Session WebSocket transport
```

### Later Plans

```text
Lens replay surface
product-driven base projection expansion
first simple study
async projection execution
offline artifact projections
journal integration
levels/gamma/0DTE
model studies
checkpointing/cache lineage
live/replay convergence
```

These remain in the vision, but they should get their own plans when they become the next feature.

## 4. Phase 1 - Base Replay Projections and Contract Cleanup

### Objective

Implement the first useful base projections and remove projection vocabulary that is not needed for this feature.

### Deliverables

Add a small base projection module layout:

```text
crates/ledger/src/projection/base/mod.rs
crates/ledger/src/projection/base/cursor.rs
crates/ledger/src/projection/base/bbo.rs
crates/ledger/src/projection/base/trades.rs
crates/ledger/src/projection/base/bars.rs
```

Add a registration helper:

```rust
pub fn register_base_projections(registry: &mut ProjectionRegistry) -> Result<()>;
pub fn base_projection_registry() -> Result<ProjectionRegistry>;
```

Wire `Ledger::new(...)` and `Ledger::from_env()` to use the base registry by default once base projections exist.

Keep `Ledger::with_projection_registry(...)` for tests and custom experiments.

Implement the initial base projections:

```text
cursor:v1
bbo:v1
canonical_trades:v1
bars:v1
```

Do not implement orders/fills yet. The execution simulator exists, but the product-grade order command surface, account state, PnL, and journal linkage are not designed.

### Contract Triage Rules

Built-in base projections should use only current runtime semantics:

```text
temporal_policy: Causal
update_mode: Online
kind: Base
source_view: ExchangeTruth unless the projection explicitly exposes visibility/execution state
```

Remove or simplify unsupported/deferred domain fields and variants as listed in the domain audit. The current runtime should no longer need guards for execution modes that no longer exist.

### 4.1 cursor:v1

Purpose:

```text
Emit the replay heartbeat/cursor through the same ProjectionFrame protocol
used by all other projections.
```

Wake policy:

```text
EveryTick
```

Payload:

```json
{
  "batch_idx": 123,
  "cursor_ts_ns": "1773266400000000000",
  "applied_batch_idx": 122
}
```

Frame semantics:

```text
ReplaceLatest
EmitEveryUpdate
```

### 4.2 bbo:v1

Purpose:

```text
Emit best bid/offer from exchange truth.
```

Wake policy:

```text
OnEventMask { bbo_changed: true }
```

Payload:

```json
{
  "bid_price": 27063,
  "bid_size": 4,
  "ask_price": 27064,
  "ask_size": 1,
  "spread_ticks": 1
}
```

Frame semantics:

```text
ReplaceLatest
EmitOnChange
```

### 4.3 canonical_trades:v1

Purpose:

```text
Expose the official trade-print stream that bars/tape/studies consume.
```

This projection should not re-interpret raw DBN. It should consume the canonical trade records already produced by replay/book processing.

Required prerequisite:

```text
SessionTick must carry the actual canonical TradeRecord values for the applied
batch, not only trade_count.
```

```json
{
  "trades": [
    {
      "ts_event_ns": "1773266400000000000",
      "price": 27064,
      "size": 2,
      "side": "ask"
    }
  ]
}
```

Frame semantics:

```text
Append or ReplaceLatest per batch
EmitEveryUpdate when trades exist
```

Initial recommendation:

```text
Use one append frame per replay batch that contains zero or more trades.
Do not build a full retained tape yet.
```

### 4.4 bars:v1

Purpose:

```text
Build simple time bars from canonical trades.
```

Initial params:

```json
{
  "seconds": 60
}
```

Dependency:

```text
canonical_trades:v1
```

Initial source:

```text
ExchangeTruth
```

Do not implement:

```text
tick bars
volume bars
visible-trader bars
delta bars
RTH/ETH session segmentation
VWAP
volume profile
```

Those can be added once the first chart loop is real.

Payload:

```json
{
  "bar_start_ts_ns": "1773266400000000000",
  "bar_end_ts_ns": "1773266460000000000",
  "open": 27064,
  "high": 27066,
  "low": 27063,
  "close": 27065,
  "volume": 48,
  "trade_count": 17,
  "final": false
}
```

Frame semantics:

```text
PatchByKey or ReplaceLatest
EmitOnWindowClose for final bars
optional current in-progress bar after the stable path works
```

### Tests

```text
base_registry_registers_expected_manifests
base_registry_rejects_duplicate_projection
ledger_new_contains_base_projection_registry
base_projection_manifests_validate
cursor_projection_emits_every_tick
bbo_projection_emits_only_on_bbo_change
canonical_trades_projection_emits_trade_batch
bars_projection_builds_one_minute_bar
bars_projection_finalizes_closed_window
bars_projection_ignores_empty_trade_batches
projection_dependencies_feed_bars_from_trades
```

### Acceptance

```text
cargo test -p ledger-domain projection
cargo test -p ledger projection::base
cargo test -p ledger projection::registry
cargo test -p ledger session
cargo test -p ledger-replay
cargo test --workspace
```

## 5. Phase 2 - Projection and Clock CLI Validation

### Objective

Give Codex and humans a headless way to validate projection behavior and deterministic Session clock behavior over real ReplayDatasets before API/Lens depend on it.

### Commands

```bash
ledger projection list
ledger projection manifest --id bbo --version 1
ledger projection graph --projection bbo:v1 --params '{}'
ledger projection run --symbol ESH6 --date 2026-03-12 --projection bbo:v1 --params '{}' --batches 10000 --digest
ledger projection run --symbol ESH6 --date 2026-03-12 --projection bars:v1 --params '{"seconds":60}' --batches 100000 --jsonl out.frames.jsonl --digest
```

### Required Behavior

`projection list`:

```text
Print available projection manifests as stable JSON.
```

`projection manifest`:

```text
Print one manifest as stable JSON.
```

`projection graph`:

```text
Resolve dependency graph without opening a ReplayDataset.
Return root, nodes, edges, and cycle=false.
```

`projection run`:

```text
open cached ReplayDataset
hydrate from R2 if missing
start Session
subscribe projection
step requested batches
write optional JSONL ProjectionFrames
print summary JSON
compute stable digest from deterministic frame fields
```

`session clock-run`:

```text
open cached ReplayDataset
hydrate from R2 if missing
start Session
subscribe projection
play with deterministic fake monotonic clock
pump feed batches due by target feed time within a batch budget
write optional JSONL ProjectionFrames
print clock summary JSON
compute stable digest from deterministic frame fields
```

Digest input should include:

```text
projection_key
generation
feed_seq
feed_ts_ns
source timestamp range
batch_idx
cursor_ts_ns
frame op
payload schema
payload
```

Digest input should exclude:

```text
produced_at_ns
wall-clock timings
logs
job ids
absolute local paths
```

### Output Summary

```json
{
  "projection": {
    "id": "bbo",
    "version": 1,
    "key": "bbo:v1:sha256:..."
  },
  "dataset": {
    "symbol": "ESH6",
    "market_date": "2026-03-12",
    "replay_dataset_id": "ES-ESH6-2026-03-12:replay:v1"
  },
  "run": {
    "requested_batches": 10000,
    "applied_batches": 10000,
    "frames": 812,
    "first_feed_ts_ns": "1773266400000000000",
    "last_feed_ts_ns": "1773266500000000000",
    "digest": "sha256:..."
  },
  "passed": true
}
```

### Tests

```text
projection_cli_list_returns_base_manifests
projection_cli_manifest_returns_requested_manifest
projection_cli_graph_resolves_bars_dependency
projection_digest_ignores_produced_at
projection_digest_changes_when_payload_changes
```

### Agentic Validation

Use the available real data:

```bash
cargo run -p ledger-cli -- projection list
cargo run -p ledger-cli -- projection manifest --id bbo --version 1
cargo run -p ledger-cli -- projection graph --projection bars:v1 --params '{"seconds":60}'
cargo run -p ledger-cli -- projection run --symbol ESH6 --date 2026-03-12 --projection bbo:v1 --params '{}' --batches 1000 --digest
```

If R2 hydration is required in the Codex sandbox, rerun the CLI command with network approval rather than adding bypass code.

## 6. Phase 3 - Session WebSocket Transport

### Objective

Expose the feed-driven `Session` over a thin WebSocket transport without adding a second replay/projection path.

### Route

```text
GET /sessions/ws
```

### Commands

```text
open_session
subscribe_projection
unsubscribe_projection
advance
play
pause
set_speed
seek
snapshot
close_session
```

### Required Behavior

```text
one WebSocket owns at most one active Session
open_session hydrates a cached ReplayDataset through Ledger
subscribe_projection returns initial snapshot frames from ProjectionRuntime
advance manually applies feed batches while paused
play/pause/set_speed use the Session clock
seek pauses the Session and returns reset generation snapshot frames
session_frame_batch returns SessionSnapshot plus canonical ProjectionFrames
nanosecond fields cross the API boundary as strings
```

The API invariant:

```text
ledger-api parses socket messages and serializes responses.
ledger-api does not compute bars, BBO, trades, dependencies, or projection payloads.
```

### Tests

```text
open_session_protocol_maps_ns_strings_to_ledger_request
invalid_nanosecond_string_is_a_protocol_error
subscribe_before_open_returns_protocol_error
subscribe_and_advance_return_projection_frames
seek_returns_reset_frames_with_new_generation
play_pump_emits_due_feed_frames_and_pause_stops_pump
```

## 7. Out of Scope for This Plan

### Lens

Lens replay rendering is intentionally not specified here.

The next plan after this one should start from the CLI-validated projection loop and Session WebSocket transport and then define:

```text
Lens connection/session/projection state
the first replay screen
```

The invariant remains:

```text
API and Lens must consume projection frames from ledger.
They must not recompute bars, BBO, trade facts, or projection dependencies.
```

### Orders and Fills

Defer `orders:v1` and `fills:v1`.

Reason:

```text
ExecutionSimulator exists, but the product-grade order command surface,
account state, PnL, partial-fill semantics, queue model UX, and journal
linkage are not designed yet.
```

Bring this back when Lens needs simulated order entry.

### Async and Offline Work

Out of scope:

```text
async projection execution
offline artifact projections
latest-wins/catch-up lag semantics
```

Reason:

```text
We need real projection usage and profiling before designing worker queues,
staleness semantics, and artifact-backed projections.
```

The current domain vocabulary should be removed until this work is designed.

### Journal

Defer journal integration until:

```text
Lens can replay a day.
Projection frames are stable.
The user has real review workflows to capture.
```

The journal should eventually reference projection identity/version/params/cursor, but it should not lead the implementation.

### Levels, Gamma, and Models

Defer until:

```text
base replay surface works
bars/trades/DOM context are visible
manual or prior-day levels are product-needed
external as-of artifact behavior has a concrete input source
```

## 7. Definition of Done for Near-Term Projection Work

Any projection added in the near-term path must satisfy:

```text
1. Manifest validates.
2. Params have a deterministic hash.
3. Projection can be listed by CLI.
4. Dependency graph can be resolved by CLI.
5. Projection can run over a real ReplayDataset from CLI.
6. CLI digest is deterministic across repeated runs.
7. Runtime tests prove wake/frame behavior.
8. Session tests prove frames are emitted from real SessionTick input.
```
