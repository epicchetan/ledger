# Ledger Study Graph Vision

---

## 1. Executive Summary

Ledger's study graph should be a **causal, typed, versioned, subscription-driven projection graph over active `Session` truth**.

It should not be implemented as a loose list of chart indicators. The graph is the market-state computation layer between deterministic replay truth and every downstream consumer: CLI validation, API/WebSocket transport, Lens charts, DOM panels, heatmaps, journal prompts, model outputs, and future live mode.

The central idea is:

```text
ReplayDataset / ContextWindow / LevelSets / Journal Context
                    ↓
              Session
   deterministic exchange truth + visibility + execution simulation
                    ↓
             ProjectionRuntime
     base projections → derived studies → composite/model studies
                    ↓
             ProjectionFrames
                    ↓
       CLI validation / API / Lens / Journal
```

The graph is **lazy**: only subscribed projections and their dependencies are instantiated. It is **layered**: expensive microstructure facts are computed once and reused. It is **typed**: every projection declares a manifest, parameters, dependencies, output schema, execution type, lag policy, trust tier, and validation expectations. It is **timing-aware**: replay truth, trader visibility, external as-of data, async model results, and review-only hindsight labels are not allowed to blur together.

This is the foundation that makes Ledger different from normal trading platforms. Most platforms expose indicators. Ledger should expose a controlled experimental market-state engine.

---

## 2. Why This Exists

The current Ledger vision already establishes several non-negotiable product goals:

```text
full-day ES L3 data
→ validated replay artifacts
→ deterministic order-book truth
→ realistic replay / visibility / execution simulation
→ studies, levels, models, charts, and heatmaps
→ journaling and review
→ AI-assisted iteration
→ better discretionary trading decisions
```

The study graph is the missing boundary between replay truth and everything the user sees or later analyzes.

It must solve these problems:

1. **Avoid one-off indicator code paths.** New studies should be modules with declared inputs, outputs, validation, and versioning.
2. **Avoid duplicated L3 work.** If three consumers need bars or batch features, Ledger computes them once.
3. **Preserve replay truth.** Study computation must not corrupt or reinterpret the canonical order book.
4. **Make timing honest.** The system must distinguish what was knowable at replay cursor `T`, what arrived late, what came from an external as-of artifact, and what is hindsight.
5. **Support rapid experimentation.** Codex should be able to implement a new study against a stable contract, and a validation agent should be able to prove it through CLI before API/Lens depend on it.
6. **Support future live mode.** Replay and live should converge below the UI. Lens should subscribe to projections, not care whether the source is replay or live.
7. **Support future checkpointing and optimization.** V1 does not need full checkpointing, but the architecture should leave clean seams for it.

---

## 3. Core Vocabulary

Use these terms consistently.

### ReplayDataset

Immutable replay inputs for a `MarketDay`.

A `ReplayDataset` is not an active user session. It is durable input data: prepared events, batches, canonical trade artifacts, book-check artifacts, validation summaries, and future cached projection artifacts.

### Session

An active mutable replay/training session over one `ReplayDataset`.

A `Session` owns the replay cursor, play/pause/speed state, visibility profile, execution profile, simulated orders/fills, and the active projection runtime.

### Exchange Truth

The exact historical event batches applied to one canonical L3 order book.

This is the ground truth used for deterministic replay, book checks, and execution simulation.

### Trader Visibility

Delayed and/or coalesced frames representing what the trader could actually see.

This is separate from exchange truth. A trainee should not be shown impossible instantaneous information unless explicitly in debug/research mode.

### Execution Simulation

Simulated orders arriving after configured latency and filling against the true book at arrival time.

Execution simulation depends on exchange truth, not on delayed UI visibility.

### Projection

Any typed, time-indexed view derived from replay truth, visibility frames, execution state, external artifacts, level sets, journal context, or other projections.

Examples:

```text
cursor
status
bbo
depth
canonical_trades
bars
session_vwap
batch_features
price_level_set
absorption_score
regime_classifier
model_score
journal_prompt
```

### Study

A projection with trading/research semantics.

Examples:

```text
sweep_detector
absorption_score
book_pressure
liquidity_pull_stack
failed_auction_detector
level_reaction_score
gamma_l3_reversal_context
continuation_quality_score
```

All studies are projections. Not all projections are studies.

### View

A Lens rendering of one or more projections.

Examples:

```text
chart candle layer
DOM panel
trade tape
marker overlay
level heatmap
score panel
journal prompt panel
replay drill trigger
```

Lens renders output schemas. It should not compute market facts locally.

### ProjectionGraph / StudyGraph

The directed dependency graph of active projection nodes.

`ProjectionGraph` is the more precise term; `StudyGraph` is acceptable product language. Internally, the graph should include base projections, derived studies, composite studies, model studies, visual projections, level projections, and journal prompt projections.

### ProjectionRuntime

The per-`Session` engine that owns active projection nodes, resolves dependencies, advances nodes when due, applies async results, coalesces frames, and emits `ProjectionFrame`s.

### ProjectionManifest

The declared contract for a projection: identity, version, parameters, dependencies, output schema, execution type, wake policy, delivery semantics, lag policy, cache policy, trust tier, cost hint, visualization hints, and validation requirements.

### ProjectionKey

The canonical identity of one projection instance:

```text
projection id + projection version + canonical params hash
```

Example:

```text
bars:v1:sha256(params)
```

Two subscriptions with the same key share the same node.

### ProjectionFrame

A typed update emitted by the runtime for CLI/API/Lens/journal consumers.

A frame always includes projection identity, generation, cursor stamps, schema, operation, payload, and timing/lag metadata.

### SessionTick

The root per-batch input from `Session` into `ProjectionRuntime`.

A `SessionTick` represents one deterministic replay advancement: exchange events applied, canonical book state after the batch, visibility frames emitted, execution state changes, fills, and cheap event flags.

---

## 4. Repository Boundary Alignment

The study graph should respect the existing crate boundaries.

```text
ledger-domain
  Shared pure DTOs and schema contracts:
  ProjectionSpec, ProjectionKey, ProjectionManifest, ProjectionFrame,
  ProjectionPayload, output schema enums, temporal policy, execution policy,
  level DTOs, timestamps, stable IDs.

ledger-book
  Deterministic L3 order-book truth over normalized MBO events.
  Must not know about studies.

ledger-replay
  Headless replay simulator, trader visibility, simulated execution,
  latency, queue-ahead, same-timestamp policy, seek behavior.
  Must not own study runtime or chart-specific computation.

ledger
  Application orchestration. Owns active Session controller and
  ProjectionRuntime integration. Feeds SessionTick into projections.

ledger-api
  Transport adapter. Exposes replay controls and projection subscription
  frames later. Should not compute projections itself.

ledger-cli
  Terminal adapter. Adds projection list/manifest/run/profile commands
  for agentic validation before API/Lens depend on projections.

lens
  Web operating surface. Subscribes to projections and renders output
  schemas. Does not compute trading facts.
```

For V1, the projection runtime can live under `crates/ledger/src/projection/` while pure DTOs live in `ledger-domain`. If it grows large, it can later move to a dedicated `ledger-projection` crate.

Recommended V1 layout:

```text
crates/domain/src/projection.rs
  ProjectionSpec
  ProjectionKey
  ProjectionManifest
  ProjectionFrame
  ProjectionPayload
  ProjectionOutputSchema
  TemporalPolicy
  SourceView
  ProjectionExecutionType
  ProjectionLagPolicy
  ProjectionDeliverySemantics
  ProjectionWakePolicy
  ProjectionFramePolicy
  ProjectionCostHint
  ValidationDecl
  VisualizationHint

crates/ledger/src/projection/
  mod.rs
  runtime.rs
  registry.rs
  node.rs
  metrics.rs
  async_scheduler.rs        # may be stubbed in early phases
  artifact_reader.rs        # may be stubbed in early phases
  base/
    cursor.rs
    status.rs
    bbo.rs
    trades.rs
    depth.rs
    bars.rs
    batch_features.rs
    vwap.rs
  studies/
    sweep_detector.rs
    absorption_score.rs
    book_pressure.rs
```

---

## 5. The Big Architectural Decision

The graph should be:

```text
pull to construct, push to update
```

### Pull to construct

A consumer subscribes to a projection:

```json
{
  "id": "absorption_score",
  "version": 1,
  "params": {
    "window_ms": 500,
    "price_levels": 3,
    "source_view": "visible"
  }
}
```

The runtime resolves its dependency tree:

```text
absorption_score:v1
└── batch_features:v1 { price_levels: 3, source_view: visible }
    ├── bbo:v1 { source_view: visible }
    ├── canonical_trades:v1 { source_view: visible }
    └── depth:v1 { levels: 3, source_view: visible }
```

Only missing nodes are instantiated. Existing nodes are reused. Reference counts are incremented. The dependency graph is checked for cycles and ordered topologically.

### Push to update

On each replay advancement:

```text
Session applies one exchange batch
→ Session emits SessionTick
→ ProjectionRuntime checks wake policies and dirty dependencies
→ active nodes advance in topological order
→ async jobs are enqueued if needed
→ completed async results are accepted or dropped by policy
→ frames are drained according to frame policies
→ CLI/API/Lens receives ProjectionFrames
```

The graph is not a polling system where every node runs blindly every batch. It is a subscription-driven DAG with explicit wake policies.

---

## 6. SessionTick: The Root Clock of the Graph

Every online projection should ultimately be driven by `SessionTick`.

A conceptual shape:

```rust
pub struct SessionTick {
    pub session_id: SessionId,
    pub replay_dataset_id: ReplayDatasetId,
    pub generation: u64,

    pub batch_idx: u64,
    pub cursor_ts_ns: UnixNanos,

    pub exchange_events: Arc<[MboEvent]>,

    pub truth_bbo_before: Option<Bbo>,
    pub truth_bbo_after: Option<Bbo>,
    pub truth_depth_after: Option<DepthSnapshotRef>,

    pub canonical_trades: Arc<[CanonicalTradePrint]>,

    pub visibility_frames: Arc<[VisibilityFrame]>,
    pub simulated_order_events: Arc<[SimOrderEvent]>,
    pub simulated_fills: Arc<[SimFill]>,

    pub book_checksum_after: Option<BookChecksum>,
    pub flags: SessionTickFlags,
}
```

Cheap event flags let the runtime skip most nodes quickly:

```rust
pub struct SessionTickFlags {
    pub has_exchange_events: bool,
    pub has_trades: bool,
    pub bbo_changed: bool,
    pub depth_changed: bool,
    pub has_visibility_frame: bool,
    pub has_fill_event: bool,
    pub has_order_event: bool,
    pub has_external_snapshot: bool,
}
```

`SessionTick` is the only online clock for the projection runtime.

Important rule:

```text
Projection nodes do not mutate the canonical book.
Projection nodes do not reach around Session to load raw data.
Projection nodes consume SessionTicks, dependency outputs, immutable snapshots,
or declared artifacts.
```

---

## 7. Source Views

A projection must declare or parameterize which source view it uses.

```rust
pub enum SourceView {
    ExchangeTruth,
    TraderVisibility,
    ExecutionSimulation,
    ExternalAsOf,
    JournalContext,
}
```

Examples:

```text
bbo:v1 { source_view: ExchangeTruth }
  Uses canonical book state after each exchange batch.

bbo:v1 { source_view: TraderVisibility }
  Uses visibility frames representing what the trader could see.

bars:v1 { source_view: TraderVisibility }
  Builds bars from visible canonical trade prints.

bars:v1 { source_view: ExchangeTruth }
  Research/debug bars from true event stream.

fills:v1 { source_view: ExecutionSimulation }
  Emits simulated fills and order lifecycle events.

level_sets:gamma:v1 { source_view: ExternalAsOf }
  Reveals latest valid gamma snapshot where snapshot_ts <= cursor.
```

Source view is not optional decoration. It protects the separation between exchange truth, trader visibility, and execution simulation.

---

## 8. Projection Manifest

Every projection must have a manifest.

The manifest is the contract that lets Codex implement modules safely and lets validation agents reject bad ones before API/Lens depend on them.

A conceptual V1 structure:

```rust
pub struct ProjectionManifest {
    pub id: String,
    pub version: u16,

    pub name: String,
    pub description: String,
    pub kind: ProjectionKind,

    pub params_schema: serde_json::Value,
    pub default_params: serde_json::Value,

    pub dependencies: Vec<DependencyDecl>,

    pub output_schema: ProjectionOutputSchema,
    pub update_mode: ProjectionUpdateMode,

    pub source_view: Option<SourceView>,
    pub temporal_policy: TemporalPolicy,

    pub wake_policy: ProjectionWakePolicy,
    pub execution_type: ProjectionExecutionType,
    pub delivery_semantics: ProjectionDeliverySemantics,
    pub frame_policy: ProjectionFramePolicy,
    pub lag_policy: ProjectionLagPolicy,

    pub cache_policy: ProjectionCachePolicy,
    pub trust_tier: TrustTier,
    pub cost_hint: ProjectionCostHint,

    pub visualization: Vec<VisualizationHint>,
    pub validation: Vec<ValidationDecl>,
}
```

### ProjectionKind

```rust
pub enum ProjectionKind {
    Base,
    DerivedStudy,
    CompositeStudy,
    ModelStudy,
    Visual,
    Journal,
    Diagnostic,
}
```

### ProjectionUpdateMode

```rust
pub enum ProjectionUpdateMode {
    Online,
    Offline,
    OnlineAndOffline,
}
```

### ProjectionCachePolicy

```rust
pub enum ProjectionCachePolicy {
    None,
    SessionMemory,
    LocalReadThrough,
    DatasetArtifact,
}
```

### TrustTier

```rust
pub enum TrustTier {
    Core,
    Experimental,
    ResearchOnly,
}
```

### CostHint

```rust
pub struct ProjectionCostHint {
    pub expected_cost: ExpectedCost,
    pub max_inline_us: Option<u64>,
    pub max_output_hz: Option<u32>,
    pub max_lag_ms: Option<u64>,
    pub memory_class: MemoryClass,
}

pub enum ExpectedCost {
    Tiny,
    Cheap,
    Medium,
    Heavy,
}

pub enum MemoryClass {
    SmallState,
    RollingWindow,
    LargeSnapshot,
    ArtifactBacked,
}
```

---

## 9. What `v1` Means

`v1` is the public contract version of a projection.

It is not:

```text
the repo version
the implementation attempt number
the CLI version
the API version
```

It means:

```text
projection id
+ parameter contract
+ dependency semantics
+ output schema
+ timestamp semantics
+ delivery semantics
+ documented behavior
```

Example:

```text
bars:v1
```

means:

```text
Projection id: bars
Projection contract version: 1
Output schema: candles_v1
Parameter contract: kind/time_seconds/trade_count/volume/source_view
Semantics: documented bar-building policy
```

Bump projection version when changing any of these materially:

```text
output schema
parameter meaning
timestamp semantics
backpainting behavior
dependency semantics
canonical trade-print policy used by the projection
study semantics or interpretation
```

Do not bump version for:

```text
internal refactor
performance improvement
bug fix that restores intended documented behavior
additional optional metadata field that old consumers can ignore
```

Store `id` and `version` as separate fields:

```rust
ProjectionSpec {
    id: "bars".to_string(),
    version: 1,
    params: ...,
}
```

Render for humans as:

```text
bars:v1
```

---

## 10. Example Manifests

### `bbo:v1`

```json
{
  "id": "bbo",
  "version": 1,
  "name": "Best Bid / Offer",
  "description": "Current BBO derived from either exchange truth or trader visibility.",
  "kind": "base",
  "params_schema": {
    "type": "object",
    "required": ["source_view"],
    "properties": {
      "source_view": { "enum": ["truth", "visible"] }
    }
  },
  "default_params": { "source_view": "visible" },
  "dependencies": [],
  "output_schema": "bbo_v1",
  "update_mode": "online",
  "source_view": "trader_visibility",
  "temporal_policy": "causal",
  "wake_policy": { "on_event_mask": { "bbo_change": true, "visibility_frame": true } },
  "execution_type": "core_sync",
  "delivery_semantics": "replace_latest",
  "frame_policy": "emit_on_change",
  "lag_policy": "prohibited",
  "cache_policy": "none",
  "trust_tier": "core",
  "cost_hint": {
    "expected_cost": "tiny",
    "max_inline_us": 10,
    "memory_class": "small_state"
  },
  "visualization": [
    { "surface": "chart", "role": "price_context" },
    { "surface": "dom", "role": "inside_market" }
  ],
  "validation": [
    { "kind": "deterministic_digest" },
    { "kind": "profile_budget" }
  ]
}
```

### `bars:v1`

```json
{
  "id": "bars",
  "version": 1,
  "name": "Bars",
  "description": "Builds time, tick, or volume bars from canonical trade prints.",
  "kind": "base",
  "params_schema": {
    "type": "object",
    "required": ["kind", "source_view"],
    "properties": {
      "kind": { "enum": ["time", "tick", "volume"] },
      "seconds": { "type": "integer", "minimum": 1 },
      "trades": { "type": "integer", "minimum": 1 },
      "volume": { "type": "integer", "minimum": 1 },
      "source_view": { "enum": ["truth", "visible"] }
    }
  },
  "default_params": {
    "kind": "time",
    "seconds": 60,
    "source_view": "visible"
  },
  "dependencies": [
    { "id": "canonical_trades", "version": 1, "params_from": ["source_view"] }
  ],
  "output_schema": "candles_v1",
  "update_mode": "online",
  "source_view": "trader_visibility",
  "temporal_policy": "causal",
  "wake_policy": { "on_event_mask": { "trades": true } },
  "execution_type": "inline_sync",
  "delivery_semantics": "patch_by_key",
  "frame_policy": "emit_on_window_close_or_coalesced_live_bar",
  "lag_policy": "prohibited_when_due",
  "cache_policy": "session_memory",
  "trust_tier": "core",
  "cost_hint": {
    "expected_cost": "cheap",
    "max_inline_us": 50,
    "memory_class": "small_state"
  },
  "visualization": [
    { "surface": "chart", "role": "primary_series" }
  ],
  "validation": [
    { "kind": "synthetic_fixture", "name": "time_bar_rollover" },
    { "kind": "synthetic_fixture", "name": "tick_bar_rollover" },
    { "kind": "deterministic_digest" },
    { "kind": "profile_budget" }
  ]
}
```

### `absorption_score:v1`

```json
{
  "id": "absorption_score",
  "version": 1,
  "name": "Absorption Score",
  "description": "Scores aggressive trade pressure that fails to displace price near recent liquidity.",
  "kind": "derived_study",
  "params_schema": {
    "type": "object",
    "required": ["window_ms", "price_levels", "source_view"],
    "properties": {
      "window_ms": { "type": "integer", "minimum": 1 },
      "price_levels": { "type": "integer", "minimum": 1 },
      "source_view": { "enum": ["truth", "visible"] }
    }
  },
  "default_params": {
    "window_ms": 500,
    "price_levels": 3,
    "source_view": "visible"
  },
  "dependencies": [
    { "id": "batch_features", "version": 1, "params_from": ["price_levels", "source_view"] },
    { "id": "bars", "version": 1, "params": { "kind": "time", "seconds": 60 } }
  ],
  "output_schema": "time_series_v1",
  "update_mode": "online",
  "source_view": "trader_visibility",
  "temporal_policy": "causal",
  "wake_policy": "when_dependency_changed",
  "execution_type": "inline_sync",
  "delivery_semantics": "patch_by_key",
  "frame_policy": "emit_on_change",
  "lag_policy": "prohibited_when_due",
  "cache_policy": "session_memory",
  "trust_tier": "experimental",
  "cost_hint": {
    "expected_cost": "cheap",
    "max_inline_us": 100,
    "memory_class": "rolling_window"
  },
  "visualization": [
    { "surface": "chart", "role": "overlay" },
    { "surface": "panel", "role": "score" }
  ],
  "validation": [
    { "kind": "synthetic_fixture", "name": "absorption_at_price_without_displacement" },
    { "kind": "no_future_leakage" },
    { "kind": "deterministic_digest" },
    { "kind": "profile_budget" }
  ]
}
```

### `regime_classifier:v1`

```json
{
  "id": "regime_classifier",
  "version": 1,
  "name": "Regime Classifier",
  "description": "Async experimental classifier for market-state regime.",
  "kind": "model_study",
  "dependencies": [
    { "id": "batch_features", "version": 1 },
    { "id": "bars", "version": 1, "params": { "kind": "time", "seconds": 60 } },
    { "id": "session_vwap", "version": 1 }
  ],
  "output_schema": "score_band_v1",
  "update_mode": "online",
  "temporal_policy": "causal",
  "wake_policy": { "every_replay_time": { "interval_ms": 1000 } },
  "execution_type": "async_latest",
  "delivery_semantics": "replace_latest",
  "frame_policy": "emit_on_accepted_async_result",
  "lag_policy": {
    "latest_wins": {
      "mark_stale_after_ms": 1500,
      "drop_after_ms": 5000,
      "cancel_pending_on_newer_input": true
    }
  },
  "cache_policy": "session_memory",
  "trust_tier": "experimental",
  "cost_hint": {
    "expected_cost": "heavy",
    "max_lag_ms": 1500,
    "max_output_hz": 1,
    "memory_class": "rolling_window"
  },
  "visualization": [
    { "surface": "panel", "role": "classification" },
    { "surface": "chart", "role": "background_band" }
  ],
  "validation": [
    { "kind": "deterministic_when_seeded" },
    { "kind": "no_future_leakage" },
    { "kind": "async_lag_budget" },
    { "kind": "profile_budget" }
  ]
}
```

---

## 11. Runtime Node Model

Do not make the graph fully generic at the Rust type level. That will make the system hard to extend.

Use typed payload enums and dynamic node dispatch.

### ProjectionFactory

```rust
pub trait ProjectionFactory {
    fn manifest(&self) -> ProjectionManifest;

    fn resolve_dependencies(
        &self,
        params: &serde_json::Value,
    ) -> Result<Vec<ProjectionSpec>>;

    fn build(
        &self,
        key: ProjectionKey,
        params: serde_json::Value,
    ) -> Result<Box<dyn ProjectionNode>>;
}
```

### ProjectionNode

```rust
pub trait ProjectionNode {
    fn key(&self) -> &ProjectionKey;

    fn wake_policy(&self) -> ProjectionWakePolicy;
    fn execution_type(&self) -> ProjectionExecutionType;
    fn delivery_semantics(&self) -> ProjectionDeliverySemantics;
    fn frame_policy(&self) -> ProjectionFramePolicy;
    fn lag_policy(&self) -> ProjectionLagPolicy;

    fn advance(&mut self, ctx: &ProjectionContext) -> Result<ProjectionAdvance>;

    fn snapshot(&self) -> ProjectionPayload;

    fn drain_frames(
        &mut self,
        ctx: &FrameContext,
    ) -> Result<Vec<ProjectionFrame>>;

    fn reset(&mut self) -> Result<()>;
}
```

### ProjectionAdvance

```rust
pub enum ProjectionAdvance {
    NoChange,
    StateChanged,
    NeedsAsyncWork(AsyncProjectionJob),
}
```

This two-phase model is important:

```text
advance state first
emit frames later
```

Projection state and UI/API frame emission are not the same thing. A projection may advance every relevant tick and emit only coalesced frames.

---

## 12. Projections Do Not Import Each Other Directly

A projection implementation should not instantiate or call another concrete projection implementation.

Bad:

```rust
use crate::projection::studies::absorption_score::AbsorptionScoreNode;

fn run() {
    let node = AbsorptionScoreNode::new(...);
    node.compute(...);
}
```

Good:

```rust
impl ProjectionFactory for LevelReactionScoreFactory {
    fn resolve_dependencies(&self, params: &Value) -> Result<Vec<ProjectionSpec>> {
        Ok(vec![
            ProjectionSpec::new("batch_features", 1, params_for_batch_features(params)),
            ProjectionSpec::new("level_sets", 1, params_for_levels(params)),
            ProjectionSpec::new("bars", 1, params_for_bars(params)),
        ])
    }
}
```

At runtime, the node receives dependency outputs through `ProjectionContext`:

```rust
impl ProjectionNode for LevelReactionScoreNode {
    fn advance(&mut self, ctx: &ProjectionContext) -> Result<ProjectionAdvance> {
        let features = ctx.input::<BatchFeaturesPayload>("batch_features")?;
        let levels = ctx.input::<PriceLevelSetPayload>("level_sets")?;
        let bars = ctx.input::<CandlesPayload>("bars")?;

        // update local state
        // maybe emit score/marker/alert later
    }
}
```

This lets studies depend on projection **contracts**, not on concrete code modules. It is essential for Codex-generated studies and validation.

---

## 13. ProjectionPayload and Output Schemas

The system should use a small set of reusable output schemas instead of inventing a new schema for every study.

Conceptual enum:

```rust
pub enum ProjectionPayload {
    Cursor(CursorPayload),
    Status(StatusPayload),
    Bbo(BboPayload),
    Trades(TradePatchPayload),
    Depth(DepthPayload),
    Candles(CandlePatchPayload),
    BatchFeatures(BatchFeaturesPayload),
    TimeSeries(TimeSeriesPayload),
    PriceSeries(PriceSeriesPayload),
    PriceLevels(PriceLevelSetPayload),
    Markers(MarkerPayload),
    Zones(ZonePayload),
    Heatmap(HeatmapPayload),
    ScoreBand(ScoreBandPayload),
    Alerts(AlertPayload),
    JournalPrompts(JournalPromptPayload),
    Diagnostics(DiagnosticsPayload),
}
```

Recommended schema set:

```text
cursor_v1
status_v1
bbo_v1
depth_v1
trade_events_v1
order_events_v1
fill_events_v1
candles_v1
batch_features_v1
time_series_v1
price_series_v1
price_level_set_v1
markers_v1
zones_v1
heatmap_cells_v1
score_band_v1
alerts_v1
journal_prompts_v1
diagnostics_v1
```

Lens should render by schema, not by study name.

Example:

```text
bars:v1
  emits candles_v1
  Lens chart renderer knows candles_v1

absorption_score:v1
  emits time_series_v1 and possibly markers_v1
  Lens chart overlay renderer knows those schemas

gamma_l3_reversal_context:v1
  emits price_level_set_v1, heatmap_cells_v1, alerts_v1
  Lens renderers already know those schemas
```

This keeps Lens stable while research evolves quickly.

---

## 14. Wake Policies

Not all projections should run every batch. Each projection declares when it wakes.

```rust
pub enum ProjectionWakePolicy {
    EveryBatch,

    OnEventMask {
        trades: bool,
        bbo_change: bool,
        depth_change: bool,
        visibility_frame: bool,
        fill_event: bool,
        order_event: bool,
        external_snapshot: bool,
    },

    WhenDependencyChanged,

    OnWindowClose {
        window: ProjectionWindow,
    },

    EveryReplayTime {
        interval_ms: u64,
    },

    EveryNBatches {
        n: u64,
    },

    OnDemand,

    OfflineOnly,
}
```

Examples:

```text
cursor:v1
  wake_policy: EveryBatch

bbo:v1
  wake_policy: OnEventMask { bbo_change: true, visibility_frame: true }

canonical_trades:v1
  wake_policy: OnEventMask { trades: true, visibility_frame: true }

bars:v1
  wake_policy: OnEventMask { trades: true }

session_vwap:v1
  wake_policy: OnEventMask { trades: true }

depth:50:v1
  wake_policy: OnEventMask { depth_change: true, visibility_frame: true }

regime_classifier:v1
  wake_policy: EveryReplayTime { interval_ms: 1000 }

gamma_levels:v1
  wake_policy: OnEventMask { external_snapshot: true }

review_labels:v1
  wake_policy: OfflineOnly
```

The runtime fast path should skip a node when all are false:

```text
no matching event mask
no dependency changed
no timer due
no explicit on-demand request
```

---

## 15. Frame Policies

Frame emission is separate from state advancement.

```rust
pub enum ProjectionFramePolicy {
    EmitEveryUpdate,
    EmitOnChange,
    EmitOnWindowClose,

    CoalesceWallClock {
        max_fps: u32,
    },

    CoalesceReplayTime {
        interval_ms: u64,
    },

    EmitOnAcceptedAsyncResult,
    SnapshotOnly,
}
```

Two kinds of coalescing matter.

### Compute coalescing

The projection state itself does not need to update every tick.

Example:

```text
regime_classifier wakes once per replay second
```

### Frame coalescing

The projection state may update often, but consumers do not need every frame.

Example:

```text
depth state may update on many batches
Lens receives at most 20 visual frames per second
```

Important invariant:

```text
Projection state cadence, ProjectionFrame cadence, and Lens render cadence are separate.
```

---

## 16. Delivery Semantics

Lag and frame handling depend on payload semantics. Newest-wins is correct for some payloads and wrong for others.

```rust
pub enum ProjectionDeliverySemantics {
    ReplaceLatest,
    AppendOrdered,
    PatchByKey,
    SnapshotOnly,
}
```

### ReplaceLatest

Use for replaceable state:

```text
cursor
status
BBO
DOM/depth snapshot
model score
regime state
current level set
latest heatmap snapshot
```

Older frames can often be dropped if newer valid frames exist.

### AppendOrdered

Use for streams where every event matters:

```text
trades
fills
orders
journal events
audit events
some alerts
```

These must preserve order and deduplicate.

### PatchByKey

Use for objects updated by stable key:

```text
candles
markers
zones
price levels
heatmap cells
```

Late patches may be valid if generation and key still match.

### SnapshotOnly

Use for projections primarily consumed as explicit snapshots.

---

## 17. Execution Types and Lag Policies

Lag policy should be driven primarily by execution type.

```rust
pub enum ProjectionExecutionType {
    CoreSync,
    InlineSync,
    CoalescedSync,
    AsyncLatest,
    AsyncOrdered,
    OfflineArtifact,
    ReviewOnly,
}
```

### 17.1 CoreSync

Core projections define replay/session truth and cannot lag.

Examples:

```text
cursor
session status
BBO if required by execution or core visibility
canonical trade prints
orders/fills
execution simulation state
visibility frame production
```

Policy:

```rust
ProjectionLagPolicy::Prohibited
```

Rules:

```text
Must complete before replay cursor advances.
Must be deterministic.
Must run on the replay thread.
If too slow in CLI profile, it is a bug.
If too slow in interactive replay, playback slows rather than lying.
```

Core cannot be latest-wins. Core cannot be stale. Core cannot skip when due.

### 17.2 InlineSync

Cheap derived projections that must remain exactly current whenever their wake policy fires.

Examples:

```text
bars
session_vwap
simple rolling delta
small rolling stats
simple sweep detector
basic absorption score
```

Policy:

```rust
ProjectionLagPolicy::ProhibitedWhenDue
```

Rules:

```text
May skip ticks by wake policy.
When due, must complete synchronously before the replay step is done.
```

### 17.3 CoalescedSync

State remains current, visual frames may be coalesced.

Examples:

```text
DOM/depth visual state
trade tape visual state
heatmap visual state
chart live-bar visual updates
```

Policy:

```rust
ProjectionLagPolicy::StateCurrentFramesCoalesced
```

Rules:

```text
Projection state advances synchronously when due.
Lens frames may be skipped, merged, or replaced.
Consumers receive latest visual state, not every internal transition.
```

### 17.4 AsyncLatest

Expensive online computations where only the newest useful result matters.

Examples:

```text
model inference
regime classifier
large feature pack
level acceptance model
slow composite study
```

Policy:

```rust
ProjectionLagPolicy::LatestWins {
    mark_stale_after_ms,
    drop_after_ms,
    cancel_pending_on_newer_input,
}
```

Rules:

```text
Never blocks replay.
Worker receives immutable input snapshot or compact feature window.
Older pending jobs for the same ProjectionKey may be dropped.
Completed results are accepted only if generation still matches.
Completed results are accepted only if input_cursor is newer than last accepted.
Result frame carries input_cursor, emitted_cursor, lag, and stale flag.
```

### 17.5 AsyncOrdered

Async work where every result matters and must be preserved in order.

Examples:

```text
audit stream enrichment
journal extraction
rare alert streams where every alert must be retained
```

Policy:

```rust
ProjectionLagPolicy::CatchUpOrdered {
    max_queue_len,
    on_overflow,
}
```

Use sparingly. Ordered catch-up can create storms of stale UI events.

### 17.6 OfflineArtifact

Expensive features precomputed through CLI and read during replay.

Examples:

```text
full-day ML features
large heatmaps
gamma surfaces
research labels
slow volume-profile variants
training datasets
```

Policy:

```rust
ProjectionLagPolicy::ArtifactAsOf
```

Rules:

```text
At replay cursor T, reveal only artifact rows where as_of <= T.
If artifact is missing, fail subscription or use declared fallback.
No hot-path computation lag because computation already happened.
```

### 17.7 ReviewOnly

Hindsight labels and review-only analytics.

Examples:

```text
future outcome labels
MFE/MAE labels
perfect reversal markers
post-session classification
```

Policy:

```rust
ProjectionLagPolicy::ReviewOnly
```

Rules:

```text
Allowed in review.
Blocked from training/live decision surfaces unless explicitly enabled in review mode.
Must be visibly labeled.
```

---

## 18. Projection Frame Stamps

Every frame should make timing explicit.

```rust
pub struct ProjectionFrameStamp {
    pub session_id: SessionId,
    pub replay_dataset_id: ReplayDatasetId,
    pub generation: u64,

    pub projection_key: ProjectionKey,

    pub input_cursor_ts_ns: UnixNanos,
    pub emitted_cursor_ts_ns: UnixNanos,
    pub batch_idx: u64,

    pub source_view: SourceView,
    pub execution_type: ProjectionExecutionType,
    pub temporal_policy: TemporalPolicy,

    pub lag_ns: Option<u64>,
    pub stale: bool,
}
```

For sync projections:

```text
input_cursor == emitted_cursor
lag = 0
stale = false
```

For coalesced sync projections:

```text
input_cursor = latest internal state included in the visual frame
emitted_cursor = current replay cursor
state is current; visual frame emission was coalesced
```

For async projections:

```text
input_cursor = market data used by worker
emitted_cursor = replay cursor when result became visible
lag = emitted_cursor - input_cursor
stale = lag > manifest threshold
```

This prevents async model outputs from being painted as if they were immediate.

---

## 19. Async Result Acceptance Gate

Async results should enter the runtime only through a strict gate.

Conceptual logic:

```rust
fn accept_async_result(
    runtime: &mut ProjectionRuntime,
    result: AsyncProjectionResult,
    current_cursor: UnixNanos,
) -> AcceptDecision {
    if result.generation != runtime.generation {
        return AcceptDecision::DropOldGeneration;
    }

    if !runtime.is_subscribed(&result.key) {
        return AcceptDecision::DropUnsubscribed;
    }

    let node = runtime.node(&result.key);

    if result.input_cursor_ts_ns <= node.last_accepted_input_cursor() {
        return AcceptDecision::DropOlderThanAccepted;
    }

    let lag_ns = current_cursor - result.input_cursor_ts_ns;

    if node.lag_policy().drop_after_exceeded(lag_ns) {
        return AcceptDecision::DropTooStale;
    }

    node.accept_result(result, lag_ns);

    AcceptDecision::Accepted {
        stale: node.lag_policy().mark_stale(lag_ns),
    }
}
```

This gate protects replay correctness, seek correctness, and UI honesty.

---

## 20. Temporal Policy / Causality

Causality is necessary. It should be implemented lightly at first, but it should be part of the contract from the beginning.

```rust
pub enum TemporalPolicy {
    Causal,
    AsOfExternal,
    HindsightReviewOnly,
}
```

### Causal

Uses only data available at or before the input cursor.

Safe for replay training and live mode.

### AsOfExternal

Uses external artifacts, but only records where `as_of_ts <= cursor_ts`.

Examples:

```text
gamma snapshot
option-derived levels
external model artifact
prior-day profile artifact
```

### HindsightReviewOnly

Uses future information or full-day outcomes.

Examples:

```text
MFE/MAE label
perfect failed-auction label generated after the session
future reversal marker
post-session trade classification
```

Allowed in review; blocked from training/live decision surfaces unless explicitly enabled.

### Why this matters

Without temporal policy, Ledger can accidentally reproduce common platform failure modes:

```text
repainting values
signals displayed before they were knowable
external levels shown before their snapshot existed
async model results painted as immediate
review labels leaking into training
```

Temporal metadata makes the training environment honest.

---

## 21. ProjectionFrame Envelope

Use one generic frame envelope rather than study-specific message types.

Conceptual JSON shape:

```json
{
  "type": "projection.frame",
  "session_id": "replay-ESH6-2026-03-12",
  "subscription_id": "main-chart-bars",
  "seq": 1285,
  "generation": 3,

  "cursor": {
    "input_ts_ns": "1773325800000000000",
    "emitted_ts_ns": "1773325800000000000",
    "batch_idx": 49122
  },

  "projection": {
    "id": "bars",
    "version": 1,
    "params_hash": "sha256:..."
  },

  "source_view": "trader_visibility",
  "execution_type": "inline_sync",
  "temporal_policy": "causal",
  "delivery_semantics": "patch_by_key",
  "lag_ns": 0,
  "stale": false,

  "op": "patch",
  "payload_schema": "candles_v1",
  "payload": {
    "bar": {
      "start_ts_ns": "1773325800000000000",
      "open": 5324.25,
      "high": 5325.00,
      "low": 5323.75,
      "close": 5324.75,
      "volume": 184
    }
  }
}
```

Recommended operations:

```text
snapshot
append
patch
replace
clear
error
diagnostic
```

The WebSocket should expose this same frame shape after CLI has validated it.

---

## 22. Runtime State

Conceptual runtime storage:

```rust
pub struct ProjectionRuntime {
    registry: ProjectionRegistry,

    nodes: IndexMap<ProjectionKey, Box<dyn ProjectionNode>>,
    deps: HashMap<ProjectionKey, Vec<ProjectionKey>>,
    reverse_deps: HashMap<ProjectionKey, Vec<ProjectionKey>>,
    active_topological_order: Vec<ProjectionKey>,

    subscribers: HashMap<ProjectionKey, SubscriberSet>,
    last_payloads: HashMap<ProjectionKey, ProjectionPayload>,

    async_scheduler: AsyncProjectionScheduler,
    artifact_reader: ProjectionArtifactReader,

    generation: u64,
    metrics: ProjectionMetrics,
}
```

### Subscribe flow

```text
1. Validate ProjectionSpec.
2. Canonicalize params.
3. Build ProjectionKey.
4. Resolve dependency tree using registry/factories.
5. Detect cycles.
6. Instantiate missing nodes.
7. Reuse existing nodes.
8. Increment subscriber/reference count.
9. Send initial snapshot frame.
```

### Tick flow

```text
1. Apply completed async results through acceptance gate.
2. Iterate active nodes in topological order.
3. Skip nodes whose wake policy is not due and whose dependencies did not change.
4. Advance due inline nodes.
5. Enqueue async jobs for due async nodes.
6. Advance artifact-backed nodes from as-of artifact reader.
7. Mark changed nodes dirty.
8. Drain frames according to frame policies.
9. Record per-node metrics.
```

Conceptual pseudo-code:

```rust
pub fn on_session_tick(&mut self, tick: &SessionTick) -> Result<Vec<ProjectionFrame>> {
    self.apply_completed_async_results(tick)?;

    let mut changed = DirtySet::new();

    for key in self.active_topological_order.iter() {
        let node = self.nodes.get_mut(key).unwrap();

        let dep_changed = self.deps[key]
            .iter()
            .any(|dep| changed.contains(dep));

        let timer_due = node.timer_due(tick.cursor_ts_ns);
        let event_due = node.wake_policy().matches(tick.flags);

        if !dep_changed && !timer_due && !event_due {
            self.metrics.record_skip(key);
            continue;
        }

        match node.execution_type() {
            ProjectionExecutionType::CoreSync
            | ProjectionExecutionType::InlineSync
            | ProjectionExecutionType::CoalescedSync => {
                let advance = node.advance(&self.context_for(key, tick))?;
                if advance.changed() {
                    changed.insert(key.clone());
                }
                if let ProjectionAdvance::NeedsAsyncWork(job) = advance {
                    self.async_scheduler.enqueue_latest(job);
                }
            }

            ProjectionExecutionType::AsyncLatest => {
                if node.should_enqueue_async(tick) {
                    let job = node.build_async_job(&self.context_for(key, tick))?;
                    self.async_scheduler.enqueue_latest(job);
                }
            }

            ProjectionExecutionType::AsyncOrdered => {
                if node.should_enqueue_async(tick) {
                    let job = node.build_async_job(&self.context_for(key, tick))?;
                    self.async_scheduler.enqueue_ordered(job);
                }
            }

            ProjectionExecutionType::OfflineArtifact => {
                let advance = node.advance_from_artifact(&self.context_for(key, tick))?;
                if advance.changed() {
                    changed.insert(key.clone());
                }
            }

            ProjectionExecutionType::ReviewOnly => {
                if self.review_mode_enabled {
                    let advance = node.advance(&self.context_for(key, tick))?;
                    if advance.changed() {
                        changed.insert(key.clone());
                    }
                }
            }
        }
    }

    self.drain_frames(tick, &changed)
}
```

---

## 23. Performance Philosophy

Performance matters architecturally now, but V1 should not become a complex parallel DAG executor.

The correct foundation:

```text
single deterministic replay core
lazy graph by subscription
shared base projections
wake policies
frame policies
execution lanes
async latest-value lane for heavy work
offline artifact lane for serious ML/research features
per-node profiling through CLI
```

### Avoid these mistakes

```text
Each study scans raw MBO independently.
Each chart computes its own bars.
DOM sends one WebSocket frame per event batch during fast replay.
Heavy ML runs inline by accident.
Everything is serde_json inside the hot path.
Depth snapshots are copied everywhere.
Optional studies backpressure replay truth.
There is no per-node timing or lag report.
```

### Prefer these patterns

```text
One canonical book process per active Session.
One shared ProjectionRuntime per active Session.
One node instance per unique ProjectionKey.
Typed Rust payloads inside Ledger.
JSON only at CLI/API boundaries.
Shared batch_features spine for L3 studies.
Coalesced visual frames.
AsyncLatest for expensive online work.
OfflineArtifact for full-day/large research features.
CLI profile gates before API/Lens exposure.
```

---

## 24. Threading Model

V1 replay core should remain single-owner and deterministic.

Single-threaded / single-owner hot path:

```text
ReplaySimulator
canonical L3 book mutation
execution simulation
visibility frame production
critical inline projections
dependency graph ordering
```

Worker threads may be used for:

```text
AsyncLatest model inference
expensive feature packs
slow composite studies
artifact loading / range reads
possibly compression/serialization outside the hot path
```

Worker threads must never mutate or directly read the canonical book.

They receive one of:

```text
immutable snapshot
compact feature window
typed dependency payload copy
Arc-backed immutable buffer
artifact range
```

They return:

```rust
pub struct AsyncProjectionResult {
    pub key: ProjectionKey,
    pub generation: u64,
    pub input_cursor_ts_ns: UnixNanos,
    pub payload: ProjectionPayload,
    pub diagnostics: Option<AsyncDiagnostics>,
}
```

The runtime accepts or drops the result by policy.

Do not start with parallel graph execution. A fully parallel DAG executor can come later if profiling proves it is needed.

---

## 25. Batch Features: The Microstructure Spine

Most L3 studies should not inspect raw events independently. They should consume a shared base projection:

```text
batch_features:v1
```

Conceptual payload:

```rust
pub struct BatchFeaturesPayload {
    pub cursor_ts_ns: UnixNanos,
    pub batch_idx: u64,

    pub bbo_before: Option<Bbo>,
    pub bbo_after: Option<Bbo>,
    pub spread_before_ticks: Option<i64>,
    pub spread_after_ticks: Option<i64>,

    pub trade_count: usize,
    pub canonical_buy_volume: u64,
    pub canonical_sell_volume: u64,
    pub largest_trade_size: u64,

    pub sweep_side: Option<Side>,
    pub sweep_levels_crossed: u32,

    pub bid_add_size_by_level: Vec<u64>,
    pub bid_cancel_size_by_level: Vec<u64>,
    pub ask_add_size_by_level: Vec<u64>,
    pub ask_cancel_size_by_level: Vec<u64>,

    pub depth_delta_by_level: Vec<DepthDeltaLevel>,
    pub queue_rebuild_flags: Vec<QueueRebuildFlag>,
    pub liquidity_pull_flags: Vec<LiquidityPullFlag>,

    pub price_displacement_ticks: i64,
    pub no_bbo: bool,
    pub locked_or_crossed: bool,

    pub touched_levels: Vec<PriceLevelTouch>,
}
```

Then studies become simpler:

```text
sweep_detector
  depends on batch_features

absorption_score
  depends on batch_features + bars

liquidity_pull_stack
  depends on batch_features + depth

level_reaction_score
  depends on batch_features + level_sets + bars

gamma_l3_reversal_context
  depends on level_reaction_score + gamma levels + regime_classifier
```

This is one of the most important scalability decisions.

---

## 26. Offline Artifacts

Some projections should not run online during interactive replay.

Examples:

```text
full-day feature matrices
heavy model features
gamma surfaces
large heatmap grids
research labels
MFE/MAE outcomes
slow volume-profile variants
large context-window calculations
```

These should be generated by CLI into artifact storage, then read by replay as time-indexed projection artifacts.

Artifact reveal rule:

```text
At replay cursor T, expose only rows where as_of_ts <= T.
```

This lets heavy research integrate with replay without corrupting causality or blocking the replay core.

---

## 27. Cache Keys and Lineage

Cache by lineage, not only by projection name.

Recommended cache key:

```text
ReplayDatasetId
+ ProjectionId
+ ProjectionVersion
+ CanonicalParamsHash
+ DependencyFingerprints
+ InputArtifactHashes
+ SourceView
+ TemporalPolicy
+ CanonicalTradePrintPolicyVersion
+ LevelSetVersion
+ CodeHash or ManifestHash
```

This prevents silent cache corruption when a base projection, trade-print policy, level mapping, or dependency semantics changes.

V1 can start simple:

```text
ReplayDatasetId + ProjectionId + Version + ParamsHash
```

But the design should leave room for full lineage.

---

## 28. Seeking, Generations, and Future Checkpointing

V1 does not need full checkpointing.

V1 seek behavior can be:

```text
1. Increment session generation.
2. Drop active ProjectionRuntime state or reset nodes.
3. Reset ReplaySimulator.
4. Replay from start or nearest existing replay seek point to target.
5. Recreate active subscriptions.
6. Emit fresh snapshots for the new generation.
```

Every frame must carry `generation` so Lens discards stale frames after seek.

Future checkpointing should add:

```rust
pub trait CheckpointableProjectionNode {
    fn checkpoint(&self) -> ProjectionCheckpoint;
    fn restore(&mut self, checkpoint: ProjectionCheckpoint) -> Result<()>;
}
```

Future replay checkpoints should include:

```text
book state
simulator state
cursor
orders/fills/execution state
visibility model state
projection node states
output buffers
dependency fingerprints
```

Checkpointing is an optimization pass after the graph is implemented and profiled.

---

## 29. Levels, 0DTE, and Gamma Integration

Levels should be first-class projection inputs, not chart annotations bolted on later.

A level should have:

```text
price
source
kind
strength
valid_from
valid_to
underlying mapping
metadata
version
as_of_ts
```

Level projection examples:

```text
level_sets:manual:v1
level_sets:prior_day:v1
level_sets:vwap:v1
level_sets:volume_profile:v1
level_sets:gamma:v1
level_sets:model:v1
```

For gamma/0DTE:

```text
At replay cursor T:
  choose latest valid option/gamma snapshot where snapshot_ts <= T
  map option-derived levels to ES using mapping version M
  emit price_level_set_v1 / heatmap_cells_v1
  expose levels to studies
```

Composite studies can then ask:

```text
Is ES approaching a major 0DTE call wall?
Is L3 showing acceptance or rejection at that level?
Is this positive-gamma mean reversion or negative-gamma continuation behavior?
Did aggressive flow fail at a gamma/futures confluence level?
```

This is a later phase, but the projection contract should support it from day one.

---

## 30. Journal Integration

Journal entries should preserve the projection environment, not only screenshots and notes.

Conceptual shape:

```rust
pub struct JournalEntry {
    pub market_day: MarketDay,
    pub replay_dataset_id: ReplayDatasetId,
    pub session_id: SessionId,

    pub entry_cursor_ts_ns: UnixNanos,
    pub exit_cursor_ts_ns: Option<UnixNanos>,

    pub visible_subscriptions: Vec<ProjectionSubscriptionSpec>,
    pub graph_fingerprint: ProjectionGraphFingerprint,
    pub level_set_fingerprints: Vec<LevelSetFingerprint>,

    pub orders: Vec<OrderSnapshot>,
    pub fills: Vec<FillSnapshot>,

    pub selected_projection_values_at_entry: Vec<ProjectionValueSnapshot>,
    pub selected_projection_values_at_exit: Vec<ProjectionValueSnapshot>,

    pub notes: String,
    pub tags: Vec<String>,
    pub screenshots: Vec<ScreenshotRef>,
    pub review_comments: Vec<ReviewComment>,
}
```

Later queries become possible:

```text
Show all long attempts near 0DTE call walls where absorption_score:v2 was high.

Compare entries where continuation_quality_score:v1 was weak but I still took breakout trades.

Find trades where a model-study signal was stale or unavailable at entry.

Review losses where liquidity_pull_stack:v1 warned before entry.
```

The journal should reference projection identities and versions so review remains reproducible.

---

## 31. CLI-First Agentic Validation

The implementation workflow should be:

```text
Codex implements projection contracts and nodes.
CLI validates manifest, dependency graph, deterministic output, schema, timing, and performance.
Only after CLI passes do API/WebSocket and Lens consume the projection.
```

Projection CLI commands should eventually include:

```bash
ledger projection list
ledger projection manifest --id bars --version 1
ledger projection graph --projection absorption_score:v1 --params params.json
ledger projection run --symbol ES --date 2026-03-12 --projection bars:v1 --params params.json --batches 10000 --jsonl out.frames.jsonl --digest
ledger projection profile --symbol ES --date 2026-03-12 --projection absorption_score:v1 --params params.json --batches 100000
ledger projection validate --symbol ES --date 2026-03-12 --projection absorption_score:v1 --params params.json --batches 100000
```

Validation should check:

```text
manifest exists
params validate
dependencies resolve
no dependency cycles
output schema valid
snapshot can be produced
deterministic digest is stable
no future leakage for causal projections
source view is respected
wake policy behaves as expected
frame policy behaves as expected
core sync projections never lag
async projections report lag/staleness correctly
profile budgets pass
no unbounded memory growth
frames carry generation and cursor stamps
```

This is the safety loop that enables rapid experimentation without losing control.

---

## 32. API and Lens Should Be Boring After CLI Validation

The WebSocket/API layer should not introduce a second computation path.

It should translate:

```json
{
  "type": "projection.subscribe",
  "projection": {
    "id": "bars",
    "version": 1,
    "params": {
      "kind": "time",
      "seconds": 60,
      "source_view": "visible"
    }
  }
}
```

into:

```rust
session.subscribe_projection(spec)
```

And stream back the same `ProjectionFrame` shape validated by CLI.

Lens should render by `payload_schema`:

```text
candles_v1 → chart candle renderer
depth_v1 → DOM/depth renderer
bbo_v1 → inside-market display
trade_events_v1 → tape renderer
time_series_v1 → line/score overlay
price_level_set_v1 → horizontal level renderer
markers_v1 → marker overlay
heatmap_cells_v1 → heatmap renderer
alerts_v1 → alert panel
journal_prompts_v1 → journal/training prompt panel
```

Lens should not compute market facts.

---

## 33. Lessons From Existing Systems

Several existing systems rhyme with this architecture, but none should be copied directly.

### kdb+ / kdb+tick

Pattern: central tickerplant, real-time subscribers, historical database, derived analytics.

Lesson:

```text
Central feed/truth plus subscribed derived analytics is proven.
Ledger should add manifests, versioning, source-view semantics, replay/live convergence,
and trader-focused journal context.
```

Reference: <https://code.kx.com/q/learn/startingkdb/tick/>

### OneTick

Pattern: tick analytics, historical simulation, real-time analytics, directed graph event processors.

Lesson:

```text
A directed graph of market-data processors is a serious tick analytics pattern.
Ledger should specialize it for ES L3 replay, discretionary training, journaling,
and AI-assisted study generation.
```

Reference: <https://www.onetick.com/onetick-tick-analytics-platform>

### LEAN / Zipline

Pattern: event-driven backtesting/live engines.

Lesson:

```text
Event-driven strategy engines are proven, but Ledger is not strategy-centric.
Ledger's graph is market-state/replay-training-centric.
```

References:

```text
https://www.quantconnect.com/docs/v2/lean-engine/getting-started
https://zipline.ml4trading.io/
```

### NinjaTrader

Pattern: exposed calculation cadence such as per bar close, per tick, or per price change.

Lesson:

```text
Cadence is a first-class concern.
Ledger should not use one global calculate mode. Every projection declares wake policy,
execution type, frame policy, delivery semantics, and lag policy.
```

Reference: <https://ninjatrader.com/support/helpguides/nt8/calculate.htm>

### TradingView / Pine

Pattern: repainting issues when historical and realtime behavior differ.

Lesson:

```text
Timing semantics must be visible. Ledger must prevent unconfirmed or hindsight values
from silently appearing as decision-available replay signals.
```

Reference: <https://www.tradingview.com/pine-script-docs/concepts/repainting/>

### Apache Flink

Pattern: streaming systems handle late data, allowed lateness, side outputs, and backpressure explicitly.

Lesson:

```text
Lag and backpressure need policies, not vibes. Optional analytics should not poison the core replay path.
```

Reference: <https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/streaming_analytics/>

### Event Sourcing

Pattern: state can be rebuilt by replaying events; snapshots/checkpoints are later optimization.

Lesson:

```text
Projection replay is powerful, but seek performance eventually needs checkpointing.
V1 should leave seams for it without implementing it too early.
```

Reference: <https://martinfowler.com/eaaDev/EventSourcing.html>

---

## 34. Non-Goals for V1

V1 should not attempt:

```text
full projection checkpointing
parallel DAG execution
GPU inference
large distributed cache
multi-session orchestration
full gamma/0DTE pipeline
model training platform
Lens support for every output schema
general plugin system
```

V1 should implement the architecture seams clearly enough that these can be added later.

---

## 35. Hard Invariants

These rules should be protected by code, tests, CLI validation, and review discipline.

```text
One active Session has one canonical truth book.

One ProjectionRuntime exists per active Session.

One ProjectionKey creates one shared node instance.

Projection nodes do not mutate the canonical book.

Projection nodes do not directly import/call other concrete projection implementations.

Dependencies are declared through manifests/factories.

Every projection output has a schema.

Every projection frame carries generation, cursor stamps, source view, temporal policy,
execution type, delivery semantics, lag/stale fields, and projection identity.

Lens renders output schemas; it does not compute trading facts.

CoreSync projections cannot lag.

InlineSync projections cannot lag when due.

CoalescedSync projections may coalesce frames, not truth state.

AsyncLatest projections never block replay; newest valid result wins.

AppendOrdered payloads preserve order; newest-wins is not applied blindly.

OfflineArtifact projections reveal data only as-of cursor.

ReviewOnly projections are blocked from training/live decision surfaces unless explicitly enabled.

Every study has a manifest, dependency declaration, output schema, trust tier, and validation path.

Cache keys eventually include dependency lineage, not only study name.

CLI validation/profiling comes before API/Lens exposure.
```

---

## 36. The North Star

The study graph is not a feature. It is Ledger's core leverage point.

The target is:

```text
A deterministic, lazy, typed, timing-aware projection graph over Session truth,
validated headlessly through CLI, exposed through a generic projection frame protocol,
and rendered by Lens without duplicating market logic.
```

That gives Ledger the flexibility normal trading platforms skim over:

```text
custom L3 microstructure studies
shared base features
AI-generated experiments
0DTE/gamma level context
async model outputs with honest lag semantics
journal memory tied to exact projection versions
replay/live convergence below the UI
future checkpoint and cache optimization
```

This document should be treated as the architectural source of truth for the study graph until the implemented system teaches us where it needs to evolve.
