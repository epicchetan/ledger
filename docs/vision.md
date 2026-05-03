# Ledger & Lens

## 1. North Star

Ledger and Lens are an internal ES-focused trading research and training system. The goal is not to build a generic trading product. The goal is to own the full stack required to practice discretionary ES trading with real L3 replay, validate market data quality, rapidly build custom studies, journal decisions, and eventually run live through the same downstream interface.

The long-term loop is:

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

Ledger is the source of truth. Lens is the operating surface. Ledger owns ingestion, storage, normalized data, L3 order-book truth, feed-driven sessions, studies, levels, validation, and eventually live adapters. Lens manages data, shows trust/validation status, renders charts/DOM/studies/levels, controls replay, and captures journal/review workflows.

The key design principle remains: **replay and live should converge below the UI**. Lens should consume normalized state, projections, levels, orders/fills, and journal data. It should not care whether the source was historical replay or live market data.

## 2. Core Naming and Semantics

Use these terms consistently.

```text
MarketDay
  One downloaded/cataloged ES trading day.
  For ES: prior day 18:00 ET → market date 17:00 ET.

ReplayDataset
  Immutable replay inputs for a MarketDay.
  Durable Layer 2 artifacts: events, batches, trades, book-check, and future cached projections.

ReplayFeed
  Replay-backed market-data feed over a ReplayDataset.
  Owns deterministic replay delivery mechanics and converts historical exchange truth into feed batches.

Session
  Active mutable training/runtime session.
  Owns feed state, cursor, play/pause/speed, visibility profile, execution profile, active orders, and projection graph.

ContextWindow
  Prior MarketDays and cached artifacts used for historical candles, prior levels, volume profile, and context.

Study
  A typed projection over ReplayDataset, Session state, level sets, other studies, or journal/context data.
```

This naming is cleaner: a dataset is immutable input; a feed delivers data; a session is something actively experienced and controlled.

## 3. Current Product Priority: Lens Data Center

The first serious Lens overhaul is the Data Center surface, not a replay chart polish pass. The old prototype viewer can remain temporarily, but Lens should first prove it can manage Ledger’s data lifecycle.

The Data Center should answer:

```text
What days do I have?
What is missing?
What is downloaded but not validated?
What durable raw and replay layers exist?
Is this day trustworthy enough to train on?
What warnings should I care about?
What job is currently changing this day?
What happened during the last prepare/rebuild/validate/delete job?
```

Data Center actions:

```text
select symbol/date
resolve full ES MarketDay
prepare raw data + ReplayDataset
rebuild ReplayDataset from existing raw data
run validation / data-quality checks again
delete ReplayDataset
delete raw data
track job progress and history
view trust report
open validated day as a Session later
```

Loading or hydrating a `ReplayDataset` is not a Data Center user action. It is an internal implementation detail used by validation today and by active replay-backed `Session` startup later.

The first Ledger API is therefore a small Data Center API:

```text
GET  /health
GET  /market-days
GET  /market-days/:symbol/:date
GET  /market-days/:symbol/:date/jobs
POST /market-days/:symbol/:date/prepare
POST /market-days/:symbol/:date/replay/build
POST /market-days/:symbol/:date/replay/validate
DELETE /market-days/:symbol/:date/replay
DELETE /market-days/:symbol/:date/raw
GET  /jobs
GET  /jobs?active=true
GET  /jobs?active=false&limit=50
GET  /jobs/:id
```

Replay controls and WebSockets come after Lens can download, validate, and trust data.

The current storage source-of-truth decision is:

```text
SQLite = local Ledger control plane
R2     = durable large-object data plane
tmp    = disposable job staging
cache  = disposable replay performance layer
```

SQLite owns the catalog, object metadata, jobs, validation summaries, and future journal/session/study state. R2 stores raw DBN files and replay artifacts. `data/tmp` is temporary staging for ingest and validation jobs, not a product-level replay cache.
`data/cache` is a read-through cache for active replay and can be rebuilt from
R2.

## 4. Data Quality and Validation Philosophy

Current ingestion and validation should be treated as **artifact integrity and deterministic replay-readiness checks**. They prove that Ledger can reconstruct and replay the data through its own deterministic pipeline. They do not yet prove that a day is free of subtle feed gaps, market-quality problems, or modeling assumptions.

Lens should present validation as a trader-facing trust report, not raw JSON.

`prepare` and `rebuild` should leave behind a persisted validation result before completing. The separate `validate` action is for a manual re-audit when validation logic changes, corruption is suspected, or a deeper check is wanted.

Recommended top-level statuses:

```text
Missing
Raw Available
ReplayDataset Available
Ready to Train
Ready with Warnings
Invalid
```

Validation should eventually cover:

```text
Coverage
  requested start/end, first/last event, gap analysis, RTH/ETH coverage

Artifact Integrity
  raw object, events, batches, trades, book-check, hashes, schema versions

Index Integrity
  rebuilt batch index match, rebuilt trade index match

Book Health
  warning count and warning types, checksum, crossed/locked states, no-BBO periods

Market Sanity
  spread distribution, abnormal price moves, empty-book intervals, action counts

Replay Readiness
  replay probe, cursor movement, visibility frames, simulator smoke test
```

Important quant distinction: not every raw `Trade`/`Fill` event should automatically become canonical chart volume. Ledger should define a canonical trade-print policy for bars, tick charts, volume profile, delta, and absorption metrics. Raw trade/fill records can still be preserved for microstructure diagnostics.

The final UI should make the result obvious:

```text
This day is safe for training.
This day is usable but has warnings.
This day should not be trusted.
```

## 5. Feed-Driven Session Model

A Session is an active training/runtime controller over a market-data feed.
Replay is one feed implementation over a ReplayDataset; live should become
another feed implementation later.

Replay has three separate layers:

```text
Exchange Truth
  Exact historical event batches applied to one canonical L3 order book.

Trader Visibility
  Delayed/coalesced frames representing what the trader could actually see.

Execution Simulation
  Orders arrive after latency and fill against true book state at arrival time.
```

This separation is non-negotiable. It allows accurate training without pretending the trader has impossible priority or perfect instantaneous information.

Initial Session responsibilities:

```text
load one ReplayDataset through a ReplayFeed
seek to RTH open or selected timestamp
advance feed batches
play/pause/speed
hold visibility and execution profiles
accept simulated orders later
own the active StudyGraph
emit projection frames to Lens
```

V1 can support one active Session at a time. Lens may have many panels/charts subscribed to that one session.

## 6. Studies as a First-Class Projection Graph

Studies are core to the vision. They are not just indicators. They are typed, versioned projections that can power charts, overlays, alerts, heatmaps, model outputs, drills, and journal review.

The detailed design lives in:

```text
docs/study_graph_vision.md
docs/study_graph_phased_implementation.md
```

The architecture should be a directed study graph:

```text
ReplayDataset / Session / LevelSets / Journal Context
  ↓
Base projections
  ↓
Derived studies
  ↓
Composite studies / model studies / visual outputs
```

Examples of base projections:

```text
bars:1m
bars:200t
dom:50
bbo
trade_stream
depth_delta
batch_features
session_vwap
level_sets:futures
level_sets:gamma
```

Examples of derived studies:

```text
absorption_score
sweep_detector
liquidity_pull_stack
book_pressure
failed_auction_detector
regime_classifier
level_reaction_score
```

Examples of composite/model studies:

```text
gamma_l3_reversal_context
continuation_quality_score
shock_regime_filter
lightweight_model:level_acceptance:v1
```

A study should declare:

```text
study id
version
parameters
input dependencies
output schema
online/offline mode
cache policy
trust tier
validation tests
visualization hints
```

This lets AI/Codex generate new studies without breaking the system. A new study becomes a documented module with declared inputs, outputs, tests, and versioning.

## 7. Efficient Study Computation

Studies should build on each other and share work. Ledger should avoid recomputing expensive L3 facts for every chart or indicator.

The efficient path is:

```text
canonical event replay
→ single L3 order book
→ shared base projections / batch features
→ derived studies
→ Lens frames
```

Principles:

```text
one canonical book process per active Session
shared StudyGraph for all Lens panels
lazy computation only for subscribed/needed studies
incremental online updates during replay
offline artifact generation for heavy studies
cache by ReplayDataset + study id + params + version
coalesce UI frames separately from exact computation
```

If three charts request `bars:1m`, Ledger should compute it once. If an absorption study depends on batch features, it should consume a shared batch-feature projection, not rescan raw MBO independently. If a model study depends on gamma levels and L3 acceptance features, it should consume those projections by declared dependency.

This architecture supports aggressive AI-assisted iteration because new studies plug into a graph instead of becoming one-off code paths.

## 8. Study Outputs and Lens Visualization

Studies should not be limited to line indicators. Outputs can include:

```text
time series
price series
candles/tick bars
DOM/depth snapshots
markers
zones
heatmaps
horizontal levels
scores/probabilities
alerts
annotations
journal prompts
replay drill triggers
```

Lens should render studies through a flexible projection protocol. A study can power a chart overlay, a separate panel, a DOM visual effect, a level heatmap, or a training prompt.

Example subscription concept:

```json
{
  "type": "subscribe",
  "study": "bars",
  "params": { "kind": "tick", "trades": 200 }
}
```

Another:

```json
{
  "type": "subscribe",
  "study": "gamma_l3_reaction",
  "params": { "levelSource": "0dte", "windowSec": 30 }
}
```

The exact protocol can evolve later. The vision is that Lens subscribes to projections, not hardcoded indicator routes.

## 9. Levels, 0DTE, and Gamma as First-Class Inputs

0DTE options are central to the future vision. They should not be bolted onto charts as a separate hack. They should enter Ledger through the same level/projection system.

Ledger should treat price levels as time-aware objects:

```text
futures levels
manual levels
prior day / overnight levels
VWAP / volume profile levels
0DTE option-derived levels
gamma walls / call walls / put walls / gamma flip
model-derived levels
```

Each level should have:

```text
price
source
kind
strength
valid_from / valid_to
underlying mapping
metadata
version
```

For 0DTE/gamma, the hard problem is not only computing levels. It is syncing them with replay/live time and mapping them cleanly onto ES futures charts. The system should support a time-indexed level heatmap:

```text
at replay time T
→ latest valid option/gamma snapshot <= T
→ mapped ES price levels
→ rendered on charts and available to studies
```

Then studies can ask:

```text
Is ES approaching a major 0DTE call wall?
Is L3 showing acceptance or rejection at that level?
Is this positive-gamma mean reversion or negative-gamma continuation behavior?
Did aggressive flow fail at a gamma/futures confluence level?
```

This is where the system can push beyond normal retail tooling: 0DTE context plus exact L3 replay plus custom studies plus journaling.

## 10. Journaling and Training Memory

The journal should capture decisions with enough context to make review and AI querying useful.

A journal entry should reference:

```text
MarketDay
ReplayDataset
Session
cursor timestamps
orders/fills
chart layout
visible studies and versions
level sets and versions
visibility/execution profile
notes/tags/screenshots
outcome stats
review comments
```

The point is not just PnL tracking. The point is to preserve the decision environment. Later we should be able to ask:

```text
Show all long attempts near 0DTE call walls where absorption was high.
Show continuation trades where liquidity pulled before breakout.
Compare my entries when the shock-regime filter was active vs inactive.
Find model-study signals I ignored that would have helped.
```

## 11. AI-Assisted Development Vision

This stack is designed for agentic development. AI should be able to add studies, tests, visualizations, and research reports because the architecture has explicit contracts.

Stable insertion points:

```text
Study manifest
Study input dependencies
Study output schema
LevelSet schema
ReplayDataset artifacts
Session projection protocol
Journal schema
Validation report schema
```

AI-generated studies should be allowed, but they should come with:

```text
versioned code
declared dependencies
unit/integration tests where possible
sample output schema
trust tier
short documentation
```

This gives us speed without losing control.

## 12. Current Implementation Map

The repo is currently split along these boundaries:

```text
ledger-domain
  Shared pure types, MarketDay resolution, EventStore, codecs, storage kind names,
  and simulator request/profile/result types.

ledger-book
  Deterministic L3 order-book truth over normalized MBO events.

ledger-replay
  Headless replay simulator, trader visibility, simulated execution, latency,
  queue-ahead, and same-timestamp policy.

ledger-store
  SQLite control plane, R2 object storage, market-day catalog, raw/replay layer
  records, validation summaries, job records, and tmp staging.

ledger-ingest
  Databento/raw DBN download or reuse, DBN preprocessing, replay artifact
  creation, book-check generation, and artifact persistence through store.

ledger
  Application orchestration: prepare, rebuild, validate, delete, replay probes,
  and shared CLI/API behavior.

ledger-api
  HTTP transport adapter for Lens: routes, DTOs, jobs, progress events, CORS,
  and response presentation.

ledger-cli
  Internal terminal adapter for resolve, ingest, status, list, validation, and
  tmp cleanup.

lens
  Web operating surface. Data Center currently manages market days, durable
  layers, jobs, row actions, trust status, and job history.
```

## 13. Near-Term Build Order

Completed foundation:

```text
1. Adopt naming: ReplayDataset = immutable inputs, Session = active runtime.
2. Add / update docs with this vision.
3. Build Lens Data Center surface.
4. Add minimal Ledger Data API for market days, jobs, prepare, rebuild, validation, and delete.
5. Move validation composition into Ledger so CLI/API share logic.
6. Split SQLite control plane from R2 durable blob storage.
7. Persist jobs, job progress, job history, and validation reports for Lens.
8. Wire Lens Data Center to the real API with table actions and trust state.
9. Add stronger data-quality report fields and Lens trust summaries.
10. Introduce active Session controller over ReplayDataset and ReplaySimulator.
11. Add headless CLI session run flow for agentic validation.
12. Add local read-through ReplayDataset cache for active replay startup.
13. Add study graph vision and phased implementation docs.
```

Next sequence:

```text
1. Add shared projection contracts in ledger-domain.
2. Add ProjectionRuntime registry and skeleton in Ledger.
3. Feed SessionTick from Session into the projection runtime.
4. Add core base projections and CLI projection run/list/manifest/graph commands.
5. Add visual base projections, wake policies, frame policies, and coalescing.
6. Add projection profiling and validation harnesses.
7. Add batch_features and first derived L3 studies.
8. Add API/WebSocket projection subscriptions only after CLI validation passes.
9. Connect Lens replay controls and schema-based projection renderers.
10. Add journaling/training memory, levels/gamma, model studies, checkpointing, and live mode in later phases.
```

The next major product boundary is now the projection graph around active `Session`. Session proves the mutable simulation core: open a validated `ReplayDataset` through a replay feed, seek, advance, and report deterministic state. The next step is defining the typed projection contracts and runtime that decide what Ledger computes and emits from that session: base projections first, derived studies after the graph is proven, then WebSocket frames for Lens.

The purpose of this sequence is to avoid painting ourselves into a corner. The Data Center proves data ownership. Session proves active feed-driven simulation. The projection graph defines the contract between session feed truth and UI rendering. StudyGraph is the extensibility layer inside that projection graph, so levels/gamma, model studies, and journaling become natural extensions instead of rewrites.

## 14. Source-of-Truth Decision

This document establishes the base direction:

```text
Ledger owns validated data, sessions, study graphs, levels, and journal truth.
Lens manages data first, then replay, then charts/studies/journal workflows.
Studies are a first-class typed projection graph, not a loose indicator list.
0DTE/gamma levels become time-aware level sets and study inputs.
Accuracy is protected through dependency declaration, as-of semantics, versioning, trust tiers, and validation — not by limiting experimentation.
```

This leaves room to push into custom L3 indicators, visual effects, model-powered studies, 0DTE heatmaps, and live/replay convergence while keeping the system understandable and testable.
