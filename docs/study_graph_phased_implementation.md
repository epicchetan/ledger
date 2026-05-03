# Ledger Study Graph Phased Implementation Plan

**Intended repo path:** `docs/study_graph_phased_implementation.md`  
**Companion vision document:** `docs/study_graph_vision.md`  
**Status:** Planning / phased roadmap. This document does not implement the study graph. It defines the order of implementation, acceptance gates, CLI validation strategy, and how Codex/agents should work through the system safely.

---

## 1. Purpose of This Plan

The study graph is large enough that it should not be implemented as one open-ended coding pass. It should be built in phases where each phase has:

```text
clear scope
clear crate boundaries
clear deliverables
clear non-goals
unit/integration tests
CLI validation gates where applicable
agentic pass/fail criteria
explicit handoff to the next phase
```

The implementation philosophy is:

```text
contracts first
runtime second
headless CLI validation third
API/WebSocket fourth
Lens rendering fifth
optimization later
```

This is important because the study graph will become the foundation for:

```text
base replay projections
custom L3 studies
levels and gamma context
model studies
alerts
journal prompts
training review
live/replay convergence
future checkpoint/cache optimization
```

The goal is not to rush visuals. The goal is to build a durable computation layer that can support rapid experimentation without corrupting replay truth.

---

## 2. Implementation Principles

Every phase should respect these principles.

### 2.1 CLI before API/Lens

A projection should be validated through CLI before API/WebSocket or Lens depends on it.

The normal path should be:

```text
Codex implements projection contract/node/tests
→ cargo test passes
→ ledger projection list/manifest/graph passes
→ ledger projection run passes
→ ledger projection profile passes
→ ledger projection validate passes
→ API/WebSocket can expose it
→ Lens can render it
```

### 2.2 No second computation path

API and Lens must not compute projections independently. They consume the same runtime and frame shapes validated by CLI.

### 2.3 Runtime state and frame emission are separate

A node can advance internal state at one cadence and emit frames at another. This distinction must exist early, before DOM/depth/model studies make it painful to retrofit.

### 2.4 Core cannot lag

Core replay/session projections must not lag or be latest-wins. They either complete synchronously or the replay step is not complete.

### 2.5 Expensive work must be explicit

Expensive work must be declared through execution type:

```text
CoreSync
InlineSync
CoalescedSync
AsyncLatest
AsyncOrdered
OfflineArtifact
ReviewOnly
```

No projection should accidentally run heavy work inline.

### 2.6 The graph is lazy

No projection node exists unless it is subscribed to or required as a dependency of a subscribed node.

### 2.7 Projections depend on contracts, not concrete modules

Projection implementations do not directly import and call other projection implementations. They declare dependencies through manifests/factories and consume typed dependency outputs through runtime context.

### 2.8 V1 ignores checkpointing but keeps the seam

Seeking can reset/replay for V1. Full checkpointing is an optimization pass after the graph is implemented and profiled.

---

## 3. Phase Map

The roadmap is split into four major groups.

### Foundation: prove the graph headlessly

```text
Phase 0  Documentation and baseline alignment
Phase 1  Shared projection contracts in ledger-domain
Phase 2  Projection registry and runtime skeleton in ledger
Phase 3  TruthTick and ReplaySession integration
Phase 4  Core base projections and CLI run
Phase 5  Visual base projections, wake/frame policies, coalescing
Phase 6  Projection profiling and validation harness
```

### Extensibility: prove studies can be added safely

```text
Phase 7  batch_features microstructure spine
Phase 8  First derived L3 studies
Phase 9  AsyncLatest and OfflineArtifact interfaces
```

### Product exposure: only after CLI proves the graph

```text
Phase 10 API/WebSocket projection protocol
Phase 11 Lens projection renderers and replay panels
Phase 12 Journal/training memory integration
```

### Advanced vision and optimization

```text
Phase 13 Levels and gamma/0DTE projection scaffolding
Phase 14 Model studies and research artifacts
Phase 15 Checkpointing, cache lineage, and performance optimization
Phase 16 Live/replay convergence pass
```

Each phase below includes enough detail to hand Codex a scoped implementation task and then have a validation agent judge the result.

---

## 4. Shared CLI Validation Shape

Many phases refer to projection CLI commands. These commands do not all exist at the beginning; they are introduced over phases.

Target command family:

```bash
ledger projection list
ledger projection manifest --id bars --version 1
ledger projection graph --projection absorption_score:v1 --params params.json
ledger projection run --symbol ES --date 2026-03-12 --projection bars:v1 --params params.json --batches 10000 --jsonl out.frames.jsonl --digest
ledger projection profile --symbol ES --date 2026-03-12 --projection absorption_score:v1 --params params.json --batches 100000
ledger projection validate --symbol ES --date 2026-03-12 --projection absorption_score:v1 --params params.json --batches 100000
```

Target `projection run` output:

```json
{
  "projection": {
    "id": "bars",
    "version": 1,
    "params_hash": "sha256:..."
  },
  "dataset": {
    "symbol": "ES",
    "market_date": "2026-03-12",
    "replay_dataset_id": "..."
  },
  "run": {
    "requested_batches": 10000,
    "applied_batches": 10000,
    "first_cursor_ts_ns": "...",
    "last_cursor_ts_ns": "...",
    "frames": 812,
    "payload_schema": "candles_v1",
    "digest": "sha256:..."
  },
  "passed": true
}
```

Target `projection profile` output:

```json
{
  "dataset": {
    "symbol": "ES",
    "market_date": "2026-03-12"
  },
  "projection": {
    "id": "absorption_score",
    "version": 1,
    "params_hash": "sha256:..."
  },
  "runtime": {
    "batches": 100000,
    "events": 2451200,
    "wall_ms": 1840,
    "batches_per_sec": 54347,
    "events_per_sec": 1332173
  },
  "nodes": [
    {
      "key": "batch_features:v1:abc",
      "execution_type": "inline_sync",
      "runs": 100000,
      "skips": 0,
      "frames": 0,
      "total_us": 820000,
      "p50_us": 6,
      "p95_us": 18,
      "p99_us": 41,
      "max_us": 115,
      "budget_us": 100,
      "budget_passed": false
    }
  ],
  "frames": {
    "emitted": 812,
    "bytes": 147221
  },
  "async": {
    "jobs_enqueued": 0,
    "jobs_completed": 0,
    "jobs_dropped": 0,
    "stale_frames": 0
  },
  "digest": "sha256:...",
  "passed": false
}
```

The exact JSON can evolve, but each command should output stable structured data so validation agents can inspect it.

---

## 5. Phase 0 — Documentation and Baseline Alignment

### Objective

Adopt the two planning documents into the repo and align future implementation work around them.

### Intended files

```text
docs/study_graph_vision.md
docs/study_graph_phased_implementation.md
```

### Scope

This phase is documentation only.

### Deliverables

```text
1. Vision document committed.
2. Phased implementation document committed.
3. README optionally updated with links to both docs.
4. No runtime or API behavior changes.
```

### Acceptance criteria

```text
Documents are present under docs/.
README or docs index links them.
No code behavior changes.
No tests should fail because of this phase.
```

### Validation

```bash
cargo test --workspace
```

Optional doc sanity:

```bash
ls docs/study_graph_vision.md docs/study_graph_phased_implementation.md
```

### Non-goals

```text
Do not implement DTOs yet.
Do not implement runtime yet.
Do not alter CLI behavior yet.
```

---

## 6. Phase 1 — Shared Projection Contracts in `ledger-domain`

### Objective

Add the pure shared types that define projection identity, manifests, frame envelopes, output schema names, execution policies, lag policies, wake policies, and validation declarations.

This phase creates the language of the graph but does not run the graph.

### Primary code areas

```text
crates/domain/src/projection.rs
crates/domain/src/lib.rs
```

### Deliverables

Add conceptual types:

```rust
ProjectionSpec
ProjectionKey
ProjectionManifest
ProjectionKind
ProjectionOutputSchema
ProjectionPayloadSchemaName
ProjectionFrame
ProjectionFrameStamp
ProjectionFrameOp
ProjectionUpdateMode
SourceView
TemporalPolicy
ProjectionWakePolicy
ProjectionExecutionType
ProjectionDeliverySemantics
ProjectionFramePolicy
ProjectionLagPolicy
ProjectionCachePolicy
TrustTier
ProjectionCostHint
ExpectedCost
MemoryClass
ValidationDecl
VisualizationHint
DependencyDecl
```

### Required behavior

`ProjectionKey` should be generated from:

```text
id
version
canonicalized params hash
```

Parameter hashing should be deterministic. JSON object key order must not affect the hash.

Projection IDs should be validated with a simple rule:

```text
lowercase snake_case or namespaced lowercase tokens
examples:
  bars
  bbo
  batch_features
  level_sets.gamma
  model.level_acceptance
```

Versions should be positive integers.

### Suggested tests

```text
projection_key_same_for_reordered_json_params
projection_key_differs_when_params_differ
projection_id_validation_accepts_expected_ids
projection_id_validation_rejects_bad_ids
manifest_roundtrips_json
frame_stamp_roundtrips_json
lag_policy_roundtrips_json
```

### Agentic validation

```bash
cargo test -p ledger-domain projection
```

If the crate has no package-level filter yet:

```bash
cargo test --workspace projection_key
cargo test --workspace manifest_roundtrips
```

### Acceptance criteria

```text
Projection DTOs compile without pulling in ledger, replay, book, API, or Lens.
DTOs serialize/deserialize cleanly.
Canonical parameter hash is stable.
No runtime logic exists yet.
No dependency on serde_json in hot runtime code is implied beyond manifest/spec boundaries.
```

### Non-goals

```text
Do not implement ProjectionRuntime.
Do not add CLI commands.
Do not add concrete projection nodes.
Do not touch API/WebSocket.
```

### Codex prompt guidance

A good Codex task for this phase:

```text
Add pure projection contract DTOs to ledger-domain. Include serde support, stable params hashing,
roundtrip tests, and no dependencies on ledger/replay/book/api/lens. Do not implement runtime.
```

---

## 7. Phase 2 — Projection Registry and Runtime Skeleton in `ledger`

### Objective

Create the runtime skeleton that can register projection factories, resolve dependency trees, instantiate nodes lazily, detect cycles, share nodes by `ProjectionKey`, and advance synthetic ticks in topological order.

This phase can use synthetic test ticks. It does not need full `ReplaySession` integration yet.

### Primary code areas

```text
crates/ledger/src/projection/mod.rs
crates/ledger/src/projection/registry.rs
crates/ledger/src/projection/runtime.rs
crates/ledger/src/projection/node.rs
crates/ledger/src/projection/metrics.rs
crates/ledger/src/lib.rs
```

### Deliverables

Core traits:

```rust
ProjectionFactory
ProjectionNode
ProjectionContext
ProjectionInputs
ProjectionAdvance
ProjectionSubscription
ProjectionRegistry
ProjectionRuntime
```

Runtime capabilities:

```text
register factory
list manifests
fetch manifest by id/version
canonicalize subscription spec
resolve dependencies recursively
instantiate missing nodes lazily
reuse existing node for same ProjectionKey
track subscriber/reference counts
detect cycles
compute topological active order
advance nodes in topological order on synthetic TruthTick
store last payloads
emit initial snapshot frame
reset runtime generation
```

### Minimal synthetic test nodes

Add test-only nodes such as:

```text
source_counter:v1
  no dependencies
  increments on every synthetic tick

double_counter:v1
  depends on source_counter
  emits source * 2

sum_counter:v1
  depends on source_counter and double_counter
  emits source + double
```

### Suggested tests

```text
runtime_registers_and_lists_manifests
runtime_subscribe_instantiates_node
runtime_reuses_same_key_for_two_subscriptions
runtime_resolves_dependency_tree
runtime_detects_dependency_cycle
runtime_advances_in_topological_order
runtime_skips_unsubscribed_nodes
runtime_reference_counts_unsubscribe
runtime_generation_increments_on_reset
runtime_initial_snapshot_contains_generation_and_projection_key
```

### Agentic validation

```bash
cargo test -p ledger projection::runtime
cargo test -p ledger projection::registry
```

Fallback:

```bash
cargo test --workspace projection
```

### Acceptance criteria

```text
Runtime exists but is headless.
No API/Lens integration.
No real replay dependency required yet.
Dependency resolution is deterministic.
Cycle detection is explicit and tested.
One ProjectionKey maps to one shared node instance.
```

### Non-goals

```text
Do not implement real base projections yet.
Do not add async worker pool yet.
Do not add offline artifact storage yet.
Do not add WebSocket protocol.
```

### Codex prompt guidance

```text
Implement a projection registry/runtime skeleton in ledger using domain projection DTOs.
Use synthetic test nodes only. Support lazy dependency resolution, cycle detection,
node sharing by ProjectionKey, topological execution, and generation reset.
Do not integrate ReplaySession or CLI yet.
```

---

## 8. Phase 3 — TruthTick and ReplaySession Integration

### Objective

Connect `ReplaySession` to `ProjectionRuntime` through a `TruthTick` abstraction.

This phase does not need many real projections; it proves the integration boundary between replay truth and projections.

### Primary code areas

```text
crates/domain/src/projection.rs          # TruthTick-related DTOs if pure
crates/replay/src/...                    # richer step result if needed
crates/ledger/src/replay_session.rs      # active session orchestration
crates/ledger/src/projection/runtime.rs
```

### Deliverables

Add conceptual tick structures:

```rust
TruthTick
TruthTickFlags
ReplayTruthStepResult or equivalent bridge type
```

Modify active replay/session flow so that:

```text
ReplaySimulator steps one batch.
Ledger builds a TruthTick.
ProjectionRuntime receives TruthTick.
ProjectionRuntime returns ProjectionFrames.
ReplaySession step report can include projection frames.
```

If current `run_replay_session` is batch-oriented, this phase can integrate internally without exposing projection frames to CLI yet.

### Required boundaries

```text
ledger-replay emits replay facts; it does not own the projection graph.
ledger owns ProjectionRuntime and converts replay step output to TruthTick.
Projection nodes do not mutate the book.
Projection nodes do not call replay directly.
```

### Suggested tests

```text
replay_session_can_step_with_empty_projection_runtime
truth_tick_generation_matches_session_generation
truth_tick_cursor_advances_monotonically
truth_tick_flags_match_synthetic_trade_batch
truth_tick_flags_match_synthetic_bbo_change
projection_runtime_receives_tick_during_session_step
seek_or_reset_increments_generation
old_generation_frames_are_not_reused_after_reset
```

### Agentic validation

```bash
cargo test -p ledger replay_session
cargo test -p ledger projection
cargo test -p ledger-replay
```

Existing CLI smoke should still pass:

```bash
ledger replay run --symbol ES --date 2026-03-12 --batches 100
```

Use the real symbol/date available in the local test environment; this command is a template.

### Acceptance criteria

```text
ReplaySession can own a ProjectionRuntime.
Replay stepping still works with no projection subscriptions.
TruthTick is built without breaking replay determinism.
Generation is present and increments on reset/seek.
ledger-replay remains study-agnostic.
```

### Non-goals

```text
Do not expose WebSocket.
Do not build Lens panels.
Do not implement heavy studies.
Do not implement checkpoints.
```

### Codex prompt guidance

```text
Integrate ProjectionRuntime into active ReplaySession by building a TruthTick per replay batch.
Keep ledger-replay study-agnostic. Ensure replay session runs still work when no projections are subscribed.
Add tests for cursor/generation/tick flags.
```

---

## 9. Phase 4 — Core Base Projections and CLI Run

### Objective

Implement the first real projections and add the first projection CLI commands. This phase proves that projections can be run headlessly over a real `ReplayDataset`.

### Projections in scope

```text
cursor:v1
status:v1
bbo:v1
canonical_trades:v1
orders:v1 / fills:v1 if the current replay surface can support them cleanly
```

### Primary code areas

```text
crates/ledger/src/projection/base/cursor.rs
crates/ledger/src/projection/base/status.rs
crates/ledger/src/projection/base/bbo.rs
crates/ledger/src/projection/base/trades.rs
crates/ledger/src/projection/base/fills.rs
crates/ledger/src/projection/registry.rs
crates/cli/src/main.rs
```

### CLI commands introduced

```bash
ledger projection list
ledger projection manifest --id bbo --version 1
ledger projection graph --projection bbo:v1 --params params.json
ledger projection run --symbol ES --date 2026-03-12 --projection bbo:v1 --params params.json --batches 10000 --jsonl out.frames.jsonl --digest
```

### Required behavior

`projection list` should output all registered manifests.

`projection manifest` should output the selected manifest as JSON.

`projection graph` should output resolved dependency graph:

```json
{
  "root": "bbo:v1:...",
  "nodes": [
    { "key": "bbo:v1:...", "id": "bbo", "version": 1 }
  ],
  "edges": [],
  "cycle": false
}
```

`projection run` should:

```text
open/hydrate ReplayDataset
start ReplaySession
subscribe root projection
step requested batches
emit optional JSONL frames
emit summary JSON with digest
```

### Digest requirements

The digest should be computed from stable frame contents, excluding nondeterministic wall-clock timing.

Include:

```text
projection key
generation
input cursor
emitted cursor
batch idx
op
payload schema
payload
```

Exclude:

```text
wall-clock timestamps
non-deterministic debug strings
memory addresses
```

### Suggested tests

```text
cursor_projection_emits_monotonic_cursor
status_projection_emits_initial_snapshot
bbo_projection_emits_on_bbo_change
canonical_trades_projection_emits_ordered_trades
projection_run_digest_stable_on_same_input
projection_run_jsonl_frames_have_required_stamp_fields
projection_graph_for_core_projection_has_no_unexpected_dependencies
```

### Agentic validation

```bash
cargo test --workspace projection
ledger projection list
ledger projection manifest --id cursor --version 1
ledger projection manifest --id bbo --version 1
ledger projection graph --projection bbo:v1 --params '{"source_view":"visible"}'
ledger projection run --symbol ES --date 2026-03-12 --projection bbo:v1 --params '{"source_view":"visible"}' --batches 10000 --digest
```

Run the same command twice and compare digest:

```bash
ledger projection run ... --digest > /tmp/run1.json
ledger projection run ... --digest > /tmp/run2.json
# validation agent compares .run.digest
```

### Acceptance criteria

```text
CLI can list and inspect manifests.
CLI can run a projection headlessly over replay.
Projection frames are stamped with generation/cursor/source/execution/temporal metadata.
Digest is stable on repeated runs.
Core projections use CoreSync execution and Prohibited lag policy.
No API or Lens work yet.
```

### Non-goals

```text
Do not implement bars/depth yet unless trivial.
Do not add profiling yet beyond digest summary.
Do not expose WebSocket.
```

### Codex prompt guidance

```text
Add core base projections cursor/status/bbo/canonical_trades and projection CLI list/manifest/graph/run.
The run command should execute against a ReplaySession and output deterministic digestable frames.
Keep API/Lens untouched.
```

---

## 10. Phase 5 — Visual Base Projections, Wake Policies, Frame Policies, and Coalescing

### Objective

Add the base projections needed for charts/DOM and implement the policy machinery that prevents every node from running or emitting on every tick.

### Projections in scope

```text
depth:v1
bars:v1
session_vwap:v1
trade_tape:v1 if distinct from canonical_trades
```

### Policies in scope

```text
ProjectionWakePolicy
ProjectionFramePolicy
ProjectionDeliverySemantics
CoalescedSync execution behavior
```

### Primary code areas

```text
crates/ledger/src/projection/base/depth.rs
crates/ledger/src/projection/base/bars.rs
crates/ledger/src/projection/base/vwap.rs
crates/ledger/src/projection/runtime.rs
crates/ledger/src/projection/metrics.rs
crates/cli/src/main.rs
```

### Required behavior

Implement wake skipping:

```text
bars wakes on trades.
depth wakes on depth changes and/or visibility frames.
vwap wakes on trades.
```

Implement frame policies:

```text
bars can emit on window close and optionally coalesced live-bar updates.
depth can coalesce visual frames.
vwap can emit on change or coalesced replay interval.
```

Implement delivery semantics:

```text
bars uses PatchByKey.
depth uses ReplaceLatest.
vwap uses ReplaceLatest or TimeSeries depending on output shape.
trade stream uses AppendOrdered.
```

### CLI additions

`projection run` should accept frame policy overrides only if explicitly useful for validation. Avoid letting validation mutate core semantics accidentally.

Potential options:

```bash
--emit-all-debug
--max-frames 1000
--jsonl out.frames.jsonl
```

### Suggested tests

```text
bars_time_rollover_fixture
bars_tick_rollover_fixture
bars_volume_rollover_fixture
bars_uses_canonical_trade_prints_not_raw_trade_events
depth_emits_replace_latest_frames
depth_frame_coalescing_limits_frames
vwap_updates_only_on_trade_ticks
runtime_skips_nodes_when_wake_policy_not_due
runtime_marks_dependency_changed_correctly
frame_policy_emit_on_window_close
frame_policy_coalesce_replay_time
```

### Agentic validation

```bash
cargo test --workspace bars
cargo test --workspace depth
cargo test --workspace wake_policy
cargo test --workspace frame_policy

ledger projection manifest --id bars --version 1
ledger projection graph --projection bars:v1 --params '{"kind":"time","seconds":60,"source_view":"visible"}'
ledger projection run --symbol ES --date 2026-03-12 --projection bars:v1 --params '{"kind":"time","seconds":60,"source_view":"visible"}' --batches 100000 --digest --jsonl out/bars.frames.jsonl

ledger projection run --symbol ES --date 2026-03-12 --projection depth:v1 --params '{"levels":50,"source_view":"visible"}' --batches 100000 --digest --jsonl out/depth.frames.jsonl
```

Validation agent checks:

```text
bars frames are monotonic by bar key/time
no duplicate final bars unless patch semantics say so
depth frames do not exceed frame policy when coalescing is enabled
payload schema matches manifest
source_view appears in frame stamp
```

### Acceptance criteria

```text
Wake policies work.
Frame policies work.
State advancement and frame emission are separate.
Visual base projections can be validated through CLI.
Frame volume is controlled.
No API or Lens work yet.
```

### Non-goals

```text
Do not add derived L3 studies yet.
Do not add async workers yet.
Do not add WebSocket.
```

### Codex prompt guidance

```text
Implement bars/depth/session_vwap projections and wake/frame/delivery policy handling.
Ensure state advancement is separate from frame emission and validate via CLI projection run.
```

---

## 11. Phase 6 — Projection Profiling and Validation Harness

### Objective

Make performance and correctness measurable. Add CLI commands that validation agents can use to reject slow, nondeterministic, or semantically invalid projections.

### Primary code areas

```text
crates/ledger/src/projection/metrics.rs
crates/ledger/src/projection/runtime.rs
crates/ledger/src/projection/validation.rs
crates/cli/src/main.rs
```

### CLI commands introduced

```bash
ledger projection profile --symbol ES --date 2026-03-12 --projection bars:v1 --params params.json --batches 100000
ledger projection validate --symbol ES --date 2026-03-12 --projection bars:v1 --params params.json --batches 100000
```

### Metrics to collect

Per runtime:

```text
batches processed
events processed
wall time
batches/sec
events/sec
total frames emitted
total frame bytes
max resident memory if available
```

Per node:

```text
runs
skips
wake reason counts
dependency-triggered runs
timer-triggered runs
event-triggered runs
frames emitted
frame bytes
total advance time
p50 advance time
p95 advance time
p99 advance time
max advance time
budget pass/fail
```

Async metrics, even if async is not fully implemented yet:

```text
jobs enqueued
jobs completed
jobs dropped old generation
jobs dropped unsubscribed
jobs dropped older than accepted
jobs dropped too stale
accepted frames
stale frames
lag p50/p95/p99/max
```

### Validation checks

```text
Manifest exists.
Params validate.
Dependencies resolve.
No cycles.
Output schema matches frames.
Frames include required stamps.
Generation is stable within run.
Cursor is monotonic.
Digest stable across repeated runs.
CoreSync projections have zero lag.
InlineSync projections have zero lag when due.
CoalescedSync projections do not violate frame policy.
AppendOrdered payloads preserve order.
PatchByKey payloads include stable keys.
TemporalPolicy::Causal projections do not emit hindsight/review-only markers.
Profile budgets pass or warnings are explicit for Experimental trust tier.
```

### Suggested tests

```text
metrics_record_runs_and_skips
metrics_compute_percentiles
profile_report_roundtrips_json
validate_fails_on_missing_manifest
validate_fails_on_cycle
validate_fails_on_schema_mismatch
validate_fails_on_core_lag
validate_warns_or_fails_on_profile_budget_by_trust_tier
```

### Agentic validation

```bash
cargo test --workspace projection::metrics
cargo test --workspace projection::validation

ledger projection profile --symbol ES --date 2026-03-12 --projection bars:v1 --params '{"kind":"time","seconds":60,"source_view":"visible"}' --batches 100000
ledger projection validate --symbol ES --date 2026-03-12 --projection bars:v1 --params '{"kind":"time","seconds":60,"source_view":"visible"}' --batches 100000
```

The validation agent should parse JSON and fail the phase if:

```text
passed != true
any Core trust-tier projection exceeds required budget
any required field missing
frames are nondeterministic across repeated runs
```

### Acceptance criteria

```text
Projection performance is visible.
Validation JSON is stable enough for agents.
Core budgets can be enforced.
Experimental warnings can be surfaced without blocking unless configured.
This becomes the gate before derived studies and API/Lens exposure.
```

### Non-goals

```text
Do not optimize prematurely unless a budget fails.
Do not add checkpointing.
Do not add WebSocket.
```

### Codex prompt guidance

```text
Add projection profile/validate CLI support with per-node metrics, deterministic digest validation,
schema checks, cursor/generation checks, and profile budgets. Keep API/Lens untouched.
```

---

## 12. Phase 7 — `batch_features:v1` Microstructure Spine

### Objective

Implement the shared L3 feature spine that future derived studies will consume instead of rescanning raw MBO events independently.

### Projection in scope

```text
batch_features:v1
```

### Primary code areas

```text
crates/ledger/src/projection/base/batch_features.rs
crates/domain/src/projection_payloads.rs or equivalent
crates/ledger/src/projection/registry.rs
```

### Dependencies

Likely dependencies:

```text
bbo:v1
canonical_trades:v1
depth:v1 or direct TruthTick book/depth summaries if more efficient
```

The design can choose direct `TruthTick` summaries for base facts, but derived studies should consume `batch_features`.

### Payload fields

Start with a pragmatic subset:

```text
cursor_ts_ns
batch_idx
bbo_before
bbo_after
spread_before_ticks
spread_after_ticks
trade_count
buy_volume
sell_volume
largest_trade_size
sweep_side
sweep_levels_crossed
depth_delta_by_level
price_displacement_ticks
no_bbo
locked_or_crossed
```

Later extension fields:

```text
bid_add_size_by_level
bid_cancel_size_by_level
ask_add_size_by_level
ask_cancel_size_by_level
queue_rebuild_flags
liquidity_pull_flags
touched_levels
```

### Required behavior

```text
Compute once per due batch.
Expose typed payload.
Do not emit huge frames by default unless explicitly subscribed.
Support diagnostic frame output for CLI validation.
Use InlineSync or CoalescedSync depending on exact cost.
Respect source_view.
```

### Suggested tests

```text
batch_features_counts_trade_volume
batch_features_detects_sweep_levels
batch_features_tracks_bbo_before_after
batch_features_flags_no_bbo
batch_features_flags_locked_crossed
batch_features_depth_delta_fixture
batch_features_deterministic_digest
batch_features_profile_budget
```

### Agentic validation

```bash
cargo test --workspace batch_features

ledger projection manifest --id batch_features --version 1
ledger projection graph --projection batch_features:v1 --params '{"price_levels":5,"source_view":"visible"}'
ledger projection run --symbol ES --date 2026-03-12 --projection batch_features:v1 --params '{"price_levels":5,"source_view":"visible"}' --batches 100000 --digest --jsonl out/batch_features.frames.jsonl
ledger projection profile --symbol ES --date 2026-03-12 --projection batch_features:v1 --params '{"price_levels":5,"source_view":"visible"}' --batches 100000
ledger projection validate --symbol ES --date 2026-03-12 --projection batch_features:v1 --params '{"price_levels":5,"source_view":"visible"}' --batches 100000
```

Validation agent checks:

```text
profile budget passes or reports exact failure
frames/payloads do not explode in size by default
no raw rescans by downstream nodes are necessary for basic L3 facts
payload schema matches batch_features_v1
```

### Acceptance criteria

```text
batch_features:v1 is available as a dependency.
It is deterministic.
It is profiled.
It is cheap enough or declared appropriately.
Derived studies can consume it.
```

### Non-goals

```text
Do not implement every possible microstructure field.
Do not optimize with checkpoints.
Do not add model inference.
```

### Codex prompt guidance

```text
Implement batch_features:v1 as the shared L3 feature spine. It should derive compact per-batch features
from TruthTick and/or base projections, expose a typed payload, include fixture tests, and pass CLI profile/validate.
```

---

## 13. Phase 8 — First Derived L3 Studies

### Objective

Prove that new studies can be built as small dependency-declared modules over shared base projections.

### Studies in scope

Recommended first set:

```text
sweep_detector:v1
absorption_score:v1
book_pressure:v1 or liquidity_pull_stack:v1
```

Do not implement too many. The goal is to validate the extension model.

### Primary code areas

```text
crates/ledger/src/projection/studies/sweep_detector.rs
crates/ledger/src/projection/studies/absorption_score.rs
crates/ledger/src/projection/studies/book_pressure.rs
crates/ledger/src/projection/registry.rs
```

### Dependency expectations

```text
sweep_detector:v1
  depends on batch_features:v1

absorption_score:v1
  depends on batch_features:v1
  optionally bars:v1

book_pressure:v1
  depends on batch_features:v1
  optionally depth:v1 and bbo:v1
```

### Output schemas

```text
sweep_detector:v1
  markers_v1 or alerts_v1

absorption_score:v1
  time_series_v1 and/or markers_v1

book_pressure:v1
  time_series_v1 or score_band_v1
```

### Required behavior

```text
Each study has a manifest.
Each study declares dependencies.
Each study has synthetic fixture tests.
Each study passes deterministic digest validation.
Each study has profile budget.
No study rescans raw MBO independently.
No study calls another concrete study module directly.
```

### Suggested tests

```text
sweep_detector_detects_single_level_sweep
sweep_detector_detects_multi_level_sweep
sweep_detector_ignores_non_sweep_trade
absorption_score_high_when_volume_fails_to_displace
absorption_score_low_when_price_displaces_normally
book_pressure_detects_depth_imbalance_fixture
study_dependency_graph_matches_expected_nodes
study_deterministic_digest
study_profile_budget
```

### Agentic validation

```bash
cargo test --workspace sweep_detector
cargo test --workspace absorption_score

ledger projection graph --projection sweep_detector:v1 --params '{"price_levels":5,"source_view":"visible"}'
ledger projection validate --symbol ES --date 2026-03-12 --projection sweep_detector:v1 --params '{"price_levels":5,"source_view":"visible"}' --batches 100000

ledger projection graph --projection absorption_score:v1 --params '{"window_ms":500,"price_levels":3,"source_view":"visible"}'
ledger projection run --symbol ES --date 2026-03-12 --projection absorption_score:v1 --params '{"window_ms":500,"price_levels":3,"source_view":"visible"}' --batches 100000 --digest --jsonl out/absorption.frames.jsonl
ledger projection profile --symbol ES --date 2026-03-12 --projection absorption_score:v1 --params '{"window_ms":500,"price_levels":3,"source_view":"visible"}' --batches 100000
```

Validation agent checks:

```text
dependency graph uses batch_features
no raw event scan in study implementation if detectable by review
frames are schema-valid
experimental trust tier is set unless the study is proven core
profile budgets are reasonable
```

### Acceptance criteria

```text
At least two derived studies pass CLI validation.
The graph can share dependencies between multiple subscribed studies.
Study manifests are useful enough for Codex and validation agents.
The architecture feels extensible.
```

### Non-goals

```text
Do not add WebSocket yet.
Do not build Lens overlays yet.
Do not implement gamma/model studies yet.
```

### Codex prompt guidance

```text
Implement sweep_detector:v1 and absorption_score:v1 as derived ProjectionNode modules.
They must consume batch_features through declared dependencies, include manifests and fixture tests,
and pass projection graph/run/profile/validate CLI commands. Do not touch API/Lens.
```

---

## 14. Phase 9 — AsyncLatest and OfflineArtifact Interfaces

### Objective

Add the architectural seams for expensive projections without committing to a full ML stack.

This phase should prove lag/staleness semantics and artifact as-of reveal behavior using simple test projections.

### Features in scope

```text
AsyncProjectionJob
AsyncProjectionResult
AsyncLatest scheduler
async result acceptance gate
lag/stale frame metadata
OfflineArtifact reader interface
artifact as-of reveal test projection
```

### Test projections

```text
slow_counter_async:v1
  synthetic AsyncLatest projection that returns delayed values

artifact_series:v1
  synthetic OfflineArtifact projection that reads rows by as_of cursor
```

### Primary code areas

```text
crates/ledger/src/projection/async_scheduler.rs
crates/ledger/src/projection/artifact_reader.rs
crates/ledger/src/projection/runtime.rs
crates/ledger/src/projection/tests or test fixtures
```

### Required AsyncLatest behavior

```text
Does not block replay.
Can drop older pending jobs for same ProjectionKey.
Accepts result only if generation matches.
Drops result if subscription no longer exists.
Drops result if input cursor <= last accepted cursor.
Marks stale if lag exceeds threshold.
Drops too-stale result if lag exceeds drop threshold.
Frame includes input_cursor, emitted_cursor, lag, stale.
```

### Required OfflineArtifact behavior

```text
Reads from an artifact abstraction, not hardcoded file paths.
At replay cursor T, returns only rows with as_of <= T.
Can emit missing-artifact error or declared fallback.
Does not leak future rows.
```

### Suggested tests

```text
async_latest_drops_old_generation_result
async_latest_drops_unsubscribed_result
async_latest_drops_older_than_last_accepted
async_latest_marks_stale_result
async_latest_drops_too_stale_result
async_latest_newest_valid_result_wins
async_latest_does_not_block_inline_runtime
artifact_projection_reveals_only_as_of_rows
artifact_projection_does_not_reveal_future_rows
artifact_projection_missing_artifact_policy
```

### Agentic validation

```bash
cargo test --workspace async_latest
cargo test --workspace offline_artifact

ledger projection validate --symbol ES --date 2026-03-12 --projection slow_counter_async:v1 --params '{"interval_ms":1000}' --batches 10000
ledger projection validate --symbol ES --date 2026-03-12 --projection artifact_series:v1 --params '{"artifact":"synthetic"}' --batches 10000
```

Expected profile fields:

```text
jobs_enqueued
jobs_completed
jobs_dropped_old_generation
jobs_dropped_unsubscribed
jobs_dropped_older_than_accepted
jobs_dropped_too_stale
accepted_frames
stale_frames
lag p50/p95/p99/max
```

### Acceptance criteria

```text
AsyncLatest execution type exists and is tested.
OfflineArtifact execution type exists and is tested.
Lag/stale metadata is reliable.
No real heavy ML model required.
No API/Lens work required.
```

### Non-goals

```text
Do not build production ML models.
Do not build full artifact storage/cache lineage yet.
Do not parallelize the entire graph.
```

### Codex prompt guidance

```text
Add AsyncLatest and OfflineArtifact interfaces to ProjectionRuntime using synthetic test projections.
Implement strict async result acceptance, lag/stale frame metadata, and as-of artifact reveal tests.
Do not add real model studies or WebSocket yet.
```

---

## 15. Phase 10 — API/WebSocket Projection Protocol

### Objective

Expose the already-validated projection runtime through a generic WebSocket protocol.

This phase should not invent new computation. It should adapt CLI-validated runtime behavior to transport.

### Primary code areas

```text
crates/api/src/...
crates/ledger/src/replay_session.rs
crates/domain/src/projection.rs
```

### Protocol commands in scope

```json
{ "type": "replay.open", "symbol": "ES", "date": "2026-03-12" }
{ "type": "replay.close" }
{ "type": "replay.step", "batches": 100 }
{ "type": "replay.play", "speed": 1.0 }
{ "type": "replay.pause" }
{ "type": "replay.seek", "ts_ns": "..." }
{ "type": "projection.subscribe", "subscription_id": "main-bars", "projection": { "id": "bars", "version": 1, "params": {} } }
{ "type": "projection.unsubscribe", "subscription_id": "main-bars" }
```

### Protocol frames in scope

```json
{ "type": "replay.state", "session_id": "...", "generation": 1, "cursor_ts_ns": "..." }
{ "type": "projection.frame", "subscription_id": "main-bars", "frame": {} }
{ "type": "projection.error", "subscription_id": "main-bars", "error": {} }
{ "type": "projection.diagnostic", "frame": {} }
```

### Required behavior

```text
WebSocket uses the same ProjectionSpec, ProjectionFrame, and manifest DTOs as CLI.
Subscribe resolves graph through ProjectionRuntime.
Initial snapshot frame is sent on subscription.
Frames include generation and cursor stamps.
Seek increments generation and sends fresh snapshots.
Unsubscribe decrements runtime subscription/reference count.
Backpressure behavior is explicit for transport.
```

### Transport backpressure

Transport should not corrupt projection semantics.

Recommended behavior:

```text
ReplaceLatest frames may be dropped/replaced in transport queue if a newer frame exists.
AppendOrdered frames must preserve order or surface overflow error.
PatchByKey frames can coalesce by key if policy allows.
Core replay stepping must not block indefinitely on a slow UI connection.
```

### Suggested tests

```text
websocket_subscribe_receives_initial_snapshot
websocket_step_receives_projection_frames
websocket_unsubscribe_stops_frames
websocket_seek_increments_generation
websocket_drops_old_generation_frames
websocket_rejects_unknown_projection
websocket_rejects_invalid_params
websocket_preserves_append_ordered_frames
websocket_coalesces_replace_latest_frames_under_backpressure
```

### Agentic validation

CLI remains the primary validation. Add API tests after CLI passes:

```bash
cargo test -p ledger-api projection
cargo test --workspace websocket

ledger projection validate --symbol ES --date 2026-03-12 --projection bars:v1 --params '{"kind":"time","seconds":60,"source_view":"visible"}' --batches 100000
```

Then API integration test can open a session and subscribe to the same projection.

### Acceptance criteria

```text
API exposes generic projection subscription protocol.
API does not compute projections independently.
Frame shape matches CLI-validated ProjectionFrame.
Generation handling works across seek/reset.
Backpressure behavior is tested enough for initial Lens integration.
```

### Non-goals

```text
Do not build all Lens panels.
Do not add new projections through API-only paths.
Do not implement live mode.
```

### Codex prompt guidance

```text
Expose ReplaySession projection subscriptions over WebSocket using existing ProjectionSpec and ProjectionFrame DTOs.
Do not create any API-specific computation path. Add subscribe/unsubscribe/step/seek tests and generation handling.
```

---

## 16. Phase 11 — Lens Projection Renderers and Initial Replay Panels

### Objective

Teach Lens to consume generic projection frames and render initial schemas. Lens should not compute market facts.

### Primary code areas

```text
lens/src/...
crates/api DTOs if generated/shared
```

### Schemas in initial scope

```text
cursor_v1
status_v1
bbo_v1
trade_events_v1
depth_v1
candles_v1
time_series_v1
markers_v1
diagnostics_v1
```

### Initial UI surfaces

```text
Replay controls: open, play, pause, step, seek.
Chart panel: candles_v1.
DOM/depth panel: depth_v1 + bbo_v1.
Trade tape: trade_events_v1.
Study overlay: time_series_v1 / markers_v1.
Diagnostics panel: generation, lag, stale, frame counts, subscription state.
```

### Required Lens behavior

```text
Lens subscribes to projections by ProjectionSpec.
Lens routes frames by payload_schema.
Lens discards old generation frames.
Lens handles snapshot/append/patch/replace/clear ops.
Lens displays stale/lagging async projections distinctly.
Lens does not compute bars, DOM facts, trade classification, or study values locally.
```

### Lag/stale UI behavior

```text
CoreSync / InlineSync:
  render normally; no lag indicator.

CoalescedSync:
  render latest frame; optional debug coalescing metrics.

AsyncLatest not stale:
  render normally or with subtle async badge.

AsyncLatest stale:
  dim, badge, or show lag indicator.

AsyncLatest too stale / dropped:
  show waiting/degraded state if relevant.

ReviewOnly:
  hide unless review mode is explicitly enabled.
```

### Suggested tests

```text
lens_routes_candles_schema_to_chart_renderer
lens_discards_old_generation_frame
lens_applies_patch_by_key_candle_update
lens_replaces_depth_snapshot
lens_appends_trade_events_in_order
lens_marks_stale_async_frame
lens_does_not_render_review_only_in_training_mode
```

### Agentic validation

Frontend validation will depend on current Lens tooling. Target examples:

```bash
cd lens
npm test
npm run typecheck
npm run lint
```

Backend smoke remains:

```bash
ledger projection validate --symbol ES --date 2026-03-12 --projection bars:v1 --params '{"kind":"time","seconds":60,"source_view":"visible"}' --batches 100000
```

End-to-end manual/agent smoke:

```text
Open validated ReplayDataset.
Subscribe chart to bars:v1.
Step replay.
Chart updates.
Seek replay.
Old generation frames are discarded.
Subscribe absorption_score:v1.
Overlay appears with correct schema and lag metadata.
```

### Acceptance criteria

```text
Lens renders generic projection frames for initial schemas.
Lens does not compute market facts.
Replay controls and subscriptions work over WebSocket.
Lag/stale/review-only metadata is respected.
```

### Non-goals

```text
Do not build all advanced visualizations.
Do not add gamma/0DTE yet.
Do not add journaling yet unless needed for debug.
```

### Codex prompt guidance

```text
Add Lens projection subscription client and schema-based renderers for initial payload schemas.
Ensure old generation frames are discarded and stale async frames are visibly marked.
Do not compute bars/studies in Lens.
```

---

## 17. Phase 12 — Journal and Training Memory Integration

### Objective

Connect journal entries to the projection environment so review can query decisions by study versions, visible subscriptions, levels, and selected projection values.

### Primary code areas

```text
crates/domain/src/journal.rs
crates/store/src/...           # SQLite schema/migrations
crates/ledger/src/journal.rs
crates/api/src/...             # journal routes later
lens/src/...                   # journal UI later
```

### Data to capture

```text
MarketDay
ReplayDatasetId
ReplaySessionId
entry cursor
exit cursor
orders/fills
visible projection subscriptions
projection graph fingerprint
level set fingerprints
selected projection values at entry
selected projection values at exit
chart/layout state
notes/tags
screenshots/references
review comments
```

### Required projection integration

```text
Journal entry can record active subscriptions.
Journal entry can record ProjectionKey/version/params hash.
Journal entry can record selected current projection payload values.
Journal entry can record stale/lag status of async projections at decision time.
Journal entry can distinguish causal vs review-only values.
```

### Suggested tests

```text
journal_entry_records_visible_projection_specs
journal_entry_records_graph_fingerprint
journal_entry_records_projection_value_snapshot
journal_entry_excludes_review_only_values_in_training_mode
journal_entry_records_async_lag_state
journal_roundtrips_sqlite
```

### Agentic validation

```bash
cargo test --workspace journal
```

Projection runtime smoke:

```bash
ledger projection validate --symbol ES --date 2026-03-12 --projection absorption_score:v1 --params '{"window_ms":500,"price_levels":3,"source_view":"visible"}' --batches 100000
```

Future CLI command concept:

```bash
ledger journal create-synthetic --symbol ES --date 2026-03-12 --cursor-ts-ns ... --include-active-projections
ledger journal inspect --id ...
```

### Acceptance criteria

```text
Journal can preserve decision environment by projection identity/version/params.
Projection graph fingerprint exists.
Selected projection values can be captured.
Review-only and stale async values are labeled correctly.
```

### Non-goals

```text
Do not build advanced review queries yet.
Do not require full screenshot pipeline yet.
Do not implement model/gamma studies yet.
```

### Codex prompt guidance

```text
Add journal data structures and persistence fields that capture active projection subscriptions,
graph fingerprint, and selected projection values. Ensure temporal policy and async lag state are preserved.
```

---

## 18. Phase 13 — Levels and Gamma/0DTE Projection Scaffolding

### Objective

Make price levels first-class projection inputs, starting with simple/manual/prior-day/VWAP levels and leaving clean seams for 0DTE/gamma.

### Projections in scope

Initial:

```text
level_sets.manual:v1
level_sets.prior_day:v1
level_sets.vwap:v1
level_sets.volume_profile:v1 if cheap enough
```

Scaffold for later:

```text
level_sets.gamma:v1
gamma_heatmap:v1
```

### Primary code areas

```text
crates/domain/src/levels.rs
crates/ledger/src/projection/base/level_sets.rs
crates/ledger/src/projection/studies/level_reaction_score.rs
crates/store/src/... optional level storage
```

### Level DTO fields

```text
price
source
kind
strength
valid_from_ts_ns
valid_to_ts_ns
as_of_ts_ns
underlying
mapping_version
metadata
version
```

### Required behavior

```text
Level sets emit price_level_set_v1.
Level sets are time-aware.
At replay cursor T, only valid/as-of levels are visible.
Level projections can be consumed by derived studies.
Manual/prior-day/VWAP levels work before gamma.
```

### First derived level study

```text
level_reaction_score:v1
  depends on level_sets.*
  depends on batch_features:v1
  depends on bars:v1
  emits time_series_v1 / markers_v1 / alerts_v1
```

### Gamma scaffolding behavior

Do not require full gamma computation yet. Define the ingestion/shape expectations:

```text
External gamma snapshot has snapshot_ts/as_of_ts.
Projection reveals latest snapshot where as_of_ts <= cursor.
Mapping to ES has explicit mapping_version.
Output is price_level_set_v1 and/or heatmap_cells_v1.
```

### Suggested tests

```text
manual_levels_visible_within_valid_window
prior_day_levels_load_for_market_day
vwap_level_updates_causally
level_set_does_not_reveal_future_as_of_level
gamma_scaffold_does_not_reveal_future_snapshot
level_reaction_score_uses_declared_level_dependency
level_reaction_score_fixture_touch_reject_accept
```

### Agentic validation

```bash
cargo test --workspace levels
cargo test --workspace level_reaction_score

ledger projection graph --projection level_reaction_score:v1 --params '{"level_source":"manual","source_view":"visible"}'
ledger projection validate --symbol ES --date 2026-03-12 --projection level_reaction_score:v1 --params '{"level_source":"manual","source_view":"visible"}' --batches 100000
```

If synthetic gamma artifact exists:

```bash
ledger projection validate --symbol ES --date 2026-03-12 --projection level_sets.gamma:v1 --params '{"artifact":"synthetic"}' --batches 100000
```

### Acceptance criteria

```text
Levels are projection inputs, not chart hacks.
As-of semantics work.
At least one level-derived study works.
Gamma scaffolding exists without forcing full gamma implementation.
```

### Non-goals

```text
Do not build production gamma calculations yet unless ready.
Do not overbuild options data ingestion.
Do not require Lens heatmap sophistication yet.
```

### Codex prompt guidance

```text
Add level DTOs and time-aware level set projections. Start with manual/prior-day/VWAP levels,
then add gamma scaffolding as an as-of external artifact shape. Implement level_reaction_score over batch_features and levels.
```

---

## 19. Phase 14 — Model Studies and Research Artifacts

### Objective

Add the first model-oriented studies using AsyncLatest and OfflineArtifact mechanisms already proven in Phase 9.

### Candidate projections

```text
regime_classifier:v1
continuation_quality_score:v1
level_acceptance_model:v1
gamma_l3_reversal_context:v1
```

### Execution type choices

Online experimental model:

```text
execution_type: AsyncLatest
delivery_semantics: ReplaceLatest
lag_policy: LatestWins
trust_tier: Experimental
```

Precomputed research/model artifact:

```text
execution_type: OfflineArtifact
temporal_policy: AsOfExternal or HindsightReviewOnly depending on artifact
lag_policy: ArtifactAsOf or ReviewOnly
trust_tier: Experimental or ResearchOnly
```

### Primary code areas

```text
crates/ledger/src/projection/studies/regime_classifier.rs
crates/ledger/src/projection/studies/continuation_quality_score.rs
crates/ledger/src/projection/studies/gamma_l3_reversal_context.rs
crates/ledger/src/projection/artifact_reader.rs
crates/cli/src/main.rs
```

### Required behavior

```text
Models must not block replay unless explicitly declared InlineSync and proven cheap.
Async results include input_cursor/emitted_cursor/lag/stale.
Offline artifacts reveal rows only as-of cursor.
Hindsight labels are ReviewOnly.
CLI profile reports async lag distribution.
```

### Suggested tests

```text
regime_classifier_async_latest_newest_wins
regime_classifier_marks_stale_when_lag_exceeds_budget
continuation_quality_offline_artifact_as_of_reveal
hindsight_model_blocked_in_training_mode
gamma_l3_reversal_context_dependency_graph
model_projection_deterministic_when_seeded
```

### Agentic validation

```bash
cargo test --workspace regime_classifier
cargo test --workspace model_studies

ledger projection profile --symbol ES --date 2026-03-12 --projection regime_classifier:v1 --params params/regime.json --batches 100000
ledger projection validate --symbol ES --date 2026-03-12 --projection regime_classifier:v1 --params params/regime.json --batches 100000
```

Validation agent checks:

```text
jobs dropped/accepted behavior matches policy
lag p95/p99 under threshold or marked failed
stale frames counted
no review-only output in training mode
model seed/determinism rules documented
```

### Acceptance criteria

```text
At least one async model study works.
At least one artifact-backed study works or synthetic artifact validates the path.
Lag/stale semantics are visible to CLI and Lens.
Review-only outputs are protected.
```

### Non-goals

```text
Do not build a full training platform.
Do not require GPU/Neural Engine integration.
Do not optimize model performance before profiling proves the need.
```

### Codex prompt guidance

```text
Implement one AsyncLatest model study and/or one OfflineArtifact research study using existing projection contracts.
Ensure lag/stale/as-of/review-only behavior is validated through CLI profile/validate.
```

---

## 20. Phase 15 — Checkpointing, Cache Lineage, and Performance Optimization Pass

### Objective

After the built graph has real projections and profile data, optimize seek/replay performance and artifact reuse.

This phase should happen after observation, not before.

### Triggers for this phase

Start this phase when one or more are true:

```text
seek-to-RTH or seek-to-arbitrary-time is too slow
projection runtime rebuild after seek is painful
batch_features or derived studies become expensive at scale
offline artifacts need reliable cache invalidation
Lens replay speed cannot keep target playback rate with desired subscriptions
```

### Features in scope

```text
Replay checkpoints
Projection checkpoints
CheckpointableProjectionNode trait
Cache lineage keys
Projection artifact storage
Read-through local projection cache
Profile-guided budget tuning
Optional parallelism for safe parts
```

### Primary code areas

```text
crates/ledger/src/projection/checkpoint.rs
crates/ledger/src/projection/cache.rs
crates/store/src/... artifact metadata
crates/domain/src/projection.rs cache/checkpoint DTOs
crates/ledger/src/replay_session.rs
```

### Checkpoint design

Replay checkpoint:

```text
book state
cursor
batch index
visibility model state
execution simulator state
orders/fills
same-timestamp policy state if any
```

Projection checkpoint:

```text
node state
output buffers if needed
last payload
dependency fingerprints
manifest/version/params hash
source view
temporal policy
```

Trait shape:

```rust
pub trait CheckpointableProjectionNode {
    fn checkpoint(&self) -> Result<ProjectionCheckpoint>;
    fn restore(&mut self, checkpoint: ProjectionCheckpoint) -> Result<()>;
}
```

### Cache lineage key

Move toward:

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
+ ManifestHash or CodeHash
```

### Suggested tests

```text
projection_checkpoint_restore_produces_same_digest
replay_checkpoint_seek_matches_full_replay_digest
cache_key_changes_when_params_change
cache_key_changes_when_dependency_version_changes
cache_key_changes_when_trade_print_policy_changes
cache_key_changes_when_level_set_version_changes
artifact_cache_read_through_rebuilds_missing_local_file
```

### Agentic validation

Digest equality is the core gate:

```bash
ledger projection run --symbol ES --date 2026-03-12 --projection absorption_score:v1 --params params.json --start-ts-ns START --batches 100000 --digest > full.json
ledger projection run --symbol ES --date 2026-03-12 --projection absorption_score:v1 --params params.json --start-ts-ns START --batches 100000 --use-checkpoints --digest > checkpointed.json
# validation agent compares digest
```

Profile comparison:

```bash
ledger projection profile --symbol ES --date 2026-03-12 --projection absorption_score:v1 --params params.json --batches 100000 --use-checkpoints
```

### Acceptance criteria

```text
Checkpointed runs produce the same deterministic digest as non-checkpointed runs.
Seek performance improves measurably.
Cache invalidation is lineage-aware.
Projection checkpointing is optional per node.
Nodes that cannot checkpoint still work through reset/replay fallback.
```

### Non-goals

```text
Do not implement checkpointing before profile data justifies it.
Do not sacrifice determinism for speed.
Do not make every projection checkpointable immediately.
```

### Codex prompt guidance

```text
Add checkpoint/cache optimization based on existing profile data. Implement digest-equality validation between full replay and checkpointed replay.
Do not change projection semantics or frame schemas unless versioned.
```

---

## 21. Phase 16 — Live/Replay Convergence Pass

### Objective

Adapt the projection graph so live data can feed the same downstream interface as replay.

This phase should happen only after replay projections, API, Lens, and validation are solid.

### Features in scope

```text
LiveSession or equivalent source adapter
Live TruthTick producer
live source view semantics
same ProjectionRuntime interface
live-safe projection subset
live lag/backpressure policies
live order/execution integration later
```

### Required behavior

```text
Lens should consume ProjectionFrames without caring whether source is replay or live.
Core live projections must keep up with ingestion.
Async studies remain latest-wins/stale-aware.
ReviewOnly projections blocked.
OfflineArtifact projections use as-of external artifacts only if valid.
```

### Suggested tests

```text
live_tick_feeds_projection_runtime
live_projection_frame_shape_matches_replay
live_core_projection_zero_lag_or_backpressure_error
live_async_projection_marks_stale
review_only_projection_rejected_in_live_mode
```

### Agentic validation

This phase may need synthetic live feed tests before real live adapters:

```bash
cargo test --workspace live_projection
```

Future CLI concept:

```bash
ledger projection live-smoke --source synthetic --projection bbo:v1 --duration-sec 30
```

### Acceptance criteria

```text
Replay and live converge below Lens.
ProjectionFrame contract holds.
Live mode cannot accidentally show review-only/hindsight outputs.
Core live projections have explicit backpressure/failure behavior.
```

### Non-goals

```text
Do not add live before replay graph is proven.
Do not force all replay-only studies to be live-safe.
```

---

## 22. Definition of Done for Any New Projection

Every new projection should satisfy this checklist before being exposed outside CLI.

### Manifest

```text
id
version
name
description
kind
params_schema
default_params
dependencies
output_schema
source_view or source_view parameter
temporal_policy
wake_policy
execution_type
delivery_semantics
frame_policy
lag_policy
cache_policy
trust_tier
cost_hint
visualization hints
validation declarations
```

### Tests

```text
params validation
manifest roundtrip if custom
synthetic fixture behavior
dependency graph expectation
output schema expectation
deterministic digest where applicable
temporal/no-future-leakage behavior
profile budget behavior
lag/stale behavior if async
artifact as-of behavior if artifact-backed
```

### CLI validation

At minimum:

```bash
ledger projection manifest --id <id> --version <version>
ledger projection graph --projection <id>:v<version> --params <params>
ledger projection run --symbol ES --date <date> --projection <id>:v<version> --params <params> --batches <n> --digest
ledger projection profile --symbol ES --date <date> --projection <id>:v<version> --params <params> --batches <n>
ledger projection validate --symbol ES --date <date> --projection <id>:v<version> --params <params> --batches <n>
```

### Review questions

```text
Does this projection need to be online, async, or artifact-backed?
Can it consume batch_features instead of raw events?
Does it respect source_view?
Could it leak future information?
What happens on seek?
What happens if it is slow?
What happens if Lens cannot keep up?
Is newest-wins correct, or does order matter?
Should this be Core, Experimental, or ResearchOnly?
What version should it be?
What output schema does Lens need?
```

---

## 23. Codex Work Packet Template

Use this template when handing implementation tasks to Codex.

```text
Task title:
  <short phase/task title>

Phase:
  <phase number and name>

Goal:
  <what should exist after this task>

Repo boundaries:
  Modify:
    <crate/file list>
  Do not modify:
    <crate/file list>

Projection contracts involved:
  <ProjectionSpec/Manifest/Frame/etc.>

Required behavior:
  <bullet list>

Validation commands:
  <cargo test commands>
  <ledger projection commands>

Acceptance criteria:
  <explicit pass/fail list>

Non-goals:
  <what Codex should not do>

Notes:
  <relevant design details from docs>
```

Example:

```text
Task title:
  Implement absorption_score:v1 as a derived study

Phase:
  Phase 8 — First Derived L3 Studies

Goal:
  Add absorption_score:v1 over batch_features:v1 with manifest, fixture tests,
  deterministic CLI run/profile/validate support.

Repo boundaries:
  Modify:
    crates/ledger/src/projection/studies/absorption_score.rs
    crates/ledger/src/projection/registry.rs
    tests/fixtures if needed
  Do not modify:
    crates/api
    lens
    ledger-replay public behavior except if a missing payload field is required and justified

Required behavior:
  Use declared dependencies.
  Consume batch_features through ProjectionContext.
  Emit time_series_v1 and/or markers_v1.
  Use InlineSync and ProhibitedWhenDue lag policy.
  TrustTier Experimental.

Validation commands:
  cargo test --workspace absorption_score
  ledger projection graph --projection absorption_score:v1 --params '{"window_ms":500,"price_levels":3,"source_view":"visible"}'
  ledger projection validate --symbol ES --date 2026-03-12 --projection absorption_score:v1 --params '{"window_ms":500,"price_levels":3,"source_view":"visible"}' --batches 100000

Non-goals:
  Do not add API/WebSocket.
  Do not add Lens overlay.
  Do not rescan raw MBO outside batch_features.
```

---

## 24. Validation Agent Checklist

A validation agent should inspect every phase using structured checks.

### General checks

```text
cargo test passes for affected crates
workspace builds
no unrelated API/Lens changes unless phase requires them
no new unchecked panics in hot path
no direct projection-to-projection concrete imports
no study code mutates canonical book
```

### Manifest checks

```text
all registered projections have manifest
manifest id/version match factory registration
params schema validates defaults
dependencies resolve
output schema exists
execution type and lag policy are compatible
delivery semantics and frame policy are compatible
trust tier present
cost hint present
validation declarations present
```

### Runtime checks

```text
dependency graph deterministic
cycle detection works
same ProjectionKey shared by subscriptions
unsubscribed nodes cleaned or refcounted
wake skipping works
dirty dependency propagation works
generation handling works
old generation frames dropped
```

### Frame checks

```text
frame has type/op/schema/payload
frame has projection id/version/params hash
frame has generation
frame has input_cursor/emitted_cursor/batch_idx
frame has source_view
frame has temporal_policy
frame has execution_type
frame has delivery_semantics
frame has lag/stale fields
payload schema matches manifest
```

### Performance checks

```text
profile report exists
per-node runs/skips make sense
CoreSync lag is zero
InlineSync lag is zero when due
CoalescedSync frame count respects policy
AsyncLatest reports jobs and lag distribution
budgets pass or fail explicitly
```

### Temporal checks

```text
Causal projections use only <= cursor data
AsOfExternal projections reveal only as_of <= cursor
ReviewOnly projections blocked outside review mode
Async results are not painted as immediate
```

---

## 25. Failure Handling Rules

When validation fails, prefer explicit rollback or scoped fix over broad redesign.

### Manifest failure

Examples:

```text
missing field
invalid params schema
bad dependency declaration
incompatible execution/lag policy
```

Action:

```text
Fix manifest and tests before touching runtime.
```

### Dependency graph failure

Examples:

```text
cycle
missing dependency
same dependency instantiated twice with equivalent params
```

Action:

```text
Fix registry/factory dependency resolution.
Add graph test.
```

### Digest failure

Examples:

```text
same run produces different digest
frame order nondeterministic
payload includes nondeterministic field
```

Action:

```text
Remove nondeterminism from frame payload or digest input.
Sort deterministic maps.
Exclude wall-clock fields from digest.
```

### Profile failure

Examples:

```text
CoreSync over budget
InlineSync too slow
frame volume too high
```

Action:

```text
First confirm measurement.
Then optimize or change execution type if semantically valid.
Do not silently keep a slow CoreSync projection.
```

### Async lag failure

Examples:

```text
stale frames too frequent
old jobs piling up
newer input not canceling older work
```

Action:

```text
Tune wake policy, queue policy, or mark projection OfflineArtifact.
Do not let AsyncLatest backpressure replay.
```

### Temporal failure

Examples:

```text
future artifact rows visible
review-only labels shown in training mode
async result appears at input timestamp without lag metadata
```

Action:

```text
Block exposure until fixed. This is correctness, not UX polish.
```

---

## 26. Phase Dependencies Summary

```text
Phase 1 required before all later phases.
Phase 2 required before any real projection node work.
Phase 3 required before CLI run over ReplaySession.
Phase 4 required before bars/depth/studies have a run path.
Phase 5 required before DOM/chart-like visual projections are safe.
Phase 6 required before adding many experimental studies.
Phase 7 required before serious L3 studies.
Phase 8 proves extensibility.
Phase 9 required before model/heavy/offline work.
Phase 10 requires CLI run/validate to already work.
Phase 11 requires WebSocket projection frames.
Phase 12 can start after initial projection frames and session state exist.
Phase 13 benefits from batch_features and journal but can begin after Phase 8.
Phase 14 requires AsyncLatest/OfflineArtifact.
Phase 15 should wait for real profile data.
Phase 16 should wait until replay graph/API/Lens are solid.
```

---

## 27. Suggested Milestone Cuts

If this becomes too large, cut releases like this.

### Milestone A — Headless Projection Runtime

Includes:

```text
Phase 1
Phase 2
Phase 3
Phase 4
```

Outcome:

```text
Can run cursor/status/bbo/trades projections through CLI over ReplaySession.
```

### Milestone B — Base Visual Projections and Validation

Includes:

```text
Phase 5
Phase 6
```

Outcome:

```text
Can run/profile/validate bars/depth/vwap with wake and frame policies.
```

### Milestone C — First Real Studies

Includes:

```text
Phase 7
Phase 8
```

Outcome:

```text
batch_features plus first derived L3 studies are CLI-validated.
```

### Milestone D — Heavy Work Seams

Includes:

```text
Phase 9
```

Outcome:

```text
AsyncLatest and OfflineArtifact are available for expensive/model work.
```

### Milestone E — Product Exposure

Includes:

```text
Phase 10
Phase 11
```

Outcome:

```text
WebSocket projection subscriptions and initial Lens renderers work.
```

### Milestone F — Training Memory and Advanced Context

Includes:

```text
Phase 12
Phase 13
Phase 14
```

Outcome:

```text
Journal captures projection environment. Levels/gamma/model studies become natural extensions.
```

### Milestone G — Optimization and Live

Includes:

```text
Phase 15
Phase 16
```

Outcome:

```text
Seek/checkpoint/cache optimization and eventual live/replay convergence.
```

---

## 28. What Not to Do

Do not implement the study graph like this:

```text
Lens asks for /bars, /dom, /absorption, /gamma as separate hardcoded routes.
Each route computes independently.
Studies read raw MBO directly.
API contains study logic.
Lens computes trading facts locally.
ML inference runs inline without explicit execution type.
DOM sends every event batch to the browser.
Async model outputs are shown without lag/stale metadata.
Hindsight labels are mixed with training signals.
Versioning is added after the fact.
CLI validation is skipped because the chart appears to work.
```

That would recreate the problems this architecture is intended to avoid.

---

## 29. Final Target State

After these phases, Ledger should have:

```text
A typed projection contract in ledger-domain.
A lazy dependency graph runtime in ledger.
ReplaySession feeding TruthTicks into the runtime.
Base projections for cursor/status/BBO/trades/depth/bars/VWAP.
Shared batch_features for L3 studies.
Derived studies implemented as manifest-declared nodes.
Execution types and lag policies that protect core replay.
AsyncLatest and OfflineArtifact seams for expensive work.
CLI run/profile/validate commands for agentic verification.
Generic WebSocket projection subscriptions.
Lens renderers keyed by payload schema.
Journal entries tied to projection identity/version/params.
Time-aware level sets and gamma scaffolding.
Future checkpoint/cache optimization path.
```

The guiding rule remains:

```text
Build and validate the computation layer first.
Expose it to API/Lens only after CLI proves it.
Optimize only after real profile data shows where the bottlenecks are.
```
