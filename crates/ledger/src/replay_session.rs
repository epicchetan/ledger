use anyhow::{bail, ensure, Context, Result};
use chrono::NaiveDate;
use ledger_domain::{
    Bbo, EventStore, ExecutionProfile, MarketDay, ProjectionFrame, ProjectionSpec, UnixNanos,
    VisibilityProfile,
};
use ledger_replay::ReplaySimulator;
use serde::{Deserialize, Serialize};

use crate::projection::{
    base_projection_registry, ProjectionMetrics, ProjectionRegistry, ProjectionRuntime,
    ProjectionRuntimeConfig, ProjectionRuntimeCursor, ProjectionSubscription,
    ProjectionSubscriptionId, TruthTick,
};
use crate::{Ledger, ObjectStore, ReplayDataset};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplaySessionConfig {
    pub session_id: Option<String>,
    pub symbol: String,
    pub market_date: NaiveDate,
    pub start_ts_ns: Option<UnixNanos>,
    pub execution_profile: ExecutionProfile,
    pub visibility_profile: VisibilityProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayPlaybackState {
    Paused,
    Playing,
    Ended,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplaySessionState {
    pub playback: ReplayPlaybackState,
    pub speed: f64,
}

impl Default for ReplaySessionState {
    fn default() -> Self {
        Self {
            playback: ReplayPlaybackState::Paused,
            speed: 1.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplaySessionSnapshot {
    pub session_id: String,
    pub replay_dataset_id: String,
    pub market_day: MarketDay,
    pub cursor_ts_ns: String,
    pub batch_idx: usize,
    pub total_batches: usize,
    pub playback: ReplayPlaybackState,
    pub speed: f64,
    pub book_checksum: String,
    pub bbo: Option<Bbo>,
    pub frame_count: usize,
    pub fill_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplaySessionStepReport {
    pub requested_batches: usize,
    pub applied_batches: usize,
    pub snapshot: ReplaySessionSnapshot,
    pub projection_frames: Vec<ProjectionFrame>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayRunRequest {
    pub symbol: String,
    pub market_date: NaiveDate,
    pub start_ts_ns: Option<UnixNanos>,
    pub batches: usize,
    pub truth_visibility: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayRunReport {
    pub requested_batches: usize,
    pub applied_batches: usize,
    pub snapshot: ReplaySessionSnapshot,
}

pub struct ReplaySession {
    id: String,
    replay_dataset_id: String,
    market_day: MarketDay,
    total_batches: usize,
    simulator: ReplaySimulator,
    projection_runtime: ProjectionRuntime,
    state: ReplaySessionState,
}

impl ReplaySession {
    pub fn new(
        session_id: String,
        dataset: ReplayDataset,
        event_store: EventStore,
        execution_profile: ExecutionProfile,
        visibility_profile: VisibilityProfile,
    ) -> Self {
        Self::new_with_projection_registry(
            session_id,
            dataset,
            event_store,
            execution_profile,
            visibility_profile,
            base_projection_registry().expect("base projection registry must be valid"),
        )
    }

    pub fn new_with_projection_registry(
        session_id: String,
        dataset: ReplayDataset,
        event_store: EventStore,
        execution_profile: ExecutionProfile,
        visibility_profile: VisibilityProfile,
        projection_registry: ProjectionRegistry,
    ) -> Self {
        let total_batches = event_store.batches.len();
        let initial_cursor = ProjectionRuntimeCursor::new(
            0,
            event_store
                .batches
                .first()
                .map(|batch| batch.ts_event_ns)
                .unwrap_or_default(),
        );
        let projection_runtime = ProjectionRuntime::new(
            projection_registry,
            ProjectionRuntimeConfig {
                session_id: session_id.clone(),
                replay_dataset_id: dataset.replay_dataset_id.clone(),
                initial_cursor,
            },
        );
        let mut session = Self {
            id: session_id,
            replay_dataset_id: dataset.replay_dataset_id,
            market_day: dataset.market_day,
            total_batches,
            simulator: ReplaySimulator::new(event_store, execution_profile, visibility_profile),
            projection_runtime,
            state: ReplaySessionState::default(),
        };
        session.refresh_end_state();
        session
    }

    pub fn seek_to(&mut self, ts_ns: UnixNanos) -> Result<ReplaySessionSnapshot> {
        self.simulator
            .seek_to(ts_ns)
            .with_context(|| format!("seeking ReplaySession {} to {ts_ns}", self.id))?;
        self.projection_runtime
            .reset_at(ProjectionRuntimeCursor::new(
                self.simulator.batch_idx() as u64,
                self.simulator.cursor_ts_ns(),
            ))
            .with_context(|| {
                format!("resetting projection runtime for ReplaySession {}", self.id)
            })?;
        self.refresh_end_state();
        Ok(self.snapshot())
    }

    pub fn step_one_batch(&mut self) -> Result<ReplaySessionStepReport> {
        self.step_batches(1)
    }

    pub fn step_batches(&mut self, batches: usize) -> Result<ReplaySessionStepReport> {
        if self.state.playback == ReplayPlaybackState::Ended {
            return Ok(ReplaySessionStepReport {
                requested_batches: batches,
                applied_batches: 0,
                snapshot: self.snapshot(),
                projection_frames: Vec::new(),
            });
        }

        let mut applied_batches = 0;
        let mut projection_frames = Vec::new();
        for _ in 0..batches {
            if self.simulator.batch_idx() >= self.total_batches {
                self.state.playback = ReplayPlaybackState::Ended;
                break;
            }
            let replay_step = self
                .simulator
                .step_next_exchange_batch()
                .with_context(|| format!("stepping ReplaySession {}", self.id))?;
            let tick = TruthTick::from_replay_step(&replay_step);
            projection_frames.extend(
                self.projection_runtime.advance(tick).with_context(|| {
                    format!("advancing projections for ReplaySession {}", self.id)
                })?,
            );
            applied_batches += 1;
        }

        self.refresh_end_state();
        Ok(ReplaySessionStepReport {
            requested_batches: batches,
            applied_batches,
            snapshot: self.snapshot(),
            projection_frames,
        })
    }

    pub fn subscribe_projection(&mut self, spec: ProjectionSpec) -> Result<ProjectionSubscription> {
        self.projection_runtime.subscribe(spec)
    }

    pub fn unsubscribe_projection(&mut self, id: ProjectionSubscriptionId) -> Result<()> {
        self.projection_runtime.unsubscribe(id)
    }

    pub fn projection_generation(&self) -> u64 {
        self.projection_runtime.generation()
    }

    pub fn projection_metrics(&self) -> &ProjectionMetrics {
        self.projection_runtime.metrics()
    }

    pub fn pause(&mut self) -> ReplaySessionSnapshot {
        if self.state.playback != ReplayPlaybackState::Ended {
            self.state.playback = ReplayPlaybackState::Paused;
        }
        self.snapshot()
    }

    pub fn set_speed(&mut self, speed: f64) -> Result<ReplaySessionSnapshot> {
        ensure!(
            speed.is_finite() && speed > 0.0,
            "ReplaySession speed must be a positive finite value"
        );
        self.state.speed = speed;
        Ok(self.snapshot())
    }

    pub fn snapshot(&self) -> ReplaySessionSnapshot {
        ReplaySessionSnapshot {
            session_id: self.id.clone(),
            replay_dataset_id: self.replay_dataset_id.clone(),
            market_day: self.market_day.clone(),
            cursor_ts_ns: self.simulator.cursor_ts_ns().to_string(),
            batch_idx: self.simulator.batch_idx(),
            total_batches: self.total_batches,
            playback: self.state.playback,
            speed: self.state.speed,
            book_checksum: self.simulator.book().checksum(),
            bbo: self.simulator.book().bbo(),
            frame_count: self.simulator.visibility().emitted().len(),
            fill_count: self.simulator.execution().fills().len(),
        }
    }

    fn refresh_end_state(&mut self) {
        if self.simulator.batch_idx() >= self.total_batches {
            self.state.playback = ReplayPlaybackState::Ended;
        } else if self.state.playback == ReplayPlaybackState::Ended {
            self.state.playback = ReplayPlaybackState::Paused;
        }
    }
}

impl<S: ObjectStore + 'static> Ledger<S> {
    pub async fn open_replay_session(&self, config: ReplaySessionConfig) -> Result<ReplaySession> {
        let dataset = self
            .load_cached_replay_dataset(&config.symbol, config.market_date)
            .await
            .with_context(|| {
                format!(
                    "loading ReplayDataset for {} {}",
                    config.symbol, config.market_date
                )
            })?;
        let session_id = config.session_id.unwrap_or_else(|| {
            format!(
                "replay-{}-{}",
                dataset.market_day.contract_symbol, dataset.market_day.market_date
            )
        });
        let event_store = dataset.event_store().await?;
        let mut session = ReplaySession::new_with_projection_registry(
            session_id,
            dataset,
            event_store,
            config.execution_profile,
            config.visibility_profile,
            self.projection_registry().clone(),
        );
        if let Some(start_ts_ns) = config.start_ts_ns {
            session.seek_to(start_ts_ns)?;
        }
        Ok(session)
    }

    pub async fn run_replay_session(&self, request: ReplayRunRequest) -> Result<ReplayRunReport> {
        if request.batches == 0 {
            bail!("ReplaySession run batches must be greater than zero");
        }

        let visibility_profile = if request.truth_visibility {
            VisibilityProfile::truth()
        } else {
            VisibilityProfile::default()
        };
        let mut session = self
            .open_replay_session(ReplaySessionConfig {
                session_id: Some("local-run".to_string()),
                symbol: request.symbol,
                market_date: request.market_date,
                start_ts_ns: request.start_ts_ns,
                execution_profile: ExecutionProfile::default(),
                visibility_profile,
            })
            .await?;
        let step = session.step_batches(request.batches)?;

        Ok(ReplayRunReport {
            requested_batches: step.requested_batches,
            applied_batches: step.applied_batches,
            snapshot: step.snapshot,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::{
        ProjectionAdvance, ProjectionContext, ProjectionFactory, ProjectionFrameDraft,
        ProjectionNode,
    };
    use chrono::NaiveDate;
    use ledger_domain::{
        build_batches, build_trade_index, BookAction, BookSide, MboEvent, PriceTicks,
        ProjectionDeliverySemantics, ProjectionFramePolicy, ProjectionId, ProjectionKey,
        ProjectionKind, ProjectionManifest, ProjectionOutputSchema, ProjectionUpdateMode,
        ProjectionVersion, ProjectionWakeEventMask, ProjectionWakePolicy, SourceView,
        TemporalPolicy,
    };
    use ledger_replay::ReplaySimulator;
    use serde_json::{json, Value};

    fn add(ts: u64, seq: u64, side: BookSide, price: i64, size: u32, order_id: u64) -> MboEvent {
        MboEvent::synthetic(
            ts,
            seq,
            BookAction::Add,
            Some(side),
            Some(PriceTicks(price)),
            size,
            order_id,
            true,
        )
    }

    fn trade(ts: u64, seq: u64, aggressor: BookSide, price: i64, size: u32) -> MboEvent {
        MboEvent::synthetic(
            ts,
            seq,
            BookAction::Trade,
            Some(aggressor),
            Some(PriceTicks(price)),
            size,
            0,
            true,
        )
    }

    fn event_store(events: Vec<MboEvent>) -> EventStore {
        EventStore {
            batches: build_batches(&events),
            trades: build_trade_index(&events),
            events,
        }
    }

    fn replay_dataset() -> ReplayDataset {
        ReplayDataset {
            replay_dataset_id: "test-replay-dataset".to_string(),
            market_day: MarketDay::resolve_es(
                "ESH6",
                NaiveDate::from_ymd_opt(2026, 3, 12).unwrap(),
            )
            .unwrap(),
            events_path: "events.v1.bin".into(),
            batches_path: "batches.v1.bin".into(),
            trades_path: "trades.v1.bin".into(),
            book_check_path: "book_check.v1.json".into(),
        }
    }

    fn replay_session(store: EventStore) -> ReplaySession {
        ReplaySession::new(
            "test-session".to_string(),
            replay_dataset(),
            store,
            ExecutionProfile::default(),
            VisibilityProfile::truth(),
        )
    }

    fn replay_session_with_registry(
        store: EventStore,
        registry: ProjectionRegistry,
    ) -> ReplaySession {
        ReplaySession::new_with_projection_registry(
            "test-session".to_string(),
            replay_dataset(),
            store,
            ExecutionProfile::default(),
            VisibilityProfile::truth(),
            registry,
        )
    }

    fn tick_echo_manifest(wake_policy: ProjectionWakePolicy) -> ProjectionManifest {
        ProjectionManifest {
            id: ProjectionId::new("tick_echo").unwrap(),
            version: ProjectionVersion::new(1).unwrap(),
            name: "tick_echo".to_string(),
            description: "Test projection that echoes TruthTick facts.".to_string(),
            kind: ProjectionKind::Base,
            params_schema: json!({ "type": "object" }),
            default_params: json!({}),
            dependencies: vec![],
            output_schema: ProjectionOutputSchema::new("tick_echo_v1").unwrap(),
            update_mode: ProjectionUpdateMode::Online,
            source_view: Some(SourceView::ExchangeTruth),
            temporal_policy: TemporalPolicy::Causal,
            wake_policy,
            delivery_semantics: ProjectionDeliverySemantics::ReplaceLatest,
            frame_policy: ProjectionFramePolicy::EmitEveryUpdate,
        }
    }

    #[derive(Clone)]
    struct TickEchoFactory {
        manifest: ProjectionManifest,
    }

    impl TickEchoFactory {
        fn new(wake_policy: ProjectionWakePolicy) -> Self {
            Self {
                manifest: tick_echo_manifest(wake_policy),
            }
        }
    }

    impl ProjectionFactory for TickEchoFactory {
        fn manifest(&self) -> &ProjectionManifest {
            &self.manifest
        }

        fn resolve_dependencies(&self, _params: &Value) -> Result<Vec<ProjectionSpec>> {
            Ok(vec![])
        }

        fn build(
            &self,
            _spec: ProjectionSpec,
            key: ProjectionKey,
        ) -> Result<Box<dyn ProjectionNode>> {
            Ok(Box::new(TickEchoNode {
                key,
                payload: json!({ "batch_idx": 0, "cursor_ts_ns": "0" }),
                pending: Vec::new(),
            }))
        }
    }

    struct TickEchoNode {
        key: ProjectionKey,
        payload: Value,
        pending: Vec<ProjectionFrameDraft>,
    }

    impl ProjectionNode for TickEchoNode {
        fn key(&self) -> &ProjectionKey {
            &self.key
        }

        fn advance(&mut self, ctx: &ProjectionContext<'_>) -> Result<ProjectionAdvance> {
            let tick = ctx.tick();
            self.payload = json!({
                "batch_idx": tick.batch_idx,
                "applied_batch_idx": tick.applied_batch_idx,
                "cursor_ts_ns": tick.cursor_ts_ns.to_string(),
                "exchange_events": tick.flags.exchange_events,
                "trades": tick.flags.trades,
                "bbo_changed": tick.flags.bbo_changed,
                "visibility_frame": tick.flags.visibility_frame,
                "fill_event": tick.flags.fill_event,
                "event_count": tick.exchange.event_count,
                "trade_count": tick.exchange.trade_count,
            });
            self.pending
                .push(ProjectionFrameDraft::replace(self.payload.clone()));
            Ok(ProjectionAdvance::StateChanged)
        }

        fn snapshot(&self) -> Value {
            self.payload.clone()
        }

        fn drain_frames(&mut self) -> Result<Vec<ProjectionFrameDraft>> {
            Ok(std::mem::take(&mut self.pending))
        }

        fn reset(&mut self) -> Result<()> {
            self.payload = json!({ "batch_idx": 0, "cursor_ts_ns": "0" });
            self.pending.clear();
            Ok(())
        }
    }

    fn registry_with_tick_echo(wake_policy: ProjectionWakePolicy) -> ProjectionRegistry {
        let mut registry = ProjectionRegistry::new();
        registry
            .register(TickEchoFactory::new(wake_policy))
            .unwrap();
        registry
    }

    fn tick_echo_spec() -> ProjectionSpec {
        ProjectionSpec::new("tick_echo", 1, json!({})).unwrap()
    }

    #[test]
    fn replay_session_steps_and_reports_snapshots() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let mut session = replay_session(store);

        let initial = session.snapshot();
        assert_eq!(initial.session_id, "test-session");
        assert_eq!(initial.cursor_ts_ns, "100");
        assert_eq!(initial.batch_idx, 0);
        assert_eq!(initial.total_batches, 2);
        assert_eq!(initial.playback, ReplayPlaybackState::Paused);
        assert_eq!(initial.bbo, None);

        let first = session.step_one_batch().unwrap();
        assert_eq!(first.requested_batches, 1);
        assert_eq!(first.applied_batches, 1);
        assert!(first.projection_frames.is_empty());
        assert_eq!(first.snapshot.cursor_ts_ns, "100");
        assert_eq!(first.snapshot.batch_idx, 1);
        assert_eq!(first.snapshot.frame_count, 1);
        assert_eq!(first.snapshot.playback, ReplayPlaybackState::Paused);
        assert_eq!(first.snapshot.bbo.unwrap().bid_price, Some(PriceTicks(100)));

        let remaining = session.step_batches(10).unwrap();
        assert_eq!(remaining.requested_batches, 10);
        assert_eq!(remaining.applied_batches, 1);
        assert_eq!(remaining.snapshot.cursor_ts_ns, "200");
        assert_eq!(remaining.snapshot.batch_idx, 2);
        assert_eq!(remaining.snapshot.frame_count, 2);
        assert_eq!(remaining.snapshot.playback, ReplayPlaybackState::Ended);
        assert_eq!(
            remaining.snapshot.bbo.unwrap().ask_price,
            Some(PriceTicks(101))
        );

        let exhausted = session.step_one_batch().unwrap();
        assert_eq!(exhausted.applied_batches, 0);
        assert!(exhausted.projection_frames.is_empty());
        assert_eq!(exhausted.snapshot.playback, ReplayPlaybackState::Ended);
    }

    #[test]
    fn replay_session_projection_subscription_receives_tick_frames() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let registry = registry_with_tick_echo(ProjectionWakePolicy::EveryTick);
        let mut session = replay_session_with_registry(store, registry);

        let subscription = session.subscribe_projection(tick_echo_spec()).unwrap();
        assert_eq!(subscription.initial_frames.len(), 1);
        assert_eq!(
            subscription.initial_frames[0].stamp.session_id,
            "test-session"
        );
        assert_eq!(
            subscription.initial_frames[0].stamp.replay_dataset_id,
            "test-replay-dataset"
        );
        assert_eq!(subscription.initial_frames[0].stamp.batch_idx, 0);
        assert_eq!(subscription.initial_frames[0].stamp.cursor_ts_ns, "100");

        let step = session.step_one_batch().unwrap();

        assert_eq!(step.applied_batches, 1);
        assert_eq!(step.projection_frames.len(), 1);
        let frame = &step.projection_frames[0];
        assert_eq!(frame.stamp.session_id, "test-session");
        assert_eq!(frame.stamp.replay_dataset_id, "test-replay-dataset");
        assert_eq!(frame.stamp.generation, 0);
        assert_eq!(frame.stamp.batch_idx, 1);
        assert_eq!(frame.stamp.cursor_ts_ns, "100");
        assert_eq!(frame.payload["batch_idx"], 1);
        assert_eq!(frame.payload["applied_batch_idx"], 0);
        assert_eq!(frame.payload["cursor_ts_ns"], "100");
        assert_eq!(frame.payload["exchange_events"], true);
    }

    #[test]
    fn projection_subscription_after_step_uses_current_cursor() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let registry = registry_with_tick_echo(ProjectionWakePolicy::EveryTick);
        let mut session = replay_session_with_registry(store, registry);
        session.step_one_batch().unwrap();

        let subscription = session.subscribe_projection(tick_echo_spec()).unwrap();

        assert_eq!(subscription.initial_frames.len(), 1);
        assert_eq!(subscription.initial_frames[0].stamp.batch_idx, 1);
        assert_eq!(subscription.initial_frames[0].stamp.cursor_ts_ns, "100");
    }

    #[test]
    fn truth_tick_flags_match_trade_batch() {
        let store = event_store(vec![trade(100, 1, BookSide::Ask, 100, 3)]);
        let registry =
            registry_with_tick_echo(ProjectionWakePolicy::OnEventMask(ProjectionWakeEventMask {
                trades: true,
                ..Default::default()
            }));
        let mut session = replay_session_with_registry(store, registry);
        session.subscribe_projection(tick_echo_spec()).unwrap();

        let step = session.step_one_batch().unwrap();

        assert_eq!(step.projection_frames.len(), 1);
        assert_eq!(step.projection_frames[0].payload["trades"], true);
        assert_eq!(step.projection_frames[0].payload["trade_count"], 1);
        assert_eq!(step.projection_frames[0].payload["bbo_changed"], false);
    }

    #[test]
    fn truth_tick_flags_match_bbo_change() {
        let store = event_store(vec![add(100, 1, BookSide::Bid, 100, 2, 1)]);
        let registry =
            registry_with_tick_echo(ProjectionWakePolicy::OnEventMask(ProjectionWakeEventMask {
                bbo_changed: true,
                ..Default::default()
            }));
        let mut session = replay_session_with_registry(store, registry);
        session.subscribe_projection(tick_echo_spec()).unwrap();

        let step = session.step_one_batch().unwrap();

        assert_eq!(step.projection_frames.len(), 1);
        assert_eq!(step.projection_frames[0].payload["bbo_changed"], true);
        assert_eq!(step.projection_frames[0].payload["event_count"], 1);
    }

    #[test]
    fn seek_increments_projection_generation() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let registry = registry_with_tick_echo(ProjectionWakePolicy::EveryTick);
        let mut session = replay_session_with_registry(store, registry);
        session.subscribe_projection(tick_echo_spec()).unwrap();

        let first = session.step_one_batch().unwrap();
        assert_eq!(first.projection_frames[0].stamp.generation, 0);

        session.seek_to(100).unwrap();
        assert_eq!(session.projection_generation(), 1);

        let second = session.step_one_batch().unwrap();
        assert_eq!(second.snapshot.cursor_ts_ns, "200");
        assert_eq!(second.projection_frames.len(), 1);
        assert_eq!(second.projection_frames[0].stamp.generation, 1);
        assert_eq!(second.projection_frames[0].stamp.batch_idx, 2);
        assert_eq!(second.projection_frames[0].stamp.cursor_ts_ns, "200");
    }

    #[test]
    fn replay_session_seek_rebuilds_from_start() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let mut session = replay_session(store);
        session.step_batches(10).unwrap();
        assert_eq!(session.snapshot().playback, ReplayPlaybackState::Ended);

        let snapshot = session.seek_to(100).unwrap();

        assert_eq!(snapshot.cursor_ts_ns, "100");
        assert_eq!(snapshot.batch_idx, 1);
        assert_eq!(snapshot.playback, ReplayPlaybackState::Paused);
        assert_eq!(snapshot.bbo.unwrap().bid_price, Some(PriceTicks(100)));
    }

    #[test]
    fn replay_session_snapshot_matches_direct_simulator_checksum() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let mut simulator = ReplaySimulator::new(
            store.clone(),
            ExecutionProfile::default(),
            VisibilityProfile::truth(),
        );
        simulator.step_next_exchange_batch().unwrap();
        simulator.step_next_exchange_batch().unwrap();

        let mut session = replay_session(store);
        let snapshot = session.step_batches(2).unwrap().snapshot;

        assert_eq!(snapshot.book_checksum, simulator.book().checksum());
        assert_eq!(snapshot.bbo, simulator.book().bbo());
    }

    #[test]
    fn replay_session_rejects_invalid_speed() {
        let store = event_store(vec![add(100, 1, BookSide::Bid, 100, 2, 1)]);
        let mut session = replay_session(store);

        let err = session.set_speed(0.0).unwrap_err();

        assert!(format!("{err:#}").contains("positive finite"));
    }
}
