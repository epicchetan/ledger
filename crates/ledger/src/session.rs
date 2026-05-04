use anyhow::{bail, ensure, Context, Result};
use chrono::NaiveDate;
use ledger_domain::{
    Bbo, EventStore, ExecutionProfile, MarketDay, ProjectionFrame, ProjectionSpec, UnixNanos,
    VisibilityProfile,
};
use ledger_replay::{ReplayFeed, ReplayFeedConfig, ReplayFeedMode, ReplaySimulator};
use serde::{Deserialize, Serialize};

use crate::projection::{
    base_projection_registry, projection_frame_digest, ProjectionMetrics, ProjectionRegistry,
    ProjectionRuntime, ProjectionRuntimeConfig, ProjectionRuntimeCursor, ProjectionSubscription,
    ProjectionSubscriptionId, SessionTick,
};
use crate::{Ledger, ObjectStore, ReplayDataset};

const NANOS_PER_MILLI: u64 = 1_000_000;

pub type MonotonicNanos = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenSessionRequest {
    pub session_id: Option<String>,
    pub symbol: String,
    pub market_date: NaiveDate,
    pub start_ts_ns: Option<UnixNanos>,
    pub feed: SessionFeedConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionFeedConfig {
    Replay(ReplayFeedConfig),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPlaybackState {
    Paused,
    Playing,
    Ended,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionState {
    pub playback: SessionPlaybackState,
    pub speed: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock: Option<SessionClock>,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            playback: SessionPlaybackState::Paused,
            speed: 1.0,
            clock: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionClock {
    pub anchor_mono_ns: MonotonicNanos,
    pub anchor_feed_ts_ns: UnixNanos,
}

impl SessionClock {
    pub fn target_feed_ts_ns(self, now_mono_ns: MonotonicNanos, speed: f64) -> Result<UnixNanos> {
        ensure!(
            now_mono_ns >= self.anchor_mono_ns,
            "session clock cannot move backwards"
        );
        ensure!(
            speed.is_finite() && speed > 0.0,
            "Session speed must be a positive finite value"
        );
        let elapsed_mono_ns = now_mono_ns - self.anchor_mono_ns;
        let elapsed_feed_ns = (elapsed_mono_ns as f64 * speed).round();
        ensure!(
            elapsed_feed_ns.is_finite() && elapsed_feed_ns >= 0.0,
            "session clock produced invalid feed delta"
        );
        ensure!(
            elapsed_feed_ns <= u64::MAX as f64,
            "session clock feed delta overflowed"
        );
        self.anchor_feed_ts_ns
            .checked_add(elapsed_feed_ns as u64)
            .context("session clock target feed timestamp overflowed")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedAdvanceBudget {
    pub max_batches: usize,
}

impl FeedAdvanceBudget {
    pub fn new(max_batches: usize) -> Result<Self> {
        Self { max_batches }.validate()?;
        Ok(Self { max_batches })
    }

    fn validate(self) -> Result<()> {
        ensure!(
            self.max_batches > 0,
            "feed advance budget max_batches must be greater than zero"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub session_id: String,
    pub replay_dataset_id: String,
    pub market_day: MarketDay,
    pub feed_seq: u64,
    pub feed_ts_ns: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_feed_ts_ns: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_first_ts_ns: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_last_ts_ns: Option<String>,
    pub batch_idx: usize,
    pub total_batches: usize,
    pub playback: SessionPlaybackState,
    pub speed: f64,
    pub book_checksum: String,
    pub bbo: Option<Bbo>,
    pub frame_count: usize,
    pub fill_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionAdvanceReport {
    pub requested_batches: usize,
    pub applied_batches: usize,
    pub snapshot: SessionSnapshot,
    pub projection_frames: Vec<ProjectionFrame>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionClockPumpReport {
    pub target_feed_ts_ns: String,
    pub applied_batches: usize,
    pub budget_exhausted: bool,
    pub behind: bool,
    pub snapshot: SessionSnapshot,
    pub projection_frames: Vec<ProjectionFrame>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSeekReport {
    pub requested_ts_ns: String,
    pub snapshot: SessionSnapshot,
    pub projection_frames: Vec<ProjectionFrame>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionRunRequest {
    pub symbol: String,
    pub market_date: NaiveDate,
    pub start_ts_ns: Option<UnixNanos>,
    pub batches: usize,
    pub truth_visibility: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionRunReport {
    pub requested_batches: usize,
    pub applied_batches: usize,
    pub snapshot: SessionSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionClockRunRequest {
    pub symbol: String,
    pub market_date: NaiveDate,
    pub start_ts_ns: Option<UnixNanos>,
    pub projection: ProjectionSpec,
    pub speed: f64,
    pub tick_ms: u64,
    pub ticks: usize,
    pub budget_batches: usize,
    pub digest: bool,
    pub truth_visibility: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionClockRunReport {
    pub projection: ProjectionRunProjectionSummary,
    pub dataset: ProjectionRunDatasetSummary,
    pub clock: SessionClockRunClockSummary,
    pub run: SessionClockRunSummary,
    pub snapshot: SessionSnapshot,
    pub passed: bool,
    #[serde(skip)]
    pub frames: Vec<ProjectionFrame>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionClockRunClockSummary {
    pub speed: f64,
    pub tick_ms: u64,
    pub ticks: usize,
    pub budget_batches: usize,
    pub simulated_mono_ns: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionClockRunSummary {
    pub applied_batches: usize,
    pub pump_count: usize,
    pub budget_exhaustions: usize,
    pub behind_ticks: usize,
    pub frames: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_feed_ts_ns: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_feed_ts_ns: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionRunRequest {
    pub symbol: String,
    pub market_date: NaiveDate,
    pub start_ts_ns: Option<UnixNanos>,
    pub projection: ProjectionSpec,
    pub batches: usize,
    pub digest: bool,
    pub truth_visibility: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionRunReport {
    pub projection: ProjectionRunProjectionSummary,
    pub dataset: ProjectionRunDatasetSummary,
    pub run: ProjectionRunSummary,
    pub passed: bool,
    #[serde(skip)]
    pub frames: Vec<ProjectionFrame>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionRunProjectionSummary {
    pub id: String,
    pub version: u16,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionRunDatasetSummary {
    pub symbol: String,
    pub market_date: String,
    pub replay_dataset_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionRunSummary {
    pub requested_batches: usize,
    pub applied_batches: usize,
    pub frames: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_feed_ts_ns: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_feed_ts_ns: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

pub struct Session {
    id: String,
    replay_dataset_id: String,
    market_day: MarketDay,
    feed: SessionFeed,
    projection_runtime: ProjectionRuntime,
    state: SessionState,
}

pub enum SessionFeed {
    Replay(ReplayFeed),
}

impl Session {
    pub fn from_replay_dataset(
        session_id: String,
        dataset: ReplayDataset,
        event_store: EventStore,
        feed_config: ReplayFeedConfig,
    ) -> Self {
        Self::from_replay_dataset_with_projection_registry(
            session_id,
            dataset,
            event_store,
            feed_config,
            base_projection_registry().expect("base projection registry must be valid"),
        )
    }

    pub fn from_replay_dataset_with_projection_registry(
        session_id: String,
        dataset: ReplayDataset,
        event_store: EventStore,
        feed_config: ReplayFeedConfig,
        projection_registry: ProjectionRegistry,
    ) -> Self {
        let replay_feed = ReplayFeed::new(
            ReplaySimulator::new(
                event_store,
                feed_config.execution_profile,
                feed_config.visibility_profile,
            ),
            feed_config.mode,
        );
        let feed_snapshot = replay_feed.snapshot();
        let initial_cursor = ProjectionRuntimeCursor::with_feed(
            feed_snapshot.feed_seq,
            feed_snapshot.feed_ts_ns,
            feed_snapshot.source_first_ts_ns,
            feed_snapshot.source_last_ts_ns,
            feed_snapshot.batch_idx as u64,
            feed_snapshot.feed_ts_ns,
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
            feed: SessionFeed::Replay(replay_feed),
            projection_runtime,
            state: SessionState::default(),
        };
        session.refresh_end_state();
        session
    }

    pub fn seek_to(&mut self, ts_ns: UnixNanos) -> Result<SessionSeekReport> {
        let feed_snapshot = match &mut self.feed {
            SessionFeed::Replay(feed) => feed
                .seek_to(ts_ns)
                .with_context(|| format!("seeking replay feed for Session {}", self.id))?,
        };
        let projection_frames = self
            .projection_runtime
            .reset_at(ProjectionRuntimeCursor::with_feed(
                feed_snapshot.feed_seq,
                feed_snapshot.feed_ts_ns,
                feed_snapshot.source_first_ts_ns,
                feed_snapshot.source_last_ts_ns,
                feed_snapshot.batch_idx as u64,
                feed_snapshot.feed_ts_ns,
            ))
            .with_context(|| format!("resetting projection runtime for Session {}", self.id))?;
        self.state.clock = None;
        if self.state.playback != SessionPlaybackState::Ended {
            self.state.playback = SessionPlaybackState::Paused;
        }
        self.refresh_end_state();
        Ok(SessionSeekReport {
            requested_ts_ns: ts_ns.to_string(),
            snapshot: self.snapshot(),
            projection_frames,
        })
    }

    pub fn advance_one_feed_batch(&mut self) -> Result<SessionAdvanceReport> {
        self.advance_feed_batches(1)
    }

    pub fn advance_feed_batches(&mut self, batches: usize) -> Result<SessionAdvanceReport> {
        if self.state.playback == SessionPlaybackState::Ended {
            return Ok(SessionAdvanceReport {
                requested_batches: batches,
                applied_batches: 0,
                snapshot: self.snapshot(),
                projection_frames: Vec::new(),
            });
        }

        let mut applied_batches = 0;
        let mut projection_frames = Vec::new();
        for _ in 0..batches {
            let feed_batch = match &mut self.feed {
                SessionFeed::Replay(feed) => feed
                    .advance_one()
                    .with_context(|| format!("advancing replay feed for Session {}", self.id))?,
            };
            let Some(feed_batch) = feed_batch else {
                self.state.playback = SessionPlaybackState::Ended;
                break;
            };
            let tick = SessionTick::from_replay_feed_batch(&feed_batch);
            projection_frames.extend(
                self.projection_runtime
                    .advance(tick)
                    .with_context(|| format!("advancing projections for Session {}", self.id))?,
            );
            applied_batches += 1;
        }

        self.refresh_end_state();
        Ok(SessionAdvanceReport {
            requested_batches: batches,
            applied_batches,
            snapshot: self.snapshot(),
            projection_frames,
        })
    }

    pub fn advance_until_feed_time(
        &mut self,
        target_feed_ts_ns: UnixNanos,
        budget: FeedAdvanceBudget,
    ) -> Result<SessionClockPumpReport> {
        budget.validate()?;
        let mut applied_batches = 0;
        let mut projection_frames = Vec::new();

        while applied_batches < budget.max_batches
            && self
                .next_feed_ts_ns()
                .is_some_and(|next_feed_ts_ns| next_feed_ts_ns <= target_feed_ts_ns)
            && self.state.playback != SessionPlaybackState::Ended
        {
            let report = self.advance_one_feed_batch()?;
            if report.applied_batches == 0 {
                break;
            }
            applied_batches += report.applied_batches;
            projection_frames.extend(report.projection_frames);
        }

        let due_after_budget = self
            .next_feed_ts_ns()
            .is_some_and(|next_feed_ts_ns| next_feed_ts_ns <= target_feed_ts_ns)
            && self.state.playback != SessionPlaybackState::Ended;
        let budget_exhausted = applied_batches == budget.max_batches && due_after_budget;

        Ok(SessionClockPumpReport {
            target_feed_ts_ns: target_feed_ts_ns.to_string(),
            applied_batches,
            budget_exhausted,
            behind: due_after_budget,
            snapshot: self.snapshot(),
            projection_frames,
        })
    }

    pub fn pump_clock(
        &mut self,
        now_mono_ns: MonotonicNanos,
        budget: FeedAdvanceBudget,
    ) -> Result<SessionClockPumpReport> {
        if self.state.playback != SessionPlaybackState::Playing {
            let target_feed_ts_ns = self.feed_ts_ns();
            return Ok(SessionClockPumpReport {
                target_feed_ts_ns: target_feed_ts_ns.to_string(),
                applied_batches: 0,
                budget_exhausted: false,
                behind: false,
                snapshot: self.snapshot(),
                projection_frames: Vec::new(),
            });
        }

        let clock = self
            .state
            .clock
            .context("playing Session has no active clock")?;
        let target_feed_ts_ns = clock.target_feed_ts_ns(now_mono_ns, self.state.speed)?;
        self.advance_until_feed_time(target_feed_ts_ns, budget)
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

    pub fn play(&mut self, speed: f64, now_mono_ns: MonotonicNanos) -> Result<SessionSnapshot> {
        ensure!(
            speed.is_finite() && speed > 0.0,
            "Session speed must be a positive finite value"
        );
        if self.state.playback != SessionPlaybackState::Ended {
            self.state.speed = speed;
            self.state.clock = Some(SessionClock {
                anchor_mono_ns: now_mono_ns,
                anchor_feed_ts_ns: self.feed_ts_ns(),
            });
            self.state.playback = SessionPlaybackState::Playing;
        }
        Ok(self.snapshot())
    }

    pub fn pause(&mut self, now_mono_ns: MonotonicNanos) -> SessionSnapshot {
        if self.state.playback != SessionPlaybackState::Ended {
            self.state.clock = Some(SessionClock {
                anchor_mono_ns: now_mono_ns,
                anchor_feed_ts_ns: self.feed_ts_ns(),
            });
            self.state.playback = SessionPlaybackState::Paused;
        }
        self.snapshot()
    }

    pub fn set_speed(
        &mut self,
        speed: f64,
        now_mono_ns: MonotonicNanos,
    ) -> Result<SessionSnapshot> {
        ensure!(
            speed.is_finite() && speed > 0.0,
            "Session speed must be a positive finite value"
        );
        self.state.speed = speed;
        if self.state.playback == SessionPlaybackState::Playing {
            self.state.clock = Some(SessionClock {
                anchor_mono_ns: now_mono_ns,
                anchor_feed_ts_ns: self.feed_ts_ns(),
            });
        }
        Ok(self.snapshot())
    }

    pub fn snapshot(&self) -> SessionSnapshot {
        let SessionFeed::Replay(feed) = &self.feed;
        let feed_snapshot = feed.snapshot();

        SessionSnapshot {
            session_id: self.id.clone(),
            replay_dataset_id: self.replay_dataset_id.clone(),
            market_day: self.market_day.clone(),
            feed_seq: feed_snapshot.feed_seq,
            feed_ts_ns: feed_snapshot.feed_ts_ns.to_string(),
            next_feed_ts_ns: feed_snapshot
                .next_feed_ts_ns
                .map(|next_feed_ts_ns| next_feed_ts_ns.to_string()),
            source_first_ts_ns: feed_snapshot
                .source_first_ts_ns
                .map(|source_first_ts_ns| source_first_ts_ns.to_string()),
            source_last_ts_ns: feed_snapshot
                .source_last_ts_ns
                .map(|source_last_ts_ns| source_last_ts_ns.to_string()),
            batch_idx: feed_snapshot.batch_idx,
            total_batches: feed_snapshot.total_batches,
            playback: self.state.playback,
            speed: self.state.speed,
            book_checksum: feed_snapshot.book_checksum,
            bbo: feed_snapshot.bbo,
            frame_count: feed_snapshot.visibility_frame_count,
            fill_count: feed_snapshot.fill_count,
        }
    }

    fn refresh_end_state(&mut self) {
        let ended = match &self.feed {
            SessionFeed::Replay(feed) => feed.ended(),
        };
        if ended {
            self.state.playback = SessionPlaybackState::Ended;
        } else if self.state.playback == SessionPlaybackState::Ended {
            self.state.playback = SessionPlaybackState::Paused;
        }
    }

    fn feed_ts_ns(&self) -> UnixNanos {
        let SessionFeed::Replay(feed) = &self.feed;
        feed.snapshot().feed_ts_ns
    }

    fn next_feed_ts_ns(&self) -> Option<UnixNanos> {
        match &self.feed {
            SessionFeed::Replay(feed) => feed.next_feed_ts_ns(),
        }
    }
}

impl<S: ObjectStore + 'static> Ledger<S> {
    pub async fn open_session(&self, request: OpenSessionRequest) -> Result<Session> {
        let dataset = self
            .load_cached_replay_dataset(&request.symbol, request.market_date)
            .await
            .with_context(|| {
                format!(
                    "loading ReplayDataset for {} {}",
                    request.symbol, request.market_date
                )
            })?;
        let session_id = request.session_id.unwrap_or_else(|| {
            format!(
                "replay-{}-{}",
                dataset.market_day.contract_symbol, dataset.market_day.market_date
            )
        });
        let event_store = dataset.event_store().await?;
        let SessionFeedConfig::Replay(feed_config) = request.feed;
        let mut session = Session::from_replay_dataset_with_projection_registry(
            session_id,
            dataset,
            event_store,
            feed_config,
            self.projection_registry().clone(),
        );
        if let Some(start_ts_ns) = request.start_ts_ns {
            session.seek_to(start_ts_ns)?;
        }
        Ok(session)
    }

    pub async fn run_session(&self, request: SessionRunRequest) -> Result<SessionRunReport> {
        if request.batches == 0 {
            bail!("Session run batches must be greater than zero");
        }

        let visibility_profile = if request.truth_visibility {
            VisibilityProfile::truth()
        } else {
            VisibilityProfile::default()
        };
        let mut session = self
            .open_session(OpenSessionRequest {
                session_id: Some("local-run".to_string()),
                symbol: request.symbol,
                market_date: request.market_date,
                start_ts_ns: request.start_ts_ns,
                feed: SessionFeedConfig::Replay(ReplayFeedConfig {
                    mode: ReplayFeedMode::ExchangeTruth,
                    execution_profile: ExecutionProfile::default(),
                    visibility_profile,
                }),
            })
            .await?;
        let step = session.advance_feed_batches(request.batches)?;

        Ok(SessionRunReport {
            requested_batches: step.requested_batches,
            applied_batches: step.applied_batches,
            snapshot: step.snapshot,
        })
    }

    pub async fn run_session_clock(
        &self,
        request: SessionClockRunRequest,
    ) -> Result<SessionClockRunReport> {
        if request.ticks == 0 {
            bail!("Session clock run ticks must be greater than zero");
        }
        if request.tick_ms == 0 {
            bail!("Session clock run tick-ms must be greater than zero");
        }
        let budget = FeedAdvanceBudget::new(request.budget_batches)?;

        let visibility_profile = if request.truth_visibility {
            VisibilityProfile::truth()
        } else {
            VisibilityProfile::default()
        };
        let mut session = self
            .open_session(OpenSessionRequest {
                session_id: Some("clock-run".to_string()),
                symbol: request.symbol.clone(),
                market_date: request.market_date,
                start_ts_ns: request.start_ts_ns,
                feed: SessionFeedConfig::Replay(ReplayFeedConfig {
                    mode: ReplayFeedMode::ExchangeTruth,
                    execution_profile: ExecutionProfile::default(),
                    visibility_profile,
                }),
            })
            .await?;

        let subscription = session
            .subscribe_projection(request.projection.clone())
            .context("subscribing projection for CLI clock run")?;
        let projection_key = subscription.key.clone();
        let mut frames = subscription
            .initial_frames
            .into_iter()
            .filter(|frame| frame.stamp.projection_key == projection_key)
            .collect::<Vec<_>>();

        session.play(request.speed, 0)?;
        let tick_ns = request
            .tick_ms
            .checked_mul(NANOS_PER_MILLI)
            .context("Session clock run tick-ms overflows nanoseconds")?;
        let mut applied_batches = 0;
        let mut pump_count = 0;
        let mut budget_exhaustions = 0;
        let mut behind_ticks = 0;
        let mut simulated_mono_ns = 0;

        for tick_idx in 1..=request.ticks {
            simulated_mono_ns = (tick_idx as u64)
                .checked_mul(tick_ns)
                .context("Session clock run simulated monotonic time overflowed")?;
            let pump = session.pump_clock(simulated_mono_ns, budget)?;
            pump_count += 1;
            applied_batches += pump.applied_batches;
            if pump.budget_exhausted {
                budget_exhaustions += 1;
            }
            if pump.behind {
                behind_ticks += 1;
            }
            frames.extend(
                pump.projection_frames
                    .into_iter()
                    .filter(|frame| frame.stamp.projection_key == projection_key),
            );
            if pump.snapshot.playback == SessionPlaybackState::Ended {
                break;
            }
        }

        let first_feed_ts_ns = frames.first().map(|frame| frame.stamp.feed_ts_ns.clone());
        let last_feed_ts_ns = frames.last().map(|frame| frame.stamp.feed_ts_ns.clone());
        let digest = if request.digest {
            Some(projection_frame_digest(&frames)?)
        } else {
            None
        };
        let snapshot = session.snapshot();

        Ok(SessionClockRunReport {
            projection: ProjectionRunProjectionSummary {
                id: projection_key.id.as_str().to_string(),
                version: projection_key.version.get(),
                key: projection_key.to_string(),
            },
            dataset: ProjectionRunDatasetSummary {
                symbol: snapshot.market_day.contract_symbol.clone(),
                market_date: snapshot.market_day.market_date.to_string(),
                replay_dataset_id: snapshot.replay_dataset_id.clone(),
            },
            clock: SessionClockRunClockSummary {
                speed: request.speed,
                tick_ms: request.tick_ms,
                ticks: request.ticks,
                budget_batches: request.budget_batches,
                simulated_mono_ns: simulated_mono_ns.to_string(),
            },
            run: SessionClockRunSummary {
                applied_batches,
                pump_count,
                budget_exhaustions,
                behind_ticks,
                frames: frames.len(),
                first_feed_ts_ns,
                last_feed_ts_ns,
                digest,
            },
            snapshot,
            passed: true,
            frames,
        })
    }

    pub async fn run_projection(
        &self,
        request: ProjectionRunRequest,
    ) -> Result<ProjectionRunReport> {
        if request.batches == 0 {
            bail!("projection run batches must be greater than zero");
        }

        let visibility_profile = if request.truth_visibility {
            VisibilityProfile::truth()
        } else {
            VisibilityProfile::default()
        };
        let mut session = self
            .open_session(OpenSessionRequest {
                session_id: Some("projection-run".to_string()),
                symbol: request.symbol.clone(),
                market_date: request.market_date,
                start_ts_ns: request.start_ts_ns,
                feed: SessionFeedConfig::Replay(ReplayFeedConfig {
                    mode: ReplayFeedMode::ExchangeTruth,
                    execution_profile: ExecutionProfile::default(),
                    visibility_profile,
                }),
            })
            .await?;

        let subscription = session
            .subscribe_projection(request.projection.clone())
            .context("subscribing projection for CLI run")?;
        let projection_key = subscription.key.clone();
        let mut frames = subscription
            .initial_frames
            .into_iter()
            .filter(|frame| frame.stamp.projection_key == projection_key)
            .collect::<Vec<_>>();

        let step = session.advance_feed_batches(request.batches)?;
        frames.extend(
            step.projection_frames
                .into_iter()
                .filter(|frame| frame.stamp.projection_key == projection_key),
        );

        let first_feed_ts_ns = frames.first().map(|frame| frame.stamp.feed_ts_ns.clone());
        let last_feed_ts_ns = frames.last().map(|frame| frame.stamp.feed_ts_ns.clone());
        let digest = if request.digest {
            Some(projection_frame_digest(&frames)?)
        } else {
            None
        };

        Ok(ProjectionRunReport {
            projection: ProjectionRunProjectionSummary {
                id: projection_key.id.as_str().to_string(),
                version: projection_key.version.get(),
                key: projection_key.to_string(),
            },
            dataset: ProjectionRunDatasetSummary {
                symbol: step.snapshot.market_day.contract_symbol.clone(),
                market_date: step.snapshot.market_day.market_date.to_string(),
                replay_dataset_id: step.snapshot.replay_dataset_id.clone(),
            },
            run: ProjectionRunSummary {
                requested_batches: request.batches,
                applied_batches: step.applied_batches,
                frames: frames.len(),
                first_feed_ts_ns,
                last_feed_ts_ns,
                digest,
            },
            passed: true,
            frames,
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

    fn replay_feed_config() -> ReplayFeedConfig {
        ReplayFeedConfig {
            mode: ReplayFeedMode::ExchangeTruth,
            execution_profile: ExecutionProfile::default(),
            visibility_profile: VisibilityProfile::truth(),
        }
    }

    fn session(store: EventStore) -> Session {
        Session::from_replay_dataset(
            "test-session".to_string(),
            replay_dataset(),
            store,
            replay_feed_config(),
        )
    }

    fn session_with_registry(store: EventStore, registry: ProjectionRegistry) -> Session {
        Session::from_replay_dataset_with_projection_registry(
            "test-session".to_string(),
            replay_dataset(),
            store,
            replay_feed_config(),
            registry,
        )
    }

    fn tick_echo_manifest(wake_policy: ProjectionWakePolicy) -> ProjectionManifest {
        ProjectionManifest {
            id: ProjectionId::new("tick_echo").unwrap(),
            version: ProjectionVersion::new(1).unwrap(),
            name: "tick_echo".to_string(),
            description: "Test projection that echoes SessionTick facts.".to_string(),
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
                payload: json!({ "batch_idx": 0, "feed_seq": 0, "feed_ts_ns": "0" }),
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
                "feed_seq": tick.feed_seq,
                "feed_ts_ns": tick.feed_ts_ns.to_string(),
                "exchange_events": tick.flags.exchange_events,
                "trades": tick.flags.trades,
                "bbo_changed": tick.flags.bbo_changed,
                "visibility_frame": tick.flags.visibility_frame,
                "fill_event": tick.flags.fill_event,
                "event_count": tick.market.event_count,
                "trade_count": tick.market.trade_count,
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
            self.payload = json!({ "batch_idx": 0, "feed_seq": 0, "feed_ts_ns": "0" });
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
    fn session_steps_and_reports_snapshots() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let mut session = session(store);

        let initial = session.snapshot();
        assert_eq!(initial.session_id, "test-session");
        assert_eq!(initial.feed_seq, 0);
        assert_eq!(initial.feed_ts_ns, "100");
        assert_eq!(initial.next_feed_ts_ns, Some("100".to_string()));
        assert_eq!(initial.source_first_ts_ns, None);
        assert_eq!(initial.source_last_ts_ns, None);
        assert_eq!(initial.batch_idx, 0);
        assert_eq!(initial.total_batches, 2);
        assert_eq!(initial.playback, SessionPlaybackState::Paused);
        assert_eq!(initial.bbo, None);

        let first = session.advance_one_feed_batch().unwrap();
        assert_eq!(first.requested_batches, 1);
        assert_eq!(first.applied_batches, 1);
        assert!(first.projection_frames.is_empty());
        assert_eq!(first.snapshot.feed_ts_ns, "100");
        assert_eq!(first.snapshot.next_feed_ts_ns, Some("200".to_string()));
        assert_eq!(first.snapshot.source_first_ts_ns, Some("100".to_string()));
        assert_eq!(first.snapshot.source_last_ts_ns, Some("100".to_string()));
        assert_eq!(first.snapshot.batch_idx, 1);
        assert_eq!(first.snapshot.frame_count, 1);
        assert_eq!(first.snapshot.playback, SessionPlaybackState::Paused);
        assert_eq!(first.snapshot.bbo.unwrap().bid_price, Some(PriceTicks(100)));

        let remaining = session.advance_feed_batches(10).unwrap();
        assert_eq!(remaining.requested_batches, 10);
        assert_eq!(remaining.applied_batches, 1);
        assert_eq!(remaining.snapshot.feed_ts_ns, "200");
        assert_eq!(remaining.snapshot.next_feed_ts_ns, None);
        assert_eq!(remaining.snapshot.batch_idx, 2);
        assert_eq!(remaining.snapshot.frame_count, 2);
        assert_eq!(remaining.snapshot.playback, SessionPlaybackState::Ended);
        assert_eq!(
            remaining.snapshot.bbo.unwrap().ask_price,
            Some(PriceTicks(101))
        );

        let exhausted = session.advance_one_feed_batch().unwrap();
        assert_eq!(exhausted.applied_batches, 0);
        assert!(exhausted.projection_frames.is_empty());
        assert_eq!(exhausted.snapshot.playback, SessionPlaybackState::Ended);
    }

    #[test]
    fn session_projection_subscription_receives_tick_frames() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let registry = registry_with_tick_echo(ProjectionWakePolicy::EveryTick);
        let mut session = session_with_registry(store, registry);

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

        let step = session.advance_one_feed_batch().unwrap();

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
        assert_eq!(frame.payload["feed_seq"], 1);
        assert_eq!(frame.payload["feed_ts_ns"], "100");
        assert_eq!(frame.payload["exchange_events"], true);
    }

    #[test]
    fn projection_subscription_after_step_uses_current_cursor() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let registry = registry_with_tick_echo(ProjectionWakePolicy::EveryTick);
        let mut session = session_with_registry(store, registry);
        session.advance_one_feed_batch().unwrap();

        let subscription = session.subscribe_projection(tick_echo_spec()).unwrap();

        assert_eq!(subscription.initial_frames.len(), 1);
        assert_eq!(subscription.initial_frames[0].stamp.batch_idx, 1);
        assert_eq!(subscription.initial_frames[0].stamp.cursor_ts_ns, "100");
    }

    #[test]
    fn session_tick_flags_match_trade_batch() {
        let store = event_store(vec![trade(100, 1, BookSide::Ask, 100, 3)]);
        let registry =
            registry_with_tick_echo(ProjectionWakePolicy::OnEventMask(ProjectionWakeEventMask {
                trades: true,
                ..Default::default()
            }));
        let mut session = session_with_registry(store, registry);
        session.subscribe_projection(tick_echo_spec()).unwrap();

        let step = session.advance_one_feed_batch().unwrap();

        assert_eq!(step.projection_frames.len(), 1);
        assert_eq!(step.projection_frames[0].payload["trades"], true);
        assert_eq!(step.projection_frames[0].payload["trade_count"], 1);
        assert_eq!(step.projection_frames[0].payload["bbo_changed"], false);
    }

    #[test]
    fn session_tick_flags_match_bbo_change() {
        let store = event_store(vec![add(100, 1, BookSide::Bid, 100, 2, 1)]);
        let registry =
            registry_with_tick_echo(ProjectionWakePolicy::OnEventMask(ProjectionWakeEventMask {
                bbo_changed: true,
                ..Default::default()
            }));
        let mut session = session_with_registry(store, registry);
        session.subscribe_projection(tick_echo_spec()).unwrap();

        let step = session.advance_one_feed_batch().unwrap();

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
        let mut session = session_with_registry(store, registry);
        session.subscribe_projection(tick_echo_spec()).unwrap();

        let first = session.advance_one_feed_batch().unwrap();
        assert_eq!(first.projection_frames[0].stamp.generation, 0);

        let seek = session.seek_to(100).unwrap();
        assert_eq!(session.projection_generation(), 1);
        assert_eq!(seek.projection_frames.len(), 1);
        assert_eq!(seek.projection_frames[0].stamp.generation, 1);
        assert_eq!(seek.projection_frames[0].stamp.batch_idx, 1);

        let second = session.advance_one_feed_batch().unwrap();
        assert_eq!(second.snapshot.feed_ts_ns, "200");
        assert_eq!(second.projection_frames.len(), 1);
        assert_eq!(second.projection_frames[0].stamp.generation, 1);
        assert_eq!(second.projection_frames[0].stamp.batch_idx, 2);
        assert_eq!(second.projection_frames[0].stamp.cursor_ts_ns, "200");
    }

    #[test]
    fn session_seek_rebuilds_from_start() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let mut session = session(store);
        session.advance_feed_batches(10).unwrap();
        assert_eq!(session.snapshot().playback, SessionPlaybackState::Ended);

        let seek = session.seek_to(100).unwrap();
        let snapshot = seek.snapshot;

        assert_eq!(snapshot.feed_ts_ns, "100");
        assert_eq!(snapshot.batch_idx, 1);
        assert_eq!(snapshot.playback, SessionPlaybackState::Paused);
        assert_eq!(snapshot.bbo.unwrap().bid_price, Some(PriceTicks(100)));
    }

    #[test]
    fn session_snapshot_matches_direct_simulator_checksum() {
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

        let mut session = session(store);
        let snapshot = session.advance_feed_batches(2).unwrap().snapshot;

        assert_eq!(snapshot.book_checksum, simulator.book().checksum());
        assert_eq!(snapshot.bbo, simulator.book().bbo());
    }

    #[test]
    fn session_rejects_invalid_speed() {
        let store = event_store(vec![add(100, 1, BookSide::Bid, 100, 2, 1)]);
        let mut session = session(store);

        let err = session.set_speed(0.0, 0).unwrap_err();

        assert!(format!("{err:#}").contains("positive finite"));
    }

    #[test]
    fn paused_clock_pump_does_not_apply_due_initial_batch() {
        let store = event_store(vec![add(100, 1, BookSide::Bid, 100, 2, 1)]);
        let mut session = session(store);

        let report = session
            .pump_clock(1_000, FeedAdvanceBudget::new(10).unwrap())
            .unwrap();

        assert_eq!(report.applied_batches, 0);
        assert!(!report.behind);
        assert_eq!(report.snapshot.playback, SessionPlaybackState::Paused);
        assert_eq!(report.snapshot.batch_idx, 0);
    }

    #[test]
    fn playing_clock_advances_batches_when_feed_time_is_due() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let mut session = session(store);

        session.play(1.0, 0).unwrap();
        let first = session
            .pump_clock(0, FeedAdvanceBudget::new(10).unwrap())
            .unwrap();
        assert_eq!(first.applied_batches, 1);
        assert_eq!(first.snapshot.batch_idx, 1);
        assert_eq!(first.snapshot.playback, SessionPlaybackState::Playing);

        let not_due = session
            .pump_clock(50, FeedAdvanceBudget::new(10).unwrap())
            .unwrap();
        assert_eq!(not_due.applied_batches, 0);
        assert_eq!(not_due.snapshot.batch_idx, 1);

        let second = session
            .pump_clock(100, FeedAdvanceBudget::new(10).unwrap())
            .unwrap();
        assert_eq!(second.applied_batches, 1);
        assert_eq!(second.snapshot.batch_idx, 2);
        assert_eq!(second.snapshot.playback, SessionPlaybackState::Ended);
    }

    #[test]
    fn clock_pump_reports_budget_exhaustion_and_behind() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(101, 2, BookSide::Ask, 101, 3, 2),
            add(102, 3, BookSide::Bid, 99, 4, 3),
        ]);
        let mut session = session(store);

        session.play(1.0, 0).unwrap();
        let report = session
            .pump_clock(100, FeedAdvanceBudget::new(1).unwrap())
            .unwrap();

        assert_eq!(report.applied_batches, 1);
        assert!(report.budget_exhausted);
        assert!(report.behind);
        assert_eq!(report.snapshot.batch_idx, 1);
    }

    #[test]
    fn speed_change_reanchors_clock_at_current_feed_time() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let mut session = session(store);

        session.play(1.0, 0).unwrap();
        session
            .pump_clock(0, FeedAdvanceBudget::new(10).unwrap())
            .unwrap();
        session.set_speed(10.0, 50).unwrap();
        let report = session
            .pump_clock(55, FeedAdvanceBudget::new(10).unwrap())
            .unwrap();

        assert_eq!(report.applied_batches, 0);
        assert_eq!(report.snapshot.batch_idx, 1);
        assert_eq!(report.snapshot.playback, SessionPlaybackState::Playing);
    }

    #[test]
    fn seek_pauses_clock_and_resets_projection_generation() {
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 3, 2),
        ]);
        let registry = registry_with_tick_echo(ProjectionWakePolicy::EveryTick);
        let mut session = session_with_registry(store, registry);
        session.subscribe_projection(tick_echo_spec()).unwrap();
        session.play(1.0, 0).unwrap();
        session
            .pump_clock(100, FeedAdvanceBudget::new(10).unwrap())
            .unwrap();

        let seek = session.seek_to(100).unwrap();
        let snapshot = seek.snapshot;

        assert_eq!(snapshot.playback, SessionPlaybackState::Paused);
        assert_eq!(snapshot.feed_seq, 0);
        assert_eq!(snapshot.source_first_ts_ns, None);
        assert_eq!(session.projection_generation(), 1);
        assert_eq!(seek.projection_frames.len(), 1);
        assert_eq!(seek.projection_frames[0].stamp.generation, 1);
    }
}
