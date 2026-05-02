use anyhow::{bail, ensure, Context, Result};
use chrono::NaiveDate;
use ledger_domain::{Bbo, EventStore, ExecutionProfile, MarketDay, UnixNanos, VisibilityProfile};
use ledger_replay::ReplaySimulator;
use serde::{Deserialize, Serialize};

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
        let total_batches = event_store.batches.len();
        let mut session = Self {
            id: session_id,
            replay_dataset_id: dataset.replay_dataset_id,
            market_day: dataset.market_day,
            total_batches,
            simulator: ReplaySimulator::new(event_store, execution_profile, visibility_profile),
            state: ReplaySessionState::default(),
        };
        session.refresh_end_state();
        session
    }

    pub fn seek_to(&mut self, ts_ns: UnixNanos) -> Result<ReplaySessionSnapshot> {
        self.simulator
            .seek_to(ts_ns)
            .with_context(|| format!("seeking ReplaySession {} to {ts_ns}", self.id))?;
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
            });
        }

        let mut applied_batches = 0;
        for _ in 0..batches {
            if self.simulator.batch_idx() >= self.total_batches {
                self.state.playback = ReplayPlaybackState::Ended;
                break;
            }
            self.simulator
                .step_next_exchange_batch()
                .with_context(|| format!("stepping ReplaySession {}", self.id))?;
            applied_batches += 1;
        }

        self.refresh_end_state();
        Ok(ReplaySessionStepReport {
            requested_batches: batches,
            applied_batches,
            snapshot: self.snapshot(),
        })
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
        let mut session = ReplaySession::new(
            session_id,
            dataset,
            event_store,
            config.execution_profile,
            config.visibility_profile,
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
    use chrono::NaiveDate;
    use ledger_domain::{
        build_batches, build_trade_index, BookAction, BookSide, MboEvent, PriceTicks,
    };
    use ledger_replay::ReplaySimulator;

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
        assert_eq!(exhausted.snapshot.playback, ReplayPlaybackState::Ended);
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
