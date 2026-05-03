use anyhow::{Context, Result};
use ledger_domain::{Bbo, ExecutionProfile, UnixNanos, VisibilityProfile};
use serde::{Deserialize, Serialize};

use crate::{ReplaySimulator, ReplayStepResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayFeedMode {
    ExchangeTruth,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayFeedConfig {
    pub mode: ReplayFeedMode,
    pub execution_profile: ExecutionProfile,
    pub visibility_profile: VisibilityProfile,
}

impl Default for ReplayFeedConfig {
    fn default() -> Self {
        Self {
            mode: ReplayFeedMode::ExchangeTruth,
            execution_profile: ExecutionProfile::default(),
            visibility_profile: VisibilityProfile::truth(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayFeedBatch {
    pub feed_seq: u64,
    pub feed_ts_ns: UnixNanos,
    pub source_first_ts_ns: Option<UnixNanos>,
    pub source_last_ts_ns: Option<UnixNanos>,
    pub replay_step: ReplayStepResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayFeedSnapshot {
    pub feed_seq: u64,
    pub feed_ts_ns: UnixNanos,
    pub next_feed_ts_ns: Option<UnixNanos>,
    pub source_first_ts_ns: Option<UnixNanos>,
    pub source_last_ts_ns: Option<UnixNanos>,
    pub batch_idx: usize,
    pub total_batches: usize,
    pub book_checksum: String,
    pub bbo: Option<Bbo>,
    pub visibility_frame_count: usize,
    pub fill_count: usize,
    pub ended: bool,
}

pub struct ReplayFeed {
    simulator: ReplaySimulator,
    mode: ReplayFeedMode,
    feed_seq: u64,
    total_batches: usize,
    source_first_ts_ns: Option<UnixNanos>,
    source_last_ts_ns: Option<UnixNanos>,
}

impl ReplayFeed {
    pub fn new(simulator: ReplaySimulator, mode: ReplayFeedMode) -> Self {
        let total_batches = simulator.total_batches();
        Self {
            simulator,
            mode,
            feed_seq: 0,
            total_batches,
            source_first_ts_ns: None,
            source_last_ts_ns: None,
        }
    }

    pub fn mode(&self) -> ReplayFeedMode {
        self.mode
    }

    pub fn advance_one(&mut self) -> Result<Option<ReplayFeedBatch>> {
        if self.ended() {
            return Ok(None);
        }
        let (source_first_ts_ns, source_last_ts_ns) = self
            .simulator
            .next_batch_source_range()
            .unwrap_or((self.simulator.cursor_ts_ns(), self.simulator.cursor_ts_ns()));
        let replay_step = self
            .simulator
            .step_next_exchange_batch()
            .context("advancing replay feed one exchange-truth batch")?;
        self.feed_seq += 1;
        self.source_first_ts_ns = Some(source_first_ts_ns);
        self.source_last_ts_ns = Some(source_last_ts_ns);
        Ok(Some(ReplayFeedBatch {
            feed_seq: self.feed_seq,
            feed_ts_ns: replay_step.cursor_ts_ns,
            source_first_ts_ns: Some(source_first_ts_ns),
            source_last_ts_ns: Some(source_last_ts_ns),
            replay_step,
        }))
    }

    pub fn seek_to(&mut self, target_ts_ns: UnixNanos) -> Result<ReplayFeedSnapshot> {
        self.simulator
            .seek_to(target_ts_ns)
            .context("seeking replay feed")?;
        self.feed_seq = 0;
        self.source_first_ts_ns = None;
        self.source_last_ts_ns = None;
        Ok(self.snapshot())
    }

    pub fn next_feed_ts_ns(&self) -> Option<UnixNanos> {
        self.simulator.next_batch_ts_ns()
    }

    pub fn snapshot(&self) -> ReplayFeedSnapshot {
        ReplayFeedSnapshot {
            feed_seq: self.feed_seq,
            feed_ts_ns: self.simulator.cursor_ts_ns(),
            next_feed_ts_ns: self.next_feed_ts_ns(),
            source_first_ts_ns: self.source_first_ts_ns,
            source_last_ts_ns: self.source_last_ts_ns,
            batch_idx: self.simulator.batch_idx(),
            total_batches: self.total_batches,
            book_checksum: self.simulator.book().checksum(),
            bbo: self.simulator.book().bbo(),
            visibility_frame_count: self.simulator.visibility().emitted().len(),
            fill_count: self.simulator.execution().fills().len(),
            ended: self.ended(),
        }
    }

    pub fn ended(&self) -> bool {
        self.simulator.batch_idx() >= self.total_batches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_domain::{
        build_batches, build_trade_index, BookAction, BookSide, EventStore, MboEvent, PriceTicks,
    };

    fn store(events: Vec<MboEvent>) -> EventStore {
        EventStore {
            batches: build_batches(&events),
            trades: build_trade_index(&events),
            events,
        }
    }

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

    fn replay_feed(events: Vec<MboEvent>) -> ReplayFeed {
        ReplayFeed::new(
            ReplaySimulator::new(
                store(events),
                ExecutionProfile::default(),
                VisibilityProfile::truth(),
            ),
            ReplayFeedMode::ExchangeTruth,
        )
    }

    #[test]
    fn snapshot_starts_at_first_batch() {
        let feed = replay_feed(vec![add(100, 1, BookSide::Bid, 100, 2, 1)]);

        let snapshot = feed.snapshot();

        assert_eq!(snapshot.feed_seq, 0);
        assert_eq!(snapshot.feed_ts_ns, 100);
        assert_eq!(snapshot.next_feed_ts_ns, Some(100));
        assert_eq!(snapshot.source_first_ts_ns, None);
        assert_eq!(snapshot.source_last_ts_ns, None);
        assert_eq!(snapshot.batch_idx, 0);
        assert_eq!(snapshot.total_batches, 1);
        assert!(!snapshot.ended);
    }

    #[test]
    fn advance_one_emits_feed_batch() {
        let mut feed = replay_feed(vec![add(100, 1, BookSide::Bid, 100, 2, 1)]);

        let batch = feed.advance_one().unwrap().unwrap();

        assert_eq!(batch.feed_seq, 1);
        assert_eq!(batch.feed_ts_ns, 100);
        assert_eq!(batch.source_first_ts_ns, Some(100));
        assert_eq!(batch.source_last_ts_ns, Some(100));
        assert_eq!(batch.replay_step.batch_idx_after, 1);
        assert_eq!(feed.snapshot().source_first_ts_ns, Some(100));
        assert_eq!(feed.snapshot().source_last_ts_ns, Some(100));
        assert!(feed.snapshot().ended);
    }

    #[test]
    fn advance_one_returns_none_at_end() {
        let mut feed = replay_feed(vec![add(100, 1, BookSide::Bid, 100, 2, 1)]);

        feed.advance_one().unwrap();

        assert!(feed.advance_one().unwrap().is_none());
    }

    #[test]
    fn seek_resets_delivery_sequence() {
        let mut feed = replay_feed(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(200, 2, BookSide::Ask, 101, 2, 2),
        ]);
        feed.advance_one().unwrap();

        let snapshot = feed.seek_to(100).unwrap();

        assert_eq!(snapshot.feed_seq, 0);
        assert_eq!(snapshot.feed_ts_ns, 100);
        assert_eq!(snapshot.source_first_ts_ns, None);
        assert_eq!(snapshot.source_last_ts_ns, None);
        assert_eq!(snapshot.batch_idx, 1);
        assert_eq!(snapshot.next_feed_ts_ns, Some(200));
    }
}
