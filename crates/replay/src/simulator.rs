use anyhow::{bail, Result};
use ledger_book::OrderBook;
use ledger_domain::{
    Bbo, EventStore, ExecutionProfile, SameTimestampPolicy, SimFill, SimOrderAccepted,
    SimOrderRequest, TradeRecord, UnixNanos, VisibilityProfile,
};
use serde::{Deserialize, Serialize};

use crate::{ExecutionSimulator, VisibilityFrame, VisibilityModel};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySimReport {
    pub cursor_ts_ns: u64,
    pub batch_idx: usize,
    pub fills: Vec<SimFill>,
    pub frames: Vec<VisibilityFrame>,
    pub final_book_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayStepResult {
    pub applied_batch_idx: usize,
    pub batch_idx_after: usize,
    pub cursor_ts_ns: UnixNanos,
    pub event_count: usize,
    pub trades: Vec<TradeRecord>,
    pub bbo_before: Option<Bbo>,
    pub bbo_after: Option<Bbo>,
    pub bbo_changed: bool,
    pub emitted_visibility_frames: usize,
    pub new_fills: usize,
}

pub struct ReplaySimulator {
    store: EventStore,
    book: OrderBook,
    batch_idx: usize,
    cursor_ts_ns: u64,
    execution: ExecutionSimulator,
    visibility: VisibilityModel,
}

impl ReplaySimulator {
    pub fn new(
        store: EventStore,
        execution_profile: ExecutionProfile,
        visibility_profile: VisibilityProfile,
    ) -> Self {
        Self {
            cursor_ts_ns: store
                .batches
                .first()
                .map(|b| b.ts_event_ns)
                .unwrap_or_default(),
            store,
            book: OrderBook::new(),
            batch_idx: 0,
            execution: ExecutionSimulator::new(execution_profile),
            visibility: VisibilityModel::new(visibility_profile),
        }
    }

    pub fn submit_order(&mut self, request: SimOrderRequest) -> SimOrderAccepted {
        self.execution.submit_order(request)
    }

    pub fn cancel_order(&mut self, order_id: u64, decision_ts_ns: u64) {
        self.execution.cancel_order(order_id, decision_ts_ns);
    }

    pub fn seek_to(&mut self, target_ts_ns: u64) -> Result<()> {
        // Phase 1 conservative seek: rebuild exchange truth from start. Open sim
        // orders are not preserved across seek; later phases can add checkpointing.
        self.book = OrderBook::new();
        self.batch_idx = 0;
        self.cursor_ts_ns = self
            .store
            .batches
            .first()
            .map(|b| b.ts_event_ns)
            .unwrap_or_default();
        while self.batch_idx < self.store.batches.len()
            && self.store.batches[self.batch_idx].ts_event_ns <= target_ts_ns
        {
            self.step_next_exchange_batch()?;
        }
        Ok(())
    }

    pub fn step_next_exchange_batch(&mut self) -> Result<ReplayStepResult> {
        if self.batch_idx >= self.store.batches.len() {
            bail!("no more batches");
        }
        let applied_batch_idx = self.batch_idx;
        let span = self.store.batches[self.batch_idx];
        let events = self.store.batch_events(span);
        let event_count = events.len();
        let fill_count_before = self.execution.fills().len();
        let out = self.book.apply_batch(events);
        let trades = out.trades.clone();
        self.execution.on_trades(&trades);
        let depth = self.book.depth(self.visibility.profile.depth_levels);
        self.visibility.on_batch(
            span.ts_event_ns,
            self.batch_idx,
            out.bbo_after,
            depth,
            trades.clone(),
        );
        self.cursor_ts_ns = span.ts_event_ns;
        self.batch_idx += 1;
        let emitted_visibility_frames = self.visibility.emit_due(self.cursor_ts_ns).len();
        let new_fills = self.execution.fills().len() - fill_count_before;
        Ok(ReplayStepResult {
            applied_batch_idx,
            batch_idx_after: self.batch_idx,
            cursor_ts_ns: self.cursor_ts_ns,
            event_count,
            trades,
            bbo_before: out.bbo_before,
            bbo_after: out.bbo_after,
            bbo_changed: out.bbo_before != out.bbo_after,
            emitted_visibility_frames,
            new_fills,
        })
    }

    pub fn run_until(&mut self, target_ts_ns: u64) -> Result<ReplaySimReport> {
        while let Some(next_ts) = self.next_event_ts() {
            if next_ts > target_ts_ns {
                break;
            }
            self.process_next_event(next_ts)?;
        }
        self.visibility.emit_due(target_ts_ns);
        self.cursor_ts_ns = target_ts_ns;
        Ok(self.report())
    }

    pub fn report(&self) -> ReplaySimReport {
        ReplaySimReport {
            cursor_ts_ns: self.cursor_ts_ns,
            batch_idx: self.batch_idx,
            fills: self.execution.fills().to_vec(),
            frames: self.visibility.emitted().to_vec(),
            final_book_checksum: self.book.checksum(),
        }
    }

    pub fn cursor_ts_ns(&self) -> u64 {
        self.cursor_ts_ns
    }

    pub fn batch_idx(&self) -> usize {
        self.batch_idx
    }

    pub fn total_batches(&self) -> usize {
        self.store.batches.len()
    }

    pub fn next_batch_ts_ns(&self) -> Option<UnixNanos> {
        self.store
            .batches
            .get(self.batch_idx)
            .map(|batch| batch.ts_event_ns)
    }

    pub fn next_batch_source_range(&self) -> Option<(UnixNanos, UnixNanos)> {
        let span = *self.store.batches.get(self.batch_idx)?;
        let events = self.store.batch_events(span);
        let first = events.first().map(|event| event.ts_event_ns)?;
        let last = events.last().map(|event| event.ts_event_ns)?;
        Some((first, last))
    }

    pub fn book(&self) -> &OrderBook {
        &self.book
    }
    pub fn execution(&self) -> &ExecutionSimulator {
        &self.execution
    }
    pub fn visibility(&self) -> &VisibilityModel {
        &self.visibility
    }

    fn next_event_ts(&self) -> Option<u64> {
        let next_batch_ts = self
            .store
            .batches
            .get(self.batch_idx)
            .map(|b| b.ts_event_ns);
        let next_exec_ts = self.execution.next_due_ts();
        let next_frame_ts = self.visibility.next_due_ts();
        [next_batch_ts, next_exec_ts, next_frame_ts]
            .into_iter()
            .flatten()
            .min()
    }

    fn process_next_event(&mut self, ts: u64) -> Result<()> {
        let exchange_first = self.execution.same_timestamp_policy()
            == SameTimestampPolicy::ExchangeFirstConservative;
        if exchange_first {
            self.process_exchange_batches_at_or_before(ts)?;
            self.process_exec_at_or_before(ts);
        } else {
            self.process_exec_at_or_before(ts);
            self.process_exchange_batches_at_or_before(ts)?;
        }
        self.visibility.emit_due(ts);
        self.cursor_ts_ns = ts;
        Ok(())
    }

    fn process_exchange_batches_at_or_before(&mut self, ts: u64) -> Result<()> {
        while self.batch_idx < self.store.batches.len()
            && self.store.batches[self.batch_idx].ts_event_ns <= ts
        {
            self.step_next_exchange_batch()?;
        }
        Ok(())
    }

    fn process_exec_at_or_before(&mut self, ts: u64) {
        let actions = self.execution.pop_due_at(ts);
        for action in actions {
            self.execution.apply_pending_action(action, ts, &self.book);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_domain::{
        build_batches, build_trade_index, BookAction, BookSide, EventStore, LatencyModel, MboEvent,
        PriceTicks, SimOrderKind, SimOrderRequest, SimOrderSide, SimOrderStatus,
    };

    fn store(events: Vec<MboEvent>) -> EventStore {
        let batches = build_batches(&events);
        let trades = build_trade_index(&events);
        EventStore {
            events,
            batches,
            trades,
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

    #[test]
    fn step_result_reports_batch_cursor_and_counts() {
        let store = store(vec![add(100, 1, BookSide::Bid, 100, 2, 1)]);
        let mut sim = ReplaySimulator::new(
            store,
            ExecutionProfile::default(),
            VisibilityProfile::truth(),
        );

        let step = sim.step_next_exchange_batch().unwrap();

        assert_eq!(step.applied_batch_idx, 0);
        assert_eq!(step.batch_idx_after, 1);
        assert_eq!(step.cursor_ts_ns, 100);
        assert_eq!(step.event_count, 1);
        assert_eq!(step.trades.len(), 0);
        assert_eq!(step.bbo_before, None);
        assert_eq!(step.bbo_after.unwrap().bid_price, Some(PriceTicks(100)));
        assert!(step.bbo_changed);
        assert_eq!(step.emitted_visibility_frames, 1);
        assert_eq!(step.new_fills, 0);
    }

    #[test]
    fn step_result_reports_trade_batch() {
        let store = store(vec![trade(100, 1, BookSide::Ask, 100, 3)]);
        let mut sim = ReplaySimulator::new(
            store,
            ExecutionProfile::default(),
            VisibilityProfile::truth(),
        );

        let step = sim.step_next_exchange_batch().unwrap();

        assert_eq!(step.trades.len(), 1);
        assert_eq!(step.trades[0].price, PriceTicks(100));
        assert_eq!(step.trades[0].size, 3);
    }

    #[test]
    fn market_order_fills_against_true_book_at_arrival_time() {
        let store = store(vec![
            add(100, 1, BookSide::Ask, 101, 5, 1),
            add(200, 2, BookSide::Ask, 102, 5, 2),
        ]);
        let exec = ExecutionProfile {
            order_entry_latency: LatencyModel::FixedNs(50),
            ..Default::default()
        };
        let mut sim = ReplaySimulator::new(store, exec, VisibilityProfile::truth());
        sim.submit_order(SimOrderRequest {
            side: SimOrderSide::Buy,
            qty: 6,
            kind: SimOrderKind::Market,
            decision_ts_ns: 100,
        });
        let report = sim.run_until(200).unwrap();
        assert_eq!(report.fills.len(), 1);
        assert_eq!(report.fills[0].price, PriceTicks(101));
        assert_eq!(report.fills[0].qty, 5);
    }

    #[test]
    fn limit_order_uses_queue_ahead_not_touch_equals_fill() {
        let store = store(vec![
            add(100, 1, BookSide::Bid, 100, 10, 1),
            trade(200, 2, BookSide::Ask, 100, 9),
            trade(300, 3, BookSide::Ask, 100, 3),
        ]);
        let exec = ExecutionProfile {
            order_entry_latency: LatencyModel::FixedNs(0),
            ..Default::default()
        };
        let mut sim = ReplaySimulator::new(store, exec, VisibilityProfile::truth());
        sim.run_until(100).unwrap();
        let accepted = sim.submit_order(SimOrderRequest {
            side: SimOrderSide::Buy,
            qty: 2,
            kind: SimOrderKind::Limit {
                limit_price: PriceTicks(100),
            },
            decision_ts_ns: 100,
        });
        sim.run_until(200).unwrap();
        assert_eq!(
            sim.execution().fills().len(),
            0,
            "9 traded but 10 were ahead in queue"
        );
        sim.run_until(300).unwrap();
        assert_eq!(sim.execution().fills().len(), 1);
        assert_eq!(sim.execution().fills()[0].order_id, accepted.order_id);
        assert_eq!(sim.execution().fills()[0].qty, 2);
    }

    #[test]
    fn cancel_can_lose_race_to_fill() {
        let store = store(vec![
            add(100, 1, BookSide::Bid, 100, 1, 1),
            trade(150, 2, BookSide::Ask, 100, 2),
        ]);
        let exec = ExecutionProfile {
            order_entry_latency: LatencyModel::FixedNs(0),
            cancel_latency: LatencyModel::FixedNs(100),
            ..Default::default()
        };
        let mut sim = ReplaySimulator::new(store, exec, VisibilityProfile::truth());
        sim.run_until(100).unwrap();
        let accepted = sim.submit_order(SimOrderRequest {
            side: SimOrderSide::Buy,
            qty: 1,
            kind: SimOrderKind::Limit {
                limit_price: PriceTicks(100),
            },
            decision_ts_ns: 100,
        });
        sim.run_until(101).unwrap();
        sim.cancel_order(accepted.order_id, 101);
        sim.run_until(300).unwrap();
        let order = sim.execution().orders().get(&accepted.order_id).unwrap();
        assert_eq!(order.status, SimOrderStatus::Filled);
        assert_eq!(sim.execution().fills().len(), 1);
    }

    #[test]
    fn visibility_frames_are_delayed() {
        let store = store(vec![add(100, 1, BookSide::Ask, 101, 5, 1)]);
        let visibility = VisibilityProfile {
            market_data_delay: LatencyModel::FixedNs(25),
            frame_interval_ns: 0,
            depth_levels: 1,
        };
        let mut sim = ReplaySimulator::new(store, ExecutionProfile::default(), visibility);
        sim.run_until(100).unwrap();
        assert_eq!(sim.visibility().emitted().len(), 0);
        sim.run_until(125).unwrap();
        assert_eq!(sim.visibility().emitted().len(), 1);
        assert_eq!(sim.visibility().emitted()[0].visible_ts_ns, 125);
    }

    #[test]
    fn exchange_first_same_timestamp_is_conservative() {
        let store = store(vec![
            add(100, 1, BookSide::Ask, 101, 5, 1),
            add(150, 2, BookSide::Ask, 102, 5, 2),
        ]);
        let exec = ExecutionProfile {
            order_entry_latency: LatencyModel::FixedNs(50),
            same_timestamp_policy: SameTimestampPolicy::ExchangeFirstConservative,
            ..Default::default()
        };
        let mut sim = ReplaySimulator::new(store, exec, VisibilityProfile::truth());
        sim.submit_order(SimOrderRequest {
            side: SimOrderSide::Buy,
            qty: 6,
            kind: SimOrderKind::Market,
            decision_ts_ns: 100,
        });
        sim.run_until(150).unwrap();
        assert_eq!(
            sim.execution().fills().len(),
            2,
            "batch at 150 is processed before order arrival at 150"
        );
    }
}
