use std::collections::{BTreeMap, HashMap};

use ledger_book::OrderBook;
use ledger_core::{
    ExecutionProfile, PriceTicks, SameTimestampPolicy, SimFill, SimLiquidity, SimOrderAccepted,
    SimOrderId, SimOrderKind, SimOrderRequest, SimOrderSide, SimOrderStatus, TradeRecord,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimOrderState {
    pub order_id: SimOrderId,
    pub request: SimOrderRequest,
    pub arrival_ts_ns: u64,
    pub status: SimOrderStatus,
    pub remaining_qty: u32,
    pub resting_price: Option<PriceTicks>,
    pub queue_ahead: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PendingExecAction {
    NewOrder(SimOrderId),
    Cancel(SimOrderId),
}

#[derive(Debug, Clone)]
pub struct ExecutionSimulator {
    profile: ExecutionProfile,
    next_order_id: SimOrderId,
    orders: HashMap<SimOrderId, SimOrderState>,
    pending: BTreeMap<(u64, u64), PendingExecAction>,
    next_pending_seq: u64,
    fills: Vec<SimFill>,
}

impl ExecutionSimulator {
    pub fn new(profile: ExecutionProfile) -> Self {
        Self {
            profile,
            next_order_id: 1,
            orders: HashMap::new(),
            pending: BTreeMap::new(),
            next_pending_seq: 1,
            fills: Vec::new(),
        }
    }

    pub fn profile(&self) -> &ExecutionProfile {
        &self.profile
    }

    pub fn submit_order(&mut self, request: SimOrderRequest) -> SimOrderAccepted {
        let order_id = self.next_order_id;
        self.next_order_id += 1;
        let arrival_ts_ns = request
            .decision_ts_ns
            .saturating_add(self.profile.order_entry_latency.sample_ns());
        let state = SimOrderState {
            order_id,
            arrival_ts_ns,
            remaining_qty: request.qty,
            request: request.clone(),
            status: SimOrderStatus::PendingArrival,
            resting_price: None,
            queue_ahead: 0,
        };
        self.orders.insert(order_id, state);
        self.push_pending(arrival_ts_ns, PendingExecAction::NewOrder(order_id));
        SimOrderAccepted {
            order_id,
            request,
            arrival_ts_ns,
        }
    }

    pub fn cancel_order(&mut self, order_id: SimOrderId, decision_ts_ns: u64) {
        let arrival_ts_ns = decision_ts_ns.saturating_add(self.profile.cancel_latency.sample_ns());
        self.push_pending(arrival_ts_ns, PendingExecAction::Cancel(order_id));
    }

    pub fn next_due_ts(&self) -> Option<u64> {
        self.pending.keys().next().map(|(ts, _seq)| *ts)
    }

    pub(crate) fn pop_due_at(&mut self, ts_ns: u64) -> Vec<PendingExecAction> {
        let keys: Vec<_> = self
            .pending
            .keys()
            .copied()
            .take_while(|(ts, _)| *ts <= ts_ns)
            .collect();
        let mut out = Vec::new();
        for key in keys {
            if let Some(action) = self.pending.remove(&key) {
                out.push(action);
            }
        }
        out
    }

    pub(crate) fn apply_pending_action(
        &mut self,
        action: PendingExecAction,
        ts_ns: u64,
        book: &OrderBook,
    ) {
        match action {
            PendingExecAction::NewOrder(order_id) => {
                self.apply_new_order_arrival(order_id, ts_ns, book)
            }
            PendingExecAction::Cancel(order_id) => self.apply_cancel_arrival(order_id),
        }
    }

    pub fn on_trades(&mut self, trades: &[TradeRecord]) {
        for trade in trades {
            self.on_trade(trade);
        }
    }

    pub fn orders(&self) -> &HashMap<SimOrderId, SimOrderState> {
        &self.orders
    }

    pub fn fills(&self) -> &[SimFill] {
        &self.fills
    }

    pub fn same_timestamp_policy(&self) -> SameTimestampPolicy {
        self.profile.same_timestamp_policy
    }

    fn apply_new_order_arrival(&mut self, order_id: SimOrderId, ts_ns: u64, book: &OrderBook) {
        let Some(snapshot) = self.orders.get(&order_id).cloned() else {
            return;
        };
        if !matches!(snapshot.status, SimOrderStatus::PendingArrival) {
            return;
        }

        match snapshot.request.kind {
            SimOrderKind::Market => {
                self.take_market(order_id, ts_ns, book, None);
            }
            SimOrderKind::Limit { limit_price } => {
                if book.is_marketable_limit(snapshot.request.side, limit_price) {
                    self.take_market(order_id, ts_ns, book, Some(limit_price));
                }
                if let Some(state) = self.orders.get_mut(&order_id) {
                    if state.remaining_qty > 0 && !matches!(state.status, SimOrderStatus::Filled) {
                        state.status = SimOrderStatus::Resting;
                        state.resting_price = Some(limit_price);
                        state.queue_ahead =
                            book.resting_size_at(state.request.side.book_side(), limit_price);
                    }
                }
            }
            SimOrderKind::StopMarket { .. } => {
                if let Some(state) = self.orders.get_mut(&order_id) {
                    state.status = SimOrderStatus::Resting;
                }
            }
        }
    }

    fn apply_cancel_arrival(&mut self, order_id: SimOrderId) {
        let Some(state) = self.orders.get_mut(&order_id) else {
            return;
        };
        if matches!(
            state.status,
            SimOrderStatus::Resting | SimOrderStatus::PendingArrival
        ) {
            state.status = SimOrderStatus::Cancelled;
            state.remaining_qty = 0;
        }
    }

    fn take_market(
        &mut self,
        order_id: SimOrderId,
        ts_ns: u64,
        book: &OrderBook,
        limit_price: Option<PriceTicks>,
    ) {
        let Some(state_snapshot) = self.orders.get(&order_id).cloned() else {
            return;
        };
        let fills = book.simulate_market_take(
            state_snapshot.request.side,
            state_snapshot.remaining_qty,
            limit_price,
        );
        let mut filled_qty = 0u32;
        for (price, qty) in fills {
            filled_qty += qty;
            self.fills.push(SimFill {
                order_id,
                ts_ns,
                side: state_snapshot.request.side,
                price,
                qty,
                liquidity: SimLiquidity::Taking,
            });
        }
        if let Some(state) = self.orders.get_mut(&order_id) {
            state.remaining_qty = state.remaining_qty.saturating_sub(filled_qty);
            if state.remaining_qty == 0 {
                state.status = SimOrderStatus::Filled;
            }
        }
    }

    fn on_trade(&mut self, trade: &TradeRecord) {
        // Stop-market trigger support.
        let stop_to_trigger: Vec<_> = self
            .orders
            .iter()
            .filter_map(|(id, state)| {
                if !matches!(state.status, SimOrderStatus::Resting) {
                    return None;
                }
                let SimOrderKind::StopMarket { stop_price } = state.request.kind else {
                    return None;
                };
                let triggered = match state.request.side {
                    SimOrderSide::Buy => trade.price >= stop_price,
                    SimOrderSide::Sell => trade.price <= stop_price,
                };
                triggered.then_some(*id)
            })
            .collect();
        let mut triggered_arrivals = Vec::new();
        for id in stop_to_trigger {
            if let Some(state) = self.orders.get_mut(&id) {
                state.status = SimOrderStatus::PendingArrival;
                let arrival = trade
                    .ts_event_ns
                    .saturating_add(self.profile.order_entry_latency.sample_ns());
                triggered_arrivals.push((arrival, id));
            }
        }
        for (arrival, id) in triggered_arrivals {
            self.push_pending(arrival, PendingExecAction::NewOrder(id));
        }

        let aggressive_side = trade.aggressor_side;
        let fillable_ids: Vec<_> = self
            .orders
            .iter()
            .filter_map(|(id, state)| {
                if !matches!(state.status, SimOrderStatus::Resting) {
                    return None;
                }
                let price = state.resting_price?;
                if price != trade.price {
                    return None;
                }
                let needed_aggressor = state.request.side.opposite_aggressor_side_for_fill();
                if aggressive_side != Some(needed_aggressor) {
                    return None;
                }
                Some(*id)
            })
            .collect();

        for id in fillable_ids {
            let Some(state) = self.orders.get_mut(&id) else {
                continue;
            };
            let mut remaining_trade_size = trade.size;
            if state.queue_ahead > 0 {
                let consumed_queue = state.queue_ahead.min(remaining_trade_size);
                state.queue_ahead -= consumed_queue;
                remaining_trade_size -= consumed_queue;
            }
            if remaining_trade_size == 0 || state.remaining_qty == 0 {
                continue;
            }
            let fill_qty = state.remaining_qty.min(remaining_trade_size);
            state.remaining_qty -= fill_qty;
            self.fills.push(SimFill {
                order_id: id,
                ts_ns: trade.ts_event_ns,
                side: state.request.side,
                price: trade.price,
                qty: fill_qty,
                liquidity: SimLiquidity::Making,
            });
            if state.remaining_qty == 0 {
                state.status = SimOrderStatus::Filled;
            }
        }
    }

    fn push_pending(&mut self, ts_ns: u64, action: PendingExecAction) {
        let seq = self.next_pending_seq;
        self.next_pending_seq += 1;
        self.pending.insert((ts_ns, seq), action);
    }
}
