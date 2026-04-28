use std::collections::{BTreeMap, HashMap};

use indexmap::IndexMap;
use ledger_core::{
    Bbo, BookAction, BookSide, Depth, DepthLevel, MboEvent, PriceTicks, SimOrderSide, TradeRecord,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookWarning {
    #[error("event missing side for action {action:?}")]
    MissingSide { action: BookAction },
    #[error("event missing price for action {action:?}")]
    MissingPrice { action: BookAction },
    #[error("duplicate order id {order_id}; replaced existing order")]
    DuplicateOrder { order_id: u64 },
    #[error("unknown order id {order_id} for action {action:?}")]
    UnknownOrder { order_id: u64, action: BookAction },
    #[error("cancel size {cancel_size} exceeds resting size {resting_size} for order {order_id}")]
    OversizedCancel {
        order_id: u64,
        cancel_size: u32,
        resting_size: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestingOrder {
    pub order_id: u64,
    pub side: BookSide,
    pub price: PriceTicks,
    pub size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PriceLevel {
    pub total_size: u32,
    pub orders: IndexMap<u64, RestingOrder>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BookEventResult {
    pub trade: Option<TradeRecord>,
    pub bbo_changed: Option<Bbo>,
    pub warnings: Vec<BookWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchBookOutput {
    pub trades: Vec<TradeRecord>,
    pub bbo_before: Option<Bbo>,
    pub bbo_after: Option<Bbo>,
    pub bbo_changed: Option<Bbo>,
    pub warnings: Vec<BookWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OrderBook {
    bids: BTreeMap<PriceTicks, PriceLevel>,
    asks: BTreeMap<PriceTicks, PriceLevel>,
    orders: HashMap<u64, RestingOrder>,
    last_published_bbo: Option<Bbo>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.orders.clear();
        self.last_published_bbo = Some(Bbo::empty());
    }

    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    pub fn level_count(&self, side: BookSide) -> usize {
        match side {
            BookSide::Bid => self.bids.len(),
            BookSide::Ask => self.asks.len(),
        }
    }

    pub fn bbo(&self) -> Option<Bbo> {
        let (bid_price, bid_size) = self
            .bids
            .iter()
            .next_back()
            .map(|(price, level)| (*price, level.total_size))
            .map_or((None, 0), |(p, s)| (Some(p), s));
        let (ask_price, ask_size) = self
            .asks
            .iter()
            .next()
            .map(|(price, level)| (*price, level.total_size))
            .map_or((None, 0), |(p, s)| (Some(p), s));

        if bid_price.is_none() && ask_price.is_none() {
            None
        } else {
            Some(Bbo {
                bid_price,
                bid_size,
                ask_price,
                ask_size,
            })
        }
    }

    pub fn depth(&self, levels: usize) -> Depth {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(levels)
            .map(|(price, level)| DepthLevel {
                price: *price,
                size: level.total_size,
                order_count: level.orders.len() as u32,
            })
            .collect();
        let asks = self
            .asks
            .iter()
            .take(levels)
            .map(|(price, level)| DepthLevel {
                price: *price,
                size: level.total_size,
                order_count: level.orders.len() as u32,
            })
            .collect();
        Depth { bids, asks }
    }

    pub fn resting_size_at(&self, side: BookSide, price: PriceTicks) -> u32 {
        self.levels(side)
            .get(&price)
            .map(|l| l.total_size)
            .unwrap_or(0)
    }

    pub fn apply_batch(&mut self, events: &[MboEvent]) -> BatchBookOutput {
        let bbo_before = self.bbo();
        let mut out = BatchBookOutput {
            bbo_before,
            ..Default::default()
        };

        for event in events {
            let result = self.apply_event_internal(event, false);
            if let Some(trade) = result.trade {
                out.trades.push(trade);
            }
            out.warnings.extend(result.warnings);
        }

        let bbo_after = self.bbo();
        out.bbo_after = bbo_after;
        if self.last_published_bbo != bbo_after {
            self.last_published_bbo = bbo_after;
            out.bbo_changed = bbo_after;
        }
        out
    }

    pub fn apply_event(&mut self, event: &MboEvent) -> BookEventResult {
        self.apply_event_internal(event, true)
    }

    fn apply_event_internal(&mut self, event: &MboEvent, publish_bbo: bool) -> BookEventResult {
        let mut result = BookEventResult::default();
        match event.action {
            BookAction::Add => self.apply_add(event, &mut result),
            BookAction::Modify => self.apply_modify(event, &mut result),
            BookAction::Cancel => self.apply_cancel(event, &mut result),
            BookAction::Clear => self.clear(),
            BookAction::Trade | BookAction::Fill => {
                if let Some(price) = event.price_ticks {
                    result.trade = Some(TradeRecord {
                        ts_event_ns: event.ts_event_ns,
                        sequence: event.sequence,
                        price,
                        size: event.size,
                        aggressor_side: event.side,
                        order_id: event.order_id,
                    });
                } else {
                    result.warnings.push(BookWarning::MissingPrice {
                        action: event.action,
                    });
                }
            }
            BookAction::None => {}
        }

        if publish_bbo {
            let bbo = self.bbo();
            if self.last_published_bbo != bbo {
                self.last_published_bbo = bbo;
                result.bbo_changed = bbo;
            }
        }

        result
    }

    fn apply_add(&mut self, event: &MboEvent, result: &mut BookEventResult) {
        let Some(side) = event.side else {
            result.warnings.push(BookWarning::MissingSide {
                action: event.action,
            });
            return;
        };
        let Some(price) = event.price_ticks else {
            result.warnings.push(BookWarning::MissingPrice {
                action: event.action,
            });
            return;
        };

        if self.orders.contains_key(&event.order_id) {
            self.remove_order(event.order_id);
            result.warnings.push(BookWarning::DuplicateOrder {
                order_id: event.order_id,
            });
        }

        let order = RestingOrder {
            order_id: event.order_id,
            side,
            price,
            size: event.size,
        };
        self.insert_order(order);
    }

    fn apply_modify(&mut self, event: &MboEvent, result: &mut BookEventResult) {
        let Some(existing) = self.orders.get(&event.order_id).cloned() else {
            result.warnings.push(BookWarning::UnknownOrder {
                order_id: event.order_id,
                action: event.action,
            });
            self.apply_add(event, result);
            return;
        };

        let side = event.side.unwrap_or(existing.side);
        let price = event.price_ticks.unwrap_or(existing.price);
        let size = event.size;

        self.remove_order(event.order_id);
        self.insert_order(RestingOrder {
            order_id: event.order_id,
            side,
            price,
            size,
        });
    }

    fn apply_cancel(&mut self, event: &MboEvent, result: &mut BookEventResult) {
        let Some(mut existing) = self.orders.get(&event.order_id).cloned() else {
            result.warnings.push(BookWarning::UnknownOrder {
                order_id: event.order_id,
                action: event.action,
            });
            return;
        };

        let cancel_size = event.size;
        if cancel_size >= existing.size {
            if cancel_size > existing.size {
                result.warnings.push(BookWarning::OversizedCancel {
                    order_id: event.order_id,
                    cancel_size,
                    resting_size: existing.size,
                });
            }
            self.remove_order(event.order_id);
        } else {
            self.remove_order(event.order_id);
            existing.size -= cancel_size;
            self.insert_order(existing);
        }
    }

    fn levels(&self, side: BookSide) -> &BTreeMap<PriceTicks, PriceLevel> {
        match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        }
    }

    fn levels_mut(&mut self, side: BookSide) -> &mut BTreeMap<PriceTicks, PriceLevel> {
        match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        }
    }

    fn insert_order(&mut self, order: RestingOrder) {
        let level = self.levels_mut(order.side).entry(order.price).or_default();
        level.total_size = level.total_size.saturating_add(order.size);
        level.orders.insert(order.order_id, order.clone());
        self.orders.insert(order.order_id, order);
    }

    fn remove_order(&mut self, order_id: u64) -> Option<RestingOrder> {
        let order = self.orders.remove(&order_id)?;
        let levels = self.levels_mut(order.side);
        if let Some(level) = levels.get_mut(&order.price) {
            level.total_size = level.total_size.saturating_sub(order.size);
            level.orders.shift_remove(&order_id);
            if level.orders.is_empty() {
                levels.remove(&order.price);
            }
        }
        Some(order)
    }

    /// Non-mutating market-order fill against the current book. This is used by
    /// the simulator. Simulated orders do not alter the historical replay truth.
    pub fn simulate_market_take(
        &self,
        side: SimOrderSide,
        qty: u32,
        limit_price: Option<PriceTicks>,
    ) -> Vec<(PriceTicks, u32)> {
        let mut remaining = qty;
        let mut fills = Vec::new();

        match side {
            SimOrderSide::Buy => {
                for (price, level) in self.asks.iter() {
                    if let Some(limit) = limit_price {
                        if *price > limit {
                            break;
                        }
                    }
                    let take = remaining.min(level.total_size);
                    if take > 0 {
                        fills.push((*price, take));
                        remaining -= take;
                    }
                    if remaining == 0 {
                        break;
                    }
                }
            }
            SimOrderSide::Sell => {
                for (price, level) in self.bids.iter().rev() {
                    if let Some(limit) = limit_price {
                        if *price < limit {
                            break;
                        }
                    }
                    let take = remaining.min(level.total_size);
                    if take > 0 {
                        fills.push((*price, take));
                        remaining -= take;
                    }
                    if remaining == 0 {
                        break;
                    }
                }
            }
        }

        fills
    }

    pub fn is_marketable_limit(&self, side: SimOrderSide, limit: PriceTicks) -> bool {
        match (side, self.bbo()) {
            (SimOrderSide::Buy, Some(bbo)) => bbo.ask_price.is_some_and(|ask| limit >= ask),
            (SimOrderSide::Sell, Some(bbo)) => bbo.bid_price.is_some_and(|bid| limit <= bid),
            _ => false,
        }
    }

    pub fn checksum(&self) -> String {
        let mut hasher = Sha256::new();
        for (price, level) in &self.bids {
            hasher.update(b"B");
            hasher.update(price.0.to_le_bytes());
            hasher.update(level.total_size.to_le_bytes());
            for (order_id, order) in &level.orders {
                hasher.update(order_id.to_le_bytes());
                hasher.update(order.size.to_le_bytes());
            }
        }
        for (price, level) in &self.asks {
            hasher.update(b"A");
            hasher.update(price.0.to_le_bytes());
            hasher.update(level.total_size.to_le_bytes());
            for (order_id, order) in &level.orders {
                hasher.update(order_id.to_le_bytes());
                hasher.update(order.size.to_le_bytes());
            }
        }
        hex::encode(hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_core::{BookAction, BookSide, MboEvent, PriceTicks};

    fn add(order_id: u64, side: BookSide, price: i64, size: u32) -> MboEvent {
        MboEvent::synthetic(
            1,
            order_id,
            BookAction::Add,
            Some(side),
            Some(PriceTicks(price)),
            size,
            order_id,
            true,
        )
    }

    fn cancel(order_id: u64, size: u32) -> MboEvent {
        MboEvent::synthetic(
            2,
            order_id,
            BookAction::Cancel,
            None,
            None,
            size,
            order_id,
            true,
        )
    }

    #[test]
    fn add_updates_bbo_and_depth() {
        let mut book = OrderBook::new();
        book.apply_event(&add(1, BookSide::Bid, 100, 10));
        book.apply_event(&add(2, BookSide::Ask, 101, 12));
        let bbo = book.bbo().unwrap();
        assert_eq!(bbo.bid_price, Some(PriceTicks(100)));
        assert_eq!(bbo.bid_size, 10);
        assert_eq!(bbo.ask_price, Some(PriceTicks(101)));
        assert_eq!(bbo.ask_size, 12);
        assert_eq!(book.depth(1).bids[0].order_count, 1);
    }

    #[test]
    fn partial_cancel_reduces_size() {
        let mut book = OrderBook::new();
        book.apply_event(&add(1, BookSide::Bid, 100, 10));
        book.apply_event(&cancel(1, 4));
        assert_eq!(book.bbo().unwrap().bid_size, 6);
    }

    #[test]
    fn full_cancel_removes_order_and_level() {
        let mut book = OrderBook::new();
        book.apply_event(&add(1, BookSide::Bid, 100, 10));
        book.apply_event(&cancel(1, 10));
        assert_eq!(book.order_count(), 0);
        assert_eq!(book.bbo(), None);
    }

    #[test]
    fn modify_moves_order_between_levels() {
        let mut book = OrderBook::new();
        book.apply_event(&add(1, BookSide::Bid, 100, 10));
        let modify = MboEvent::synthetic(
            2,
            2,
            BookAction::Modify,
            Some(BookSide::Bid),
            Some(PriceTicks(101)),
            7,
            1,
            true,
        );
        book.apply_event(&modify);
        let bbo = book.bbo().unwrap();
        assert_eq!(bbo.bid_price, Some(PriceTicks(101)));
        assert_eq!(bbo.bid_size, 7);
        assert_eq!(book.level_count(BookSide::Bid), 1);
    }

    #[test]
    fn clear_resets_book() {
        let mut book = OrderBook::new();
        book.apply_event(&add(1, BookSide::Bid, 100, 10));
        let clear = MboEvent::synthetic(2, 2, BookAction::Clear, None, None, 0, 0, true);
        book.apply_event(&clear);
        assert_eq!(book.bbo(), None);
        assert_eq!(book.order_count(), 0);
    }

    #[test]
    fn trade_emits_trade_but_does_not_affect_book() {
        let mut book = OrderBook::new();
        book.apply_event(&add(1, BookSide::Ask, 101, 10));
        let trade = MboEvent::synthetic(
            2,
            2,
            BookAction::Trade,
            Some(BookSide::Bid),
            Some(PriceTicks(101)),
            3,
            0,
            true,
        );
        let out = book.apply_event(&trade);
        assert_eq!(out.trade.unwrap().size, 3);
        assert_eq!(book.bbo().unwrap().ask_size, 10);
    }

    #[test]
    fn batch_publishes_bbo_once_after_batch_end() {
        let mut book = OrderBook::new();
        let events = vec![
            MboEvent::synthetic(
                1,
                1,
                BookAction::Add,
                Some(BookSide::Bid),
                Some(PriceTicks(100)),
                10,
                1,
                false,
            ),
            MboEvent::synthetic(
                1,
                2,
                BookAction::Add,
                Some(BookSide::Ask),
                Some(PriceTicks(101)),
                10,
                2,
                true,
            ),
        ];
        let out = book.apply_batch(&events);
        assert_eq!(out.bbo_before, None);
        assert_eq!(out.bbo_changed.unwrap().bid_price, Some(PriceTicks(100)));
        assert_eq!(out.bbo_changed.unwrap().ask_price, Some(PriceTicks(101)));
    }
}
