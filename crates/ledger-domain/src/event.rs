use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::UnixNanos;

pub const ES_TICK_SIZE_FIXED_PRICE: i64 = 250_000_000;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct PriceTicks(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookAction {
    Add,
    Modify,
    Cancel,
    Clear,
    Trade,
    Fill,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MboEvent {
    pub ts_event_ns: UnixNanos,
    pub ts_recv_ns: UnixNanos,
    pub sequence: u64,
    pub action: BookAction,
    pub side: Option<BookSide>,
    pub price_ticks: Option<PriceTicks>,
    pub size: u32,
    pub order_id: u64,
    pub flags: u8,
    pub is_last: bool,
}

impl MboEvent {
    pub fn synthetic(
        ts_event_ns: UnixNanos,
        sequence: u64,
        action: BookAction,
        side: Option<BookSide>,
        price_ticks: Option<PriceTicks>,
        size: u32,
        order_id: u64,
        is_last: bool,
    ) -> Self {
        Self {
            ts_event_ns,
            ts_recv_ns: ts_event_ns,
            sequence,
            action,
            side,
            price_ticks,
            size,
            order_id,
            flags: if is_last { 0x80 } else { 0 },
            is_last,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchSpan {
    pub start_idx: usize,
    pub end_idx: usize,
    pub ts_event_ns: UnixNanos,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeRecord {
    pub ts_event_ns: UnixNanos,
    pub sequence: u64,
    pub price: PriceTicks,
    pub size: u32,
    pub aggressor_side: Option<BookSide>,
    pub order_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EventStore {
    pub events: Vec<MboEvent>,
    pub batches: Vec<BatchSpan>,
    pub trades: Vec<TradeRecord>,
}

impl EventStore {
    pub fn batch_events(&self, span: BatchSpan) -> &[MboEvent] {
        &self.events[span.start_idx..span.end_idx]
    }

    pub fn validate(&self) -> Result<()> {
        let expected_batches = build_batches(&self.events);
        if self.batches != expected_batches {
            bail!(
                "event store batch index mismatch: decoded {} batches, rebuilt {} batches",
                self.batches.len(),
                expected_batches.len()
            );
        }

        let expected_trades = build_trade_index(&self.events);
        if self.trades != expected_trades {
            bail!(
                "event store trade index mismatch: decoded {} trades, rebuilt {} trades",
                self.trades.len(),
                expected_trades.len()
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Bbo {
    pub bid_price: Option<PriceTicks>,
    pub bid_size: u32,
    pub ask_price: Option<PriceTicks>,
    pub ask_size: u32,
}

impl Bbo {
    pub fn empty() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DepthLevel {
    pub price: PriceTicks,
    pub size: u32,
    pub order_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Depth {
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

pub fn dbn_fixed_price_to_es_ticks(price: i64) -> Option<PriceTicks> {
    if price == i64::MAX {
        return None;
    }
    Some(PriceTicks(price / ES_TICK_SIZE_FIXED_PRICE))
}

pub fn build_batches(events: &[MboEvent]) -> Vec<BatchSpan> {
    let mut batches = Vec::new();
    let mut start_idx = 0;
    for (idx, event) in events.iter().enumerate() {
        if event.is_last {
            batches.push(BatchSpan {
                start_idx,
                end_idx: idx + 1,
                ts_event_ns: event.ts_event_ns,
            });
            start_idx = idx + 1;
        }
    }
    if start_idx < events.len() {
        let ts_event_ns = events.last().map(|e| e.ts_event_ns).unwrap_or_default();
        batches.push(BatchSpan {
            start_idx,
            end_idx: events.len(),
            ts_event_ns,
        });
    }
    batches
}

pub fn build_trade_index(events: &[MboEvent]) -> Vec<TradeRecord> {
    events
        .iter()
        .filter_map(|event| {
            if !matches!(event.action, BookAction::Trade | BookAction::Fill) {
                return None;
            }
            Some(TradeRecord {
                ts_event_ns: event.ts_event_ns,
                sequence: event.sequence,
                price: event.price_ticks?,
                size: event.size,
                aggressor_side: event.side,
                order_id: event.order_id,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_price_to_es_ticks() {
        assert_eq!(
            dbn_fixed_price_to_es_ticks(4_500_250_000_000),
            Some(PriceTicks(18_001))
        );
    }

    #[test]
    fn batch_builder_uses_last_flag() {
        let events = vec![
            MboEvent::synthetic(
                100,
                1,
                BookAction::Add,
                Some(BookSide::Bid),
                Some(PriceTicks(1)),
                1,
                1,
                false,
            ),
            MboEvent::synthetic(
                100,
                2,
                BookAction::Add,
                Some(BookSide::Ask),
                Some(PriceTicks(2)),
                1,
                2,
                true,
            ),
            MboEvent::synthetic(
                101,
                3,
                BookAction::Trade,
                Some(BookSide::Bid),
                Some(PriceTicks(2)),
                1,
                0,
                true,
            ),
        ];
        let batches = build_batches(&events);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].start_idx, 0);
        assert_eq!(batches[0].end_idx, 2);
    }

    #[test]
    fn trade_index_keeps_trade_and_fill() {
        let events = vec![
            MboEvent::synthetic(
                100,
                1,
                BookAction::Add,
                Some(BookSide::Bid),
                Some(PriceTicks(1)),
                1,
                1,
                true,
            ),
            MboEvent::synthetic(
                101,
                2,
                BookAction::Trade,
                Some(BookSide::Ask),
                Some(PriceTicks(1)),
                2,
                0,
                true,
            ),
            MboEvent::synthetic(
                102,
                3,
                BookAction::Fill,
                Some(BookSide::Ask),
                Some(PriceTicks(1)),
                3,
                9,
                true,
            ),
        ];
        let trades = build_trade_index(&events);
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[1].size, 3);
    }

    #[test]
    fn event_store_validate_accepts_matching_indexes() {
        let events = vec![
            MboEvent::synthetic(
                100,
                1,
                BookAction::Add,
                Some(BookSide::Bid),
                Some(PriceTicks(1)),
                1,
                1,
                true,
            ),
            MboEvent::synthetic(
                101,
                2,
                BookAction::Trade,
                Some(BookSide::Ask),
                Some(PriceTicks(1)),
                2,
                0,
                true,
            ),
        ];
        let store = EventStore {
            batches: build_batches(&events),
            trades: build_trade_index(&events),
            events,
        };

        store.validate().unwrap();
    }

    #[test]
    fn event_store_validate_rejects_mismatched_batches() {
        let events = vec![MboEvent::synthetic(
            100,
            1,
            BookAction::Add,
            Some(BookSide::Bid),
            Some(PriceTicks(1)),
            1,
            1,
            true,
        )];
        let mut store = EventStore {
            batches: build_batches(&events),
            trades: build_trade_index(&events),
            events,
        };
        store.batches[0].end_idx = 0;

        let err = store.validate().unwrap_err().to_string();
        assert!(err.contains("batch index mismatch"));
    }

    #[test]
    fn event_store_validate_rejects_mismatched_trades() {
        let events = vec![MboEvent::synthetic(
            100,
            1,
            BookAction::Trade,
            Some(BookSide::Ask),
            Some(PriceTicks(1)),
            2,
            0,
            true,
        )];
        let mut store = EventStore {
            batches: build_batches(&events),
            trades: build_trade_index(&events),
            events,
        };
        store.trades.clear();

        let err = store.validate().unwrap_err().to_string();
        assert!(err.contains("trade index mismatch"));
    }
}
