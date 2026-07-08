use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

pub type UnixNanos = u64;

pub const ES_TICK_SIZE_FIXED_PRICE: i64 = 250_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
pub struct EsMboEvent {
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

/// A canonical trade print per the ledger trade-print policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradePrint {
    pub ts_event_ns: UnixNanos,
    pub price_ticks: PriceTicks,
    pub size: u32,
    /// Aggressor side as reported by the venue; `None` when unattributed.
    pub aggressor: Option<BookSide>,
}

/// Canonical trade-print policy: `Trade` actions with a price and nonzero
/// size count as chart volume; `Fill` mirrors the resting side of the same
/// match and is excluded to avoid double-counting.
pub fn canonical_trade_print(event: &EsMboEvent) -> Option<TradePrint> {
    if event.action != BookAction::Trade || event.size == 0 {
        return None;
    }
    let price_ticks = event.price_ticks?;
    Some(TradePrint {
        ts_event_ns: event.ts_event_ns,
        price_ticks,
        size: event.size,
        aggressor: event.side,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsMboBatchSpan {
    pub start_idx: usize,
    pub end_idx: usize,
    pub ts_event_ns: UnixNanos,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsMboEventStore {
    pub events: Vec<EsMboEvent>,
    pub batches: Vec<EsMboBatchSpan>,
}

impl EsMboEventStore {
    pub fn validate(&self) -> Result<()> {
        let expected_batches = build_batches(&self.events);
        if self.batches != expected_batches {
            bail!(
                "event store batch index mismatch: decoded {} batches, rebuilt {} batches",
                self.batches.len(),
                expected_batches.len()
            );
        }
        Ok(())
    }
}

pub fn dbn_fixed_price_to_es_ticks(price: i64) -> Option<PriceTicks> {
    if price == i64::MAX {
        return None;
    }
    Some(PriceTicks(price / ES_TICK_SIZE_FIXED_PRICE))
}

pub fn build_batches(events: &[EsMboEvent]) -> Vec<EsMboBatchSpan> {
    let mut batches = Vec::new();
    let mut start_idx = 0;
    for (idx, event) in events.iter().enumerate() {
        if event.is_last {
            batches.push(EsMboBatchSpan {
                start_idx,
                end_idx: idx + 1,
                ts_event_ns: event.ts_event_ns,
            });
            start_idx = idx + 1;
        }
    }
    if start_idx < events.len() {
        let ts_event_ns = events
            .last()
            .map(|event| event.ts_event_ns)
            .unwrap_or_default();
        batches.push(EsMboBatchSpan {
            start_idx,
            end_idx: events.len(),
            ts_event_ns,
        });
    }
    batches
}
