use anyhow::Result;
use ledger_book::OrderBook;
use ledger_core::EventStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookCheckReport {
    pub event_count: usize,
    pub batch_count: usize,
    pub trade_count: usize,
    pub bbo_change_count: usize,
    pub warning_count: usize,
    pub final_book_checksum: String,
    pub final_order_count: usize,
    pub final_bid_levels: usize,
    pub final_ask_levels: usize,
}

pub fn run_book_check(store: &EventStore) -> Result<BookCheckReport> {
    let mut book = OrderBook::new();
    let mut bbo_change_count = 0;
    let mut warning_count = 0;

    for span in &store.batches {
        let out = book.apply_batch(store.batch_events(*span));
        if out.bbo_changed.is_some() {
            bbo_change_count += 1;
        }
        warning_count += out.warnings.len();
    }

    Ok(BookCheckReport {
        event_count: store.events.len(),
        batch_count: store.batches.len(),
        trade_count: store.trades.len(),
        bbo_change_count,
        warning_count,
        final_book_checksum: book.checksum(),
        final_order_count: book.order_count(),
        final_bid_levels: book.level_count(ledger_core::BookSide::Bid),
        final_ask_levels: book.level_count(ledger_core::BookSide::Ask),
    })
}
