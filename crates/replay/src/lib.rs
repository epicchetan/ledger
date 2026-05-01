//! Headless replay and simulated execution over prepared Ledger event stores.
//!
//! `ledger-replay` consumes a `ledger_domain::EventStore`, steps exchange batches
//! through `ledger_book::OrderBook`, schedules simulated order arrivals and
//! cancels, and emits delayed/coalesced visibility frames.
//!
//! This crate must not download, preprocess, locate, hydrate, or cache market
//! data. Replay dataset loading belongs to `ledger` and storage/catalog work
//! belongs to `ledger-store`.

mod execution;
mod simulator;
mod visibility;

pub use execution::*;
pub use simulator::*;
pub use visibility::*;
