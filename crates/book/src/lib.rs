//! Pure L3 order book for normalized Ledger market data.
//!
//! `ledger-book` applies `ledger_domain::MboEvent` values to an in-memory book,
//! preserving sorted price levels and FIFO order within each level. It publishes
//! BBO/depth views, deterministic checksums, warnings, and liquidity helpers
//! used by replay execution.
//!
//! This crate must not know about Databento downloads, DBN files, R2, SQLite,
//! session loading, replay timing, or CLI/API behavior.

mod order_book;

pub use order_book::*;
