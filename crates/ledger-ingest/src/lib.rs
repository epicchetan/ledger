//! Databento ingestion and replay artifact production.
//!
//! This crate owns the ingest pipeline that turns a requested market day into
//! replay-ready artifacts. It integrates with Databento, stages raw DBN input
//! through `ledger-store`, normalizes MBO records into `ledger-core` events,
//! writes replay artifacts, runs the order-book check, and returns an ingest
//! report.
//!
//! Persistence policy belongs to `ledger-store`; this crate calls store APIs
//! for staging, durable object registration, and replay session commits. Runtime
//! and replay loading APIs belong to `ledger-runtime` and `ledger-replay`.

pub mod book_check;
pub mod databento_downloader;
pub mod pipeline;
pub mod preprocess;
pub mod provider;

pub use book_check::*;
pub use databento_downloader::*;
pub use pipeline::*;
pub use preprocess::*;
pub use provider::*;
