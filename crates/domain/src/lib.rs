//! Shared domain types and pure helpers for Ledger.
//!
//! `ledger-domain` defines the common types used across ingestion, storage,
//! order-book, replay, application, and CLI layers. It owns market-day resolution,
//! normalized MBO events, replay artifact codecs, storage object kind names, and
//! simulator request/profile/result types, and projection/study graph contracts.
//!
//! This crate must stay free of Databento clients, R2/SQLite/filesystem policy,
//! order-book mutation logic, replay orchestration, and CLI behavior.

pub mod artifact_codec;
pub mod event;
pub mod market_day;
pub mod projection;
pub mod sim;
pub mod storage;
pub mod time;

pub use artifact_codec::*;
pub use event::*;
pub use market_day::*;
pub use projection::*;
pub use sim::*;
pub use storage::*;
pub use time::*;
