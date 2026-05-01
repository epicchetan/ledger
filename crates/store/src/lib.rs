//! Storage boundary for Ledger.
//!
//! `ledger-store` owns the durable and local storage mechanics used by ingest
//! and replay:
//!
//! - SQLite catalog state in `data/catalog.sqlite`.
//! - Durable object persistence in R2 or another [`ObjectStore`].
//! - Content-addressed object keys for raw DBN files and derived artifacts.
//! - Ingest staging under `data/tmp/ingest/...`.
//! - Replay dataset loading under `data/sessions/...`.
//! - Replay dataset cache pruning.
//!
//! Raw DBN files are cataloged and stored remotely, but they are not part of the
//! replay dataset cache. Replay datasets cache only the artifacts needed to run
//! a replay: events, batches, trades, and book-check output.
//!
//! This crate should not own provider-specific decoding, order book behavior,
//! replay execution, or CLI presentation. Those belong in neighboring crates.

pub mod cache;
pub mod catalog;
pub mod hash;
pub mod keys;
pub mod local_store;
pub mod object_store;
pub mod r2_store;
pub mod store;

pub use cache::*;
pub use catalog::*;
pub use hash::*;
pub use keys::*;
pub use local_store::*;
pub use object_store::*;
pub use r2_store::*;
pub use store::*;
