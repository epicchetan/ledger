//! Storage boundary for Ledger.
//!
//! `ledger-store` owns the durable and local storage mechanics used by ingest
//! and replay:
//!
//! - Durable object persistence in R2 or another [`ObjectStore`].
//! - SQLite catalog/control-plane state.
//! - Content-addressed object keys for raw DBN files and derived artifacts.
//! - Ingest staging under `data/tmp/ingest/...`.
//! - Replay dataset staging under `data/tmp/validate/...`.
//!
//! Raw DBN files and replay dataset artifacts are stored remotely. SQLite is
//! the local catalog and control plane. Local files under `tmp` are disposable
//! job staging.
//!
//! This crate should not own provider-specific decoding, order book behavior,
//! replay execution, or CLI presentation. Those belong in neighboring crates.

pub mod catalog;
pub mod hash;
pub mod keys;
pub mod local_store;
pub mod object_store;
pub mod r2_store;
pub mod store;

pub use catalog::*;
pub use hash::*;
pub use keys::*;
pub use local_store::*;
pub use object_store::*;
pub use r2_store::*;
pub use store::*;
