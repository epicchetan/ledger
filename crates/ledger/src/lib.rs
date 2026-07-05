//! Ledger market-domain data management.
//!
//! This crate owns feed-specific domain knowledge that does not require a
//! running runtime session: ES day derivation, DBN normalization, replay
//! artifact creation, and day cataloging over the object store.

pub mod error;
pub mod feed;
pub mod market;

pub use error::LedgerError;
