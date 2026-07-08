//! Ledger market-domain data management.
//!
//! This crate owns feed-specific domain knowledge that does not require a
//! running runtime session: ES day derivation, DBN normalization, replay
//! artifact creation, and day cataloging over the object store.
//! It also wires prepared feeds into runtime-backed sessions.

pub mod clock;
pub mod error;
pub mod feed;
pub mod market;
pub mod projection;
pub mod session;

pub use error::LedgerError;
