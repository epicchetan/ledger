//! Standalone active state primitives for the Ledger runtime.
//!
//! This crate intentionally does not know about feeds, projections, scheduling,
//! persistence, or Ledger domain payloads. It provides named, owned `Value<T>`
//! and `Array<T>` cells with typed handles, owner-checked mutations, and
//! changed-key effects.

mod cache;
mod cell;
mod error;
mod key;

pub use cache::Cache;
pub use cell::{ArrayKey, CellDescriptor, CellKind, CellOwner, ValueKey, WriteEffects};
pub use error::CacheError;
pub use key::Key;
