//! Generic runtime data plane primitives.
//!
//! This crate intentionally does not know about Ledger domain payloads. It
//! provides named, owned `Value<T>` and `Array<T>` cells with typed handles,
//! owner-checked mutations, and changed-key effects.

mod cell;
mod data_plane;
mod error;
mod key;

pub use cell::{ArrayKey, CellDescriptor, CellKind, CellOwner, ValueKey, WriteEffects};
pub use data_plane::DataPlane;
pub use error::DataPlaneError;
pub use key::Key;
