//! Generic object registry and local object storage for Ledger.
//!
//! `store` stores raw files and derived artifacts as content-addressed objects. It
//! owns descriptor persistence, R2 object mechanics, local object hydration, and
//! local object pruning. It does not know Ledger market-domain types, feed internals,
//! runtime cells, or projections.

pub mod descriptor;
pub mod error;
pub mod hash;
pub mod id;
pub mod local_objects;
pub mod local_store;
pub mod registry;
pub mod remote_store;
pub mod store;

pub use descriptor::*;
pub use error::*;
pub use hash::*;
pub use id::*;
pub use local_objects::*;
pub use local_store::*;
pub use registry::*;
pub use remote_store::*;
pub use store::*;
