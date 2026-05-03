//! Headless projection graph runtime.
//!
//! Phase 2 intentionally stays synthetic: the runtime can register projection
//! factories, resolve dependency trees, lazily instantiate nodes, and advance
//! deterministic ticks without depending on `ReplaySession` yet.

mod metrics;
mod node;
mod registry;
mod runtime;

pub use metrics::*;
pub use node::*;
pub use registry::*;
pub use runtime::*;
