//! Headless projection graph runtime.
//!
//! The runtime can register projection factories, resolve dependency trees,
//! lazily instantiate nodes, and advance deterministic truth ticks from an
//! active `ReplaySession`.

mod metrics;
mod node;
mod registry;
mod runtime;
mod tick;

pub use metrics::*;
pub use node::*;
pub use registry::*;
pub use runtime::*;
pub use tick::*;
