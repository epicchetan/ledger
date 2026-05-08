//! Generic scheduler and projection runtime.
//!
//! This crate intentionally does not know about Ledger domain payloads or feed
//! implementations. It schedules external cache writes and projection runs
//! against the standalone `cache` crate.

mod projection;
mod runtime;

pub use projection::{
    Projection, ProjectionContext, ProjectionDescriptor, ProjectionError, ProjectionId,
};
pub use runtime::{ExternalWriteBatch, RunStats, Runtime, RuntimeError, RuntimeStep};
