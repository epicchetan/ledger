//! Generic component runtime.
//!
//! This crate intentionally does not know about Ledger domain payloads, feeds,
//! studies, replay, playback, or trade concepts. It owns process execution,
//! finite task scheduling, and cache write application against the standalone
//! `cache` crate.

mod component;
mod error;
mod handle;
mod process;
mod runtime;
mod task;
mod worker;
mod write;

pub use component::{
    ComponentDescriptor, ComponentId, ComponentKind, ComponentStatus, TaskDescriptor,
};
pub use error::{ComponentError, RuntimeError};
pub use handle::{ComponentHandle, RuntimeHandle};
pub use process::{ProcessContext, ProcessPrepareContext, RuntimeProcess, ShutdownReceiver};
pub use runtime::{
    ExternalWriteBatch, ExternalWriteReceiver, ExternalWriteSink, RunStats, Runtime, RuntimeStep,
};
pub use task::{RuntimeTask, TaskContext, TaskPrepareContext, TaskWake};
pub use worker::RuntimeWorker;
pub use write::ComponentWriteContext;
