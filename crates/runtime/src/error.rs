use cache::CacheError;
use thiserror::Error;

use crate::{ComponentId, ComponentKind};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ComponentError {
    #[error("invalid component id `{0}`")]
    InvalidComponentId(String),

    #[error(transparent)]
    Cache(#[from] CacheError),

    #[error("runtime external write ingress closed")]
    RuntimeIngressClosed,

    #[error("component task join failed: {0}")]
    Join(String),

    #[error("{0}")]
    Message(String),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    #[error("duplicate component `{0}`")]
    DuplicateComponent(ComponentId),

    #[error("missing component `{0}`")]
    MissingComponent(ComponentId),

    #[error("component `{id}` has kind `{found:?}`, expected `{expected:?}`")]
    WrongComponentKind {
        id: ComponentId,
        expected: ComponentKind,
        found: ComponentKind,
    },

    #[error(transparent)]
    Cache(#[from] CacheError),

    #[error("component `{id}` failed: {source}")]
    Component {
        id: ComponentId,
        source: ComponentError,
    },

    #[error("runtime command channel closed")]
    RuntimeCommandClosed,

    #[error("runtime external write ingress closed")]
    RuntimeIngressClosed,

    #[error("runtime worker stopped")]
    RuntimeStopped,

    #[error("process `{id}` join failed: {message}")]
    ProcessJoin { id: ComponentId, message: String },

    #[error("run limit exceeded after {max_steps} steps")]
    RunLimitExceeded { max_steps: usize },
}
